# filter_branch.py - Git filter-branch functionality
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# You should have received a copy of the licenses; if not, see
# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
# License, Version 2.0.
#

"""Git filter-branch implementation."""

__all__ = [
    "CommitData",
    "CommitFilter",
    "filter_refs",
]

import os
import tempfile
import warnings
from collections.abc import Callable, Sequence
from typing import TypedDict

from .index import Index, build_index_from_tree
from .object_store import BaseObjectStore
from .objects import Commit, ObjectID, Tag, Tree
from .refs import Ref, RefsContainer, local_tag_name


class CommitData(TypedDict, total=False):
    """TypedDict for commit data fields."""

    author: bytes
    author_time: int
    author_timezone: int
    committer: bytes
    commit_time: int
    commit_timezone: int
    message: bytes
    encoding: bytes


class CommitFilter:
    """Filter for rewriting commits during filter-branch operations."""

    def __init__(
        self,
        object_store: BaseObjectStore,
        *,
        filter_fn: Callable[[Commit], CommitData | None] | None = None,
        filter_author: Callable[[bytes], bytes | None] | None = None,
        filter_committer: Callable[[bytes], bytes | None] | None = None,
        filter_message: Callable[[bytes], bytes | None] | None = None,
        tree_filter: Callable[[ObjectID, str], ObjectID | None] | None = None,
        index_filter: Callable[[ObjectID, str], ObjectID | None] | None = None,
        parent_filter: Callable[[Sequence[ObjectID]], list[ObjectID]] | None = None,
        commit_filter: Callable[[Commit, ObjectID], ObjectID | None] | None = None,
        subdirectory_filter: bytes | None = None,
        prune_empty: bool = False,
        tag_name_filter: Callable[[bytes], bytes | None] | None = None,
    ):
        """Initialize a commit filter.

        Args:
          object_store: Object store to read from and write to
          filter_fn: Optional callable that takes a Commit object and returns
            a dict of updated fields (author, committer, message, etc.)
          filter_author: Optional callable that takes author bytes and returns
            updated author bytes or None to keep unchanged
          filter_committer: Optional callable that takes committer bytes and returns
            updated committer bytes or None to keep unchanged
          filter_message: Optional callable that takes commit message bytes
            and returns updated message bytes
          tree_filter: Optional callable that takes (tree_sha, temp_dir) and returns
            new tree SHA after modifying working directory
          index_filter: Optional callable that takes (tree_sha, temp_index_path) and
            returns new tree SHA after modifying index
          parent_filter: Optional callable that takes parent list and returns
            modified parent list
          commit_filter: Optional callable that takes (Commit, tree_sha) and returns
            new commit SHA or None to skip commit
          subdirectory_filter: Optional subdirectory path to extract as new root
          prune_empty: Whether to prune commits that become empty
          tag_name_filter: Optional callable to rename tags
        """
        self.object_store = object_store
        self.filter_fn = filter_fn
        self.filter_author = filter_author
        self.filter_committer = filter_committer
        self.filter_message = filter_message
        self.tree_filter = tree_filter
        self.index_filter = index_filter
        self.parent_filter = parent_filter
        self.commit_filter = commit_filter
        self.subdirectory_filter = subdirectory_filter
        self.prune_empty = prune_empty
        self.tag_name_filter = tag_name_filter
        self._old_to_new: dict[ObjectID, ObjectID] = {}
        self._processed: set[ObjectID] = set()
        self._tree_cache: dict[ObjectID, ObjectID] = {}  # Cache for filtered trees

    def _filter_tree_with_subdirectory(
        self, tree_sha: ObjectID, subdirectory: bytes
    ) -> ObjectID | None:
        """Extract a subdirectory from a tree as the new root.

        Args:
          tree_sha: SHA of the tree to filter
          subdirectory: Path to subdirectory to extract

        Returns:
          SHA of the new tree containing only the subdirectory, or None if not found
        """
        try:
            tree = self.object_store[tree_sha]
            if not isinstance(tree, Tree):
                return None
        except KeyError:
            return None

        # Split subdirectory path
        parts = subdirectory.split(b"/")
        current_tree = tree

        # Navigate to subdirectory
        for part in parts:
            if not part:
                continue
            found = False
            for entry in current_tree.items():
                if entry.path == part:
                    try:
                        assert entry.sha is not None
                        obj = self.object_store[entry.sha]
                        if isinstance(obj, Tree):
                            current_tree = obj
                            found = True
                            break
                    except KeyError:
                        return None
            if not found:
                # Subdirectory not found, return empty tree
                empty_tree = Tree()
                self.object_store.add_object(empty_tree)
                return empty_tree.id

        # Return the subdirectory tree
        return current_tree.id

    def _apply_tree_filter(self, tree_sha: ObjectID) -> ObjectID:
        """Apply tree filter by checking out tree and running filter.

        Args:
          tree_sha: SHA of the tree to filter

        Returns:
          SHA of the filtered tree
        """
        if tree_sha in self._tree_cache:
            return self._tree_cache[tree_sha]

        if not self.tree_filter:
            self._tree_cache[tree_sha] = tree_sha
            return tree_sha

        # Create temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            # Check out tree to temp directory
            # We need a proper checkout implementation here
            # For now, pass tmpdir to filter and let it handle checkout
            new_tree_sha = self.tree_filter(tree_sha, tmpdir)
            if new_tree_sha is None:
                new_tree_sha = tree_sha

            self._tree_cache[tree_sha] = new_tree_sha
            return new_tree_sha

    def _apply_index_filter(self, tree_sha: ObjectID) -> ObjectID:
        """Apply index filter by creating temp index and running filter.

        Args:
          tree_sha: SHA of the tree to filter

        Returns:
          SHA of the filtered tree
        """
        if tree_sha in self._tree_cache:
            return self._tree_cache[tree_sha]

        if not self.index_filter:
            self._tree_cache[tree_sha] = tree_sha
            return tree_sha

        # Create temporary index file
        with tempfile.NamedTemporaryFile(delete=False) as tmp_index:
            tmp_index_path = tmp_index.name

        try:
            # Build index from tree
            build_index_from_tree(".", tmp_index_path, self.object_store, tree_sha)

            # Run index filter
            new_tree_sha = self.index_filter(tree_sha, tmp_index_path)
            if new_tree_sha is None:
                # Read back the modified index and create new tree
                index = Index(tmp_index_path)
                new_tree_sha = index.commit(self.object_store)

            self._tree_cache[tree_sha] = new_tree_sha
            return new_tree_sha
        finally:
            os.unlink(tmp_index_path)

    def process_commit(self, commit_sha: ObjectID) -> ObjectID | None:
        """Process a single commit, creating a filtered version.

        Args:
          commit_sha: SHA of the commit to process

        Returns:
          SHA of the new commit, or None if object not found
        """
        if commit_sha in self._processed:
            return self._old_to_new.get(commit_sha, commit_sha)

        self._processed.add(commit_sha)

        try:
            commit = self.object_store[commit_sha]
        except KeyError:
            # Object not found
            return None

        if not isinstance(commit, Commit):
            # Not a commit, return as-is
            self._old_to_new[commit_sha] = commit_sha
            return commit_sha

        # Process parents first
        new_parents = []
        for parent in commit.parents:
            new_parent = self.process_commit(parent)
            if new_parent:  # Skip None parents
                new_parents.append(new_parent)

        # Apply parent filter
        if self.parent_filter:
            new_parents = self.parent_filter(new_parents)

        # Apply tree filters
        new_tree = commit.tree

        # Subdirectory filter takes precedence
        if self.subdirectory_filter:
            filtered_tree = self._filter_tree_with_subdirectory(
                commit.tree, self.subdirectory_filter
            )
            if filtered_tree:
                new_tree = filtered_tree

        # Then apply tree filter
        if self.tree_filter:
            new_tree = self._apply_tree_filter(new_tree)

        # Or apply index filter
        elif self.index_filter:
            new_tree = self._apply_index_filter(new_tree)

        # Check if we should prune empty commits
        if self.prune_empty and len(new_parents) == 1:
            # Check if tree is same as parent's tree
            parent_commit = self.object_store[new_parents[0]]
            if isinstance(parent_commit, Commit) and parent_commit.tree == new_tree:
                # This commit doesn't change anything, skip it
                self._old_to_new[commit_sha] = new_parents[0]
                return new_parents[0]

        # Apply filters
        new_data: CommitData = {}

        # Custom filter function takes precedence
        if self.filter_fn:
            filtered = self.filter_fn(commit)
            if filtered:
                new_data.update(filtered)

        # Apply specific filters
        if self.filter_author and "author" not in new_data:
            new_author = self.filter_author(commit.author)
            if new_author is not None:
                new_data["author"] = new_author

        if self.filter_committer and "committer" not in new_data:
            new_committer = self.filter_committer(commit.committer)
            if new_committer is not None:
                new_data["committer"] = new_committer

        if self.filter_message and "message" not in new_data:
            new_message = self.filter_message(commit.message)
            if new_message is not None:
                new_data["message"] = new_message

        # Create new commit if anything changed
        if new_data or new_parents != commit.parents or new_tree != commit.tree:
            new_commit = Commit()
            new_commit.tree = new_tree
            new_commit.parents = new_parents
            new_commit.author = new_data.get("author", commit.author)
            new_commit.author_time = new_data.get("author_time", commit.author_time)
            new_commit.author_timezone = new_data.get(
                "author_timezone", commit.author_timezone
            )
            new_commit.committer = new_data.get("committer", commit.committer)
            new_commit.commit_time = new_data.get("commit_time", commit.commit_time)
            new_commit.commit_timezone = new_data.get(
                "commit_timezone", commit.commit_timezone
            )
            new_commit.message = new_data.get("message", commit.message)
            new_commit.encoding = new_data.get("encoding", commit.encoding)

            # Copy extra fields
            if hasattr(commit, "_author_timezone_neg_utc"):
                new_commit._author_timezone_neg_utc = commit._author_timezone_neg_utc
            if hasattr(commit, "_commit_timezone_neg_utc"):
                new_commit._commit_timezone_neg_utc = commit._commit_timezone_neg_utc
            if hasattr(commit, "_extra"):
                new_commit._extra = list(commit._extra)
            if hasattr(commit, "_gpgsig"):
                new_commit._gpgsig = commit._gpgsig
            if hasattr(commit, "_mergetag"):
                new_commit._mergetag = list(commit._mergetag)

            # Apply commit filter if provided
            if self.commit_filter:
                # The commit filter can create a completely new commit
                new_commit_sha = self.commit_filter(new_commit, new_tree)
                if new_commit_sha is None:
                    # Skip this commit
                    if len(new_parents) == 1:
                        self._old_to_new[commit_sha] = new_parents[0]
                        return new_parents[0]
                    elif len(new_parents) == 0:
                        return None
                    else:
                        # Multiple parents, can't skip
                        # Store the new commit anyway
                        self.object_store.add_object(new_commit)
                        self._old_to_new[commit_sha] = new_commit.id
                        return new_commit.id
                else:
                    self._old_to_new[commit_sha] = new_commit_sha
                    return new_commit_sha
            else:
                # Store the new commit
                self.object_store.add_object(new_commit)
                self._old_to_new[commit_sha] = new_commit.id
                return new_commit.id
        else:
            # No changes, keep original
            self._old_to_new[commit_sha] = commit_sha
            return commit_sha

    def get_mapping(self) -> dict[ObjectID, ObjectID]:
        """Get the mapping of old commit SHAs to new commit SHAs.

        Returns:
          Dictionary mapping old SHAs to new SHAs
        """
        return self._old_to_new.copy()


def filter_refs(
    refs: RefsContainer,
    object_store: BaseObjectStore,
    ref_names: Sequence[bytes],
    commit_filter: CommitFilter,
    *,
    keep_original: bool = True,
    force: bool = False,
    tag_callback: Callable[[Ref, Ref], None] | None = None,
) -> dict[ObjectID, ObjectID]:
    """Filter commits reachable from the given refs.

    Args:
      refs: Repository refs container
      object_store: Object store containing commits
      ref_names: List of ref names to filter
      commit_filter: CommitFilter instance to use
      keep_original: Keep original refs under refs/original/
      force: Force operation even if refs have been filtered before
      tag_callback: Optional callback for processing tags

    Returns:
      Dictionary mapping old commit SHAs to new commit SHAs

    Raises:
      ValueError: If refs have already been filtered and force is False
    """
    # Check if already filtered
    if keep_original and not force:
        for ref in ref_names:
            original_ref = Ref(b"refs/original/" + ref)
            if original_ref in refs:
                raise ValueError(
                    f"Branch {ref.decode()} appears to have been filtered already. "
                    "Use force=True to force re-filtering."
                )

    # Process commits starting from refs
    for ref in ref_names:
        try:
            # Get the commit SHA for this ref
            ref_obj = Ref(ref)
            if ref_obj in refs:
                ref_sha = refs[ref_obj]
                if ref_sha:
                    commit_filter.process_commit(ref_sha)
        except KeyError:
            # Skip refs that can't be resolved
            warnings.warn(f"Could not process ref {ref!r}: ref not found")
            continue

    # Update refs
    mapping = commit_filter.get_mapping()
    for ref in ref_names:
        try:
            ref_obj = Ref(ref)
            if ref_obj in refs:
                old_sha = refs[ref_obj]
                new_sha = mapping.get(old_sha, old_sha)

                if old_sha != new_sha:
                    # Save original ref if requested
                    if keep_original:
                        original_ref = Ref(b"refs/original/" + ref)
                        refs[original_ref] = old_sha

                    # Update ref to new commit
                    refs[ref_obj] = new_sha
        except KeyError:
            # Not a valid ref, skip updating
            warnings.warn(f"Could not update ref {ref!r}: ref not found")
            continue

    # Handle tag filtering
    if commit_filter.tag_name_filter and tag_callback:
        # Process all tags
        for ref in refs.allkeys():
            if ref.startswith(b"refs/tags/"):
                # Get the tag object or commit it points to
                tag_sha = refs[ref]
                tag_obj = object_store[tag_sha]
                tag_name = ref[10:]  # Remove 'refs/tags/'

                # Check if it's an annotated tag
                if isinstance(tag_obj, Tag):
                    # Get the commit it points to
                    target_sha = tag_obj.object[1]
                    # Process tag if:
                    # 1. It points to a rewritten commit, OR
                    # 2. We want to rename the tag regardless
                    if (
                        target_sha in mapping
                        or commit_filter.tag_name_filter is not None
                    ):
                        new_tag_name = commit_filter.tag_name_filter(tag_name)
                        if new_tag_name and new_tag_name != tag_name:
                            # For annotated tags pointing to rewritten commits,
                            # we need to create a new tag object
                            if target_sha in mapping:
                                new_target = mapping[target_sha]
                                # Create new tag object pointing to rewritten commit
                                new_tag = Tag()
                                new_tag.object = (tag_obj.object[0], new_target)
                                new_tag.name = new_tag_name
                                new_tag.message = tag_obj.message
                                new_tag.tagger = tag_obj.tagger
                                new_tag.tag_time = tag_obj.tag_time
                                new_tag.tag_timezone = tag_obj.tag_timezone
                                object_store.add_object(new_tag)
                                # Update ref to point to new tag object
                                refs[local_tag_name(new_tag_name)] = new_tag.id
                                # Delete old tag
                                del refs[ref]
                            else:
                                # Just rename the tag
                                new_ref = local_tag_name(new_tag_name)
                                tag_callback(ref, new_ref)
                elif isinstance(tag_obj, Commit):
                    # Lightweight tag - points directly to a commit
                    # Process if commit was rewritten or we want to rename
                    if tag_sha in mapping or commit_filter.tag_name_filter is not None:
                        new_tag_name = commit_filter.tag_name_filter(tag_name)
                        if new_tag_name and new_tag_name != tag_name:
                            new_ref = local_tag_name(new_tag_name)
                            if tag_sha in mapping:
                                # Point to rewritten commit
                                refs[new_ref] = mapping[tag_sha]
                                del refs[ref]
                            else:
                                # Just rename
                                tag_callback(ref, new_ref)

    return mapping
