# stash.py
# Copyright (C) 2018 Jelmer Vernooij <jelmer@samba.org>
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

"""Stash handling."""

import os
import sys
from typing import TYPE_CHECKING, Optional, TypedDict

from .diff_tree import tree_changes
from .file import GitFile
from .index import (
    IndexEntry,
    _tree_to_fs_path,
    build_file_from_blob,
    commit_tree,
    index_entry_from_stat,
    iter_fresh_objects,
    iter_tree_contents,
    symlink,
    update_working_tree,
    validate_path,
    validate_path_element_default,
    validate_path_element_hfs,
    validate_path_element_ntfs,
)
from .objects import S_IFGITLINK, Blob, Commit, ObjectID
from .reflog import drop_reflog_entry, read_reflog
from .refs import Ref

if TYPE_CHECKING:
    from .reflog import Entry
    from .repo import Repo


class CommitKwargs(TypedDict, total=False):
    """Keyword arguments for do_commit."""

    committer: bytes
    author: bytes


DEFAULT_STASH_REF = b"refs/stash"


class Stash:
    """A Git stash.

    Note that this doesn't currently update the working tree.
    """

    def __init__(self, repo: "Repo", ref: Ref = DEFAULT_STASH_REF) -> None:
        self._ref = ref
        self._repo = repo

    @property
    def _reflog_path(self) -> str:
        return os.path.join(self._repo.commondir(), "logs", os.fsdecode(self._ref))

    def stashes(self) -> list["Entry"]:
        try:
            with GitFile(self._reflog_path, "rb") as f:
                return list(reversed(list(read_reflog(f))))
        except FileNotFoundError:
            return []

    @classmethod
    def from_repo(cls, repo: "Repo") -> "Stash":
        """Create a new stash from a Repo object."""
        return cls(repo)

    def drop(self, index: int) -> None:
        """Drop entry with specified index."""
        with open(self._reflog_path, "rb+") as f:
            drop_reflog_entry(f, index, rewrite=True)
        if len(self) == 0:
            os.remove(self._reflog_path)
            del self._repo.refs[self._ref]
            return
        if index == 0:
            self._repo.refs[self._ref] = self[0].new_sha

    def pop(self, index: int) -> "Entry":
        """Pop a stash entry and apply its changes.

        Args:
          index: Index of the stash entry to pop (0 is the most recent)

        Returns:
          The stash entry that was popped
        """
        # Get the stash entry before removing it
        entry = self[index]

        # Get the stash commit
        stash_commit = self._repo.get_object(entry.new_sha)
        assert isinstance(stash_commit, Commit)

        # The stash commit has the working tree changes
        # Its first parent is the commit the stash was based on
        # Its second parent is the index commit
        if len(stash_commit.parents) < 1:
            raise ValueError("Invalid stash entry: no parent commits")

        base_commit_sha = stash_commit.parents[0]

        # Get current HEAD to determine if we can apply cleanly
        try:
            current_head = self._repo.refs[b"HEAD"]
        except KeyError:
            raise ValueError("Cannot pop stash: no HEAD")

        # Check if we're at the same commit where the stash was created
        # If not, we need to do a three-way merge
        if current_head != base_commit_sha:
            # For now, we'll apply changes directly but this could cause conflicts
            # A full implementation would do a three-way merge
            pass

        # Apply the stash changes to the working tree and index
        # Get config for working directory update
        config = self._repo.get_config()
        honor_filemode = config.get_boolean(b"core", b"filemode", os.name != "nt")

        if config.get_boolean(b"core", b"core.protectNTFS", os.name == "nt"):
            validate_path_element = validate_path_element_ntfs
        elif config.get_boolean(b"core", b"core.protectHFS", sys.platform == "darwin"):
            validate_path_element = validate_path_element_hfs
        else:
            validate_path_element = validate_path_element_default

        if config.get_boolean(b"core", b"symlinks", True):
            symlink_fn = symlink
        else:

            def symlink_fn(source, target) -> None:  # type: ignore
                mode = "w" + ("b" if isinstance(source, bytes) else "")
                with open(target, mode) as f:
                    f.write(source)

        # Get blob normalizer for line ending conversion
        blob_normalizer = self._repo.get_blob_normalizer()

        # Open the index
        repo_index = self._repo.open_index()

        # Apply working tree changes
        stash_tree_id = stash_commit.tree
        repo_path = os.fsencode(self._repo.path)

        # First, if we have index changes (second parent), restore the index state
        if len(stash_commit.parents) >= 2:
            index_commit_sha = stash_commit.parents[1]
            index_commit = self._repo.get_object(index_commit_sha)
            assert isinstance(index_commit, Commit)
            index_tree_id = index_commit.tree

            # Update index entries from the stashed index tree
            for entry in iter_tree_contents(self._repo.object_store, index_tree_id):
                if not validate_path(entry.path, validate_path_element):
                    continue

                # Add to index with stage 0 (normal)
                # Get file stats for the entry
                full_path = _tree_to_fs_path(repo_path, entry.path)
                try:
                    st = os.lstat(full_path)
                except FileNotFoundError:
                    # File doesn't exist yet, use dummy stats
                    st = os.stat_result((entry.mode, 0, 0, 0, 0, 0, 0, 0, 0, 0))
                repo_index[entry.path] = index_entry_from_stat(st, entry.sha)

        # Apply working tree changes from the stash
        for entry in iter_tree_contents(self._repo.object_store, stash_tree_id):
            if not validate_path(entry.path, validate_path_element):
                continue

            full_path = _tree_to_fs_path(repo_path, entry.path)

            # Create parent directories if needed
            parent_dir = os.path.dirname(full_path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir)

            # Write the file
            if entry.mode == S_IFGITLINK:
                # Submodule - just create directory
                if not os.path.isdir(full_path):
                    os.mkdir(full_path)
                st = os.lstat(full_path)
            else:
                obj = self._repo.object_store[entry.sha]
                assert isinstance(obj, Blob)
                # Apply blob normalization for checkout if normalizer is provided
                if blob_normalizer is not None:
                    obj = blob_normalizer.checkout_normalize(obj, entry.path)
                st = build_file_from_blob(
                    obj,
                    entry.mode,
                    full_path,
                    honor_filemode=honor_filemode,
                    symlink_fn=symlink_fn,
                )

            # Update index if the file wasn't already staged
            if entry.path not in repo_index:
                # Update with file stats from disk
                repo_index[entry.path] = index_entry_from_stat(st, entry.sha)
            else:
                existing_entry = repo_index[entry.path]

                if (
                    isinstance(existing_entry, IndexEntry)
                    and existing_entry.mode == entry.mode
                    and existing_entry.sha == entry.sha
                ):
                    # Update with file stats from disk
                    repo_index[entry.path] = index_entry_from_stat(st, entry.sha)

        # Write the updated index
        repo_index.write()

        # Remove the stash entry
        self.drop(index)

        return entry

    def push(
        self,
        committer: Optional[bytes] = None,
        author: Optional[bytes] = None,
        message: Optional[bytes] = None,
    ) -> ObjectID:
        """Create a new stash.

        Args:
          committer: Optional committer name to use
          author: Optional author name to use
          message: Optional commit message
        """
        # First, create the index commit.
        commit_kwargs = CommitKwargs()
        if committer is not None:
            commit_kwargs["committer"] = committer
        if author is not None:
            commit_kwargs["author"] = author

        index = self._repo.open_index()
        index_tree_id = index.commit(self._repo.object_store)
        # Create a dangling commit for the index state
        # Note: We pass ref=None which is handled specially in do_commit
        # to create a commit without updating any reference
        index_commit_id = self._repo.get_worktree().commit(
            tree=index_tree_id,
            message=b"Index stash",
            merge_heads=[self._repo.head()],
            no_verify=True,
            ref=None,  # Don't update any ref
            **commit_kwargs,
        )

        # Then, the working tree one.
        # Filter out entries with None values since commit_tree expects non-None values
        fresh_objects = [
            (path, sha, mode)
            for path, sha, mode in iter_fresh_objects(
                index,
                os.fsencode(self._repo.path),
                object_store=self._repo.object_store,
            )
            if sha is not None and mode is not None
        ]
        stash_tree_id = commit_tree(
            self._repo.object_store,
            fresh_objects,
        )

        if message is None:
            message = b"A stash on " + self._repo.head()

        # TODO(jelmer): Just pass parents into do_commit()?
        self._repo.refs[self._ref] = self._repo.head()

        cid = self._repo.get_worktree().commit(
            ref=self._ref,
            tree=stash_tree_id,
            message=message,
            merge_heads=[index_commit_id],
            no_verify=True,
            **commit_kwargs,
        )

        # Reset working tree and index to HEAD to match git's behavior
        # Use update_working_tree to reset from stash tree to HEAD tree
        # Get HEAD tree
        head_commit = self._repo.get_object(self._repo.head())
        assert isinstance(head_commit, Commit)
        head_tree_id = head_commit.tree

        # Update from stash tree to HEAD tree
        # This will remove files that were in stash but not in HEAD,
        # and restore files to their HEAD versions
        changes = tree_changes(self._repo.object_store, stash_tree_id, head_tree_id)
        update_working_tree(
            self._repo,
            old_tree_id=stash_tree_id,
            new_tree_id=head_tree_id,
            change_iterator=changes,
            allow_overwrite_modified=True,  # We need to overwrite modified files
        )

        return cid

    def __getitem__(self, index: int) -> "Entry":
        return list(self.stashes())[index]

    def __len__(self) -> int:
        return len(list(self.stashes()))
