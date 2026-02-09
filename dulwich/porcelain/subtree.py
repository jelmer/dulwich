# subtree.py -- Porcelain for subtree operations
# Copyright (C) 2025 Dulwich contributors
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

"""Porcelain for git subtree operations."""

__all__ = [
    "subtree_add",
    "subtree_merge",
    "subtree_pull",
    "subtree_push",
    "subtree_split",
]

import os
import time
from typing import TYPE_CHECKING

from dulwich.errors import NotCommitError
from dulwich.graph import find_merge_base
from dulwich.merge import three_way_merge
from dulwich.objects import Commit, ObjectID, Tag, Tree
from dulwich.objectspec import parse_commit
from dulwich.refs import LOCAL_BRANCH_PREFIX
from dulwich.repo import Repo
from dulwich.subtree import (
    add_subtree_metadata,
    create_tree_with_subtree,
    extract_subtree,
)

if TYPE_CHECKING:
    from . import RepoPath
else:
    RepoPath = str | os.PathLike[str] | Repo


def _create_subtree_commit(
    repo: Repo,
    tree_id: ObjectID,
    parents: list[ObjectID],
    message: bytes,
    base_commit: Commit,
) -> ObjectID:
    """Create a subtree commit with standard metadata.

    Args:
      repo: Repository to create commit in
      tree_id: Tree ID for the commit
      parents: Parent commit IDs
      message: Commit message (with trailers already added)
      base_commit: Base commit to copy author/committer info from

    Returns:
      Object ID of the new commit
    """
    new_commit_obj = base_commit.copy()
    if not isinstance(new_commit_obj, Commit):
        raise TypeError("Expected Commit copy")
    new_commit_obj.tree = tree_id
    new_commit_obj.parents = parents
    new_commit_obj.author_time = int(time.time())
    new_commit_obj.commit_time = int(time.time())
    new_commit_obj.message = message

    repo.object_store.add_object(new_commit_obj)
    return new_commit_obj.id


def subtree_add(
    repo: RepoPath,
    prefix: bytes | str,
    repository: str | None = None,
    ref: bytes | str | None = None,
    commit: bytes | str | Commit | Tag | None = None,
    squash: bool = False,
    message: bytes | None = None,
) -> ObjectID:
    """Add a subtree to the repository.

    Args:
      repo: Repository to add subtree to
      prefix: Directory path where subtree will be added
      repository: URL of repository to add (for remote subtrees)
      ref: Ref to fetch from repository
      commit: Commit object to add (for local subtrees)
      squash: If True, squash history into a single commit
      message: Custom commit message

    Returns:
      Object ID of the new commit

    Raises:
      Error: If prefix already exists or commit cannot be found
      ValueError: If neither (repository, ref) nor commit is provided
    """
    from . import Error, fetch, open_repo_closing

    if isinstance(prefix, str):
        prefix = prefix.encode("utf-8")

    with open_repo_closing(repo) as r:
        # Validate that prefix doesn't already exist
        head_commit = r[b"HEAD"]
        if not isinstance(head_commit, Commit):
            raise Error("HEAD is not a commit")

        head_tree = r[head_commit.tree]
        if not isinstance(head_tree, Tree):
            raise Error("HEAD tree is invalid")

        if extract_subtree(r.object_store, head_tree, prefix) is not None:
            raise Error(f"prefix '{prefix.decode('utf-8', 'replace')}' already exists")

        # Fetch or parse the commit to add
        if commit is not None:
            # Local commit by SHA or commit object already in this repo
            if isinstance(commit, (bytes, str)):
                if isinstance(commit, str):
                    commit = commit.encode("utf-8")
                subtree_commit = parse_commit(r, commit)
            elif isinstance(commit, Tag):
                obj = r[commit.object[1]]
                if not isinstance(obj, Commit):
                    raise NotCommitError(commit.object[1])
                subtree_commit = obj
            else:
                # Commit object - must already be in this repo's object store
                subtree_commit = commit
        elif repository and ref:
            # Remote repository - fetch it
            if isinstance(ref, str):
                ref = ref.encode("utf-8")

            # Fetch from remote (fetches all refs)
            from io import StringIO

            fetch_result = fetch(r, repository, outstream=StringIO())

            # Get the fetched commit
            from dulwich.refs import Ref

            if ref.startswith(b"refs/"):
                ref_key = Ref(ref)
            else:
                ref_key = Ref(LOCAL_BRANCH_PREFIX + ref)

            commit_id = fetch_result.refs.get(ref_key)
            if commit_id is None:
                # Ref not found, maybe it's a commit SHA already in the repo
                try:
                    subtree_commit_obj = r[ref]
                    if not isinstance(subtree_commit_obj, Commit):
                        raise NotCommitError(ref)
                    subtree_commit = subtree_commit_obj
                except KeyError:
                    raise Error(
                        f"Ref or commit '{ref.decode('utf-8', 'replace')}' not found"
                    )
            else:
                subtree_commit_obj = r[commit_id]
                if not isinstance(subtree_commit_obj, Commit):
                    raise NotCommitError(commit_id)
                subtree_commit = subtree_commit_obj
        else:
            raise ValueError("Either commit or (repository, ref) must be provided")

        # Get the tree to add
        subtree_tree = r[subtree_commit.tree]
        if not isinstance(subtree_tree, Tree):
            raise Error("Subtree commit tree is invalid")

        # Create new tree with subtree inserted
        new_tree_id = create_tree_with_subtree(
            r.object_store, head_tree, prefix, subtree_tree
        )

        # Create commit message
        if message is None:
            if repository:
                ref_str = (
                    ref.decode("utf-8", "replace") if isinstance(ref, bytes) else ref
                )
                subtree_desc = f"'{repository}' at '{ref_str}'"
            else:
                subtree_desc = str(subtree_commit.id)
            message = f"Add '{prefix.decode('utf-8', 'replace')}' from {subtree_desc}".encode()

        # Add subtree metadata
        message = add_subtree_metadata(message, prefix, split=subtree_commit.id)

        # Create commit parents
        parents = [head_commit.id]
        if not squash:
            parents.append(subtree_commit.id)

        # Create new commit
        new_commit_id = _create_subtree_commit(
            r, new_tree_id, parents, message, head_commit
        )

        # Update HEAD
        r[b"HEAD"] = new_commit_id

        return new_commit_id


def subtree_merge(
    repo: RepoPath,
    prefix: bytes | str,
    commit: bytes | str | Commit | Tag,
    squash: bool = False,
    message: bytes | None = None,
) -> ObjectID:
    """Merge changes into an existing subtree.

    Args:
      repo: Repository to merge into
      prefix: Directory path of the subtree
      commit: Commit to merge
      squash: If True, squash history into a single commit
      message: Custom commit message

    Returns:
      Object ID of the merge commit

    Raises:
      Error: If prefix doesn't exist or merge fails
    """
    from . import Error, open_repo_closing

    if isinstance(prefix, str):
        prefix = prefix.encode("utf-8")

    with open_repo_closing(repo) as r:
        # Parse the commit to merge
        if isinstance(commit, (bytes, str)):
            if isinstance(commit, str):
                commit = commit.encode("utf-8")
            merge_commit = parse_commit(r, commit)
        elif isinstance(commit, Tag):
            obj = r[commit.object[1]]
            if not isinstance(obj, Commit):
                raise NotCommitError(commit.object[1])
            merge_commit = obj
        else:
            merge_commit = commit

        # Get current HEAD
        head_commit = r[b"HEAD"]
        if not isinstance(head_commit, Commit):
            raise Error("HEAD is not a commit")

        # Validate that prefix exists
        head_tree = r[head_commit.tree]
        if not isinstance(head_tree, Tree):
            raise Error("HEAD tree is invalid")

        if extract_subtree(r.object_store, head_tree, prefix) is None:
            raise Error(f"prefix '{prefix.decode('utf-8', 'replace')}' does not exist")

        # Find the merge base for proper three-way merge
        merge_bases = find_merge_base(r, [head_commit.id, merge_commit.id])

        if merge_bases:
            # Get the base commit for the merge
            base_commit_obj = r[merge_bases[0]]
            if not isinstance(base_commit_obj, Commit):
                raise Error("Merge base is not a commit")
        else:
            # No common ancestor - this is unusual for subtree merge
            base_commit_obj = None

        # Perform three-way merge on the subtree portion
        merge_tree_obj = r[merge_commit.tree]
        if not isinstance(merge_tree_obj, Tree):
            raise Error("Merge commit tree is invalid")

        # Extract the subtree from each version
        merge_subtree = extract_subtree(r.object_store, merge_tree_obj, prefix)
        if merge_subtree is None:
            # The merge commit doesn't have this subtree path - treat as deletion
            # For subtree merge, we typically want the whole tree from merge_commit
            merge_subtree = merge_tree_obj

        head_subtree = extract_subtree(r.object_store, head_tree, prefix)
        assert head_subtree is not None  # We already checked this exists

        base_subtree = None
        if base_commit_obj:
            base_tree_obj = r[base_commit_obj.tree]
            if isinstance(base_tree_obj, Tree):
                base_subtree = extract_subtree(r.object_store, base_tree_obj, prefix)

        # Perform three-way merge on the subtrees
        # Create temporary commits to use the merge infrastructure
        temp_head = head_commit.copy()
        assert isinstance(temp_head, Commit)
        temp_head.tree = head_subtree.id

        temp_merge = merge_commit.copy()
        assert isinstance(temp_merge, Commit)
        temp_merge.tree = merge_subtree.id

        if base_subtree and base_commit_obj:
            temp_base_obj = base_commit_obj.copy()
            if not isinstance(temp_base_obj, Commit):
                raise TypeError("Expected Commit copy")
            temp_base_obj.tree = base_subtree.id
            temp_base = temp_base_obj
        else:
            temp_base = None

        # Perform the three-way merge
        merged_subtree, conflicts = three_way_merge(
            r.object_store, temp_base, temp_head, temp_merge
        )

        if conflicts and not squash:
            # Report conflicts but continue - user will need to resolve
            import sys

            for path in conflicts:
                prefix_str = prefix.decode("utf-8", "replace")
                path_str = path.decode("utf-8", "replace")
                print(
                    f"CONFLICT (content): Merge conflict in {prefix_str}/{path_str}",
                    file=sys.stderr,
                )

        # Store the merged subtree
        r.object_store.add_object(merged_subtree)

        # Insert the merged subtree back into the main tree
        new_tree_id = create_tree_with_subtree(
            r.object_store, head_tree, prefix, merged_subtree
        )

        # Create commit message
        if message is None:
            merge_id_str = merge_commit.id.decode("utf-8", "replace")
            prefix_str = prefix.decode("utf-8", "replace")
            message = f"Merge commit '{merge_id_str}' as '{prefix_str}'".encode()

        # Add subtree metadata
        message = add_subtree_metadata(message, prefix, split=merge_commit.id)

        # Create commit parents
        parents = [head_commit.id]
        if not squash:
            parents.append(merge_commit.id)

        # Create merge commit
        new_commit_id = _create_subtree_commit(
            r, new_tree_id, parents, message, head_commit
        )

        # Update HEAD
        r[b"HEAD"] = new_commit_id

        return new_commit_id


def subtree_split(
    repo: RepoPath,
    prefix: bytes | str,
    branch: bytes | str | None = None,
    rejoin: bool = False,
    onto: bytes | str | None = None,
) -> ObjectID:
    """Split a subtree into a separate branch.

    Args:
      repo: Repository to split from
      prefix: Directory path of the subtree to split
      branch: Branch name to create (optional)
      rejoin: If True, merge the split branch back with metadata
      onto: Commit to use as the initial parent (optional)

    Returns:
      Object ID of the split commit

    Raises:
      Error: If prefix doesn't exist
    """
    from . import Error, open_repo_closing

    if isinstance(prefix, str):
        prefix = prefix.encode("utf-8")
    if isinstance(branch, str):
        branch = branch.encode("utf-8")
    if isinstance(onto, str):
        onto = onto.encode("utf-8")

    with open_repo_closing(repo) as r:
        # Get current HEAD
        head_commit = r[b"HEAD"]
        if not isinstance(head_commit, Commit):
            raise Error("HEAD is not a commit")

        # Validate that prefix exists
        head_tree = r[head_commit.tree]
        if not isinstance(head_tree, Tree):
            raise Error("HEAD tree is invalid")

        subtree = extract_subtree(r.object_store, head_tree, prefix)
        if subtree is None:
            raise Error(f"prefix '{prefix.decode('utf-8', 'replace')}' does not exist")

        # Walk the history and extract subtree commits
        def process_commit(
            commit_id: ObjectID,
            cache: dict[ObjectID, ObjectID | None],
        ) -> ObjectID | None:
            """Process a commit and return the rewritten commit ID."""
            if commit_id in cache:
                return cache[commit_id]

            commit = r[commit_id]
            if not isinstance(commit, Commit):
                cache[commit_id] = None
                return None

            # Get the subtree for this commit
            commit_tree = r[commit.tree]
            if not isinstance(commit_tree, Tree):
                cache[commit_id] = None
                return None

            commit_subtree = extract_subtree(r.object_store, commit_tree, prefix)
            if commit_subtree is None:
                # This commit doesn't have the subtree
                cache[commit_id] = None
                return None

            # Process parents
            new_parents: list[ObjectID] = []
            for parent_id in commit.parents:
                parent_split = process_commit(parent_id, cache)
                if parent_split is not None:
                    new_parents.append(parent_split)

            # Add onto commit as parent if specified and this is the first commit
            if onto and not new_parents:
                if isinstance(onto, bytes):
                    onto_commit = parse_commit(r, onto)
                    new_parents.append(onto_commit.id)

            # Check if the subtree changed from parent
            if new_parents:
                parent_commit = r[commit.parents[0]]
                if isinstance(parent_commit, Commit):
                    parent_tree = r[parent_commit.tree]
                    if isinstance(parent_tree, Tree):
                        parent_subtree = extract_subtree(
                            r.object_store, parent_tree, prefix
                        )
                        if parent_subtree and parent_subtree.id == commit_subtree.id:
                            # Subtree unchanged, reuse parent's split
                            cache[commit_id] = new_parents[0]
                            return new_parents[0]

            # Create new commit with subtree as root
            new_commit_obj = commit.copy()
            if not isinstance(new_commit_obj, Commit):
                raise TypeError("Expected Commit copy")
            new_commit_obj.tree = commit_subtree.id
            new_commit_obj.parents = new_parents

            r.object_store.add_object(new_commit_obj)
            cache[commit_id] = new_commit_obj.id
            return new_commit_obj.id

        # Process the history
        cache: dict[ObjectID, ObjectID | None] = {}
        split_commit_id = process_commit(head_commit.id, cache)

        if split_commit_id is None:
            raise Error("Failed to split subtree")

        # Create branch if requested
        if branch:
            from dulwich.refs import Ref

            ref_name = Ref(LOCAL_BRANCH_PREFIX + branch)
            r.refs[ref_name] = split_commit_id

        # Rejoin if requested
        if rejoin:
            # Create a merge commit that joins the split back
            rejoin_message = (
                f"Split '{prefix.decode('utf-8', 'replace')}' into branch '{branch.decode('utf-8', 'replace') if branch else 'split'}'"
            ).encode()
            rejoin_message = add_subtree_metadata(
                rejoin_message, prefix, split=split_commit_id
            )

            rejoin_commit_obj = head_commit.copy()
            if not isinstance(rejoin_commit_obj, Commit):
                raise TypeError("Expected Commit copy")
            rejoin_commit_obj.tree = head_tree.id
            rejoin_commit_obj.parents = [head_commit.id, split_commit_id]
            rejoin_commit_obj.author_time = int(time.time())
            rejoin_commit_obj.commit_time = int(time.time())
            rejoin_commit_obj.message = rejoin_message

            r.object_store.add_object(rejoin_commit_obj)
            r[b"HEAD"] = rejoin_commit_obj.id

        return split_commit_id


def subtree_pull(
    repo: RepoPath,
    prefix: bytes | str,
    repository: str,
    ref: bytes | str,
    squash: bool = False,
    message: bytes | None = None,
) -> ObjectID:
    """Pull changes from a remote repository into a subtree.

    Args:
      repo: Repository to pull into
      prefix: Directory path of the subtree
      repository: URL of repository to pull from
      ref: Ref to fetch
      squash: If True, squash history into a single commit
      message: Custom commit message

    Returns:
      Object ID of the merge commit

    Raises:
      Error: If fetch or merge fails
    """
    from . import fetch, open_repo_closing

    if isinstance(prefix, str):
        prefix = prefix.encode("utf-8")
    if isinstance(ref, str):
        ref = ref.encode("utf-8")

    with open_repo_closing(repo) as r:
        # Fetch from remote
        from io import StringIO

        from dulwich.refs import Ref

        fetch_result = fetch(r, repository, outstream=StringIO())

        # Get the fetched commit
        if ref.startswith(b"refs/"):
            ref_key = Ref(ref)
        else:
            ref_key = Ref(LOCAL_BRANCH_PREFIX + ref)

        commit_id = fetch_result.refs.get(ref_key)
        if commit_id is None:
            # Try as a commit SHA - ref itself might be a commit SHA
            merge_commit = parse_commit(r, ref)
            return subtree_merge(
                repo, prefix, merge_commit, squash=squash, message=message
            )

        # Merge the fetched commit
        merge_commit_obj = r[commit_id]
        if not isinstance(merge_commit_obj, Commit):
            raise NotCommitError(commit_id)
        return subtree_merge(
            repo, prefix, merge_commit_obj, squash=squash, message=message
        )


def subtree_push(
    repo: RepoPath,
    prefix: bytes | str,
    repository: str,
    ref: bytes | str,
) -> None:
    """Push a subtree to a remote repository.

    Args:
      repo: Repository to push from
      prefix: Directory path of the subtree
      repository: URL of repository to push to
      ref: Ref to push to

    Raises:
      Error: If split or push fails
    """
    from . import open_repo_closing, push

    if isinstance(prefix, str):
        prefix = prefix.encode("utf-8")
    if isinstance(ref, str):
        ref = ref.encode("utf-8")

    with open_repo_closing(repo) as r:
        # Split the subtree
        split_commit_id = subtree_split(repo, prefix)

        # Push the split commit to the remote
        if not ref.startswith(b"refs/"):
            ref = LOCAL_BRANCH_PREFIX + ref

        # Create a refspec for pushing
        refspec = split_commit_id + b":" + ref
        push(r, repository, refspec)
