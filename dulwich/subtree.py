# subtree.py -- Git subtree implementation
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

"""Git subtree implementation."""

__all__ = [
    "add_subtree_metadata",
    "create_tree_with_subtree",
    "extract_subtree",
    "find_subtree_splits",
    "parse_subtree_metadata",
]

import stat
from collections import deque
from typing import TypedDict

from dulwich.object_store import BaseObjectStore
from dulwich.objects import Commit, ObjectID, Tree
from dulwich.trailers import parse_trailers


def _validate_prefix(prefix: bytes) -> None:
    """Validate that a prefix is a valid path.

    Args:
      prefix: Path prefix to validate

    Raises:
      ValueError: If prefix is invalid
    """
    if not prefix:
        return  # Empty prefix is valid (means root)

    # Check for invalid path components
    parts = prefix.split(b"/")
    for part in parts:
        if part in (b".", b"..", b""):
            raise ValueError(f"Invalid prefix path component: {part!r}")
        if part == b".git":
            raise ValueError("Prefix cannot contain '.git'")

    # Check for leading/trailing slashes
    if prefix.startswith(b"/"):
        raise ValueError("Prefix cannot start with '/'")
    if prefix.endswith(b"/"):
        raise ValueError("Prefix cannot end with '/'")


class SubtreeMetadata(TypedDict, total=False):
    """Metadata stored in commit trailers for subtree operations."""

    dir: bytes
    split: ObjectID
    mainline: ObjectID


def parse_subtree_metadata(commit: Commit) -> SubtreeMetadata:
    """Parse subtree metadata from commit message trailers.

    Args:
      commit: Commit to parse metadata from

    Returns:
      Dictionary with subtree metadata (dir, split, mainline)
    """
    metadata: SubtreeMetadata = {}

    # Parse commit message for trailers
    message = commit.message
    if not message:
        return metadata

    # Use dulwich's trailer parser
    _, trailers = parse_trailers(message)

    # Extract subtree-specific trailers
    for trailer in trailers:
        if trailer.key == "git-subtree-dir":
            metadata["dir"] = trailer.value.encode("utf-8")
        elif trailer.key == "git-subtree-split":
            metadata["split"] = ObjectID(trailer.value.encode("utf-8"))
        elif trailer.key == "git-subtree-mainline":
            metadata["mainline"] = ObjectID(trailer.value.encode("utf-8"))

    return metadata


def add_subtree_metadata(
    message: bytes,
    prefix: bytes,
    split: ObjectID | None = None,
    mainline: ObjectID | None = None,
) -> bytes:
    """Add subtree metadata trailers to a commit message.

    Args:
      message: Original commit message
      prefix: Subtree directory path
      split: Optional split commit SHA
      mainline: Optional mainline commit SHA

    Returns:
      Commit message with subtree metadata trailers appended
    """
    # Ensure message ends with exactly one newline
    message = message.rstrip(b"\n") + b"\n"

    # Add a blank line if message doesn't end with one
    if not message.endswith(b"\n\n"):
        message += b"\n"

    # Add subtree trailers
    message += b"git-subtree-dir: " + prefix + b"\n"
    if split:
        message += b"git-subtree-split: " + split + b"\n"
    if mainline:
        message += b"git-subtree-mainline: " + mainline + b"\n"

    return message


def extract_subtree(store: BaseObjectStore, tree: Tree, prefix: bytes) -> Tree | None:
    """Extract a subtree from a tree at the given prefix path.

    Args:
      store: Object store to read from
      tree: Tree object to extract from
      prefix: Path to subtree (e.g., b"vendor/lib")

    Returns:
      Tree object for the subtree, or None if path doesn't exist

    Raises:
      ValueError: If prefix is invalid
    """
    _validate_prefix(prefix)

    # Split prefix into components
    parts = [p for p in prefix.split(b"/") if p]

    if not parts:
        # Empty prefix means the whole tree
        return tree

    # Walk down the tree
    current_tree = tree
    for part in parts:
        if part not in current_tree:
            return None

        _mode, sha = current_tree[part]
        obj = store[sha]

        if not isinstance(obj, Tree):
            # Path exists but is not a tree
            return None

        current_tree = obj

    return current_tree


def create_tree_with_subtree(
    store: BaseObjectStore,
    base_tree: Tree | None,
    prefix: bytes,
    subtree: Tree,
) -> ObjectID:
    """Create a new tree with a subtree inserted at the given prefix.

    Args:
      store: Object store to write to
      base_tree: Base tree to insert into (None for empty tree)
      prefix: Path where subtree should be inserted (e.g., b"vendor/lib")
      subtree: Tree to insert

    Returns:
      Object ID of the new tree

    Raises:
      ValueError: If prefix is invalid
    """
    _validate_prefix(prefix)

    # Split prefix into components
    parts = [p for p in prefix.split(b"/") if p]

    if not parts:
        # Empty prefix means replace the whole tree
        return subtree.id

    def build_tree_recursive(
        existing_tree: Tree | None, path_parts: list[bytes], index: int
    ) -> ObjectID:
        """Recursively build tree with subtree inserted."""
        # Create new tree based on existing or empty
        new_tree = Tree()

        if existing_tree:
            # Copy all entries from existing tree
            for name, mode, sha in existing_tree.iteritems():
                new_tree[name] = (mode, sha)

        # Get the component we're working with
        component = path_parts[index]

        if index == len(path_parts) - 1:
            # Last component - insert the subtree here
            new_tree[component] = (stat.S_IFDIR, subtree.id)
        else:
            # Not the last component - recurse
            existing_subtree = None
            if existing_tree and component in existing_tree:
                mode, sha = existing_tree[component]
                obj = store[sha]
                if isinstance(obj, Tree):
                    existing_subtree = obj

            # Recursively build the subtree
            new_subtree_id = build_tree_recursive(
                existing_subtree, path_parts, index + 1
            )
            new_tree[component] = (stat.S_IFDIR, new_subtree_id)

        # Write the new tree to the object store
        store.add_object(new_tree)
        return new_tree.id

    return build_tree_recursive(base_tree, parts, 0)


def find_subtree_splits(
    store: BaseObjectStore, commit: Commit, prefix: bytes, limit: int | None = None
) -> list[tuple[ObjectID, ObjectID]]:
    """Find previous subtree split commits in history.

    Args:
      store: Object store to read from
      commit: Starting commit
      prefix: Subtree prefix to search for
      limit: Maximum number of splits to find (None for all)

    Returns:
      List of (commit_id, split_id) tuples for commits with subtree metadata
    """
    splits: list[tuple[ObjectID, ObjectID]] = []
    seen: set[ObjectID] = set()
    queue: deque[ObjectID] = deque([commit.id])

    while queue and (limit is None or len(splits) < limit):
        commit_id = queue.popleft()

        if commit_id in seen:
            continue
        seen.add(commit_id)

        try:
            current_commit = store[commit_id]
            if not isinstance(current_commit, Commit):
                continue
        except KeyError:
            continue

        # Check for subtree metadata
        metadata = parse_subtree_metadata(current_commit)
        if metadata.get("dir") == prefix and metadata.get("split"):
            splits.append((commit_id, metadata["split"]))

        # Add parents to queue
        queue.extend(current_commit.parents)

    return splits
