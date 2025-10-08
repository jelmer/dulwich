# diff_tree.py -- Utilities for diffing files and trees.
# Copyright (C) 2010 Google, Inc.
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

"""Utilities for diffing files and trees."""

import stat
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from io import BytesIO
from itertools import chain
from typing import TYPE_CHECKING, Any, Callable, NamedTuple, Optional, TypeVar

from .object_store import BaseObjectStore
from .objects import S_ISGITLINK, ObjectID, ShaFile, Tree, TreeEntry

# TreeChange type constants.
CHANGE_ADD = "add"
CHANGE_MODIFY = "modify"
CHANGE_DELETE = "delete"
CHANGE_RENAME = "rename"
CHANGE_COPY = "copy"
CHANGE_UNCHANGED = "unchanged"

RENAME_CHANGE_TYPES = (CHANGE_RENAME, CHANGE_COPY)

# _NULL_ENTRY removed - using None instead

_MAX_SCORE = 100
RENAME_THRESHOLD = 60
MAX_FILES = 200
REWRITE_THRESHOLD: Optional[int] = None


class TreeChange(NamedTuple):
    """Named tuple a single change between two trees."""

    type: str
    old: Optional[TreeEntry]
    new: Optional[TreeEntry]

    @classmethod
    def add(cls, new: TreeEntry) -> "TreeChange":
        """Create a TreeChange for an added entry.

        Args:
          new: New tree entry

        Returns:
          TreeChange instance
        """
        return cls(CHANGE_ADD, None, new)

    @classmethod
    def delete(cls, old: TreeEntry) -> "TreeChange":
        """Create a TreeChange for a deleted entry.

        Args:
          old: Old tree entry

        Returns:
          TreeChange instance
        """
        return cls(CHANGE_DELETE, old, None)


def _tree_entries(path: bytes, tree: Tree) -> list[TreeEntry]:
    result: list[TreeEntry] = []
    if not tree:
        return result
    for entry in tree.iteritems(name_order=True):
        result.append(entry.in_path(path))
    return result


def _merge_entries(
    path: bytes, tree1: Tree, tree2: Tree
) -> list[tuple[Optional[TreeEntry], Optional[TreeEntry]]]:
    """Merge the entries of two trees.

    Args:
      path: A path to prepend to all tree entry names.
      tree1: The first Tree object to iterate, or None.
      tree2: The second Tree object to iterate, or None.

    Returns:
      A list of pairs of TreeEntry objects for each pair of entries in
        the trees. If an entry exists in one tree but not the other, the other
        entry will be None. If both entries exist, they are guaranteed to match.
    """
    entries1 = _tree_entries(path, tree1)
    entries2 = _tree_entries(path, tree2)
    i1 = i2 = 0
    len1 = len(entries1)
    len2 = len(entries2)

    result: list[tuple[Optional[TreeEntry], Optional[TreeEntry]]] = []
    while i1 < len1 and i2 < len2:
        entry1 = entries1[i1]
        entry2 = entries2[i2]
        if entry1.path < entry2.path:
            result.append((entry1, None))
            i1 += 1
        elif entry1.path > entry2.path:
            result.append((None, entry2))
            i2 += 1
        else:
            result.append((entry1, entry2))
            i1 += 1
            i2 += 1
    for i in range(i1, len1):
        result.append((entries1[i], None))
    for i in range(i2, len2):
        result.append((None, entries2[i]))
    return result


def _is_tree(entry: Optional[TreeEntry]) -> bool:
    if entry is None or entry.mode is None:
        return False
    return stat.S_ISDIR(entry.mode)


def walk_trees(
    store: BaseObjectStore,
    tree1_id: Optional[ObjectID],
    tree2_id: Optional[ObjectID],
    prune_identical: bool = False,
    paths: Optional[Sequence[bytes]] = None,
) -> Iterator[tuple[Optional[TreeEntry], Optional[TreeEntry]]]:
    """Recursively walk all the entries of two trees.

    Iteration is depth-first pre-order, as in e.g. os.walk.

    Args:
      store: An ObjectStore for looking up objects.
      tree1_id: The SHA of the first Tree object to iterate, or None.
      tree2_id: The SHA of the second Tree object to iterate, or None.
      prune_identical: If True, identical subtrees will not be walked.
      paths: Optional list of paths to filter to (as bytes).

    Returns:
      Iterator over Pairs of TreeEntry objects for each pair of entries
        in the trees and their subtrees recursively. If an entry exists in one
        tree but not the other, the other entry will be None. If both entries
        exist, they are guaranteed to match.
    """
    # This could be fairly easily generalized to >2 trees if we find a use
    # case.
    entry1 = TreeEntry(b"", stat.S_IFDIR, tree1_id) if tree1_id else None
    entry2 = TreeEntry(b"", stat.S_IFDIR, tree2_id) if tree2_id else None
    todo: list[tuple[Optional[TreeEntry], Optional[TreeEntry]]] = [(entry1, entry2)]
    while todo:
        entry1, entry2 = todo.pop()
        is_tree1 = _is_tree(entry1)
        is_tree2 = _is_tree(entry2)
        if prune_identical and is_tree1 and is_tree2 and entry1 == entry2:
            continue

        tree1 = (is_tree1 and entry1 and store[entry1.sha]) or None
        tree2 = (is_tree2 and entry2 and store[entry2.sha]) or None
        path = (
            (entry1.path if entry1 else None)
            or (entry2.path if entry2 else None)
            or b""
        )

        # If we have path filters, check if we should process this tree
        if paths is not None and (is_tree1 or is_tree2) and path is not None:
            # Special case for root tree
            if path == b"":
                should_recurse = True
            else:
                # Check if any of our filter paths could be under this tree
                should_recurse = False
                for filter_path in paths:
                    if filter_path == path:
                        # Exact match - we want this directory itself
                        should_recurse = True
                        break
                    elif filter_path.startswith(path + b"/"):
                        # Filter path is under this directory
                        should_recurse = True
                        break
                    elif path.startswith(filter_path + b"/"):
                        # This directory is under a filter path
                        should_recurse = True
                        break
            if not should_recurse:
                # Skip this tree entirely
                continue

        # Ensure trees are Tree objects before merging
        if tree1 is not None and not isinstance(tree1, Tree):
            tree1 = None
        if tree2 is not None and not isinstance(tree2, Tree):
            tree2 = None

        if tree1 is not None or tree2 is not None:
            # Use empty trees for None values
            if tree1 is None:
                tree1 = Tree()
            if tree2 is None:
                tree2 = Tree()
            assert path is not None
            todo.extend(reversed(_merge_entries(path, tree1, tree2)))

        # Only yield entries that match our path filters
        if paths is None:
            yield entry1, entry2
        else:
            # Check if this entry matches any of our filters
            for filter_path in paths:
                if path == filter_path:
                    # Exact match
                    yield entry1, entry2
                    break
                elif path is not None and path.startswith(filter_path + b"/"):
                    # This entry is under a filter directory
                    yield entry1, entry2
                    break
                elif (
                    path is not None
                    and filter_path.startswith(path + b"/")
                    and (is_tree1 or is_tree2)
                ):
                    # This is a parent directory of a filter path
                    yield entry1, entry2
                    break


def _skip_tree(entry: Optional[TreeEntry], include_trees: bool) -> Optional[TreeEntry]:
    if entry is None or entry.mode is None:
        return None
    if not include_trees and stat.S_ISDIR(entry.mode):
        return None
    return entry


def tree_changes(
    store: BaseObjectStore,
    tree1_id: Optional[ObjectID],
    tree2_id: Optional[ObjectID],
    want_unchanged: bool = False,
    rename_detector: Optional["RenameDetector"] = None,
    include_trees: bool = False,
    change_type_same: bool = False,
    paths: Optional[Sequence[bytes]] = None,
) -> Iterator[TreeChange]:
    """Find the differences between the contents of two trees.

    Args:
      store: An ObjectStore for looking up objects.
      tree1_id: The SHA of the source tree.
      tree2_id: The SHA of the target tree.
      want_unchanged: If True, include TreeChanges for unmodified entries
        as well.
      include_trees: Whether to include trees
      rename_detector: RenameDetector object for detecting renames.
      change_type_same: Whether to report change types in the same
        entry or as delete+add.
      paths: Optional list of paths to filter to (as bytes).

    Returns:
      Iterator over TreeChange instances for each change between the
        source and target tree.
    """
    if rename_detector is not None and tree1_id is not None and tree2_id is not None:
        yield from rename_detector.changes_with_renames(
            tree1_id,
            tree2_id,
            want_unchanged=want_unchanged,
            include_trees=include_trees,
        )
        return

    entries = walk_trees(
        store, tree1_id, tree2_id, prune_identical=(not want_unchanged), paths=paths
    )
    for entry1, entry2 in entries:
        if entry1 == entry2 and not want_unchanged:
            continue

        # Treat entries for trees as missing.
        entry1 = _skip_tree(entry1, include_trees)
        entry2 = _skip_tree(entry2, include_trees)

        if entry1 is not None and entry2 is not None:
            if (
                entry1.mode is not None
                and entry2.mode is not None
                and stat.S_IFMT(entry1.mode) != stat.S_IFMT(entry2.mode)
                and not change_type_same
            ):
                # File type changed: report as delete/add.
                yield TreeChange.delete(entry1)
                entry1 = None
                change_type = CHANGE_ADD
            elif entry1 == entry2:
                change_type = CHANGE_UNCHANGED
            else:
                change_type = CHANGE_MODIFY
        elif entry1 is not None:
            change_type = CHANGE_DELETE
        elif entry2 is not None:
            change_type = CHANGE_ADD
        else:
            # Both were None because at least one was a tree.
            continue
        yield TreeChange(change_type, entry1, entry2)


T = TypeVar("T")
U = TypeVar("U")


def _all_eq(seq: Sequence[T], key: Callable[[T], U], value: U) -> bool:
    for e in seq:
        if key(e) != value:
            return False
    return True


def _all_same(seq: Sequence[Any], key: Callable[[Any], Any]) -> bool:
    return _all_eq(seq[1:], key, key(seq[0]))


def tree_changes_for_merge(
    store: BaseObjectStore,
    parent_tree_ids: Sequence[ObjectID],
    tree_id: ObjectID,
    rename_detector: Optional["RenameDetector"] = None,
) -> Iterator[list[Optional[TreeChange]]]:
    """Get the tree changes for a merge tree relative to all its parents.

    Args:
      store: An ObjectStore for looking up objects.
      parent_tree_ids: An iterable of the SHAs of the parent trees.
      tree_id: The SHA of the merge tree.
      rename_detector: RenameDetector object for detecting renames.

    Returns:
      Iterator over lists of TreeChange objects, one per conflicted path
      in the merge.

      Each list contains one element per parent, with the TreeChange for that
      path relative to that parent. An element may be None if it never
      existed in one parent and was deleted in two others.

      A path is only included in the output if it is a conflict, i.e. its SHA
      in the merge tree is not found in any of the parents, or in the case of
      deletes, if not all of the old SHAs match.
    """
    all_parent_changes = [
        tree_changes(store, t, tree_id, rename_detector=rename_detector)
        for t in parent_tree_ids
    ]
    num_parents = len(parent_tree_ids)
    changes_by_path: dict[bytes, list[Optional[TreeChange]]] = defaultdict(
        lambda: [None] * num_parents
    )

    # Organize by path.
    for i, parent_changes in enumerate(all_parent_changes):
        for change in parent_changes:
            if change.type == CHANGE_DELETE:
                assert change.old is not None
                path = change.old.path
            else:
                assert change.new is not None
                path = change.new.path
            assert path is not None
            changes_by_path[path][i] = change

    def old_sha(c: TreeChange) -> Optional[ObjectID]:
        return c.old.sha if c.old is not None else None

    def change_type(c: TreeChange) -> str:
        return c.type

    # Yield only conflicting changes.
    for _, changes in sorted(changes_by_path.items()):
        assert len(changes) == num_parents
        have = [c for c in changes if c is not None]
        if _all_eq(have, change_type, CHANGE_DELETE):
            if not _all_same(have, old_sha):
                yield changes
        elif not _all_same(have, change_type):
            yield changes
        elif None not in changes:
            # If no change was found relative to one parent, that means the SHA
            # must have matched the SHA in that parent, so it is not a
            # conflict.
            yield changes


_BLOCK_SIZE = 64


def _count_blocks(obj: ShaFile) -> dict[int, int]:
    """Count the blocks in an object.

    Splits the data into blocks either on lines or <=64-byte chunks of lines.

    Args:
      obj: The object to count blocks for.

    Returns:
      A dict of block hashcode -> total bytes occurring.
    """
    block_counts: dict[int, int] = defaultdict(int)
    block = BytesIO()
    n = 0

    # Cache attrs as locals to avoid expensive lookups in the inner loop.
    block_write = block.write
    block_seek = block.seek
    block_truncate = block.truncate
    block_getvalue = block.getvalue

    for c in chain.from_iterable(obj.as_raw_chunks()):
        cb = c.to_bytes(1, "big")
        block_write(cb)
        n += 1
        if cb == b"\n" or n == _BLOCK_SIZE:
            value = block_getvalue()
            block_counts[hash(value)] += len(value)
            block_seek(0)
            block_truncate()
            n = 0
    if n > 0:
        last_block = block_getvalue()
        block_counts[hash(last_block)] += len(last_block)
    return block_counts


def _common_bytes(blocks1: Mapping[int, int], blocks2: Mapping[int, int]) -> int:
    """Count the number of common bytes in two block count dicts.

    Args:
      blocks1: The first dict of block hashcode -> total bytes.
      blocks2: The second dict of block hashcode -> total bytes.

    Returns:
      The number of bytes in common between blocks1 and blocks2. This is
      only approximate due to possible hash collisions.
    """
    # Iterate over the smaller of the two dicts, since this is symmetrical.
    if len(blocks1) > len(blocks2):
        blocks1, blocks2 = blocks2, blocks1
    score = 0
    for block, count1 in blocks1.items():
        count2 = blocks2.get(block)
        if count2:
            score += min(count1, count2)
    return score


def _similarity_score(
    obj1: ShaFile,
    obj2: ShaFile,
    block_cache: Optional[dict[ObjectID, dict[int, int]]] = None,
) -> int:
    """Compute a similarity score for two objects.

    Args:
      obj1: The first object to score.
      obj2: The second object to score.
      block_cache: An optional dict of SHA to block counts to cache
        results between calls.

    Returns:
      The similarity score between the two objects, defined as the
        number of bytes in common between the two objects divided by the
        maximum size, scaled to the range 0-100.
    """
    if block_cache is None:
        block_cache = {}
    if obj1.id not in block_cache:
        block_cache[obj1.id] = _count_blocks(obj1)
    if obj2.id not in block_cache:
        block_cache[obj2.id] = _count_blocks(obj2)

    common_bytes = _common_bytes(block_cache[obj1.id], block_cache[obj2.id])
    max_size = max(obj1.raw_length(), obj2.raw_length())
    if not max_size:
        return _MAX_SCORE
    return int(float(common_bytes) * _MAX_SCORE / max_size)


def _tree_change_key(entry: TreeChange) -> tuple[bytes, bytes]:
    # Sort by old path then new path. If only one exists, use it for both keys.
    path1 = entry.old.path if entry.old is not None else None
    path2 = entry.new.path if entry.new is not None else None
    if path1 is None:
        path1 = path2
    if path2 is None:
        path2 = path1
    assert path1 is not None
    assert path2 is not None
    return (path1, path2)


class RenameDetector:
    """Object for handling rename detection between two trees."""

    _adds: list[TreeChange]
    _deletes: list[TreeChange]
    _changes: list[TreeChange]
    _candidates: list[tuple[int, TreeChange]]

    def __init__(
        self,
        store: BaseObjectStore,
        rename_threshold: int = RENAME_THRESHOLD,
        max_files: Optional[int] = MAX_FILES,
        rewrite_threshold: Optional[int] = REWRITE_THRESHOLD,
        find_copies_harder: bool = False,
    ) -> None:
        """Initialize the rename detector.

        Args:
          store: An ObjectStore for looking up objects.
          rename_threshold: The threshold similarity score for considering
            an add/delete pair to be a rename/copy; see _similarity_score.
          max_files: The maximum number of adds and deletes to consider,
            or None for no limit. The detector is guaranteed to compare no more
            than max_files ** 2 add/delete pairs. This limit is provided
            because rename detection can be quadratic in the project size. If
            the limit is exceeded, no content rename detection is attempted.
          rewrite_threshold: The threshold similarity score below which a
            modify should be considered a delete/add, or None to not break
            modifies; see _similarity_score.
          find_copies_harder: If True, consider unmodified files when
            detecting copies.
        """
        self._store = store
        self._rename_threshold = rename_threshold
        self._rewrite_threshold = rewrite_threshold
        self._max_files = max_files
        self._find_copies_harder = find_copies_harder
        self._want_unchanged = False

    def _reset(self) -> None:
        self._adds = []
        self._deletes = []
        self._changes = []

    def _should_split(self, change: TreeChange) -> bool:
        if self._rewrite_threshold is None or change.type != CHANGE_MODIFY:
            return False
        assert change.old is not None and change.new is not None
        if change.old.sha == change.new.sha:
            return False
        assert change.old.sha is not None
        assert change.new.sha is not None
        old_obj = self._store[change.old.sha]
        new_obj = self._store[change.new.sha]
        return _similarity_score(old_obj, new_obj) < self._rewrite_threshold

    def _add_change(self, change: TreeChange) -> None:
        if change.type == CHANGE_ADD:
            self._adds.append(change)
        elif change.type == CHANGE_DELETE:
            self._deletes.append(change)
        elif self._should_split(change):
            assert change.old is not None and change.new is not None
            self._deletes.append(TreeChange.delete(change.old))
            self._adds.append(TreeChange.add(change.new))
        elif (
            self._find_copies_harder and change.type == CHANGE_UNCHANGED
        ) or change.type == CHANGE_MODIFY:
            # Treat all modifies as potential deletes for rename detection,
            # but don't split them (to avoid spurious renames). Setting
            # find_copies_harder means we treat unchanged the same as
            # modified.
            self._deletes.append(change)
        else:
            self._changes.append(change)

    def _collect_changes(
        self, tree1_id: Optional[ObjectID], tree2_id: Optional[ObjectID]
    ) -> None:
        want_unchanged = self._find_copies_harder or self._want_unchanged
        for change in tree_changes(
            self._store,
            tree1_id,
            tree2_id,
            want_unchanged=want_unchanged,
            include_trees=self._include_trees,
        ):
            self._add_change(change)

    def _prune(
        self, add_paths: AbstractSet[bytes], delete_paths: AbstractSet[bytes]
    ) -> None:
        def check_add(a: TreeChange) -> bool:
            assert a.new is not None
            return a.new.path not in add_paths

        def check_delete(d: TreeChange) -> bool:
            assert d.old is not None
            return d.old.path not in delete_paths

        self._adds = [a for a in self._adds if check_add(a)]
        self._deletes = [d for d in self._deletes if check_delete(d)]

    def _find_exact_renames(self) -> None:
        add_map = defaultdict(list)
        for add in self._adds:
            assert add.new is not None
            add_map[add.new.sha].append(add.new)
        delete_map = defaultdict(list)
        for delete in self._deletes:
            # Keep track of whether the delete was actually marked as a delete.
            # If not, it needs to be marked as a copy.
            is_delete = delete.type == CHANGE_DELETE
            assert delete.old is not None
            delete_map[delete.old.sha].append((delete.old, is_delete))

        add_paths = set()
        delete_paths = set()
        for sha, sha_deletes in delete_map.items():
            sha_adds = add_map[sha]
            for (old, is_delete), new in zip(sha_deletes, sha_adds):
                assert old.mode is not None
                assert new.mode is not None
                if stat.S_IFMT(old.mode) != stat.S_IFMT(new.mode):
                    continue
                if is_delete:
                    assert old.path is not None
                    delete_paths.add(old.path)
                assert new.path is not None
                add_paths.add(new.path)
                new_type = (is_delete and CHANGE_RENAME) or CHANGE_COPY
                self._changes.append(TreeChange(new_type, old, new))

            num_extra_adds = len(sha_adds) - len(sha_deletes)
            # TODO(dborowitz): Less arbitrary way of dealing with extra copies.
            old = sha_deletes[0][0]
            if num_extra_adds > 0:
                for new in sha_adds[-num_extra_adds:]:
                    assert new.path is not None
                    add_paths.add(new.path)
                    self._changes.append(TreeChange(CHANGE_COPY, old, new))
        self._prune(add_paths, delete_paths)

    def _should_find_content_renames(self) -> bool:
        if self._max_files is None:
            return True
        return len(self._adds) * len(self._deletes) <= self._max_files**2

    def _rename_type(
        self, check_paths: bool, delete: TreeChange, add: TreeChange
    ) -> str:
        assert delete.old is not None and add.new is not None
        if check_paths and delete.old.path == add.new.path:
            # If the paths match, this must be a split modify, so make sure it
            # comes out as a modify.
            return CHANGE_MODIFY
        elif delete.type != CHANGE_DELETE:
            # If it's in deletes but not marked as a delete, it must have been
            # added due to find_copies_harder, and needs to be marked as a
            # copy.
            return CHANGE_COPY
        return CHANGE_RENAME

    def _find_content_rename_candidates(self) -> None:
        candidates = self._candidates = []
        # TODO: Optimizations:
        #  - Compare object sizes before counting blocks.
        #  - Skip if delete's S_IFMT differs from all adds.
        #  - Skip if adds or deletes is empty.
        # Match C git's behavior of not attempting to find content renames if
        # the matrix size exceeds the threshold.
        if not self._should_find_content_renames():
            return

        block_cache = {}
        check_paths = self._rename_threshold is not None
        for delete in self._deletes:
            assert delete.old is not None
            assert delete.old.mode is not None
            if S_ISGITLINK(delete.old.mode):
                continue  # Git links don't exist in this repo.
            assert delete.old.sha is not None
            old_sha = delete.old.sha
            old_obj = self._store[old_sha]
            block_cache[old_sha] = _count_blocks(old_obj)
            for add in self._adds:
                assert add.new is not None
                assert add.new.mode is not None
                if stat.S_IFMT(delete.old.mode) != stat.S_IFMT(add.new.mode):
                    continue
                assert add.new.sha is not None
                new_obj = self._store[add.new.sha]
                score = _similarity_score(old_obj, new_obj, block_cache=block_cache)
                if score > self._rename_threshold:
                    new_type = self._rename_type(check_paths, delete, add)
                    rename = TreeChange(new_type, delete.old, add.new)
                    candidates.append((-score, rename))

    def _choose_content_renames(self) -> None:
        # Sort scores from highest to lowest, but keep names in ascending
        # order.
        self._candidates.sort()

        delete_paths = set()
        add_paths = set()
        for _, change in self._candidates:
            assert change.old is not None and change.new is not None
            new_path = change.new.path
            assert new_path is not None
            if new_path in add_paths:
                continue
            old_path = change.old.path
            assert old_path is not None
            orig_type = change.type
            if old_path in delete_paths:
                change = TreeChange(CHANGE_COPY, change.old, change.new)

            # If the candidate was originally a copy, that means it came from a
            # modified or unchanged path, so we don't want to prune it.
            if orig_type != CHANGE_COPY:
                delete_paths.add(old_path)
            add_paths.add(new_path)
            self._changes.append(change)
        self._prune(add_paths, delete_paths)

    def _join_modifies(self) -> None:
        if self._rewrite_threshold is None:
            return

        modifies = {}
        delete_map = {}
        for d in self._deletes:
            assert d.old is not None
            delete_map[d.old.path] = d
        for add in self._adds:
            assert add.new is not None
            path = add.new.path
            delete = delete_map.get(path)
            if (
                delete is not None
                and delete.old is not None
                and delete.old.mode is not None
                and add.new.mode is not None
                and stat.S_IFMT(delete.old.mode) == stat.S_IFMT(add.new.mode)
            ):
                modifies[path] = TreeChange(CHANGE_MODIFY, delete.old, add.new)

        def check_add_mod(a: TreeChange) -> bool:
            assert a.new is not None
            return a.new.path not in modifies

        def check_delete_mod(d: TreeChange) -> bool:
            assert d.old is not None
            return d.old.path not in modifies

        self._adds = [a for a in self._adds if check_add_mod(a)]
        self._deletes = [d for d in self._deletes if check_delete_mod(d)]
        self._changes += modifies.values()

    def _sorted_changes(self) -> list[TreeChange]:
        result = []
        result.extend(self._adds)
        result.extend(self._deletes)
        result.extend(self._changes)
        result.sort(key=_tree_change_key)
        return result

    def _prune_unchanged(self) -> None:
        if self._want_unchanged:
            return
        self._deletes = [d for d in self._deletes if d.type != CHANGE_UNCHANGED]

    def changes_with_renames(
        self,
        tree1_id: Optional[ObjectID],
        tree2_id: Optional[ObjectID],
        want_unchanged: bool = False,
        include_trees: bool = False,
    ) -> list[TreeChange]:
        """Iterate TreeChanges between two tree SHAs, with rename detection."""
        self._reset()
        self._want_unchanged = want_unchanged
        self._include_trees = include_trees
        self._collect_changes(tree1_id, tree2_id)
        self._find_exact_renames()
        self._find_content_rename_candidates()
        self._choose_content_renames()
        self._join_modifies()
        self._prune_unchanged()
        return self._sorted_changes()


# Hold on to the pure-python implementations for testing.
_is_tree_py = _is_tree
_merge_entries_py = _merge_entries
_count_blocks_py = _count_blocks

if TYPE_CHECKING:
    # For type checking, use the Python implementations
    pass
else:
    # At runtime, try to import Rust extensions
    try:
        # Try to import Rust versions
        from dulwich._diff_tree import (
            _count_blocks as _rust_count_blocks,
        )
        from dulwich._diff_tree import (
            _is_tree as _rust_is_tree,
        )
        from dulwich._diff_tree import (
            _merge_entries as _rust_merge_entries,
        )

        # Override with Rust versions
        _count_blocks = _rust_count_blocks
        _is_tree = _rust_is_tree
        _merge_entries = _rust_merge_entries
    except ImportError:
        pass
