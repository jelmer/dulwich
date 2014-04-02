# diff_tree.py -- Utilities for diffing files and trees.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

"""Utilities for diffing files and trees."""

from collections import (
    defaultdict,
    namedtuple,
    )

from io import BytesIO
import itertools
import stat

from dulwich.objects import (
    S_ISGITLINK,
    TreeEntry,
    )

# TreeChange type constants.
CHANGE_ADD = 'add'
CHANGE_MODIFY = 'modify'
CHANGE_DELETE = 'delete'
CHANGE_RENAME = 'rename'
CHANGE_COPY = 'copy'
CHANGE_UNCHANGED = 'unchanged'

RENAME_CHANGE_TYPES = (CHANGE_RENAME, CHANGE_COPY)

_NULL_ENTRY = TreeEntry(None, None, None)

_MAX_SCORE = 100
RENAME_THRESHOLD = 60
MAX_FILES = 200
REWRITE_THRESHOLD = None


class TreeChange(namedtuple('TreeChange', ['type', 'old', 'new'])):
    """Named tuple a single change between two trees."""

    @classmethod
    def add(cls, new):
        return cls(CHANGE_ADD, _NULL_ENTRY, new)

    @classmethod
    def delete(cls, old):
        return cls(CHANGE_DELETE, old, _NULL_ENTRY)


def _tree_entries(path, tree):
    result = []
    if not tree:
        return result
    for entry in tree.iteritems(name_order=True):
        result.append(entry.in_path(path))
    return result


def _merge_entries(path, tree1, tree2):
    """Merge the entries of two trees.

    :param path: A path to prepend to all tree entry names.
    :param tree1: The first Tree object to iterate, or None.
    :param tree2: The second Tree object to iterate, or None.
    :return: A list of pairs of TreeEntry objects for each pair of entries in
        the trees. If an entry exists in one tree but not the other, the other
        entry will have all attributes set to None. If neither entry's path is
        None, they are guaranteed to match.
    """
    entries1 = _tree_entries(path, tree1)
    entries2 = _tree_entries(path, tree2)
    i1 = i2 = 0
    len1 = len(entries1)
    len2 = len(entries2)

    result = []
    while i1 < len1 and i2 < len2:
        entry1 = entries1[i1]
        entry2 = entries2[i2]
        if entry1.path < entry2.path:
            result.append((entry1, _NULL_ENTRY))
            i1 += 1
        elif entry1.path > entry2.path:
            result.append((_NULL_ENTRY, entry2))
            i2 += 1
        else:
            result.append((entry1, entry2))
            i1 += 1
            i2 += 1
    for i in xrange(i1, len1):
        result.append((entries1[i], _NULL_ENTRY))
    for i in xrange(i2, len2):
        result.append((_NULL_ENTRY, entries2[i]))
    return result


def _is_tree(entry):
    mode = entry.mode
    if mode is None:
        return False
    return stat.S_ISDIR(mode)


def walk_trees(store, tree1_id, tree2_id, prune_identical=False):
    """Recursively walk all the entries of two trees.

    Iteration is depth-first pre-order, as in e.g. os.walk.

    :param store: An ObjectStore for looking up objects.
    :param tree1_id: The SHA of the first Tree object to iterate, or None.
    :param tree2_id: The SHA of the second Tree object to iterate, or None.
    :param prune_identical: If True, identical subtrees will not be walked.
    :return: Iterator over Pairs of TreeEntry objects for each pair of entries
        in the trees and their subtrees recursively. If an entry exists in one
        tree but not the other, the other entry will have all attributes set
        to None. If neither entry's path is None, they are guaranteed to
        match.
    """
    # This could be fairly easily generalized to >2 trees if we find a use case.
    mode1 = tree1_id and stat.S_IFDIR or None
    mode2 = tree2_id and stat.S_IFDIR or None
    todo = [(TreeEntry('', mode1, tree1_id), TreeEntry('', mode2, tree2_id))]
    while todo:
        entry1, entry2 = todo.pop()
        is_tree1 = _is_tree(entry1)
        is_tree2 = _is_tree(entry2)
        if prune_identical and is_tree1 and is_tree2 and entry1 == entry2:
            continue

        tree1 = is_tree1 and store[entry1.sha] or None
        tree2 = is_tree2 and store[entry2.sha] or None
        path = entry1.path or entry2.path
        todo.extend(reversed(_merge_entries(path, tree1, tree2)))
        yield entry1, entry2


def _skip_tree(entry):
    if entry.mode is None or stat.S_ISDIR(entry.mode):
        return _NULL_ENTRY
    return entry


def tree_changes(store, tree1_id, tree2_id, want_unchanged=False,
                 rename_detector=None):
    """Find the differences between the contents of two trees.

    :param store: An ObjectStore for looking up objects.
    :param tree1_id: The SHA of the source tree.
    :param tree2_id: The SHA of the target tree.
    :param want_unchanged: If True, include TreeChanges for unmodified entries
        as well.
    :param rename_detector: RenameDetector object for detecting renames.
    :return: Iterator over TreeChange instances for each change between the
        source and target tree.
    """
    if (rename_detector is not None and tree1_id is not None and
        tree2_id is not None):
        for change in rename_detector.changes_with_renames(
          tree1_id, tree2_id, want_unchanged=want_unchanged):
            yield change
        return

    entries = walk_trees(store, tree1_id, tree2_id,
                         prune_identical=(not want_unchanged))
    for entry1, entry2 in entries:
        if entry1 == entry2 and not want_unchanged:
            continue

        # Treat entries for trees as missing.
        entry1 = _skip_tree(entry1)
        entry2 = _skip_tree(entry2)

        if entry1 != _NULL_ENTRY and entry2 != _NULL_ENTRY:
            if stat.S_IFMT(entry1.mode) != stat.S_IFMT(entry2.mode):
                # File type changed: report as delete/add.
                yield TreeChange.delete(entry1)
                entry1 = _NULL_ENTRY
                change_type = CHANGE_ADD
            elif entry1 == entry2:
                change_type = CHANGE_UNCHANGED
            else:
                change_type = CHANGE_MODIFY
        elif entry1 != _NULL_ENTRY:
            change_type = CHANGE_DELETE
        elif entry2 != _NULL_ENTRY:
            change_type = CHANGE_ADD
        else:
            # Both were None because at least one was a tree.
            continue
        yield TreeChange(change_type, entry1, entry2)


def _all_eq(seq, key, value):
    for e in seq:
        if key(e) != value:
            return False
    return True


def _all_same(seq, key):
    return _all_eq(seq[1:], key, key(seq[0]))


def tree_changes_for_merge(store, parent_tree_ids, tree_id,
                           rename_detector=None):
    """Get the tree changes for a merge tree relative to all its parents.

    :param store: An ObjectStore for looking up objects.
    :param parent_tree_ids: An iterable of the SHAs of the parent trees.
    :param tree_id: The SHA of the merge tree.
    :param rename_detector: RenameDetector object for detecting renames.

    :return: Iterator over lists of TreeChange objects, one per conflicted path
        in the merge.

        Each list contains one element per parent, with the TreeChange for that
        path relative to that parent. An element may be None if it never existed
        in one parent and was deleted in two others.

        A path is only included in the output if it is a conflict, i.e. its SHA
        in the merge tree is not found in any of the parents, or in the case of
        deletes, if not all of the old SHAs match.
    """
    all_parent_changes = [tree_changes(store, t, tree_id,
                                       rename_detector=rename_detector)
                          for t in parent_tree_ids]
    num_parents = len(parent_tree_ids)
    changes_by_path = defaultdict(lambda: [None] * num_parents)

    # Organize by path.
    for i, parent_changes in enumerate(all_parent_changes):
        for change in parent_changes:
            if change.type == CHANGE_DELETE:
                path = change.old.path
            else:
                path = change.new.path
            changes_by_path[path][i] = change

    old_sha = lambda c: c.old.sha
    change_type = lambda c: c.type

    # Yield only conflicting changes.
    for _, changes in sorted(changes_by_path.iteritems()):
        assert len(changes) == num_parents
        have = [c for c in changes if c is not None]
        if _all_eq(have, change_type, CHANGE_DELETE):
            if not _all_same(have, old_sha):
                yield changes
        elif not _all_same(have, change_type):
            yield changes
        elif None not in changes:
            # If no change was found relative to one parent, that means the SHA
            # must have matched the SHA in that parent, so it is not a conflict.
            yield changes


_BLOCK_SIZE = 64


def _count_blocks(obj):
    """Count the blocks in an object.

    Splits the data into blocks either on lines or <=64-byte chunks of lines.

    :param obj: The object to count blocks for.
    :return: A dict of block hashcode -> total bytes occurring.
    """
    block_counts = defaultdict(int)
    block = BytesIO()
    n = 0

    # Cache attrs as locals to avoid expensive lookups in the inner loop.
    block_write = block.write
    block_seek = block.seek
    block_truncate = block.truncate
    block_getvalue = block.getvalue

    for c in itertools.chain(*obj.as_raw_chunks()):
        block_write(c)
        n += 1
        if c == '\n' or n == _BLOCK_SIZE:
            value = block_getvalue()
            block_counts[hash(value)] += len(value)
            block_seek(0)
            block_truncate()
            n = 0
    if n > 0:
        last_block = block_getvalue()
        block_counts[hash(last_block)] += len(last_block)
    return block_counts


def _common_bytes(blocks1, blocks2):
    """Count the number of common bytes in two block count dicts.

    :param block1: The first dict of block hashcode -> total bytes.
    :param block2: The second dict of block hashcode -> total bytes.
    :return: The number of bytes in common between blocks1 and blocks2. This is
        only approximate due to possible hash collisions.
    """
    # Iterate over the smaller of the two dicts, since this is symmetrical.
    if len(blocks1) > len(blocks2):
        blocks1, blocks2 = blocks2, blocks1
    score = 0
    for block, count1 in blocks1.iteritems():
        count2 = blocks2.get(block)
        if count2:
            score += min(count1, count2)
    return score


def _similarity_score(obj1, obj2, block_cache=None):
    """Compute a similarity score for two objects.

    :param obj1: The first object to score.
    :param obj2: The second object to score.
    :param block_cache: An optional dict of SHA to block counts to cache results
        between calls.
    :return: The similarity score between the two objects, defined as the number
        of bytes in common between the two objects divided by the maximum size,
        scaled to the range 0-100.
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


def _tree_change_key(entry):
    # Sort by old path then new path. If only one exists, use it for both keys.
    path1 = entry.old.path
    path2 = entry.new.path
    if path1 is None:
        path1 = path2
    if path2 is None:
        path2 = path1
    return (path1, path2)


class RenameDetector(object):
    """Object for handling rename detection between two trees."""

    def __init__(self, store, rename_threshold=RENAME_THRESHOLD,
                 max_files=MAX_FILES,
                 rewrite_threshold=REWRITE_THRESHOLD,
                 find_copies_harder=False):
        """Initialize the rename detector.

        :param store: An ObjectStore for looking up objects.
        :param rename_threshold: The threshold similarity score for considering
            an add/delete pair to be a rename/copy; see _similarity_score.
        :param max_files: The maximum number of adds and deletes to consider, or
            None for no limit. The detector is guaranteed to compare no more
            than max_files ** 2 add/delete pairs. This limit is provided because
            rename detection can be quadratic in the project size. If the limit
            is exceeded, no content rename detection is attempted.
        :param rewrite_threshold: The threshold similarity score below which a
            modify should be considered a delete/add, or None to not break
            modifies; see _similarity_score.
        :param find_copies_harder: If True, consider unmodified files when
            detecting copies.
        """
        self._store = store
        self._rename_threshold = rename_threshold
        self._rewrite_threshold = rewrite_threshold
        self._max_files = max_files
        self._find_copies_harder = find_copies_harder
        self._want_unchanged = False

    def _reset(self):
        self._adds = []
        self._deletes = []
        self._changes = []

    def _should_split(self, change):
        if (self._rewrite_threshold is None or change.type != CHANGE_MODIFY or
            change.old.sha == change.new.sha):
            return False
        old_obj = self._store[change.old.sha]
        new_obj = self._store[change.new.sha]
        return _similarity_score(old_obj, new_obj) < self._rewrite_threshold

    def _add_change(self, change):
        if change.type == CHANGE_ADD:
            self._adds.append(change)
        elif change.type == CHANGE_DELETE:
            self._deletes.append(change)
        elif self._should_split(change):
            self._deletes.append(TreeChange.delete(change.old))
            self._adds.append(TreeChange.add(change.new))
        elif ((self._find_copies_harder and change.type == CHANGE_UNCHANGED)
              or change.type == CHANGE_MODIFY):
            # Treat all modifies as potential deletes for rename detection,
            # but don't split them (to avoid spurious renames). Setting
            # find_copies_harder means we treat unchanged the same as
            # modified.
            self._deletes.append(change)
        else:
            self._changes.append(change)

    def _collect_changes(self, tree1_id, tree2_id):
        want_unchanged = self._find_copies_harder or self._want_unchanged
        for change in tree_changes(self._store, tree1_id, tree2_id,
                                   want_unchanged=want_unchanged):
            self._add_change(change)

    def _prune(self, add_paths, delete_paths):
        self._adds = [a for a in self._adds if a.new.path not in add_paths]
        self._deletes = [d for d in self._deletes
                         if d.old.path not in delete_paths]

    def _find_exact_renames(self):
        add_map = defaultdict(list)
        for add in self._adds:
            add_map[add.new.sha].append(add.new)
        delete_map = defaultdict(list)
        for delete in self._deletes:
            # Keep track of whether the delete was actually marked as a delete.
            # If not, it needs to be marked as a copy.
            is_delete = delete.type == CHANGE_DELETE
            delete_map[delete.old.sha].append((delete.old, is_delete))

        add_paths = set()
        delete_paths = set()
        for sha, sha_deletes in delete_map.iteritems():
            sha_adds = add_map[sha]
            for (old, is_delete), new in itertools.izip(sha_deletes, sha_adds):
                if stat.S_IFMT(old.mode) != stat.S_IFMT(new.mode):
                    continue
                if is_delete:
                    delete_paths.add(old.path)
                add_paths.add(new.path)
                new_type = is_delete and CHANGE_RENAME or CHANGE_COPY
                self._changes.append(TreeChange(new_type, old, new))

            num_extra_adds = len(sha_adds) - len(sha_deletes)
            # TODO(dborowitz): Less arbitrary way of dealing with extra copies.
            old = sha_deletes[0][0]
            if num_extra_adds:
                for new in sha_adds[-num_extra_adds:]:
                    add_paths.add(new.path)
                    self._changes.append(TreeChange(CHANGE_COPY, old, new))
        self._prune(add_paths, delete_paths)

    def _should_find_content_renames(self):
        return len(self._adds) * len(self._deletes) <= self._max_files ** 2

    def _rename_type(self, check_paths, delete, add):
        if check_paths and delete.old.path == add.new.path:
            # If the paths match, this must be a split modify, so make sure it
            # comes out as a modify.
            return CHANGE_MODIFY
        elif delete.type != CHANGE_DELETE:
            # If it's in deletes but not marked as a delete, it must have been
            # added due to find_copies_harder, and needs to be marked as a copy.
            return CHANGE_COPY
        return CHANGE_RENAME

    def _find_content_rename_candidates(self):
        candidates = self._candidates = []
        # TODO: Optimizations:
        #  - Compare object sizes before counting blocks.
        #  - Skip if delete's S_IFMT differs from all adds.
        #  - Skip if adds or deletes is empty.
        # Match C git's behavior of not attempting to find content renames if
        # the matrix size exceeds the threshold.
        if not self._should_find_content_renames():
            return

        check_paths = self._rename_threshold is not None
        for delete in self._deletes:
            if S_ISGITLINK(delete.old.mode):
                continue  # Git links don't exist in this repo.
            old_sha = delete.old.sha
            old_obj = self._store[old_sha]
            old_blocks = _count_blocks(old_obj)
            for add in self._adds:
                if stat.S_IFMT(delete.old.mode) != stat.S_IFMT(add.new.mode):
                    continue
                new_obj = self._store[add.new.sha]
                score = _similarity_score(old_obj, new_obj,
                                          block_cache={old_sha: old_blocks})
                if score > self._rename_threshold:
                    new_type = self._rename_type(check_paths, delete, add)
                    rename = TreeChange(new_type, delete.old, add.new)
                    candidates.append((-score, rename))

    def _choose_content_renames(self):
        # Sort scores from highest to lowest, but keep names in ascending order.
        self._candidates.sort()

        delete_paths = set()
        add_paths = set()
        for _, change in self._candidates:
            new_path = change.new.path
            if new_path in add_paths:
                continue
            old_path = change.old.path
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

    def _join_modifies(self):
        if self._rewrite_threshold is None:
            return

        modifies = {}
        delete_map = dict((d.old.path, d) for d in self._deletes)
        for add in self._adds:
            path = add.new.path
            delete = delete_map.get(path)
            if (delete is not None and
              stat.S_IFMT(delete.old.mode) == stat.S_IFMT(add.new.mode)):
                modifies[path] = TreeChange(CHANGE_MODIFY, delete.old, add.new)

        self._adds = [a for a in self._adds if a.new.path not in modifies]
        self._deletes = [a for a in self._deletes if a.new.path not in modifies]
        self._changes += modifies.values()

    def _sorted_changes(self):
        result = []
        result.extend(self._adds)
        result.extend(self._deletes)
        result.extend(self._changes)
        result.sort(key=_tree_change_key)
        return result

    def _prune_unchanged(self):
        if self._want_unchanged:
            return
        self._deletes = [d for d in self._deletes if d.type != CHANGE_UNCHANGED]

    def changes_with_renames(self, tree1_id, tree2_id, want_unchanged=False):
        """Iterate TreeChanges between two tree SHAs, with rename detection."""
        self._reset()
        self._want_unchanged = want_unchanged
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
try:
    # Try to import C versions
    from dulwich._diff_tree import _is_tree, _merge_entries, _count_blocks
except ImportError:
    pass
