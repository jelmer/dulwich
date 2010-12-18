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

from cStringIO import StringIO
import itertools
import stat

from dulwich.misc import (
    defaultdict,
    TreeChangeTuple,
    )
from dulwich.objects import (
    TreeEntry,
    )

# TreeChange type constants.
CHANGE_ADD = 'add'
CHANGE_MODIFY = 'modify'
CHANGE_DELETE = 'delete'
CHANGE_RENAME = 'rename'
CHANGE_COPY = 'copy'
CHANGE_UNCHANGED = 'unchanged'

_NULL_ENTRY = TreeEntry(None, None, None)


class TreeChange(TreeChangeTuple):
    """Class encapsulating a single change between two trees."""


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


def walk_trees(store, tree1_id, tree2_id, prune_identical=False):
    """Recursively walk all the entries of two trees.

    Iteration is depth-first pre-order, as in e.g. os.walk.

    :param store: An ObjectStore for looking up objects.
    :param tree1_id: The SHA of the first Tree object to iterate, or None.
    :param tree2_id: The SHA of the second Tree object to iterate, or None.
    :param prune_identical: If True, identical subtrees will not be walked.
    :yield: Pairs of TreeEntry objects for each pair of entries in the trees and
        their subtrees recursively. If an entry exists in one tree but not the
        other, the other entry will have all attributes set to None. If neither
        entry's path is None, they are guaranteed to match.
    """
    # This could be fairly easily generalized to >2 trees if we find a use case.
    mode1 = tree1_id and stat.S_IFDIR or None
    mode2 = tree2_id and stat.S_IFDIR or None
    todo = [(TreeEntry('', mode1, tree1_id), TreeEntry('', mode2, tree2_id))]
    while todo:
        entry1, entry2 = todo.pop()
        is_tree1 = entry1.mode and stat.S_ISDIR(entry1.mode)
        is_tree2 = entry2.mode and stat.S_ISDIR(entry2.mode)
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


def tree_changes(store, tree1_id, tree2_id, want_unchanged=False):
    """Find the differences between the contents of two trees.

    :param store: An ObjectStore for looking up objects.
    :param tree1_id: The SHA of the source tree.
    :param tree2_id: The SHA of the target tree.
    :param want_unchanged: If True, include TreeChanges for unmodified entries
        as well.
    :yield: TreeChange instances for each change between the source and target
        tree.
    """
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
                yield TreeChange(CHANGE_DELETE, entry1, _NULL_ENTRY)
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


_BLOCK_SIZE = 64


def _count_blocks(obj):
    """Count the blocks in an object.

    Splits the data into blocks either on lines or <=64-byte chunks of lines.

    :param obj: The object to count blocks for.
    :return: A dict of block -> number of occurrences.
    """
    block_counts = defaultdict(int)
    block = StringIO()
    for c in itertools.chain(*obj.as_raw_chunks()):
        block.write(c)
        if c == '\n' or block.tell() == _BLOCK_SIZE:
            block_counts[block.getvalue()] += 1
            block.seek(0)
            block.truncate()
    last_block = block.getvalue()
    if last_block:
        block_counts[last_block] += 1
    return block_counts
