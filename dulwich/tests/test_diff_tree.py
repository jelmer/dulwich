# test_diff_tree.py -- Tests for file and tree diff utilities.
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

"""Tests for file and tree diff utilities."""

from dulwich.diff_tree import (
    CHANGE_MODIFY,
    CHANGE_RENAME,
    CHANGE_COPY,
    CHANGE_UNCHANGED,
    TreeChange,
    _merge_entries,
    tree_changes,
    _count_blocks,
    _similarity_score,
    _tree_change_key,
    )
from dulwich.index import (
    commit_tree,
    )
from dulwich.misc import (
    permutations,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    ShaFile,
    Blob,
    TreeEntry,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    make_object,
    )

# Shorthand mode for Files.
F = 0100644


class DiffTestCase(TestCase):

    def setUp(self):
        super(DiffTestCase, self).setUp()
        self.store = MemoryObjectStore()
        self.empty_tree = self.commit_tree([])

    def commit_tree(self, blobs):
        commit_blobs = []
        for entry in blobs:
            if len(entry) == 2:
                path, blob = entry
                mode = F
            else:
                path, blob, mode = entry
            self.store.add_object(blob)
            commit_blobs.append((path, blob.id, mode))
        return self.store[commit_tree(self.store, commit_blobs)]


class TreeChangesTest(DiffTestCase):

    def test_merge_entries(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        blob_b1 = make_object(Blob, data='b1')
        blob_c2 = make_object(Blob, data='c2')
        tree1 = self.commit_tree([('a', blob_a1, 0100644),
                                  ('b', blob_b1, 0100755)])
        tree2 = self.commit_tree([('a', blob_a2, 0100644),
                                  ('c', blob_c2, 0100755)])

        self.assertEqual([], _merge_entries('', self.empty_tree,
                                            self.empty_tree))
        self.assertEqual([
          ((None, None, None), ('a', 0100644, blob_a1.id)),
          ((None, None, None), ('b', 0100755, blob_b1.id)),
          ], _merge_entries('', self.empty_tree, tree1))
        self.assertEqual([
          ((None, None, None), ('x/a', 0100644, blob_a1.id)),
          ((None, None, None), ('x/b', 0100755, blob_b1.id)),
          ], _merge_entries('x', self.empty_tree, tree1))

        self.assertEqual([
          (('a', 0100644, blob_a2.id), (None, None, None)),
          (('c', 0100755, blob_c2.id), (None, None, None)),
          ], _merge_entries('', tree2, self.empty_tree))

        self.assertEqual([
          (('a', 0100644, blob_a1.id), ('a', 0100644, blob_a2.id)),
          (('b', 0100755, blob_b1.id), (None, None, None)),
          ((None, None, None), ('c', 0100755, blob_c2.id)),
          ], _merge_entries('', tree1, tree2))

        self.assertEqual([
          (('a', 0100644, blob_a2.id), ('a', 0100644, blob_a1.id)),
          ((None, None, None), ('b', 0100755, blob_b1.id)),
          (('c', 0100755, blob_c2.id), (None, None, None)),
          ], _merge_entries('', tree2, tree1))

    def assertChangesEqual(self, expected, tree1, tree2, **kwargs):
        actual = list(tree_changes(self.store, tree1.id, tree2.id, **kwargs))
        self.assertEqual(expected, actual)

    # For brevity, the following tests use tuples instead of TreeEntry objects.

    def test_tree_changes_empty(self):
        self.assertChangesEqual([], self.empty_tree, self.empty_tree)

    def test_tree_changes_no_changes(self):
        blob = make_object(Blob, data='blob')
        tree = self.commit_tree([('a', blob), ('b/c', blob)])
        self.assertChangesEqual([], self.empty_tree, self.empty_tree)
        self.assertChangesEqual([], tree, tree)
        self.assertChangesEqual(
          [TreeChange(CHANGE_UNCHANGED, ('a', F, blob.id), ('a', F, blob.id)),
           TreeChange(CHANGE_UNCHANGED, ('b/c', F, blob.id),
                      ('b/c', F, blob.id))],
          tree, tree, want_unchanged=True)

    def test_tree_changes_add_delete(self):
        blob_a = make_object(Blob, data='a')
        blob_b = make_object(Blob, data='b')
        tree = self.commit_tree([('a', blob_a, 0100644),
                                 ('x/b', blob_b, 0100755)])
        self.assertChangesEqual(
          [TreeChange.add(('a', 0100644, blob_a.id)),
           TreeChange.add(('x/b', 0100755, blob_b.id))],
          self.empty_tree, tree)
        self.assertChangesEqual(
          [TreeChange.delete(('a', 0100644, blob_a.id)),
           TreeChange.delete(('x/b', 0100755, blob_b.id))],
          tree, self.empty_tree)

    def test_tree_changes_modify_contents(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        tree1 = self.commit_tree([('a', blob_a1)])
        tree2 = self.commit_tree([('a', blob_a2)])
        self.assertChangesEqual(
          [TreeChange(CHANGE_MODIFY, ('a', F, blob_a1.id),
                      ('a', F, blob_a2.id))], tree1, tree2)

    def test_tree_changes_modify_mode(self):
        blob_a = make_object(Blob, data='a')
        tree1 = self.commit_tree([('a', blob_a, 0100644)])
        tree2 = self.commit_tree([('a', blob_a, 0100755)])
        self.assertChangesEqual(
          [TreeChange(CHANGE_MODIFY, ('a', 0100644, blob_a.id),
                      ('a', 0100755, blob_a.id))], tree1, tree2)

    def test_tree_changes_change_type(self):
        blob_a1 = make_object(Blob, data='a')
        blob_a2 = make_object(Blob, data='/foo/bar')
        tree1 = self.commit_tree([('a', blob_a1, 0100644)])
        tree2 = self.commit_tree([('a', blob_a2, 0120000)])
        self.assertChangesEqual(
          [TreeChange.delete(('a', 0100644, blob_a1.id)),
           TreeChange.add(('a', 0120000, blob_a2.id))],
          tree1, tree2)

    def test_tree_changes_to_tree(self):
        blob_a = make_object(Blob, data='a')
        blob_x = make_object(Blob, data='x')
        tree1 = self.commit_tree([('a', blob_a)])
        tree2 = self.commit_tree([('a/x', blob_x)])
        self.assertChangesEqual(
          [TreeChange.delete(('a', F, blob_a.id)),
           TreeChange.add(('a/x', F, blob_x.id))],
          tree1, tree2)

    def test_tree_changes_complex(self):
        blob_a_1 = make_object(Blob, data='a1_1')
        blob_bx1_1 = make_object(Blob, data='bx1_1')
        blob_bx2_1 = make_object(Blob, data='bx2_1')
        blob_by1_1 = make_object(Blob, data='by1_1')
        blob_by2_1 = make_object(Blob, data='by2_1')
        tree1 = self.commit_tree([
          ('a', blob_a_1),
          ('b/x/1', blob_bx1_1),
          ('b/x/2', blob_bx2_1),
          ('b/y/1', blob_by1_1),
          ('b/y/2', blob_by2_1),
          ])

        blob_a_2 = make_object(Blob, data='a1_2')
        blob_bx1_2 = blob_bx1_1
        blob_by_2 = make_object(Blob, data='by_2')
        blob_c_2 = make_object(Blob, data='c_2')
        tree2 = self.commit_tree([
          ('a', blob_a_2),
          ('b/x/1', blob_bx1_2),
          ('b/y', blob_by_2),
          ('c', blob_c_2),
          ])

        self.assertChangesEqual(
          [TreeChange(CHANGE_MODIFY, ('a', F, blob_a_1.id),
                      ('a', F, blob_a_2.id)),
           TreeChange.delete(('b/x/2', F, blob_bx2_1.id)),
           TreeChange.add(('b/y', F, blob_by_2.id)),
           TreeChange.delete(('b/y/1', F, blob_by1_1.id)),
           TreeChange.delete(('b/y/2', F, blob_by2_1.id)),
           TreeChange.add(('c', F, blob_c_2.id))],
          tree1, tree2)

    def test_tree_changes_name_order(self):
        blob = make_object(Blob, data='a')
        tree1 = self.commit_tree([('a', blob), ('a.', blob), ('a..', blob)])
        # Tree order is the reverse of this, so if we used tree order, 'a..'
        # would not be merged.
        tree2 = self.commit_tree([('a/x', blob), ('a./x', blob), ('a..', blob)])

        self.assertChangesEqual(
          [TreeChange.delete(('a', F, blob.id)),
           TreeChange.add(('a/x', F, blob.id)),
           TreeChange.delete(('a.', F, blob.id)),
           TreeChange.add(('a./x', F, blob.id))],
          tree1, tree2)

    def test_tree_changes_prune(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        blob_x = make_object(Blob, data='x')
        tree1 = self.commit_tree([('a', blob_a1), ('b/x', blob_x)])
        tree2 = self.commit_tree([('a', blob_a2), ('b/x', blob_x)])
        # Remove identical items so lookups will fail unless we prune.
        subtree = self.store[tree1['b'][1]]
        for entry in subtree.iteritems():
            del self.store[entry.sha]
        del self.store[subtree.id]

        self.assertChangesEqual(
          [TreeChange(CHANGE_MODIFY, ('a', F, blob_a1.id),
                      ('a', F, blob_a2.id))],
          tree1, tree2)


class RenameDetectionTest(TestCase):

    def test_count_blocks(self):
        blob = make_object(Blob, data='a\nb\na\n')
        self.assertEqual({'a\n': 2, 'b\n': 1}, _count_blocks(blob))

    def test_count_blocks_no_newline(self):
        blob = make_object(Blob, data='a\na')
        self.assertEqual({'a\n': 1, 'a': 1}, _count_blocks(blob))

    def test_count_blocks_chunks(self):
        blob = ShaFile.from_raw_chunks(Blob.type_num, ['a\nb', '\na\n'])
        self.assertEqual({'a\n': 2, 'b\n': 1}, _count_blocks(blob))

    def test_count_blocks_long_lines(self):
        a = 'a' * 64
        data = a + 'xxx\ny\n' + a + 'zzz\n'
        blob = make_object(Blob, data=data)
        self.assertEqual({'a' * 64: 2, 'xxx\n': 1, 'y\n': 1, 'zzz\n': 1},
                         _count_blocks(blob))

    def assertSimilar(self, expected_score, blob1, blob2):
        self.assertEqual(expected_score, _similarity_score(blob1, blob2))
        self.assertEqual(expected_score, _similarity_score(blob2, blob1))

    def test_similarity_score(self):
        blob0 = make_object(Blob, data='')
        blob1 = make_object(Blob, data='ab\ncd\ncd\n')
        blob2 = make_object(Blob, data='ab\n')
        blob3 = make_object(Blob, data='cd\n')
        blob4 = make_object(Blob, data='cd\ncd\n')

        self.assertSimilar(100, blob0, blob0)
        self.assertSimilar(0, blob0, blob1)
        self.assertSimilar(33, blob1, blob2)
        self.assertSimilar(33, blob1, blob3)
        self.assertSimilar(66, blob1, blob4)
        self.assertSimilar(0, blob2, blob3)
        self.assertSimilar(50, blob3, blob4)

    def test_similarity_score_cache(self):
        blob1 = make_object(Blob, data='ab\ncd\n')
        blob2 = make_object(Blob, data='ab\n')

        block_cache = {}
        self.assertEqual(
          50, _similarity_score(blob1, blob2, block_cache=block_cache))
        self.assertEqual(set([blob1.id, blob2.id]), set(block_cache))

        def fail_chunks():
            self.fail('Unexpected call to as_raw_chunks()')

        blob1.as_raw_chunks = blob2.as_raw_chunks = fail_chunks
        blob1.raw_length = lambda: 6
        blob2.raw_length = lambda: 3
        self.assertEqual(
          50, _similarity_score(blob1, blob2, block_cache=block_cache))

    def test_tree_entry_sort(self):
        sha = 'abcd' * 10
        expected_entries = [
          TreeChange.add(TreeEntry('aaa', F, sha)),
          TreeChange(CHANGE_COPY, TreeEntry('bbb', F, sha),
                     TreeEntry('aab', F, sha)),
          TreeChange(CHANGE_MODIFY, TreeEntry('bbb', F, sha),
                     TreeEntry('bbb', F, 'dabc' * 10)),
          TreeChange(CHANGE_RENAME, TreeEntry('bbc', F, sha),
                     TreeEntry('ddd', F, sha)),
          TreeChange.delete(TreeEntry('ccc', F, sha)),
          ]

        for perm in permutations(expected_entries):
            self.assertEqual(expected_entries,
                             sorted(perm, key=_tree_change_key))
