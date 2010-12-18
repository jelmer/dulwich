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
    CHANGE_ADD,
    CHANGE_MODIFY,
    CHANGE_DELETE,
    CHANGE_UNCHANGED,
    _NULL_ENTRY,
    TreeChange,
    _merge_entries,
    tree_changes,
    _count_blocks,
    _similarity_score,
    )
from dulwich.index import (
    commit_tree,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    ShaFile,
    Blob,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    make_object,
    )


class TreeChangesTest(TestCase):

    def setUp(self):
        super(TreeChangesTest, self).setUp()
        self.store = MemoryObjectStore()
        self.empty_tree = self.commit_tree([])

    def commit_tree(self, blobs):
        commit_blobs = []
        for path, blob, mode in blobs:
            self.store.add_object(blob)
            commit_blobs.append((path, blob.id, mode))
        return self.store[commit_tree(self.store, commit_blobs)]

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
        tree = self.commit_tree([('a', blob, 0100644),
                                 ('b/c', blob, 0100644)])
        self.assertChangesEqual([], self.empty_tree, self.empty_tree)
        self.assertChangesEqual([], tree, tree)
        self.assertChangesEqual(
          [TreeChange(CHANGE_UNCHANGED, ('a', 0100644, blob.id),
                      ('a', 0100644, blob.id)),
           TreeChange(CHANGE_UNCHANGED, ('b/c', 0100644, blob.id),
                      ('b/c', 0100644, blob.id))],
          tree, tree, want_unchanged=True)

    def test_tree_changes_add_delete(self):
        blob_a = make_object(Blob, data='a')
        blob_b = make_object(Blob, data='b')
        tree = self.commit_tree([('a', blob_a, 0100644),
                                 ('x/b', blob_b, 0100755)])
        self.assertChangesEqual(
          [TreeChange(CHANGE_ADD, _NULL_ENTRY, ('a', 0100644, blob_a.id)),
           TreeChange(CHANGE_ADD, _NULL_ENTRY, ('x/b', 0100755, blob_b.id))],
          self.empty_tree, tree)
        self.assertChangesEqual(
          [TreeChange(CHANGE_DELETE, ('a', 0100644, blob_a.id), _NULL_ENTRY),
           TreeChange(CHANGE_DELETE, ('x/b', 0100755, blob_b.id), _NULL_ENTRY)],
          tree, self.empty_tree)

    def test_tree_changes_modify_contents(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        tree1 = self.commit_tree([('a', blob_a1, 0100644)])
        tree2 = self.commit_tree([('a', blob_a2, 0100644)])
        self.assertChangesEqual(
          [TreeChange(CHANGE_MODIFY, ('a', 0100644, blob_a1.id),
                      ('a', 0100644, blob_a2.id))], tree1, tree2)

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
          [TreeChange(CHANGE_DELETE, ('a', 0100644, blob_a1.id), _NULL_ENTRY),
           TreeChange(CHANGE_ADD, _NULL_ENTRY, ('a', 0120000, blob_a2.id))],
          tree1, tree2)

    def test_tree_changes_to_tree(self):
        blob_a = make_object(Blob, data='a')
        blob_x = make_object(Blob, data='x')
        tree1 = self.commit_tree([('a', blob_a, 0100644)])
        tree2 = self.commit_tree([('a/x', blob_x, 0100644)])
        self.assertChangesEqual(
          [TreeChange(CHANGE_DELETE, ('a', 0100644, blob_a.id), _NULL_ENTRY),
           TreeChange(CHANGE_ADD, _NULL_ENTRY, ('a/x', 0100644, blob_x.id))],
          tree1, tree2)

    def test_tree_changes_complex(self):
        blob_a_1 = make_object(Blob, data='a1_1')
        blob_bx1_1 = make_object(Blob, data='bx1_1')
        blob_bx2_1 = make_object(Blob, data='bx2_1')
        blob_by1_1 = make_object(Blob, data='by1_1')
        blob_by2_1 = make_object(Blob, data='by2_1')
        tree1 = self.commit_tree([
          ('a', blob_a_1, 0100644),
          ('b/x/1', blob_bx1_1, 0100644),
          ('b/x/2', blob_bx2_1, 0100644),
          ('b/y/1', blob_by1_1, 0100644),
          ('b/y/2', blob_by2_1, 0100644),
          ])

        blob_a_2 = make_object(Blob, data='a1_2')
        blob_bx1_2 = blob_bx1_1
        blob_by_2 = make_object(Blob, data='by_2')
        blob_c_2 = make_object(Blob, data='c_2')
        tree2 = self.commit_tree([
          ('a', blob_a_2, 0100644),
          ('b/x/1', blob_bx1_2, 0100644),
          ('b/y', blob_by_2, 0100644),
          ('c', blob_c_2, 0100644),
          ])

        self.assertChangesEqual(
          [TreeChange(CHANGE_MODIFY, ('a', 0100644, blob_a_1.id),
                      ('a', 0100644, blob_a_2.id)),
           TreeChange(CHANGE_DELETE, ('b/x/2', 0100644, blob_bx2_1.id),
                      _NULL_ENTRY),
           TreeChange(CHANGE_ADD, _NULL_ENTRY, ('b/y', 0100644, blob_by_2.id)),
           TreeChange(CHANGE_DELETE, ('b/y/1', 0100644, blob_by1_1.id),
                      _NULL_ENTRY),
           TreeChange(CHANGE_DELETE, ('b/y/2', 0100644, blob_by2_1.id),
                      _NULL_ENTRY),
           TreeChange(CHANGE_ADD, _NULL_ENTRY, ('c', 0100644, blob_c_2.id))],
          tree1, tree2)

    def test_tree_changes_name_order(self):
        blob = make_object(Blob, data='a')
        tree1 = self.commit_tree([
          ('a', blob, 0100644),
          ('a.', blob, 0100644),
          ('a..', blob, 0100644),
          ])
        # Tree order is the reverse of this, so if we used tree order, 'a..'
        # would not be merged.
        tree2 = self.commit_tree([
          ('a/x', blob, 0100644),
          ('a./x', blob, 0100644),
          ('a..', blob, 0100644),
          ])

        self.assertChangesEqual(
          [TreeChange(CHANGE_DELETE, ('a', 0100644, blob.id), _NULL_ENTRY),
           TreeChange(CHANGE_ADD, _NULL_ENTRY, ('a/x', 0100644, blob.id)),
           TreeChange(CHANGE_DELETE, ('a.', 0100644, blob.id), _NULL_ENTRY),
           TreeChange(CHANGE_ADD, _NULL_ENTRY, ('a./x', 0100644, blob.id))],
          tree1, tree2)

    def test_tree_changes_prune(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        blob_x = make_object(Blob, data='x')
        tree1 = self.commit_tree([('a', blob_a1, 0100644),
                                  ('b/x', blob_x, 0100644)])
        tree2 = self.commit_tree([('a', blob_a2, 0100644),
                                  ('b/x', blob_x, 0100644)])
        # Remove identical items so lookups will fail unless we prune.
        subtree = self.store[tree1['b'][1]]
        for entry in subtree.iteritems():
            del self.store[entry.sha]
        del self.store[subtree.id]

        self.assertChangesEqual(
          [TreeChange(CHANGE_MODIFY, ('a', 0100644, blob_a1.id),
                      ('a', 0100644, blob_a2.id))],
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
