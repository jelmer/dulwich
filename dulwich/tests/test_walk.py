# test_walk.py -- Tests for commit walking functionality.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# or (at your option) any later version of the License.
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

"""Tests for commit walking functionality."""

from dulwich.diff_tree import (
    CHANGE_ADD,
    CHANGE_MODIFY,
    CHANGE_RENAME,
    TreeChange,
    RenameDetector,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Commit,
    Blob,
    )
from dulwich.walk import (
    WalkEntry,
    Walker,
    )
from dulwich.tests import TestCase
from utils import (
    F,
    make_object,
    build_commit_graph,
    )


class TestWalkEntry(object):

    def __init__(self, commit, changes):
        self.commit = commit
        self.changes = changes

    def __repr__(self):
        return '<TestWalkEntry commit=%s, changes=%r>' % (
          self.commit.id, self.changes)

    def __eq__(self, other):
        if not isinstance(other, WalkEntry) or self.commit != other.commit:
            return False
        if self.changes is None:
            return True
        return self.changes == other.changes()


class WalkerTest(TestCase):

    def setUp(self):
        self.store = MemoryObjectStore()

    def make_commits(self, commit_spec, **kwargs):
        return build_commit_graph(self.store, commit_spec, **kwargs)

    def make_linear_commits(self, num_commits, **kwargs):
        commit_spec = []
        for i in xrange(1, num_commits + 1):
            c = [i]
            if i > 1:
                c.append(i - 1)
            commit_spec.append(c)
        return self.make_commits(commit_spec, **kwargs)

    def assertWalkYields(self, expected, *args, **kwargs):
        walker = Walker(self.store, *args, **kwargs)
        for i, entry in enumerate(expected):
            if isinstance(entry, Commit):
                expected[i] = TestWalkEntry(entry, None)
        actual = list(walker)
        self.assertEqual(expected, actual)

    def test_linear(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([c1], [c1.id])
        self.assertWalkYields([c2, c1], [c2.id])
        self.assertWalkYields([c3, c2, c1], [c3.id])
        self.assertWalkYields([c3, c2, c1], [c3.id, c1.id])
        self.assertWalkYields([c3, c2], [c3.id], exclude=[c1.id])
        self.assertWalkYields([c3, c2], [c3.id, c1.id], exclude=[c1.id])
        self.assertWalkYields([c3], [c3.id, c1.id], exclude=[c2.id])

    def test_branch(self):
        c1, x2, x3, y4 = self.make_commits([[1], [2, 1], [3, 2], [4, 1]])
        self.assertWalkYields([x3, x2, c1], [x3.id])
        self.assertWalkYields([y4, c1], [y4.id])
        self.assertWalkYields([y4, x2, c1], [y4.id, x2.id])
        self.assertWalkYields([y4, x2], [y4.id, x2.id], exclude=[c1.id])
        self.assertWalkYields([y4, x3], [y4.id, x3.id], exclude=[x2.id])
        self.assertWalkYields([y4], [y4.id], exclude=[x3.id])
        self.assertWalkYields([x3, x2], [x3.id], exclude=[y4.id])

    def test_reverse(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([c1, c2, c3], [c3.id], reverse=True)

    def test_max_entries(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([c3, c2, c1], [c3.id], max_entries=3)
        self.assertWalkYields([c3, c2], [c3.id], max_entries=2)
        self.assertWalkYields([c3], [c3.id], max_entries=1)

    def test_reverse_after_max_entries(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([c1, c2, c3], [c3.id], max_entries=3,
                              reverse=True)
        self.assertWalkYields([c2, c3], [c3.id], max_entries=2, reverse=True)
        self.assertWalkYields([c3], [c3.id], max_entries=1, reverse=True)

    def test_changes_one_parent(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        blob_b2 = make_object(Blob, data='b2')
        c1, c2 = self.make_linear_commits(
          2, trees={1: [('a', blob_a1)],
                    2: [('a', blob_a2), ('b', blob_b2)]})
        e1 = TestWalkEntry(c1, [TreeChange.add(('a', F, blob_a1.id))])
        e2 = TestWalkEntry(c2, [TreeChange(CHANGE_MODIFY, ('a', F, blob_a1.id),
                                           ('a', F, blob_a2.id)),
                                TreeChange.add(('b', F, blob_b2.id))])
        self.assertWalkYields([e2, e1], [c2.id])

    def test_changes_multiple_parents(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_b2 = make_object(Blob, data='b2')
        blob_a3 = make_object(Blob, data='a3')
        c1, c2, c3 = self.make_commits(
          [[1], [2], [3, 1, 2]],
          trees={1: [('a', blob_a1)], 2: [('b', blob_b2)],
                 3: [('a', blob_a3), ('b', blob_b2)]})
        # a is a modify/add conflict and b is not conflicted.
        changes = [[
          TreeChange(CHANGE_MODIFY, ('a', F, blob_a1.id), ('a', F, blob_a3.id)),
          TreeChange.add(('a', F, blob_a3.id)),
          ]]
        self.assertWalkYields([TestWalkEntry(c3, changes)], [c3.id],
                              exclude=[c1.id, c2.id])

    def test_path_matches(self):
        walker = Walker(None, [], paths=['foo', 'bar', 'baz/quux'])
        self.assertTrue(walker._path_matches('foo'))
        self.assertTrue(walker._path_matches('foo/a'))
        self.assertTrue(walker._path_matches('foo/a/b'))
        self.assertTrue(walker._path_matches('bar'))
        self.assertTrue(walker._path_matches('baz/quux'))
        self.assertTrue(walker._path_matches('baz/quux/a'))

        self.assertFalse(walker._path_matches(None))
        self.assertFalse(walker._path_matches('oops'))
        self.assertFalse(walker._path_matches('fool'))
        self.assertFalse(walker._path_matches('baz'))
        self.assertFalse(walker._path_matches('baz/quu'))

    def test_paths(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_b2 = make_object(Blob, data='b2')
        blob_a3 = make_object(Blob, data='a3')
        blob_b3 = make_object(Blob, data='b3')
        c1, c2, c3 = self.make_linear_commits(
          3, trees={1: [('a', blob_a1)],
                    2: [('a', blob_a1), ('x/b', blob_b2)],
                    3: [('a', blob_a3), ('x/b', blob_b3)]})

        self.assertWalkYields([c3, c2, c1], [c3.id])
        self.assertWalkYields([c3, c1], [c3.id], paths=['a'])
        self.assertWalkYields([c3, c2], [c3.id], paths=['x/b'])

        # All changes are included, not just for requested paths.
        changes = [
          TreeChange(CHANGE_MODIFY, ('a', F, blob_a1.id),
                     ('a', F, blob_a3.id)),
          TreeChange(CHANGE_MODIFY, ('x/b', F, blob_b2.id),
                     ('x/b', F, blob_b3.id)),
          ]
        self.assertWalkYields([TestWalkEntry(c3, changes)], [c3.id],
                              max_entries=1, paths=['a'])

    def test_paths_subtree(self):
        blob_a = make_object(Blob, data='a')
        blob_b = make_object(Blob, data='b')
        c1, c2, c3 = self.make_linear_commits(
          3, trees={1: [('x/a', blob_a)],
                    2: [('b', blob_b), ('x/a', blob_a)],
                    3: [('b', blob_b), ('x/a', blob_a), ('x/b', blob_b)]})
        self.assertWalkYields([c2], [c3.id], paths=['b'])
        self.assertWalkYields([c3, c1], [c3.id], paths=['x'])

    def test_paths_max_entries(self):
        blob_a = make_object(Blob, data='a')
        blob_b = make_object(Blob, data='b')
        c1, c2 = self.make_linear_commits(
          2, trees={1: [('a', blob_a)],
                    2: [('a', blob_a), ('b', blob_b)]})
        self.assertWalkYields([c2], [c2.id], paths=['b'], max_entries=1)
        self.assertWalkYields([c1], [c1.id], paths=['a'], max_entries=1)

    def test_paths_merge(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        blob_a3 = make_object(Blob, data='a3')
        x1, y2, m3, m4 = self.make_commits(
          [[1], [2], [3, 1, 2], [4, 1, 2]],
          trees={1: [('a', blob_a1)],
                 2: [('a', blob_a2)],
                 3: [('a', blob_a3)],
                 4: [('a', blob_a1)]})  # Non-conflicting
        self.assertWalkYields([m3, y2, x1], [m3.id], paths=['a'])
        self.assertWalkYields([y2, x1], [m4.id], paths=['a'])

    def test_changes_with_renames(self):
        blob = make_object(Blob, data='blob')
        c1, c2 = self.make_linear_commits(
          2, trees={1: [('a', blob)], 2: [('b', blob)]})
        entry_a = ('a', F, blob.id)
        entry_b = ('b', F, blob.id)
        changes_without_renames = [TreeChange.delete(entry_a),
                                   TreeChange.add(entry_b)]
        changes_with_renames = [TreeChange(CHANGE_RENAME, entry_a, entry_b)]
        self.assertWalkYields(
          [TestWalkEntry(c2, changes_without_renames)], [c2.id], max_entries=1)
        detector = RenameDetector(self.store)
        self.assertWalkYields(
          [TestWalkEntry(c2, changes_with_renames)], [c2.id], max_entries=1,
          rename_detector=detector)
