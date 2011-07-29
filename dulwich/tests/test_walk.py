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
    CHANGE_COPY,
    TreeChange,
    RenameDetector,
    )
from dulwich.errors import (
    MissingCommitError,
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

    def assertNextFails(self, walk_iter, missing_sha):
        try:
            next(walk_iter)
            self.fail('Failed to error on missing sha %s' % missing_sha)
        except MissingCommitError, e:
            self.assertEqual(missing_sha, e.sha)

    def test_missing(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([c3, c2, c1], [c3.id])

        del self.store[c1.id]
        self.assertWalkYields([c3], [c3.id], max_entries=1)
        walk_iter = iter(Walker(self.store, [c3.id]))
        self.assertEqual(TestWalkEntry(c3, None), next(walk_iter))
        self.assertNextFails(walk_iter, c1.id)
        self.assertNextFails(iter(Walker(self.store, [c2.id])), c1.id)
        self.assertRaises(MissingCommitError, Walker, self.store, c1.id)

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

    def test_follow_rename(self):
        blob = make_object(Blob, data='blob')
        names = ['a', 'a', 'b', 'b', 'c', 'c']

        trees = dict((i + 1, [(n, blob, F)]) for i, n in enumerate(names))
        c1, c2, c3, c4, c5, c6 = self.make_linear_commits(6, trees=trees)
        self.assertWalkYields([c5], [c6.id], paths=['c'])

        e = lambda n: (n, F, blob.id)
        self.assertWalkYields(
          [TestWalkEntry(c5, [TreeChange(CHANGE_RENAME, e('b'), e('c'))]),
           TestWalkEntry(c3, [TreeChange(CHANGE_RENAME, e('a'), e('b'))]),
           TestWalkEntry(c1, [TreeChange.add(e('a'))])],
          [c6.id], paths=['c'], follow=True)

    def test_follow_rename_remove_path(self):
        blob = make_object(Blob, data='blob')
        _, _, _, c4, c5, c6 = self.make_linear_commits(
          6, trees={1: [('a', blob), ('c', blob)],
                    2: [],
                    3: [],
                    4: [('b', blob)],
                    5: [('a', blob)],
                    6: [('c', blob)]})

        e = lambda n: (n, F, blob.id)
        # Once the path changes to b, we aren't interested in a or c anymore.
        self.assertWalkYields(
          [TestWalkEntry(c6, [TreeChange(CHANGE_RENAME, e('a'), e('c'))]),
           TestWalkEntry(c5, [TreeChange(CHANGE_RENAME, e('b'), e('a'))]),
           TestWalkEntry(c4, [TreeChange.add(e('b'))])],
          [c6.id], paths=['c'], follow=True)

    def test_since(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([c3, c2, c1], [c3.id], since=-1)
        self.assertWalkYields([c3, c2, c1], [c3.id], since=0)
        self.assertWalkYields([c3, c2], [c3.id], since=1)
        self.assertWalkYields([c3, c2], [c3.id], since=99)
        self.assertWalkYields([c3, c2], [c3.id], since=100)
        self.assertWalkYields([c3], [c3.id], since=101)
        self.assertWalkYields([c3], [c3.id], since=199)
        self.assertWalkYields([c3], [c3.id], since=200)
        self.assertWalkYields([], [c3.id], since=201)
        self.assertWalkYields([], [c3.id], since=300)

    def test_until(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([], [c3.id], until=-1)
        self.assertWalkYields([c1], [c3.id], until=0)
        self.assertWalkYields([c1], [c3.id], until=1)
        self.assertWalkYields([c1], [c3.id], until=99)
        self.assertWalkYields([c2, c1], [c3.id], until=100)
        self.assertWalkYields([c2, c1], [c3.id], until=101)
        self.assertWalkYields([c2, c1], [c3.id], until=199)
        self.assertWalkYields([c3, c2, c1], [c3.id], until=200)
        self.assertWalkYields([c3, c2, c1], [c3.id], until=201)
        self.assertWalkYields([c3, c2, c1], [c3.id], until=300)

    def test_since_until(self):
        c1, c2, c3 = self.make_linear_commits(3)
        self.assertWalkYields([], [c3.id], since=100, until=99)
        self.assertWalkYields([c3, c2, c1], [c3.id], since=-1, until=201)
        self.assertWalkYields([c2], [c3.id], since=100, until=100)
        self.assertWalkYields([c2], [c3.id], since=50, until=150)

    def test_since_over_scan(self):
        times = [9, 0, 1, 2, 3, 4, 5, 8, 6, 7, 9]
        attrs = dict((i + 1, {'commit_time': t}) for i, t in enumerate(times))
        commits = self.make_linear_commits(11, attrs=attrs)
        c8, _, c10, c11 = commits[-4:]
        del self.store[commits[0].id]
        # c9 is older than we want to walk, but is out of order with its parent,
        # so we need to walk past it to get to c8.
        # c1 would also match, but we've deleted it, and it should get pruned
        # even with over-scanning.
        self.assertWalkYields([c11, c10, c8], [c11.id], since=7)
