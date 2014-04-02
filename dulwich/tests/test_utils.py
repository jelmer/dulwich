# test_utils.py -- Tests for git test utilities.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License

# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA  02110-1301, USA.

"""Tests for git test utilities."""

from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    make_object,
    build_commit_graph,
    )


class BuildCommitGraphTest(TestCase):

    def setUp(self):
        super(BuildCommitGraphTest, self).setUp()
        self.store = MemoryObjectStore()

    def test_linear(self):
        c1, c2 = build_commit_graph(self.store, [[1], [2, 1]])
        for obj_id in [c1.id, c2.id, c1.tree, c2.tree]:
            self.assertTrue(obj_id in self.store)
        self.assertEqual([], c1.parents)
        self.assertEqual([c1.id], c2.parents)
        self.assertEqual(c1.tree, c2.tree)
        self.assertEqual([], list(self.store[c1.tree].iteritems()))
        self.assertTrue(c2.commit_time > c1.commit_time)

    def test_merge(self):
        c1, c2, c3, c4 = build_commit_graph(self.store,
                                            [[1], [2, 1], [3, 1], [4, 2, 3]])
        self.assertEqual([c2.id, c3.id], c4.parents)
        self.assertTrue(c4.commit_time > c2.commit_time)
        self.assertTrue(c4.commit_time > c3.commit_time)

    def test_missing_parent(self):
        self.assertRaises(ValueError, build_commit_graph, self.store,
                          [[1], [3, 2], [2, 1]])

    def test_trees(self):
        a1 = make_object(Blob, data='aaa1')
        a2 = make_object(Blob, data='aaa2')
        c1, c2 = build_commit_graph(self.store, [[1], [2, 1]],
                                    trees={1: [('a', a1)],
                                           2: [('a', a2, 0o100644)]})
        self.assertEqual((0o100644, a1.id), self.store[c1.tree]['a'])
        self.assertEqual((0o100644, a2.id), self.store[c2.tree]['a'])

    def test_attrs(self):
        c1, c2 = build_commit_graph(self.store, [[1], [2, 1]],
                                    attrs={1: {'message': 'Hooray!'}})
        self.assertEqual('Hooray!', c1.message)
        self.assertEqual('Commit 2', c2.message)

    def test_commit_time(self):
        c1, c2, c3 = build_commit_graph(self.store, [[1], [2, 1], [3, 2]],
                                        attrs={1: {'commit_time': 124},
                                               2: {'commit_time': 123}})
        self.assertEqual(124, c1.commit_time)
        self.assertEqual(123, c2.commit_time)
        self.assertTrue(c2.commit_time < c1.commit_time < c3.commit_time)
