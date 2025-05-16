# test_graph.py -- Tests for merge base
# Copyright (c) 2020 Kevin B. Hendricks, Stratford Ontario Canada
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

"""Tests for dulwich.graph."""

from dulwich.graph import (
    WorkList,
    _find_lcas,
    can_fast_forward,
    find_merge_base,
    find_octopus_base,
)
from dulwich.repo import MemoryRepo
from dulwich.tests.utils import make_commit

from . import TestCase


class FindMergeBaseTests(TestCase):
    @staticmethod
    def run_test(dag, inputs):
        def lookup_parents(commit_id):
            return dag[commit_id]

        def lookup_stamp(commit_id) -> int:
            # any constant timestamp value here will work to force
            # this test to test the same behaviour as done previously
            return 100

        c1 = inputs[0]
        c2s = inputs[1:]
        return set(_find_lcas(lookup_parents, c1, c2s, lookup_stamp))

    def test_multiple_lca(self) -> None:
        # two lowest common ancestors
        graph = {
            "5": ["1", "2"],
            "4": ["3", "1"],
            "3": ["2"],
            "2": ["0"],
            "1": [],
            "0": [],
        }
        self.assertEqual(self.run_test(graph, ["4", "5"]), {"1", "2"})

    def test_no_common_ancestor(self) -> None:
        # no common ancestor
        graph = {
            "4": ["2"],
            "3": ["1"],
            "2": [],
            "1": ["0"],
            "0": [],
        }
        self.assertEqual(self.run_test(graph, ["4", "3"]), set())

    def test_ancestor(self) -> None:
        # ancestor
        graph = {
            "G": ["D", "F"],
            "F": ["E"],
            "D": ["C"],
            "C": ["B"],
            "E": ["B"],
            "B": ["A"],
            "A": [],
        }
        self.assertEqual(self.run_test(graph, ["D", "C"]), {"C"})

    def test_direct_parent(self) -> None:
        # parent
        graph = {
            "G": ["D", "F"],
            "F": ["E"],
            "D": ["C"],
            "C": ["B"],
            "E": ["B"],
            "B": ["A"],
            "A": [],
        }
        self.assertEqual(self.run_test(graph, ["G", "D"]), {"D"})

    def test_another_crossover(self) -> None:
        # Another cross over
        graph = {
            "G": ["D", "F"],
            "F": ["E", "C"],
            "D": ["C", "E"],
            "C": ["B"],
            "E": ["B"],
            "B": ["A"],
            "A": [],
        }
        self.assertEqual(self.run_test(graph, ["D", "F"]), {"E", "C"})

    def test_three_way_merge_lca(self) -> None:
        # three way merge commit straight from git docs
        graph = {
            "C": ["C1"],
            "C1": ["C2"],
            "C2": ["C3"],
            "C3": ["C4"],
            "C4": ["2"],
            "B": ["B1"],
            "B1": ["B2"],
            "B2": ["B3"],
            "B3": ["1"],
            "A": ["A1"],
            "A1": ["A2"],
            "A2": ["A3"],
            "A3": ["1"],
            "1": ["2"],
            "2": [],
        }
        # assumes a theoretical merge M exists that merges B and C first
        # which actually means find the first LCA from either of B OR C with A
        self.assertEqual(self.run_test(graph, ["A", "B", "C"]), {"1"})

    def test_octopus(self) -> None:
        # octopus algorithm test
        # test straight from git docs of A, B, and C
        # but this time use octopus to find lcas of A, B, and C simultaneously
        graph = {
            "C": ["C1"],
            "C1": ["C2"],
            "C2": ["C3"],
            "C3": ["C4"],
            "C4": ["2"],
            "B": ["B1"],
            "B1": ["B2"],
            "B2": ["B3"],
            "B3": ["1"],
            "A": ["A1"],
            "A1": ["A2"],
            "A2": ["A3"],
            "A3": ["1"],
            "1": ["2"],
            "2": [],
        }

        def lookup_parents(cid):
            return graph[cid]

        def lookup_stamp(commit_id) -> int:
            # any constant timestamp value here will work to force
            # this test to test the same behaviour as done previously
            return 100

        lcas = ["A"]
        others = ["B", "C"]
        for cmt in others:
            next_lcas = []
            for ca in lcas:
                res = _find_lcas(lookup_parents, cmt, [ca], lookup_stamp)
                next_lcas.extend(res)
            lcas = next_lcas[:]
        self.assertEqual(set(lcas), {"2"})


class FindMergeBaseFunctionTests(TestCase):
    def test_find_merge_base_empty(self) -> None:
        r = MemoryRepo()
        # Empty list of commits
        self.assertEqual([], find_merge_base(r, []))

    def test_find_merge_base_single(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        r.object_store.add_objects([(base, None)])
        # Single commit returns itself
        self.assertEqual([base.id], find_merge_base(r, [base.id]))

    def test_find_merge_base_identical(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        r.object_store.add_objects([(base, None)])
        # When the same commit is in both positions
        self.assertEqual([base.id], find_merge_base(r, [base.id, base.id]))

    def test_find_merge_base_linear(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        c1 = make_commit(parents=[base.id])
        c2 = make_commit(parents=[c1.id])
        r.object_store.add_objects([(base, None), (c1, None), (c2, None)])
        # Base of c1 and c2 is c1
        self.assertEqual([c1.id], find_merge_base(r, [c1.id, c2.id]))
        # Base of c2 and c1 is c1
        self.assertEqual([c1.id], find_merge_base(r, [c2.id, c1.id]))

    def test_find_merge_base_diverged(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        c1 = make_commit(parents=[base.id])
        c2a = make_commit(parents=[c1.id], message=b"2a")
        c2b = make_commit(parents=[c1.id], message=b"2b")
        r.object_store.add_objects([(base, None), (c1, None), (c2a, None), (c2b, None)])
        # Merge base of two diverged commits is their common parent
        self.assertEqual([c1.id], find_merge_base(r, [c2a.id, c2b.id]))

    def test_find_merge_base_with_min_stamp(self) -> None:
        r = MemoryRepo()
        base = make_commit(commit_time=100)
        c1 = make_commit(parents=[base.id], commit_time=200)
        c2 = make_commit(parents=[c1.id], commit_time=300)
        r.object_store.add_objects([(base, None), (c1, None), (c2, None)])

        # Normal merge base finding works
        self.assertEqual([c1.id], find_merge_base(r, [c1.id, c2.id]))

    def test_find_merge_base_multiple_common_ancestors(self) -> None:
        r = MemoryRepo()
        base = make_commit(commit_time=100)
        c1a = make_commit(parents=[base.id], commit_time=200, message=b"c1a")
        c1b = make_commit(parents=[base.id], commit_time=201, message=b"c1b")
        c2 = make_commit(parents=[c1a.id, c1b.id], commit_time=300)
        c3 = make_commit(parents=[c1a.id, c1b.id], commit_time=301)
        r.object_store.add_objects(
            [(base, None), (c1a, None), (c1b, None), (c2, None), (c3, None)]
        )

        # Merge base should include both c1a and c1b since both are common ancestors
        bases = find_merge_base(r, [c2.id, c3.id])
        self.assertEqual(2, len(bases))
        self.assertIn(c1a.id, bases)
        self.assertIn(c1b.id, bases)


class FindOctopusBaseTests(TestCase):
    def test_find_octopus_base_empty(self) -> None:
        r = MemoryRepo()
        # Empty list of commits
        self.assertEqual([], find_octopus_base(r, []))

    def test_find_octopus_base_single(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        r.object_store.add_objects([(base, None)])
        # Single commit returns itself
        self.assertEqual([base.id], find_octopus_base(r, [base.id]))

    def test_find_octopus_base_two_commits(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        c1 = make_commit(parents=[base.id])
        c2 = make_commit(parents=[c1.id])
        r.object_store.add_objects([(base, None), (c1, None), (c2, None)])
        # With two commits it should call find_merge_base
        self.assertEqual([c1.id], find_octopus_base(r, [c1.id, c2.id]))

    def test_find_octopus_base_multiple(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        c1 = make_commit(parents=[base.id])
        c2a = make_commit(parents=[c1.id], message=b"2a")
        c2b = make_commit(parents=[c1.id], message=b"2b")
        c2c = make_commit(parents=[c1.id], message=b"2c")
        r.object_store.add_objects(
            [(base, None), (c1, None), (c2a, None), (c2b, None), (c2c, None)]
        )
        # Common ancestor of all three branches
        self.assertEqual([c1.id], find_octopus_base(r, [c2a.id, c2b.id, c2c.id]))


class CanFastForwardTests(TestCase):
    def test_ff(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        c1 = make_commit(parents=[base.id])
        c2 = make_commit(parents=[c1.id])
        r.object_store.add_objects([(base, None), (c1, None), (c2, None)])
        self.assertTrue(can_fast_forward(r, c1.id, c1.id))
        self.assertTrue(can_fast_forward(r, base.id, c1.id))
        self.assertTrue(can_fast_forward(r, c1.id, c2.id))
        self.assertFalse(can_fast_forward(r, c2.id, c1.id))

    def test_diverged(self) -> None:
        r = MemoryRepo()
        base = make_commit()
        c1 = make_commit(parents=[base.id])
        c2a = make_commit(parents=[c1.id], message=b"2a")
        c2b = make_commit(parents=[c1.id], message=b"2b")
        r.object_store.add_objects([(base, None), (c1, None), (c2a, None), (c2b, None)])
        self.assertTrue(can_fast_forward(r, c1.id, c2a.id))
        self.assertTrue(can_fast_forward(r, c1.id, c2b.id))
        self.assertFalse(can_fast_forward(r, c2a.id, c2b.id))
        self.assertFalse(can_fast_forward(r, c2b.id, c2a.id))


class WorkListTest(TestCase):
    def test_WorkList(self) -> None:
        # tuples of (timestamp, value) are stored in a Priority MaxQueue
        # repeated use of get should return them in maxheap timestamp
        # order: largest time value (most recent in time) first then earlier/older
        wlst = WorkList()
        wlst.add((100, "Test Value 1"))
        wlst.add((50, "Test Value 2"))
        wlst.add((200, "Test Value 3"))
        self.assertEqual(wlst.get(), (200, "Test Value 3"))
        self.assertEqual(wlst.get(), (100, "Test Value 1"))
        wlst.add((150, "Test Value 4"))
        self.assertEqual(wlst.get(), (150, "Test Value 4"))
        self.assertEqual(wlst.get(), (50, "Test Value 2"))

    def test_WorkList_iter(self) -> None:
        # Test the iter method of WorkList
        wlst = WorkList()
        wlst.add((100, "Value 1"))
        wlst.add((200, "Value 2"))
        wlst.add((50, "Value 3"))

        # Collect all items from iter
        items = list(wlst.iter())

        # Items should be in their original order, not sorted
        self.assertEqual(len(items), 3)

        # Check the values are present with correct timestamps
        timestamps = [dt for dt, _ in items]
        values = [val for _, val in items]

        self.assertIn(100, timestamps)
        self.assertIn(200, timestamps)
        self.assertIn(50, timestamps)
        self.assertIn("Value 1", values)
        self.assertIn("Value 2", values)
        self.assertIn("Value 3", values)

    def test_WorkList_empty_get(self) -> None:
        # Test getting from an empty WorkList
        wlst = WorkList()
        with self.assertRaises(IndexError):
            wlst.get()

    def test_WorkList_empty_iter(self) -> None:
        # Test iterating over an empty WorkList
        wlst = WorkList()
        items = list(wlst.iter())
        self.assertEqual([], items)

    def test_WorkList_empty_heap(self) -> None:
        # The current implementation raises IndexError when the heap is empty
        wlst = WorkList()
        # Ensure pq is empty
        wlst.pq = []
        # get should raise IndexError when heap is empty
        with self.assertRaises(IndexError):
            wlst.get()
