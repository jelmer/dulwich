# test_missing_obj_finder.py -- tests for MissingObjectFinder
# Copyright (C) 2012 syntevo GmbH
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

from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    )
from dulwich.tests import TestCase
from dulwich.tests.utils import (
    make_object,
    build_commit_graph,
    )


class MissingObjectFinderTest(TestCase):

    def setUp(self):
        super(MissingObjectFinderTest, self).setUp()
        self.store = MemoryObjectStore()
        self.commits = []

    def cmt(self, n):
        return self.commits[n-1]

    def assertMissingMatch(self, haves, wants, expected):
        for sha, path in self.store.find_missing_objects(haves, wants):
            self.assertTrue(sha in expected,
                "(%s,%s) erroneously reported as missing" % (sha, path))
            expected.remove(sha)

        self.assertEqual(len(expected), 0,
            "some objects are not reported as missing: %s" % (expected, ))


class MOFLinearRepoTest(MissingObjectFinderTest):

    def setUp(self):
        super(MOFLinearRepoTest, self).setUp()
        f1_1 = make_object(Blob, data='f1') # present in 1, removed in 3
        f2_1 = make_object(Blob, data='f2') # present in all revisions, changed in 2 and 3
        f2_2 = make_object(Blob, data='f2-changed')
        f2_3 = make_object(Blob, data='f2-changed-again')
        f3_2 = make_object(Blob, data='f3') # added in 2, left unmodified in 3

        commit_spec = [[1], [2, 1], [3, 2]]
        trees = {1: [('f1', f1_1), ('f2', f2_1)],
                2: [('f1', f1_1), ('f2', f2_2), ('f3', f3_2)],
                3: [('f2', f2_3), ('f3', f3_2)] }
        # commit 1: f1 and f2
        # commit 2: f3 added, f2 changed. Missing shall report commit id and a
        # tree referenced by commit
        # commit 3: f1 removed, f2 changed. Commit sha and root tree sha shall
        # be reported as modified
        self.commits = build_commit_graph(self.store, commit_spec, trees)
        self.missing_1_2 = [self.cmt(2).id, self.cmt(2).tree, f2_2.id, f3_2.id]
        self.missing_2_3 = [self.cmt(3).id, self.cmt(3).tree, f2_3.id]
        self.missing_1_3 = [
            self.cmt(2).id, self.cmt(3).id,
            self.cmt(2).tree, self.cmt(3).tree,
            f2_2.id, f3_2.id, f2_3.id]

    def test_1_to_2(self):
        self.assertMissingMatch([self.cmt(1).id], [self.cmt(2).id],
            self.missing_1_2)

    def test_2_to_3(self):
        self.assertMissingMatch([self.cmt(2).id], [self.cmt(3).id],
            self.missing_2_3)

    def test_1_to_3(self):
        self.assertMissingMatch([self.cmt(1).id], [self.cmt(3).id],
            self.missing_1_3)

    def test_bogus_haves_failure(self):
        """Ensure non-existent SHA in haves are not tolerated"""
        bogus_sha = self.cmt(2).id[::-1]
        haves = [self.cmt(1).id, bogus_sha]
        wants = [self.cmt(3).id]
        self.assertRaises(KeyError, self.store.find_missing_objects,
            self.store, haves, wants)

    def test_bogus_wants_failure(self):
        """Ensure non-existent SHA in wants are not tolerated"""
        bogus_sha = self.cmt(2).id[::-1]
        haves = [self.cmt(1).id]
        wants = [self.cmt(3).id, bogus_sha]
        self.assertRaises(KeyError, self.store.find_missing_objects,
            self.store, haves, wants)

    def test_no_changes(self):
        self.assertMissingMatch([self.cmt(3).id], [self.cmt(3).id], [])


class MOFMergeForkRepoTest(MissingObjectFinderTest):
    # 1 --- 2 --- 4 --- 6 --- 7
    #          \        /
    #           3  ---
    #            \
    #             5

    def setUp(self):
        super(MOFMergeForkRepoTest, self).setUp()
        f1_1 = make_object(Blob, data='f1')
        f1_2 = make_object(Blob, data='f1-2')
        f1_4 = make_object(Blob, data='f1-4')
        f1_7 = make_object(Blob, data='f1-2') # same data as in rev 2
        f2_1 = make_object(Blob, data='f2')
        f2_3 = make_object(Blob, data='f2-3')
        f3_3 = make_object(Blob, data='f3')
        f3_5 = make_object(Blob, data='f3-5')
        commit_spec = [[1], [2, 1], [3, 2], [4, 2], [5, 3], [6, 3, 4], [7, 6]]
        trees = {1: [('f1', f1_1), ('f2', f2_1)],
                2: [('f1', f1_2), ('f2', f2_1)], # f1 changed
                # f3 added, f2 changed
                3: [('f1', f1_2), ('f2', f2_3), ('f3', f3_3)],
                4: [('f1', f1_4), ('f2', f2_1)],  # f1 changed
                5: [('f1', f1_2), ('f3', f3_5)], # f2 removed, f3 changed
                6: [('f1', f1_4), ('f2', f2_3), ('f3', f3_3)], # merged 3 and 4
                # f1 changed to match rev2. f3 removed
                7: [('f1', f1_7), ('f2', f2_3)]}
        self.commits = build_commit_graph(self.store, commit_spec, trees)

        self.f1_2_id = f1_2.id
        self.f1_4_id = f1_4.id
        self.f1_7_id = f1_7.id
        self.f2_3_id = f2_3.id
        self.f3_3_id = f3_3.id

        self.assertEqual(f1_2.id, f1_7.id, "[sanity]")

    def test_have6_want7(self):
        # have 6, want 7. Ideally, shall not report f1_7 as it's the same as
        # f1_2, however, to do so, MissingObjectFinder shall not record trees
        # of common commits only, but also all parent trees and tree items,
        # which is an overkill (i.e. in sha_done it records f1_4 as known, and
        # doesn't record f1_2 was known prior to that, hence can't detect f1_7
        # is in fact f1_2 and shall not be reported)
        self.assertMissingMatch([self.cmt(6).id], [self.cmt(7).id],
            [self.cmt(7).id, self.cmt(7).tree, self.f1_7_id])

    def test_have4_want7(self):
        # have 4, want 7. Shall not include rev5 as it is not in the tree
        # between 4 and 7 (well, it is, but its SHA's are irrelevant for 4..7
        # commit hierarchy)
        self.assertMissingMatch([self.cmt(4).id], [self.cmt(7).id], [
            self.cmt(7).id, self.cmt(6).id, self.cmt(3).id,
            self.cmt(7).tree, self.cmt(6).tree, self.cmt(3).tree,
            self.f2_3_id, self.f3_3_id])

    def test_have1_want6(self):
        # have 1, want 6. Shall not include rev5
        self.assertMissingMatch([self.cmt(1).id], [self.cmt(6).id], [
            self.cmt(6).id, self.cmt(4).id, self.cmt(3).id, self.cmt(2).id,
            self.cmt(6).tree, self.cmt(4).tree, self.cmt(3).tree,
            self.cmt(2).tree, self.f1_2_id, self.f1_4_id, self.f2_3_id,
            self.f3_3_id])

    def test_have3_want6(self):
        # have 3, want 7. Shall not report rev2 and its tree, because
        # haves(3) means has parents, i.e. rev2, too
        # BUT shall report any changes descending rev2 (excluding rev3)
        # Shall NOT report f1_7 as it's techically == f1_2
        self.assertMissingMatch([self.cmt(3).id], [self.cmt(7).id], [
              self.cmt(7).id, self.cmt(6).id, self.cmt(4).id,
              self.cmt(7).tree, self.cmt(6).tree, self.cmt(4).tree,
              self.f1_4_id])

    def test_have5_want7(self):
        # have 5, want 7. Common parent is rev2, hence children of rev2 from
        # a descent line other than rev5 shall be reported
        # expects f1_4 from rev6. f3_5 is known in rev5;
        # f1_7 shall be the same as f1_2 (known, too)
        self.assertMissingMatch([self.cmt(5).id], [self.cmt(7).id], [
              self.cmt(7).id, self.cmt(6).id, self.cmt(4).id,
              self.cmt(7).tree, self.cmt(6).tree, self.cmt(4).tree,
              self.f1_4_id])
