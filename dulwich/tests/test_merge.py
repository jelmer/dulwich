# -*- coding: utf-8 -*-
# test_index.py -- Tests for merge
# encoding: utf-8
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
#
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
#

"""Tests for merge."""

import os
import shutil
import tempfile

from dulwich.tests import TestCase
from dulwich.tests.utils import build_commit_graph

from dulwich.merge import merge, find_merge_base
from dulwich.repo import Repo


class MergeTests(TestCase):

    def setUp(self):
        super(MergeTests, self).setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo1_path = os.path.join(self.test_dir, 'repo1')
        self.repo2_path = os.path.join(self.test_dir, 'repo2')
        self.repo1 = Repo.init(self.repo1_path, mkdir=True)
        self.common_cid = self._add_file(self.repo1, 'a')
        self.repo2 = self.repo1.clone(self.repo2_path, mkdir=True)
        self.addCleanup(self.repo2.close)
        self.addCleanup(self.repo1.close)

    def _add_file(self, repo, name, contents='a line\n'):
        with open(os.path.join(repo.path, name), 'w') as f:
            f.write('Added in repo 1')
        repo.stage([name])
        return repo.do_commit(('Add file %s' % name).encode('ascii'))

    def test_both_adds(self):
        # Two trees both add a new file
        cid1 = self._add_file(self.repo1, 'b')
        cid2 = self._add_file(self.repo2, 'c')
        self.repo2.fetch(self.repo1)
        self.assertEqual(cid1, self.repo1.head())
        conflicts = merge(self.repo1, [cid2])
        self.assertEqual([], conflicts)


class FindMergeBaseTests(TestCase):

    def setUp(self):
        super(FindMergeBaseTests, self).setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def test_both_adds(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1]])
        self.assertEqual(
            [c1.id], find_merge_base(self.repo.object_store, [c2.id, c3.id]))

    def test_fast_forward(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 2]])
        self.assertEqual(
            [c2.id], find_merge_base(self.repo.object_store, [c2.id, c3.id]))
