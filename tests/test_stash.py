# test_stash.py -- tests for stash
# Copyright (C) 2018 Jelmer Vernooij <jelmer@jelmer.uk>
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
#

"""Tests for stashes."""

import os
import shutil
import tempfile

from dulwich.objects import Blob, Tree
from dulwich.repo import Repo
from dulwich.stash import DEFAULT_STASH_REF, Stash

from . import TestCase


class StashTests(TestCase):
    """Tests for stash."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.repo_dir = os.path.join(self.test_dir, "repo")
        os.makedirs(self.repo_dir)
        self.repo = Repo.init(self.repo_dir)

        # Create initial commit so we can have a HEAD
        blob = Blob()
        blob.data = b"initial data"
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"initial.txt", 0o100644, blob.id)
        tree_id = self.repo.object_store.add_object(tree)

        self.commit_id = self.repo.do_commit(b"Initial commit", tree=tree_id)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_obtain(self) -> None:
        stash = Stash.from_repo(self.repo)
        self.assertIsInstance(stash, Stash)

    def test_empty_stash(self) -> None:
        stash = Stash.from_repo(self.repo)
        # Make sure logs directory exists for reflog
        os.makedirs(os.path.join(self.repo.commondir(), "logs"), exist_ok=True)

        self.assertEqual(0, len(stash))
        self.assertEqual([], list(stash.stashes()))

    def test_push_stash(self) -> None:
        stash = Stash.from_repo(self.repo)

        # Make sure logs directory exists for reflog
        os.makedirs(os.path.join(self.repo.commondir(), "logs"), exist_ok=True)

        # Create a file and add it to the index
        file_path = os.path.join(self.repo_dir, "testfile.txt")
        with open(file_path, "wb") as f:
            f.write(b"test data")
        self.repo.stage(["testfile.txt"])

        # Push to stash
        commit_id = stash.push(message=b"Test stash message")
        self.assertIsNotNone(commit_id)

        # Verify stash was created
        self.assertEqual(1, len(stash))

        # Verify stash entry
        entry = stash[0]
        self.assertEqual(commit_id, entry.new_sha)
        self.assertTrue(b"Test stash message" in entry.message)

    def test_drop_stash(self) -> None:
        stash = Stash.from_repo(self.repo)

        # Make sure logs directory exists for reflog
        logs_dir = os.path.join(self.repo.commondir(), "logs")
        os.makedirs(logs_dir, exist_ok=True)

        # Create a couple of files and stash them
        file1_path = os.path.join(self.repo_dir, "testfile1.txt")
        with open(file1_path, "wb") as f:
            f.write(b"test data 1")
        self.repo.stage(["testfile1.txt"])
        commit_id1 = stash.push(message=b"Test stash 1")

        file2_path = os.path.join(self.repo_dir, "testfile2.txt")
        with open(file2_path, "wb") as f:
            f.write(b"test data 2")
        self.repo.stage(["testfile2.txt"])
        stash.push(message=b"Test stash 2")

        self.assertEqual(2, len(stash))

        # Drop the newest stash
        stash.drop(0)
        self.assertEqual(1, len(stash))
        self.assertEqual(commit_id1, stash[0].new_sha)

        # Drop the remaining stash
        stash.drop(0)
        self.assertEqual(0, len(stash))
        self.assertNotIn(DEFAULT_STASH_REF, self.repo.refs)

    def test_custom_ref(self) -> None:
        custom_ref = b"refs/custom_stash"
        stash = Stash(self.repo, ref=custom_ref)
        self.assertEqual(custom_ref, stash._ref)
