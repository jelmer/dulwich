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

    def test_pop_stash(self) -> None:
        stash = Stash.from_repo(self.repo)

        # Make sure logs directory exists for reflog
        os.makedirs(os.path.join(self.repo.commondir(), "logs"), exist_ok=True)

        # Create a file and add it to the index
        file_path = os.path.join(self.repo_dir, "testfile.txt")
        with open(file_path, "wb") as f:
            f.write(b"test data")
        self.repo.stage(["testfile.txt"])

        # Push to stash
        stash.push(message=b"Test stash message")
        self.assertEqual(1, len(stash))

        # After stash push, the file should be removed from working tree
        # (matching git's behavior)
        self.assertFalse(os.path.exists(file_path))

        # Pop the stash
        stash.pop(0)

        # Verify file is restored
        self.assertTrue(os.path.exists(file_path))
        with open(file_path, "rb") as f:
            self.assertEqual(b"test data", f.read())

        # Verify stash is empty
        self.assertEqual(0, len(stash))

        # Verify the file is in the index
        index = self.repo.open_index()
        self.assertIn(b"testfile.txt", index)

    def test_pop_stash_with_index_changes(self) -> None:
        stash = Stash.from_repo(self.repo)

        # Make sure logs directory exists for reflog
        os.makedirs(os.path.join(self.repo.commondir(), "logs"), exist_ok=True)

        # First commit a file so we have tracked files
        tracked_path = os.path.join(self.repo_dir, "tracked.txt")
        with open(tracked_path, "wb") as f:
            f.write(b"original content")
        self.repo.stage(["tracked.txt"])
        self.repo.do_commit(b"Add tracked file")

        # Modify the tracked file and stage it
        with open(tracked_path, "wb") as f:
            f.write(b"staged changes")
        self.repo.stage(["tracked.txt"])

        # Modify it again but don't stage
        with open(tracked_path, "wb") as f:
            f.write(b"working tree changes")

        # Create a new file and stage it
        new_file_path = os.path.join(self.repo_dir, "new.txt")
        with open(new_file_path, "wb") as f:
            f.write(b"new file content")
        self.repo.stage(["new.txt"])

        # Push to stash
        stash.push(message=b"Test stash with index")
        self.assertEqual(1, len(stash))

        # After stash push, new file should be removed and tracked file reset
        self.assertFalse(os.path.exists(new_file_path))
        with open(tracked_path, "rb") as f:
            self.assertEqual(b"original content", f.read())

        # Pop the stash
        stash.pop(0)

        # Verify tracked file has working tree changes
        self.assertTrue(os.path.exists(tracked_path))
        with open(tracked_path, "rb") as f:
            self.assertEqual(b"working tree changes", f.read())

        # Verify new file is restored
        self.assertTrue(os.path.exists(new_file_path))
        with open(new_file_path, "rb") as f:
            self.assertEqual(b"new file content", f.read())

        # Verify index has the staged changes
        index = self.repo.open_index()
        self.assertIn(b"new.txt", index)
