# test_worktree.py -- tests for porcelain worktree functions
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
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

"""Tests for porcelain worktree functions."""

import os
import shutil
import tempfile

from dulwich import porcelain
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo

from .. import TestCase


class WorktreeErrorTests(TestCase):
    """Tests for worktree function error handling."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)

        # Create a simple commit
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"Test commit"
        self.repo.object_store.add_object(commit)

        self.repo.refs[b"refs/heads/main"] = commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super().tearDown()

    def test_worktree_add_without_path(self) -> None:
        """Test worktree_add raises ValueError when path is None."""
        with self.assertRaises(ValueError) as cm:
            porcelain.worktree_add(self.repo, path=None)

        self.assertEqual("Path is required for worktree add", str(cm.exception))

    def test_worktree_remove_without_path(self) -> None:
        """Test worktree_remove raises ValueError when path is None."""
        with self.assertRaises(ValueError) as cm:
            porcelain.worktree_remove(self.repo, path=None)

        self.assertEqual("Path is required for worktree remove", str(cm.exception))

    def test_worktree_lock_without_path(self) -> None:
        """Test worktree_lock raises ValueError when path is None."""
        with self.assertRaises(ValueError) as cm:
            porcelain.worktree_lock(self.repo, path=None)

        self.assertEqual("Path is required for worktree lock", str(cm.exception))

    def test_worktree_unlock_without_path(self) -> None:
        """Test worktree_unlock raises ValueError when path is None."""
        with self.assertRaises(ValueError) as cm:
            porcelain.worktree_unlock(self.repo, path=None)

        self.assertEqual("Path is required for worktree unlock", str(cm.exception))

    def test_worktree_move_without_old_path(self) -> None:
        """Test worktree_move raises ValueError when old_path is None."""
        new_path = os.path.join(self.test_dir, "new_worktree")

        with self.assertRaises(ValueError) as cm:
            porcelain.worktree_move(self.repo, old_path=None, new_path=new_path)

        self.assertEqual(
            "Both old_path and new_path are required for worktree move",
            str(cm.exception),
        )

    def test_worktree_move_without_new_path(self) -> None:
        """Test worktree_move raises ValueError when new_path is None."""
        old_path = os.path.join(self.test_dir, "old_worktree")

        with self.assertRaises(ValueError) as cm:
            porcelain.worktree_move(self.repo, old_path=old_path, new_path=None)

        self.assertEqual(
            "Both old_path and new_path are required for worktree move",
            str(cm.exception),
        )

    def test_worktree_move_without_both_paths(self) -> None:
        """Test worktree_move raises ValueError when both paths are None."""
        with self.assertRaises(ValueError) as cm:
            porcelain.worktree_move(self.repo, old_path=None, new_path=None)

        self.assertEqual(
            "Both old_path and new_path are required for worktree move",
            str(cm.exception),
        )

    def test_worktree_repair_with_no_paths(self) -> None:
        """Test worktree_repair with paths=None (repairs all)."""
        # Should not raise, just return empty list if no worktrees to repair
        result = porcelain.worktree_repair(self.repo, paths=None)
        self.assertIsInstance(result, list)
