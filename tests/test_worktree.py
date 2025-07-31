# test_worktree.py -- Tests for dulwich.worktree
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.worktree."""

import os
import shutil
import stat
import tempfile
from unittest import skipIf

from dulwich import porcelain
from dulwich.errors import CommitError
from dulwich.object_store import tree_lookup_path
from dulwich.repo import Repo
from dulwich.worktree import (
    WorkTree,
    add_worktree,
    list_worktrees,
    lock_worktree,
    move_worktree,
    prune_worktrees,
    remove_worktree,
    unlock_worktree,
)

from . import TestCase


class WorkTreeTestCase(TestCase):
    """Base test case for WorkTree tests."""

    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.tempdir, "main")
        self.repo = Repo.init(self.test_dir, mkdir=True)

        # Create initial commit with a file
        with open(os.path.join(self.test_dir, "a"), "wb") as f:
            f.write(b"contents of file a")
        self.repo.get_worktree().stage(["a"])
        self.root_commit = self.repo.get_worktree().commit(
            message=b"Initial commit",
            committer=b"Test Committer <test@nodomain.com>",
            author=b"Test Author <test@nodomain.com>",
            commit_timestamp=12345,
            commit_timezone=0,
            author_timestamp=12345,
            author_timezone=0,
        )
        self.worktree = self.repo.get_worktree()

    def tearDown(self):
        self.repo.close()
        super().tearDown()

    def write_file(self, filename, content):
        """Helper to write a file in the repo."""
        with open(os.path.join(self.test_dir, filename), "wb") as f:
            f.write(content)


class WorkTreeInitTests(TestCase):
    """Tests for WorkTree initialization."""

    def test_init_with_repo_path(self):
        """Test WorkTree initialization with same path as repo."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repo.init(tmpdir)
            worktree = WorkTree(repo, tmpdir)

            self.assertEqual(worktree.path, tmpdir)
            self.assertEqual(worktree._repo, repo)
            self.assertTrue(os.path.isabs(worktree.path))

    def test_init_with_different_path(self):
        """Test WorkTree initialization with different path from repo."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = os.path.join(tmpdir, "repo")
            worktree_path = os.path.join(tmpdir, "worktree")

            os.makedirs(repo_path)
            os.makedirs(worktree_path)

            repo = Repo.init(repo_path)
            worktree = WorkTree(repo, worktree_path)

            self.assertNotEqual(worktree.path, repo.path)
            self.assertEqual(worktree.path, worktree_path)
            self.assertEqual(worktree._repo, repo)
            self.assertTrue(os.path.isabs(worktree.path))

    def test_init_with_bytes_path(self):
        """Test WorkTree initialization with bytes path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repo.init(tmpdir)
            worktree = WorkTree(repo, tmpdir.encode("utf-8"))

            self.assertEqual(worktree.path, tmpdir)
            self.assertIsInstance(worktree.path, str)


class WorkTreeStagingTests(WorkTreeTestCase):
    """Tests for WorkTree staging operations."""

    def test_stage_absolute(self):
        """Test that staging with absolute paths raises ValueError."""
        r = self.repo
        os.remove(os.path.join(r.path, "a"))
        self.assertRaises(ValueError, self.worktree.stage, [os.path.join(r.path, "a")])

    def test_stage_deleted(self):
        """Test staging a deleted file."""
        r = self.repo
        os.remove(os.path.join(r.path, "a"))
        self.worktree.stage(["a"])
        self.worktree.stage(["a"])  # double-stage a deleted path
        self.assertEqual([], list(r.open_index()))

    def test_stage_directory(self):
        """Test staging a directory."""
        r = self.repo
        os.mkdir(os.path.join(r.path, "c"))
        self.worktree.stage(["c"])
        self.assertEqual([b"a"], list(r.open_index()))

    def test_stage_submodule(self):
        """Test staging a submodule."""
        r = self.repo
        s = Repo.init(os.path.join(r.path, "sub"), mkdir=True)
        s.get_worktree().commit(
            message=b"message",
        )
        self.worktree.stage(["sub"])
        self.assertEqual([b"a", b"sub"], list(r.open_index()))


class WorkTreeUnstagingTests(WorkTreeTestCase):
    """Tests for WorkTree unstaging operations."""

    def test_unstage_modify_file_with_dir(self):
        """Test unstaging a modified file in a directory."""
        os.mkdir(os.path.join(self.repo.path, "new_dir"))
        full_path = os.path.join(self.repo.path, "new_dir", "foo")

        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "a") as f:
            f.write("something new")
        self.worktree.unstage(["new_dir/foo"])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": []}, [b"new_dir/foo"], []], status
        )

    def test_unstage_while_no_commit(self):
        """Test unstaging when there are no commits."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], ["foo"]], status)

    def test_unstage_add_file(self):
        """Test unstaging a newly added file."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], ["foo"]], status)

    def test_unstage_modify_file(self):
        """Test unstaging a modified file."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "a") as f:
            f.write("broken")
        porcelain.add(self.repo, paths=[full_path])
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))

        self.assertEqual(
            [{"add": [], "delete": [], "modify": []}, [b"foo"], []], status
        )

    def test_unstage_remove_file(self):
        """Test unstaging a removed file."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        os.remove(full_path)
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": []}, [b"foo"], []], status
        )


class WorkTreeCommitTests(WorkTreeTestCase):
    """Tests for WorkTree commit operations."""

    def test_commit_modified(self):
        """Test committing a modified file."""
        r = self.repo
        with open(os.path.join(r.path, "a"), "wb") as f:
            f.write(b"new contents")
        self.worktree.stage(["a"])
        commit_sha = self.worktree.commit(
            b"modified a",
            committer=b"Test Committer <test@nodomain.com>",
            author=b"Test Author <test@nodomain.com>",
            commit_timestamp=12395,
            commit_timezone=0,
            author_timestamp=12395,
            author_timezone=0,
        )
        self.assertEqual([self.root_commit], r[commit_sha].parents)
        a_mode, a_id = tree_lookup_path(r.get_object, r[commit_sha].tree, b"a")
        self.assertEqual(stat.S_IFREG | 0o644, a_mode)
        self.assertEqual(b"new contents", r[a_id].data)

    @skipIf(not getattr(os, "symlink", None), "Requires symlink support")
    def test_commit_symlink(self):
        """Test committing a symlink."""
        r = self.repo
        os.symlink("a", os.path.join(r.path, "b"))
        self.worktree.stage(["a", "b"])
        commit_sha = self.worktree.commit(
            b"Symlink b",
            committer=b"Test Committer <test@nodomain.com>",
            author=b"Test Author <test@nodomain.com>",
            commit_timestamp=12395,
            commit_timezone=0,
            author_timestamp=12395,
            author_timezone=0,
        )
        self.assertEqual([self.root_commit], r[commit_sha].parents)
        b_mode, b_id = tree_lookup_path(r.get_object, r[commit_sha].tree, b"b")
        self.assertEqual(stat.S_IFLNK, b_mode)
        self.assertEqual(b"a", r[b_id].data)


class WorkTreeResetTests(WorkTreeTestCase):
    """Tests for WorkTree reset operations."""

    def test_reset_index(self):
        """Test resetting the index."""
        # Make some changes and stage them
        with open(os.path.join(self.repo.path, "a"), "wb") as f:
            f.write(b"modified contents")
        self.worktree.stage(["a"])

        # Reset index should restore to HEAD
        self.worktree.reset_index()

        # Check that the working tree file was restored
        with open(os.path.join(self.repo.path, "a"), "rb") as f:
            contents = f.read()
        self.assertEqual(b"contents of file a", contents)


class WorkTreeSparseCheckoutTests(WorkTreeTestCase):
    """Tests for WorkTree sparse checkout operations."""

    def test_get_sparse_checkout_patterns_empty(self):
        """Test getting sparse checkout patterns when file doesn't exist."""
        patterns = self.worktree.get_sparse_checkout_patterns()
        self.assertEqual([], patterns)

    def test_set_sparse_checkout_patterns(self):
        """Test setting sparse checkout patterns."""
        patterns = ["*.py", "docs/"]
        self.worktree.set_sparse_checkout_patterns(patterns)

        # Read back the patterns
        retrieved_patterns = self.worktree.get_sparse_checkout_patterns()
        self.assertEqual(patterns, retrieved_patterns)

    def test_configure_for_cone_mode(self):
        """Test configuring repository for cone mode."""
        self.worktree.configure_for_cone_mode()

        config = self.repo.get_config()
        self.assertEqual(b"true", config.get((b"core",), b"sparseCheckout"))
        self.assertEqual(b"true", config.get((b"core",), b"sparseCheckoutCone"))

    def test_infer_cone_mode_false(self):
        """Test inferring cone mode when not configured."""
        self.assertFalse(self.worktree.infer_cone_mode())

    def test_infer_cone_mode_true(self):
        """Test inferring cone mode when configured."""
        self.worktree.configure_for_cone_mode()
        self.assertTrue(self.worktree.infer_cone_mode())

    def test_set_cone_mode_patterns(self):
        """Test setting cone mode patterns."""
        dirs = ["src", "tests"]
        self.worktree.set_cone_mode_patterns(dirs)

        patterns = self.worktree.get_sparse_checkout_patterns()
        expected = ["/*", "!/*/", "/src/", "/tests/"]
        self.assertEqual(expected, patterns)

    def test_set_cone_mode_patterns_empty(self):
        """Test setting cone mode patterns with empty list."""
        self.worktree.set_cone_mode_patterns([])

        patterns = self.worktree.get_sparse_checkout_patterns()
        expected = ["/*", "!/*/"]
        self.assertEqual(expected, patterns)

    def test_set_cone_mode_patterns_duplicates(self):
        """Test that duplicate patterns are not added."""
        dirs = ["src", "src"]  # duplicate
        self.worktree.set_cone_mode_patterns(dirs)

        patterns = self.worktree.get_sparse_checkout_patterns()
        expected = ["/*", "!/*/", "/src/"]
        self.assertEqual(expected, patterns)

    def test_sparse_checkout_file_path(self):
        """Test getting the sparse checkout file path."""
        expected_path = os.path.join(self.repo.controldir(), "info", "sparse-checkout")
        actual_path = self.worktree._sparse_checkout_file_path()
        self.assertEqual(expected_path, actual_path)


class WorkTreeBackwardCompatibilityTests(WorkTreeTestCase):
    """Tests for backward compatibility of deprecated Repo methods."""

    def test_deprecated_stage_delegates_to_worktree(self):
        """Test that deprecated Repo.stage delegates to WorkTree."""
        with open(os.path.join(self.repo.path, "new_file"), "w") as f:
            f.write("test content")

        # This should show a deprecation warning but still work
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.repo.stage(
                ["new_file"]
            )  # Call deprecated method on Repo, not WorkTree
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

    def test_deprecated_unstage_delegates_to_worktree(self):
        """Test that deprecated Repo.unstage delegates to WorkTree."""
        # This should show a deprecation warning but still work
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.repo.unstage(["a"])  # Call deprecated method on Repo, not WorkTree
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

    def test_deprecated_sparse_checkout_methods(self):
        """Test that deprecated sparse checkout methods delegate to WorkTree."""
        import warnings

        # Test get_sparse_checkout_patterns
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            patterns = (
                self.repo.get_sparse_checkout_patterns()
            )  # Call deprecated method on Repo
            self.assertEqual([], patterns)
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

        # Test set_sparse_checkout_patterns
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.repo.set_sparse_checkout_patterns(
                ["*.py"]
            )  # Call deprecated method on Repo
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

    def test_pre_commit_hook_fail(self):
        """Test that failing pre-commit hook raises CommitError."""
        if os.name != "posix":
            self.skipTest("shell hook tests requires POSIX shell")

        # Create a failing pre-commit hook
        hooks_dir = os.path.join(self.repo.controldir(), "hooks")
        os.makedirs(hooks_dir, exist_ok=True)
        hook_path = os.path.join(hooks_dir, "pre-commit")

        with open(hook_path, "w") as f:
            f.write("#!/bin/sh\nexit 1\n")
        os.chmod(hook_path, 0o755)

        # Try to commit
        worktree = self.repo.get_worktree()
        with self.assertRaises(CommitError):
            worktree.commit(b"No message")

    def write_file(self, filename, content):
        """Helper to write a file in the repo."""
        with open(os.path.join(self.test_dir, filename), "wb") as f:
            f.write(content)


class WorkTreeOperationsTests(WorkTreeTestCase):
    """Tests for worktree operations like add, list, remove."""

    def test_list_worktrees_single(self) -> None:
        """Test listing worktrees when only main worktree exists."""
        worktrees = list_worktrees(self.repo)
        self.assertEqual(len(worktrees), 1)
        self.assertEqual(worktrees[0].path, self.repo.path)
        self.assertEqual(worktrees[0].bare, False)
        self.assertIsNotNone(worktrees[0].head)
        self.assertIsNotNone(worktrees[0].branch)

    def test_add_worktree_new_branch(self) -> None:
        """Test adding a worktree with a new branch."""
        # Create a commit first
        worktree = self.repo.get_worktree()
        self.write_file("test.txt", b"test content")
        worktree.stage(["test.txt"])
        commit_id = worktree.commit(message=b"Initial commit")

        # Add a new worktree
        wt_path = os.path.join(self.tempdir, "new-worktree")
        add_worktree(self.repo, wt_path, branch=b"feature-branch")

        # Verify worktree was created
        self.assertTrue(os.path.exists(wt_path))
        self.assertTrue(os.path.exists(os.path.join(wt_path, ".git")))

        # Verify it appears in the list
        worktrees = list_worktrees(self.repo)
        self.assertEqual(len(worktrees), 2)

        # Find the new worktree in the list
        new_wt = None
        for wt in worktrees:
            if wt.path == wt_path:
                new_wt = wt
                break

        self.assertIsNotNone(new_wt)
        self.assertEqual(new_wt.branch, b"refs/heads/feature-branch")
        self.assertEqual(new_wt.head, commit_id)
        self.assertFalse(new_wt.detached)

    def test_add_worktree_detached(self) -> None:
        """Test adding a worktree with detached HEAD."""
        # Create a commit
        worktree = self.repo.get_worktree()
        self.write_file("test.txt", b"test content")
        worktree.stage(["test.txt"])
        commit_id = worktree.commit(message=b"Initial commit")

        # Add a detached worktree
        wt_path = os.path.join(self.tempdir, "detached-worktree")
        add_worktree(self.repo, wt_path, commit=commit_id, detach=True)

        # Verify it's detached
        worktrees = list_worktrees(self.repo)
        self.assertEqual(len(worktrees), 2)

        for wt in worktrees:
            if wt.path == wt_path:
                self.assertTrue(wt.detached)
                self.assertIsNone(wt.branch)
                self.assertEqual(wt.head, commit_id)

    def test_add_worktree_existing_path(self) -> None:
        """Test that adding a worktree to existing path fails."""
        wt_path = os.path.join(self.tempdir, "existing")
        os.mkdir(wt_path)

        with self.assertRaises(ValueError) as cm:
            add_worktree(self.repo, wt_path)
        self.assertIn("Path already exists", str(cm.exception))

    def test_add_worktree_branch_already_checked_out(self) -> None:
        """Test that checking out same branch in multiple worktrees fails."""
        # Create initial commit
        worktree = self.repo.get_worktree()
        self.write_file("test.txt", b"test content")
        worktree.stage(["test.txt"])
        worktree.commit(message=b"Initial commit")

        # First worktree should succeed with a new branch
        wt_path1 = os.path.join(self.tempdir, "wt1")
        add_worktree(self.repo, wt_path1, branch=b"feature")

        # Second worktree with same branch should fail
        wt_path2 = os.path.join(self.tempdir, "wt2")
        with self.assertRaises(ValueError) as cm:
            add_worktree(self.repo, wt_path2, branch=b"feature")
        self.assertIn("already checked out", str(cm.exception))

        # But should work with force=True
        add_worktree(self.repo, wt_path2, branch=b"feature", force=True)

    def test_remove_worktree(self) -> None:
        """Test removing a worktree."""
        # Create a worktree
        wt_path = os.path.join(self.tempdir, "to-remove")
        add_worktree(self.repo, wt_path)

        # Verify it exists
        self.assertTrue(os.path.exists(wt_path))
        self.assertEqual(len(list_worktrees(self.repo)), 2)

        # Remove it
        remove_worktree(self.repo, wt_path)

        # Verify it's gone
        self.assertFalse(os.path.exists(wt_path))
        self.assertEqual(len(list_worktrees(self.repo)), 1)

    def test_remove_main_worktree_fails(self) -> None:
        """Test that removing the main worktree fails."""
        with self.assertRaises(ValueError) as cm:
            remove_worktree(self.repo, self.repo.path)
        self.assertIn("Cannot remove the main working tree", str(cm.exception))

    def test_remove_nonexistent_worktree(self) -> None:
        """Test that removing non-existent worktree fails."""
        with self.assertRaises(ValueError) as cm:
            remove_worktree(self.repo, "/nonexistent/path")
        self.assertIn("Worktree not found", str(cm.exception))

    def test_lock_unlock_worktree(self) -> None:
        """Test locking and unlocking a worktree."""
        # Create a worktree
        wt_path = os.path.join(self.tempdir, "lockable")
        add_worktree(self.repo, wt_path)

        # Lock it
        lock_worktree(self.repo, wt_path, reason="Testing lock")

        # Verify it's locked
        worktrees = list_worktrees(self.repo)
        for wt in worktrees:
            if wt.path == wt_path:
                self.assertTrue(wt.locked)

        # Try to remove locked worktree (should fail)
        with self.assertRaises(ValueError) as cm:
            remove_worktree(self.repo, wt_path)
        self.assertIn("locked", str(cm.exception))

        # Unlock it
        unlock_worktree(self.repo, wt_path)

        # Verify it's unlocked
        worktrees = list_worktrees(self.repo)
        for wt in worktrees:
            if wt.path == wt_path:
                self.assertFalse(wt.locked)

        # Now removal should work
        remove_worktree(self.repo, wt_path)

    def test_prune_worktrees(self) -> None:
        """Test pruning worktrees."""
        # Create a worktree
        wt_path = os.path.join(self.tempdir, "to-prune")
        add_worktree(self.repo, wt_path)

        # Manually remove the worktree directory
        shutil.rmtree(wt_path)

        # Verify it still shows up as prunable
        worktrees = list_worktrees(self.repo)
        prunable_count = sum(1 for wt in worktrees if wt.prunable)
        self.assertEqual(prunable_count, 1)

        # Prune it
        pruned = prune_worktrees(self.repo)
        self.assertEqual(len(pruned), 1)

        # Verify it's gone from the list
        worktrees = list_worktrees(self.repo)
        self.assertEqual(len(worktrees), 1)

    def test_prune_dry_run(self) -> None:
        """Test prune with dry_run doesn't remove anything."""
        # Create and manually remove a worktree
        wt_path = os.path.join(self.tempdir, "dry-run-test")
        add_worktree(self.repo, wt_path)
        shutil.rmtree(wt_path)

        # Dry run should report but not remove
        pruned = prune_worktrees(self.repo, dry_run=True)
        self.assertEqual(len(pruned), 1)

        # Worktree should still be in list
        worktrees = list_worktrees(self.repo)
        self.assertEqual(len(worktrees), 2)

    def test_prune_locked_worktree_not_pruned(self) -> None:
        """Test that locked worktrees are not pruned."""
        # Create and lock a worktree
        wt_path = os.path.join(self.tempdir, "locked-prune")
        add_worktree(self.repo, wt_path)
        lock_worktree(self.repo, wt_path)

        # Remove the directory
        shutil.rmtree(wt_path)

        # Prune should not remove locked worktree
        pruned = prune_worktrees(self.repo)
        self.assertEqual(len(pruned), 0)

        # Worktree should still be in list
        worktrees = list_worktrees(self.repo)
        self.assertEqual(len(worktrees), 2)

    def test_move_worktree(self) -> None:
        """Test moving a worktree."""
        # Create a worktree
        wt_path = os.path.join(self.tempdir, "to-move")
        add_worktree(self.repo, wt_path)

        # Create a file in the worktree
        test_file = os.path.join(wt_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        # Move it
        new_path = os.path.join(self.tempdir, "moved")
        move_worktree(self.repo, wt_path, new_path)

        # Verify old path doesn't exist
        self.assertFalse(os.path.exists(wt_path))

        # Verify new path exists with contents
        self.assertTrue(os.path.exists(new_path))
        self.assertTrue(os.path.exists(os.path.join(new_path, "test.txt")))

        # Verify it's in the list at new location
        worktrees = list_worktrees(self.repo)
        paths = [wt.path for wt in worktrees]
        self.assertIn(new_path, paths)
        self.assertNotIn(wt_path, paths)

    def test_move_main_worktree_fails(self) -> None:
        """Test that moving the main worktree fails."""
        new_path = os.path.join(self.tempdir, "new-main")
        with self.assertRaises(ValueError) as cm:
            move_worktree(self.repo, self.repo.path, new_path)
        self.assertIn("Cannot move the main working tree", str(cm.exception))

    def test_move_to_existing_path_fails(self) -> None:
        """Test that moving to an existing path fails."""
        # Create a worktree
        wt_path = os.path.join(self.tempdir, "worktree")
        add_worktree(self.repo, wt_path)

        # Create target directory
        new_path = os.path.join(self.tempdir, "existing")
        os.makedirs(new_path)

        with self.assertRaises(ValueError) as cm:
            move_worktree(self.repo, wt_path, new_path)
        self.assertIn("Path already exists", str(cm.exception))
