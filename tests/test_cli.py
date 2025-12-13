#!/usr/bin/env python
# test_cli.py -- tests for dulwich.cli
# vim: expandtab
#
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

"""Tests for dulwich.cli."""

import io
import os
import shutil
import sys
import tempfile
import unittest
from unittest import skipIf
from unittest.mock import MagicMock, patch

from dulwich import cli
from dulwich.cli import (
    AutoFlushBinaryIOWrapper,
    AutoFlushTextIOWrapper,
    _should_auto_flush,
    detect_terminal_width,
    format_bytes,
    launch_editor,
    write_columns,
)
from dulwich.repo import Repo
from dulwich.tests.utils import (
    build_commit_graph,
)

from . import TestCase


class DulwichCliTestCase(TestCase):
    """Base class for CLI tests."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, "repo")
        os.mkdir(self.repo_path)
        self.repo = Repo.init(self.repo_path)
        self.addCleanup(self.repo.close)

    def _run_cli(self, *args, stdout_stream=None):
        """Run CLI command and capture output."""

        class MockStream:
            def __init__(self):
                self._buffer = io.BytesIO()
                self.buffer = self._buffer

            def write(self, data):
                if isinstance(data, bytes):
                    self._buffer.write(data)
                else:
                    self._buffer.write(data.encode("utf-8"))

            def getvalue(self):
                value = self._buffer.getvalue()
                try:
                    return value.decode("utf-8")
                except UnicodeDecodeError:
                    return value

            def __getattr__(self, name):
                return getattr(self._buffer, name)

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_cwd = os.getcwd()

        try:
            # Use custom stdout_stream if provided, otherwise use MockStream
            if stdout_stream:
                sys.stdout = stdout_stream
                if not hasattr(sys.stdout, "buffer"):
                    sys.stdout.buffer = sys.stdout
            else:
                sys.stdout = MockStream()

            sys.stderr = MockStream()

            os.chdir(self.repo_path)
            result = cli.main(list(args))
            return result, sys.stdout.getvalue(), sys.stderr.getvalue()
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            os.chdir(old_cwd)


class InitCommandTest(DulwichCliTestCase):
    """Tests for init command."""

    def test_init_basic(self):
        # Create a new directory for init
        new_repo_path = os.path.join(self.test_dir, "new_repo")
        _result, _stdout, _stderr = self._run_cli("init", new_repo_path)
        self.assertTrue(os.path.exists(os.path.join(new_repo_path, ".git")))

    def test_init_bare(self):
        # Create a new directory for bare repo
        bare_repo_path = os.path.join(self.test_dir, "bare_repo")
        _result, _stdout, _stderr = self._run_cli("init", "--bare", bare_repo_path)
        self.assertTrue(os.path.exists(os.path.join(bare_repo_path, "HEAD")))
        self.assertFalse(os.path.exists(os.path.join(bare_repo_path, ".git")))

    def test_init_objectformat_sha256(self) -> None:
        # Create a new directory for init with SHA-256
        new_repo_path = os.path.join(self.test_dir, "sha256_repo")
        _result, _stdout, _stderr = self._run_cli(
            "init", "--objectformat=sha256", new_repo_path
        )
        self.assertTrue(os.path.exists(os.path.join(new_repo_path, ".git")))
        # Verify the object format
        repo = Repo(new_repo_path)
        self.addCleanup(repo.close)
        config = repo.get_config()
        self.assertEqual(b"sha256", config.get((b"extensions",), b"objectformat"))

    def test_init_objectformat_sha1(self) -> None:
        # Create a new directory for init with SHA-1
        new_repo_path = os.path.join(self.test_dir, "sha1_repo")
        _result, _stdout, _stderr = self._run_cli(
            "init", "--objectformat=sha1", new_repo_path
        )
        self.assertTrue(os.path.exists(os.path.join(new_repo_path, ".git")))
        # SHA-1 is the default, so objectformat should not be set
        repo = Repo(new_repo_path)
        self.addCleanup(repo.close)
        config = repo.get_config()
        # The extensions section may not exist at all for SHA-1
        if config.has_section((b"extensions",)):
            object_format = config.get((b"extensions",), b"objectformat")
            self.assertNotEqual(b"sha256", object_format)
        # If the section doesn't exist, that's also fine (SHA-1 is default)

    def test_init_bare_objectformat_sha256(self) -> None:
        # Create a bare repo with SHA-256
        bare_repo_path = os.path.join(self.test_dir, "bare_sha256_repo")
        _result, _stdout, _stderr = self._run_cli(
            "init", "--bare", "--objectformat=sha256", bare_repo_path
        )
        self.assertTrue(os.path.exists(os.path.join(bare_repo_path, "HEAD")))
        self.assertFalse(os.path.exists(os.path.join(bare_repo_path, ".git")))
        # Verify the object format
        repo = Repo(bare_repo_path)
        self.addCleanup(repo.close)
        config = repo.get_config()
        self.assertEqual(b"sha256", config.get((b"extensions",), b"objectformat"))


class HelperFunctionsTest(TestCase):
    """Tests for CLI helper functions."""

    def test_format_bytes(self):
        self.assertEqual("0.0 B", format_bytes(0))
        self.assertEqual("100.0 B", format_bytes(100))
        self.assertEqual("1.0 KB", format_bytes(1024))
        self.assertEqual("1.5 KB", format_bytes(1536))
        self.assertEqual("1.0 MB", format_bytes(1024 * 1024))
        self.assertEqual("1.0 GB", format_bytes(1024 * 1024 * 1024))
        self.assertEqual("1.0 TB", format_bytes(1024 * 1024 * 1024 * 1024))

    def test_launch_editor_with_cat(self):
        """Test launch_editor by using cat as the editor."""
        self.overrideEnv("GIT_EDITOR", "cat")
        result = launch_editor(b"Test template content")
        self.assertEqual(b"Test template content", result)

    def test_parse_time_to_timestamp(self):
        """Test parsing time specifications to Unix timestamps."""
        import time

        from dulwich.cli import parse_time_to_timestamp

        # Test special values
        self.assertEqual(0, parse_time_to_timestamp("never"))
        future_time = parse_time_to_timestamp("all")
        self.assertGreater(future_time, int(time.time()))

        # Test Unix timestamp
        self.assertEqual(1234567890, parse_time_to_timestamp("1234567890"))

        # Test relative time
        now = int(time.time())
        result = parse_time_to_timestamp("1 day ago")
        expected = now - 86400
        # Allow 2 second tolerance for test execution time
        self.assertAlmostEqual(expected, result, delta=2)


class AddCommandTest(DulwichCliTestCase):
    """Tests for add command."""

    def test_add_single_file(self):
        # Create a file to add
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        _result, _stdout, _stderr = self._run_cli("add", "test.txt")
        # Check that file is in index
        self.assertIn(b"test.txt", self.repo.open_index())

    def test_add_multiple_files(self):
        # Create multiple files
        for i in range(3):
            test_file = os.path.join(self.repo_path, f"test{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"content {i}")

        _result, _stdout, _stderr = self._run_cli(
            "add", "test0.txt", "test1.txt", "test2.txt"
        )
        index = self.repo.open_index()
        self.assertIn(b"test0.txt", index)
        self.assertIn(b"test1.txt", index)
        self.assertIn(b"test2.txt", index)


class RmCommandTest(DulwichCliTestCase):
    """Tests for rm command."""

    def test_rm_file(self):
        # Create, add and commit a file first
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Add test file")

        # Now remove it from index and working directory
        _result, _stdout, _stderr = self._run_cli("rm", "test.txt")
        # Check that file is not in index
        self.assertNotIn(b"test.txt", self.repo.open_index())


class CommitCommandTest(DulwichCliTestCase):
    """Tests for commit command."""

    def test_commit_basic(self):
        # Create and add a file
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")

        # Commit
        _result, _stdout, _stderr = self._run_cli("commit", "--message=Initial commit")
        # Check that HEAD points to a commit
        self.assertIsNotNone(self.repo.head())

    def test_commit_all_flag(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Modify the file (don't stage it)
        with open(test_file, "w") as f:
            f.write("modified content")

        # Create another file and don't add it (untracked)
        untracked_file = os.path.join(self.repo_path, "untracked.txt")
        with open(untracked_file, "w") as f:
            f.write("untracked content")

        # Commit with -a flag should stage and commit the modified file,
        # but not the untracked file
        _result, _stdout, _stderr = self._run_cli(
            "commit", "-a", "--message=Modified commit"
        )
        self.assertIsNotNone(self.repo.head())

        # Check that the modification was committed
        with open(test_file) as f:
            content = f.read()
        self.assertEqual(content, "modified content")

        # Check that untracked file is still untracked
        self.assertTrue(os.path.exists(untracked_file))

    def test_commit_all_flag_no_changes(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Try to commit with -a when there are no changes
        # This should still work (git allows this)
        _result, _stdout, _stderr = self._run_cli(
            "commit", "-a", "--message=No changes commit"
        )
        self.assertIsNotNone(self.repo.head())

    def test_commit_all_flag_multiple_files(self):
        # Create initial commit with multiple files
        file1 = os.path.join(self.repo_path, "file1.txt")
        file2 = os.path.join(self.repo_path, "file2.txt")

        with open(file1, "w") as f:
            f.write("content1")
        with open(file2, "w") as f:
            f.write("content2")

        self._run_cli("add", "file1.txt", "file2.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Modify both files
        with open(file1, "w") as f:
            f.write("modified content1")
        with open(file2, "w") as f:
            f.write("modified content2")

        # Create an untracked file
        untracked_file = os.path.join(self.repo_path, "untracked.txt")
        with open(untracked_file, "w") as f:
            f.write("untracked content")

        # Commit with -a should stage both modified files but not untracked
        _result, _stdout, _stderr = self._run_cli(
            "commit", "-a", "--message=Modified both files"
        )
        self.assertIsNotNone(self.repo.head())

        # Verify modifications were committed
        with open(file1) as f:
            self.assertEqual(f.read(), "modified content1")
        with open(file2) as f:
            self.assertEqual(f.read(), "modified content2")

        # Verify untracked file still exists
        self.assertTrue(os.path.exists(untracked_file))

    @patch("dulwich.cli.launch_editor")
    def test_commit_editor_success(self, mock_editor):
        """Test commit with editor when user provides a message."""
        # Create and add a file
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")

        # Mock editor to return a commit message
        mock_editor.return_value = b"My commit message\n\n# This is a comment\n"

        # Commit without --message flag
        _result, _stdout, _stderr = self._run_cli("commit")

        # Check that HEAD points to a commit
        commit = self.repo[self.repo.head()]
        self.assertEqual(commit.message, b"My commit message")

        # Verify editor was called
        mock_editor.assert_called_once()

    @patch("dulwich.cli.launch_editor")
    def test_commit_editor_empty_message(self, mock_editor):
        """Test commit with editor when user provides empty message."""
        # Create and add a file
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")

        # Mock editor to return only comments
        mock_editor.return_value = b"# All lines are comments\n# No actual message\n"

        # Commit without --message flag should fail with exit code 1
        result, _stdout, _stderr = self._run_cli("commit")
        self.assertEqual(result, 1)

    @patch("dulwich.cli.launch_editor")
    def test_commit_editor_unchanged_template(self, mock_editor):
        """Test commit with editor when user doesn't change the template."""
        # Create and add a file
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")

        # Mock editor to return the exact template that was passed to it
        def return_unchanged_template(template):
            return template

        mock_editor.side_effect = return_unchanged_template

        # Commit without --message flag should fail with exit code 1
        result, _stdout, _stderr = self._run_cli("commit")
        self.assertEqual(result, 1)


class LogCommandTest(DulwichCliTestCase):
    """Tests for log command."""

    def test_log_empty_repo(self):
        _result, _stdout, _stderr = self._run_cli("log")
        # Empty repo should not crash

    def test_log_with_commits(self):
        # Create some commits
        _c1, _c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        _result, stdout, _stderr = self._run_cli("log")
        self.assertIn("Commit 3", stdout)
        self.assertIn("Commit 2", stdout)
        self.assertIn("Commit 1", stdout)

    def test_log_reverse(self):
        # Create some commits
        _c1, _c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        _result, stdout, _stderr = self._run_cli("log", "--reverse")
        # Check order - commit 1 should appear before commit 3
        pos1 = stdout.index("Commit 1")
        pos3 = stdout.index("Commit 3")
        self.assertLess(pos1, pos3)


class StatusCommandTest(DulwichCliTestCase):
    """Tests for status command."""

    def test_status_empty(self):
        _result, _stdout, _stderr = self._run_cli("status")
        # Should not crash on empty repo

    def test_status_with_untracked(self):
        # Create an untracked file
        test_file = os.path.join(self.repo_path, "untracked.txt")
        with open(test_file, "w") as f:
            f.write("untracked content")

        _result, stdout, _stderr = self._run_cli("status")
        self.assertIn("Untracked files:", stdout)
        self.assertIn("untracked.txt", stdout)

    def test_status_with_column(self):
        # Create multiple untracked files
        for i in range(5):
            test_file = os.path.join(self.repo_path, f"file{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"content {i}")

        _result, stdout, _stderr = self._run_cli("status", "--column")
        self.assertIn("Untracked files:", stdout)
        # Check that files are present in output
        self.assertIn("file0.txt", stdout)
        self.assertIn("file1.txt", stdout)
        # With column format, multiple files should appear on same line
        # (at least for 5 short filenames)
        lines = stdout.split("\n")
        untracked_section = False
        for line in lines:
            if "Untracked files:" in line:
                untracked_section = True
            if untracked_section and "file" in line:
                # At least one line should contain multiple files
                if line.count("file") > 1:
                    return  # Test passes
        # If we get here and have multiple files, column formatting worked
        # (even if each is on its own line due to terminal width)


class BranchCommandTest(DulwichCliTestCase):
    """Tests for branch command."""

    def test_branch_create(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Create branch
        _result, _stdout, _stderr = self._run_cli("branch", "test-branch")
        self.assertIn(b"refs/heads/test-branch", self.repo.refs.keys())

    def test_branch_delete(self):
        # Create initial commit and branch
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")
        self._run_cli("branch", "test-branch")

        # Delete branch
        _result, _stdout, _stderr = self._run_cli("branch", "-d", "test-branch")
        self.assertNotIn(b"refs/heads/test-branch", self.repo.refs.keys())

    def test_branch_list_all(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Create local test branches
        self._run_cli("branch", "feature-1")
        self._run_cli("branch", "feature-2")

        # Setup a remote and create remote branches
        self.repo.refs[b"refs/remotes/origin/master"] = self.repo.refs[
            b"refs/heads/master"
        ]
        self.repo.refs[b"refs/remotes/origin/feature-remote"] = self.repo.refs[
            b"refs/heads/master"
        ]

        # Test --all listing
        result, stdout, _stderr = self._run_cli("branch", "--all")
        self.assertEqual(result, 0)

        expected_branches = {
            "feature-1",  # local branch
            "feature-2",  # local branch
            "master",  # local branch
            "origin/master",  # remote branch
            "origin/feature-remote",  # remote branch
        }
        lines = [line.strip() for line in stdout.splitlines()]

        # All branches from stdout
        all_branches = set(line for line in lines)
        self.assertEqual(all_branches, expected_branches)

    def test_branch_list_merged(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        master_sha = self.repo.refs[b"refs/heads/master"]

        # Create a merged branch (points to same commit as master)
        self.repo.refs[b"refs/heads/merged-branch"] = master_sha

        # Create a new branch with different content (not merged)
        test_file2 = os.path.join(self.repo_path, "test2.txt")
        with open(test_file2, "w") as f:
            f.write("test2")
        self._run_cli("add", "test2.txt")
        self._run_cli("commit", "--message=New branch commit")

        new_branch_sha = self.repo.refs[b"HEAD"]

        # Switch back to master
        self.repo.refs[b"HEAD"] = master_sha

        # Create a non-merged branch that points to the new branch commit
        self.repo.refs[b"refs/heads/non-merged-branch"] = new_branch_sha

        # Test --merged listing
        result, stdout, _stderr = self._run_cli("branch", "--merged")
        self.assertEqual(result, 0)

        branches = [line.strip() for line in stdout.splitlines()]
        expected_branches = {"master", "merged-branch"}
        self.assertEqual(set(branches), expected_branches)

    def test_branch_list_no_merged(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        master_sha = self.repo.refs[b"refs/heads/master"]

        # Create a merged branch (points to same commit as master)
        self.repo.refs[b"refs/heads/merged-branch"] = master_sha

        # Create a new branch with different content (not merged)
        test_file2 = os.path.join(self.repo_path, "test2.txt")
        with open(test_file2, "w") as f:
            f.write("test2")
        self._run_cli("add", "test2.txt")
        self._run_cli("commit", "--message=New branch commit")
        new_branch_sha = self.repo.refs[b"HEAD"]

        # Switch back to master
        self.repo.refs[b"HEAD"] = master_sha

        # Create a non-merged branch that points to the new branch commit
        self.repo.refs[b"refs/heads/non-merged-branch"] = new_branch_sha

        # Test --no-merged listing
        result, stdout, _stderr = self._run_cli("branch", "--no-merged")
        self.assertEqual(result, 0)

        branches = [line.strip() for line in stdout.splitlines()]
        expected_branches = {"non-merged-branch"}

        self.assertEqual(set(branches), expected_branches)

    def test_branch_list_remotes(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Setup a remote and create remote branches
        self.repo.refs[b"refs/remotes/origin/master"] = self.repo.refs[
            b"refs/heads/master"
        ]
        self.repo.refs[b"refs/remotes/origin/feature-remote-1"] = self.repo.refs[
            b"refs/heads/master"
        ]
        self.repo.refs[b"refs/remotes/origin/feature-remote-2"] = self.repo.refs[
            b"refs/heads/master"
        ]

        # Test --remotes listing
        result, stdout, _stderr = self._run_cli("branch", "--remotes")
        self.assertEqual(result, 0)

        branches = [line.strip() for line in stdout.splitlines()]
        expected_branches = [
            "origin/feature-remote-1",
            "origin/feature-remote-2",
            "origin/master",
        ]

        self.assertEqual(branches, expected_branches)

    def test_branch_list_contains(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        initial_commit_sha = self.repo.refs[b"HEAD"]

        # Create first branch from initial commit
        self._run_cli("branch", "branch-1")

        # Make a new commit on master
        test_file2 = os.path.join(self.repo_path, "test2.txt")
        with open(test_file2, "w") as f:
            f.write("test2")
        self._run_cli("add", "test2.txt")
        self._run_cli("commit", "--message=Second commit")

        second_commit_sha = self.repo.refs[b"HEAD"]

        # Create second branch from current master (contains both commits)
        self._run_cli("branch", "branch-2")

        # Create third branch that doesn't contain the second commit
        # Switch to initial commit and create branch from there
        self.repo.refs[b"HEAD"] = initial_commit_sha
        self._run_cli("branch", "branch-3")

        # Switch back to master
        self.repo.refs[b"HEAD"] = second_commit_sha

        # Test --contains with second commit (should include master and branch-2)
        result, stdout, stderr = self._run_cli(
            "branch", "--contains", second_commit_sha.decode()
        )
        self.assertEqual(result, 0)

        branches = [line.strip() for line in stdout.splitlines()]
        expected_branches = {"master", "branch-2"}
        self.assertEqual(set(branches), expected_branches)

        # Test --contains with initial commit (should include all branches)
        result, stdout, stderr = self._run_cli(
            "branch", "--contains", initial_commit_sha.decode()
        )
        self.assertEqual(result, 0)

        branches = [line.strip() for line in stdout.splitlines()]
        expected_branches = {"master", "branch-1", "branch-2", "branch-3"}
        self.assertEqual(set(branches), expected_branches)

        # Test --contains without argument (uses HEAD, which is second commit)
        result, stdout, stderr = self._run_cli("branch", "--contains")
        self.assertEqual(result, 0)

        branches = [line.strip() for line in stdout.splitlines()]
        expected_branches = {"master", "branch-2"}
        self.assertEqual(set(branches), expected_branches)

        # Test with invalid commit hash
        result, stdout, stderr = self._run_cli("branch", "--contains", "invalid123")
        self.assertNotEqual(result, 0)
        self.assertIn("error: object name invalid123 not found", stderr)

    def test_branch_list_column(self):
        """Test branch --column formatting"""
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        self._run_cli("branch", "feature-1")
        self._run_cli("branch", "feature-2")
        self._run_cli("branch", "feature-3")

        # Run branch --column
        result, stdout, _stderr = self._run_cli("branch", "--all", "--column")
        self.assertEqual(result, 0)

        expected = ["feature-1", "feature-2", "feature-3"]

        for branch in expected:
            self.assertIn(branch, stdout)

        multiple_columns = any(
            sum(branch in line for branch in expected) > 1
            for line in stdout.strip().split("\n")
        )
        self.assertTrue(multiple_columns)

    def test_branch_list_flag(self):
        # Create an initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Create local branches
        self._run_cli("branch", "feature-1")
        self._run_cli("branch", "feature-2")
        self._run_cli("branch", "branch-1")

        # Run `branch --list` with a pattern "feature-*"
        result, stdout, _stderr = self._run_cli(
            "branch", "--all", "--list", "feature-*"
        )
        self.assertEqual(result, 0)

        # Collect branches from the output
        branches = [line.strip() for line in stdout.splitlines()]

        # Expected branches — exactly those matching the pattern
        expected_branches = ["feature-1", "feature-2"]

        self.assertEqual(branches, expected_branches)


class TestTerminalWidth(TestCase):
    @patch("os.get_terminal_size")
    def test_terminal_size(self, mock_get_terminal_size):
        """Test os.get_terminal_size mocking."""
        mock_get_terminal_size.return_value.columns = 100
        width = detect_terminal_width()
        self.assertEqual(width, 100)

    @patch("os.get_terminal_size")
    def test_terminal_size_os_error(self, mock_get_terminal_size):
        """Test os.get_terminal_size raising OSError."""
        mock_get_terminal_size.side_effect = OSError("No terminal")
        width = detect_terminal_width()
        self.assertEqual(width, 80)


class TestWriteColumns(TestCase):
    """Tests for write_columns function"""

    def test_basic_functionality(self):
        """Test basic functionality with default terminal width."""
        out = io.StringIO()
        items = [b"main", b"dev", b"feature/branch-1"]
        write_columns(items, out, width=80)

        output_text = out.getvalue()
        self.assertEqual(output_text, "main  dev  feature/branch-1\n")

    def test_narrow_terminal_single_column(self):
        """Test with narrow terminal forcing single column."""
        out = io.StringIO()

        items = [b"main", b"dev", b"feature/branch-1"]
        write_columns(items, out, 20)

        self.assertEqual(out.getvalue(), "main\ndev\nfeature/branch-1\n")

    def test_wide_terminal_multiple_columns(self):
        """Test with wide terminal allowing multiple columns."""
        out = io.StringIO()
        items = [
            b"main",
            b"dev",
            b"feature/branch-1",
            b"feature/branch-2",
            b"feature/branch-3",
        ]
        write_columns(items, out, 120)

        output_text = out.getvalue()
        self.assertEqual(
            output_text,
            "main  dev  feature/branch-1  feature/branch-2  feature/branch-3\n",
        )

    def test_single_item(self):
        """Test with single item."""
        out = io.StringIO()
        write_columns([b"single"], out, 80)

        output_text = out.getvalue()
        self.assertEqual("single\n", output_text)
        self.assertTrue(output_text.endswith("\n"))

    def test_os_error_fallback(self):
        """Test fallback behavior when os.get_terminal_size raises OSError."""
        with patch("os.get_terminal_size", side_effect=OSError("No terminal")):
            out = io.StringIO()
            items = [b"main", b"dev"]
            write_columns(items, out)

            output_text = out.getvalue()
            # With default width (80), should display in columns
            self.assertEqual(output_text, "main  dev\n")

    def test_iterator_input(self):
        """Test with iterator input instead of list."""
        out = io.StringIO()
        items = [b"main", b"dev", b"feature/branch-1"]
        items_iterator = iter(items)
        write_columns(items_iterator, out, 80)

        output_text = out.getvalue()
        self.assertEqual(output_text, "main  dev  feature/branch-1\n")

    def test_column_alignment(self):
        """Test that columns are properly aligned."""
        out = io.StringIO()
        items = [b"short", b"medium_length", b"very_long______name"]
        write_columns(items, out, 50)

        output_text = out.getvalue()
        self.assertEqual(output_text, "short  medium_length  very_long______name\n")

    def test_columns_formatting(self):
        """Test that items are formatted in columns within single line."""
        out = io.StringIO()
        items = [b"branch-1", b"branch-2", b"branch-3", b"branch-4", b"branch-5"]
        write_columns(items, out, 80)

        output_text = out.getvalue()

        self.assertEqual(output_text.count("\n"), 1)
        self.assertTrue(output_text.endswith("\n"))

        line = output_text.strip()
        for item in items:
            self.assertIn(item.decode(), line)

    def test_column_alignment_multiple_lines(self):
        """Test that columns are properly aligned across multiple lines."""
        items = [
            b"short",
            b"medium_length",
            b"very_long_branch_name",
            b"another",
            b"more",
            b"even_longer_branch_name_here",
        ]

        out = io.StringIO()

        write_columns(items, out, width=60)

        output_text = out.getvalue()
        lines = output_text.strip().split("\n")

        self.assertGreater(len(lines), 1)

        line_lengths = [len(line) for line in lines if line.strip()]

        for length in line_lengths:
            self.assertLessEqual(length, 60)

        all_output = " ".join(lines)
        for item in items:
            self.assertIn(item.decode(), all_output)


class CheckoutCommandTest(DulwichCliTestCase):
    """Tests for checkout command."""

    def test_checkout_branch(self):
        # Create initial commit and branch
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")
        self._run_cli("branch", "test-branch")

        # Checkout branch
        _result, _stdout, _stderr = self._run_cli("checkout", "test-branch")
        self.assertEqual(
            self.repo.refs.read_ref(b"HEAD"), b"ref: refs/heads/test-branch"
        )


class TagCommandTest(DulwichCliTestCase):
    """Tests for tag command."""

    def test_tag_create(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Create tag
        _result, _stdout, _stderr = self._run_cli("tag", "v1.0")
        self.assertIn(b"refs/tags/v1.0", self.repo.refs.keys())


class VerifyCommitCommandTest(DulwichCliTestCase):
    """Tests for verify-commit command."""

    def test_verify_commit_basic(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Mock the porcelain.verify_commit function since we don't have GPG setup
        with patch("dulwich.cli.porcelain.verify_commit") as mock_verify:
            _result, stdout, _stderr = self._run_cli("verify-commit", "HEAD")
            mock_verify.assert_called_once_with(".", "HEAD")
            self.assertIn("Good signature", stdout)

    def test_verify_commit_multiple(self):
        # Create multiple commits
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test1")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=First")

        with open(test_file, "w") as f:
            f.write("test2")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Second")

        # Mock the porcelain.verify_commit function
        with patch("dulwich.cli.porcelain.verify_commit") as mock_verify:
            _result, stdout, _stderr = self._run_cli("verify-commit", "HEAD", "HEAD~1")
            self.assertEqual(mock_verify.call_count, 2)
            self.assertIn("HEAD", stdout)
            self.assertIn("HEAD~1", stdout)

    def test_verify_commit_default_head(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Mock the porcelain.verify_commit function
        with patch("dulwich.cli.porcelain.verify_commit") as mock_verify:
            # Test that verify-commit without arguments defaults to HEAD
            _result, stdout, _stderr = self._run_cli("verify-commit")
            mock_verify.assert_called_once_with(".", "HEAD")
            self.assertIn("Good signature", stdout)


class VerifyTagCommandTest(DulwichCliTestCase):
    """Tests for verify-tag command."""

    def test_verify_tag_basic(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Create an annotated tag
        self._run_cli("tag", "--annotated", "v1.0")

        # Mock the porcelain.verify_tag function since we don't have GPG setup
        with patch("dulwich.cli.porcelain.verify_tag") as mock_verify:
            _result, stdout, _stderr = self._run_cli("verify-tag", "v1.0")
            mock_verify.assert_called_once_with(".", "v1.0")
            self.assertIn("Good signature", stdout)

    def test_verify_tag_multiple(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Create multiple annotated tags
        self._run_cli("tag", "--annotated", "v1.0")
        self._run_cli("tag", "--annotated", "v2.0")

        # Mock the porcelain.verify_tag function
        with patch("dulwich.cli.porcelain.verify_tag") as mock_verify:
            _result, stdout, _stderr = self._run_cli("verify-tag", "v1.0", "v2.0")
            self.assertEqual(mock_verify.call_count, 2)
            self.assertIn("v1.0", stdout)
            self.assertIn("v2.0", stdout)


class DiffCommandTest(DulwichCliTestCase):
    """Tests for diff command."""

    def test_diff_working_tree(self):
        # Create and commit a file
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Modify the file
        with open(test_file, "w") as f:
            f.write("initial content\nmodified\n")

        # Test unstaged diff
        _result, stdout, _stderr = self._run_cli("diff")
        self.assertIn("+modified", stdout)

    def test_diff_staged(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Modify and stage the file
        with open(test_file, "w") as f:
            f.write("initial content\nnew file\n")
        self._run_cli("add", "test.txt")

        # Test staged diff
        _result, stdout, _stderr = self._run_cli("diff", "--staged")
        self.assertIn("+new file", stdout)

    def test_diff_cached(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Modify and stage the file
        with open(test_file, "w") as f:
            f.write("initial content\nnew file\n")
        self._run_cli("add", "test.txt")

        # Test cached diff (alias for staged)
        _result, stdout, _stderr = self._run_cli("diff", "--cached")
        self.assertIn("+new file", stdout)

    def test_diff_commit(self):
        # Create two commits
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("first version\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=First")

        with open(test_file, "w") as f:
            f.write("first version\nsecond line\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Second")

        # Add working tree changes
        with open(test_file, "a") as f:
            f.write("working tree change\n")

        # Test single commit diff (should show working tree vs HEAD)
        _result, stdout, _stderr = self._run_cli("diff", "HEAD")
        self.assertIn("+working tree change", stdout)

    def test_diff_two_commits(self):
        # Create two commits
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("first version\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=First")

        # Get first commit SHA
        first_commit = self.repo.refs[b"HEAD"].decode()

        with open(test_file, "w") as f:
            f.write("first version\nsecond line\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Second")

        # Get second commit SHA
        second_commit = self.repo.refs[b"HEAD"].decode()

        # Test diff between two commits
        _result, stdout, _stderr = self._run_cli("diff", first_commit, second_commit)
        self.assertIn("+second line", stdout)

    def test_diff_commit_vs_working_tree(self):
        # Test that diff <commit> shows working tree vs commit (not commit vs parent)
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("first version\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=First")

        first_commit = self.repo.refs[b"HEAD"].decode()

        with open(test_file, "w") as f:
            f.write("first version\nsecond line\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Second")

        # Add changes to working tree
        with open(test_file, "w") as f:
            f.write("completely different\n")

        # diff <first_commit> should show working tree vs first commit
        _result, stdout, _stderr = self._run_cli("diff", first_commit)
        self.assertIn("-first version", stdout)
        self.assertIn("+completely different", stdout)

    def test_diff_with_paths(self):
        # Test path filtering
        # Create multiple files
        file1 = os.path.join(self.repo_path, "file1.txt")
        file2 = os.path.join(self.repo_path, "file2.txt")
        subdir = os.path.join(self.repo_path, "subdir")
        os.makedirs(subdir)
        file3 = os.path.join(subdir, "file3.txt")

        with open(file1, "w") as f:
            f.write("content1\n")
        with open(file2, "w") as f:
            f.write("content2\n")
        with open(file3, "w") as f:
            f.write("content3\n")

        self._run_cli("add", ".")
        self._run_cli("commit", "--message=Initial")

        # Modify all files
        with open(file1, "w") as f:
            f.write("modified1\n")
        with open(file2, "w") as f:
            f.write("modified2\n")
        with open(file3, "w") as f:
            f.write("modified3\n")

        # Test diff with specific file
        _result, stdout, _stderr = self._run_cli("diff", "--", "file1.txt")
        self.assertIn("file1.txt", stdout)
        self.assertNotIn("file2.txt", stdout)
        self.assertNotIn("file3.txt", stdout)

        # Test diff with directory
        _result, stdout, _stderr = self._run_cli("diff", "--", "subdir")
        self.assertNotIn("file1.txt", stdout)
        self.assertNotIn("file2.txt", stdout)
        self.assertIn("file3.txt", stdout)

        # Test staged diff with paths
        self._run_cli("add", "file1.txt")
        _result, stdout, _stderr = self._run_cli("diff", "--staged", "--", "file1.txt")
        self.assertIn("file1.txt", stdout)
        self.assertIn("+modified1", stdout)

        # Test diff with multiple paths (file2 and file3 are still unstaged)
        _result, stdout, _stderr = self._run_cli(
            "diff", "--", "file2.txt", "subdir/file3.txt"
        )
        self.assertIn("file2.txt", stdout)
        self.assertIn("file3.txt", stdout)
        self.assertNotIn("file1.txt", stdout)

        # Test diff with commit and paths
        first_commit = self.repo.refs[b"HEAD"].decode()
        with open(file1, "w") as f:
            f.write("newer1\n")
        _result, stdout, _stderr = self._run_cli(
            "diff", first_commit, "--", "file1.txt"
        )
        self.assertIn("file1.txt", stdout)
        self.assertIn("-content1", stdout)
        self.assertIn("+newer1", stdout)
        self.assertNotIn("file2.txt", stdout)

    def test_diff_stat(self):
        # Create and commit a file
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content\n")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Modify the file
        with open(test_file, "w") as f:
            f.write("initial content\nmodified\n")

        # Test --stat output
        _result, stdout, _stderr = self._run_cli("diff", "--stat")
        self.assertEqual(
            stdout,
            " test.txt | 1 +\n 1 files changed, 1 insertions(+), 0 deletions(-)\n",
        )


class FilterBranchCommandTest(DulwichCliTestCase):
    """Tests for filter-branch command."""

    def setUp(self):
        super().setUp()
        # Create a more complex repository structure for testing
        # Create some files in subdirectories
        os.makedirs(os.path.join(self.repo_path, "subdir"))
        os.makedirs(os.path.join(self.repo_path, "other"))

        # Create files
        files = {
            "README.md": "# Test Repo",
            "subdir/file1.txt": "File in subdir",
            "subdir/file2.txt": "Another file in subdir",
            "other/file3.txt": "File in other dir",
            "root.txt": "File at root",
        }

        for path, content in files.items():
            file_path = os.path.join(self.repo_path, path)
            with open(file_path, "w") as f:
                f.write(content)

        # Add all files and create initial commit
        self._run_cli("add", ".")
        self._run_cli("commit", "--message=Initial commit")

        # Create a second commit modifying subdir
        with open(os.path.join(self.repo_path, "subdir/file1.txt"), "a") as f:
            f.write("\nModified content")
        self._run_cli("add", "subdir/file1.txt")
        self._run_cli("commit", "--message=Modify subdir file")

        # Create a third commit in other dir
        with open(os.path.join(self.repo_path, "other/file3.txt"), "a") as f:
            f.write("\nMore content")
        self._run_cli("add", "other/file3.txt")
        self._run_cli("commit", "--message=Modify other file")

        # Create a branch
        self._run_cli("branch", "test-branch")

        # Create a tag
        self._run_cli("tag", "v1.0")

    def test_filter_branch_subdirectory_filter(self):
        """Test filter-branch with subdirectory filter."""
        # Run filter-branch to extract only the subdir
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "filter-branch", "--subdirectory-filter", "subdir"
            )

            # Check that the operation succeeded
            self.assertEqual(result, 0)
            log_output = "\n".join(cm.output)
            self.assertIn("Rewrite HEAD", log_output)

        # filter-branch rewrites history but doesn't update working tree
        # We need to check the commit contents, not the working tree
        # Reset to the rewritten HEAD to update working tree
        self._run_cli("reset", "--hard", "HEAD")

        # Now check that only files from subdir remain at root level
        self.assertTrue(os.path.exists(os.path.join(self.repo_path, "file1.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.repo_path, "file2.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.repo_path, "README.md")))
        self.assertFalse(os.path.exists(os.path.join(self.repo_path, "root.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.repo_path, "other")))
        self.assertFalse(os.path.exists(os.path.join(self.repo_path, "subdir")))

        # Check that original refs were backed up
        original_refs = [
            ref for ref in self.repo.refs.keys() if ref.startswith(b"refs/original/")
        ]
        self.assertTrue(
            len(original_refs) > 0, "No original refs found after filter-branch"
        )

    @skipIf(sys.platform == "win32", "sed command not available on Windows")
    def test_filter_branch_msg_filter(self):
        """Test filter-branch with message filter."""
        # Run filter-branch to prepend [FILTERED] to commit messages
        result, stdout, _stderr = self._run_cli(
            "filter-branch", "--msg-filter", "sed 's/^/[FILTERED] /'"
        )

        self.assertEqual(result, 0)

        # Check that commit messages were modified
        result, stdout, _stderr = self._run_cli("log")
        self.assertIn("[FILTERED] Modify other file", stdout)
        self.assertIn("[FILTERED] Modify subdir file", stdout)
        self.assertIn("[FILTERED] Initial commit", stdout)

    def test_filter_branch_env_filter(self):
        """Test filter-branch with environment filter."""
        # Run filter-branch to change author email
        env_filter = """
        if [ "$GIT_AUTHOR_EMAIL" = "test@example.com" ]; then
            export GIT_AUTHOR_EMAIL="filtered@example.com"
        fi
        """
        result, _stdout, _stderr = self._run_cli(
            "filter-branch", "--env-filter", env_filter
        )

        self.assertEqual(result, 0)

    def test_filter_branch_prune_empty(self):
        """Test filter-branch with prune-empty option."""
        # Create a commit that only touches files outside subdir
        with open(os.path.join(self.repo_path, "root.txt"), "a") as f:
            f.write("\nNew line")
        self._run_cli("add", "root.txt")
        self._run_cli("commit", "--message=Modify root file only")

        # Run filter-branch to extract subdir with prune-empty
        result, stdout, _stderr = self._run_cli(
            "filter-branch", "--subdirectory-filter", "subdir", "--prune-empty"
        )

        self.assertEqual(result, 0)

        # The last commit should have been pruned
        result, stdout, _stderr = self._run_cli("log")
        self.assertNotIn("Modify root file only", stdout)

    @skipIf(sys.platform == "win32", "sed command not available on Windows")
    def test_filter_branch_force(self):
        """Test filter-branch with force option."""
        # Run filter-branch once with a filter that actually changes something
        result, _stdout, _stderr = self._run_cli(
            "filter-branch", "--msg-filter", "sed 's/^/[TEST] /'"
        )
        self.assertEqual(result, 0)

        # Check that backup refs were created
        # The implementation backs up refs under refs/original/
        original_refs = [
            ref for ref in self.repo.refs.keys() if ref.startswith(b"refs/original/")
        ]
        self.assertTrue(len(original_refs) > 0, "No original refs found")

        # Run again without force - should fail
        with self.assertLogs("dulwich.cli", level="ERROR") as cm:
            result, _stdout, _stderr = self._run_cli(
                "filter-branch", "--msg-filter", "sed 's/^/[TEST2] /'"
            )
            self.assertEqual(result, 1)
            log_output = "\n".join(cm.output)
            self.assertIn("Cannot create a new backup", log_output)
            self.assertIn("refs/original", log_output)

        # Run with force - should succeed
        result, _stdout, _stderr = self._run_cli(
            "filter-branch", "--force", "--msg-filter", "sed 's/^/[TEST3] /'"
        )
        self.assertEqual(result, 0)

    @skipIf(sys.platform == "win32", "sed command not available on Windows")
    def test_filter_branch_specific_branch(self):
        """Test filter-branch on a specific branch."""
        # Switch to test-branch and add a commit
        self._run_cli("checkout", "test-branch")
        with open(os.path.join(self.repo_path, "branch-file.txt"), "w") as f:
            f.write("Branch specific file")
        self._run_cli("add", "branch-file.txt")
        self._run_cli("commit", "--message=Branch commit")

        # Run filter-branch on the test-branch
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, stdout, _stderr = self._run_cli(
                "filter-branch", "--msg-filter", "sed 's/^/[BRANCH] /'", "test-branch"
            )

            self.assertEqual(result, 0)
            log_output = "\n".join(cm.output)
            self.assertIn("Ref 'refs/heads/test-branch' was rewritten", log_output)

        # Check that only test-branch was modified
        result, stdout, _stderr = self._run_cli("log")
        self.assertIn("[BRANCH] Branch commit", stdout)

        # Switch to master and check it wasn't modified
        self._run_cli("checkout", "master")
        result, stdout, _stderr = self._run_cli("log")
        self.assertNotIn("[BRANCH]", stdout)

    def test_filter_branch_tree_filter(self):
        """Test filter-branch with tree filter."""
        # Use a tree filter to remove a specific file
        tree_filter = "rm -f root.txt"
        result, stdout, _stderr = self._run_cli(
            "filter-branch", "--tree-filter", tree_filter
        )

        self.assertEqual(result, 0)

        # Check that the file was removed from the latest commit
        # We need to check the commit tree, not the working directory
        result, stdout, _stderr = self._run_cli("ls-tree", "HEAD")
        self.assertNotIn("root.txt", stdout)

    def test_filter_branch_index_filter(self):
        """Test filter-branch with index filter."""
        # Use an index filter to remove a file from the index
        index_filter = "git rm --cached --ignore-unmatch root.txt"
        result, _stdout, _stderr = self._run_cli(
            "filter-branch", "--index-filter", index_filter
        )

        self.assertEqual(result, 0)

    def test_filter_branch_parent_filter(self):
        """Test filter-branch with parent filter."""
        # Create a merge commit first
        self._run_cli("checkout", "HEAD", "-b", "feature")
        with open(os.path.join(self.repo_path, "feature.txt"), "w") as f:
            f.write("Feature")
        self._run_cli("add", "feature.txt")
        self._run_cli("commit", "--message=Feature commit")

        self._run_cli("checkout", "master")
        self._run_cli("merge", "feature", "--message=Merge feature")

        # Use parent filter to linearize history (remove second parent)
        parent_filter = "cut -d' ' -f1"
        result, _stdout, _stderr = self._run_cli(
            "filter-branch", "--parent-filter", parent_filter
        )

        self.assertEqual(result, 0)

    def test_filter_branch_commit_filter(self):
        """Test filter-branch with commit filter."""
        # Use commit filter to skip commits with certain messages
        commit_filter = """
        if grep -q "Modify other" <<< "$GIT_COMMIT_MESSAGE"; then
            skip_commit "$@"
        else
            git commit-tree "$@"
        fi
        """
        _result, _stdout, _stderr = self._run_cli(
            "filter-branch", "--commit-filter", commit_filter
        )

        # Note: This test may fail because the commit filter syntax is simplified
        # In real Git, skip_commit is a function, but our implementation may differ

    def test_filter_branch_tag_name_filter(self):
        """Test filter-branch with tag name filter."""
        # Run filter-branch with tag name filter to rename tags
        result, _stdout, _stderr = self._run_cli(
            "filter-branch",
            "--tag-name-filter",
            "sed 's/^v/version-/'",
            "--msg-filter",
            "cat",
        )

        self.assertEqual(result, 0)

        # Check that tag was renamed
        self.assertIn(b"refs/tags/version-1.0", self.repo.refs.keys())

    def test_filter_branch_errors(self):
        """Test filter-branch error handling."""
        # Test with invalid subdirectory
        result, _stdout, _stderr = self._run_cli(
            "filter-branch", "--subdirectory-filter", "nonexistent"
        )
        # Should still succeed but produce empty history
        self.assertEqual(result, 0)

    def test_filter_branch_no_args(self):
        """Test filter-branch with no arguments."""
        # Should work as no-op
        result, _stdout, _stderr = self._run_cli("filter-branch")
        self.assertEqual(result, 0)


class ShowCommandTest(DulwichCliTestCase):
    """Tests for show command."""

    def test_show_commit(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        _result, stdout, _stderr = self._run_cli("show", "HEAD")
        self.assertIn("Test commit", stdout)


class ShowRefCommandTest(DulwichCliTestCase):
    """Tests for show-ref command."""

    def test_show_ref_basic(self):
        """Test basic show-ref functionality."""
        # Create a commit to have a HEAD ref
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Create a branch
        self._run_cli("branch", "test-branch")

        # Get the exact SHAs
        master_sha = self.repo.refs[b"refs/heads/master"].decode()
        test_branch_sha = self.repo.refs[b"refs/heads/test-branch"].decode()

        # Run show-ref
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("show-ref")
            output = "\n".join([record.message for record in cm.records])

        expected = (
            f"{master_sha} refs/heads/master\n{test_branch_sha} refs/heads/test-branch"
        )
        self.assertEqual(output, expected)

    def test_show_ref_with_head(self):
        """Test show-ref with --head option."""
        # Create a commit to have a HEAD ref
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Get the exact SHAs
        head_sha = self.repo.refs[b"HEAD"].decode()
        master_sha = self.repo.refs[b"refs/heads/master"].decode()

        # Run show-ref with --head
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("show-ref", "--head")
            output = "\n".join([record.message for record in cm.records])

        expected = f"{head_sha} HEAD\n{master_sha} refs/heads/master"
        self.assertEqual(output, expected)

    def test_show_ref_with_pattern(self):
        """Test show-ref with pattern matching."""
        # Create commits and branches
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")
        self._run_cli("branch", "feature-1")
        self._run_cli("branch", "feature-2")
        self._run_cli("branch", "bugfix-1")

        # Get the exact SHA for master
        master_sha = self.repo.refs[b"refs/heads/master"].decode()

        # Test pattern matching for "master"
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("show-ref", "master")
            output = "\n".join([record.message for record in cm.records])

        expected = f"{master_sha} refs/heads/master"
        self.assertEqual(output, expected)

    def test_show_ref_branches_only(self):
        """Test show-ref with --branches option."""
        # Create commits and a tag
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")
        self._run_cli("tag", "v1.0")

        # Get the exact SHA for master
        master_sha = self.repo.refs[b"refs/heads/master"].decode()

        # Run show-ref with --branches
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("show-ref", "--branches")
            output = "\n".join([record.message for record in cm.records])

        expected = f"{master_sha} refs/heads/master"
        self.assertEqual(output, expected)

    def test_show_ref_tags_only(self):
        """Test show-ref with --tags option."""
        # Create commits and tags
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")
        self._run_cli("tag", "v1.0")
        self._run_cli("tag", "v2.0")

        # Get the exact SHAs for tags
        v1_sha = self.repo.refs[b"refs/tags/v1.0"].decode()
        v2_sha = self.repo.refs[b"refs/tags/v2.0"].decode()

        # Run show-ref with --tags
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("show-ref", "--tags")
            output = "\n".join([record.message for record in cm.records])

        expected = f"{v1_sha} refs/tags/v1.0\n{v2_sha} refs/tags/v2.0"
        self.assertEqual(output, expected)

    def test_show_ref_hash_only(self):
        """Test show-ref with --hash option to show only OID."""
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Get the exact SHA for master
        master_sha = self.repo.refs[b"refs/heads/master"].decode()

        # Run show-ref with --hash
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli(
                "show-ref", "--hash", "--", "master"
            )
            output = "\n".join([record.message for record in cm.records])

        expected = f"{master_sha}"
        self.assertEqual(output, expected)

    def test_show_ref_verify(self):
        """Test show-ref with --verify option for exact matching."""
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Get the exact SHA for master
        master_sha = self.repo.refs[b"refs/heads/master"].decode()

        # Verify with exact ref path should succeed
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "show-ref", "--verify", "refs/heads/master"
            )
            self.assertEqual(result, 0)
            output = "\n".join([record.message for record in cm.records])

        expected = f"{master_sha} refs/heads/master"
        self.assertEqual(output, expected)

        # Verify with partial name should fail
        result, _stdout, _stderr = self._run_cli("show-ref", "--verify", "master")
        self.assertEqual(result, 1)

    def test_show_ref_exists(self):
        """Test show-ref with --exists option."""
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Check if existing ref exists
        result, _stdout, _stderr = self._run_cli(
            "show-ref", "--exists", "refs/heads/master"
        )
        self.assertEqual(result, 0)

        # Check if non-existing ref exists
        result, _stdout, _stderr = self._run_cli(
            "show-ref", "--exists", "refs/heads/nonexistent"
        )
        self.assertEqual(result, 2)

    def test_show_ref_quiet(self):
        """Test show-ref with --quiet option."""
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Run show-ref with --quiet - should not log anything
        result, _stdout, _stderr = self._run_cli("show-ref", "--quiet")
        self.assertEqual(result, 0)

    def test_show_ref_abbrev(self):
        """Test show-ref with --abbrev option."""
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Get the exact SHA for master
        master_sha = self.repo.refs[b"refs/heads/master"].decode()

        # Run show-ref with --abbrev=7
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("show-ref", "--abbrev=7")
            output = "\n".join([record.message for record in cm.records])

        expected = f"{master_sha[:7]} refs/heads/master"
        self.assertEqual(output, expected)

    def test_show_ref_no_matches(self):
        """Test show-ref returns error when no matches found."""
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        # Search for non-existent pattern
        result, _stdout, _stderr = self._run_cli("show-ref", "nonexistent")
        self.assertEqual(result, 1)


class ShowBranchCommandTest(DulwichCliTestCase):
    """Tests for show-branch command."""

    def test_show_branch_basic(self):
        """Test basic show-branch functionality."""
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Create a branch and add a commit
        self._run_cli("branch", "branch1")
        self._run_cli("checkout", "branch1")
        with open(test_file, "a") as f:
            f.write("\nbranch1 content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Branch1 commit")

        # Switch back to master
        self._run_cli("checkout", "master")

        # Run show-branch
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli(
                "show-branch", "master", "branch1"
            )
            output = "\n".join([record.message for record in cm.records])

        # Check exact output
        expected = (
            "! [branch1] Branch1 commit\n"
            " ![master] Initial commit\n"
            "----\n"
            "*  [Branch1 commit]\n"
            "+* [Initial commit]"
        )
        self.assertEqual(expected, output)

    def test_show_branch_list(self):
        """Test show-branch with --list option."""
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Create branches
        self._run_cli("branch", "branch1")
        self._run_cli("branch", "branch2")

        # Run show-branch --list
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("show-branch", "--list")
            output = "\n".join([record.message for record in cm.records])

        # Check exact output (only branch headers, no separator)
        expected = (
            "!  [branch1] Initial commit\n"
            " ! [branch2] Initial commit\n"
            "  ![master] Initial commit"
        )
        self.assertEqual(expected, output)

    def test_show_branch_independent(self):
        """Test show-branch with --independent option."""
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Create a branch and add a commit
        self._run_cli("branch", "branch1")
        self._run_cli("checkout", "branch1")
        with open(test_file, "a") as f:
            f.write("\nbranch1 content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Branch1 commit")

        # Run show-branch --independent
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli(
                "show-branch", "--independent", "master", "branch1"
            )
            output = "\n".join([record.message for record in cm.records])

        # Only branch1 should be shown (it's not reachable from master)
        expected = "branch1"
        self.assertEqual(expected, output)

    def test_show_branch_merge_base(self):
        """Test show-branch with --merge-base option."""
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Get the initial commit SHA
        initial_sha = self.repo.refs[b"HEAD"]

        # Create a branch and add a commit
        self._run_cli("branch", "branch1")
        self._run_cli("checkout", "branch1")
        with open(test_file, "a") as f:
            f.write("\nbranch1 content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Branch1 commit")

        # Switch back to master and add a different commit
        self._run_cli("checkout", "master")
        with open(test_file, "a") as f:
            f.write("\nmaster content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Master commit")

        # Run show-branch --merge-base
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli(
                "show-branch", "--merge-base", "master", "branch1"
            )
            output = "\n".join([record.message for record in cm.records])

        # The merge base should be the initial commit SHA
        expected = initial_sha.decode("ascii")
        self.assertEqual(expected, output)


class FormatPatchCommandTest(DulwichCliTestCase):
    """Tests for format-patch command."""

    def test_format_patch_single_commit(self):
        # Create a commit with actual content
        from dulwich.objects import Blob, Tree

        # Initial commit
        tree1 = Tree()
        self.repo.object_store.add_object(tree1)
        self.repo.get_worktree().commit(
            message=b"Initial commit",
            tree=tree1.id,
        )

        # Second commit with a file
        blob = Blob.from_string(b"Hello, World!\n")
        self.repo.object_store.add_object(blob)
        tree2 = Tree()
        tree2.add(b"hello.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree2)
        self.repo.get_worktree().commit(
            message=b"Add hello.txt",
            tree=tree2.id,
        )

        # Test format-patch for last commit
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli("format-patch", "-n", "1")
            self.assertEqual(result, None)
            log_output = "\n".join(cm.output)
            self.assertIn("0001-Add-hello.txt.patch", log_output)

        # Check patch contents
        patch_file = os.path.join(self.repo_path, "0001-Add-hello.txt.patch")
        with open(patch_file, "rb") as f:
            content = f.read()
            # Check header
            self.assertIn(b"Subject: [PATCH 1/1] Add hello.txt", content)
            self.assertIn(b"From:", content)
            self.assertIn(b"Date:", content)
            # Check diff content
            self.assertIn(b"diff --git a/hello.txt b/hello.txt", content)
            self.assertIn(b"new file mode", content)
            self.assertIn(b"+Hello, World!", content)
            # Check footer
            self.assertIn(b"-- \nDulwich", content)

        # Clean up
        os.remove(patch_file)

    def test_format_patch_multiple_commits(self):
        from dulwich.objects import Blob, Tree

        # Initial commit
        tree1 = Tree()
        self.repo.object_store.add_object(tree1)
        self.repo.get_worktree().commit(
            message=b"Initial commit",
            tree=tree1.id,
        )

        # Second commit
        blob1 = Blob.from_string(b"File 1 content\n")
        self.repo.object_store.add_object(blob1)
        tree2 = Tree()
        tree2.add(b"file1.txt", 0o100644, blob1.id)
        self.repo.object_store.add_object(tree2)
        self.repo.get_worktree().commit(
            message=b"Add file1.txt",
            tree=tree2.id,
        )

        # Third commit
        blob2 = Blob.from_string(b"File 2 content\n")
        self.repo.object_store.add_object(blob2)
        tree3 = Tree()
        tree3.add(b"file1.txt", 0o100644, blob1.id)
        tree3.add(b"file2.txt", 0o100644, blob2.id)
        self.repo.object_store.add_object(tree3)
        self.repo.get_worktree().commit(
            message=b"Add file2.txt",
            tree=tree3.id,
        )

        # Test format-patch for last 2 commits
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli("format-patch", "-n", "2")
            self.assertEqual(result, None)
            log_output = "\n".join(cm.output)
            self.assertIn("0001-Add-file1.txt.patch", log_output)
            self.assertIn("0002-Add-file2.txt.patch", log_output)

        # Check first patch
        with open(os.path.join(self.repo_path, "0001-Add-file1.txt.patch"), "rb") as f:
            content = f.read()
            self.assertIn(b"Subject: [PATCH 1/2] Add file1.txt", content)
            self.assertIn(b"+File 1 content", content)

        # Check second patch
        with open(os.path.join(self.repo_path, "0002-Add-file2.txt.patch"), "rb") as f:
            content = f.read()
            self.assertIn(b"Subject: [PATCH 2/2] Add file2.txt", content)
            self.assertIn(b"+File 2 content", content)

        # Clean up
        os.remove(os.path.join(self.repo_path, "0001-Add-file1.txt.patch"))
        os.remove(os.path.join(self.repo_path, "0002-Add-file2.txt.patch"))

    def test_format_patch_output_directory(self):
        from dulwich.objects import Blob, Tree

        # Create a commit
        blob = Blob.from_string(b"Test content\n")
        self.repo.object_store.add_object(blob)
        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)
        self.repo.get_worktree().commit(
            message=b"Test commit",
            tree=tree.id,
        )

        # Create output directory
        output_dir = os.path.join(self.test_dir, "patches")
        os.makedirs(output_dir)

        # Test format-patch with output directory
        result, _stdout, _stderr = self._run_cli(
            "format-patch", "-o", output_dir, "-n", "1"
        )
        self.assertEqual(result, None)

        # Check that file was created in output directory with correct content
        patch_file = os.path.join(output_dir, "0001-Test-commit.patch")
        self.assertTrue(os.path.exists(patch_file))
        with open(patch_file, "rb") as f:
            content = f.read()
            self.assertIn(b"Subject: [PATCH 1/1] Test commit", content)
            self.assertIn(b"+Test content", content)

    def test_format_patch_commit_range(self):
        from dulwich.objects import Blob, Tree

        # Create commits with actual file changes
        commits = []
        trees = []

        # Initial empty commit
        tree0 = Tree()
        self.repo.object_store.add_object(tree0)
        trees.append(tree0)
        c0 = self.repo.get_worktree().commit(
            message=b"Initial commit",
            tree=tree0.id,
        )
        commits.append(c0)

        # Add three files in separate commits
        for i in range(1, 4):
            blob = Blob.from_string(f"Content {i}\n".encode())
            self.repo.object_store.add_object(blob)
            tree = Tree()
            # Copy previous files
            for j in range(1, i):
                prev_blob_id = trees[j][f"file{j}.txt".encode()][1]
                tree.add(f"file{j}.txt".encode(), 0o100644, prev_blob_id)
            # Add new file
            tree.add(f"file{i}.txt".encode(), 0o100644, blob.id)
            self.repo.object_store.add_object(tree)
            trees.append(tree)

            c = self.repo.get_worktree().commit(
                message=f"Add file{i}.txt".encode(),
                tree=tree.id,
            )
            commits.append(c)

        # Test format-patch with commit range (should get commits 2 and 3)
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "format-patch", f"{commits[1].decode()}..{commits[3].decode()}"
            )
            self.assertEqual(result, None)

            # Should create patches for commits 2 and 3
            log_output = "\n".join(cm.output)
            self.assertIn("0001-Add-file2.txt.patch", log_output)
            self.assertIn("0002-Add-file3.txt.patch", log_output)

        # Verify patch contents
        with open(os.path.join(self.repo_path, "0001-Add-file2.txt.patch"), "rb") as f:
            content = f.read()
            self.assertIn(b"Subject: [PATCH 1/2] Add file2.txt", content)
            self.assertIn(b"+Content 2", content)
            self.assertNotIn(b"file3.txt", content)  # Should not include file3

        with open(os.path.join(self.repo_path, "0002-Add-file3.txt.patch"), "rb") as f:
            content = f.read()
            self.assertIn(b"Subject: [PATCH 2/2] Add file3.txt", content)
            self.assertIn(b"+Content 3", content)
            self.assertNotIn(b"file2.txt", content)  # Should not modify file2

        # Clean up
        os.remove(os.path.join(self.repo_path, "0001-Add-file2.txt.patch"))
        os.remove(os.path.join(self.repo_path, "0002-Add-file3.txt.patch"))

    def test_format_patch_stdout(self):
        from dulwich.objects import Blob, Tree

        # Create a commit with modified file
        tree1 = Tree()
        blob1 = Blob.from_string(b"Original content\n")
        self.repo.object_store.add_object(blob1)
        tree1.add(b"file.txt", 0o100644, blob1.id)
        self.repo.object_store.add_object(tree1)
        self.repo.get_worktree().commit(
            message=b"Initial commit",
            tree=tree1.id,
        )

        tree2 = Tree()
        blob2 = Blob.from_string(b"Modified content\n")
        self.repo.object_store.add_object(blob2)
        tree2.add(b"file.txt", 0o100644, blob2.id)
        self.repo.object_store.add_object(tree2)
        self.repo.get_worktree().commit(
            message=b"Modify file.txt",
            tree=tree2.id,
        )

        # Mock stdout as a BytesIO for binary output
        stdout_stream = io.BytesIO()
        stdout_stream.buffer = stdout_stream

        # Run command with --stdout
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_cwd = os.getcwd()
        try:
            sys.stdout = stdout_stream
            sys.stderr = io.StringIO()
            os.chdir(self.repo_path)
            cli.main(["format-patch", "--stdout", "-n", "1"])
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            os.chdir(old_cwd)

        # Check output
        stdout_stream.seek(0)
        output = stdout_stream.read()
        self.assertIn(b"Subject: [PATCH 1/1] Modify file.txt", output)
        self.assertIn(b"diff --git a/file.txt b/file.txt", output)
        self.assertIn(b"-Original content", output)
        self.assertIn(b"+Modified content", output)
        self.assertIn(b"-- \nDulwich", output)

    def test_format_patch_empty_repo(self):
        # Test with empty repository
        result, stdout, _stderr = self._run_cli("format-patch", "-n", "5")
        self.assertEqual(result, None)
        # Should produce no output for empty repo
        self.assertEqual(stdout.strip(), "")


class FetchPackCommandTest(DulwichCliTestCase):
    """Tests for fetch-pack command."""

    @patch("dulwich.cli.get_transport_and_path")
    def test_fetch_pack_basic(self, mock_transport):
        # Mock the transport
        mock_client = MagicMock()
        mock_transport.return_value = (mock_client, "/path/to/repo")
        mock_client.fetch.return_value = None

        _result, _stdout, _stderr = self._run_cli(
            "fetch-pack", "git://example.com/repo.git"
        )
        mock_client.fetch.assert_called_once()


class LsRemoteCommandTest(DulwichCliTestCase):
    """Tests for ls-remote command."""

    def test_ls_remote_basic(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Test basic ls-remote
        _result, stdout, _stderr = self._run_cli("ls-remote", self.repo_path)
        lines = stdout.strip().split("\n")
        self.assertTrue(any("HEAD" in line for line in lines))
        self.assertTrue(any("refs/heads/master" in line for line in lines))

    def test_ls_remote_symref(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Test ls-remote with --symref option
        _result, stdout, _stderr = self._run_cli(
            "ls-remote", "--symref", self.repo_path
        )
        lines = stdout.strip().split("\n")
        # Should show symref for HEAD in exact format: "ref: refs/heads/master\tHEAD"
        expected_line = "ref: refs/heads/master\tHEAD"
        self.assertIn(
            expected_line,
            lines,
            f"Expected line '{expected_line}' not found in output: {lines}",
        )


class PullCommandTest(DulwichCliTestCase):
    """Tests for pull command."""

    @patch("dulwich.porcelain.pull")
    def test_pull_basic(self, mock_pull):
        _result, _stdout, _stderr = self._run_cli("pull", "origin")
        mock_pull.assert_called_once()

    @patch("dulwich.porcelain.pull")
    def test_pull_with_refspec(self, mock_pull):
        _result, _stdout, _stderr = self._run_cli("pull", "origin", "master")
        mock_pull.assert_called_once()


class PushCommandTest(DulwichCliTestCase):
    """Tests for push command."""

    @patch("dulwich.porcelain.push")
    def test_push_basic(self, mock_push):
        _result, _stdout, _stderr = self._run_cli("push", "origin")
        mock_push.assert_called_once()

    @patch("dulwich.porcelain.push")
    def test_push_force(self, mock_push):
        _result, _stdout, _stderr = self._run_cli("push", "-f", "origin")
        mock_push.assert_called_with(".", "origin", None, force=True)


class ArchiveCommandTest(DulwichCliTestCase):
    """Tests for archive command."""

    def test_archive_basic(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Archive produces binary output, so use BytesIO
        _result, stdout, _stderr = self._run_cli(
            "archive", "HEAD", stdout_stream=io.BytesIO()
        )
        # Should complete without error and produce some binary output
        self.assertIsInstance(stdout, bytes)
        self.assertGreater(len(stdout), 0)


class ForEachRefCommandTest(DulwichCliTestCase):
    """Tests for for-each-ref command."""

    def test_for_each_ref(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("for-each-ref")
            log_output = "\n".join(cm.output)
            # Just check that we have some refs output and it contains refs/heads
            self.assertTrue(len(cm.output) > 0, "Expected some ref output")
            self.assertIn("refs/heads/", log_output)


class PackRefsCommandTest(DulwichCliTestCase):
    """Tests for pack-refs command."""

    def test_pack_refs(self):
        # Create some refs
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")
        self._run_cli("branch", "test-branch")

        _result, _stdout, _stderr = self._run_cli("pack-refs", "--all")
        # Check that packed-refs file exists
        self.assertTrue(
            os.path.exists(os.path.join(self.repo_path, ".git", "packed-refs"))
        )


class SubmoduleCommandTest(DulwichCliTestCase):
    """Tests for submodule commands."""

    def test_submodule_list(self):
        # Create an initial commit so repo has a HEAD
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        _result, _stdout, _stderr = self._run_cli("submodule")
        # Should not crash on repo without submodules

    def test_submodule_init(self):
        # Create .gitmodules file for init to work
        gitmodules = os.path.join(self.repo_path, ".gitmodules")
        with open(gitmodules, "w") as f:
            f.write("")  # Empty .gitmodules file

        _result, _stdout, _stderr = self._run_cli("submodule", "init")
        # Should not crash


class StashCommandTest(DulwichCliTestCase):
    """Tests for stash commands."""

    def test_stash_list_empty(self):
        _result, _stdout, _stderr = self._run_cli("stash", "list")
        # Should not crash on empty stash

    def test_stash_push_pop(self):
        # Create a file and modify it
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Modify file
        with open(test_file, "w") as f:
            f.write("modified")

        # Stash changes
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("stash", "push")
            self.assertIn("Saved working directory", cm.output[0])

        # Note: Dulwich stash doesn't currently update the working tree
        # so the file remains modified after stash push

        # Note: stash pop is not fully implemented in Dulwich yet
        # so we only test stash push here


class MergeCommandTest(DulwichCliTestCase):
    """Tests for merge command."""

    def test_merge_basic(self):
        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        # Create and checkout new branch
        self._run_cli("branch", "feature")
        self._run_cli("checkout", "feature")

        # Make changes in feature branch
        with open(test_file, "w") as f:
            f.write("feature changes")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Feature commit")

        # Go back to main
        self._run_cli("checkout", "master")

        # Merge feature branch
        _result, _stdout, _stderr = self._run_cli("merge", "feature")


class HelpCommandTest(DulwichCliTestCase):
    """Tests for help command."""

    def test_help_basic(self):
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("help")
            log_output = "\n".join(cm.output)
            self.assertIn("dulwich command line tool", log_output)

    def test_help_all(self):
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("help", "-a")
            log_output = "\n".join(cm.output)
            self.assertIn("Available commands:", log_output)
            self.assertIn("add", log_output)
            self.assertIn("commit", log_output)


class RemoteCommandTest(DulwichCliTestCase):
    """Tests for remote commands."""

    def test_remote_add(self):
        _result, _stdout, _stderr = self._run_cli(
            "remote", "add", "origin", "https://github.com/example/repo.git"
        )
        # Check remote was added to config
        config = self.repo.get_config()
        self.assertEqual(
            config.get((b"remote", b"origin"), b"url"),
            b"https://github.com/example/repo.git",
        )


class CheckIgnoreCommandTest(DulwichCliTestCase):
    """Tests for check-ignore command."""

    def test_check_ignore(self):
        # Create .gitignore
        gitignore = os.path.join(self.repo_path, ".gitignore")
        with open(gitignore, "w") as f:
            f.write("*.log\n")

        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli(
                "check-ignore", "test.log", "test.txt"
            )
            log_output = "\n".join(cm.output)
            self.assertIn("test.log", log_output)
            self.assertNotIn("test.txt", log_output)


class LsFilesCommandTest(DulwichCliTestCase):
    """Tests for ls-files command."""

    def test_ls_files(self):
        # Add some files
        for name in ["a.txt", "b.txt", "c.txt"]:
            path = os.path.join(self.repo_path, name)
            with open(path, "w") as f:
                f.write(f"content of {name}")
        self._run_cli("add", "a.txt", "b.txt", "c.txt")

        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("ls-files")
            log_output = "\n".join(cm.output)
            self.assertIn("a.txt", log_output)
            self.assertIn("b.txt", log_output)
            self.assertIn("c.txt", log_output)


class LsTreeCommandTest(DulwichCliTestCase):
    """Tests for ls-tree command."""

    def test_ls_tree(self):
        # Create a directory structure
        os.mkdir(os.path.join(self.repo_path, "subdir"))
        with open(os.path.join(self.repo_path, "file.txt"), "w") as f:
            f.write("file content")
        with open(os.path.join(self.repo_path, "subdir", "nested.txt"), "w") as f:
            f.write("nested content")

        self._run_cli("add", ".")
        self._run_cli("commit", "--message=Initial")

        _result, stdout, _stderr = self._run_cli("ls-tree", "HEAD")
        self.assertIn("file.txt", stdout)
        self.assertIn("subdir", stdout)

    def test_ls_tree_recursive(self):
        # Create nested structure
        os.mkdir(os.path.join(self.repo_path, "subdir"))
        with open(os.path.join(self.repo_path, "subdir", "nested.txt"), "w") as f:
            f.write("nested")

        self._run_cli("add", ".")
        self._run_cli("commit", "--message=Initial")

        _result, stdout, _stderr = self._run_cli("ls-tree", "-r", "HEAD")
        self.assertIn("subdir/nested.txt", stdout)


class DescribeCommandTest(DulwichCliTestCase):
    """Tests for describe command."""

    def test_describe(self):
        # Create tagged commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")
        self._run_cli("tag", "v1.0")

        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            _result, _stdout, _stderr = self._run_cli("describe")
            self.assertIn("v1.0", cm.output[0])


class FsckCommandTest(DulwichCliTestCase):
    """Tests for fsck command."""

    def test_fsck(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        _result, _stdout, _stderr = self._run_cli("fsck")
        # Should complete without errors


class GrepCommandTest(DulwichCliTestCase):
    """Tests for grep command."""

    def test_grep_basic(self):
        # Create test files
        with open(os.path.join(self.repo_path, "file1.txt"), "w") as f:
            f.write("hello world\n")
        with open(os.path.join(self.repo_path, "file2.txt"), "w") as f:
            f.write("foo bar\n")

        self._run_cli("add", "file1.txt", "file2.txt")
        self._run_cli("commit", "--message=Add files")

        _result, stdout, _stderr = self._run_cli("grep", "world")
        self.assertEqual("file1.txt:hello world\n", stdout.replace("\r\n", "\n"))

    def test_grep_line_numbers(self):
        with open(os.path.join(self.repo_path, "test.txt"), "w") as f:
            f.write("line1\nline2\nline3\n")

        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Add test")

        _result, stdout, _stderr = self._run_cli("grep", "-n", "line")
        self.assertEqual(
            "test.txt:1:line1\ntest.txt:2:line2\ntest.txt:3:line3\n",
            stdout.replace("\r\n", "\n"),
        )

    def test_grep_case_insensitive(self):
        with open(os.path.join(self.repo_path, "case.txt"), "w") as f:
            f.write("Hello World\n")

        self._run_cli("add", "case.txt")
        self._run_cli("commit", "--message=Add case")

        _result, stdout, _stderr = self._run_cli("grep", "-i", "hello")
        self.assertEqual("case.txt:Hello World\n", stdout.replace("\r\n", "\n"))

    def test_grep_no_matches(self):
        with open(os.path.join(self.repo_path, "empty.txt"), "w") as f:
            f.write("nothing here\n")

        self._run_cli("add", "empty.txt")
        self._run_cli("commit", "--message=Add empty")

        _result, stdout, _stderr = self._run_cli("grep", "nonexistent")
        self.assertEqual("", stdout)


class RepackCommandTest(DulwichCliTestCase):
    """Tests for repack command."""

    def test_repack(self):
        # Create some objects
        for i in range(5):
            test_file = os.path.join(self.repo_path, f"test{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"content {i}")
            self._run_cli("add", f"test{i}.txt")
            self._run_cli("commit", f"--message=Commit {i}")

        _result, _stdout, _stderr = self._run_cli("repack")
        # Should create pack files
        pack_dir = os.path.join(self.repo_path, ".git", "objects", "pack")
        self.assertTrue(any(f.endswith(".pack") for f in os.listdir(pack_dir)))

    def test_repack_write_bitmap_index(self):
        """Test repack with --write-bitmap-index flag."""
        # Create some objects
        for i in range(5):
            test_file = os.path.join(self.repo_path, f"test{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"content {i}")
            self._run_cli("add", f"test{i}.txt")
            self._run_cli("commit", f"--message=Commit {i}")

        _result, _stdout, _stderr = self._run_cli("repack", "--write-bitmap-index")
        # Should create pack and bitmap files
        pack_dir = os.path.join(self.repo_path, ".git", "objects", "pack")
        self.assertTrue(any(f.endswith(".pack") for f in os.listdir(pack_dir)))
        self.assertTrue(any(f.endswith(".bitmap") for f in os.listdir(pack_dir)))


class ResetCommandTest(DulwichCliTestCase):
    """Tests for reset command."""

    def test_reset_soft(self):
        # Create commits
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("first")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=First")
        first_commit = self.repo.head()

        with open(test_file, "w") as f:
            f.write("second")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Second")

        # Reset soft
        _result, _stdout, _stderr = self._run_cli(
            "reset", "--soft", first_commit.decode()
        )
        # HEAD should be at first commit
        self.assertEqual(self.repo.head(), first_commit)


class WriteTreeCommandTest(DulwichCliTestCase):
    """Tests for write-tree command."""

    def test_write_tree(self):
        # Create and add files
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")

        _result, stdout, _stderr = self._run_cli("write-tree")
        # Should output tree SHA
        self.assertEqual(len(stdout.strip()), 40)


class UpdateServerInfoCommandTest(DulwichCliTestCase):
    """Tests for update-server-info command."""

    def test_update_server_info(self):
        _result, _stdout, _stderr = self._run_cli("update-server-info")
        # Should create info/refs file
        info_refs = os.path.join(self.repo_path, ".git", "info", "refs")
        self.assertTrue(os.path.exists(info_refs))


class SymbolicRefCommandTest(DulwichCliTestCase):
    """Tests for symbolic-ref command."""

    def test_symbolic_ref(self):
        # Create a branch
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")
        self._run_cli("branch", "test-branch")

        _result, _stdout, _stderr = self._run_cli(
            "symbolic-ref", "HEAD", "refs/heads/test-branch"
        )
        # HEAD should now point to test-branch
        self.assertEqual(
            self.repo.refs.read_ref(b"HEAD"), b"ref: refs/heads/test-branch"
        )


class BundleCommandTest(DulwichCliTestCase):
    """Tests for bundle commands."""

    def setUp(self):
        super().setUp()
        # Create a basic repository with some commits for bundle testing
        # Create initial commit
        test_file = os.path.join(self.repo_path, "file1.txt")
        with open(test_file, "w") as f:
            f.write("Content of file1\n")
        self._run_cli("add", "file1.txt")
        self._run_cli("commit", "--message=Initial commit")

        # Create second commit
        test_file2 = os.path.join(self.repo_path, "file2.txt")
        with open(test_file2, "w") as f:
            f.write("Content of file2\n")
        self._run_cli("add", "file2.txt")
        self._run_cli("commit", "--message=Add file2")

        # Create a branch and tag for testing
        self._run_cli("branch", "feature")
        self._run_cli("tag", "v1.0")

    def test_bundle_create_basic(self):
        """Test basic bundle creation."""
        bundle_file = os.path.join(self.test_dir, "test.bundle")

        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))
        self.assertGreater(os.path.getsize(bundle_file), 0)

    def test_bundle_create_all_refs(self):
        """Test bundle creation with --all flag."""
        bundle_file = os.path.join(self.test_dir, "all.bundle")

        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", "--all", bundle_file
        )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))

    def test_bundle_create_specific_refs(self):
        """Test bundle creation with specific refs."""
        bundle_file = os.path.join(self.test_dir, "refs.bundle")

        # Only use HEAD since feature branch may not exist
        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))

    def test_bundle_create_with_range(self):
        """Test bundle creation with commit range."""
        # Get the first commit SHA by looking at the log
        result, stdout, _stderr = self._run_cli("log", "--reverse")
        lines = stdout.strip().split("\n")
        # Find first commit line that contains a SHA
        first_commit = None
        for line in lines:
            if line.startswith("commit "):
                first_commit = line.split()[1][:8]  # Get short SHA
                break

        if first_commit:
            bundle_file = os.path.join(self.test_dir, "range.bundle")

            result, stdout, _stderr = self._run_cli(
                "bundle", "create", bundle_file, f"{first_commit}..HEAD"
            )
            self.assertEqual(result, 0)
            self.assertTrue(os.path.exists(bundle_file))
        else:
            self.skipTest("Could not determine first commit SHA")

    def test_bundle_create_to_stdout(self):
        """Test bundle creation to stdout."""
        result, stdout, _stderr = self._run_cli("bundle", "create", "-", "HEAD")
        self.assertEqual(result, 0)
        self.assertGreater(len(stdout), 0)
        # Bundle output is binary, so check it's not empty
        self.assertIsInstance(stdout, (str, bytes))

    def test_bundle_create_no_refs(self):
        """Test bundle creation with no refs specified."""
        bundle_file = os.path.join(self.test_dir, "noref.bundle")

        with self.assertLogs("dulwich.cli", level="ERROR") as cm:
            result, _stdout, _stderr = self._run_cli("bundle", "create", bundle_file)
            self.assertEqual(result, 1)
            self.assertIn("No refs specified", cm.output[0])

    def test_bundle_create_empty_bundle_refused(self):
        """Test that empty bundles are refused."""
        bundle_file = os.path.join(self.test_dir, "empty.bundle")

        # Try to create bundle with non-existent ref - this should fail with KeyError
        with self.assertRaises(KeyError):
            _result, _stdout, _stderr = self._run_cli(
                "bundle", "create", bundle_file, "nonexistent-ref"
            )

    def test_bundle_verify_valid(self):
        """Test bundle verification of valid bundle."""
        bundle_file = os.path.join(self.test_dir, "valid.bundle")

        # First create a bundle
        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)

        # Now verify it
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli("bundle", "verify", bundle_file)
            self.assertEqual(result, 0)
            self.assertIn("valid and can be applied", cm.output[0])

    def test_bundle_verify_quiet(self):
        """Test bundle verification with quiet flag."""
        bundle_file = os.path.join(self.test_dir, "quiet.bundle")

        # Create bundle
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Verify quietly
        result, stdout, _stderr = self._run_cli(
            "bundle", "verify", "--quiet", bundle_file
        )
        self.assertEqual(result, 0)
        self.assertEqual(stdout.strip(), "")

    def test_bundle_verify_from_stdin(self):
        """Test bundle verification from stdin."""
        bundle_file = os.path.join(self.test_dir, "stdin.bundle")

        # Create bundle
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Read bundle content
        with open(bundle_file, "rb") as f:
            bundle_content = f.read()

        # Mock stdin with bundle content
        old_stdin = sys.stdin
        try:
            sys.stdin = io.BytesIO(bundle_content)
            sys.stdin.buffer = sys.stdin
            result, _stdout, _stderr = self._run_cli("bundle", "verify", "-")
            self.assertEqual(result, 0)
        finally:
            sys.stdin = old_stdin

    def test_bundle_list_heads(self):
        """Test listing bundle heads."""
        bundle_file = os.path.join(self.test_dir, "heads.bundle")

        # Create bundle with HEAD only
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # List heads
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "bundle", "list-heads", bundle_file
            )
            self.assertEqual(result, 0)
            # Should contain at least the HEAD reference
            self.assertTrue(len(cm.output) > 0)

    def test_bundle_list_heads_specific_refs(self):
        """Test listing specific bundle heads."""
        bundle_file = os.path.join(self.test_dir, "specific.bundle")

        # Create bundle with HEAD
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # List heads without filtering
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "bundle", "list-heads", bundle_file
            )
            self.assertEqual(result, 0)
            # Should contain some reference
            self.assertTrue(len(cm.output) > 0)

    def test_bundle_list_heads_from_stdin(self):
        """Test listing bundle heads from stdin."""
        bundle_file = os.path.join(self.test_dir, "stdin-heads.bundle")

        # Create bundle
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Read bundle content
        with open(bundle_file, "rb") as f:
            bundle_content = f.read()

        # Mock stdin
        old_stdin = sys.stdin
        try:
            sys.stdin = io.BytesIO(bundle_content)
            sys.stdin.buffer = sys.stdin
            result, _stdout, _stderr = self._run_cli("bundle", "list-heads", "-")
            self.assertEqual(result, 0)
        finally:
            sys.stdin = old_stdin

    def test_bundle_unbundle(self):
        """Test bundle unbundling."""
        bundle_file = os.path.join(self.test_dir, "unbundle.bundle")

        # Create bundle
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Unbundle
        result, _stdout, _stderr = self._run_cli("bundle", "unbundle", bundle_file)
        self.assertEqual(result, 0)

    def test_bundle_unbundle_specific_refs(self):
        """Test unbundling specific refs."""
        bundle_file = os.path.join(self.test_dir, "unbundle-specific.bundle")

        # Create bundle with HEAD
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Unbundle only HEAD
        result, _stdout, _stderr = self._run_cli(
            "bundle", "unbundle", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)

    def test_bundle_unbundle_from_stdin(self):
        """Test unbundling from stdin."""
        bundle_file = os.path.join(self.test_dir, "stdin-unbundle.bundle")

        # Create bundle
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Read bundle content to simulate stdin
        with open(bundle_file, "rb") as f:
            bundle_content = f.read()

        # Mock stdin with bundle content
        old_stdin = sys.stdin
        try:
            # Create a BytesIO object with buffer attribute
            mock_stdin = io.BytesIO(bundle_content)
            mock_stdin.buffer = mock_stdin
            sys.stdin = mock_stdin

            result, _stdout, _stderr = self._run_cli("bundle", "unbundle", "-")
            self.assertEqual(result, 0)
        finally:
            sys.stdin = old_stdin

    def test_bundle_unbundle_with_progress(self):
        """Test unbundling with progress output."""
        bundle_file = os.path.join(self.test_dir, "progress.bundle")

        # Create bundle
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Unbundle with progress
        result, _stdout, _stderr = self._run_cli(
            "bundle", "unbundle", "--progress", bundle_file
        )
        self.assertEqual(result, 0)

    def test_bundle_create_with_progress(self):
        """Test bundle creation with progress output."""
        bundle_file = os.path.join(self.test_dir, "create-progress.bundle")

        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", "--progress", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))

    def test_bundle_create_with_quiet(self):
        """Test bundle creation with quiet flag."""
        bundle_file = os.path.join(self.test_dir, "quiet-create.bundle")

        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", "--quiet", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))

    def test_bundle_create_version_2(self):
        """Test bundle creation with specific version."""
        bundle_file = os.path.join(self.test_dir, "v2.bundle")

        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", "--version", "2", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))

    def test_bundle_create_version_3(self):
        """Test bundle creation with version 3."""
        bundle_file = os.path.join(self.test_dir, "v3.bundle")

        result, _stdout, _stderr = self._run_cli(
            "bundle", "create", "--version", "3", bundle_file, "HEAD"
        )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))

    def test_bundle_invalid_subcommand(self):
        """Test invalid bundle subcommand."""
        with self.assertLogs("dulwich.cli", level="ERROR") as cm:
            result, _stdout, _stderr = self._run_cli("bundle", "invalid-command")
            self.assertEqual(result, 1)
            self.assertIn("Unknown bundle subcommand", cm.output[0])

    def test_bundle_no_subcommand(self):
        """Test bundle command with no subcommand."""
        with self.assertLogs("dulwich.cli", level="ERROR") as cm:
            result, _stdout, _stderr = self._run_cli("bundle")
            self.assertEqual(result, 1)
            self.assertIn("Usage: bundle", cm.output[0])

    def test_bundle_create_with_stdin_refs(self):
        """Test bundle creation reading refs from stdin."""
        bundle_file = os.path.join(self.test_dir, "stdin-refs.bundle")

        # Mock stdin with refs
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("master\nfeature\n")
            result, _stdout, _stderr = self._run_cli(
                "bundle", "create", "--stdin", bundle_file
            )
            self.assertEqual(result, 0)
            self.assertTrue(os.path.exists(bundle_file))
        finally:
            sys.stdin = old_stdin

    def test_bundle_verify_missing_prerequisites(self):
        """Test bundle verification with missing prerequisites."""
        # Create a simple bundle first
        bundle_file = os.path.join(self.test_dir, "prereq.bundle")
        self._run_cli("bundle", "create", bundle_file, "HEAD")

        # Create a new repo to simulate missing objects
        new_repo_path = os.path.join(self.test_dir, "new_repo")
        os.mkdir(new_repo_path)
        new_repo = Repo.init(new_repo_path)
        new_repo.close()

        # Try to verify in new repo
        old_cwd = os.getcwd()
        try:
            os.chdir(new_repo_path)
            result, _stdout, _stderr = self._run_cli("bundle", "verify", bundle_file)
            # Just check that verification runs - result depends on bundle content
            self.assertIn(result, [0, 1])
        finally:
            os.chdir(old_cwd)

    def test_bundle_create_with_committish_range(self):
        """Test bundle creation with commit range using parse_committish_range."""
        # Create additional commits for range testing
        test_file3 = os.path.join(self.repo_path, "file3.txt")
        with open(test_file3, "w") as f:
            f.write("Content of file3\n")
        self._run_cli("add", "file3.txt")
        self._run_cli("commit", "--message=Add file3")

        # Get commit SHAs
        result, stdout, _stderr = self._run_cli("log")
        lines = stdout.strip().split("\n")
        # Extract SHAs from commit lines
        commits = []
        for line in lines:
            if line.startswith("commit:"):
                sha = line.split()[1]
                commits.append(sha[:8])  # Get short SHA

        # We should have exactly 3 commits: Add file3, Add file2, Initial commit
        self.assertEqual(len(commits), 3)

        bundle_file = os.path.join(self.test_dir, "range-test.bundle")

        # Test with commit range using .. syntax
        # Create a bundle containing commits reachable from commits[0] but not from commits[2]
        result, stdout, stderr = self._run_cli(
            "bundle", "create", bundle_file, f"{commits[2]}..HEAD"
        )
        if result != 0:
            self.fail(
                f"Bundle create failed with exit code {result}. stdout: {stdout!r}, stderr: {stderr!r}"
            )
        self.assertEqual(result, 0)
        self.assertTrue(os.path.exists(bundle_file))

        # Verify the bundle was created
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, stdout, stderr = self._run_cli("bundle", "verify", bundle_file)
            self.assertEqual(result, 0)
            self.assertIn("valid and can be applied", cm.output[0])


class FormatBytesTestCase(TestCase):
    """Tests for format_bytes function."""

    def test_bytes(self):
        """Test formatting bytes."""
        self.assertEqual("0.0 B", format_bytes(0))
        self.assertEqual("1.0 B", format_bytes(1))
        self.assertEqual("512.0 B", format_bytes(512))
        self.assertEqual("1023.0 B", format_bytes(1023))

    def test_kilobytes(self):
        """Test formatting kilobytes."""
        self.assertEqual("1.0 KB", format_bytes(1024))
        self.assertEqual("1.5 KB", format_bytes(1536))
        self.assertEqual("2.0 KB", format_bytes(2048))
        self.assertEqual("1023.0 KB", format_bytes(1024 * 1023))

    def test_megabytes(self):
        """Test formatting megabytes."""
        self.assertEqual("1.0 MB", format_bytes(1024 * 1024))
        self.assertEqual("1.5 MB", format_bytes(1024 * 1024 * 1.5))
        self.assertEqual("10.0 MB", format_bytes(1024 * 1024 * 10))
        self.assertEqual("1023.0 MB", format_bytes(1024 * 1024 * 1023))

    def test_gigabytes(self):
        """Test formatting gigabytes."""
        self.assertEqual("1.0 GB", format_bytes(1024 * 1024 * 1024))
        self.assertEqual("2.5 GB", format_bytes(1024 * 1024 * 1024 * 2.5))
        self.assertEqual("1023.0 GB", format_bytes(1024 * 1024 * 1024 * 1023))

    def test_terabytes(self):
        """Test formatting terabytes."""
        self.assertEqual("1.0 TB", format_bytes(1024 * 1024 * 1024 * 1024))
        self.assertEqual("5.0 TB", format_bytes(1024 * 1024 * 1024 * 1024 * 5))
        self.assertEqual("1000.0 TB", format_bytes(1024 * 1024 * 1024 * 1024 * 1000))


class GetPagerTest(TestCase):
    """Tests for get_pager function."""

    def setUp(self):
        super().setUp()
        # Save original environment
        self.original_env = os.environ.copy()
        # Clear pager-related environment variables
        for var in ["DULWICH_PAGER", "GIT_PAGER", "PAGER"]:
            os.environ.pop(var, None)
        # Reset the global pager disable flag
        cli.get_pager._disabled = False

    def tearDown(self):
        super().tearDown()
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_env)
        # Reset the global pager disable flag
        cli.get_pager._disabled = False

    def test_pager_disabled_globally(self):
        """Test that globally disabled pager returns stdout wrapper."""
        cli.disable_pager()
        pager = cli.get_pager()
        self.assertIsInstance(pager, cli._StreamContextAdapter)
        self.assertEqual(pager.stream, sys.stdout)

    def test_pager_not_tty(self):
        """Test that pager is disabled when stdout is not a TTY."""
        with patch("sys.stdout.isatty", return_value=False):
            pager = cli.get_pager()
            self.assertIsInstance(pager, cli._StreamContextAdapter)

    def test_pager_env_dulwich_pager(self):
        """Test DULWICH_PAGER environment variable."""
        os.environ["DULWICH_PAGER"] = "custom_pager"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager()
            self.assertIsInstance(pager, cli.Pager)
            self.assertEqual(pager.pager_cmd, "custom_pager")

    def test_pager_env_dulwich_pager_false(self):
        """Test DULWICH_PAGER=false disables pager."""
        os.environ["DULWICH_PAGER"] = "false"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager()
            self.assertIsInstance(pager, cli._StreamContextAdapter)

    def test_pager_env_git_pager(self):
        """Test GIT_PAGER environment variable."""
        os.environ["GIT_PAGER"] = "git_custom_pager"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager()
            self.assertIsInstance(pager, cli.Pager)
            self.assertEqual(pager.pager_cmd, "git_custom_pager")

    def test_pager_env_pager(self):
        """Test PAGER environment variable."""
        os.environ["PAGER"] = "my_pager"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager()
            self.assertIsInstance(pager, cli.Pager)
            self.assertEqual(pager.pager_cmd, "my_pager")

    def test_pager_env_priority(self):
        """Test environment variable priority order."""
        os.environ["PAGER"] = "pager_low"
        os.environ["GIT_PAGER"] = "pager_medium"
        os.environ["DULWICH_PAGER"] = "pager_high"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager()
            self.assertEqual(pager.pager_cmd, "pager_high")

    def test_pager_config_core_pager(self):
        """Test core.pager configuration."""
        config = MagicMock()
        config.get.return_value = b"config_pager"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager(config=config)
            self.assertIsInstance(pager, cli.Pager)
            self.assertEqual(pager.pager_cmd, "config_pager")
            config.get.assert_called_with(("core",), b"pager")

    def test_pager_config_core_pager_false(self):
        """Test core.pager=false disables pager."""
        config = MagicMock()
        config.get.return_value = b"false"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager(config=config)
            self.assertIsInstance(pager, cli._StreamContextAdapter)

    def test_pager_config_core_pager_empty(self):
        """Test core.pager="" disables pager."""
        config = MagicMock()
        config.get.return_value = b""
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager(config=config)
            self.assertIsInstance(pager, cli._StreamContextAdapter)

    def test_pager_config_per_command(self):
        """Test per-command pager configuration."""
        config = MagicMock()
        config.get.side_effect = lambda section, key: {
            (("pager",), b"log"): b"log_pager",
        }.get((section, key), KeyError())

        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager(config=config, cmd_name="log")
            self.assertIsInstance(pager, cli.Pager)
            self.assertEqual(pager.pager_cmd, "log_pager")

    def test_pager_config_per_command_false(self):
        """Test per-command pager=false disables pager."""
        config = MagicMock()
        config.get.return_value = b"false"
        with patch("sys.stdout.isatty", return_value=True):
            pager = cli.get_pager(config=config, cmd_name="log")
            self.assertIsInstance(pager, cli._StreamContextAdapter)

    def test_pager_config_per_command_true(self):
        """Test per-command pager=true uses default pager."""
        config = MagicMock()

        def get_side_effect(section, key):
            if section == ("pager",) and key == b"log":
                return b"true"
            raise KeyError

        config.get.side_effect = get_side_effect

        with patch("sys.stdout.isatty", return_value=True):
            with patch("shutil.which", side_effect=lambda cmd: cmd == "less"):
                pager = cli.get_pager(config=config, cmd_name="log")
                self.assertIsInstance(pager, cli.Pager)
                self.assertEqual(pager.pager_cmd, "less -FRX")

    def test_pager_priority_order(self):
        """Test complete priority order."""
        # Set up all possible configurations
        os.environ["PAGER"] = "env_pager"
        os.environ["GIT_PAGER"] = "env_git_pager"

        config = MagicMock()

        def get_side_effect(section, key):
            if section == ("pager",) and key == b"log":
                return b"cmd_pager"
            elif section == ("core",) and key == b"pager":
                return b"core_pager"
            raise KeyError

        config.get.side_effect = get_side_effect

        with patch("sys.stdout.isatty", return_value=True):
            # Per-command config should win
            pager = cli.get_pager(config=config, cmd_name="log")
            self.assertEqual(pager.pager_cmd, "cmd_pager")

    def test_pager_fallback_less(self):
        """Test fallback to less with proper flags."""
        with patch("sys.stdout.isatty", return_value=True):
            with patch("shutil.which", side_effect=lambda cmd: cmd == "less"):
                pager = cli.get_pager()
                self.assertIsInstance(pager, cli.Pager)
                self.assertEqual(pager.pager_cmd, "less -FRX")

    def test_pager_fallback_more(self):
        """Test fallback to more when less is not available."""
        with patch("sys.stdout.isatty", return_value=True):
            with patch("shutil.which", side_effect=lambda cmd: cmd == "more"):
                pager = cli.get_pager()
                self.assertIsInstance(pager, cli.Pager)
                self.assertEqual(pager.pager_cmd, "more")

    def test_pager_fallback_cat(self):
        """Test ultimate fallback to cat."""
        with patch("sys.stdout.isatty", return_value=True):
            with patch("shutil.which", return_value=None):
                pager = cli.get_pager()
                self.assertIsInstance(pager, cli.Pager)
                self.assertEqual(pager.pager_cmd, "cat")

    def test_pager_context_manager(self):
        """Test that pager works as a context manager."""
        with patch("sys.stdout.isatty", return_value=True):
            with cli.get_pager() as pager:
                self.assertTrue(hasattr(pager, "write"))
                self.assertTrue(hasattr(pager, "flush"))


class WorktreeCliTests(DulwichCliTestCase):
    """Tests for worktree CLI commands."""

    def setUp(self):
        super().setUp()
        # Base class already creates and initializes the repo
        # Just create initial commit
        with open(os.path.join(self.repo_path, "test.txt"), "w") as f:
            f.write("test content")
        from dulwich import porcelain

        porcelain.add(self.repo_path, ["test.txt"])
        porcelain.commit(self.repo_path, message=b"Initial commit")

    def test_worktree_list(self):
        """Test worktree list command."""
        # Change to repo directory
        old_cwd = os.getcwd()
        os.chdir(self.repo_path)
        try:
            io.StringIO()
            cmd = cli.cmd_worktree()
            result = cmd.run(["list"])

            # Should list the main worktree
            self.assertEqual(result, 0)
        finally:
            os.chdir(old_cwd)

    def test_worktree_add(self):
        """Test worktree add command."""
        wt_path = os.path.join(self.test_dir, "worktree1")

        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "worktree", "add", wt_path, "feature"
            )
            self.assertEqual(result, 0)
            self.assertTrue(os.path.exists(wt_path))
            log_output = "\n".join(cm.output)
            self.assertIn("Worktree added:", log_output)

    def test_worktree_add_detached(self):
        """Test worktree add with detached HEAD."""
        wt_path = os.path.join(self.test_dir, "detached-wt")

        # Change to repo directory
        old_cwd = os.getcwd()
        os.chdir(self.repo_path)
        try:
            cmd = cli.cmd_worktree()
            with patch("sys.stdout", new_callable=io.StringIO):
                result = cmd.run(["add", "--detach", wt_path])

            self.assertEqual(result, 0)
            self.assertTrue(os.path.exists(wt_path))
        finally:
            os.chdir(old_cwd)

    def test_worktree_remove(self):
        """Test worktree remove command."""
        # First add a worktree
        wt_path = os.path.join(self.test_dir, "to-remove")
        result, _stdout, _stderr = self._run_cli("worktree", "add", wt_path)
        self.assertEqual(result, 0)

        # Then remove it
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli("worktree", "remove", wt_path)
            self.assertEqual(result, 0)
            self.assertFalse(os.path.exists(wt_path))
            log_output = "\n".join(cm.output)
            self.assertIn("Worktree removed:", log_output)

    def test_worktree_prune(self):
        """Test worktree prune command."""
        # Add a worktree and manually remove it
        wt_path = os.path.join(self.test_dir, "to-prune")
        result, _stdout, _stderr = self._run_cli("worktree", "add", wt_path)
        self.assertEqual(result, 0)
        shutil.rmtree(wt_path)

        # Prune
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli("worktree", "prune", "-v")
            self.assertEqual(result, 0)
            log_output = "\n".join(cm.output)
            self.assertIn("to-prune", log_output)

    def test_worktree_lock_unlock(self):
        """Test worktree lock and unlock commands."""
        # Add a worktree
        wt_path = os.path.join(self.test_dir, "lockable")
        result, _stdout, _stderr = self._run_cli("worktree", "add", wt_path)
        self.assertEqual(result, 0)

        # Lock it
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "worktree", "lock", wt_path, "--reason", "Testing"
            )
            self.assertEqual(result, 0)
            log_output = "\n".join(cm.output)
            self.assertIn("Worktree locked:", log_output)

        # Unlock it
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli("worktree", "unlock", wt_path)
            self.assertEqual(result, 0)
            log_output = "\n".join(cm.output)
            self.assertIn("Worktree unlocked:", log_output)

    def test_worktree_move(self):
        """Test worktree move command."""
        # Add a worktree
        old_path = os.path.join(self.test_dir, "old-location")
        new_path = os.path.join(self.test_dir, "new-location")
        result, _stdout, _stderr = self._run_cli("worktree", "add", old_path)
        self.assertEqual(result, 0)

        # Move it
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli(
                "worktree", "move", old_path, new_path
            )
            self.assertEqual(result, 0)
            self.assertFalse(os.path.exists(old_path))
            self.assertTrue(os.path.exists(new_path))
            log_output = "\n".join(cm.output)
            self.assertIn("Worktree moved:", log_output)

    def test_worktree_invalid_command(self):
        """Test invalid worktree subcommand."""
        cmd = cli.cmd_worktree()
        with patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit):
                cmd.run(["invalid"])


class MergeBaseCommandTest(DulwichCliTestCase):
    """Tests for merge-base command."""

    def _create_commits(self):
        """Helper to create a commit history for testing."""
        # Create three commits in linear history
        for i in range(1, 4):
            test_file = os.path.join(self.repo_path, f"file{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"content{i}")
            self._run_cli("add", f"file{i}.txt")
            self._run_cli("commit", f"--message=Commit {i}")

    def test_merge_base_linear_history(self):
        """Test merge-base with linear history."""
        self._create_commits()

        result, stdout, _stderr = self._run_cli("merge-base", "HEAD", "HEAD~1")
        self.assertEqual(result, 0)

        # Should return HEAD~1 as the merge base
        output = stdout.strip()
        # Verify it's a valid commit ID (40 hex chars)
        self.assertEqual(len(output), 40)
        self.assertTrue(all(c in "0123456789abcdef" for c in output))

    def test_merge_base_is_ancestor_true(self):
        """Test merge-base --is-ancestor when true."""
        self._create_commits()

        result, _stdout, _stderr = self._run_cli(
            "merge-base", "--is-ancestor", "HEAD~1", "HEAD"
        )
        self.assertEqual(result, 0)  # Exit code 0 means true

    def test_merge_base_is_ancestor_false(self):
        """Test merge-base --is-ancestor when false."""
        self._create_commits()

        result, _stdout, _stderr = self._run_cli(
            "merge-base", "--is-ancestor", "HEAD", "HEAD~1"
        )
        self.assertEqual(result, 1)  # Exit code 1 means false

    def test_merge_base_independent(self):
        """Test merge-base --independent."""
        self._create_commits()

        # All three commits in linear history - only HEAD should be independent
        head = self.repo.refs[b"HEAD"]
        head_1 = self.repo[head].parents[0]
        head_2 = self.repo[head_1].parents[0]

        result, stdout, _stderr = self._run_cli(
            "merge-base",
            "--independent",
            head.decode(),
            head_1.decode(),
            head_2.decode(),
        )
        self.assertEqual(result, 0)

        # Only HEAD should be in output (as it's the only independent commit)
        lines = stdout.strip().split("\n")
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0], head.decode())

    def test_merge_base_requires_two_commits(self):
        """Test merge-base requires at least two commits."""
        self._create_commits()

        result, _stdout, _stderr = self._run_cli("merge-base", "HEAD")
        self.assertEqual(result, 1)

    def test_merge_base_is_ancestor_requires_two_commits(self):
        """Test merge-base --is-ancestor requires exactly two commits."""
        self._create_commits()

        result, _stdout, _stderr = self._run_cli("merge-base", "--is-ancestor", "HEAD")
        self.assertEqual(result, 1)


class ConfigCommandTest(DulwichCliTestCase):
    """Tests for config command."""

    def test_config_set_and_get(self):
        """Test setting and getting a config value."""
        # Set a config value
        result, stdout, _stderr = self._run_cli("config", "user.name", "Test User")
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "")

        # Get the value back
        result, stdout, _stderr = self._run_cli("config", "user.name")
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "Test User\n")

    def test_config_set_and_get_subsection(self):
        """Test setting and getting a config value with subsection."""
        # Set a config value with subsection (e.g., remote.origin.url)
        result, stdout, _stderr = self._run_cli(
            "config", "remote.origin.url", "https://example.com/repo.git"
        )
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "")

        # Get the value back
        result, stdout, _stderr = self._run_cli("config", "remote.origin.url")
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "https://example.com/repo.git\n")

    def test_config_list(self):
        """Test listing all config values."""
        # Set some config values
        self._run_cli("config", "user.name", "Test User")
        self._run_cli("config", "user.email", "test@example.com")

        # Get the actual config values that may vary by platform
        config = self.repo.get_config()
        filemode = config.get((b"core",), b"filemode")
        try:
            symlinks = config.get((b"core",), b"symlinks")
        except KeyError:
            symlinks = None

        # List all values
        result, stdout, _stderr = self._run_cli("config", "--list")
        self.assertEqual(result, 0)

        # Build expected output with platform-specific values
        expected = "core.repositoryformatversion=0\n"
        expected += f"core.filemode={filemode.decode('utf-8')}\n"
        if symlinks is not None:
            expected += f"core.symlinks={symlinks.decode('utf-8')}\n"
        expected += (
            "core.bare=false\n"
            "core.logallrefupdates=true\n"
            "user.name=Test User\n"
            "user.email=test@example.com\n"
        )

        self.assertEqual(stdout, expected)

    def test_config_unset(self):
        """Test unsetting a config value."""
        # Set a config value
        self._run_cli("config", "user.name", "Test User")

        # Verify it's set
        result, stdout, _stderr = self._run_cli("config", "user.name")
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "Test User\n")

        # Unset it
        result, stdout, _stderr = self._run_cli("config", "--unset", "user.name")
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "")

        # Verify it's gone
        result, stdout, _stderr = self._run_cli("config", "user.name")
        self.assertEqual(result, 1)
        self.assertEqual(stdout, "")

    def test_config_get_nonexistent(self):
        """Test getting a nonexistent config value."""
        result, stdout, _stderr = self._run_cli("config", "nonexistent.key")
        self.assertEqual(result, 1)
        self.assertEqual(stdout, "")

    def test_config_unset_nonexistent(self):
        """Test unsetting a nonexistent config value."""
        result, _stdout, _stderr = self._run_cli("config", "--unset", "nonexistent.key")
        self.assertEqual(result, 1)

    def test_config_invalid_key_format(self):
        """Test config with invalid key format."""
        result, stdout, _stderr = self._run_cli("config", "invalidkey")
        self.assertEqual(result, 1)
        self.assertEqual(stdout, "")

    def test_config_get_all(self):
        """Test getting all values for a multivar."""
        # Set multiple values for the same key
        config = self.repo.get_config()
        config.set(("test",), "multivar", "value1")
        config.add(("test",), "multivar", "value2")
        config.add(("test",), "multivar", "value3")
        config.write_to_path()

        # Get all values
        result, stdout, _stderr = self._run_cli("config", "--get-all", "test.multivar")
        self.assertEqual(result, 0)
        self.assertEqual(stdout, "value1\nvalue2\nvalue3\n")


class GitFlushTest(TestCase):
    """Tests for GIT_FLUSH environment variable support."""

    def test_should_auto_flush_with_git_flush_1(self):
        """Test that GIT_FLUSH=1 enables auto-flushing."""

        mock_stream = MagicMock()
        mock_stream.isatty.return_value = True

        self.assertTrue(_should_auto_flush(mock_stream, env={"GIT_FLUSH": "1"}))

    def test_should_auto_flush_with_git_flush_0(self):
        """Test that GIT_FLUSH=0 disables auto-flushing."""
        mock_stream = MagicMock()
        mock_stream.isatty.return_value = True

        self.assertFalse(_should_auto_flush(mock_stream, env={"GIT_FLUSH": "0"}))

    def test_should_auto_flush_auto_detect_tty(self):
        """Test that auto-detect returns False for TTY (no flush needed)."""
        mock_stream = MagicMock()
        mock_stream.isatty.return_value = True

        self.assertFalse(_should_auto_flush(mock_stream, env={}))

    def test_should_auto_flush_auto_detect_pipe(self):
        """Test that auto-detect returns True for pipes (flush needed)."""
        mock_stream = MagicMock()
        mock_stream.isatty.return_value = False

        self.assertTrue(_should_auto_flush(mock_stream, env={}))

    def test_text_wrapper_flushes_on_write(self):
        """Test that AutoFlushTextIOWrapper flushes after write."""
        mock_stream = MagicMock()
        wrapper = AutoFlushTextIOWrapper(mock_stream)

        wrapper.write("test")
        mock_stream.write.assert_called_once_with("test")
        mock_stream.flush.assert_called_once()

    def test_text_wrapper_flushes_on_writelines(self):
        """Test that AutoFlushTextIOWrapper flushes after writelines."""
        from dulwich.cli import AutoFlushTextIOWrapper

        mock_stream = MagicMock()
        wrapper = AutoFlushTextIOWrapper(mock_stream)

        wrapper.writelines(["line1\n", "line2\n"])
        mock_stream.writelines.assert_called_once()
        mock_stream.flush.assert_called_once()

    def test_binary_wrapper_flushes_on_write(self):
        """Test that AutoFlushBinaryIOWrapper flushes after write."""
        mock_stream = MagicMock()
        wrapper = AutoFlushBinaryIOWrapper(mock_stream)

        wrapper.write(b"test")
        mock_stream.write.assert_called_once_with(b"test")
        mock_stream.flush.assert_called_once()

    def test_text_wrapper_env_classmethod(self):
        """Test that AutoFlushTextIOWrapper.env() respects GIT_FLUSH."""
        mock_stream = MagicMock()
        mock_stream.isatty.return_value = False

        wrapper = AutoFlushTextIOWrapper.env(mock_stream, env={"GIT_FLUSH": "1"})
        self.assertIsInstance(wrapper, AutoFlushTextIOWrapper)

        wrapper = AutoFlushTextIOWrapper.env(mock_stream, env={"GIT_FLUSH": "0"})
        self.assertIs(mock_stream, wrapper)

    def test_binary_wrapper_env_classmethod(self):
        """Test that AutoFlushBinaryIOWrapper.env() respects GIT_FLUSH."""
        mock_stream = MagicMock()
        mock_stream.isatty.return_value = False

        wrapper = AutoFlushBinaryIOWrapper.env(mock_stream, env={"GIT_FLUSH": "1"})
        self.assertIsInstance(wrapper, AutoFlushBinaryIOWrapper)

        wrapper = AutoFlushBinaryIOWrapper.env(mock_stream, env={"GIT_FLUSH": "0"})
        self.assertIs(wrapper, mock_stream)

    def test_wrapper_delegates_attributes(self):
        """Test that wrapper delegates unknown attributes to stream."""
        mock_stream = MagicMock()
        mock_stream.encoding = "utf-8"
        wrapper = AutoFlushTextIOWrapper(mock_stream)

        self.assertEqual(wrapper.encoding, "utf-8")

    def test_wrapper_context_manager(self):
        """Test that wrapper supports context manager protocol."""
        mock_stream = MagicMock()
        wrapper = AutoFlushTextIOWrapper(mock_stream)

        with wrapper as w:
            self.assertIs(w, wrapper)


class MaintenanceCommandTest(DulwichCliTestCase):
    """Tests for maintenance command."""

    def setUp(self):
        super().setUp()
        # Set up a temporary HOME for testing global config
        self.temp_home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_home)
        self.overrideEnv("HOME", self.temp_home)

    def test_maintenance_run_default(self):
        """Test maintenance run with default tasks."""
        result, _stdout, _stderr = self._run_cli("maintenance", "run")
        self.assertIsNone(result)

    def test_maintenance_run_specific_task(self):
        """Test maintenance run with a specific task."""
        result, _stdout, _stderr = self._run_cli(
            "maintenance", "run", "--task", "pack-refs"
        )
        self.assertIsNone(result)

    def test_maintenance_run_multiple_tasks(self):
        """Test maintenance run with multiple specific tasks."""
        result, _stdout, _stderr = self._run_cli(
            "maintenance", "run", "--task", "pack-refs", "--task", "gc"
        )
        self.assertIsNone(result)

    def test_maintenance_run_quiet(self):
        """Test maintenance run with quiet flag."""
        result, _stdout, _stderr = self._run_cli("maintenance", "run", "--quiet")
        self.assertIsNone(result)

    def test_maintenance_run_auto(self):
        """Test maintenance run with auto flag."""
        result, _stdout, _stderr = self._run_cli("maintenance", "run", "--auto")
        self.assertIsNone(result)

    def test_maintenance_no_subcommand(self):
        """Test maintenance command without subcommand shows help."""
        result, _stdout, _stderr = self._run_cli("maintenance")
        self.assertEqual(result, 1)

    def test_maintenance_register(self):
        """Test maintenance register subcommand."""
        result, _stdout, _stderr = self._run_cli("maintenance", "register")
        self.assertIsNone(result)

    def test_maintenance_unregister(self):
        """Test maintenance unregister subcommand."""
        # First register
        _result, _stdout, _stderr = self._run_cli("maintenance", "register")

        # Then unregister
        result, _stdout, _stderr = self._run_cli("maintenance", "unregister")
        self.assertIsNone(result)

    def test_maintenance_unregister_not_registered(self):
        """Test unregistering a repository that is not registered."""
        result, _stdout, _stderr = self._run_cli("maintenance", "unregister")
        self.assertEqual(result, 1)

    def test_maintenance_unregister_force(self):
        """Test unregistering with --force flag."""
        result, _stdout, _stderr = self._run_cli("maintenance", "unregister", "--force")
        self.assertIsNone(result)

    def test_maintenance_unimplemented_subcommand(self):
        """Test unimplemented maintenance subcommands."""
        for subcommand in ["start", "stop"]:
            result, _stdout, _stderr = self._run_cli("maintenance", subcommand)
            self.assertEqual(result, 1)


class InterpretTrailersCommandTest(DulwichCliTestCase):
    """Tests for interpret-trailers command."""

    def test_parse_trailers_from_file(self):
        """Test parsing trailers from a file."""
        # Create a message file with trailers
        msg_file = os.path.join(self.test_dir, "message.txt")
        with open(msg_file, "wb") as f:
            f.write(b"Subject\n\nBody\n\nSigned-off-by: Alice <alice@example.com>\n")

        result, stdout, _stderr = self._run_cli(
            "interpret-trailers", "--only-trailers", msg_file
        )
        self.assertIsNone(result)
        self.assertIn("Signed-off-by: Alice <alice@example.com>", stdout)

    def test_add_trailer_to_message(self):
        """Test adding a trailer to a message."""
        msg_file = os.path.join(self.test_dir, "message.txt")
        with open(msg_file, "wb") as f:
            f.write(b"Subject\n\nBody text\n")

        result, stdout, _stderr = self._run_cli(
            "interpret-trailers",
            "--trailer",
            "Signed-off-by:Alice <alice@example.com>",
            msg_file,
        )
        self.assertIsNone(result)
        self.assertIn("Signed-off-by: Alice <alice@example.com>", stdout)
        self.assertIn("Subject", stdout)
        self.assertIn("Body text", stdout)

    def test_add_multiple_trailers(self):
        """Test adding multiple trailers."""
        msg_file = os.path.join(self.test_dir, "message.txt")
        with open(msg_file, "wb") as f:
            f.write(b"Subject\n\nBody\n")

        result, stdout, _stderr = self._run_cli(
            "interpret-trailers",
            "--trailer",
            "Signed-off-by:Alice",
            "--trailer",
            "Reviewed-by:Bob",
            msg_file,
        )
        self.assertIsNone(result)
        self.assertIn("Signed-off-by: Alice", stdout)
        self.assertIn("Reviewed-by: Bob", stdout)

    def test_parse_shorthand(self):
        """Test --parse shorthand option."""
        msg_file = os.path.join(self.test_dir, "message.txt")
        with open(msg_file, "wb") as f:
            f.write(b"Subject\n\nBody\n\nSigned-off-by: Alice\n")

        result, stdout, _stderr = self._run_cli(
            "interpret-trailers", "--parse", msg_file
        )
        self.assertIsNone(result)
        # --parse is shorthand for --only-trailers --only-input --unfold
        self.assertEqual(stdout, "Signed-off-by: Alice\n")

    def test_trim_empty(self):
        """Test --trim-empty option."""
        msg_file = os.path.join(self.test_dir, "message.txt")
        with open(msg_file, "wb") as f:
            f.write(b"Subject\n\nBody\n\nSigned-off-by: Alice\nReviewed-by: \n")

        result, stdout, _stderr = self._run_cli(
            "interpret-trailers", "--trim-empty", "--only-trailers", msg_file
        )
        self.assertIsNone(result)
        self.assertIn("Signed-off-by: Alice", stdout)
        self.assertNotIn("Reviewed-by:", stdout)

    def test_if_exists_replace(self):
        """Test --if-exists replace option."""
        msg_file = os.path.join(self.test_dir, "message.txt")
        with open(msg_file, "wb") as f:
            f.write(b"Subject\n\nBody\n\nSigned-off-by: Alice\n")

        result, stdout, _stderr = self._run_cli(
            "interpret-trailers",
            "--if-exists",
            "replace",
            "--trailer",
            "Signed-off-by:Bob",
            msg_file,
        )
        self.assertIsNone(result)
        self.assertIn("Signed-off-by: Bob", stdout)
        self.assertNotIn("Alice", stdout)

    def test_trailer_with_equals(self):
        """Test trailer with equals separator."""
        msg_file = os.path.join(self.test_dir, "message.txt")
        with open(msg_file, "wb") as f:
            f.write(b"Subject\n\nBody\n")

        result, stdout, _stderr = self._run_cli(
            "interpret-trailers", "--trailer", "Bug=12345", msg_file
        )
        self.assertIsNone(result)
        self.assertIn("Bug: 12345", stdout)


class ReplaceCommandTest(DulwichCliTestCase):
    """Tests for replace command."""

    def test_replace_create(self):
        """Test creating a replacement ref."""
        # Create two commits
        [c1, c2] = build_commit_graph(self.repo.object_store, [[1], [2]])
        self.repo[b"HEAD"] = c1.id

        # Create replacement using the create form (decode to string for CLI)
        c1_str = c1.id.decode("ascii")
        c2_str = c2.id.decode("ascii")
        _result, _stdout, _stderr = self._run_cli("replace", c1_str, c2_str)

        # Verify the replacement ref was created
        replace_ref = b"refs/replace/" + c1.id
        self.assertIn(replace_ref, self.repo.refs.keys())
        self.assertEqual(c2.id, self.repo.refs[replace_ref])

    def test_replace_list_empty(self):
        """Test listing replacements when there are none."""
        _result, stdout, _stderr = self._run_cli("replace", "list")
        self.assertEqual("", stdout)

    def test_replace_list(self):
        """Test listing replacement refs."""
        # Create two commits
        [c1, c2] = build_commit_graph(self.repo.object_store, [[1], [2]])
        self.repo[b"HEAD"] = c1.id

        # Create replacement
        c1_str = c1.id.decode("ascii")
        c2_str = c2.id.decode("ascii")
        self._run_cli("replace", c1_str, c2_str)

        # List replacements
        _result, stdout, _stderr = self._run_cli("replace", "list")
        self.assertIn(c1_str, stdout)
        self.assertIn(c2_str, stdout)

    def test_replace_default_list(self):
        """Test that replace without subcommand defaults to list."""
        # Create two commits
        [c1, c2] = build_commit_graph(self.repo.object_store, [[1], [2]])
        self.repo[b"HEAD"] = c1.id

        # Create replacement
        c1_str = c1.id.decode("ascii")
        c2_str = c2.id.decode("ascii")
        self._run_cli("replace", c1_str, c2_str)

        # Call replace without subcommand (should list)
        _result, stdout, _stderr = self._run_cli("replace")
        self.assertIn(c1_str, stdout)
        self.assertIn(c2_str, stdout)

    def test_replace_delete(self):
        """Test deleting a replacement ref."""
        # Create two commits
        [c1, c2] = build_commit_graph(self.repo.object_store, [[1], [2]])
        self.repo[b"HEAD"] = c1.id

        # Create replacement
        c1_str = c1.id.decode("ascii")
        c2_str = c2.id.decode("ascii")
        self._run_cli("replace", c1_str, c2_str)

        # Verify it exists
        replace_ref = b"refs/replace/" + c1.id
        self.assertIn(replace_ref, self.repo.refs.keys())

        # Delete the replacement
        _result, _stdout, _stderr = self._run_cli("replace", "delete", c1_str)

        # Verify it's gone
        self.assertNotIn(replace_ref, self.repo.refs.keys())

    def test_replace_delete_nonexistent(self):
        """Test deleting a nonexistent replacement ref fails."""
        # Create a commit
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id

        # Try to delete a non-existent replacement
        c1_str = c1.id.decode("ascii")
        result, _stdout, _stderr = self._run_cli("replace", "delete", c1_str)
        self.assertEqual(result, 1)


class StripspaceCommandTest(DulwichCliTestCase):
    """Tests for stripspace command."""

    def test_stripspace_from_file(self):
        """Test stripspace from a file."""
        # Create a text file with whitespace issues
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"  hello  \n\n\n\n  world  \n\n")

        result, stdout, _stderr = self._run_cli("stripspace", text_file)
        self.assertIsNone(result)
        self.assertEqual(stdout, "hello\n\nworld\n")

    def test_stripspace_simple(self):
        """Test basic stripspace functionality."""
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"hello\nworld\n")

        result, stdout, _stderr = self._run_cli("stripspace", text_file)
        self.assertIsNone(result)
        self.assertEqual(stdout, "hello\nworld\n")

    def test_stripspace_trailing_whitespace(self):
        """Test that trailing whitespace is removed."""
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"hello  \nworld\t\n")

        result, stdout, _stderr = self._run_cli("stripspace", text_file)
        self.assertIsNone(result)
        self.assertEqual(stdout, "hello\nworld\n")

    def test_stripspace_strip_comments(self):
        """Test stripping comments."""
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"# comment\nhello\n# another comment\nworld\n")

        result, stdout, _stderr = self._run_cli(
            "stripspace", "--strip-comments", text_file
        )
        self.assertIsNone(result)
        self.assertEqual(stdout, "hello\nworld\n")

    def test_stripspace_comment_lines(self):
        """Test prepending comment character."""
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"hello\nworld\n")

        result, stdout, _stderr = self._run_cli(
            "stripspace", "--comment-lines", text_file
        )
        self.assertIsNone(result)
        self.assertEqual(stdout, "# hello\n# world\n")

    def test_stripspace_custom_comment_char(self):
        """Test using custom comment character."""
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"; comment\nhello\n; another comment\nworld\n")

        result, stdout, _stderr = self._run_cli(
            "stripspace", "--strip-comments", "--comment-char", ";", text_file
        )
        self.assertIsNone(result)
        self.assertEqual(stdout, "hello\nworld\n")

    def test_stripspace_leading_trailing_blanks(self):
        """Test removing leading and trailing blank lines."""
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"\n\nhello\nworld\n\n\n")

        result, stdout, _stderr = self._run_cli("stripspace", text_file)
        self.assertIsNone(result)
        self.assertEqual(stdout, "hello\nworld\n")

    def test_stripspace_collapse_blank_lines(self):
        """Test collapsing multiple blank lines."""
        text_file = os.path.join(self.test_dir, "message.txt")
        with open(text_file, "wb") as f:
            f.write(b"hello\n\n\n\nworld\n")

        result, stdout, _stderr = self._run_cli("stripspace", text_file)
        self.assertIsNone(result)
        self.assertEqual(stdout, "hello\n\nworld\n")


class ColumnCommandTest(DulwichCliTestCase):
    """Tests for column command."""

    def test_column_mode_default(self):
        """Test column mode (default) - fills columns first."""
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n")
            result, stdout, _stderr = self._run_cli("column", "--width", "40")
            self.assertIsNone(result)
            # In column mode, items go down then across
            # With 12 items and width 40, should fit in multiple columns
            lines = stdout.strip().split("\n")
            # First line should start with "1"
            self.assertTrue(lines[0].startswith("1"))
        finally:
            sys.stdin = old_stdin

    def test_column_mode_row(self):
        """Test row mode - fills rows first."""
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("1\n2\n3\n4\n5\n6\n")
            result, stdout, _stderr = self._run_cli(
                "column", "--mode", "row", "--width", "40"
            )
            self.assertIsNone(result)
            # In row mode, items go across then down
            # Should have items 1, 2, 3... on first line
            lines = stdout.strip().split("\n")
            self.assertTrue("1" in lines[0])
            self.assertTrue("2" in lines[0])
        finally:
            sys.stdin = old_stdin

    def test_column_mode_plain(self):
        """Test plain mode - one item per line."""
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("apple\nbanana\ncherry\n")
            result, stdout, _stderr = self._run_cli("column", "--mode", "plain")
            self.assertIsNone(result)
            self.assertEqual(stdout, "apple\nbanana\ncherry\n")
        finally:
            sys.stdin = old_stdin

    def test_column_padding(self):
        """Test custom padding between columns."""
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("a\nb\nc\nd\ne\nf\n")
            result, stdout, _stderr = self._run_cli(
                "column", "--mode", "row", "--padding", "5", "--width", "80"
            )
            self.assertIsNone(result)
            # With padding=5, should have 5 spaces between items
            self.assertIn("     ", stdout)
        finally:
            sys.stdin = old_stdin

    def test_column_indent(self):
        """Test indent prepended to each line."""
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("apple\nbanana\n")
            result, stdout, _stderr = self._run_cli(
                "column", "--mode", "plain", "--indent", "  "
            )
            self.assertIsNone(result)
            lines = stdout.split("\n")
            self.assertTrue(lines[0].startswith("  apple"))
            self.assertTrue(lines[1].startswith("  banana"))
        finally:
            sys.stdin = old_stdin

    def test_column_empty_input(self):
        """Test with empty input."""
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("")
            result, stdout, _stderr = self._run_cli("column")
            self.assertIsNone(result)
            self.assertEqual(stdout, "")
        finally:
            sys.stdin = old_stdin

    def test_column_single_item(self):
        """Test with single item."""
        old_stdin = sys.stdin
        try:
            sys.stdin = io.StringIO("single\n")
            result, stdout, _stderr = self._run_cli("column")
            self.assertIsNone(result)
            self.assertEqual(stdout, "single\n")
        finally:
            sys.stdin = old_stdin


class MailinfoCommandTests(DulwichCliTestCase):
    """Tests for the mailinfo command."""

    def test_mailinfo_basic(self):
        """Test basic mailinfo command."""
        email_content = b"""From: Test User <test@example.com>
Subject: [PATCH] Add feature
Date: Mon, 1 Jan 2024 12:00:00 +0000

This is the commit message.

---
diff --git a/file.txt b/file.txt
"""
        email_file = os.path.join(self.test_dir, "email.txt")
        with open(email_file, "wb") as f:
            f.write(email_content)

        msg_file = os.path.join(self.test_dir, "msg")
        patch_file = os.path.join(self.test_dir, "patch")

        result, stdout, _stderr = self._run_cli(
            "mailinfo", msg_file, patch_file, email_file
        )
        self.assertIsNone(result)

        # Check stdout contains author info
        self.assertIn("Author: Test User", stdout)
        self.assertIn("Email: test@example.com", stdout)
        self.assertIn("Subject: Add feature", stdout)

        # Check files were written
        self.assertTrue(os.path.exists(msg_file))
        self.assertTrue(os.path.exists(patch_file))

        # Check file contents
        with open(msg_file) as f:
            msg_content = f.read()
            self.assertIn("This is the commit message.", msg_content)

        with open(patch_file) as f:
            patch_content = f.read()
            self.assertIn("diff --git", patch_content)

    def test_mailinfo_keep_subject(self):
        """Test mailinfo with -k flag."""
        email_content = b"""From: Test <test@example.com>
Subject: [PATCH 1/2] Feature

Body
"""
        email_file = os.path.join(self.test_dir, "email.txt")
        with open(email_file, "wb") as f:
            f.write(email_content)

        msg_file = os.path.join(self.test_dir, "msg")
        patch_file = os.path.join(self.test_dir, "patch")

        result, stdout, _stderr = self._run_cli(
            "mailinfo", "-k", msg_file, patch_file, email_file
        )
        self.assertIsNone(result)
        self.assertIn("Subject: [PATCH 1/2] Feature", stdout)

    def test_mailinfo_keep_non_patch(self):
        """Test mailinfo with -b flag."""
        email_content = b"""From: Test <test@example.com>
Subject: [RFC][PATCH] Feature

Body
"""
        email_file = os.path.join(self.test_dir, "email.txt")
        with open(email_file, "wb") as f:
            f.write(email_content)

        msg_file = os.path.join(self.test_dir, "msg")
        patch_file = os.path.join(self.test_dir, "patch")

        result, stdout, _stderr = self._run_cli(
            "mailinfo", "-b", msg_file, patch_file, email_file
        )
        self.assertIsNone(result)
        self.assertIn("Subject: [RFC] Feature", stdout)

    def test_mailinfo_scissors(self):
        """Test mailinfo with --scissors flag."""
        email_content = b"""From: Test <test@example.com>
Subject: Test

Ignore this part

-- >8 --

Keep this part
"""
        email_file = os.path.join(self.test_dir, "email.txt")
        with open(email_file, "wb") as f:
            f.write(email_content)

        msg_file = os.path.join(self.test_dir, "msg")
        patch_file = os.path.join(self.test_dir, "patch")

        result, _stdout, _stderr = self._run_cli(
            "mailinfo", "--scissors", msg_file, patch_file, email_file
        )
        self.assertIsNone(result)

        # Check message file
        with open(msg_file) as f:
            msg_content = f.read()
            self.assertIn("Keep this part", msg_content)
            self.assertNotIn("Ignore this part", msg_content)

    def test_mailinfo_message_id(self):
        """Test mailinfo with -m flag."""
        email_content = b"""From: Test <test@example.com>
Subject: Test
Message-ID: <test123@example.com>

Body
"""
        email_file = os.path.join(self.test_dir, "email.txt")
        with open(email_file, "wb") as f:
            f.write(email_content)

        msg_file = os.path.join(self.test_dir, "msg")
        patch_file = os.path.join(self.test_dir, "patch")

        result, _stdout, _stderr = self._run_cli(
            "mailinfo", "-m", msg_file, patch_file, email_file
        )
        self.assertIsNone(result)

        # Check message file contains Message-ID
        with open(msg_file) as f:
            msg_content = f.read()
            self.assertIn("Message-ID:", msg_content)

    def test_mailinfo_encoding(self):
        """Test mailinfo with --encoding flag."""
        email_content = (
            b"From: Test <test@example.com>\n"
            b"Subject: Test\n"
            b"Content-Type: text/plain; charset=utf-8\n"
            b"\n"
            b"Body with UTF-8: " + "naïve".encode() + b"\n"
        )
        email_file = os.path.join(self.test_dir, "email.txt")
        with open(email_file, "wb") as f:
            f.write(email_content)

        msg_file = os.path.join(self.test_dir, "msg")
        patch_file = os.path.join(self.test_dir, "patch")

        result, _stdout, _stderr = self._run_cli(
            "mailinfo", "--encoding", "utf-8", msg_file, patch_file, email_file
        )
        self.assertIsNone(result)

        # Just verify the command runs successfully
        with open(msg_file) as f:
            msg_content = f.read()
            self.assertIn("Body", msg_content)


class DiagnoseCommandTest(DulwichCliTestCase):
    """Tests for diagnose command."""

    def test_diagnose(self):
        """Test the diagnose command."""
        with self.assertLogs("dulwich.cli", level="INFO") as cm:
            result, _stdout, _stderr = self._run_cli("diagnose")
            self.assertIsNone(result)

            # Check that key information is present in log output
            log_output = "\n".join(cm.output)
            self.assertIn("Python version:", log_output)
            self.assertIn("Python executable:", log_output)
            self.assertIn("PYTHONPATH:", log_output)
            self.assertIn("sys.path:", log_output)
            self.assertIn("Dulwich version:", log_output)
            self.assertIn("Installed dependencies:", log_output)

            # Check that at least core dependencies are listed
            self.assertIn("urllib3:", log_output)


if __name__ == "__main__":
    unittest.main()
