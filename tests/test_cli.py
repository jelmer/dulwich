#!/usr/bin/env python
# test_cli.py -- tests for dulwich.cli
# vim: expandtab
#
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.cli."""

import io
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from dulwich import cli
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
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_cwd = os.getcwd()
        try:
            sys.stdout = stdout_stream or io.StringIO()
            sys.stderr = io.StringIO()
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
        result, stdout, stderr = self._run_cli("init", new_repo_path)
        self.assertTrue(os.path.exists(os.path.join(new_repo_path, ".git")))

    def test_init_bare(self):
        # Create a new directory for bare repo
        bare_repo_path = os.path.join(self.test_dir, "bare_repo")
        result, stdout, stderr = self._run_cli("init", "--bare", bare_repo_path)
        self.assertTrue(os.path.exists(os.path.join(bare_repo_path, "HEAD")))
        self.assertFalse(os.path.exists(os.path.join(bare_repo_path, ".git")))


class AddCommandTest(DulwichCliTestCase):
    """Tests for add command."""

    def test_add_single_file(self):
        # Create a file to add
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        result, stdout, stderr = self._run_cli("add", "test.txt")
        # Check that file is in index
        self.assertIn(b"test.txt", self.repo.open_index())

    def test_add_multiple_files(self):
        # Create multiple files
        for i in range(3):
            test_file = os.path.join(self.repo_path, f"test{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"content {i}")

        result, stdout, stderr = self._run_cli(
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
        result, stdout, stderr = self._run_cli("rm", "test.txt")
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
        result, stdout, stderr = self._run_cli("commit", "--message=Initial commit")
        # Check that HEAD points to a commit
        self.assertIsNotNone(self.repo.head())


class LogCommandTest(DulwichCliTestCase):
    """Tests for log command."""

    def test_log_empty_repo(self):
        result, stdout, stderr = self._run_cli("log")
        # Empty repo should not crash

    def test_log_with_commits(self):
        # Create some commits
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        result, stdout, stderr = self._run_cli("log")
        self.assertIn("Commit 3", stdout)
        self.assertIn("Commit 2", stdout)
        self.assertIn("Commit 1", stdout)

    def test_log_reverse(self):
        # Create some commits
        c1, c2, c3 = build_commit_graph(
            self.repo.object_store, [[1], [2, 1], [3, 1, 2]]
        )
        self.repo.refs[b"HEAD"] = c3.id

        result, stdout, stderr = self._run_cli("log", "--reverse")
        # Check order - commit 1 should appear before commit 3
        pos1 = stdout.index("Commit 1")
        pos3 = stdout.index("Commit 3")
        self.assertLess(pos1, pos3)


class StatusCommandTest(DulwichCliTestCase):
    """Tests for status command."""

    def test_status_empty(self):
        result, stdout, stderr = self._run_cli("status")
        # Should not crash on empty repo

    def test_status_with_untracked(self):
        # Create an untracked file
        test_file = os.path.join(self.repo_path, "untracked.txt")
        with open(test_file, "w") as f:
            f.write("untracked content")

        result, stdout, stderr = self._run_cli("status")
        self.assertIn("Untracked files:", stdout)
        self.assertIn("untracked.txt", stdout)


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
        result, stdout, stderr = self._run_cli("branch", "test-branch")
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
        result, stdout, stderr = self._run_cli("branch", "-d", "test-branch")
        self.assertNotIn(b"refs/heads/test-branch", self.repo.refs.keys())


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
        result, stdout, stderr = self._run_cli("checkout", "test-branch")
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
        result, stdout, stderr = self._run_cli("tag", "v1.0")
        self.assertIn(b"refs/tags/v1.0", self.repo.refs.keys())


class ShowCommandTest(DulwichCliTestCase):
    """Tests for show command."""

    def test_show_commit(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Test commit")

        result, stdout, stderr = self._run_cli("show", "HEAD")
        self.assertIn("Test commit", stdout)


class FetchPackCommandTest(DulwichCliTestCase):
    """Tests for fetch-pack command."""

    @patch("dulwich.cli.get_transport_and_path")
    def test_fetch_pack_basic(self, mock_transport):
        # Mock the transport
        mock_client = MagicMock()
        mock_transport.return_value = (mock_client, "/path/to/repo")
        mock_client.fetch.return_value = None

        result, stdout, stderr = self._run_cli(
            "fetch-pack", "git://example.com/repo.git"
        )
        mock_client.fetch.assert_called_once()


class PullCommandTest(DulwichCliTestCase):
    """Tests for pull command."""

    @patch("dulwich.porcelain.pull")
    def test_pull_basic(self, mock_pull):
        result, stdout, stderr = self._run_cli("pull", "origin")
        mock_pull.assert_called_once()

    @patch("dulwich.porcelain.pull")
    def test_pull_with_refspec(self, mock_pull):
        result, stdout, stderr = self._run_cli("pull", "origin", "master")
        mock_pull.assert_called_once()


class PushCommandTest(DulwichCliTestCase):
    """Tests for push command."""

    @patch("dulwich.porcelain.push")
    def test_push_basic(self, mock_push):
        result, stdout, stderr = self._run_cli("push", "origin")
        mock_push.assert_called_once()

    @patch("dulwich.porcelain.push")
    def test_push_force(self, mock_push):
        result, stdout, stderr = self._run_cli("push", "-f", "origin")
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
        result, stdout, stderr = self._run_cli(
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

        result, stdout, stderr = self._run_cli("for-each-ref")
        self.assertIn("refs/heads/master", stdout)


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

        result, stdout, stderr = self._run_cli("pack-refs", "--all")
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

        result, stdout, stderr = self._run_cli("submodule")
        # Should not crash on repo without submodules

    def test_submodule_init(self):
        # Create .gitmodules file for init to work
        gitmodules = os.path.join(self.repo_path, ".gitmodules")
        with open(gitmodules, "w") as f:
            f.write("")  # Empty .gitmodules file

        result, stdout, stderr = self._run_cli("submodule", "init")
        # Should not crash


class StashCommandTest(DulwichCliTestCase):
    """Tests for stash commands."""

    def test_stash_list_empty(self):
        result, stdout, stderr = self._run_cli("stash", "list")
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
        result, stdout, stderr = self._run_cli("stash", "push")
        self.assertIn("Saved working directory", stdout)

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
        result, stdout, stderr = self._run_cli("merge", "feature")


class HelpCommandTest(DulwichCliTestCase):
    """Tests for help command."""

    def test_help_basic(self):
        result, stdout, stderr = self._run_cli("help")
        self.assertIn("dulwich command line tool", stdout)

    def test_help_all(self):
        result, stdout, stderr = self._run_cli("help", "-a")
        self.assertIn("Available commands:", stdout)
        self.assertIn("add", stdout)
        self.assertIn("commit", stdout)


class RemoteCommandTest(DulwichCliTestCase):
    """Tests for remote commands."""

    def test_remote_add(self):
        result, stdout, stderr = self._run_cli(
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

        result, stdout, stderr = self._run_cli("check-ignore", "test.log", "test.txt")
        self.assertIn("test.log", stdout)
        self.assertNotIn("test.txt", stdout)


class LsFilesCommandTest(DulwichCliTestCase):
    """Tests for ls-files command."""

    def test_ls_files(self):
        # Add some files
        for name in ["a.txt", "b.txt", "c.txt"]:
            path = os.path.join(self.repo_path, name)
            with open(path, "w") as f:
                f.write(f"content of {name}")
        self._run_cli("add", "a.txt", "b.txt", "c.txt")

        result, stdout, stderr = self._run_cli("ls-files")
        self.assertIn("a.txt", stdout)
        self.assertIn("b.txt", stdout)
        self.assertIn("c.txt", stdout)


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

        result, stdout, stderr = self._run_cli("ls-tree", "HEAD")
        self.assertIn("file.txt", stdout)
        self.assertIn("subdir", stdout)

    def test_ls_tree_recursive(self):
        # Create nested structure
        os.mkdir(os.path.join(self.repo_path, "subdir"))
        with open(os.path.join(self.repo_path, "subdir", "nested.txt"), "w") as f:
            f.write("nested")

        self._run_cli("add", ".")
        self._run_cli("commit", "--message=Initial")

        result, stdout, stderr = self._run_cli("ls-tree", "-r", "HEAD")
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

        result, stdout, stderr = self._run_cli("describe")
        self.assertIn("v1.0", stdout)


class FsckCommandTest(DulwichCliTestCase):
    """Tests for fsck command."""

    def test_fsck(self):
        # Create a commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        self._run_cli("add", "test.txt")
        self._run_cli("commit", "--message=Initial")

        result, stdout, stderr = self._run_cli("fsck")
        # Should complete without errors


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

        result, stdout, stderr = self._run_cli("repack")
        # Should create pack files
        pack_dir = os.path.join(self.repo_path, ".git", "objects", "pack")
        self.assertTrue(any(f.endswith(".pack") for f in os.listdir(pack_dir)))


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
        result, stdout, stderr = self._run_cli("reset", "--soft", first_commit.decode())
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

        result, stdout, stderr = self._run_cli("write-tree")
        # Should output tree SHA
        self.assertEqual(len(stdout.strip()), 40)


class UpdateServerInfoCommandTest(DulwichCliTestCase):
    """Tests for update-server-info command."""

    def test_update_server_info(self):
        result, stdout, stderr = self._run_cli("update-server-info")
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

        result, stdout, stderr = self._run_cli(
            "symbolic-ref", "HEAD", "refs/heads/test-branch"
        )
        # HEAD should now point to test-branch
        self.assertEqual(
            self.repo.refs.read_ref(b"HEAD"), b"ref: refs/heads/test-branch"
        )


if __name__ == "__main__":
    unittest.main()
