# test_cli_merge.py -- Tests for dulwich merge CLI command
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

"""Tests for dulwich merge CLI command."""

import importlib.util
import os
import tempfile
import unittest

from dulwich import porcelain
from dulwich.cli import main

from . import DependencyMissing, TestCase


class CLIMergeTests(TestCase):
    """Tests for the dulwich merge CLI command."""

    def test_merge_fast_forward(self):
        """Test CLI merge with fast-forward."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master
            porcelain.checkout(tmpdir, "master")

            # Test merge via CLI
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="INFO") as cm:
                    ret = main(["merge", "feature"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 0)  # Success
                self.assertIn("Merge successful", log_output)

                # Check that file2.txt exists
                self.assertTrue(os.path.exists(os.path.join(tmpdir, "file2.txt")))
            finally:
                os.chdir(old_cwd)

    def test_merge_with_conflicts(self):
        """Test CLI merge with conflicts."""

        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch and modify file1
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout(tmpdir, "feature")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Modify file1 in feature")

            # Go back to master and modify file1 differently
            porcelain.checkout(tmpdir, "master")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Master content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Modify file1 in master")

            # Test merge via CLI - should exit with error
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="WARNING") as cm:
                    retcode = main(["merge", "feature"])
                    self.assertEqual(retcode, 1)
                    log_output = "\n".join(cm.output)

                self.assertIn("Merge conflicts", log_output)
                self.assertIn("file1.txt", log_output)
            finally:
                os.chdir(old_cwd)

    def test_merge_already_up_to_date(self):
        """Test CLI merge when already up to date."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Test merge via CLI
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="INFO") as cm:
                    ret = main(["merge", "HEAD"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 0)  # Success
                self.assertIn("Already up to date", log_output)
            finally:
                os.chdir(old_cwd)

    def test_merge_no_commit(self):
        """Test CLI merge with --no-commit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master and add another file
            porcelain.checkout(tmpdir, "master")
            with open(os.path.join(tmpdir, "file3.txt"), "w") as f:
                f.write("Master content\n")
            porcelain.add(tmpdir, paths=["file3.txt"])
            porcelain.commit(tmpdir, message=b"Add file3")

            # Test merge via CLI with --no-commit
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="INFO") as cm:
                    ret = main(["merge", "--no-commit", "feature"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 0)  # Success
                self.assertIn("not committing", log_output)

                # Check that files are merged
                self.assertTrue(os.path.exists(os.path.join(tmpdir, "file2.txt")))
                self.assertTrue(os.path.exists(os.path.join(tmpdir, "file3.txt")))
            finally:
                os.chdir(old_cwd)

    def test_merge_no_ff(self):
        """Test CLI merge with --no-ff."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master
            porcelain.checkout(tmpdir, "master")

            # Test merge via CLI with --no-ff
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="INFO") as cm:
                    ret = main(["merge", "--no-ff", "feature"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 0)  # Success
                self.assertIn("Merge successful", log_output)
                self.assertIn("Created merge commit", log_output)
            finally:
                os.chdir(old_cwd)

    def test_merge_with_message(self):
        """Test CLI merge with custom message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master and add another file
            porcelain.checkout(tmpdir, "master")
            with open(os.path.join(tmpdir, "file3.txt"), "w") as f:
                f.write("Master content\n")
            porcelain.add(tmpdir, paths=["file3.txt"])
            porcelain.commit(tmpdir, message=b"Add file3")

            # Test merge via CLI with custom message
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="INFO") as cm:
                    ret = main(["merge", "-m", "Custom merge message", "feature"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 0)  # Success
                self.assertIn("Merge successful", log_output)
            finally:
                os.chdir(old_cwd)

    def test_octopus_merge_three_branches(self):
        """Test CLI octopus merge with three branches."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit with three files
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("File 1 content\n")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("File 2 content\n")
            with open(os.path.join(tmpdir, "file3.txt"), "w") as f:
                f.write("File 3 content\n")
            porcelain.add(tmpdir, paths=["file1.txt", "file2.txt", "file3.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create branch1 and modify file1
            porcelain.branch_create(tmpdir, "branch1")
            porcelain.checkout(tmpdir, "branch1")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Branch1 modified file1\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Branch1 modifies file1")

            # Create branch2 and modify file2
            porcelain.checkout(tmpdir, "master")
            porcelain.branch_create(tmpdir, "branch2")
            porcelain.checkout(tmpdir, "branch2")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Branch2 modified file2\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Branch2 modifies file2")

            # Create branch3 and modify file3
            porcelain.checkout(tmpdir, "master")
            porcelain.branch_create(tmpdir, "branch3")
            porcelain.checkout(tmpdir, "branch3")
            with open(os.path.join(tmpdir, "file3.txt"), "w") as f:
                f.write("Branch3 modified file3\n")
            porcelain.add(tmpdir, paths=["file3.txt"])
            porcelain.commit(tmpdir, message=b"Branch3 modifies file3")

            # Go back to master and octopus merge all three branches via CLI
            porcelain.checkout(tmpdir, "master")
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="INFO") as cm:
                    ret = main(["merge", "branch1", "branch2", "branch3"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 0)  # Success
                self.assertIn("Octopus merge successful", log_output)

                # Check that all modifications are present
                with open(os.path.join(tmpdir, "file1.txt")) as f:
                    self.assertEqual(f.read(), "Branch1 modified file1\n")
                with open(os.path.join(tmpdir, "file2.txt")) as f:
                    self.assertEqual(f.read(), "Branch2 modified file2\n")
                with open(os.path.join(tmpdir, "file3.txt")) as f:
                    self.assertEqual(f.read(), "Branch3 modified file3\n")
            finally:
                os.chdir(old_cwd)

    def test_octopus_merge_with_conflicts(self):
        """Test CLI octopus merge with conflicts."""

        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create branch1 and modify file1
            porcelain.branch_create(tmpdir, "branch1")
            porcelain.checkout(tmpdir, "branch1")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Branch1 content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Branch1 modifies file1")

            # Create branch2 and modify file1 differently
            porcelain.checkout(tmpdir, "master")
            porcelain.branch_create(tmpdir, "branch2")
            porcelain.checkout(tmpdir, "branch2")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Branch2 content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Branch2 modifies file1")

            # Go back to master and try octopus merge via CLI - should fail
            porcelain.checkout(tmpdir, "master")
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="WARNING") as cm:
                    ret = main(["merge", "branch1", "branch2"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 1)  # Error
                self.assertIn("Octopus merge failed", log_output)
                self.assertIn("refusing to merge with conflicts", log_output)
            finally:
                os.chdir(old_cwd)

    def test_octopus_merge_no_commit(self):
        """Test CLI octopus merge with --no-commit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("File 1 content\n")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("File 2 content\n")
            porcelain.add(tmpdir, paths=["file1.txt", "file2.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create branch1 and modify file1
            porcelain.branch_create(tmpdir, "branch1")
            porcelain.checkout(tmpdir, "branch1")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Branch1 modified file1\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Branch1 modifies file1")

            # Create branch2 and modify file2
            porcelain.checkout(tmpdir, "master")
            porcelain.branch_create(tmpdir, "branch2")
            porcelain.checkout(tmpdir, "branch2")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Branch2 modified file2\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Branch2 modifies file2")

            # Go back to master and octopus merge with --no-commit
            porcelain.checkout(tmpdir, "master")
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with self.assertLogs("dulwich.cli", level="INFO") as cm:
                    ret = main(["merge", "--no-commit", "branch1", "branch2"])
                    log_output = "\n".join(cm.output)

                self.assertEqual(ret, 0)  # Success
                self.assertIn("not committing", log_output)

                # Check that files are merged
                with open(os.path.join(tmpdir, "file1.txt")) as f:
                    self.assertEqual(f.read(), "Branch1 modified file1\n")
                with open(os.path.join(tmpdir, "file2.txt")) as f:
                    self.assertEqual(f.read(), "Branch2 modified file2\n")
            finally:
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
