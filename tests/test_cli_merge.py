# test_cli_merge.py -- Tests for dulwich merge CLI command
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
#

"""Tests for dulwich merge CLI command."""

import io
import os
import tempfile
import unittest
from unittest.mock import patch

from dulwich import porcelain
from dulwich.cli import main
from dulwich.tests import TestCase


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
            porcelain.checkout_branch(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master
            porcelain.checkout_branch(tmpdir, "master")

            # Test merge via CLI
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                    ret = main(["merge", "feature"])
                    output = mock_stdout.getvalue()

                self.assertEqual(ret, None)  # Success
                self.assertIn("Merge successful", output)

                # Check that file2.txt exists
                self.assertTrue(os.path.exists(os.path.join(tmpdir, "file2.txt")))
            finally:
                os.chdir(old_cwd)

    def test_merge_with_conflicts(self):
        """Test CLI merge with conflicts."""
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
            porcelain.checkout_branch(tmpdir, "feature")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(
                tmpdir, message=b"Modify file1 in feature"
            )

            # Go back to master and modify file1 differently
            porcelain.checkout_branch(tmpdir, "master")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Master content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Modify file1 in master")

            # Test merge via CLI - should exit with error
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                    with patch("sys.exit") as mock_exit:
                        main(["merge", "feature"])
                        mock_exit.assert_called_with(1)
                    output = mock_stdout.getvalue()

                self.assertIn("Merge conflicts", output)
                self.assertIn("file1.txt", output)
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
                with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                    ret = main(["merge", "HEAD"])
                    output = mock_stdout.getvalue()

                self.assertEqual(ret, None)  # Success
                self.assertIn("Already up to date", output)
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
            porcelain.checkout_branch(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master and add another file
            porcelain.checkout_branch(tmpdir, "master")
            with open(os.path.join(tmpdir, "file3.txt"), "w") as f:
                f.write("Master content\n")
            porcelain.add(tmpdir, paths=["file3.txt"])
            porcelain.commit(tmpdir, message=b"Add file3")

            # Test merge via CLI with --no-commit
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                    ret = main(["merge", "--no-commit", "feature"])
                    output = mock_stdout.getvalue()

                self.assertEqual(ret, None)  # Success
                self.assertIn("not committing", output)

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
            porcelain.checkout_branch(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master
            porcelain.checkout_branch(tmpdir, "master")

            # Test merge via CLI with --no-ff
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                    ret = main(["merge", "--no-ff", "feature"])
                    output = mock_stdout.getvalue()

                self.assertEqual(ret, None)  # Success
                self.assertIn("Merge successful", output)
                self.assertIn("Created merge commit", output)
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
            porcelain.checkout_branch(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master and add another file
            porcelain.checkout_branch(tmpdir, "master")
            with open(os.path.join(tmpdir, "file3.txt"), "w") as f:
                f.write("Master content\n")
            porcelain.add(tmpdir, paths=["file3.txt"])
            porcelain.commit(tmpdir, message=b"Add file3")

            # Test merge via CLI with custom message
            old_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                    ret = main(["merge", "-m", "Custom merge message", "feature"])
                    output = mock_stdout.getvalue()

                self.assertEqual(ret, None)  # Success
                self.assertIn("Merge successful", output)
            finally:
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
