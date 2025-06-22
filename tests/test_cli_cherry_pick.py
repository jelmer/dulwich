# test_cli_cherry_pick.py -- Tests for CLI cherry-pick command
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

"""Tests for CLI cherry-pick command."""

import os
import tempfile

from dulwich import porcelain
from dulwich.cli import cmd_cherry_pick

from . import TestCase


class CherryPickCommandTests(TestCase):
    """Tests for the cherry-pick CLI command."""

    def test_cherry_pick_simple(self):
        """Test simple cherry-pick via CLI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save current directory
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)

                # Initialize repo
                porcelain.init(".")

                # Create initial commit
                with open("file1.txt", "w") as f:
                    f.write("Initial content\n")
                porcelain.add(".", paths=["file1.txt"])
                porcelain.commit(".", message=b"Initial commit")

                # Create a branch and switch to it
                porcelain.branch_create(".", "feature")
                porcelain.checkout_branch(".", "feature")

                # Add a file on feature branch
                with open("file2.txt", "w") as f:
                    f.write("Feature content\n")
                porcelain.add(".", paths=["file2.txt"])
                feature_commit = porcelain.commit(".", message=b"Add feature file")

                # Go back to master
                porcelain.checkout_branch(".", "master")

                # Cherry-pick via CLI
                cmd = cmd_cherry_pick()
                result = cmd.run([feature_commit.decode()])

                self.assertIsNone(result)  # Success
                self.assertTrue(os.path.exists("file2.txt"))

            finally:
                os.chdir(orig_cwd)

    def test_cherry_pick_no_commit(self):
        """Test cherry-pick with --no-commit via CLI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)

                # Initialize repo
                porcelain.init(".")

                # Create initial commit
                with open("file1.txt", "w") as f:
                    f.write("Initial content\n")
                porcelain.add(".", paths=["file1.txt"])
                porcelain.commit(".", message=b"Initial commit")

                # Create a branch and switch to it
                porcelain.branch_create(".", "feature")
                porcelain.checkout_branch(".", "feature")

                # Add a file on feature branch
                with open("file2.txt", "w") as f:
                    f.write("Feature content\n")
                porcelain.add(".", paths=["file2.txt"])
                feature_commit = porcelain.commit(".", message=b"Add feature file")

                # Go back to master
                porcelain.checkout_branch(".", "master")

                # Cherry-pick with --no-commit
                cmd = cmd_cherry_pick()
                result = cmd.run(["--no-commit", feature_commit.decode()])

                self.assertIsNone(result)  # Success
                self.assertTrue(os.path.exists("file2.txt"))

                # Check that file is staged but not committed
                status = porcelain.status(".")
                self.assertTrue(
                    any(b"file2.txt" in changes for changes in status.staged.values())
                )

            finally:
                os.chdir(orig_cwd)

    def test_cherry_pick_missing_argument(self):
        """Test cherry-pick without commit argument."""
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                porcelain.init(".")

                # Try to cherry-pick without argument
                cmd = cmd_cherry_pick()
                with self.assertRaises(SystemExit) as cm:
                    cmd.run([])

                self.assertEqual(cm.exception.code, 2)  # argparse error code

            finally:
                os.chdir(orig_cwd)
