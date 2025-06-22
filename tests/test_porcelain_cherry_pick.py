# test_porcelain_cherry_pick.py -- Tests for porcelain cherry-pick functionality
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

"""Tests for porcelain cherry-pick functionality."""

import os
import tempfile

from dulwich import porcelain

from . import TestCase


class PorcelainCherryPickTests(TestCase):
    """Tests for the porcelain cherry-pick functionality."""

    def test_cherry_pick_simple(self):
        """Test simple cherry-pick."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch and switch to it
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout_branch(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            feature_commit = porcelain.commit(tmpdir, message=b"Add feature file")

            # Go back to master
            porcelain.checkout_branch(tmpdir, "master")

            # Cherry-pick the feature commit
            new_commit = porcelain.cherry_pick(tmpdir, feature_commit)

            self.assertIsNotNone(new_commit)
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "file2.txt")))

            # Check the content
            with open(os.path.join(tmpdir, "file2.txt")) as f:
                self.assertEqual(f.read(), "Feature content\n")

    def test_cherry_pick_no_commit(self):
        """Test cherry-pick with --no-commit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch and switch to it
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout_branch(tmpdir, "feature")

            # Add a file on feature branch
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            feature_commit = porcelain.commit(tmpdir, message=b"Add feature file")

            # Go back to master
            porcelain.checkout_branch(tmpdir, "master")

            # Cherry-pick with no-commit
            result = porcelain.cherry_pick(tmpdir, feature_commit, no_commit=True)

            self.assertIsNone(result)
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "file2.txt")))

            # Check that the change is staged but not committed
            status = porcelain.status(tmpdir)
            # The file should be in staged changes
            self.assertTrue(
                any(b"file2.txt" in changes for changes in status.staged.values())
            )

    def test_cherry_pick_conflict(self):
        """Test cherry-pick with conflicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit with a file
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch and modify the file
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout_branch(tmpdir, "feature")

            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Feature modification\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            feature_commit = porcelain.commit(tmpdir, message=b"Modify file on feature")

            # Go back to master and make a different modification
            porcelain.checkout_branch(tmpdir, "master")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Master modification\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Modify file on master")

            # Try to cherry-pick - should raise error due to conflicts
            with self.assertRaises(porcelain.Error) as cm:
                porcelain.cherry_pick(tmpdir, feature_commit)

            self.assertIn("Conflicts in:", str(cm.exception))
            self.assertIn("file1.txt", str(cm.exception))

    def test_cherry_pick_root_commit(self):
        """Test cherry-pick of root commit (should fail)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            root_commit = porcelain.commit(tmpdir, message=b"Root commit")

            # Create another branch with a different initial commit
            porcelain.branch_create(tmpdir, "other")
            porcelain.checkout_branch(tmpdir, "other")

            # Try to cherry-pick root commit - should fail
            with self.assertRaises(porcelain.Error) as cm:
                porcelain.cherry_pick(tmpdir, root_commit)

            self.assertIn("Cannot cherry-pick root commit", str(cm.exception))

    def test_cherry_pick_abort(self):
        """Test aborting a cherry-pick."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create branch and make conflicting changes
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout_branch(tmpdir, "feature")

            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            feature_commit = porcelain.commit(tmpdir, message=b"Feature change")

            # Go back to master and make different change
            porcelain.checkout_branch(tmpdir, "master")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Master content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Master change")

            # Try to cherry-pick - will conflict
            try:
                porcelain.cherry_pick(tmpdir, feature_commit)
            except porcelain.Error:
                pass  # Expected

            # Abort the cherry-pick
            result = porcelain.cherry_pick(tmpdir, None, abort=True)
            self.assertIsNone(result)

            # Check that we're back to the master state
            with open(os.path.join(tmpdir, "file1.txt")) as f:
                self.assertEqual(f.read(), "Master content\n")
