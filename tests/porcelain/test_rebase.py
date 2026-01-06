# test_rebase.py -- tests for porcelain rebase
# Copyright (C) 2025 Dulwich contributors
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

"""Tests for porcelain rebase functions."""

import os
import tempfile

from dulwich import porcelain
from dulwich.repo import Repo

from .. import TestCase


class RebasePorcelainTestCase(TestCase):
    """Tests for the porcelain rebase function."""

    def setUp(self):
        """Set up test repository."""
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.test_dir)

        # Create initial commit
        with open(os.path.join(self.test_dir, "README.md"), "wb") as f:
            f.write(b"# Test Repository\n")

        self.repo.get_worktree().stage(["README.md"])
        self.initial_commit = self.repo.get_worktree().commit(
            message=b"Initial commit",
            committer=b"Test User <test@example.com>",
            author=b"Test User <test@example.com>",
        )

    def tearDown(self):
        """Clean up test directory."""
        import shutil

        shutil.rmtree(self.test_dir)

    def test_porcelain_rebase(self):
        """Test rebase through porcelain interface."""
        # Create and checkout feature branch
        self.repo.refs[b"refs/heads/feature"] = self.initial_commit
        porcelain.checkout(self.repo, "feature")

        # Add commit to feature branch
        with open(os.path.join(self.test_dir, "feature.txt"), "wb") as f:
            f.write(b"Feature file\n")

        porcelain.add(self.repo, ["feature.txt"])
        porcelain.commit(
            self.repo,
            message="Add feature",
            author="Test User <test@example.com>",
            committer="Test User <test@example.com>",
        )

        # Switch to main and add different commit
        porcelain.checkout(self.repo, "master")

        with open(os.path.join(self.test_dir, "main.txt"), "wb") as f:
            f.write(b"Main file\n")

        porcelain.add(self.repo, ["main.txt"])
        porcelain.commit(
            self.repo,
            message="Main update",
            author="Test User <test@example.com>",
            committer="Test User <test@example.com>",
        )

        # Switch back to feature and rebase
        porcelain.checkout(self.repo, "feature")

        # Perform rebase
        new_shas = porcelain.rebase(self.repo, "master")

        # Should have rebased one commit
        self.assertEqual(len(new_shas), 1)

        # Check that the rebased commit has the correct parent and tree
        feature_head = self.repo.refs[b"refs/heads/feature"]
        feature_commit_obj = self.repo[feature_head]

        # Should have master as parent
        master_head = self.repo.refs[b"refs/heads/master"]
        self.assertEqual(feature_commit_obj.parents, [master_head])

        # Tree should have both files
        tree = self.repo[feature_commit_obj.tree]
        self.assertIn(b"feature.txt", tree)
        self.assertIn(b"main.txt", tree)
        self.assertIn(b"README.md", tree)
