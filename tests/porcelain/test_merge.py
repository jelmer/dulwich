# test_porcelain_merge.py -- Tests for porcelain merge functionality
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

"""Tests for porcelain merge functionality."""

import importlib.util
import os
import tempfile
import unittest

from dulwich import porcelain
from dulwich.objects import ZERO_SHA
from dulwich.repo import Repo

from .. import DependencyMissing, TestCase


class PorcelainMergeTests(TestCase):
    """Tests for the porcelain merge functionality."""

    def test_merge_fast_forward(self):
        """Test fast-forward merge."""
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
            feature_commit = porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master
            porcelain.checkout(tmpdir, "master")

            # Merge feature branch (should fast-forward)
            merge_commit, conflicts = porcelain.merge(tmpdir, "feature")

            self.assertEqual(merge_commit, feature_commit)
            self.assertEqual(conflicts, [])

            # Check that file2.txt exists
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "file2.txt")))

    def test_merge_already_up_to_date(self):
        """Test merge when already up to date."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Try to merge the same commit
            merge_commit, conflicts = porcelain.merge(tmpdir, "HEAD")

            self.assertIsNone(merge_commit)
            self.assertEqual(conflicts, [])

    def test_merge_no_ff(self):
        """Test merge with --no-ff flag."""
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
            feature_commit = porcelain.commit(tmpdir, message=b"Add feature")

            # Go back to master
            porcelain.checkout(tmpdir, "master")

            # Merge feature branch with no-ff
            merge_commit, conflicts = porcelain.merge(tmpdir, "feature", no_ff=True)

            # Should create a new merge commit
            self.assertIsNotNone(merge_commit)
            self.assertNotEqual(merge_commit, feature_commit)
            self.assertEqual(conflicts, [])

            # Check that it's a merge commit with two parents
            with Repo(tmpdir) as repo:
                commit = repo[merge_commit]
                self.assertEqual(len(commit.parents), 2)

    def test_merge_three_way(self):
        """Test three-way merge without conflicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Initial file2\n")
            porcelain.add(tmpdir, paths=["file1.txt", "file2.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create a branch and modify file1
            porcelain.branch_create(tmpdir, "feature")
            porcelain.checkout(tmpdir, "feature")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Feature content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Modify file1 in feature")

            # Go back to master and modify file2
            porcelain.checkout(tmpdir, "master")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Master file2\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Modify file2 in master")

            # Merge feature branch
            merge_commit, conflicts = porcelain.merge(tmpdir, "feature")

            self.assertIsNotNone(merge_commit)
            self.assertEqual(conflicts, [])

            # Check both modifications are present
            with open(os.path.join(tmpdir, "file1.txt")) as f:
                self.assertEqual(f.read(), "Feature content\n")
            with open(os.path.join(tmpdir, "file2.txt")) as f:
                self.assertEqual(f.read(), "Master file2\n")

    def test_merge_with_conflicts(self):
        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        """Test merge with conflicts."""
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

            # Merge feature branch - should have conflicts
            merge_commit, conflicts = porcelain.merge(tmpdir, "feature")

            self.assertIsNone(merge_commit)
            self.assertEqual(len(conflicts), 1)
            self.assertEqual(conflicts[0], b"file1.txt")

            # Check conflict markers in file
            with open(os.path.join(tmpdir, "file1.txt"), "rb") as f:
                content = f.read()
                self.assertIn(b"<<<<<<< ours", content)
                self.assertIn(b"=======", content)
                self.assertIn(b">>>>>>> theirs", content)

    def test_merge_no_commit(self):
        """Test merge with no_commit flag."""
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
            master_commit = porcelain.commit(tmpdir, message=b"Add file3")

            # Merge feature branch with no_commit
            merge_commit, conflicts = porcelain.merge(tmpdir, "feature", no_commit=True)

            self.assertIsNone(merge_commit)
            self.assertEqual(conflicts, [])

            # Check that files are merged but no commit was created
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "file2.txt")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "file3.txt")))

            # HEAD should still point to master_commit
            with Repo(tmpdir) as repo:
                self.assertEqual(repo.refs[b"HEAD"], master_commit)

    def test_merge_no_head(self):
        """Test merge with no HEAD reference."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo without any commits
            porcelain.init(tmpdir)

            # Try to merge - should fail with no HEAD
            self.assertRaises(porcelain.Error, porcelain.merge, tmpdir, "nonexistent")

    def test_merge_invalid_commit(self):
        """Test merge with invalid commit reference."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Try to merge nonexistent commit
            self.assertRaises(porcelain.Error, porcelain.merge, tmpdir, "nonexistent")

    def test_octopus_merge_three_branches(self):
        """Test octopus merge with three branches."""
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

            # Go back to master and octopus merge all three branches
            porcelain.checkout(tmpdir, "master")
            merge_commit, conflicts = porcelain.merge(
                tmpdir, ["branch1", "branch2", "branch3"]
            )

            # Should succeed with no conflicts
            self.assertIsNotNone(merge_commit)
            self.assertEqual(conflicts, [])

            # Check that the merge commit has 4 parents (master + 3 branches)
            with Repo(tmpdir) as repo:
                commit = repo[merge_commit]
                self.assertEqual(len(commit.parents), 4)

            # Check that all modifications are present
            with open(os.path.join(tmpdir, "file1.txt")) as f:
                self.assertEqual(f.read(), "Branch1 modified file1\n")
            with open(os.path.join(tmpdir, "file2.txt")) as f:
                self.assertEqual(f.read(), "Branch2 modified file2\n")
            with open(os.path.join(tmpdir, "file3.txt")) as f:
                self.assertEqual(f.read(), "Branch3 modified file3\n")

    def test_octopus_merge_with_conflicts(self):
        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        """Test that octopus merge refuses to proceed with conflicts."""
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

            # Go back to master and try octopus merge - should fail
            porcelain.checkout(tmpdir, "master")
            merge_commit, conflicts = porcelain.merge(tmpdir, ["branch1", "branch2"])

            # Should have conflicts and no merge commit
            self.assertIsNone(merge_commit)
            self.assertEqual(len(conflicts), 1)
            self.assertEqual(conflicts[0], b"file1.txt")

    def test_octopus_merge_no_commit_flag(self):
        """Test octopus merge with no_commit flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("File 1 content\n")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("File 2 content\n")
            porcelain.add(tmpdir, paths=["file1.txt", "file2.txt"])
            master_commit = porcelain.commit(tmpdir, message=b"Initial commit")

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

            # Go back to master and octopus merge with no_commit
            porcelain.checkout(tmpdir, "master")
            merge_commit, conflicts = porcelain.merge(
                tmpdir, ["branch1", "branch2"], no_commit=True
            )

            # Should not create commit
            self.assertIsNone(merge_commit)
            self.assertEqual(conflicts, [])

            # Check that files are merged but no commit was created
            with open(os.path.join(tmpdir, "file1.txt")) as f:
                self.assertEqual(f.read(), "Branch1 modified file1\n")
            with open(os.path.join(tmpdir, "file2.txt")) as f:
                self.assertEqual(f.read(), "Branch2 modified file2\n")

            # HEAD should still point to master_commit
            with Repo(tmpdir) as repo:
                self.assertEqual(repo.refs[b"HEAD"], master_commit)

    def test_octopus_merge_single_branch(self):
        """Test that octopus merge with single branch falls back to regular merge."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create initial commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Initial content\n")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Initial file2\n")
            porcelain.add(tmpdir, paths=["file1.txt", "file2.txt"])
            porcelain.commit(tmpdir, message=b"Initial commit")

            # Create branch and modify file1
            porcelain.branch_create(tmpdir, "branch1")
            porcelain.checkout(tmpdir, "branch1")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Branch1 content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            porcelain.commit(tmpdir, message=b"Branch1 changes")

            # Go back to master and modify file2 to prevent fast-forward
            porcelain.checkout(tmpdir, "master")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Master file2\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            porcelain.commit(tmpdir, message=b"Master changes")

            # Merge with list containing one branch
            merge_commit, conflicts = porcelain.merge(tmpdir, ["branch1"])

            # Should create a regular merge commit
            self.assertIsNotNone(merge_commit)
            self.assertEqual(conflicts, [])

            # Check the merge commit (should have 2 parents for regular merge)
            with Repo(tmpdir) as repo:
                commit = repo[merge_commit]
                self.assertEqual(len(commit.parents), 2)


class PorcelainMergeTreeTests(TestCase):
    """Tests for the porcelain merge_tree functionality."""

    def test_merge_tree_no_conflicts(self):
        """Test merge_tree with no conflicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)
            repo = Repo(tmpdir)
            self.addCleanup(repo.close)

            # Create base tree
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Base content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            base_commit = porcelain.commit(tmpdir, message=b"Base commit")

            # Create our branch
            porcelain.branch_create(tmpdir, "ours")
            porcelain.checkout(tmpdir, "ours")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Our content\n")
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Our new file\n")
            porcelain.add(tmpdir, paths=["file1.txt", "file2.txt"])
            our_commit = porcelain.commit(tmpdir, message=b"Our commit")

            # Create their branch
            porcelain.checkout(tmpdir, b"master")
            porcelain.branch_create(tmpdir, "theirs")
            porcelain.checkout(tmpdir, "theirs")
            with open(os.path.join(tmpdir, "file3.txt"), "w") as f:
                f.write("Their new file\n")
            porcelain.add(tmpdir, paths=["file3.txt"])
            their_commit = porcelain.commit(tmpdir, message=b"Their commit")

            # Perform merge_tree
            merged_tree_id, conflicts = porcelain.merge_tree(
                tmpdir, base_commit, our_commit, their_commit
            )

            # Should have no conflicts
            self.assertEqual(conflicts, [])

            # Check merged tree contains all files
            merged_tree = repo[merged_tree_id]
            self.assertIn(b"file1.txt", merged_tree)
            self.assertIn(b"file2.txt", merged_tree)
            self.assertIn(b"file3.txt", merged_tree)

    def test_merge_tree_with_conflicts(self):
        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        """Test merge_tree with conflicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)
            repo = Repo(tmpdir)
            self.addCleanup(repo.close)

            # Create base tree
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Base content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            base_commit = porcelain.commit(tmpdir, message=b"Base commit")

            # Create our branch with changes
            porcelain.branch_create(tmpdir, "ours")
            porcelain.checkout(tmpdir, "ours")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Our content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            our_commit = porcelain.commit(tmpdir, message=b"Our commit")

            # Create their branch with conflicting changes
            porcelain.checkout(tmpdir, b"master")
            porcelain.branch_create(tmpdir, "theirs")
            porcelain.checkout(tmpdir, "theirs")
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Their content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            their_commit = porcelain.commit(tmpdir, message=b"Their commit")

            # Perform merge_tree
            merged_tree_id, conflicts = porcelain.merge_tree(
                tmpdir, base_commit, our_commit, their_commit
            )

            # Should have conflicts
            self.assertEqual(conflicts, [b"file1.txt"])

            # Check merged tree exists and contains conflict markers
            merged_tree = repo[merged_tree_id]
            self.assertIn(b"file1.txt", merged_tree)

            # Get the merged blob content
            _file_mode, file_sha = merged_tree[b"file1.txt"]
            merged_blob = repo[file_sha]
            content = merged_blob.data

            # Should contain conflict markers
            self.assertIn(b"<<<<<<< ours", content)
            self.assertIn(b"=======", content)
            self.assertIn(b">>>>>>> theirs", content)

    def test_merge_tree_no_base(self):
        """Test merge_tree without a base commit."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)
            repo = Repo(tmpdir)
            self.addCleanup(repo.close)

            # Create our tree
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Our content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            our_commit = porcelain.commit(tmpdir, message=b"Our commit")

            # Create their tree (independent)
            os.remove(os.path.join(tmpdir, "file1.txt"))
            with open(os.path.join(tmpdir, "file2.txt"), "w") as f:
                f.write("Their content\n")
            porcelain.add(tmpdir, paths=["file2.txt"])
            their_commit = porcelain.commit(tmpdir, message=b"Their commit")

            # Perform merge_tree without base
            merged_tree_id, conflicts = porcelain.merge_tree(
                tmpdir, None, our_commit, their_commit
            )

            # Should have no conflicts (different files)
            self.assertEqual(conflicts, [])

            # Check merged tree contains both files
            merged_tree = repo[merged_tree_id]
            self.assertIn(b"file1.txt", merged_tree)
            self.assertIn(b"file2.txt", merged_tree)

    def test_merge_tree_with_tree_objects(self):
        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        """Test merge_tree with tree objects instead of commits."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)
            repo = Repo(tmpdir)
            self.addCleanup(repo.close)

            # Create base tree
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Base content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            base_commit_id = porcelain.commit(tmpdir, message=b"Base commit")
            base_tree_id = repo[base_commit_id].tree

            # Create our tree
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Our content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            our_commit_id = porcelain.commit(tmpdir, message=b"Our commit")
            our_tree_id = repo[our_commit_id].tree

            # Create their tree
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Their content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            their_commit_id = porcelain.commit(tmpdir, message=b"Their commit")
            their_tree_id = repo[their_commit_id].tree

            # Perform merge_tree with tree SHAs
            _merged_tree_id, conflicts = porcelain.merge_tree(
                tmpdir,
                base_tree_id if base_tree_id else None,
                our_tree_id,
                their_tree_id,
            )

            # Should have conflicts
            self.assertEqual(conflicts, [b"file1.txt"])

    def test_merge_tree_invalid_object(self):
        """Test merge_tree with invalid object reference."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Initialize repo
            porcelain.init(tmpdir)

            # Create a commit
            with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
                f.write("Content\n")
            porcelain.add(tmpdir, paths=["file1.txt"])
            commit_id = porcelain.commit(tmpdir, message=b"Commit")

            # Try to merge with nonexistent object
            self.assertRaises(
                KeyError,
                porcelain.merge_tree,
                tmpdir,
                None,
                commit_id,
                ZERO_SHA,
            )


if __name__ == "__main__":
    unittest.main()
