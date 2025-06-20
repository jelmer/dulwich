# test_rebase.py -- rebase tests
# Copyright (C) 2025 Dulwich contributors
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

"""Tests for dulwich.rebase."""

import os
import tempfile

from dulwich.objects import Blob, Commit, Tree
from dulwich.rebase import RebaseConflict, Rebaser, rebase
from dulwich.repo import MemoryRepo, Repo
from dulwich.tests.utils import make_commit

from . import TestCase


class RebaserTestCase(TestCase):
    """Tests for the Rebaser class."""

    def setUp(self):
        """Set up test repository."""
        super().setUp()
        self.repo = MemoryRepo()

    def _setup_initial_commit(self):
        """Set up initial commit for tests."""
        # Create initial commit
        blob = Blob.from_string(b"Initial content\n")
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"file.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        self.initial_commit = Commit()
        self.initial_commit.tree = tree.id
        self.initial_commit.parents = []
        self.initial_commit.message = b"Initial commit"
        self.initial_commit.committer = b"Test User <test@example.com>"
        self.initial_commit.author = b"Test User <test@example.com>"
        self.initial_commit.commit_time = 1000000
        self.initial_commit.author_time = 1000000
        self.initial_commit.commit_timezone = 0
        self.initial_commit.author_timezone = 0
        self.repo.object_store.add_object(self.initial_commit)

        # Set up branches
        self.repo.refs[b"refs/heads/master"] = self.initial_commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

    def test_simple_rebase(self):
        """Test simple rebase with no conflicts."""
        self._setup_initial_commit()
        # Create feature branch with one commit
        feature_blob = Blob.from_string(b"Feature content\n")
        self.repo.object_store.add_object(feature_blob)

        feature_tree = Tree()
        feature_tree.add(b"feature.txt", 0o100644, feature_blob.id)
        feature_tree.add(
            b"file.txt", 0o100644, self.repo[self.initial_commit.tree][b"file.txt"][1]
        )
        self.repo.object_store.add_object(feature_tree)

        feature_commit = Commit()
        feature_commit.tree = feature_tree.id
        feature_commit.parents = [self.initial_commit.id]
        feature_commit.message = b"Add feature"
        feature_commit.committer = b"Test User <test@example.com>"
        feature_commit.author = b"Test User <test@example.com>"
        feature_commit.commit_time = 1000100
        feature_commit.author_time = 1000100
        feature_commit.commit_timezone = 0
        feature_commit.author_timezone = 0
        self.repo.object_store.add_object(feature_commit)
        self.repo.refs[b"refs/heads/feature"] = feature_commit.id

        # Create main branch advancement
        main_blob = Blob.from_string(b"Main advancement\n")
        self.repo.object_store.add_object(main_blob)

        main_tree = Tree()
        main_tree.add(b"main.txt", 0o100644, main_blob.id)
        main_tree.add(
            b"file.txt", 0o100644, self.repo[self.initial_commit.tree][b"file.txt"][1]
        )
        self.repo.object_store.add_object(main_tree)

        main_commit = Commit()
        main_commit.tree = main_tree.id
        main_commit.parents = [self.initial_commit.id]
        main_commit.message = b"Main advancement"
        main_commit.committer = b"Test User <test@example.com>"
        main_commit.author = b"Test User <test@example.com>"
        main_commit.commit_time = 1000200
        main_commit.author_time = 1000200
        main_commit.commit_timezone = 0
        main_commit.author_timezone = 0
        self.repo.object_store.add_object(main_commit)
        self.repo.refs[b"refs/heads/master"] = main_commit.id

        # Switch to feature branch
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/feature")

        # Check refs before rebase
        main_ref_before = self.repo.refs[b"refs/heads/master"]
        feature_ref_before = self.repo.refs[b"refs/heads/feature"]

        # Double check that refs are correctly set up
        self.assertEqual(main_ref_before, main_commit.id)
        self.assertEqual(feature_ref_before, feature_commit.id)

        # Perform rebase
        rebaser = Rebaser(self.repo)
        commits = rebaser.start_rebase(
            b"refs/heads/master", branch=b"refs/heads/feature"
        )

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0].id, feature_commit.id)

        # Continue rebase
        result = rebaser.continue_rebase()
        self.assertIsNone(result)  # Rebase complete

        # Check that feature branch was updated
        new_feature_head = self.repo.refs[b"refs/heads/feature"]
        new_commit = self.repo[new_feature_head]

        # Should have main commit as parent
        self.assertEqual(new_commit.parents, [main_commit.id])

        # Should have same tree as original (both files present)
        new_tree = self.repo[new_commit.tree]
        self.assertIn(b"feature.txt", new_tree)
        self.assertIn(b"main.txt", new_tree)
        self.assertIn(b"file.txt", new_tree)

    def test_rebase_with_conflicts(self):
        """Test rebase with merge conflicts."""
        self._setup_initial_commit()
        # Create feature branch with conflicting change
        feature_blob = Blob.from_string(b"Feature change to file\n")
        self.repo.object_store.add_object(feature_blob)

        feature_tree = Tree()
        feature_tree.add(b"file.txt", 0o100644, feature_blob.id)
        self.repo.object_store.add_object(feature_tree)

        feature_commit = Commit()
        feature_commit.tree = feature_tree.id
        feature_commit.parents = [self.initial_commit.id]
        feature_commit.message = b"Feature change"
        feature_commit.committer = b"Test User <test@example.com>"
        feature_commit.author = b"Test User <test@example.com>"
        feature_commit.commit_time = 1000100
        feature_commit.author_time = 1000100
        feature_commit.commit_timezone = 0
        feature_commit.author_timezone = 0
        self.repo.object_store.add_object(feature_commit)
        self.repo.refs[b"refs/heads/feature"] = feature_commit.id

        # Create main branch with conflicting change
        main_blob = Blob.from_string(b"Main change to file\n")
        self.repo.object_store.add_object(main_blob)

        main_tree = Tree()
        main_tree.add(b"file.txt", 0o100644, main_blob.id)
        self.repo.object_store.add_object(main_tree)

        main_commit = Commit()
        main_commit.tree = main_tree.id
        main_commit.parents = [self.initial_commit.id]
        main_commit.message = b"Main change"
        main_commit.committer = b"Test User <test@example.com>"
        main_commit.author = b"Test User <test@example.com>"
        main_commit.commit_time = 1000200
        main_commit.author_time = 1000200
        main_commit.commit_timezone = 0
        main_commit.author_timezone = 0
        self.repo.object_store.add_object(main_commit)
        self.repo.refs[b"refs/heads/master"] = main_commit.id

        # Attempt rebase - should fail with conflicts
        with self.assertRaises(RebaseConflict) as cm:
            rebase(self.repo, b"refs/heads/master", branch=b"refs/heads/feature")

        self.assertIn(b"file.txt", cm.exception.conflicted_files)

    def test_abort_rebase(self):
        """Test aborting a rebase."""
        self._setup_initial_commit()
        # Set up branches similar to simple rebase test
        feature_blob = Blob.from_string(b"Feature content\n")
        self.repo.object_store.add_object(feature_blob)

        feature_tree = Tree()
        feature_tree.add(b"feature.txt", 0o100644, feature_blob.id)
        feature_tree.add(
            b"file.txt", 0o100644, self.repo[self.initial_commit.tree][b"file.txt"][1]
        )
        self.repo.object_store.add_object(feature_tree)

        feature_commit = Commit()
        feature_commit.tree = feature_tree.id
        feature_commit.parents = [self.initial_commit.id]
        feature_commit.message = b"Add feature"
        feature_commit.committer = b"Test User <test@example.com>"
        feature_commit.author = b"Test User <test@example.com>"
        feature_commit.commit_time = 1000100
        feature_commit.author_time = 1000100
        feature_commit.commit_timezone = 0
        feature_commit.author_timezone = 0
        self.repo.object_store.add_object(feature_commit)
        self.repo.refs[b"refs/heads/feature"] = feature_commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/feature")

        # Start rebase
        rebaser = Rebaser(self.repo)
        rebaser.start_rebase(b"refs/heads/master")

        # Abort rebase
        rebaser.abort_rebase()

        # Check that HEAD is restored
        self.assertEqual(self.repo.refs.read_ref(b"HEAD"), b"ref: refs/heads/feature")
        self.assertEqual(self.repo.refs[b"refs/heads/feature"], feature_commit.id)

        # Check that REBASE_HEAD is cleaned up
        self.assertNotIn(b"REBASE_HEAD", self.repo.refs)

    def test_rebase_no_commits(self):
        """Test rebase when already up to date."""
        self._setup_initial_commit()
        # Both branches point to same commit
        self.repo.refs[b"refs/heads/feature"] = self.initial_commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/feature")

        # Perform rebase
        result = rebase(self.repo, b"refs/heads/master")

        # Should return empty list (no new commits)
        self.assertEqual(result, [])

    def test_rebase_onto(self):
        """Test rebase with --onto option."""
        self._setup_initial_commit()
        # Create a chain of commits: initial -> A -> B -> C
        blob_a = Blob.from_string(b"Commit A\n")
        self.repo.object_store.add_object(blob_a)

        tree_a = Tree()
        tree_a.add(b"a.txt", 0o100644, blob_a.id)
        tree_a.add(
            b"file.txt", 0o100644, self.repo[self.initial_commit.tree][b"file.txt"][1]
        )
        self.repo.object_store.add_object(tree_a)

        commit_a = make_commit(
            id=b"a" * 40,
            tree=tree_a.id,
            parents=[self.initial_commit.id],
            message=b"Commit A",
            committer=b"Test User <test@example.com>",
            author=b"Test User <test@example.com>",
            commit_time=1000100,
            author_time=1000100,
        )
        self.repo.object_store.add_object(commit_a)

        blob_b = Blob.from_string(b"Commit B\n")
        self.repo.object_store.add_object(blob_b)

        tree_b = Tree()
        tree_b.add(b"b.txt", 0o100644, blob_b.id)
        tree_b.add(b"a.txt", 0o100644, blob_a.id)
        tree_b.add(
            b"file.txt", 0o100644, self.repo[self.initial_commit.tree][b"file.txt"][1]
        )
        self.repo.object_store.add_object(tree_b)

        commit_b = make_commit(
            id=b"b" * 40,
            tree=tree_b.id,
            parents=[commit_a.id],
            message=b"Commit B",
            committer=b"Test User <test@example.com>",
            author=b"Test User <test@example.com>",
            commit_time=1000200,
            author_time=1000200,
        )
        self.repo.object_store.add_object(commit_b)

        blob_c = Blob.from_string(b"Commit C\n")
        self.repo.object_store.add_object(blob_c)

        tree_c = Tree()
        tree_c.add(b"c.txt", 0o100644, blob_c.id)
        tree_c.add(b"b.txt", 0o100644, blob_b.id)
        tree_c.add(b"a.txt", 0o100644, blob_a.id)
        tree_c.add(
            b"file.txt", 0o100644, self.repo[self.initial_commit.tree][b"file.txt"][1]
        )
        self.repo.object_store.add_object(tree_c)

        commit_c = make_commit(
            id=b"c" * 40,
            tree=tree_c.id,
            parents=[commit_b.id],
            message=b"Commit C",
            committer=b"Test User <test@example.com>",
            author=b"Test User <test@example.com>",
            commit_time=1000300,
            author_time=1000300,
        )
        self.repo.object_store.add_object(commit_c)

        # Create separate branch at commit A
        self.repo.refs[b"refs/heads/topic"] = commit_c.id
        self.repo.refs[b"refs/heads/newbase"] = commit_a.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/topic")

        # Rebase B and C onto initial commit (skipping A)
        rebaser = Rebaser(self.repo)
        commits = rebaser.start_rebase(
            upstream=commit_a.id,
            onto=self.initial_commit.id,
            branch=b"refs/heads/topic",
        )

        # Should rebase commits B and C
        self.assertEqual(len(commits), 2)
        self.assertEqual(commits[0].id, commit_b.id)
        self.assertEqual(commits[1].id, commit_c.id)

        # Continue rebase
        result = rebaser.continue_rebase()
        self.assertIsNone(result)

        # Check result
        new_head = self.repo.refs[b"refs/heads/topic"]
        new_c = self.repo[new_head]
        new_b = self.repo[new_c.parents[0]]

        # B should now have initial commit as parent (not A)
        self.assertEqual(new_b.parents, [self.initial_commit.id])

        # Trees should still have b.txt and c.txt but not a.txt
        new_b_tree = self.repo[new_b.tree]
        self.assertIn(b"b.txt", new_b_tree)
        self.assertNotIn(b"a.txt", new_b_tree)

        new_c_tree = self.repo[new_c.tree]
        self.assertIn(b"c.txt", new_c_tree)
        self.assertIn(b"b.txt", new_c_tree)
        self.assertNotIn(b"a.txt", new_c_tree)


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

        self.repo.stage(["README.md"])
        self.initial_commit = self.repo.do_commit(
            b"Initial commit",
            committer=b"Test User <test@example.com>",
            author=b"Test User <test@example.com>",
        )

    def tearDown(self):
        """Clean up test directory."""
        import shutil

        shutil.rmtree(self.test_dir)

    def test_porcelain_rebase(self):
        """Test rebase through porcelain interface."""
        from dulwich import porcelain

        # Create and checkout feature branch
        self.repo.refs[b"refs/heads/feature"] = self.initial_commit
        porcelain.checkout_branch(self.repo, "feature")

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
        porcelain.checkout_branch(self.repo, "master")

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
        porcelain.checkout_branch(self.repo, "feature")

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
