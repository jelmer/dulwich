# test_bundle.py -- test bundle compatibility with CGit
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for bundle compatibility with CGit."""

import os
import tempfile

from dulwich.bundle import create_bundle_from_repo, read_bundle, write_bundle
from dulwich.objects import Commit, Tree
from dulwich.repo import Repo

from .utils import CompatTestCase, rmtree_ro, run_git_or_fail


class CompatBundleTestCase(CompatTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)
        self.addCleanup(self.repo.close)

    def test_create_bundle_git_compat(self) -> None:
        """Test creating a bundle that git can read."""
        # Create a commit
        commit = Commit()
        commit.committer = commit.author = b"Test User <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit"

        tree = Tree()
        self.repo.object_store.add_object(tree)
        commit.tree = tree.id
        self.repo.object_store.add_object(commit)

        # Update ref
        self.repo.refs[b"refs/heads/master"] = commit.id

        # Create bundle using dulwich
        bundle_path = os.path.join(self.test_dir, "test.bundle")

        # Use create_bundle_from_repo helper
        bundle = create_bundle_from_repo(self.repo)

        with open(bundle_path, "wb") as f:
            write_bundle(f, bundle)

        # Verify git can read the bundle (must run from a repo directory)
        output = run_git_or_fail(["bundle", "verify", bundle_path], cwd=self.repo_path)
        self.assertIn(b"The bundle contains", output)
        self.assertIn(b"refs/heads/master", output)

    def test_read_git_bundle(self) -> None:
        """Test reading a bundle created by git."""
        # Create a commit using git
        run_git_or_fail(["config", "user.name", "Test User"], cwd=self.repo_path)
        run_git_or_fail(
            ["config", "user.email", "test@example.com"], cwd=self.repo_path
        )

        # Create a file and commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content\n")

        run_git_or_fail(["add", "test.txt"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Test commit"], cwd=self.repo_path)

        # Create bundle using git
        bundle_path = os.path.join(self.test_dir, "git.bundle")
        run_git_or_fail(["bundle", "create", bundle_path, "HEAD"], cwd=self.repo_path)

        # Read bundle using dulwich
        with open(bundle_path, "rb") as f:
            bundle = read_bundle(f)

        # Verify bundle contents
        self.assertEqual(2, bundle.version)
        self.assertIn(b"HEAD", bundle.references)
        self.assertEqual({}, bundle.capabilities)
        self.assertEqual([], bundle.prerequisites)

    def test_read_git_bundle_multiple_refs(self) -> None:
        """Test reading a bundle with multiple references created by git."""
        # Create commits and branches using git
        run_git_or_fail(["config", "user.name", "Test User"], cwd=self.repo_path)
        run_git_or_fail(
            ["config", "user.email", "test@example.com"], cwd=self.repo_path
        )

        # Create initial commit
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("initial content\n")
        run_git_or_fail(["add", "test.txt"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Initial commit"], cwd=self.repo_path)

        # Create feature branch
        run_git_or_fail(["checkout", "-b", "feature"], cwd=self.repo_path)
        with open(test_file, "w") as f:
            f.write("feature content\n")
        run_git_or_fail(["add", "test.txt"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Feature commit"], cwd=self.repo_path)

        # Create another branch
        run_git_or_fail(["checkout", "-b", "develop", "master"], cwd=self.repo_path)
        dev_file = os.path.join(self.repo_path, "dev.txt")
        with open(dev_file, "w") as f:
            f.write("dev content\n")
        run_git_or_fail(["add", "dev.txt"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Dev commit"], cwd=self.repo_path)

        # Create bundle with all branches
        bundle_path = os.path.join(self.test_dir, "multi_ref.bundle")
        run_git_or_fail(["bundle", "create", bundle_path, "--all"], cwd=self.repo_path)

        # Read bundle using dulwich
        with open(bundle_path, "rb") as f:
            bundle = read_bundle(f)

        # Verify bundle contains all refs
        self.assertIn(b"refs/heads/master", bundle.references)
        self.assertIn(b"refs/heads/feature", bundle.references)
        self.assertIn(b"refs/heads/develop", bundle.references)

    def test_read_git_bundle_with_prerequisites(self) -> None:
        """Test reading a bundle with prerequisites created by git."""
        # Create initial commits using git
        run_git_or_fail(["config", "user.name", "Test User"], cwd=self.repo_path)
        run_git_or_fail(
            ["config", "user.email", "test@example.com"], cwd=self.repo_path
        )

        # Create base commits
        test_file = os.path.join(self.repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("content 1\n")
        run_git_or_fail(["add", "test.txt"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Commit 1"], cwd=self.repo_path)

        with open(test_file, "a") as f:
            f.write("content 2\n")
        run_git_or_fail(["add", "test.txt"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Commit 2"], cwd=self.repo_path)

        # Get the first commit hash to use as base
        first_commit = run_git_or_fail(
            ["rev-parse", "HEAD~1"], cwd=self.repo_path
        ).strip()

        # Create more commits
        with open(test_file, "a") as f:
            f.write("content 3\n")
        run_git_or_fail(["add", "test.txt"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Commit 3"], cwd=self.repo_path)

        # Create bundle with prerequisites (only commits after first_commit)
        bundle_path = os.path.join(self.test_dir, "prereq.bundle")
        run_git_or_fail(
            ["bundle", "create", bundle_path, f"{first_commit.decode()}..HEAD"],
            cwd=self.repo_path,
        )

        # Read bundle using dulwich
        with open(bundle_path, "rb") as f:
            bundle = read_bundle(f)

        # Verify bundle has prerequisites
        self.assertGreater(len(bundle.prerequisites), 0)
        # The prerequisite should be the first commit
        prereq_ids = [p[0] for p in bundle.prerequisites]
        self.assertIn(first_commit, prereq_ids)

    def test_read_git_bundle_complex_pack(self) -> None:
        """Test reading a bundle with complex pack data (multiple objects) created by git."""
        # Create a more complex repository structure
        run_git_or_fail(["config", "user.name", "Test User"], cwd=self.repo_path)
        run_git_or_fail(
            ["config", "user.email", "test@example.com"], cwd=self.repo_path
        )

        # Create multiple files in subdirectories
        os.makedirs(os.path.join(self.repo_path, "src", "main"), exist_ok=True)
        os.makedirs(os.path.join(self.repo_path, "tests"), exist_ok=True)

        # Add various file types
        files = [
            ("README.md", "# Test Project\n\nThis is a test."),
            ("src/main/app.py", "def main():\n    print('Hello')\n"),
            ("src/main/utils.py", "def helper():\n    return 42\n"),
            ("tests/test_app.py", "def test_main():\n    pass\n"),
            (".gitignore", "*.pyc\n__pycache__/\n"),
        ]

        for filepath, content in files:
            full_path = os.path.join(self.repo_path, filepath)
            with open(full_path, "w") as f:
                f.write(content)
            run_git_or_fail(["add", filepath], cwd=self.repo_path)

        run_git_or_fail(["commit", "-m", "Initial complex commit"], cwd=self.repo_path)

        # Make additional changes
        with open(os.path.join(self.repo_path, "src/main/app.py"), "a") as f:
            f.write("\nif __name__ == '__main__':\n    main()\n")
        run_git_or_fail(["add", "src/main/app.py"], cwd=self.repo_path)
        run_git_or_fail(["commit", "-m", "Update app.py"], cwd=self.repo_path)

        # Create bundle
        bundle_path = os.path.join(self.test_dir, "complex.bundle")
        run_git_or_fail(["bundle", "create", bundle_path, "HEAD"], cwd=self.repo_path)

        # Read bundle using dulwich
        with open(bundle_path, "rb") as f:
            bundle = read_bundle(f)

        # Verify bundle contents
        self.assertEqual(2, bundle.version)
        self.assertIn(b"HEAD", bundle.references)

        # Verify pack data exists
        self.assertIsNotNone(bundle.pack_data)
        self.assertGreater(len(bundle.pack_data), 0)

    def test_clone_from_git_bundle(self) -> None:
        """Test cloning from a bundle created by git."""
        # Create a repository with some history
        run_git_or_fail(["config", "user.name", "Test User"], cwd=self.repo_path)
        run_git_or_fail(
            ["config", "user.email", "test@example.com"], cwd=self.repo_path
        )

        # Create commits
        test_file = os.path.join(self.repo_path, "test.txt")
        for i in range(3):
            with open(test_file, "a") as f:
                f.write(f"Line {i}\n")
            run_git_or_fail(["add", "test.txt"], cwd=self.repo_path)
            run_git_or_fail(["commit", "-m", f"Commit {i}"], cwd=self.repo_path)

        # Create bundle
        bundle_path = os.path.join(self.test_dir, "clone.bundle")
        run_git_or_fail(["bundle", "create", bundle_path, "HEAD"], cwd=self.repo_path)

        # Read bundle using dulwich
        with open(bundle_path, "rb") as f:
            bundle = read_bundle(f)

        # Verify bundle was read correctly
        self.assertEqual(2, bundle.version)
        self.assertIn(b"HEAD", bundle.references)
        self.assertEqual([], bundle.prerequisites)

        # Verify pack data exists
        self.assertIsNotNone(bundle.pack_data)
        self.assertGreater(len(bundle.pack_data), 0)

        # Use git to verify the bundle can be used for cloning
        clone_path = os.path.join(self.test_dir, "cloned_repo")
        run_git_or_fail(["clone", bundle_path, clone_path])

        # Verify the cloned repository exists and has content
        self.assertTrue(os.path.exists(clone_path))
        self.assertTrue(os.path.exists(os.path.join(clone_path, "test.txt")))
