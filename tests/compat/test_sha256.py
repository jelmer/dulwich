# test_sha256.py -- Compatibility tests for SHA256 support
# Copyright (C) 2024 The Dulwich contributors
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

"""Compatibility tests for SHA256 support with git command line tools."""

import os
import tempfile

from dulwich.object_format import SHA256
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo

from .utils import CompatTestCase, rmtree_ro, run_git_or_fail


class GitSHA256CompatibilityTests(CompatTestCase):
    """Test SHA256 compatibility with git command line tools."""

    min_git_version = (2, 29, 0)

    def _run_git(self, args, cwd=None):
        """Run git command in the specified directory."""
        return run_git_or_fail(args, cwd=cwd)

    def test_sha256_repo_creation_compat(self):
        """Test that dulwich-created SHA256 repos are readable by git."""
        # Create SHA256 repo with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        repo = Repo.init(repo_path, mkdir=False, object_format="sha256")

        # Add a blob and tree using dulwich
        blob = Blob.from_string(b"Hello SHA256 world!")
        tree = Tree()
        tree.add(b"hello.txt", 0o100644, blob.get_id(SHA256))

        # Create objects in the repository
        object_store = repo.object_store
        object_store.add_object(blob)
        object_store.add_object(tree)

        repo.close()

        # Verify git can read the repository
        config_output = self._run_git(
            ["config", "--get", "extensions.objectformat"], cwd=repo_path
        )
        self.assertEqual(config_output.strip(), b"sha256")

        # Verify git recognizes it as a SHA256 repository
        rev_parse_output = self._run_git(
            ["rev-parse", "--show-object-format"], cwd=repo_path
        )
        self.assertEqual(rev_parse_output.strip(), b"sha256")

    def test_git_created_sha256_repo_readable(self):
        """Test that git-created SHA256 repos are readable by dulwich."""
        # Create SHA256 repo with git
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        self._run_git(["init", "--object-format=sha256", repo_path])

        # Create a file and commit with git
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("Test SHA256 content")

        self._run_git(["add", "test.txt"], cwd=repo_path)
        self._run_git(["commit", "-m", "Test SHA256 commit"], cwd=repo_path)

        # Read with dulwich
        repo = Repo(repo_path)

        # Verify dulwich detects SHA256
        self.assertEqual(repo.object_format, SHA256)

        # Verify dulwich can read objects
        # Try both main and master branches (git default changed over time)
        try:
            head_ref = repo.refs[b"refs/heads/main"]
        except KeyError:
            head_ref = repo.refs[b"refs/heads/master"]
        self.assertEqual(len(head_ref), 64)  # SHA256 length

        # Read the commit object
        commit = repo[head_ref]
        self.assertIsInstance(commit, Commit)
        self.assertEqual(len(commit.tree), 64)  # SHA256 tree ID

        repo.close()

    def test_object_hashing_consistency(self):
        """Test that object hashing is consistent between dulwich and git."""
        # Create SHA256 repo with git
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        self._run_git(["init", "--object-format=sha256", repo_path])

        # Create a test file with known content
        test_content = b"Test content for SHA256 hashing consistency"
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Get git's hash for the content
        git_hash = self._run_git(["hash-object", "test.txt"], cwd=repo_path)
        git_hash = git_hash.strip().decode("ascii")

        # Create same blob with dulwich
        blob = Blob.from_string(test_content)
        dulwich_hash = blob.get_id(SHA256).decode("ascii")

        # Hashes should match
        self.assertEqual(git_hash, dulwich_hash)

    def test_tree_hashing_consistency(self):
        """Test that tree hashing is consistent between dulwich and git."""
        # Create SHA256 repo with git
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        self._run_git(["init", "--object-format=sha256", repo_path])

        # Create a test file and add to index
        test_content = b"Tree test content"
        test_file = os.path.join(repo_path, "tree_test.txt")
        with open(test_file, "wb") as f:
            f.write(test_content)

        self._run_git(["add", "tree_test.txt"], cwd=repo_path)

        # Get git's tree hash
        git_tree_hash = self._run_git(["write-tree"], cwd=repo_path)
        git_tree_hash = git_tree_hash.strip().decode("ascii")

        # Create same tree with dulwich
        blob = Blob.from_string(test_content)
        tree = Tree()
        tree.add(b"tree_test.txt", 0o100644, blob.get_id(SHA256))

        dulwich_tree_hash = tree.get_id(SHA256).decode("ascii")

        # Tree hashes should match
        self.assertEqual(git_tree_hash, dulwich_tree_hash)

    def test_commit_creation_interop(self):
        """Test commit creation interoperability between dulwich and git."""
        # Create SHA256 repo with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        repo = Repo.init(repo_path, mkdir=False, object_format="sha256")

        # Create objects with dulwich
        blob = Blob.from_string(b"Interop test content")
        tree = Tree()
        tree.add(b"interop.txt", 0o100644, blob.get_id(SHA256))

        commit = Commit()
        commit.tree = tree.get_id(SHA256)
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test SHA256 commit from dulwich"

        # Add objects to repo
        object_store = repo.object_store
        object_store.add_object(blob)
        object_store.add_object(tree)
        object_store.add_object(commit)

        # Update HEAD
        commit_id = commit.get_id(SHA256)
        repo.refs[b"refs/heads/master"] = commit_id
        repo.close()

        # Verify git can read the commit
        commit_hash = self._run_git(["rev-parse", "HEAD"], cwd=repo_path)
        commit_hash = commit_hash.strip().decode("ascii")
        self.assertEqual(len(commit_hash), 64)  # SHA256 length

        # Verify git can show the commit
        commit_message = self._run_git(["log", "--format=%s", "-n", "1"], cwd=repo_path)
        self.assertEqual(commit_message.strip(), b"Test SHA256 commit from dulwich")

        # Verify git can list the tree
        tree_content = self._run_git(["ls-tree", "HEAD"], cwd=repo_path)
        self.assertIn(b"interop.txt", tree_content)

    def test_ref_updates_interop(self):
        """Test that ref updates work between dulwich and git."""
        # Create repo with git
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        self._run_git(["init", "--object-format=sha256", repo_path])

        # Create initial commit with git
        test_file = os.path.join(repo_path, "initial.txt")
        with open(test_file, "w") as f:
            f.write("Initial content")

        self._run_git(["add", "initial.txt"], cwd=repo_path)
        self._run_git(["commit", "-m", "Initial commit"], cwd=repo_path)

        initial_commit = self._run_git(["rev-parse", "HEAD"], cwd=repo_path)
        initial_commit = initial_commit.strip()

        # Update ref with dulwich
        repo = Repo(repo_path)

        # Create new commit with dulwich
        blob = Blob.from_string(b"New content from dulwich")
        tree = Tree()
        tree.add(b"dulwich.txt", 0o100644, blob.get_id(SHA256))

        commit = Commit()
        commit.tree = tree.get_id(SHA256)
        commit.parents = [initial_commit]
        commit.author = commit.committer = b"Dulwich User <dulwich@example.com>"
        commit.commit_time = commit.author_time = 1234567891
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Commit from dulwich"

        # Add objects and update ref
        object_store = repo.object_store
        object_store.add_object(blob)
        object_store.add_object(tree)
        object_store.add_object(commit)

        new_commit_hash = commit.get_id(SHA256)
        repo.refs[b"refs/heads/master"] = new_commit_hash
        repo.close()

        # Verify git sees the update
        current_commit = self._run_git(["rev-parse", "HEAD"], cwd=repo_path)
        current_commit = current_commit.strip().decode("ascii")
        self.assertEqual(current_commit, new_commit_hash.decode("ascii"))

        # Verify git can access the new tree
        tree_listing = self._run_git(["ls-tree", "HEAD"], cwd=repo_path)
        self.assertIn(b"dulwich.txt", tree_listing)

    def test_clone_sha256_repo_git_to_dulwich(self):
        """Test cloning a git SHA256 repository with dulwich."""
        # Create source repo with git
        source_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, source_path)
        self._run_git(["init", "--object-format=sha256", source_path])

        # Add content
        test_file = os.path.join(source_path, "clone_test.txt")
        with open(test_file, "w") as f:
            f.write("Content to be cloned")

        self._run_git(["add", "clone_test.txt"], cwd=source_path)
        self._run_git(["commit", "-m", "Initial commit"], cwd=source_path)

        # Clone with dulwich
        target_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, target_path)

        target_repo = Repo.init(target_path, mkdir=False, object_format="sha256")

        # Copy objects (simplified clone)
        source_repo = Repo(source_path)

        # Copy all objects
        for obj_id in source_repo.object_store:
            obj = source_repo.object_store[obj_id]
            target_repo.object_store.add_object(obj)

        # Copy refs
        for ref_name in source_repo.refs.keys():
            ref_id = source_repo.refs[ref_name]
            target_repo.refs[ref_name] = ref_id

        # Set HEAD
        target_repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        source_repo.close()
        target_repo.close()

        # Verify with git
        output = self._run_git(["rev-parse", "--show-object-format"], cwd=target_path)
        self.assertEqual(output.strip(), b"sha256")

        # Verify content
        self._run_git(["checkout", "HEAD", "--", "."], cwd=target_path)
        cloned_file = os.path.join(target_path, "clone_test.txt")
        with open(cloned_file) as f:
            content = f.read()
        self.assertEqual(content, "Content to be cloned")

    def test_fsck_sha256_repo(self):
        """Test that git fsck works on dulwich-created SHA256 repos."""
        # Create repo with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        repo = Repo.init(repo_path, mkdir=False, object_format="sha256")

        # Create a more complex object graph
        # Multiple blobs
        blobs = []
        for i in range(5):
            blob = Blob.from_string(f"Blob content {i}".encode())
            repo.object_store.add_object(blob)
            blobs.append(blob)

        # Multiple trees
        subtree = Tree()
        subtree.add(b"subfile1.txt", 0o100644, blobs[0].get_id(SHA256))
        subtree.add(b"subfile2.txt", 0o100644, blobs[1].get_id(SHA256))
        repo.object_store.add_object(subtree)

        main_tree = Tree()
        main_tree.add(b"file1.txt", 0o100644, blobs[2].get_id(SHA256))
        main_tree.add(b"file2.txt", 0o100644, blobs[3].get_id(SHA256))
        main_tree.add(b"subdir", 0o040000, subtree.get_id(SHA256))
        repo.object_store.add_object(main_tree)

        # Create commits
        commit1 = Commit()
        commit1.tree = main_tree.get_id(SHA256)
        commit1.author = commit1.committer = b"Test <test@example.com>"
        commit1.commit_time = commit1.author_time = 1234567890
        commit1.commit_timezone = commit1.author_timezone = 0
        commit1.message = b"First commit"
        repo.object_store.add_object(commit1)

        commit2 = Commit()
        commit2.tree = main_tree.get_id(SHA256)
        commit2.parents = [commit1.get_id(SHA256)]
        commit2.author = commit2.committer = b"Test <test@example.com>"
        commit2.commit_time = commit2.author_time = 1234567891
        commit2.commit_timezone = commit2.author_timezone = 0
        commit2.message = b"Second commit"
        repo.object_store.add_object(commit2)

        # Set refs
        repo.refs[b"refs/heads/master"] = commit2.get_id(SHA256)
        repo.refs[b"refs/heads/branch1"] = commit1.get_id(SHA256)

        repo.close()

        # Run git fsck
        fsck_output = self._run_git(["fsck", "--full"], cwd=repo_path)
        # fsck should not report any errors (empty output or success message)
        self.assertNotIn(b"error", fsck_output.lower())
        self.assertNotIn(b"missing", fsck_output.lower())
        self.assertNotIn(b"broken", fsck_output.lower())

    def test_dulwich_clone_sha256_repo(self):
        """Test that dulwich's clone() auto-detects SHA-256 format from git repo."""
        from dulwich.client import LocalGitClient

        # Create source SHA-256 repo with git
        source_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, source_path)
        self._run_git(
            ["init", "--object-format=sha256", "--initial-branch=main", source_path]
        )

        # Add content and commit
        test_file = os.path.join(source_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("SHA-256 clone test")

        self._run_git(["add", "test.txt"], cwd=source_path)
        self._run_git(["commit", "-m", "Test commit"], cwd=source_path)

        # Clone with dulwich LocalGitClient
        target_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, target_path)

        client = LocalGitClient()
        cloned_repo = client.clone(source_path, target_path, mkdir=False)
        self.addCleanup(cloned_repo.close)

        # Verify the cloned repo is SHA-256
        self.assertEqual(cloned_repo.object_format, SHA256)

        # Verify config has correct objectformat extension
        config = cloned_repo.get_config()
        self.assertEqual(b"sha256", config.get((b"extensions",), b"objectformat"))

        # Verify git also sees it as SHA-256
        output = self._run_git(["rev-parse", "--show-object-format"], cwd=target_path)
        self.assertEqual(output.strip(), b"sha256")

        # Verify objects were cloned correctly
        source_head = self._run_git(["rev-parse", "refs/heads/main"], cwd=source_path)
        cloned_head = cloned_repo.refs[b"refs/heads/main"]
        self.assertEqual(source_head.strip(), cloned_head)
