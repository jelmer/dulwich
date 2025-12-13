# test_sha256_packs.py -- Compatibility tests for SHA256 pack files
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

"""Compatibility tests for SHA256 pack files with git command line tools."""

import os
import tempfile

from dulwich.object_format import SHA256
from dulwich.objects import Blob, Commit, Tree
from dulwich.pack import load_pack_index_file
from dulwich.repo import Repo

from .utils import CompatTestCase, rmtree_ro, run_git_or_fail


class GitSHA256PackCompatibilityTests(CompatTestCase):
    """Test SHA256 pack file compatibility with git command line tools."""

    min_git_version = (2, 29, 0)

    def _run_git(self, args, cwd=None):
        """Run git command in the specified directory."""
        return run_git_or_fail(args, cwd=cwd)

    def test_git_pack_readable_by_dulwich(self):
        """Test that git-created SHA256 pack files are readable by dulwich."""
        # Create SHA256 repo with git
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        self._run_git(["init", "--object-format=sha256", repo_path])

        # Create multiple files to ensure pack creation
        for i in range(20):
            test_file = os.path.join(repo_path, f"file{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"Content for file {i}\n")

        self._run_git(["add", "."], cwd=repo_path)
        self._run_git(["commit", "-m", "Add 20 files"], cwd=repo_path)

        # Force pack creation
        self._run_git(["gc"], cwd=repo_path)

        # Open with dulwich
        repo = Repo(repo_path)
        self.assertEqual(repo.object_format, SHA256)

        # Find pack files
        pack_dir = os.path.join(repo_path, ".git", "objects", "pack")
        pack_files = [f for f in os.listdir(pack_dir) if f.endswith(".pack")]
        self.assertGreater(len(pack_files), 0, "No pack files created")

        # Read pack with dulwich
        for pack_file in pack_files:
            pack_path = os.path.join(pack_dir, pack_file)
            idx_path = pack_path[:-5] + ".idx"

            # Load pack index with SHA256 algorithm
            with open(idx_path, "rb") as f:
                pack_idx = load_pack_index_file(idx_path, f, SHA256)

            # Verify it's detected as SHA256
            self.assertEqual(pack_idx.hash_size, 32)

            # Verify we can iterate objects
            obj_count = 0
            for sha, offset, crc32 in pack_idx.iterentries():
                self.assertEqual(len(sha), 32)  # SHA256
                obj_count += 1

            self.assertGreater(obj_count, 20)  # At least our files + trees + commit

        # Verify we can read all objects through the repo interface
        head_ref = repo.refs[b"refs/heads/master"]
        commit = repo[head_ref]
        self.assertIsInstance(commit, Commit)

        # Read the tree
        tree = repo[commit.tree]
        self.assertIsInstance(tree, Tree)

        # Verify all files are there
        file_count = 0
        for name, mode, sha in tree.items():
            if name.startswith(b"file") and name.endswith(b".txt"):
                file_count += 1
                # Read the blob
                blob = repo[sha]
                self.assertIsInstance(blob, Blob)

        self.assertEqual(file_count, 20)
        repo.close()

    def test_dulwich_objects_readable_by_git(self):
        """Test that dulwich-created SHA256 objects are readable by git."""
        # Create SHA256 repo with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        repo = Repo.init(repo_path, mkdir=False, object_format="sha256")

        # Create objects
        blobs = []
        for i in range(10):
            blob = Blob.from_string(f"Dulwich blob content {i}".encode())
            repo.object_store.add_object(blob)
            blobs.append(blob)

        # Create a tree with all blobs
        tree = Tree()
        for i, blob in enumerate(blobs):
            tree.add(f"blob{i}.txt".encode(), 0o100644, blob.get_id(SHA256))
        repo.object_store.add_object(tree)

        # Create a commit
        commit = Commit()
        commit.tree = tree.get_id(SHA256)
        commit.author = commit.committer = b"Dulwich Test <test@dulwich.org>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit with blobs"
        repo.object_store.add_object(commit)

        # Update HEAD
        repo.refs[b"refs/heads/master"] = commit.get_id(SHA256)
        repo.close()

        # Verify git can read all objects
        output = self._run_git(["rev-parse", "HEAD"], cwd=repo_path)
        self.assertEqual(len(output.strip()), 64)  # SHA256

        # List tree contents
        tree_output = self._run_git(["ls-tree", "HEAD"], cwd=repo_path)
        # Count lines instead of occurrences of "blob" since "blob" appears twice per line
        lines = tree_output.strip().split(b"\n")
        self.assertEqual(len(lines), 10)

        # Verify git can check out the content
        self._run_git(["checkout", "HEAD", "--", "."], cwd=repo_path)

        # Verify files exist with correct content
        for i in range(10):
            file_path = os.path.join(repo_path, f"blob{i}.txt")
            self.assertTrue(os.path.exists(file_path))
            with open(file_path, "rb") as f:
                content = f.read()
                self.assertEqual(content, f"Dulwich blob content {i}".encode())

    def test_large_pack_interop(self):
        """Test large pack file interoperability."""
        # Create repo with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        repo = Repo.init(repo_path, mkdir=False, object_format="sha256")

        # Create a large file that will use delta compression
        large_content = b"A" * 10000
        blobs = []

        # Create similar blobs to trigger delta compression
        for i in range(10):
            content = large_content + f" variation {i}".encode()
            blob = Blob.from_string(content)
            repo.object_store.add_object(blob)
            blobs.append(blob)

        # Create tree
        tree = Tree()
        for i, blob in enumerate(blobs):
            tree.add(f"large{i}.txt".encode(), 0o100644, blob.get_id(SHA256))
        repo.object_store.add_object(tree)

        # Create commit
        commit = Commit()
        commit.tree = tree.get_id(SHA256)
        commit.author = commit.committer = b"Test <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Large files for delta compression test"
        repo.object_store.add_object(commit)

        repo.refs[b"refs/heads/master"] = commit.get_id(SHA256)
        repo.close()

        # Run git gc to create packs with delta compression
        self._run_git(["gc", "--aggressive"], cwd=repo_path)

        # Verify git created a pack
        pack_dir = os.path.join(repo_path, ".git", "objects", "pack")
        pack_files = [f for f in os.listdir(pack_dir) if f.endswith(".pack")]
        self.assertGreater(len(pack_files), 0)

        # Re-open with dulwich and verify we can read everything
        repo = Repo(repo_path)
        head = repo.refs[b"refs/heads/master"]
        commit = repo[head]
        tree = repo[commit.tree]

        # Read all blobs
        for i in range(10):
            name = f"large{i}.txt".encode()
            _mode, sha = tree[name]
            blob = repo[sha]
            expected = large_content + f" variation {i}".encode()
            self.assertEqual(blob.data, expected)

        repo.close()

    def test_mixed_loose_packed_objects(self):
        """Test repositories with both loose and packed objects."""
        # Create repo with git
        repo_path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, repo_path)
        self._run_git(["init", "--object-format=sha256", repo_path])

        # Create initial objects that will be packed
        for i in range(5):
            test_file = os.path.join(repo_path, f"packed{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"Will be packed {i}\n")

        self._run_git(["add", "."], cwd=repo_path)
        self._run_git(["commit", "-m", "Initial packed objects"], cwd=repo_path)
        self._run_git(["gc"], cwd=repo_path)

        # Create more objects that will remain loose
        for i in range(5):
            test_file = os.path.join(repo_path, f"loose{i}.txt")
            with open(test_file, "w") as f:
                f.write(f"Will stay loose {i}\n")

        self._run_git(["add", "."], cwd=repo_path)
        self._run_git(["commit", "-m", "Loose objects"], cwd=repo_path)

        # Open with dulwich
        repo = Repo(repo_path)

        # Count objects in packs vs loose
        pack_dir = os.path.join(repo_path, ".git", "objects", "pack")
        pack_count = len([f for f in os.listdir(pack_dir) if f.endswith(".pack")])
        self.assertGreater(pack_count, 0)

        # Verify we can read all objects
        head = repo.refs[b"refs/heads/master"]
        commit = repo[head]

        # Walk the commit history
        commit_count = 0
        while commit.parents:
            commit_count += 1
            tree = repo[commit.tree]
            # Verify we can read the tree
            self.assertGreater(len(tree), 0)

            if commit.parents:
                commit = repo[commit.parents[0]]
            else:
                break

        self.assertEqual(commit_count, 1)  # We made 2 commits total
        repo.close()


if __name__ == "__main__":
    import unittest

    unittest.main()
