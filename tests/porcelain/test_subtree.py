# test_subtree.py -- tests for porcelain subtree functions
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

"""Tests for porcelain subtree functions."""

import importlib.util
import os
import shutil
import stat
import tempfile

from dulwich import porcelain
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo
from dulwich.subtree import (
    add_subtree_metadata,
    create_tree_with_subtree,
    extract_subtree,
    parse_subtree_metadata,
)

from .. import DependencyMissing, TestCase


class SubtreeMetadataTests(TestCase):
    """Tests for subtree metadata parsing and creation."""

    def test_parse_empty_commit(self) -> None:
        """Test parsing commit with no subtree metadata."""
        commit = Commit()
        commit.message = b"Regular commit\n"
        metadata = parse_subtree_metadata(commit)
        self.assertEqual({}, metadata)

    def test_parse_with_metadata(self) -> None:
        """Test parsing commit with subtree metadata."""
        commit = Commit()
        commit.message = (
            b"Add subtree\n\n"
            b"git-subtree-dir: vendor/lib\n"
            b"git-subtree-split: 1234567890abcdef1234567890abcdef12345678\n"
        )
        metadata = parse_subtree_metadata(commit)
        self.assertEqual(b"vendor/lib", metadata["dir"])
        self.assertEqual(b"1234567890abcdef1234567890abcdef12345678", metadata["split"])

    def test_add_metadata(self) -> None:
        """Test adding subtree metadata to message."""
        message = b"Add subtree"
        prefix = b"vendor/lib"
        split_sha = b"1234567890abcdef1234567890abcdef12345678"

        result = add_subtree_metadata(message, prefix, split=split_sha)

        self.assertIn(b"git-subtree-dir: vendor/lib", result)
        self.assertIn(
            b"git-subtree-split: 1234567890abcdef1234567890abcdef12345678", result
        )
        self.assertTrue(result.startswith(b"Add subtree\n"))


class SubtreeTreeOperationsTests(TestCase):
    """Tests for tree manipulation operations."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)
        self.addCleanup(self.repo.close)

    def _create_simple_tree(self) -> Tree:
        """Create a simple tree with a file."""
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
        self.repo.object_store.add_object(tree)
        return tree

    def test_extract_nonexistent_subtree(self) -> None:
        """Test extracting a subtree that doesn't exist."""
        tree = self._create_simple_tree()
        result = extract_subtree(self.repo.object_store, tree, b"nonexistent/path")
        self.assertIsNone(result)

    def test_extract_empty_prefix(self) -> None:
        """Test extracting with empty prefix returns whole tree."""
        tree = self._create_simple_tree()
        result = extract_subtree(self.repo.object_store, tree, b"")
        self.assertEqual(tree.id, result.id)

    def test_create_tree_with_subtree_root(self) -> None:
        """Test inserting subtree at root."""
        subtree = self._create_simple_tree()
        result_id = create_tree_with_subtree(self.repo.object_store, None, b"", subtree)
        self.assertEqual(subtree.id, result_id)

    def test_create_tree_with_subtree_nested(self) -> None:
        """Test inserting subtree at nested path."""
        subtree = self._create_simple_tree()
        base_tree = Tree()
        self.repo.object_store.add_object(base_tree)

        result_id = create_tree_with_subtree(
            self.repo.object_store, base_tree, b"vendor/lib", subtree
        )

        # Verify the structure
        result_tree = self.repo.object_store[result_id]
        self.assertIsInstance(result_tree, Tree)
        self.assertIn(b"vendor", result_tree)

        vendor_mode, vendor_id = result_tree[b"vendor"]
        self.assertTrue(stat.S_ISDIR(vendor_mode))

        vendor_tree = self.repo.object_store[vendor_id]
        self.assertIsInstance(vendor_tree, Tree)
        self.assertIn(b"lib", vendor_tree)


class SubtreeAddTests(TestCase):
    """Tests for subtree_add function."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.main_repo_path = os.path.join(self.test_dir, "main")
        self.sub_repo_path = os.path.join(self.test_dir, "sub")

        # Create main repo
        self.main_repo = Repo.init(self.main_repo_path, mkdir=True)
        self.addCleanup(self.main_repo.close)

        # Create initial commit in main repo
        blob = Blob.from_string(b"main content")
        self.main_repo.object_store.add_object(blob)
        tree = Tree()
        tree.add(b"main.txt", stat.S_IFREG | 0o644, blob.id)
        self.main_repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"Initial commit"
        self.main_repo.object_store.add_object(commit)
        self.main_repo.refs[b"refs/heads/main"] = commit.id
        self.main_repo.refs[b"HEAD"] = commit.id

        # Create sub repo
        self.sub_repo = Repo.init(self.sub_repo_path, mkdir=True)
        self.addCleanup(self.sub_repo.close)

        # Create initial commit in sub repo
        sub_blob = Blob.from_string(b"sub content")
        self.sub_repo.object_store.add_object(sub_blob)
        sub_tree = Tree()
        sub_tree.add(b"sub.txt", stat.S_IFREG | 0o644, sub_blob.id)
        self.sub_repo.object_store.add_object(sub_tree)

        sub_commit = Commit()
        sub_commit.tree = sub_tree.id
        sub_commit.author = sub_commit.committer = b"Test <test@example.com>"
        sub_commit.author_time = sub_commit.commit_time = 1234567890
        sub_commit.author_timezone = sub_commit.commit_timezone = 0
        sub_commit.encoding = b"UTF-8"
        sub_commit.message = b"Sub repo initial commit"
        self.sub_repo.object_store.add_object(sub_commit)
        self.sub_repo.refs[b"refs/heads/main"] = sub_commit.id
        self.sub_repo.refs[b"HEAD"] = sub_commit.id

    def test_subtree_add_with_local_path(self) -> None:
        """Test adding a subtree using a local repository path."""
        new_commit_id = porcelain.subtree_add(
            self.main_repo,
            prefix=b"vendor/lib",
            repository=self.sub_repo_path,
            ref=b"main",
        )

        # Verify the new commit was created
        new_commit = self.main_repo[new_commit_id]
        self.assertIsInstance(new_commit, Commit)

        # Verify the tree structure
        new_tree = self.main_repo[new_commit.tree]
        self.assertIsInstance(new_tree, Tree)
        self.assertIn(b"vendor", new_tree)
        self.assertIn(b"main.txt", new_tree)  # Original file should still exist

        # Verify metadata in commit message
        metadata = parse_subtree_metadata(new_commit)
        self.assertEqual(b"vendor/lib", metadata["dir"])

    def test_subtree_add_already_exists(self) -> None:
        """Test that adding to an existing prefix raises an error."""
        # Add once successfully
        porcelain.subtree_add(
            self.main_repo,
            prefix=b"vendor/lib",
            repository=self.sub_repo_path,
            ref=b"main",
        )

        # Try to add again - should fail
        with self.assertRaises(porcelain.Error) as cm:
            porcelain.subtree_add(
                self.main_repo,
                prefix=b"vendor/lib",
                repository=self.sub_repo_path,
                ref=b"main",
            )
        self.assertIn("already exists", str(cm.exception))


class SubtreeSplitTests(TestCase):
    """Tests for subtree_split function."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)
        self.addCleanup(self.repo.close)

        # Create a repo with a subtree
        # First commit: add main file
        blob1 = Blob.from_string(b"main content")
        self.repo.object_store.add_object(blob1)
        tree1 = Tree()
        tree1.add(b"main.txt", stat.S_IFREG | 0o644, blob1.id)
        self.repo.object_store.add_object(tree1)

        commit1 = Commit()
        commit1.tree = tree1.id
        commit1.author = commit1.committer = b"Test <test@example.com>"
        commit1.author_time = commit1.commit_time = 1234567890
        commit1.author_timezone = commit1.commit_timezone = 0
        commit1.encoding = b"UTF-8"
        commit1.message = b"Initial commit"
        self.repo.object_store.add_object(commit1)

        # Second commit: add subtree
        blob2 = Blob.from_string(b"sub content")
        self.repo.object_store.add_object(blob2)

        sub_tree = Tree()
        sub_tree.add(b"sub.txt", stat.S_IFREG | 0o644, blob2.id)
        self.repo.object_store.add_object(sub_tree)

        vendor_tree = Tree()
        vendor_tree.add(b"lib", stat.S_IFDIR, sub_tree.id)
        self.repo.object_store.add_object(vendor_tree)

        tree2 = Tree()
        tree2.add(b"main.txt", stat.S_IFREG | 0o644, blob1.id)
        tree2.add(b"vendor", stat.S_IFDIR, vendor_tree.id)
        self.repo.object_store.add_object(tree2)

        commit2 = Commit()
        commit2.tree = tree2.id
        commit2.parents = [commit1.id]
        commit2.author = commit2.committer = b"Test <test@example.com>"
        commit2.author_time = commit2.commit_time = 1234567891
        commit2.author_timezone = commit2.commit_timezone = 0
        commit2.encoding = b"UTF-8"
        commit2.message = b"Add vendor/lib"
        self.repo.object_store.add_object(commit2)

        self.repo.refs[b"refs/heads/main"] = commit2.id
        self.repo.refs[b"HEAD"] = commit2.id

    def test_subtree_split_basic(self) -> None:
        """Test basic subtree split."""
        split_id = porcelain.subtree_split(self.repo, prefix=b"vendor/lib")

        # Verify split commit was created
        split_commit = self.repo[split_id]
        self.assertIsInstance(split_commit, Commit)

        # Verify the tree is just the subtree
        split_tree = self.repo[split_commit.tree]
        self.assertIsInstance(split_tree, Tree)
        self.assertIn(b"sub.txt", split_tree)
        self.assertNotIn(b"main.txt", split_tree)

    def test_subtree_split_with_branch(self) -> None:
        """Test splitting subtree to a named branch."""
        split_id = porcelain.subtree_split(
            self.repo, prefix=b"vendor/lib", branch=b"split-branch"
        )

        # Verify branch was created
        self.assertIn(b"refs/heads/split-branch", self.repo.refs)
        self.assertEqual(split_id, self.repo.refs[b"refs/heads/split-branch"])

    def test_subtree_split_nonexistent(self) -> None:
        """Test splitting a nonexistent subtree raises an error."""
        with self.assertRaises(porcelain.Error) as cm:
            porcelain.subtree_split(self.repo, prefix=b"nonexistent/path")
        self.assertIn("does not exist", str(cm.exception))


class SubtreeMergeTests(TestCase):
    """Tests for subtree_merge function with proper three-way merge."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)
        self.addCleanup(self.repo.close)

        # Create base commit with initial content
        blob1 = Blob.from_string(b"base content\n")
        self.repo.object_store.add_object(blob1)

        sub_tree = Tree()
        sub_tree.add(b"file.txt", stat.S_IFREG | 0o644, blob1.id)
        self.repo.object_store.add_object(sub_tree)

        vendor_tree = Tree()
        vendor_tree.add(b"lib", stat.S_IFDIR, sub_tree.id)
        self.repo.object_store.add_object(vendor_tree)

        tree = Tree()
        tree.add(b"vendor", stat.S_IFDIR, vendor_tree.id)
        self.repo.object_store.add_object(tree)

        self.base_commit = Commit()
        self.base_commit.tree = tree.id
        self.base_commit.author = self.base_commit.committer = (
            b"Test <test@example.com>"
        )
        self.base_commit.author_time = self.base_commit.commit_time = 1234567890
        self.base_commit.author_timezone = self.base_commit.commit_timezone = 0
        self.base_commit.encoding = b"UTF-8"
        self.base_commit.message = b"Initial commit"
        self.repo.object_store.add_object(self.base_commit)
        self.repo.refs[b"refs/heads/main"] = self.base_commit.id
        self.repo.refs[b"HEAD"] = self.base_commit.id

    def test_subtree_merge_no_conflict(self) -> None:
        """Test merging when there are no conflicts."""
        # Create a commit that modifies the subtree
        blob2 = Blob.from_string(b"base content\nmodified line\n")
        self.repo.object_store.add_object(blob2)

        sub_tree2 = Tree()
        sub_tree2.add(b"file.txt", stat.S_IFREG | 0o644, blob2.id)
        self.repo.object_store.add_object(sub_tree2)

        vendor_tree2 = Tree()
        vendor_tree2.add(b"lib", stat.S_IFDIR, sub_tree2.id)
        self.repo.object_store.add_object(vendor_tree2)

        tree2 = Tree()
        tree2.add(b"vendor", stat.S_IFDIR, vendor_tree2.id)
        self.repo.object_store.add_object(tree2)

        merge_commit = Commit()
        merge_commit.tree = tree2.id
        merge_commit.parents = [self.base_commit.id]
        merge_commit.author = merge_commit.committer = b"Test <test@example.com>"
        merge_commit.author_time = merge_commit.commit_time = 1234567891
        merge_commit.author_timezone = merge_commit.commit_timezone = 0
        merge_commit.encoding = b"UTF-8"
        merge_commit.message = b"Modify subtree"
        self.repo.object_store.add_object(merge_commit)

        # Merge the subtree
        result_id = porcelain.subtree_merge(
            self.repo, prefix=b"vendor/lib", commit=merge_commit
        )

        # Verify merge commit was created
        result_commit = self.repo[result_id]
        self.assertIsInstance(result_commit, Commit)
        self.assertEqual(2, len(result_commit.parents))

        # Verify the merged content
        result_tree = self.repo[result_commit.tree]
        vendor_tree = self.repo[result_tree[b"vendor"][1]]
        lib_tree = self.repo[vendor_tree[b"lib"][1]]
        file_blob = self.repo[lib_tree[b"file.txt"][1]]
        self.assertEqual(b"base content\nmodified line\n", file_blob.data)

    def test_subtree_merge_with_conflict(self) -> None:
        """Test merging when there are conflicts."""
        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        # Create a commit in HEAD that modifies the subtree one way
        blob_head = Blob.from_string(b"base content\nhead modification\n")
        self.repo.object_store.add_object(blob_head)

        sub_tree_head = Tree()
        sub_tree_head.add(b"file.txt", stat.S_IFREG | 0o644, blob_head.id)
        self.repo.object_store.add_object(sub_tree_head)

        vendor_tree_head = Tree()
        vendor_tree_head.add(b"lib", stat.S_IFDIR, sub_tree_head.id)
        self.repo.object_store.add_object(vendor_tree_head)

        tree_head = Tree()
        tree_head.add(b"vendor", stat.S_IFDIR, vendor_tree_head.id)
        self.repo.object_store.add_object(tree_head)

        head_commit = Commit()
        head_commit.tree = tree_head.id
        head_commit.parents = [self.base_commit.id]
        head_commit.author = head_commit.committer = b"Test <test@example.com>"
        head_commit.author_time = head_commit.commit_time = 1234567891
        head_commit.author_timezone = head_commit.commit_timezone = 0
        head_commit.encoding = b"UTF-8"
        head_commit.message = b"Modify subtree in HEAD"
        self.repo.object_store.add_object(head_commit)
        self.repo.refs[b"HEAD"] = head_commit.id

        # Create a commit that modifies the same file differently
        blob_merge = Blob.from_string(b"base content\nmerge modification\n")
        self.repo.object_store.add_object(blob_merge)

        sub_tree_merge = Tree()
        sub_tree_merge.add(b"file.txt", stat.S_IFREG | 0o644, blob_merge.id)
        self.repo.object_store.add_object(sub_tree_merge)

        vendor_tree_merge = Tree()
        vendor_tree_merge.add(b"lib", stat.S_IFDIR, sub_tree_merge.id)
        self.repo.object_store.add_object(vendor_tree_merge)

        tree_merge = Tree()
        tree_merge.add(b"vendor", stat.S_IFDIR, vendor_tree_merge.id)
        self.repo.object_store.add_object(tree_merge)

        merge_commit = Commit()
        merge_commit.tree = tree_merge.id
        merge_commit.parents = [self.base_commit.id]
        merge_commit.author = merge_commit.committer = b"Test <test@example.com>"
        merge_commit.author_time = merge_commit.commit_time = 1234567892
        merge_commit.author_timezone = merge_commit.commit_timezone = 0
        merge_commit.encoding = b"UTF-8"
        merge_commit.message = b"Modify subtree differently"
        self.repo.object_store.add_object(merge_commit)

        # Merge the subtree - should create a merge commit with conflict markers
        result_id = porcelain.subtree_merge(
            self.repo, prefix=b"vendor/lib", commit=merge_commit
        )

        # Verify merge commit was created
        result_commit = self.repo[result_id]
        self.assertIsInstance(result_commit, Commit)

        # Verify the merged content contains conflict markers
        result_tree = self.repo[result_commit.tree]
        vendor_tree = self.repo[result_tree[b"vendor"][1]]
        lib_tree = self.repo[vendor_tree[b"lib"][1]]
        file_blob = self.repo[lib_tree[b"file.txt"][1]]

        # Check for conflict markers
        self.assertIn(b"<<<<<<< ours", file_blob.data)
        self.assertIn(b"=======", file_blob.data)
        self.assertIn(b">>>>>>> theirs", file_blob.data)

    def test_subtree_merge_nonexistent_prefix(self) -> None:
        """Test merging to a nonexistent prefix raises an error."""
        with self.assertRaises(porcelain.Error) as cm:
            porcelain.subtree_merge(
                self.repo, prefix=b"nonexistent/path", commit=self.base_commit
            )
        self.assertIn("does not exist", str(cm.exception))

    def test_subtree_merge_uses_merge_base(self) -> None:
        """Test that merge properly finds and uses the merge base."""
        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise DependencyMissing("merge3")

        # Create a divergent history
        # Branch 1: HEAD moves forward
        blob_head = Blob.from_string(b"base content\nhead line\n")
        self.repo.object_store.add_object(blob_head)

        sub_tree_head = Tree()
        sub_tree_head.add(b"file.txt", stat.S_IFREG | 0o644, blob_head.id)
        self.repo.object_store.add_object(sub_tree_head)

        vendor_tree_head = Tree()
        vendor_tree_head.add(b"lib", stat.S_IFDIR, sub_tree_head.id)
        self.repo.object_store.add_object(vendor_tree_head)

        tree_head = Tree()
        tree_head.add(b"vendor", stat.S_IFDIR, vendor_tree_head.id)
        self.repo.object_store.add_object(tree_head)

        head_commit = Commit()
        head_commit.tree = tree_head.id
        head_commit.parents = [self.base_commit.id]
        head_commit.author = head_commit.committer = b"Test <test@example.com>"
        head_commit.author_time = head_commit.commit_time = 1234567891
        head_commit.author_timezone = head_commit.commit_timezone = 0
        head_commit.encoding = b"UTF-8"
        head_commit.message = b"HEAD modification"
        self.repo.object_store.add_object(head_commit)
        self.repo.refs[b"HEAD"] = head_commit.id

        # Branch 2: Create a different commit from base
        blob_merge = Blob.from_string(b"base content\nmerge line\n")
        self.repo.object_store.add_object(blob_merge)

        sub_tree_merge = Tree()
        sub_tree_merge.add(b"file.txt", stat.S_IFREG | 0o644, blob_merge.id)
        self.repo.object_store.add_object(sub_tree_merge)

        vendor_tree_merge = Tree()
        vendor_tree_merge.add(b"lib", stat.S_IFDIR, sub_tree_merge.id)
        self.repo.object_store.add_object(vendor_tree_merge)

        tree_merge = Tree()
        tree_merge.add(b"vendor", stat.S_IFDIR, vendor_tree_merge.id)
        self.repo.object_store.add_object(tree_merge)

        merge_commit = Commit()
        merge_commit.tree = tree_merge.id
        merge_commit.parents = [self.base_commit.id]
        merge_commit.author = merge_commit.committer = b"Test <test@example.com>"
        merge_commit.author_time = merge_commit.commit_time = 1234567892
        merge_commit.author_timezone = merge_commit.commit_timezone = 0
        merge_commit.encoding = b"UTF-8"
        merge_commit.message = b"Branch modification"
        self.repo.object_store.add_object(merge_commit)

        # Merge - should find base_commit as merge base
        result_id = porcelain.subtree_merge(
            self.repo, prefix=b"vendor/lib", commit=merge_commit
        )

        # Verify result has both parents
        result_commit = self.repo[result_id]
        self.assertEqual(2, len(result_commit.parents))
        self.assertEqual(head_commit.id, result_commit.parents[0])
        self.assertEqual(merge_commit.id, result_commit.parents[1])
