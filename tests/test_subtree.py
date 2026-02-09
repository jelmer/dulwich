# test_subtree.py -- tests for subtree.py
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

"""Tests for subtree handling."""

import stat

from dulwich.object_store import MemoryObjectStore
from dulwich.objects import Blob, Commit, ObjectID, Tree
from dulwich.subtree import (
    add_subtree_metadata,
    create_tree_with_subtree,
    extract_subtree,
    find_subtree_splits,
    parse_subtree_metadata,
)

from . import TestCase


class ParseSubtreeMetadataTests(TestCase):
    """Tests for parse_subtree_metadata."""

    def test_empty_commit(self) -> None:
        """Test parsing a commit with no metadata."""
        commit = Commit()
        commit.message = b"Normal commit\n"
        metadata = parse_subtree_metadata(commit)
        self.assertEqual({}, metadata)

    def test_with_all_metadata(self) -> None:
        """Test parsing a commit with all subtree metadata fields."""
        commit = Commit()
        commit.message = (
            b"Subtree operation\n\n"
            b"git-subtree-dir: vendor/lib\n"
            b"git-subtree-split: 1234567890abcdef1234567890abcdef12345678\n"
            b"git-subtree-mainline: abcdef1234567890abcdef1234567890abcdef12\n"
        )
        metadata = parse_subtree_metadata(commit)
        self.assertEqual(b"vendor/lib", metadata["dir"])
        self.assertEqual(b"1234567890abcdef1234567890abcdef12345678", metadata["split"])
        self.assertEqual(
            b"abcdef1234567890abcdef1234567890abcdef12", metadata["mainline"]
        )

    def test_with_only_dir(self) -> None:
        """Test parsing a commit with only dir metadata."""
        commit = Commit()
        commit.message = b"Subtree operation\n\ngit-subtree-dir: vendor/lib\n"
        metadata = parse_subtree_metadata(commit)
        self.assertEqual(b"vendor/lib", metadata["dir"])
        self.assertNotIn("split", metadata)
        self.assertNotIn("mainline", metadata)

    def test_metadata_at_end(self) -> None:
        """Test that metadata is parsed when properly at the end."""
        commit = Commit()
        commit.message = b"Subtree operation\n\nSome description text.\n\ngit-subtree-dir: vendor/lib\n"
        metadata = parse_subtree_metadata(commit)
        # Metadata at the end is parsed correctly
        self.assertEqual(b"vendor/lib", metadata["dir"])


class AddSubtreeMetadataTests(TestCase):
    """Tests for add_subtree_metadata."""

    def test_add_to_simple_message(self) -> None:
        """Test adding metadata to a simple message."""
        message = b"Add subtree"
        prefix = b"vendor/lib"
        split_sha = ObjectID(b"1234567890abcdef1234567890abcdef12345678")

        result = add_subtree_metadata(message, prefix, split=split_sha)

        self.assertIn(b"Add subtree", result)
        self.assertIn(b"git-subtree-dir: vendor/lib", result)
        self.assertIn(
            b"git-subtree-split: 1234567890abcdef1234567890abcdef12345678", result
        )

    def test_add_all_fields(self) -> None:
        """Test adding all metadata fields."""
        message = b"Subtree merge"
        prefix = b"lib"
        split_sha = ObjectID(b"1234567890abcdef1234567890abcdef12345678")
        mainline_sha = ObjectID(b"abcdef1234567890abcdef1234567890abcdef12")

        result = add_subtree_metadata(
            message, prefix, split=split_sha, mainline=mainline_sha
        )

        self.assertIn(b"git-subtree-dir: lib", result)
        self.assertIn(
            b"git-subtree-split: 1234567890abcdef1234567890abcdef12345678", result
        )
        self.assertIn(
            b"git-subtree-mainline: abcdef1234567890abcdef1234567890abcdef12", result
        )

    def test_ensures_blank_line_before_trailers(self) -> None:
        """Test that a blank line is added before trailers."""
        message = b"Short message"
        prefix = b"sub"

        result = add_subtree_metadata(message, prefix)

        # Should have: message + newline + blank line + trailer
        self.assertIn(b"Short message\n\ngit-subtree-dir:", result)


class ExtractSubtreeTests(TestCase):
    """Tests for extract_subtree."""

    def setUp(self):
        super().setUp()
        self.store = MemoryObjectStore()

    def _create_tree_with_path(self, path: bytes, content: bytes = b"test") -> Tree:
        """Helper to create a tree with a file at the given path."""
        blob = Blob.from_string(content)
        self.store.add_object(blob)

        parts = [p for p in path.split(b"/") if p]
        if not parts:
            # Just root with a file
            tree = Tree()
            tree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
            self.store.add_object(tree)
            return tree

        # Build from the bottom up
        current_tree = Tree()
        current_tree.add(
            parts[-1] if len(parts) == 1 else b"file.txt", stat.S_IFREG | 0o644, blob.id
        )
        self.store.add_object(current_tree)

        for part in reversed(parts[:-1]):
            parent_tree = Tree()
            parent_tree.add(parts[parts.index(part) + 1], stat.S_IFDIR, current_tree.id)
            self.store.add_object(parent_tree)
            current_tree = parent_tree

        root_tree = Tree()
        root_tree.add(parts[0], stat.S_IFDIR, current_tree.id)
        self.store.add_object(root_tree)
        return root_tree

    def test_extract_empty_prefix(self) -> None:
        """Test that empty prefix returns the whole tree."""
        blob = Blob.from_string(b"content")
        self.store.add_object(blob)

        tree = Tree()
        tree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(tree)

        result = extract_subtree(self.store, tree, b"")
        self.assertEqual(tree.id, result.id)

    def test_extract_nonexistent(self) -> None:
        """Test extracting a nonexistent subtree returns None."""
        blob = Blob.from_string(b"content")
        self.store.add_object(blob)

        tree = Tree()
        tree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(tree)

        result = extract_subtree(self.store, tree, b"nonexistent/path")
        self.assertIsNone(result)

    def test_extract_single_level(self) -> None:
        """Test extracting a single-level subtree."""
        blob = Blob.from_string(b"sub content")
        self.store.add_object(blob)

        subtree = Tree()
        subtree.add(b"subfile.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(subtree)

        root_tree = Tree()
        root_tree.add(b"subdir", stat.S_IFDIR, subtree.id)
        self.store.add_object(root_tree)

        result = extract_subtree(self.store, root_tree, b"subdir")
        self.assertIsNotNone(result)
        self.assertEqual(subtree.id, result.id)

    def test_extract_nested(self) -> None:
        """Test extracting a nested subtree."""
        blob = Blob.from_string(b"deep content")
        self.store.add_object(blob)

        deep_tree = Tree()
        deep_tree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(deep_tree)

        mid_tree = Tree()
        mid_tree.add(b"lib", stat.S_IFDIR, deep_tree.id)
        self.store.add_object(mid_tree)

        root_tree = Tree()
        root_tree.add(b"vendor", stat.S_IFDIR, mid_tree.id)
        self.store.add_object(root_tree)

        result = extract_subtree(self.store, root_tree, b"vendor/lib")
        self.assertIsNotNone(result)
        self.assertEqual(deep_tree.id, result.id)

    def test_extract_file_not_tree(self) -> None:
        """Test extracting a path that points to a file returns None."""
        blob = Blob.from_string(b"content")
        self.store.add_object(blob)

        tree = Tree()
        tree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(tree)

        result = extract_subtree(self.store, tree, b"file.txt")
        self.assertIsNone(result)


class CreateTreeWithSubtreeTests(TestCase):
    """Tests for create_tree_with_subtree."""

    def setUp(self):
        super().setUp()
        self.store = MemoryObjectStore()

    def test_empty_prefix(self) -> None:
        """Test that empty prefix replaces the whole tree."""
        blob = Blob.from_string(b"content")
        self.store.add_object(blob)

        subtree = Tree()
        subtree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(subtree)

        result_id = create_tree_with_subtree(self.store, None, b"", subtree)
        self.assertEqual(subtree.id, result_id)

    def test_single_level(self) -> None:
        """Test inserting at single level."""
        blob = Blob.from_string(b"sub content")
        self.store.add_object(blob)

        subtree = Tree()
        subtree.add(b"subfile.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(subtree)

        base_blob = Blob.from_string(b"base content")
        self.store.add_object(base_blob)

        base_tree = Tree()
        base_tree.add(b"base.txt", stat.S_IFREG | 0o644, base_blob.id)
        self.store.add_object(base_tree)

        result_id = create_tree_with_subtree(self.store, base_tree, b"vendor", subtree)

        result_tree = self.store[result_id]
        self.assertIsInstance(result_tree, Tree)
        self.assertIn(b"vendor", result_tree)
        self.assertIn(b"base.txt", result_tree)

        mode, vendor_id = result_tree[b"vendor"]
        self.assertTrue(stat.S_ISDIR(mode))
        self.assertEqual(subtree.id, vendor_id)

    def test_nested_path(self) -> None:
        """Test inserting at nested path."""
        blob = Blob.from_string(b"content")
        self.store.add_object(blob)

        subtree = Tree()
        subtree.add(b"file.txt", stat.S_IFREG | 0o644, blob.id)
        self.store.add_object(subtree)

        result_id = create_tree_with_subtree(self.store, None, b"vendor/lib", subtree)

        result_tree = self.store[result_id]
        self.assertIsInstance(result_tree, Tree)
        self.assertIn(b"vendor", result_tree)

        mode, vendor_id = result_tree[b"vendor"]
        self.assertTrue(stat.S_ISDIR(mode))

        vendor_tree = self.store[vendor_id]
        self.assertIn(b"lib", vendor_tree)

        _mode, lib_id = vendor_tree[b"lib"]
        self.assertEqual(subtree.id, lib_id)


class FindSubtreeSplitsTests(TestCase):
    """Tests for find_subtree_splits."""

    def setUp(self):
        super().setUp()
        self.store = MemoryObjectStore()

    def test_no_splits(self) -> None:
        """Test finding splits in history with no subtree metadata."""
        tree = Tree()
        self.store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.message = b"Regular commit\n"
        commit.author = commit.committer = b"Test <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        self.store.add_object(commit)

        splits = find_subtree_splits(self.store, commit, b"vendor/lib")
        self.assertEqual([], splits)

    def test_with_split(self) -> None:
        """Test finding a split in history."""
        tree = Tree()
        self.store.add_object(tree)

        split_sha = ObjectID(b"1234567890abcdef1234567890abcdef12345678")
        commit = Commit()
        commit.tree = tree.id
        commit.message = add_subtree_metadata(
            b"Add subtree\n", b"vendor/lib", split=split_sha
        )
        commit.author = commit.committer = b"Test <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        self.store.add_object(commit)

        splits = find_subtree_splits(self.store, commit, b"vendor/lib")
        self.assertEqual(1, len(splits))
        self.assertEqual(commit.id, splits[0][0])
        self.assertEqual(split_sha, splits[0][1])

    def test_limit(self) -> None:
        """Test that limit parameter works."""
        tree = Tree()
        self.store.add_object(tree)

        # Create two commits with splits
        commit1 = Commit()
        commit1.tree = tree.id
        commit1.message = add_subtree_metadata(
            b"First\n",
            b"lib",
            split=ObjectID(b"1111111111111111111111111111111111111111"),
        )
        commit1.author = commit1.committer = b"Test <test@example.com>"
        commit1.author_time = commit1.commit_time = 1234567890
        commit1.author_timezone = commit1.commit_timezone = 0
        commit1.encoding = b"UTF-8"
        self.store.add_object(commit1)

        commit2 = Commit()
        commit2.tree = tree.id
        commit2.parents = [commit1.id]
        commit2.message = add_subtree_metadata(
            b"Second\n",
            b"lib",
            split=ObjectID(b"2222222222222222222222222222222222222222"),
        )
        commit2.author = commit2.committer = b"Test <test@example.com>"
        commit2.author_time = commit2.commit_time = 1234567891
        commit2.author_timezone = commit2.commit_timezone = 0
        commit2.encoding = b"UTF-8"
        self.store.add_object(commit2)

        # Find with limit
        splits = find_subtree_splits(self.store, commit2, b"lib", limit=1)
        self.assertEqual(1, len(splits))
        self.assertEqual(commit2.id, splits[0][0])
