# test_object_filters.py -- Tests for object filtering
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

"""Tests for object filtering (partial clone filter specifications)."""

import os
import tempfile

from dulwich.object_filters import (
    BlobLimitFilter,
    BlobNoneFilter,
    CombineFilter,
    SparseOidFilter,
    TreeDepthFilter,
    filter_pack_objects,
    parse_filter_spec,
)
from dulwich.object_store import MemoryObjectStore
from dulwich.objects import Blob, Tree
from dulwich.repo import Repo
from dulwich.tests.utils import make_commit

from . import TestCase


class ParseFilterSpecTests(TestCase):
    """Test parse_filter_spec function."""

    def test_parse_blob_none(self):
        """Test parsing 'blob:none' filter."""
        filter_spec = parse_filter_spec("blob:none")
        self.assertIsInstance(filter_spec, BlobNoneFilter)
        self.assertEqual("blob:none", filter_spec.to_spec_string())

    def test_parse_blob_none_bytes(self):
        """Test parsing 'blob:none' as bytes."""
        filter_spec = parse_filter_spec(b"blob:none")
        self.assertIsInstance(filter_spec, BlobNoneFilter)

    def test_parse_blob_limit_bytes(self):
        """Test parsing 'blob:limit=100' in bytes."""
        filter_spec = parse_filter_spec("blob:limit=100")
        self.assertIsInstance(filter_spec, BlobLimitFilter)
        self.assertEqual(100, filter_spec.limit)

    def test_parse_blob_limit_kb(self):
        """Test parsing 'blob:limit=10k'."""
        filter_spec = parse_filter_spec("blob:limit=10k")
        self.assertIsInstance(filter_spec, BlobLimitFilter)
        self.assertEqual(10 * 1024, filter_spec.limit)

    def test_parse_blob_limit_mb(self):
        """Test parsing 'blob:limit=5m'."""
        filter_spec = parse_filter_spec("blob:limit=5m")
        self.assertIsInstance(filter_spec, BlobLimitFilter)
        self.assertEqual(5 * 1024 * 1024, filter_spec.limit)

    def test_parse_blob_limit_gb(self):
        """Test parsing 'blob:limit=1g'."""
        filter_spec = parse_filter_spec("blob:limit=1g")
        self.assertIsInstance(filter_spec, BlobLimitFilter)
        self.assertEqual(1024 * 1024 * 1024, filter_spec.limit)

    def test_parse_tree_depth(self):
        """Test parsing 'tree:0' filter."""
        filter_spec = parse_filter_spec("tree:0")
        self.assertIsInstance(filter_spec, TreeDepthFilter)
        self.assertEqual(0, filter_spec.max_depth)

    def test_parse_tree_depth_nonzero(self):
        """Test parsing 'tree:3' filter."""
        filter_spec = parse_filter_spec("tree:3")
        self.assertIsInstance(filter_spec, TreeDepthFilter)
        self.assertEqual(3, filter_spec.max_depth)

    def test_parse_sparse_oid(self):
        """Test parsing 'sparse:oid=<oid>' filter."""
        oid = b"1234567890abcdef1234567890abcdef12345678"
        filter_spec = parse_filter_spec(f"sparse:oid={oid.decode('ascii')}")
        self.assertIsInstance(filter_spec, SparseOidFilter)
        self.assertEqual(oid, filter_spec.oid)

    def test_parse_combine(self):
        """Test parsing 'combine:blob:none+tree:0' filter."""
        filter_spec = parse_filter_spec("combine:blob:none+tree:0")
        self.assertIsInstance(filter_spec, CombineFilter)
        self.assertEqual(2, len(filter_spec.filters))
        self.assertIsInstance(filter_spec.filters[0], BlobNoneFilter)
        self.assertIsInstance(filter_spec.filters[1], TreeDepthFilter)

    def test_parse_combine_multiple(self):
        """Test parsing combine filter with 3+ filters."""
        filter_spec = parse_filter_spec("combine:blob:none+tree:0+blob:limit=1m")
        self.assertIsInstance(filter_spec, CombineFilter)
        self.assertEqual(3, len(filter_spec.filters))

    def test_parse_unknown_spec(self):
        """Test that unknown filter specs raise ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("unknown:spec")
        self.assertIn("Unknown filter specification", str(cm.exception))

    def test_parse_invalid_tree_depth(self):
        """Test that invalid tree depth raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("tree:invalid")
        self.assertIn("Invalid tree filter", str(cm.exception))

    def test_parse_invalid_blob_limit(self):
        """Test that invalid blob limit raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("blob:limit=invalid")
        self.assertIn("Invalid", str(cm.exception))

    def test_parse_empty_spec(self):
        """Test that empty filter spec raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("")
        self.assertIn("cannot be empty", str(cm.exception))

    def test_parse_blob_limit_no_value(self):
        """Test that blob:limit without value raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("blob:limit=")
        self.assertIn("requires a size value", str(cm.exception))

    def test_parse_tree_no_value(self):
        """Test that tree: without depth raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("tree:")
        self.assertIn("requires a depth value", str(cm.exception))

    def test_parse_tree_negative_depth(self):
        """Test that negative tree depth raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("tree:-1")
        self.assertIn("non-negative", str(cm.exception))

    def test_parse_sparse_oid_invalid_length(self):
        """Test that invalid OID length raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("sparse:oid=abc123")
        self.assertIn("40 or 64 hex chars", str(cm.exception))

    def test_parse_sparse_oid_invalid_hex(self):
        """Test that non-hex OID raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("sparse:oid=" + "x" * 40)
        self.assertIn("valid object ID", str(cm.exception))

    def test_parse_combine_single_filter(self):
        """Test that combine with single filter raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("combine:blob:none")
        self.assertIn("at least two filters", str(cm.exception))

    def test_parse_unknown_with_helpful_message(self):
        """Test that unknown spec gives helpful error message."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("unknown:spec")
        error_msg = str(cm.exception)
        self.assertIn("Unknown filter specification", error_msg)
        self.assertIn("Supported formats", error_msg)
        self.assertIn("blob:none", error_msg)


class BlobNoneFilterTests(TestCase):
    """Test BlobNoneFilter class."""

    def test_should_include_blob(self):
        """Test that BlobNoneFilter excludes all blobs."""
        filter_spec = BlobNoneFilter()
        self.assertFalse(filter_spec.should_include_blob(0))
        self.assertFalse(filter_spec.should_include_blob(100))
        self.assertFalse(filter_spec.should_include_blob(1024 * 1024))

    def test_should_include_tree(self):
        """Test that BlobNoneFilter includes all trees."""
        filter_spec = BlobNoneFilter()
        self.assertTrue(filter_spec.should_include_tree(0))
        self.assertTrue(filter_spec.should_include_tree(1))
        self.assertTrue(filter_spec.should_include_tree(100))

    def test_to_spec_string(self):
        """Test conversion back to spec string."""
        filter_spec = BlobNoneFilter()
        self.assertEqual("blob:none", filter_spec.to_spec_string())

    def test_repr(self):
        """Test repr output."""
        filter_spec = BlobNoneFilter()
        self.assertEqual("BlobNoneFilter()", repr(filter_spec))


class BlobLimitFilterTests(TestCase):
    """Test BlobLimitFilter class."""

    def test_should_include_blob_under_limit(self):
        """Test that blobs under limit are included."""
        filter_spec = BlobLimitFilter(1024)
        self.assertTrue(filter_spec.should_include_blob(0))
        self.assertTrue(filter_spec.should_include_blob(512))
        self.assertTrue(filter_spec.should_include_blob(1024))

    def test_should_include_blob_over_limit(self):
        """Test that blobs over limit are excluded."""
        filter_spec = BlobLimitFilter(1024)
        self.assertFalse(filter_spec.should_include_blob(1025))
        self.assertFalse(filter_spec.should_include_blob(2048))

    def test_should_include_tree(self):
        """Test that BlobLimitFilter includes all trees."""
        filter_spec = BlobLimitFilter(1024)
        self.assertTrue(filter_spec.should_include_tree(0))
        self.assertTrue(filter_spec.should_include_tree(100))

    def test_to_spec_string_bytes(self):
        """Test conversion to spec string with bytes."""
        filter_spec = BlobLimitFilter(100)
        self.assertEqual("blob:limit=100", filter_spec.to_spec_string())

    def test_to_spec_string_kb(self):
        """Test conversion to spec string with KB."""
        filter_spec = BlobLimitFilter(10 * 1024)
        self.assertEqual("blob:limit=10k", filter_spec.to_spec_string())

    def test_to_spec_string_mb(self):
        """Test conversion to spec string with MB."""
        filter_spec = BlobLimitFilter(5 * 1024 * 1024)
        self.assertEqual("blob:limit=5m", filter_spec.to_spec_string())

    def test_to_spec_string_gb(self):
        """Test conversion to spec string with GB."""
        filter_spec = BlobLimitFilter(2 * 1024 * 1024 * 1024)
        self.assertEqual("blob:limit=2g", filter_spec.to_spec_string())

    def test_to_spec_string_not_round(self):
        """Test conversion to spec string with non-round size."""
        filter_spec = BlobLimitFilter(1500)
        self.assertEqual("blob:limit=1500", filter_spec.to_spec_string())

    def test_repr(self):
        """Test repr output."""
        filter_spec = BlobLimitFilter(1024)
        self.assertEqual("BlobLimitFilter(limit=1024)", repr(filter_spec))


class TreeDepthFilterTests(TestCase):
    """Test TreeDepthFilter class."""

    def test_should_include_blob(self):
        """Test that TreeDepthFilter includes all blobs."""
        filter_spec = TreeDepthFilter(0)
        self.assertTrue(filter_spec.should_include_blob(0))
        self.assertTrue(filter_spec.should_include_blob(1024))

    def test_should_include_tree_at_depth(self):
        """Test that trees at or below max_depth are included."""
        filter_spec = TreeDepthFilter(2)
        self.assertTrue(filter_spec.should_include_tree(0))
        self.assertTrue(filter_spec.should_include_tree(1))
        self.assertTrue(filter_spec.should_include_tree(2))

    def test_should_include_tree_beyond_depth(self):
        """Test that trees beyond max_depth are excluded."""
        filter_spec = TreeDepthFilter(2)
        self.assertFalse(filter_spec.should_include_tree(3))
        self.assertFalse(filter_spec.should_include_tree(10))

    def test_to_spec_string(self):
        """Test conversion back to spec string."""
        filter_spec = TreeDepthFilter(3)
        self.assertEqual("tree:3", filter_spec.to_spec_string())

    def test_repr(self):
        """Test repr output."""
        filter_spec = TreeDepthFilter(2)
        self.assertEqual("TreeDepthFilter(max_depth=2)", repr(filter_spec))


class SparseOidFilterTests(TestCase):
    """Test SparseOidFilter class."""

    def test_should_include_blob(self):
        """Test that SparseOidFilter includes all blobs."""
        oid = b"1234567890abcdef1234567890abcdef12345678"
        filter_spec = SparseOidFilter(oid)
        self.assertTrue(filter_spec.should_include_blob(0))
        self.assertTrue(filter_spec.should_include_blob(1024))

    def test_should_include_tree(self):
        """Test that SparseOidFilter includes all trees."""
        oid = b"1234567890abcdef1234567890abcdef12345678"
        filter_spec = SparseOidFilter(oid)
        self.assertTrue(filter_spec.should_include_tree(0))
        self.assertTrue(filter_spec.should_include_tree(10))

    def test_to_spec_string(self):
        """Test conversion back to spec string."""
        oid = b"1234567890abcdef1234567890abcdef12345678"
        filter_spec = SparseOidFilter(oid)
        expected = "sparse:oid=1234567890abcdef1234567890abcdef12345678"
        self.assertEqual(expected, filter_spec.to_spec_string())

    def test_repr(self):
        """Test repr output."""
        oid = b"1234567890abcdef1234567890abcdef12345678"
        filter_spec = SparseOidFilter(oid)
        self.assertIn("SparseOidFilter", repr(filter_spec))
        self.assertIn("1234567890abcdef1234567890abcdef12345678", repr(filter_spec))

    def test_load_patterns_from_blob(self):
        """Test loading sparse patterns from a blob object."""
        from dulwich.object_store import MemoryObjectStore
        from dulwich.objects import Blob

        # Create a sparse patterns blob
        patterns = b"*.txt\n!*.log\n/src/\n"
        blob = Blob.from_string(patterns)

        object_store = MemoryObjectStore()
        object_store.add_object(blob)

        filter_spec = SparseOidFilter(blob.id, object_store=object_store)
        filter_spec._load_patterns()

        # Verify patterns were loaded
        self.assertIsNotNone(filter_spec._patterns)
        self.assertEqual(3, len(filter_spec._patterns))

    def test_load_patterns_missing_blob(self):
        """Test error when sparse blob is not found."""
        from dulwich.object_store import MemoryObjectStore

        oid = b"1234567890abcdef1234567890abcdef12345678"
        object_store = MemoryObjectStore()

        filter_spec = SparseOidFilter(oid, object_store=object_store)

        with self.assertRaises(ValueError) as cm:
            filter_spec._load_patterns()
        self.assertIn("not found", str(cm.exception))

    def test_load_patterns_not_a_blob(self):
        """Test error when sparse OID points to non-blob object."""
        from dulwich.object_store import MemoryObjectStore
        from dulwich.objects import Tree

        tree = Tree()
        object_store = MemoryObjectStore()
        object_store.add_object(tree)

        filter_spec = SparseOidFilter(tree.id, object_store=object_store)

        with self.assertRaises(ValueError) as cm:
            filter_spec._load_patterns()
        self.assertIn("not a blob", str(cm.exception))

    def test_load_patterns_without_object_store(self):
        """Test error when trying to load patterns without object store."""
        oid = b"1234567890abcdef1234567890abcdef12345678"
        filter_spec = SparseOidFilter(oid)

        with self.assertRaises(ValueError) as cm:
            filter_spec._load_patterns()
        self.assertIn("without an object store", str(cm.exception))

    def test_should_include_path_matching(self):
        """Test path matching with sparse patterns."""
        from dulwich.object_store import MemoryObjectStore
        from dulwich.objects import Blob

        # Create a sparse patterns blob: include *.txt files
        patterns = b"*.txt\n"
        blob = Blob.from_string(patterns)

        object_store = MemoryObjectStore()
        object_store.add_object(blob)

        filter_spec = SparseOidFilter(blob.id, object_store=object_store)

        # .txt files should be included
        self.assertTrue(filter_spec.should_include_path("readme.txt"))
        self.assertTrue(filter_spec.should_include_path("docs/file.txt"))

        # Other files should not be included
        self.assertFalse(filter_spec.should_include_path("readme.md"))
        self.assertFalse(filter_spec.should_include_path("script.py"))

    def test_should_include_path_negation(self):
        """Test path matching with negation patterns."""
        from dulwich.object_store import MemoryObjectStore
        from dulwich.objects import Blob

        # Include all .txt files except logs
        patterns = b"*.txt\n!*.log\n"
        blob = Blob.from_string(patterns)

        object_store = MemoryObjectStore()
        object_store.add_object(blob)

        filter_spec = SparseOidFilter(blob.id, object_store=object_store)

        # .txt files should be included
        self.assertTrue(filter_spec.should_include_path("readme.txt"))

        # But .log files should be excluded (even though they end in .txt pattern)
        # Note: This depends on pattern order and sparse_patterns implementation
        self.assertFalse(filter_spec.should_include_path("debug.log"))


class CombineFilterTests(TestCase):
    """Test CombineFilter class."""

    def test_should_include_blob_all_allow(self):
        """Test that blob is included when all filters allow it."""
        filters = [BlobLimitFilter(1024), BlobLimitFilter(2048)]
        filter_spec = CombineFilter(filters)
        self.assertTrue(filter_spec.should_include_blob(512))

    def test_should_include_blob_one_denies(self):
        """Test that blob is excluded when one filter denies it."""
        filters = [BlobLimitFilter(1024), BlobNoneFilter()]
        filter_spec = CombineFilter(filters)
        self.assertFalse(filter_spec.should_include_blob(512))

    def test_should_include_tree_all_allow(self):
        """Test that tree is included when all filters allow it."""
        filters = [TreeDepthFilter(2), TreeDepthFilter(3)]
        filter_spec = CombineFilter(filters)
        self.assertTrue(filter_spec.should_include_tree(1))

    def test_should_include_tree_one_denies(self):
        """Test that tree is excluded when one filter denies it."""
        filters = [TreeDepthFilter(2), TreeDepthFilter(1)]
        filter_spec = CombineFilter(filters)
        self.assertFalse(filter_spec.should_include_tree(2))

    def test_to_spec_string(self):
        """Test conversion back to spec string."""
        filters = [BlobNoneFilter(), TreeDepthFilter(0)]
        filter_spec = CombineFilter(filters)
        self.assertEqual("combine:blob:none+tree:0", filter_spec.to_spec_string())

    def test_repr(self):
        """Test repr output."""
        filters = [BlobNoneFilter()]
        filter_spec = CombineFilter(filters)
        self.assertIn("CombineFilter", repr(filter_spec))


class FilterPackObjectsTests(TestCase):
    """Test filter_pack_objects function."""

    def setUp(self):
        super().setUp()
        self.store = MemoryObjectStore()

        # Create test objects
        self.small_blob = Blob.from_string(b"small")
        self.large_blob = Blob.from_string(b"x" * 2000)
        self.tree = Tree()
        self.commit = make_commit(tree=self.tree.id)

        # Add objects to store
        self.store.add_object(self.small_blob)
        self.store.add_object(self.large_blob)
        self.store.add_object(self.tree)
        self.store.add_object(self.commit)

    def test_filter_blob_none(self):
        """Test that blob:none filter excludes all blobs."""
        object_ids = [
            self.small_blob.id,
            self.large_blob.id,
            self.tree.id,
            self.commit.id,
        ]

        filter_spec = BlobNoneFilter()
        filtered = filter_pack_objects(self.store, object_ids, filter_spec)

        # Should exclude both blobs but keep tree and commit
        self.assertNotIn(self.small_blob.id, filtered)
        self.assertNotIn(self.large_blob.id, filtered)
        self.assertIn(self.tree.id, filtered)
        self.assertIn(self.commit.id, filtered)

    def test_filter_blob_limit(self):
        """Test that blob:limit filter excludes blobs over size limit."""
        object_ids = [
            self.small_blob.id,
            self.large_blob.id,
            self.tree.id,
        ]

        # Set limit to 100 bytes
        filter_spec = BlobLimitFilter(100)
        filtered = filter_pack_objects(self.store, object_ids, filter_spec)

        # Should keep small blob but exclude large blob
        self.assertIn(self.small_blob.id, filtered)
        self.assertNotIn(self.large_blob.id, filtered)
        self.assertIn(self.tree.id, filtered)

    def test_filter_no_filter_keeps_all(self):
        """Test that without filtering all objects are kept."""
        # Create a filter that includes everything
        filter_spec = BlobLimitFilter(10000)  # Large limit

        object_ids = [
            self.small_blob.id,
            self.large_blob.id,
            self.tree.id,
            self.commit.id,
        ]

        filtered = filter_pack_objects(self.store, object_ids, filter_spec)

        # All objects should be included
        self.assertEqual(len(filtered), len(object_ids))
        for oid in object_ids:
            self.assertIn(oid, filtered)

    def test_filter_missing_object(self):
        """Test that missing objects are skipped without error."""
        from dulwich.objects import ObjectID

        fake_id = ObjectID(b"0" * 40)
        object_ids = [fake_id, self.small_blob.id]

        filter_spec = BlobNoneFilter()
        filtered = filter_pack_objects(self.store, object_ids, filter_spec)

        # Should skip the missing object
        self.assertNotIn(fake_id, filtered)

    def test_filter_combine(self):
        """Test combined filters."""
        object_ids = [
            self.small_blob.id,
            self.large_blob.id,
            self.tree.id,
        ]

        # Combine blob:limit with another filter
        filter_spec = CombineFilter(
            [
                BlobLimitFilter(100),
                BlobNoneFilter(),  # This will exclude ALL blobs
            ]
        )

        filtered = filter_pack_objects(self.store, object_ids, filter_spec)

        # Should exclude all blobs due to BlobNoneFilter
        self.assertNotIn(self.small_blob.id, filtered)
        self.assertNotIn(self.large_blob.id, filtered)
        self.assertIn(self.tree.id, filtered)


class PartialCloneIntegrationTests(TestCase):
    """Integration tests for partial clone with real repositories."""

    def setUp(self):
        super().setUp()
        self.repo_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup)
        self.repo = Repo.init(self.repo_dir)

    def _cleanup(self):
        """Clean up test repository."""
        import shutil

        if os.path.exists(self.repo_dir):
            shutil.rmtree(self.repo_dir)

    def test_blob_none_filter_with_real_repo(self):
        """Test blob:none filter excludes blobs in real repository."""
        # Create a tree with files
        tree = Tree()

        # Add some blobs to the tree
        blob1 = Blob.from_string(b"file1 content")
        blob2 = Blob.from_string(b"file2 content")
        tree.add(b"file1.txt", 0o100644, blob1.id)
        tree.add(b"file2.txt", 0o100644, blob2.id)

        # Add objects to repo
        self.repo.object_store.add_object(blob1)
        self.repo.object_store.add_object(blob2)
        self.repo.object_store.add_object(tree)

        # Create commit
        commit = make_commit(tree=tree.id, message=b"Test commit")
        self.repo.object_store.add_object(commit)

        # Get all objects
        object_ids = [blob1.id, blob2.id, tree.id, commit.id]

        # Apply blob:none filter
        filter_spec = BlobNoneFilter()
        filtered = filter_pack_objects(self.repo.object_store, object_ids, filter_spec)

        # Verify blobs are excluded
        self.assertNotIn(blob1.id, filtered)
        self.assertNotIn(blob2.id, filtered)
        # But tree and commit are included
        self.assertIn(tree.id, filtered)
        self.assertIn(commit.id, filtered)

        # Verify we have only 2 objects (tree + commit)
        self.assertEqual(2, len(filtered))

    def test_blob_limit_filter_with_mixed_sizes(self):
        """Test blob:limit filter with mixed blob sizes."""
        tree = Tree()

        # Create blobs of different sizes
        small_blob = Blob.from_string(b"small")  # 5 bytes
        medium_blob = Blob.from_string(b"x" * 50)  # 50 bytes
        large_blob = Blob.from_string(b"y" * 500)  # 500 bytes

        tree.add(b"small.txt", 0o100644, small_blob.id)
        tree.add(b"medium.txt", 0o100644, medium_blob.id)
        tree.add(b"large.txt", 0o100644, large_blob.id)

        # Add to repo
        self.repo.object_store.add_object(small_blob)
        self.repo.object_store.add_object(medium_blob)
        self.repo.object_store.add_object(large_blob)
        self.repo.object_store.add_object(tree)

        commit = make_commit(tree=tree.id)
        self.repo.object_store.add_object(commit)

        # Test with 100 byte limit
        object_ids = [
            small_blob.id,
            medium_blob.id,
            large_blob.id,
            tree.id,
            commit.id,
        ]

        filter_spec = BlobLimitFilter(100)
        filtered = filter_pack_objects(self.repo.object_store, object_ids, filter_spec)

        # Small and medium should be included
        self.assertIn(small_blob.id, filtered)
        self.assertIn(medium_blob.id, filtered)
        # Large should be excluded
        self.assertNotIn(large_blob.id, filtered)
        # Tree and commit included
        self.assertIn(tree.id, filtered)
        self.assertIn(commit.id, filtered)

    def test_combined_filter_integration(self):
        """Test combined filters in real scenario."""
        tree = Tree()

        blob1 = Blob.from_string(b"content1")
        blob2 = Blob.from_string(b"x" * 1000)

        tree.add(b"file1.txt", 0o100644, blob1.id)
        tree.add(b"file2.txt", 0o100644, blob2.id)

        self.repo.object_store.add_object(blob1)
        self.repo.object_store.add_object(blob2)
        self.repo.object_store.add_object(tree)

        commit = make_commit(tree=tree.id)
        self.repo.object_store.add_object(commit)

        # Combine: limit to 500 bytes, but also apply blob:none
        # This should exclude ALL blobs (blob:none overrides limit)
        filter_spec = CombineFilter(
            [
                BlobLimitFilter(500),
                BlobNoneFilter(),
            ]
        )

        object_ids = [blob1.id, blob2.id, tree.id, commit.id]
        filtered = filter_pack_objects(self.repo.object_store, object_ids, filter_spec)

        # All blobs excluded
        self.assertNotIn(blob1.id, filtered)
        self.assertNotIn(blob2.id, filtered)
        # Only tree and commit
        self.assertEqual(2, len(filtered))


class FilterPackObjectsWithPathsTests(TestCase):
    """Test filter_pack_objects_with_paths function."""

    def setUp(self):
        super().setUp()
        self.object_store = MemoryObjectStore()

    def test_tree_depth_filtering(self):
        """Test filtering by tree depth."""
        from dulwich.object_filters import (
            TreeDepthFilter,
            filter_pack_objects_with_paths,
        )
        from dulwich.objects import Blob, Tree
        from dulwich.tests.utils import make_commit

        # Create a nested tree structure:
        # root/
        #   file1.txt (blob1)
        #   dir1/
        #     file2.txt (blob2)
        #     dir2/
        #       file3.txt (blob3)

        blob1 = Blob.from_string(b"file1 content")
        blob2 = Blob.from_string(b"file2 content")
        blob3 = Blob.from_string(b"file3 content")

        # deepest tree (dir2)
        tree_dir2 = Tree()
        tree_dir2.add(b"file3.txt", 0o100644, blob3.id)

        # middle tree (dir1)
        tree_dir1 = Tree()
        tree_dir1.add(b"file2.txt", 0o100644, blob2.id)
        tree_dir1.add(b"dir2", 0o040000, tree_dir2.id)

        # root tree
        tree_root = Tree()
        tree_root.add(b"file1.txt", 0o100644, blob1.id)
        tree_root.add(b"dir1", 0o040000, tree_dir1.id)

        # Add all objects to store
        for obj in [blob1, blob2, blob3, tree_dir2, tree_dir1, tree_root]:
            self.object_store.add_object(obj)

        commit = make_commit(tree=tree_root.id)
        self.object_store.add_object(commit)

        # Filter with depth=1 (root + 1 level deep)
        filter_spec = TreeDepthFilter(1)
        filtered = filter_pack_objects_with_paths(
            self.object_store, [commit.id], filter_spec
        )

        # Should include: commit, tree_root (depth 0), tree_dir1 (depth 1),
        # blob1 (in root), blob2 (in dir1)
        # Should exclude: tree_dir2 (depth 2), blob3 (in dir2)
        self.assertIn(commit.id, filtered)
        self.assertIn(tree_root.id, filtered)
        self.assertIn(tree_dir1.id, filtered)
        self.assertIn(blob1.id, filtered)
        self.assertIn(blob2.id, filtered)
        self.assertNotIn(tree_dir2.id, filtered)
        self.assertNotIn(blob3.id, filtered)

    def test_sparse_oid_path_filtering(self):
        """Test filtering by sparse checkout patterns."""
        from dulwich.object_filters import (
            SparseOidFilter,
            filter_pack_objects_with_paths,
        )
        from dulwich.objects import Blob, Tree
        from dulwich.tests.utils import make_commit

        # Create sparse patterns blob that includes only *.txt files
        patterns = b"*.txt\n"
        patterns_blob = Blob.from_string(patterns)
        self.object_store.add_object(patterns_blob)

        # Create a tree with mixed file types:
        # root/
        #   readme.txt (should be included)
        #   script.py (should be excluded)
        #   docs/
        #     guide.txt (should be included)
        #     image.png (should be excluded)

        blob_readme = Blob.from_string(b"readme content")
        blob_script = Blob.from_string(b"script content")
        blob_guide = Blob.from_string(b"guide content")
        blob_image = Blob.from_string(b"image content")

        tree_docs = Tree()
        tree_docs.add(b"guide.txt", 0o100644, blob_guide.id)
        tree_docs.add(b"image.png", 0o100644, blob_image.id)

        tree_root = Tree()
        tree_root.add(b"readme.txt", 0o100644, blob_readme.id)
        tree_root.add(b"script.py", 0o100644, blob_script.id)
        tree_root.add(b"docs", 0o040000, tree_docs.id)

        # Add all objects
        for obj in [
            blob_readme,
            blob_script,
            blob_guide,
            blob_image,
            tree_docs,
            tree_root,
        ]:
            self.object_store.add_object(obj)

        commit = make_commit(tree=tree_root.id)
        self.object_store.add_object(commit)

        # Create sparse filter
        filter_spec = SparseOidFilter(patterns_blob.id, object_store=self.object_store)
        filtered = filter_pack_objects_with_paths(
            self.object_store, [commit.id], filter_spec
        )

        # Should include: commit, trees, and .txt blobs
        self.assertIn(commit.id, filtered)
        self.assertIn(tree_root.id, filtered)
        self.assertIn(tree_docs.id, filtered)
        self.assertIn(blob_readme.id, filtered)
        self.assertIn(blob_guide.id, filtered)

        # Should exclude: non-.txt blobs
        self.assertNotIn(blob_script.id, filtered)
        self.assertNotIn(blob_image.id, filtered)

    def test_blob_size_filtering_with_paths(self):
        """Test that blob size filtering still works with path tracking."""
        from dulwich.object_filters import (
            BlobLimitFilter,
            filter_pack_objects_with_paths,
        )
        from dulwich.objects import Blob, Tree
        from dulwich.tests.utils import make_commit

        # Create blobs of different sizes
        blob_small = Blob.from_string(b"small")  # 5 bytes
        blob_large = Blob.from_string(b"x" * 1000)  # 1000 bytes

        tree = Tree()
        tree.add(b"small.txt", 0o100644, blob_small.id)
        tree.add(b"large.txt", 0o100644, blob_large.id)

        for obj in [blob_small, blob_large, tree]:
            self.object_store.add_object(obj)

        commit = make_commit(tree=tree.id)
        self.object_store.add_object(commit)

        # Filter with 100 byte limit
        filter_spec = BlobLimitFilter(100)
        filtered = filter_pack_objects_with_paths(
            self.object_store, [commit.id], filter_spec
        )

        # Should include small blob but not large
        self.assertIn(commit.id, filtered)
        self.assertIn(tree.id, filtered)
        self.assertIn(blob_small.id, filtered)
        self.assertNotIn(blob_large.id, filtered)

    def test_combined_sparse_and_size_filter(self):
        """Test combining sparse patterns with blob size limits."""
        from dulwich.object_filters import (
            BlobLimitFilter,
            CombineFilter,
            SparseOidFilter,
            filter_pack_objects_with_paths,
        )
        from dulwich.objects import Blob, Tree
        from dulwich.tests.utils import make_commit

        # Create sparse patterns: only *.txt files
        patterns = b"*.txt\n"
        patterns_blob = Blob.from_string(patterns)
        self.object_store.add_object(patterns_blob)

        # Create files:
        # - small.txt (5 bytes, .txt) -> should be included
        # - large.txt (1000 bytes, .txt) -> excluded by size
        # - small.py (5 bytes, .py) -> excluded by pattern
        # - large.py (1000 bytes, .py) -> excluded by both

        blob_small_txt = Blob.from_string(b"small txt")
        blob_large_txt = Blob.from_string(b"x" * 1000)
        blob_small_py = Blob.from_string(b"small py")
        blob_large_py = Blob.from_string(b"y" * 1000)

        tree = Tree()
        tree.add(b"small.txt", 0o100644, blob_small_txt.id)
        tree.add(b"large.txt", 0o100644, blob_large_txt.id)
        tree.add(b"small.py", 0o100644, blob_small_py.id)
        tree.add(b"large.py", 0o100644, blob_large_py.id)

        for obj in [blob_small_txt, blob_large_txt, blob_small_py, blob_large_py, tree]:
            self.object_store.add_object(obj)

        commit = make_commit(tree=tree.id)
        self.object_store.add_object(commit)

        # Combine: sparse filter + 100 byte limit
        filter_spec = CombineFilter(
            [
                SparseOidFilter(patterns_blob.id, object_store=self.object_store),
                BlobLimitFilter(100),
            ]
        )

        filtered = filter_pack_objects_with_paths(
            self.object_store, [commit.id], filter_spec
        )

        # Only small.txt should be included (matches pattern AND size limit)
        self.assertIn(commit.id, filtered)
        self.assertIn(tree.id, filtered)
        self.assertIn(blob_small_txt.id, filtered)
        self.assertNotIn(blob_large_txt.id, filtered)  # Too large
        self.assertNotIn(blob_small_py.id, filtered)  # Wrong pattern
        self.assertNotIn(blob_large_py.id, filtered)  # Both wrong

    def test_blob_none_filter_with_paths(self):
        """Test that blob:none excludes all blobs with path tracking."""
        from dulwich.object_filters import (
            BlobNoneFilter,
            filter_pack_objects_with_paths,
        )
        from dulwich.objects import Blob, Tree
        from dulwich.tests.utils import make_commit

        blob1 = Blob.from_string(b"content1")
        blob2 = Blob.from_string(b"content2")

        tree = Tree()
        tree.add(b"file1.txt", 0o100644, blob1.id)
        tree.add(b"file2.txt", 0o100644, blob2.id)

        for obj in [blob1, blob2, tree]:
            self.object_store.add_object(obj)

        commit = make_commit(tree=tree.id)
        self.object_store.add_object(commit)

        filter_spec = BlobNoneFilter()
        filtered = filter_pack_objects_with_paths(
            self.object_store, [commit.id], filter_spec
        )

        # Should include commit and tree but no blobs
        self.assertIn(commit.id, filtered)
        self.assertIn(tree.id, filtered)
        self.assertNotIn(blob1.id, filtered)
        self.assertNotIn(blob2.id, filtered)

    def test_direct_tree_want(self):
        """Test filtering when a tree (not commit) is wanted."""
        from dulwich.object_filters import (
            BlobLimitFilter,
            filter_pack_objects_with_paths,
        )
        from dulwich.objects import Blob, Tree

        blob_small = Blob.from_string(b"small")
        blob_large = Blob.from_string(b"x" * 1000)

        tree = Tree()
        tree.add(b"small.txt", 0o100644, blob_small.id)
        tree.add(b"large.txt", 0o100644, blob_large.id)

        for obj in [blob_small, blob_large, tree]:
            self.object_store.add_object(obj)

        # Want the tree directly (not via commit)
        filter_spec = BlobLimitFilter(100)
        filtered = filter_pack_objects_with_paths(
            self.object_store, [tree.id], filter_spec
        )

        # Should include tree and small blob
        self.assertIn(tree.id, filtered)
        self.assertIn(blob_small.id, filtered)
        self.assertNotIn(blob_large.id, filtered)
