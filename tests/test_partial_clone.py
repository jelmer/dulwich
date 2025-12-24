# test_partial_clone.py -- Tests for partial clone filter specifications
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

"""Tests for partial clone filter specifications."""

from dulwich.partial_clone import (
    BlobLimitFilter,
    BlobNoneFilter,
    CombineFilter,
    SparseOidFilter,
    TreeDepthFilter,
    parse_filter_spec,
)

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
        self.assertIn("Invalid tree depth", str(cm.exception))

    def test_parse_invalid_blob_limit(self):
        """Test that invalid blob limit raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            parse_filter_spec("blob:limit=invalid")
        self.assertIn("Invalid size specification", str(cm.exception))


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
