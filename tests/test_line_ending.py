# test_line_ending.py -- Tests for the line ending functions
# Copyright (C) 2018-2019 Boris Feld <boris.feld@comet.ml>
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

"""Tests for the line ending conversion."""

from dulwich.line_ending import (
    BlobNormalizer,
    LineEndingFilter,
    convert_crlf_to_lf,
    convert_lf_to_crlf,
    get_clean_filter_autocrlf,
    get_smudge_filter_autocrlf,
    normalize_blob,
)
from dulwich.objects import Blob

from . import TestCase


class LineEndingConversion(TestCase):
    """Test the line ending conversion functions in various cases."""

    def test_convert_crlf_to_lf_no_op(self) -> None:
        self.assertEqual(convert_crlf_to_lf(b"foobar"), b"foobar")

    def test_convert_crlf_to_lf(self) -> None:
        self.assertEqual(convert_crlf_to_lf(b"line1\r\nline2"), b"line1\nline2")

    def test_convert_crlf_to_lf_mixed(self) -> None:
        self.assertEqual(convert_crlf_to_lf(b"line1\r\n\nline2"), b"line1\n\nline2")

    def test_convert_lf_to_crlf_no_op(self) -> None:
        self.assertEqual(convert_lf_to_crlf(b"foobar"), b"foobar")

    def test_convert_lf_to_crlf(self) -> None:
        self.assertEqual(convert_lf_to_crlf(b"line1\nline2"), b"line1\r\nline2")

    def test_convert_lf_to_crlf_mixed(self) -> None:
        self.assertEqual(convert_lf_to_crlf(b"line1\r\n\nline2"), b"line1\r\n\r\nline2")


class GetLineEndingAutocrlfFilters(TestCase):
    def test_get_clean_filter_autocrlf_default(self) -> None:
        clean_filter = get_clean_filter_autocrlf(b"false")

        self.assertEqual(clean_filter, None)

    def test_get_clean_filter_autocrlf_true(self) -> None:
        clean_filter = get_clean_filter_autocrlf(b"true")

        self.assertEqual(clean_filter, convert_crlf_to_lf)

    def test_get_clean_filter_autocrlf_input(self) -> None:
        clean_filter = get_clean_filter_autocrlf(b"input")

        self.assertEqual(clean_filter, convert_crlf_to_lf)

    def test_get_smudge_filter_autocrlf_default(self) -> None:
        smudge_filter = get_smudge_filter_autocrlf(b"false")

        self.assertEqual(smudge_filter, None)

    def test_get_smudge_filter_autocrlf_true(self) -> None:
        smudge_filter = get_smudge_filter_autocrlf(b"true")

        self.assertEqual(smudge_filter, convert_lf_to_crlf)

    def test_get_smudge_filter_autocrlf_input(self) -> None:
        smudge_filter = get_smudge_filter_autocrlf(b"input")

        self.assertEqual(smudge_filter, None)


class NormalizeBlobTestCase(TestCase):
    def test_normalize_to_lf_no_op(self) -> None:
        base_content = b"line1\nline2"
        base_sha = "f8be7bb828880727816015d21abcbc37d033f233"

        base_blob = Blob()
        base_blob.set_raw_string(base_content)

        self.assertEqual(base_blob.as_raw_chunks(), [base_content])
        self.assertEqual(base_blob.sha().hexdigest(), base_sha)

        filtered_blob = normalize_blob(
            base_blob, convert_crlf_to_lf, binary_detection=False
        )

        self.assertEqual(filtered_blob.as_raw_chunks(), [base_content])
        self.assertEqual(filtered_blob.sha().hexdigest(), base_sha)

    def test_normalize_to_lf(self) -> None:
        base_content = b"line1\r\nline2"
        base_sha = "3a1bd7a52799fe5cf6411f1d35f4c10bacb1db96"

        base_blob = Blob()
        base_blob.set_raw_string(base_content)

        self.assertEqual(base_blob.as_raw_chunks(), [base_content])
        self.assertEqual(base_blob.sha().hexdigest(), base_sha)

        filtered_blob = normalize_blob(
            base_blob, convert_crlf_to_lf, binary_detection=False
        )

        normalized_content = b"line1\nline2"
        normalized_sha = "f8be7bb828880727816015d21abcbc37d033f233"

        self.assertEqual(filtered_blob.as_raw_chunks(), [normalized_content])
        self.assertEqual(filtered_blob.sha().hexdigest(), normalized_sha)

    def test_normalize_to_lf_binary(self) -> None:
        base_content = b"line1\r\nline2\0"
        base_sha = "b44504193b765f7cd79673812de8afb55b372ab2"

        base_blob = Blob()
        base_blob.set_raw_string(base_content)

        self.assertEqual(base_blob.as_raw_chunks(), [base_content])
        self.assertEqual(base_blob.sha().hexdigest(), base_sha)

        filtered_blob = normalize_blob(
            base_blob, convert_crlf_to_lf, binary_detection=True
        )

        self.assertEqual(filtered_blob.as_raw_chunks(), [base_content])
        self.assertEqual(filtered_blob.sha().hexdigest(), base_sha)

    def test_normalize_to_crlf_no_op(self) -> None:
        base_content = b"line1\r\nline2"
        base_sha = "3a1bd7a52799fe5cf6411f1d35f4c10bacb1db96"

        base_blob = Blob()
        base_blob.set_raw_string(base_content)

        self.assertEqual(base_blob.as_raw_chunks(), [base_content])
        self.assertEqual(base_blob.sha().hexdigest(), base_sha)

        filtered_blob = normalize_blob(
            base_blob, convert_lf_to_crlf, binary_detection=False
        )

        self.assertEqual(filtered_blob.as_raw_chunks(), [base_content])
        self.assertEqual(filtered_blob.sha().hexdigest(), base_sha)

    def test_normalize_to_crlf(self) -> None:
        base_content = b"line1\nline2"
        base_sha = "f8be7bb828880727816015d21abcbc37d033f233"

        base_blob = Blob()
        base_blob.set_raw_string(base_content)

        self.assertEqual(base_blob.as_raw_chunks(), [base_content])
        self.assertEqual(base_blob.sha().hexdigest(), base_sha)

        filtered_blob = normalize_blob(
            base_blob, convert_lf_to_crlf, binary_detection=False
        )

        normalized_content = b"line1\r\nline2"
        normalized_sha = "3a1bd7a52799fe5cf6411f1d35f4c10bacb1db96"

        self.assertEqual(filtered_blob.as_raw_chunks(), [normalized_content])
        self.assertEqual(filtered_blob.sha().hexdigest(), normalized_sha)

    def test_normalize_to_crlf_binary(self) -> None:
        base_content = b"line1\r\nline2\0"
        base_sha = "b44504193b765f7cd79673812de8afb55b372ab2"

        base_blob = Blob()
        base_blob.set_raw_string(base_content)

        self.assertEqual(base_blob.as_raw_chunks(), [base_content])
        self.assertEqual(base_blob.sha().hexdigest(), base_sha)

        filtered_blob = normalize_blob(
            base_blob, convert_lf_to_crlf, binary_detection=True
        )

        self.assertEqual(filtered_blob.as_raw_chunks(), [base_content])
        self.assertEqual(filtered_blob.sha().hexdigest(), base_sha)


class LineEndingFilterTests(TestCase):
    """Test the LineEndingFilter class."""

    def test_clean_no_conversion(self) -> None:
        """Test clean with no conversion function."""
        filter = LineEndingFilter()
        data = b"test\r\ndata"
        self.assertEqual(filter.clean(data), data)

    def test_clean_with_conversion(self) -> None:
        """Test clean with CRLF to LF conversion."""
        filter = LineEndingFilter(clean_conversion=convert_crlf_to_lf)
        data = b"test\r\ndata"
        self.assertEqual(filter.clean(data), b"test\ndata")

    def test_clean_binary_detection(self) -> None:
        """Test clean skips binary files."""
        filter = LineEndingFilter(
            clean_conversion=convert_crlf_to_lf, binary_detection=True
        )
        # Binary data with null byte
        data = b"test\r\n\x00data"
        self.assertEqual(filter.clean(data), data)  # Should not convert

    def test_smudge_no_conversion(self) -> None:
        """Test smudge with no conversion function."""
        filter = LineEndingFilter()
        data = b"test\ndata"
        self.assertEqual(filter.smudge(data), data)

    def test_smudge_with_conversion(self) -> None:
        """Test smudge with LF to CRLF conversion."""
        filter = LineEndingFilter(smudge_conversion=convert_lf_to_crlf)
        data = b"test\ndata"
        self.assertEqual(filter.smudge(data), b"test\r\ndata")

    def test_smudge_binary_detection(self) -> None:
        """Test smudge skips binary files."""
        filter = LineEndingFilter(
            smudge_conversion=convert_lf_to_crlf, binary_detection=True
        )
        # Binary data with null byte
        data = b"test\n\x00data"
        self.assertEqual(filter.smudge(data), data)  # Should not convert


class BlobNormalizerTests(TestCase):
    """Test the BlobNormalizer class integration with filters."""

    def setUp(self) -> None:
        super().setUp()
        from dulwich.config import ConfigDict

        self.config = ConfigDict()
        self.gitattributes = {}

    def test_autocrlf_true_checkin(self) -> None:
        """Test checkin with autocrlf=true."""
        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"true")

        # Create blob with CRLF
        blob = Blob()
        blob.data = b"line1\r\nline2\r\n"

        # Should convert to LF on checkin
        result = normalizer.checkin_normalize(blob, b"test.txt")
        self.assertEqual(result.data, b"line1\nline2\n")

    def test_autocrlf_true_checkout(self) -> None:
        """Test checkout with autocrlf=true."""
        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"true")

        # Create blob with LF
        blob = Blob()
        blob.data = b"line1\nline2\n"

        # Should convert to CRLF on checkout
        result = normalizer.checkout_normalize(blob, b"test.txt")
        self.assertEqual(result.data, b"line1\r\nline2\r\n")

    def test_autocrlf_input_checkin(self) -> None:
        """Test checkin with autocrlf=input."""
        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"input")

        # Create blob with CRLF
        blob = Blob()
        blob.data = b"line1\r\nline2\r\n"

        # Should convert to LF on checkin
        result = normalizer.checkin_normalize(blob, b"test.txt")
        self.assertEqual(result.data, b"line1\nline2\n")

    def test_autocrlf_input_checkout(self) -> None:
        """Test checkout with autocrlf=input."""
        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"input")

        # Create blob with LF
        blob = Blob()
        blob.data = b"line1\nline2\n"

        # Should NOT convert on checkout with input mode
        result = normalizer.checkout_normalize(blob, b"test.txt")
        self.assertIs(result, blob)  # Same object, no conversion

    def test_autocrlf_false(self) -> None:
        """Test with autocrlf=false (no conversion)."""
        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"false")

        # Create blob with mixed line endings
        blob = Blob()
        blob.data = b"line1\r\nline2\nline3"

        # Should not convert on either operation
        result = normalizer.checkin_normalize(blob, b"test.txt")
        self.assertIs(result, blob)

        result = normalizer.checkout_normalize(blob, b"test.txt")
        self.assertIs(result, blob)

    def test_gitattributes_text_attr(self) -> None:
        """Test gitattributes text attribute overrides autocrlf."""
        # Set gitattributes to force text conversion
        self.gitattributes[b"*.txt"] = {b"text": True}

        # Even with autocrlf=false, should convert based on gitattributes
        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"false")

        blob = Blob()
        blob.data = b"line1\r\nline2\r\n"

        # Should still convert because of gitattributes
        result = normalizer.checkin_normalize(blob, b"test.txt")
        # Note: with just text=true and no eol setting, it follows platform defaults
        # For checkin, it should always normalize to LF
        self.assertIsNot(result, blob)

    def test_gitattributes_binary_attr(self) -> None:
        """Test gitattributes -text attribute prevents conversion."""
        # Set gitattributes to force binary (no conversion)
        self.gitattributes[b"*.bin"] = {b"text": False}

        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"true")

        blob = Blob()
        blob.data = b"line1\r\nline2\r\n"

        # Should not convert despite autocrlf=true
        result = normalizer.checkin_normalize(blob, b"test.bin")
        self.assertIs(result, blob)

    def test_binary_file_detection(self) -> None:
        """Test that binary files are not converted."""
        normalizer = BlobNormalizer(self.config, self.gitattributes, autocrlf=b"true")

        # Create blob with binary content
        blob = Blob()
        blob.data = b"line1\r\n\x00\xffbinary\r\ndata"

        # Should not convert binary files
        result = normalizer.checkin_normalize(blob, b"binary.dat")
        self.assertIs(result, blob)

        result = normalizer.checkout_normalize(blob, b"binary.dat")
        self.assertIs(result, blob)


class LineEndingIntegrationTests(TestCase):
    """Integration tests for line ending conversion with the filter system."""

    def setUp(self) -> None:
        super().setUp()
        from dulwich.config import ConfigDict
        from dulwich.filters import FilterRegistry

        self.config = ConfigDict()
        self.registry = FilterRegistry(self.config)

    def test_filter_registry_with_line_endings(self) -> None:
        """Test that line ending filters work through the registry."""
        # Register a custom text filter that does line ending conversion
        filter = LineEndingFilter(
            clean_conversion=convert_crlf_to_lf,
            smudge_conversion=convert_lf_to_crlf,
            binary_detection=True,
        )
        self.registry.register_driver("text", filter)

        # Set up gitattributes
        # Create GitAttributes
        from dulwich.attrs import GitAttributes, Pattern

        patterns = [(Pattern(b"*.txt"), {b"filter": b"text"})]
        gitattributes = GitAttributes(patterns)

        # Create normalizer
        from dulwich.filters import FilterBlobNormalizer

        normalizer = FilterBlobNormalizer(self.config, gitattributes, self.registry)

        # Test round trip
        blob = Blob()
        blob.data = b"Hello\r\nWorld\r\n"

        # Checkin should convert CRLF to LF
        checked_in = normalizer.checkin_normalize(blob, b"test.txt")
        self.assertEqual(checked_in.data, b"Hello\nWorld\n")

        # Checkout should convert LF to CRLF
        checked_out = normalizer.checkout_normalize(checked_in, b"test.txt")
        self.assertEqual(checked_out.data, b"Hello\r\nWorld\r\n")

    def test_mixed_filters(self) -> None:
        """Test multiple filters can coexist (line endings and LFS)."""
        # This would be a more complex test requiring LFS setup
        # For now, just verify the structure works
        text_filter = LineEndingFilter(
            clean_conversion=convert_crlf_to_lf,
            smudge_conversion=convert_lf_to_crlf,
        )
        self.registry.register_driver("text", text_filter)

        # Mock LFS filter
        class MockLFSFilter:
            def clean(self, data):
                return b"LFS pointer"

            def smudge(self, data):
                return b"LFS content"

        self.registry.register_driver("lfs", MockLFSFilter())

        # Different files use different filters
        from dulwich.attrs import GitAttributes, Pattern

        patterns = [
            (Pattern(b"*.txt"), {b"filter": b"text"}),
            (Pattern(b"*.bin"), {b"filter": b"lfs"}),
        ]
        gitattributes = GitAttributes(patterns)

        from dulwich.filters import FilterBlobNormalizer

        normalizer = FilterBlobNormalizer(self.config, gitattributes, self.registry)

        # Text file gets line ending conversion
        text_blob = Blob()
        text_blob.data = b"text\r\nfile"
        result = normalizer.checkin_normalize(text_blob, b"test.txt")
        self.assertEqual(result.data, b"text\nfile")

        # Binary file gets LFS conversion
        bin_blob = Blob()
        bin_blob.data = b"binary content"
        result = normalizer.checkin_normalize(bin_blob, b"test.bin")
        self.assertEqual(result.data, b"LFS pointer")
