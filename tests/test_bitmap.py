# test_bitmap.py -- Tests for bitmap support
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

"""Tests for bitmap support."""

import os
import shutil
import tempfile
import unittest
from io import BytesIO

from dulwich.bitmap import (
    BITMAP_OPT_FULL_DAG,
    BITMAP_OPT_HASH_CACHE,
    BITMAP_OPT_LOOKUP_TABLE,
    BITMAP_SIGNATURE,
    BITMAP_VERSION,
    BitmapEntry,
    EWAHBitmap,
    PackBitmap,
    _encode_ewah_words,
    read_bitmap_file,
    write_bitmap_file,
)
from dulwich.object_store import BitmapReachability, GraphTraversalReachability


class EWAHCompressionTests(unittest.TestCase):
    """Tests for EWAH compression helper functions."""

    def test_encode_empty_words(self):
        """Test encoding empty word list."""
        result = _encode_ewah_words([])
        self.assertEqual([], result)

    def test_encode_single_literal(self):
        """Test encoding single literal word."""
        result = _encode_ewah_words([0x123])
        # Should be: RLW(0 run, 1 literal) + literal
        self.assertEqual(2, len(result))
        # RLW bit layout: [literal_words(31)][running_len(32)][running_bit(1)]
        # running_bit=0, running_len=0, literal_words=1
        expected_rlw = (1 << 33) | (0 << 1) | 0
        self.assertEqual(expected_rlw, result[0])
        self.assertEqual(0x123, result[1])

    def test_encode_zero_run(self):
        """Test encoding run of zeros."""
        result = _encode_ewah_words([0, 0, 0])
        # Should be: RLW(3 zeros, 0 literals)
        self.assertEqual(1, len(result))
        # RLW bit layout: [literal_words(31)][running_len(32)][running_bit(1)]
        # running_bit=0, running_len=3, literal_words=0
        expected_rlw = (0 << 33) | (3 << 1) | 0
        self.assertEqual(expected_rlw, result[0])

    def test_encode_ones_run(self):
        """Test encoding run of all-ones."""
        result = _encode_ewah_words([0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF])
        # Should be: RLW(2 ones, 0 literals)
        self.assertEqual(1, len(result))
        # RLW bit layout: [literal_words(31)][running_len(32)][running_bit(1)]
        # running_bit=1, running_len=2, literal_words=0
        expected_rlw = (0 << 33) | (2 << 1) | 1
        self.assertEqual(expected_rlw, result[0])

    def test_encode_run_followed_by_literals(self):
        """Test encoding run followed by literal words."""
        result = _encode_ewah_words([0, 0, 0x123, 0x456])
        # Should be: RLW(2 zeros, 2 literals) + literals
        self.assertEqual(3, len(result))
        # RLW bit layout: [literal_words(31)][running_len(32)][running_bit(1)]
        # running_bit=0, running_len=2, literal_words=2
        expected_rlw = (2 << 33) | (2 << 1) | 0
        self.assertEqual(expected_rlw, result[0])
        self.assertEqual(0x123, result[1])
        self.assertEqual(0x456, result[2])

    def test_encode_mixed_pattern(self):
        """Test encoding mixed runs and literals."""
        result = _encode_ewah_words(
            [0, 0, 0x123, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF]
        )
        # Should be: RLW(2 zeros, 1 literal) + literal + RLW(2 ones, 0 literals)
        self.assertEqual(3, len(result))


class EWAHCompatibilityTests(unittest.TestCase):
    """Tests for EWAH encode/decode compatibility."""

    def test_encode_decode_sparse_bitmap(self):
        """Test encoding and decoding a sparse bitmap with runs."""
        # Create a bitmap with a pattern that benefits from run-length encoding
        # Bits: 0, 1, 128 (has runs of zeros between set bits)
        bitmap = EWAHBitmap()
        bitmap.add(0)
        bitmap.add(1)
        bitmap.add(128)

        # Encode
        encoded = bitmap.encode()

        # Decode
        bitmap2 = EWAHBitmap(encoded)

        # Verify all bits are preserved
        self.assertEqual(len(bitmap), len(bitmap2))
        self.assertIn(0, bitmap2)
        self.assertIn(1, bitmap2)
        self.assertIn(128, bitmap2)
        self.assertNotIn(2, bitmap2)
        self.assertNotIn(127, bitmap2)

    def test_encode_decode_dense_bitmap(self):
        """Test encoding and decoding a dense bitmap."""
        # Create a bitmap with many consecutive bits set
        bitmap = EWAHBitmap()
        for i in range(64):
            bitmap.add(i)

        # Encode
        encoded = bitmap.encode()

        # Decode
        bitmap2 = EWAHBitmap(encoded)

        # Verify all bits are preserved
        self.assertEqual(64, len(bitmap2))
        for i in range(64):
            self.assertIn(i, bitmap2)
        self.assertNotIn(64, bitmap2)

    def test_encode_decode_runs_of_zeros(self):
        """Test encoding and decoding bitmap with long runs of zeros."""
        # Create bitmap: bits 0, 200, 400 (lots of zeros in between)
        bitmap = EWAHBitmap()
        bitmap.add(0)
        bitmap.add(200)
        bitmap.add(400)

        # Encode
        encoded = bitmap.encode()

        # Decode
        bitmap2 = EWAHBitmap(encoded)

        # Verify
        self.assertEqual(3, len(bitmap2))
        self.assertIn(0, bitmap2)
        self.assertIn(200, bitmap2)
        self.assertIn(400, bitmap2)
        self.assertNotIn(1, bitmap2)
        self.assertNotIn(199, bitmap2)
        self.assertNotIn(201, bitmap2)

    def test_encode_decode_mixed_pattern(self):
        """Test encoding and decoding bitmap with mixed dense/sparse regions."""
        bitmap = EWAHBitmap()
        # Dense region: 0-63
        for i in range(64):
            bitmap.add(i)
        # Sparse region: 200, 300, 400
        bitmap.add(200)
        bitmap.add(300)
        bitmap.add(400)
        # Another dense region: 500-563
        for i in range(500, 564):
            bitmap.add(i)

        # Encode
        encoded = bitmap.encode()

        # Decode
        bitmap2 = EWAHBitmap(encoded)

        # Verify all bits preserved
        self.assertEqual(len(bitmap), len(bitmap2))
        for i in range(64):
            self.assertIn(i, bitmap2)
        self.assertIn(200, bitmap2)
        self.assertIn(300, bitmap2)
        self.assertIn(400, bitmap2)
        for i in range(500, 564):
            self.assertIn(i, bitmap2)
        # Check some bits that shouldn't be set
        self.assertNotIn(64, bitmap2)
        self.assertNotIn(199, bitmap2)
        self.assertNotIn(201, bitmap2)
        self.assertNotIn(499, bitmap2)

    def test_encode_decode_preserves_bitwise_ops(self):
        """Test that encode/decode doesn't break bitwise operations."""
        # Create two bitmaps
        bitmap1 = EWAHBitmap()
        bitmap1.add(0)
        bitmap1.add(5)
        bitmap1.add(100)

        bitmap2 = EWAHBitmap()
        bitmap2.add(5)
        bitmap2.add(10)
        bitmap2.add(100)

        # Encode and decode both
        bitmap1_decoded = EWAHBitmap(bitmap1.encode())
        bitmap2_decoded = EWAHBitmap(bitmap2.encode())

        # Perform operations on decoded bitmaps
        or_result = bitmap1_decoded | bitmap2_decoded
        and_result = bitmap1_decoded & bitmap2_decoded
        xor_result = bitmap1_decoded ^ bitmap2_decoded

        # Verify results
        # OR: {0, 5, 10, 100}
        self.assertEqual(4, len(or_result))
        self.assertIn(0, or_result)
        self.assertIn(5, or_result)
        self.assertIn(10, or_result)
        self.assertIn(100, or_result)

        # AND: {5, 100}
        self.assertEqual(2, len(and_result))
        self.assertIn(5, and_result)
        self.assertIn(100, and_result)

        # XOR: {0, 10}
        self.assertEqual(2, len(xor_result))
        self.assertIn(0, xor_result)
        self.assertIn(10, xor_result)

    def test_round_trip_large_bitmap(self):
        """Test round-trip encoding/decoding of a large bitmap."""
        # Create a large bitmap with various patterns
        bitmap = EWAHBitmap()
        # Add bits at various intervals
        for i in range(0, 10000, 7):
            bitmap.add(i)

        original_bits = set(bitmap.bits)

        # Encode and decode
        encoded = bitmap.encode()
        decoded = EWAHBitmap(encoded)

        # Verify all bits preserved
        self.assertEqual(original_bits, decoded.bits)
        self.assertEqual(len(bitmap), len(decoded))


class EWAHBitmapTests(unittest.TestCase):
    """Tests for EWAH bitmap compression."""

    def test_empty_bitmap(self):
        """Test empty bitmap."""
        bitmap = EWAHBitmap()
        self.assertEqual(0, len(bitmap))
        self.assertEqual(0, bitmap.bit_count)

    def test_add_bit(self):
        """Test adding bits to bitmap."""
        bitmap = EWAHBitmap()
        bitmap.add(0)
        bitmap.add(5)
        bitmap.add(100)

        self.assertEqual(3, len(bitmap))
        self.assertEqual(101, bitmap.bit_count)
        self.assertIn(0, bitmap)
        self.assertIn(5, bitmap)
        self.assertIn(100, bitmap)
        self.assertNotIn(1, bitmap)
        self.assertNotIn(99, bitmap)

    def test_encode_decode(self):
        """Test encoding and decoding bitmaps."""
        bitmap = EWAHBitmap()
        bitmap.add(0)
        bitmap.add(1)
        bitmap.add(64)
        bitmap.add(128)

        # Encode
        data = bitmap.encode()
        self.assertIsInstance(data, bytes)

        # Decode
        bitmap2 = EWAHBitmap(data)
        self.assertEqual(len(bitmap), len(bitmap2))
        self.assertIn(0, bitmap2)
        self.assertIn(1, bitmap2)
        self.assertIn(64, bitmap2)
        self.assertIn(128, bitmap2)

    def test_bitwise_or(self):
        """Test bitwise OR operation."""
        bitmap1 = EWAHBitmap()
        bitmap1.add(0)
        bitmap1.add(5)

        bitmap2 = EWAHBitmap()
        bitmap2.add(5)
        bitmap2.add(10)

        result = bitmap1 | bitmap2
        self.assertEqual(3, len(result))
        self.assertIn(0, result)
        self.assertIn(5, result)
        self.assertIn(10, result)

    def test_bitwise_and(self):
        """Test bitwise AND operation."""
        bitmap1 = EWAHBitmap()
        bitmap1.add(0)
        bitmap1.add(5)

        bitmap2 = EWAHBitmap()
        bitmap2.add(5)
        bitmap2.add(10)

        result = bitmap1 & bitmap2
        self.assertEqual(1, len(result))
        self.assertIn(5, result)
        self.assertNotIn(0, result)
        self.assertNotIn(10, result)

    def test_bitwise_xor(self):
        """Test bitwise XOR operation."""
        bitmap1 = EWAHBitmap()
        bitmap1.add(0)
        bitmap1.add(5)

        bitmap2 = EWAHBitmap()
        bitmap2.add(5)
        bitmap2.add(10)

        result = bitmap1 ^ bitmap2
        self.assertEqual(2, len(result))
        self.assertIn(0, result)
        self.assertIn(10, result)
        self.assertNotIn(5, result)


class BitmapEntryTests(unittest.TestCase):
    """Tests for bitmap entries."""

    def test_create_entry(self):
        """Test creating a bitmap entry."""
        bitmap = EWAHBitmap()
        bitmap.add(0)
        bitmap.add(10)

        entry = BitmapEntry(
            object_pos=100,
            xor_offset=0,
            flags=0,
            bitmap=bitmap,
        )

        self.assertEqual(100, entry.object_pos)
        self.assertEqual(0, entry.xor_offset)
        self.assertEqual(0, entry.flags)
        self.assertEqual(bitmap, entry.bitmap)


class PackBitmapTests(unittest.TestCase):
    """Tests for pack bitmap."""

    def test_create_bitmap(self):
        """Test creating a pack bitmap."""
        bitmap = PackBitmap()
        self.assertEqual(BITMAP_VERSION, bitmap.version)
        self.assertEqual(BITMAP_OPT_FULL_DAG, bitmap.flags)
        self.assertIsNone(bitmap.pack_checksum)

    def test_bitmap_with_entries(self):
        """Test bitmap with entries."""
        bitmap = PackBitmap()
        commit_sha = b"\x00" * 20

        ewah_bitmap = EWAHBitmap()
        ewah_bitmap.add(0)
        ewah_bitmap.add(5)

        entry = BitmapEntry(
            object_pos=0,
            xor_offset=0,
            flags=0,
            bitmap=ewah_bitmap,
        )

        bitmap.entries[commit_sha] = entry

        self.assertTrue(bitmap.has_commit(commit_sha))
        self.assertFalse(bitmap.has_commit(b"\x01" * 20))

    def test_iter_commits(self):
        """Test iterating over commits with bitmaps."""
        bitmap = PackBitmap()
        commit1 = b"\x00" * 20
        commit2 = b"\x01" * 20

        ewah_bitmap = EWAHBitmap()
        entry = BitmapEntry(0, 0, 0, ewah_bitmap)

        bitmap.entries[commit1] = entry
        bitmap.entries[commit2] = entry

        commits = list(bitmap.iter_commits())
        self.assertEqual(2, len(commits))
        self.assertIn(commit1, commits)
        self.assertIn(commit2, commits)


class BitmapFileTests(unittest.TestCase):
    """Tests for bitmap file I/O."""

    def test_write_read_empty_bitmap(self):
        """Test writing and reading an empty bitmap."""
        bitmap = PackBitmap()
        bitmap.pack_checksum = b"\x00" * 20

        # Write to bytes
        f = BytesIO()
        write_bitmap_file(f, bitmap)

        # Read back
        f.seek(0)
        bitmap2 = read_bitmap_file(f)

        self.assertEqual(bitmap.version, bitmap2.version)
        self.assertEqual(bitmap.flags, bitmap2.flags)
        self.assertEqual(bitmap.pack_checksum, bitmap2.pack_checksum)

    def test_write_read_with_type_bitmaps(self):
        """Test writing and reading bitmaps with type information."""
        bitmap = PackBitmap()
        bitmap.pack_checksum = b"\xaa" * 20

        # Add some type bitmap data
        bitmap.commit_bitmap.add(0)
        bitmap.commit_bitmap.add(5)
        bitmap.tree_bitmap.add(1)
        bitmap.blob_bitmap.add(2)
        bitmap.tag_bitmap.add(3)

        # Write to bytes
        f = BytesIO()
        write_bitmap_file(f, bitmap)

        # Read back
        f.seek(0)
        bitmap2 = read_bitmap_file(f)

        self.assertEqual(bitmap.version, bitmap2.version)
        self.assertEqual(bitmap.pack_checksum, bitmap2.pack_checksum)

        # Check type bitmaps
        self.assertIn(0, bitmap2.commit_bitmap)
        self.assertIn(5, bitmap2.commit_bitmap)
        self.assertIn(1, bitmap2.tree_bitmap)
        self.assertIn(2, bitmap2.blob_bitmap)
        self.assertIn(3, bitmap2.tag_bitmap)

    def test_invalid_signature(self):
        """Test reading file with invalid signature."""
        f = BytesIO(b"XXXX\x00\x01\x00\x00")
        with self.assertRaises(ValueError) as cm:
            read_bitmap_file(f)
        self.assertIn("Invalid bitmap signature", str(cm.exception))

    def test_invalid_version(self):
        """Test reading file with invalid version."""
        f = BytesIO(BITMAP_SIGNATURE + b"\x00\x02\x00\x01")
        with self.assertRaises(ValueError) as cm:
            read_bitmap_file(f)
        self.assertIn("Unsupported bitmap version", str(cm.exception))

    def test_incomplete_header(self):
        """Test reading file with incomplete header."""
        f = BytesIO(BITMAP_SIGNATURE + b"\x00")
        with self.assertRaises(ValueError) as cm:
            read_bitmap_file(f)
        self.assertIn("Incomplete bitmap header", str(cm.exception))


class BitmapIntegrationTests(unittest.TestCase):
    """Integration tests for bitmap functionality."""

    def test_round_trip_file(self):
        """Test writing and reading bitmap to/from actual file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bitmap_path = os.path.join(tmpdir, "test.bitmap")

            # Create bitmap
            bitmap = PackBitmap()
            bitmap.pack_checksum = b"\xff" * 20
            bitmap.commit_bitmap.add(10)
            bitmap.tree_bitmap.add(20)

            # Write to file
            from dulwich.bitmap import write_bitmap

            write_bitmap(bitmap_path, bitmap)

            # Read back
            from dulwich.bitmap import read_bitmap

            bitmap2 = read_bitmap(bitmap_path)

            self.assertEqual(bitmap.version, bitmap2.version)
            self.assertEqual(bitmap.pack_checksum, bitmap2.pack_checksum)
            self.assertIn(10, bitmap2.commit_bitmap)
            self.assertIn(20, bitmap2.tree_bitmap)

    def test_xor_decompression(self):
        """Test XOR decompression of bitmap entries."""
        bitmap = PackBitmap()

        # Create base bitmap
        base_bitmap = EWAHBitmap()
        base_bitmap.add(0)
        base_bitmap.add(1)
        base_bitmap.add(2)

        base_entry = BitmapEntry(
            object_pos=0,
            xor_offset=0,
            flags=0,
            bitmap=base_bitmap,
        )

        # Create XOR'd bitmap
        # If we XOR base (bits 0,1,2) with XOR bitmap (bits 1,3),
        # result should be (bits 0,2,3)
        xor_bitmap = EWAHBitmap()
        xor_bitmap.add(1)
        xor_bitmap.add(3)

        xor_entry = BitmapEntry(
            object_pos=1,
            xor_offset=1,  # Reference the previous entry
            flags=0,
            bitmap=xor_bitmap,
        )

        # Add entries
        base_sha = b"\x00" * 20
        xor_sha = b"\x01" * 20

        bitmap.entries[base_sha] = base_entry
        bitmap.entries_list.append((base_sha, base_entry))

        bitmap.entries[xor_sha] = xor_entry
        bitmap.entries_list.append((xor_sha, xor_entry))

        # Get decompressed bitmap
        result = bitmap.get_bitmap(xor_sha)

        self.assertIsNotNone(result)
        self.assertIn(0, result)
        self.assertNotIn(1, result)  # XOR cancels this
        self.assertIn(2, result)
        self.assertIn(3, result)

    def test_lookup_table_round_trip(self):
        """Test reading and writing lookup tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bitmap_path = os.path.join(tmpdir, "test.bitmap")

            # Create bitmap with lookup table
            bitmap = PackBitmap()
            bitmap.flags = BITMAP_OPT_FULL_DAG | BITMAP_OPT_LOOKUP_TABLE
            bitmap.pack_checksum = b"\xaa" * 20

            # Add some bitmap entries (required for lookup table to be written/read)
            for i in range(3):
                ewah = EWAHBitmap()
                ewah.add(i)
                entry = BitmapEntry(
                    object_pos=i,
                    xor_offset=0,
                    flags=0,
                    bitmap=ewah,
                )
                sha = i.to_bytes(20, byteorder="big")
                bitmap.entries[sha] = entry
                bitmap.entries_list.append((sha, entry))

            bitmap.lookup_table = [
                (0, 100, 0),
                (1, 200, 0),
                (2, 300, 1),
            ]

            # Write to file
            from dulwich.bitmap import write_bitmap

            write_bitmap(bitmap_path, bitmap)

            # Read back
            from dulwich.bitmap import read_bitmap

            bitmap2 = read_bitmap(bitmap_path)

            self.assertEqual(bitmap.flags, bitmap2.flags)
            self.assertIsNotNone(bitmap2.lookup_table)
            self.assertEqual(3, len(bitmap2.lookup_table))
            self.assertEqual((0, 100, 0), bitmap2.lookup_table[0])
            self.assertEqual((1, 200, 0), bitmap2.lookup_table[1])
            self.assertEqual((2, 300, 1), bitmap2.lookup_table[2])

    def test_name_hash_cache_round_trip(self):
        """Test reading and writing name-hash cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bitmap_path = os.path.join(tmpdir, "test.bitmap")

            # Create bitmap with name-hash cache
            bitmap = PackBitmap()
            bitmap.flags = BITMAP_OPT_FULL_DAG | BITMAP_OPT_HASH_CACHE
            bitmap.pack_checksum = b"\xbb" * 20
            bitmap.name_hash_cache = [0x12345678, 0x9ABCDEF0, 0xFEDCBA98]

            # Write to file
            from dulwich.bitmap import write_bitmap

            write_bitmap(bitmap_path, bitmap)

            # Read back
            from dulwich.bitmap import read_bitmap

            bitmap2 = read_bitmap(bitmap_path)

            self.assertEqual(bitmap.flags, bitmap2.flags)
            self.assertIsNotNone(bitmap2.name_hash_cache)
            self.assertEqual(3, len(bitmap2.name_hash_cache))
            self.assertEqual(0x12345678, bitmap2.name_hash_cache[0])
            self.assertEqual(0x9ABCDEF0, bitmap2.name_hash_cache[1])
            self.assertEqual(0xFEDCBA98, bitmap2.name_hash_cache[2])


class BitmapErrorHandlingTests(unittest.TestCase):
    """Tests for error handling in bitmap reading."""

    def test_truncated_header(self):
        """Test reading bitmap with truncated header."""
        # Only 10 bytes instead of full header
        data = b"BITM\x00\x01\x00\x00\x00\x00"

        with self.assertRaises(ValueError) as ctx:
            read_bitmap_file(BytesIO(data))

        # Should raise ValueError about missing/incomplete header data
        self.assertIsInstance(ctx.exception, ValueError)

    def test_invalid_signature(self):
        """Test reading bitmap with invalid signature."""
        data = b"JUNK\x00\x01\x00\x00" + b"\x00" * 20

        with self.assertRaises(ValueError) as ctx:
            read_bitmap_file(BytesIO(data))

        self.assertIn("signature", str(ctx.exception).lower())

    def test_unsupported_version(self):
        """Test reading bitmap with unsupported version."""
        data = BITMAP_SIGNATURE
        data += b"\x00\x99"  # Version 153 (unsupported)
        data += b"\x00\x00"  # Flags
        data += b"\x00\x00\x00\x00"  # Entry count
        data += b"\x00" * 20  # Pack checksum

        with self.assertRaises(ValueError) as ctx:
            read_bitmap_file(BytesIO(data))

        self.assertIn("version", str(ctx.exception).lower())

    def test_truncated_type_bitmap(self):
        """Test reading bitmap with truncated type bitmap data."""
        # Valid header
        data = BITMAP_SIGNATURE
        data += BITMAP_VERSION.to_bytes(2, "big")
        data += b"\x00\x00"  # Flags
        data += b"\x00\x00\x00\x00"  # Entry count
        data += b"\x00" * 20  # Pack checksum

        # Truncated type bitmap (incomplete EWAH header)
        data += b"\x00\x00\x00\x05"  # bit_count
        data += b"\x00\x00"  # Incomplete word_count

        with self.assertRaises(ValueError) as ctx:
            read_bitmap_file(BytesIO(data))

        self.assertIn("type bitmap", str(ctx.exception).lower())

    def test_truncated_bitmap_entry(self):
        """Test reading bitmap with truncated entry."""
        # Valid header with 1 entry
        data = BITMAP_SIGNATURE
        data += BITMAP_VERSION.to_bytes(2, "big")
        data += b"\x00\x00"  # Flags
        data += b"\x00\x00\x00\x01"  # 1 entry
        data += b"\x00" * 20  # Pack checksum

        # Write empty type bitmaps
        for _ in range(4):
            empty_ewah = EWAHBitmap().encode()
            data += empty_ewah

        # Truncated entry (only object position, missing rest)
        data += b"\x00\x00\x00\x00"  # Object position
        # Missing: XOR offset, flags, bitmap data

        with self.assertRaises(ValueError) as ctx:
            read_bitmap_file(BytesIO(data))

        # Should raise ValueError about missing data
        self.assertIsInstance(ctx.exception, ValueError)

    def test_empty_bitmap_file(self):
        """Test reading completely empty file."""
        with self.assertRaises(ValueError):
            read_bitmap_file(BytesIO(b""))

    def test_bitmap_with_zero_entries(self):
        """Test valid bitmap with zero entries."""
        bitmap = PackBitmap()
        bitmap.pack_checksum = b"\x00" * 20

        f = BytesIO()
        write_bitmap_file(f, bitmap)
        f.seek(0)

        # Should read successfully
        bitmap2 = read_bitmap_file(f)
        self.assertEqual(0, len(bitmap2.entries))
        self.assertIsNotNone(bitmap2.pack_checksum)


class BitmapEdgeCaseTests(unittest.TestCase):
    """Tests for edge cases in bitmap handling."""

    def test_very_large_bitmap(self):
        """Test bitmap with many bits set."""
        bitmap = EWAHBitmap()
        # Add 100,000 bits
        for i in range(100000):
            if i % 3 == 0:  # Every 3rd bit
                bitmap.add(i)

        # Should encode and decode without issues
        encoded = bitmap.encode()
        decoded = EWAHBitmap(encoded)

        self.assertEqual(len(bitmap), len(decoded))
        # Verify a sample of bits
        self.assertIn(0, decoded)
        self.assertIn(99999, decoded)
        self.assertNotIn(1, decoded)
        self.assertNotIn(99998, decoded)

    def test_bitmap_with_large_gaps(self):
        """Test bitmap with large gaps between set bits."""
        bitmap = EWAHBitmap()
        bitmap.add(0)
        bitmap.add(100000)
        bitmap.add(200000)

        encoded = bitmap.encode()
        decoded = EWAHBitmap(encoded)

        self.assertEqual(3, len(decoded))
        self.assertIn(0, decoded)
        self.assertIn(100000, decoded)
        self.assertIn(200000, decoded)

    def test_bitmap_all_bits_in_word(self):
        """Test bitmap with all 64 bits in a word set."""
        bitmap = EWAHBitmap()
        for i in range(64):
            bitmap.add(i)

        encoded = bitmap.encode()
        decoded = EWAHBitmap(encoded)

        self.assertEqual(64, len(decoded))
        for i in range(64):
            self.assertIn(i, decoded)

    def test_multiple_flags_combined(self):
        """Test bitmap with multiple flags set."""
        bitmap = PackBitmap(
            flags=BITMAP_OPT_FULL_DAG | BITMAP_OPT_HASH_CACHE | BITMAP_OPT_LOOKUP_TABLE
        )
        bitmap.pack_checksum = b"\x00" * 20
        bitmap.lookup_table = [(0, 0, 0), (1, 100, 0)]
        bitmap.name_hash_cache = [0x12345678, 0xABCDEF00]

        # Add an entry
        test_bitmap = EWAHBitmap()
        test_bitmap.add(0)
        test_bitmap.add(5)
        entry = BitmapEntry(object_pos=0, xor_offset=0, flags=0, bitmap=test_bitmap)
        bitmap.entries[b"\x00" * 20] = entry

        # Write and read back
        f = BytesIO()
        write_bitmap_file(f, bitmap)
        f.seek(0)

        bitmap2 = read_bitmap_file(f)

        self.assertEqual(bitmap.flags, bitmap2.flags)
        self.assertTrue(bitmap2.flags & BITMAP_OPT_FULL_DAG)
        self.assertTrue(bitmap2.flags & BITMAP_OPT_HASH_CACHE)
        self.assertTrue(bitmap2.flags & BITMAP_OPT_LOOKUP_TABLE)
        self.assertIsNotNone(bitmap2.lookup_table)
        self.assertIsNotNone(bitmap2.name_hash_cache)


class BitmapConfigTests(unittest.TestCase):
    """Tests for bitmap-related configuration settings."""

    def test_pack_write_bitmaps_default(self):
        """Test pack.writeBitmaps defaults to false."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        self.assertFalse(config.get_boolean((b"pack",), b"writeBitmaps", False))

    def test_pack_write_bitmaps_true(self):
        """Test pack.writeBitmaps = true."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        config.set((b"pack",), b"writeBitmaps", b"true")
        self.assertTrue(config.get_boolean((b"pack",), b"writeBitmaps", False))

    def test_pack_write_bitmaps_false(self):
        """Test pack.writeBitmaps = false."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        config.set((b"pack",), b"writeBitmaps", b"false")
        self.assertFalse(config.get_boolean((b"pack",), b"writeBitmaps", False))

    def test_pack_write_bitmap_hash_cache_default(self):
        """Test pack.writeBitmapHashCache defaults to true."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        self.assertTrue(config.get_boolean((b"pack",), b"writeBitmapHashCache", True))

    def test_pack_write_bitmap_hash_cache_false(self):
        """Test pack.writeBitmapHashCache = false."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        config.set((b"pack",), b"writeBitmapHashCache", b"false")
        self.assertFalse(config.get_boolean((b"pack",), b"writeBitmapHashCache", True))

    def test_pack_write_bitmap_lookup_table_default(self):
        """Test pack.writeBitmapLookupTable defaults to true."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        self.assertTrue(config.get_boolean((b"pack",), b"writeBitmapLookupTable", True))

    def test_repack_write_bitmaps(self):
        """Test repack.writeBitmaps configuration."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        config.set((b"repack",), b"writeBitmaps", b"true")
        self.assertTrue(config.get_boolean((b"repack",), b"writeBitmaps", False))

    def test_pack_use_bitmap_index_default(self):
        """Test pack.useBitmapIndex defaults to true."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        self.assertTrue(config.get_boolean((b"pack",), b"useBitmapIndex", True))

    def test_pack_use_bitmap_index_false(self):
        """Test pack.useBitmapIndex = false."""
        from dulwich.config import ConfigFile

        config = ConfigFile()
        config.set((b"pack",), b"useBitmapIndex", b"false")
        self.assertFalse(config.get_boolean((b"pack",), b"useBitmapIndex", True))


class ReachabilityProviderTests(unittest.TestCase):
    """Tests for ObjectReachabilityProvider implementations."""

    def setUp(self):
        """Set up test repository with commits."""
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Blob, Commit, Tree

        self.test_dir = tempfile.mkdtemp()
        self.store = DiskObjectStore(self.test_dir)

        # Create a simple commit history:
        # commit1 -> commit2 -> commit3
        #         \-> commit4

        # Create blob and tree
        self.blob1 = Blob.from_string(b"test content 1")
        self.store.add_object(self.blob1)

        self.blob2 = Blob.from_string(b"test content 2")
        self.store.add_object(self.blob2)

        self.tree1 = Tree()
        self.tree1[b"file1.txt"] = (0o100644, self.blob1.id)
        self.store.add_object(self.tree1)

        self.tree2 = Tree()
        self.tree2[b"file1.txt"] = (0o100644, self.blob1.id)
        self.tree2[b"file2.txt"] = (0o100644, self.blob2.id)
        self.store.add_object(self.tree2)

        # Create commit1 (root)
        self.commit1 = Commit()
        self.commit1.tree = self.tree1.id
        self.commit1.message = b"First commit"
        self.commit1.author = self.commit1.committer = b"Test <test@example.com>"
        self.commit1.author_time = self.commit1.commit_time = 1234567890
        self.commit1.author_timezone = self.commit1.commit_timezone = 0
        self.store.add_object(self.commit1)

        # Create commit2 (child of commit1)
        self.commit2 = Commit()
        self.commit2.tree = self.tree1.id
        self.commit2.parents = [self.commit1.id]
        self.commit2.message = b"Second commit"
        self.commit2.author = self.commit2.committer = b"Test <test@example.com>"
        self.commit2.author_time = self.commit2.commit_time = 1234567891
        self.commit2.author_timezone = self.commit2.commit_timezone = 0
        self.store.add_object(self.commit2)

        # Create commit3 (child of commit2)
        self.commit3 = Commit()
        self.commit3.tree = self.tree2.id
        self.commit3.parents = [self.commit2.id]
        self.commit3.message = b"Third commit"
        self.commit3.author = self.commit3.committer = b"Test <test@example.com>"
        self.commit3.author_time = self.commit3.commit_time = 1234567892
        self.commit3.author_timezone = self.commit3.commit_timezone = 0
        self.store.add_object(self.commit3)

        # Create commit4 (child of commit1, creates a branch)
        self.commit4 = Commit()
        self.commit4.tree = self.tree2.id
        self.commit4.parents = [self.commit1.id]
        self.commit4.message = b"Fourth commit"
        self.commit4.author = self.commit4.committer = b"Test <test@example.com>"
        self.commit4.author_time = self.commit4.commit_time = 1234567893
        self.commit4.author_timezone = self.commit4.commit_timezone = 0
        self.store.add_object(self.commit4)

    def tearDown(self):
        """Clean up test directory."""
        import shutil

        # Close store to release file handles on Windows
        self.store.close()
        shutil.rmtree(self.test_dir)

    def test_graph_traversal_reachability_single_commit(self):
        """Test GraphTraversalReachability with single commit."""
        from dulwich.object_store import GraphTraversalReachability

        provider = GraphTraversalReachability(self.store)

        # Get reachable commits from commit1
        reachable = provider.get_reachable_commits(
            [self.commit1.id], exclude=None, shallow=None
        )

        # Should only include commit1
        self.assertEqual({self.commit1.id}, reachable)

    def test_graph_traversal_reachability_linear_history(self):
        """Test GraphTraversalReachability with linear history."""
        from dulwich.object_store import GraphTraversalReachability

        provider = GraphTraversalReachability(self.store)

        # Get reachable commits from commit3
        reachable = provider.get_reachable_commits(
            [self.commit3.id], exclude=None, shallow=None
        )

        # Should include commit3, commit2, and commit1
        expected = {self.commit1.id, self.commit2.id, self.commit3.id}
        self.assertEqual(expected, reachable)

    def test_graph_traversal_reachability_with_exclusion(self):
        """Test GraphTraversalReachability with exclusion."""
        from dulwich.object_store import GraphTraversalReachability

        provider = GraphTraversalReachability(self.store)

        # Get commits reachable from commit3 but not from commit1
        reachable = provider.get_reachable_commits(
            [self.commit3.id], exclude=[self.commit1.id], shallow=None
        )

        # Should include commit3 and commit2, but not commit1
        expected = {self.commit2.id, self.commit3.id}
        self.assertEqual(expected, reachable)

    def test_graph_traversal_reachability_branching(self):
        """Test GraphTraversalReachability with branching history."""
        from dulwich.object_store import GraphTraversalReachability

        provider = GraphTraversalReachability(self.store)

        # Get reachable commits from both commit3 and commit4
        reachable = provider.get_reachable_commits(
            [self.commit3.id, self.commit4.id], exclude=None, shallow=None
        )

        # Should include all commits
        expected = {self.commit1.id, self.commit2.id, self.commit3.id, self.commit4.id}
        self.assertEqual(expected, reachable)

    def test_graph_traversal_reachable_objects(self):
        """Test GraphTraversalReachability.get_reachable_objects()."""
        from dulwich.object_store import GraphTraversalReachability

        provider = GraphTraversalReachability(self.store)

        # Get all objects reachable from commit3
        reachable = provider.get_reachable_objects(
            [self.commit3.id], exclude_commits=None
        )

        # Should include commit3, blob1, and blob2 (but not tree objects themselves)
        self.assertIn(self.commit3.id, reachable)
        self.assertIn(self.blob1.id, reachable)
        self.assertIn(self.blob2.id, reachable)
        # Verify at least 3 objects
        self.assertGreaterEqual(len(reachable), 3)

    def test_graph_traversal_reachable_objects_with_exclusion(self):
        """Test GraphTraversalReachability.get_reachable_objects() with exclusion."""
        from dulwich.object_store import GraphTraversalReachability

        provider = GraphTraversalReachability(self.store)

        # Get objects reachable from commit3 but not from commit2
        reachable = provider.get_reachable_objects(
            [self.commit3.id], exclude_commits=[self.commit2.id]
        )

        # commit2 uses tree1 (which has blob1), commit3 uses tree2 (which has blob1 + blob2)
        # So should include commit3 and blob2 (new in commit3)
        # blob1 should be excluded because it's in tree1 (reachable from commit2)
        self.assertIn(self.commit3.id, reachable)
        self.assertIn(self.blob2.id, reachable)

    def test_get_reachability_provider_without_bitmaps(self):
        """Test get_reachability_provider returns GraphTraversalReachability when no bitmaps."""
        from dulwich.object_store import GraphTraversalReachability

        provider = self.store.get_reachability_provider()

        # Should return GraphTraversalReachability when no bitmaps available
        self.assertIsInstance(provider, GraphTraversalReachability)

    def test_get_reachability_provider_prefer_bitmaps_false(self):
        """Test get_reachability_provider with prefer_bitmaps=False."""
        from dulwich.object_store import GraphTraversalReachability

        provider = self.store.get_reachability_provider(prefer_bitmaps=False)

        # Should return GraphTraversalReachability when prefer_bitmaps=False
        self.assertIsInstance(provider, GraphTraversalReachability)

    def test_bitmap_reachability_fallback_without_bitmaps(self):
        """Test BitmapReachability falls back to graph traversal without bitmaps."""
        provider = BitmapReachability(self.store)

        # Without bitmaps, should fall back to graph traversal
        reachable = provider.get_reachable_commits(
            [self.commit3.id], exclude=None, shallow=None
        )

        # Should still work via fallback
        expected = {self.commit1.id, self.commit2.id, self.commit3.id}
        self.assertEqual(expected, reachable)

    def test_bitmap_reachability_fallback_with_shallow(self):
        """Test BitmapReachability falls back for shallow clones."""
        provider = BitmapReachability(self.store)

        # With shallow boundary, should fall back to graph traversal
        reachable = provider.get_reachable_commits(
            [self.commit3.id], exclude=None, shallow={self.commit2.id}
        )

        # Should include commit3 and commit2 (shallow boundary includes boundary commit)
        # but not commit1 (beyond shallow boundary)
        self.assertEqual({self.commit2.id, self.commit3.id}, reachable)

    def test_reachability_provider_protocol(self):
        """Test that both providers implement the same interface."""
        graph_provider = GraphTraversalReachability(self.store)
        bitmap_provider = BitmapReachability(self.store)

        # Both should have the same methods
        for method in [
            "get_reachable_commits",
            "get_reachable_objects",
            "get_tree_objects",
        ]:
            self.assertTrue(hasattr(graph_provider, method))
            self.assertTrue(hasattr(bitmap_provider, method))

    def test_graph_traversal_vs_bitmap_consistency(self):
        """Test that GraphTraversalReachability and BitmapReachability produce same results."""
        graph_provider = GraphTraversalReachability(self.store)
        bitmap_provider = BitmapReachability(self.store)  # Will use fallback

        # Test get_reachable_commits
        graph_commits = graph_provider.get_reachable_commits(
            [self.commit3.id], exclude=[self.commit1.id], shallow=None
        )
        bitmap_commits = bitmap_provider.get_reachable_commits(
            [self.commit3.id], exclude=[self.commit1.id], shallow=None
        )
        self.assertEqual(graph_commits, bitmap_commits)

        # Test get_reachable_objects
        graph_objects = graph_provider.get_reachable_objects(
            [self.commit3.id], exclude_commits=None
        )
        bitmap_objects = bitmap_provider.get_reachable_objects(
            [self.commit3.id], exclude_commits=None
        )
        self.assertEqual(graph_objects, bitmap_objects)


class PackEnsureBitmapTests(unittest.TestCase):
    """Tests for Pack.ensure_bitmap() method."""

    def setUp(self):
        """Set up test repository with a pack."""
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Blob, Commit, Tree

        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)

        # Create pack directory
        os.makedirs(os.path.join(self.temp_dir, "pack"))

        self.store = DiskObjectStore(self.temp_dir)
        # Close store before cleanup to release file handles on Windows
        self.addCleanup(self.store.close)

        # Create test objects
        self.blob = Blob.from_string(b"test content")
        self.store.add_object(self.blob)

        self.tree = Tree()
        self.tree.add(b"file.txt", 0o100644, self.blob.id)
        self.store.add_object(self.tree)

        self.commit = Commit()
        self.commit.tree = self.tree.id
        self.commit.author = self.commit.committer = b"Test <test@example.com>"
        self.commit.author_time = self.commit.commit_time = 1234567890
        self.commit.author_timezone = self.commit.commit_timezone = 0
        self.commit.message = b"Test commit"
        self.store.add_object(self.commit)

        # Repack to create a pack
        self.store.repack()
        self.pack = self.store.packs[0]

    def test_ensure_bitmap_creates_bitmap(self):
        """Test that ensure_bitmap creates a bitmap file."""
        # Initially no bitmap
        self.assertFalse(os.path.exists(self.pack._bitmap_path))

        # Ensure bitmap with commit_interval=1 to ensure our single commit is selected
        refs = {b"refs/heads/master": self.commit.id}
        bitmap = self.pack.ensure_bitmap(self.store, refs, commit_interval=1)

        # Bitmap should now exist
        self.assertIsNotNone(bitmap)
        self.assertTrue(os.path.exists(self.pack._bitmap_path))
        # Verify it's a PackBitmap instance
        from dulwich.bitmap import PackBitmap

        self.assertIsInstance(bitmap, PackBitmap)

    def test_ensure_bitmap_returns_existing(self):
        """Test that ensure_bitmap returns existing bitmap without regenerating."""
        refs = {b"refs/heads/master": self.commit.id}

        # Create bitmap with commit_interval=1
        self.pack.ensure_bitmap(self.store, refs, commit_interval=1)
        mtime1 = os.path.getmtime(self.pack._bitmap_path)

        # Ensure again - should return existing
        import time

        time.sleep(0.01)  # Ensure time difference
        self.pack.ensure_bitmap(self.store, refs, commit_interval=1)
        mtime2 = os.path.getmtime(self.pack._bitmap_path)

        # File should not have been regenerated
        self.assertEqual(mtime1, mtime2)

    def test_ensure_bitmap_with_custom_interval(self):
        """Test ensure_bitmap with custom commit_interval."""
        refs = {b"refs/heads/master": self.commit.id}
        bitmap = self.pack.ensure_bitmap(self.store, refs, commit_interval=50)
        self.assertIsNotNone(bitmap)


class GeneratePackBitmapsTests(unittest.TestCase):
    """Tests for PackBasedObjectStore.generate_pack_bitmaps()."""

    def setUp(self):
        """Set up test repository."""
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Blob, Commit, Tree

        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)

        # Create pack directory
        os.makedirs(os.path.join(self.temp_dir, "pack"))

        self.store = DiskObjectStore(self.temp_dir)
        # Close store before cleanup to release file handles on Windows
        self.addCleanup(self.store.close)

        # Create multiple commits
        self.commits = []
        for i in range(3):
            blob = Blob.from_string(f"content {i}".encode())
            self.store.add_object(blob)

            tree = Tree()
            tree.add(f"file{i}.txt".encode(), 0o100644, blob.id)
            self.store.add_object(tree)

            commit = Commit()
            commit.tree = tree.id
            if i > 0:
                commit.parents = [self.commits[-1].id]
            commit.author = commit.committer = b"Test <test@example.com>"
            commit.author_time = commit.commit_time = 1234567890 + i
            commit.author_timezone = commit.commit_timezone = 0
            commit.message = f"Commit {i}".encode()
            self.store.add_object(commit)
            self.commits.append(commit)

        # Repack to create pack
        self.store.repack()

    def test_generate_pack_bitmaps(self):
        """Test generating bitmaps for all packs."""
        refs = {b"refs/heads/master": self.commits[-1].id}

        # Initially no bitmaps
        for pack in self.store.packs:
            self.assertFalse(os.path.exists(pack._bitmap_path))

        # Generate bitmaps
        count = self.store.generate_pack_bitmaps(refs)

        # Should have generated bitmaps
        self.assertEqual(count, len(self.store.packs))
        for pack in self.store.packs:
            self.assertTrue(os.path.exists(pack._bitmap_path))

    def test_generate_pack_bitmaps_multiple_calls(self):
        """Test that calling generate_pack_bitmaps multiple times is safe."""
        refs = {b"refs/heads/master": self.commits[-1].id}

        # Generate once
        self.store.generate_pack_bitmaps(refs)
        mtimes1 = [os.path.getmtime(p._bitmap_path) for p in self.store.packs]

        # Generate again
        import time

        time.sleep(0.01)
        self.store.generate_pack_bitmaps(refs)
        mtimes2 = [os.path.getmtime(p._bitmap_path) for p in self.store.packs]

        # Should not regenerate existing bitmaps
        self.assertEqual(mtimes1, mtimes2)

    def test_generate_pack_bitmaps_with_progress(self):
        """Test generate_pack_bitmaps with progress callback."""
        refs = {b"refs/heads/master": self.commits[-1].id}
        messages = []

        def progress(msg):
            messages.append(msg)

        self.store.generate_pack_bitmaps(refs, progress=progress)

        # Should have received progress messages
        self.assertGreater(len(messages), 0)
