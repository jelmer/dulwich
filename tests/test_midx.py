# test_midx.py -- Tests for multi-pack-index
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

"""Tests for multi-pack-index (MIDX) functionality."""

import os
import tempfile
from io import BytesIO
from unittest import TestCase

from dulwich.midx import (
    HASH_ALGORITHM_SHA1,
    MultiPackIndex,
    write_midx,
    write_midx_file,
)


class MIDXWriteTests(TestCase):
    """Tests for writing MIDX files."""

    def test_write_empty_midx(self):
        """Test writing an empty MIDX file."""
        f = BytesIO()
        pack_entries = []
        checksum = write_midx(f, pack_entries, HASH_ALGORITHM_SHA1)

        # Checksum should be 20 bytes
        self.assertEqual(20, len(checksum))

        # Should be able to read it back
        f.seek(0)
        midx = MultiPackIndex("test.midx", file=f, contents=f.read())
        self.assertEqual(0, len(midx))
        self.assertEqual(0, midx.pack_count)
        self.assertEqual([], midx.pack_names)

    def test_write_single_pack_midx(self):
        """Test writing a MIDX file with a single pack."""
        f = BytesIO()

        # Create some fake pack entries
        pack_entries = [
            (
                "pack-abc123.idx",
                [
                    (b"\x01" * 20, 100, 0x12345678),  # sha, offset, crc32
                    (b"\x02" * 20, 200, 0x87654321),
                    (b"\x03" * 20, 300, 0xABCDEF00),
                ],
            )
        ]

        checksum = write_midx(f, pack_entries, HASH_ALGORITHM_SHA1)
        self.assertEqual(20, len(checksum))

        # Read it back
        f.seek(0)
        midx = MultiPackIndex("test.midx", file=f, contents=f.read())

        self.assertEqual(3, len(midx))
        self.assertEqual(1, midx.pack_count)
        self.assertEqual(["pack-abc123.idx"], midx.pack_names)

        # Check object lookups
        result = midx.object_offset(b"\x01" * 20)
        self.assertIsNotNone(result)
        pack_name, offset = result
        self.assertEqual("pack-abc123.idx", pack_name)
        self.assertEqual(100, offset)

        result = midx.object_offset(b"\x02" * 20)
        self.assertIsNotNone(result)
        pack_name, offset = result
        self.assertEqual("pack-abc123.idx", pack_name)
        self.assertEqual(200, offset)

        result = midx.object_offset(b"\x03" * 20)
        self.assertIsNotNone(result)
        pack_name, offset = result
        self.assertEqual("pack-abc123.idx", pack_name)
        self.assertEqual(300, offset)

        # Check non-existent object
        result = midx.object_offset(b"\xff" * 20)
        self.assertIsNone(result)

    def test_write_multiple_packs_midx(self):
        """Test writing a MIDX file with multiple packs."""
        f = BytesIO()

        pack_entries = [
            (
                "pack-111.idx",
                [
                    (b"\x01" * 20, 100, 0),
                    (b"\x03" * 20, 300, 0),
                ],
            ),
            (
                "pack-222.idx",
                [
                    (b"\x02" * 20, 50, 0),
                    (b"\x04" * 20, 150, 0),
                ],
            ),
        ]

        checksum = write_midx(f, pack_entries, HASH_ALGORITHM_SHA1)
        self.assertEqual(20, len(checksum))

        # Read it back
        f.seek(0)
        midx = MultiPackIndex("test.midx", file=f, contents=f.read())

        self.assertEqual(4, len(midx))
        self.assertEqual(2, midx.pack_count)
        self.assertEqual(["pack-111.idx", "pack-222.idx"], midx.pack_names)

        # Objects should be findable across packs
        result = midx.object_offset(b"\x01" * 20)
        self.assertIsNotNone(result)
        self.assertEqual("pack-111.idx", result[0])

        result = midx.object_offset(b"\x02" * 20)
        self.assertIsNotNone(result)
        self.assertEqual("pack-222.idx", result[0])

    def test_write_large_offsets(self):
        """Test writing a MIDX file with large offsets (>= 2^31)."""
        f = BytesIO()

        large_offset = 2**32  # Offset that requires LOFF chunk
        pack_entries = [
            (
                "pack-large.idx",
                [
                    (b"\x01" * 20, 100, 0),
                    (b"\x02" * 20, large_offset, 0),  # Large offset
                ],
            )
        ]

        checksum = write_midx(f, pack_entries, HASH_ALGORITHM_SHA1)
        self.assertEqual(20, len(checksum))

        # Read it back
        f.seek(0)
        midx = MultiPackIndex("test.midx", file=f, contents=f.read())

        self.assertEqual(2, len(midx))

        # Small offset should work
        result = midx.object_offset(b"\x01" * 20)
        self.assertIsNotNone(result)
        self.assertEqual(100, result[1])

        # Large offset should work
        result = midx.object_offset(b"\x02" * 20)
        self.assertIsNotNone(result)
        self.assertEqual(large_offset, result[1])

    def test_write_midx_file(self):
        """Test writing a MIDX file to disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            midx_path = os.path.join(tmpdir, "multi-pack-index")

            pack_entries = [
                (
                    "pack-test.idx",
                    [
                        (b"\xaa" * 20, 1000, 0),
                    ],
                )
            ]

            checksum = write_midx_file(midx_path, pack_entries, HASH_ALGORITHM_SHA1)
            self.assertEqual(20, len(checksum))

            # Verify file was created
            self.assertTrue(os.path.exists(midx_path))

            # Read it back from disk
            with open(midx_path, "rb") as f:
                midx = MultiPackIndex(midx_path, file=f, contents=f.read())

            self.assertEqual(1, len(midx))
            result = midx.object_offset(b"\xaa" * 20)
            self.assertIsNotNone(result)
            self.assertEqual("pack-test.idx", result[0])
            self.assertEqual(1000, result[1])


class MIDXContainsTests(TestCase):
    """Tests for MIDX __contains__ method."""

    def test_contains_object(self):
        """Test checking if an object is in the MIDX."""
        f = BytesIO()
        pack_entries = [
            (
                "pack-test.idx",
                [
                    (b"\x01" * 20, 100, 0),
                    (b"\x02" * 20, 200, 0),
                ],
            )
        ]

        write_midx(f, pack_entries, HASH_ALGORITHM_SHA1)
        f.seek(0)
        midx = MultiPackIndex("test.midx", file=f, contents=f.read())

        self.assertTrue(b"\x01" * 20 in midx)
        self.assertTrue(b"\x02" * 20 in midx)
        self.assertFalse(b"\xff" * 20 in midx)


class MIDXIterEntriesTests(TestCase):
    """Tests for MIDX iterentries method."""

    def test_iterentries(self):
        """Test iterating over MIDX entries."""
        f = BytesIO()
        pack_entries = [
            (
                "pack-111.idx",
                [
                    (b"\x01" * 20, 100, 0),
                    (b"\x03" * 20, 300, 0),
                ],
            ),
            (
                "pack-222.idx",
                [
                    (b"\x02" * 20, 50, 0),
                ],
            ),
        ]

        write_midx(f, pack_entries, HASH_ALGORITHM_SHA1)
        f.seek(0)
        midx = MultiPackIndex("test.midx", file=f, contents=f.read())

        entries = list(midx.iterentries())
        self.assertEqual(3, len(entries))

        # Entries should be sorted by SHA
        self.assertEqual(b"\x01" * 20, entries[0][0])
        self.assertEqual("pack-111.idx", entries[0][1])
        self.assertEqual(100, entries[0][2])

        self.assertEqual(b"\x02" * 20, entries[1][0])
        self.assertEqual("pack-222.idx", entries[1][1])
        self.assertEqual(50, entries[1][2])

        self.assertEqual(b"\x03" * 20, entries[2][0])
        self.assertEqual("pack-111.idx", entries[2][1])
        self.assertEqual(300, entries[2][2])
