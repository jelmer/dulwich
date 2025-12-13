# test_sha256_pack.py -- Tests for SHA256 pack support
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

"""Tests for SHA256 pack support in Dulwich."""

import shutil
import tempfile
import unittest
from io import BytesIO

from dulwich.object_format import SHA256
from dulwich.pack import (
    load_pack_index_file,
    write_pack_index_v2,
)


class SHA256PackTests(unittest.TestCase):
    """Tests for SHA256 pack support."""

    def setUp(self):
        """Set up test repository directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test repository."""
        shutil.rmtree(self.test_dir)

    def test_pack_index_v2_with_sha256(self):
        """Test that pack index v2 correctly handles SHA256 hashes."""
        # Create SHA256 entries manually (simulating what would happen in a SHA256 repo)
        entries = []
        for i in range(5):
            # Create a fake SHA256 hash
            sha256_hash = SHA256.hash_func(f"test object {i}".encode()).digest()
            offset = i * 1000  # Fake offsets
            crc32 = i  # Fake CRC32
            entries.append((sha256_hash, offset, crc32))

        # Sort entries by SHA (required for pack index)
        entries.sort(key=lambda e: e[0])

        # Write SHA256 pack index with SHA1 pack checksum (Git always uses SHA1 for pack checksums)
        index_buf = BytesIO()
        from hashlib import sha1

        pack_checksum = sha1(b"fake pack data").digest()
        write_pack_index_v2(index_buf, entries, pack_checksum)

        # Load and verify the index
        index_buf.seek(0)
        pack_idx = load_pack_index_file("<memory>", index_buf, SHA256)

        # Check that the index loaded correctly
        self.assertEqual(len(pack_idx), 5)
        self.assertEqual(pack_idx.version, 2)

        # Verify hash_size detection
        self.assertEqual(pack_idx.hash_size, 32)

        # Verify we can look up objects by SHA256
        for sha256_hash, offset, _ in entries:
            # This should not raise KeyError
            found_offset = pack_idx.object_offset(sha256_hash)
            self.assertEqual(found_offset, offset)

    def test_pack_index_v1_with_sha256(self):
        """Test that pack index v1 correctly handles SHA256 hashes."""
        # Create SHA256 entries manually
        entries = []
        for i in range(5):
            # Create a fake SHA256 hash
            sha256_hash = SHA256.hash_func(f"test v1 object {i}".encode()).digest()
            offset = i * 1000  # Fake offsets
            crc32 = None  # v1 doesn't store CRC32
            entries.append((sha256_hash, offset, crc32))

        # Sort entries by SHA (required for pack index)
        entries.sort(key=lambda e: e[0])

        # Import write_pack_index_v1
        from dulwich.pack import write_pack_index_v1

        # Write SHA256 pack index v1 with SHA1 pack checksum
        index_buf = BytesIO()
        from hashlib import sha1

        pack_checksum = sha1(b"fake v1 pack data").digest()
        write_pack_index_v1(index_buf, entries, pack_checksum)

        # Load and verify the index
        index_buf.seek(0)
        pack_idx = load_pack_index_file("<memory>", index_buf, SHA256)

        # Check that the index loaded correctly
        self.assertEqual(len(pack_idx), 5)
        self.assertEqual(pack_idx.version, 1)

        # Verify hash_size detection
        self.assertEqual(pack_idx.hash_size, 32)

        # Verify we can look up objects by SHA256
        for sha256_hash, offset, _ in entries:
            # This should not raise KeyError
            found_offset = pack_idx.object_offset(sha256_hash)
            self.assertEqual(found_offset, offset)


if __name__ == "__main__":
    unittest.main()
