# test_lfs_integration.py -- Integration tests for LFS with filters
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Integration tests for LFS with the filter system."""

import shutil
import tempfile

from dulwich.config import ConfigDict
from dulwich.filters import FilterBlobNormalizer, FilterRegistry
from dulwich.lfs import LFSFilterDriver, LFSStore
from dulwich.objects import Blob

from . import TestCase


class LFSFilterIntegrationTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        # Create temporary directory for LFS store
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)

        # Set up LFS store and filter
        self.lfs_store = LFSStore.create(self.test_dir)
        self.lfs_filter = LFSFilterDriver(self.lfs_store)

        # Set up filter registry and normalizer
        self.config = ConfigDict()
        self.registry = FilterRegistry(self.config)
        self.registry.register_driver("lfs", self.lfs_filter)

        # Set up gitattributes to use LFS for .bin files
        self.gitattributes = {
            b"*.bin": {b"filter": b"lfs"},
        }

        self.normalizer = FilterBlobNormalizer(
            self.config, self.gitattributes, self.registry
        )

    def test_lfs_round_trip(self) -> None:
        """Test complete LFS round trip through filter normalizer."""
        # Create a blob with binary content
        original_content = b"This is a large binary file content" * 100
        blob = Blob()
        blob.data = original_content

        # Checkin: should convert to LFS pointer
        checked_in = self.normalizer.checkin_normalize(blob, b"large.bin")

        # Verify it's an LFS pointer
        self.assertLess(len(checked_in.data), len(original_content))
        self.assertTrue(
            checked_in.data.startswith(b"version https://git-lfs.github.com/spec/v1")
        )

        # Checkout: should restore original content
        checked_out = self.normalizer.checkout_normalize(checked_in, b"large.bin")

        # Verify we got back the original content
        self.assertEqual(checked_out.data, original_content)

    def test_non_lfs_file(self) -> None:
        """Test that non-LFS files pass through unchanged."""
        # Create a text file (not matching *.bin pattern)
        content = b"This is a regular text file"
        blob = Blob()
        blob.data = content

        # Both operations should return the original blob
        checked_in = self.normalizer.checkin_normalize(blob, b"file.txt")
        self.assertIs(checked_in, blob)

        checked_out = self.normalizer.checkout_normalize(blob, b"file.txt")
        self.assertIs(checked_out, blob)

    def test_lfs_pointer_file(self) -> None:
        """Test handling of files that are already LFS pointers."""
        # Create an LFS pointer manually
        from dulwich.lfs import LFSPointer

        # First store some content
        content = b"Content to be stored in LFS"
        sha = self.lfs_store.write_object([content])

        # Create pointer
        pointer = LFSPointer(sha, len(content))
        blob = Blob()
        blob.data = pointer.to_bytes()

        # Checkin should recognize it's already a pointer and not change it
        checked_in = self.normalizer.checkin_normalize(blob, b"data.bin")
        self.assertIs(checked_in, blob)

        # Checkout should expand it
        checked_out = self.normalizer.checkout_normalize(blob, b"data.bin")
        self.assertEqual(checked_out.data, content)

    def test_missing_lfs_object(self) -> None:
        """Test handling of LFS pointer with missing object."""
        from dulwich.lfs import LFSPointer

        # Create pointer to non-existent object
        pointer = LFSPointer(
            "0000000000000000000000000000000000000000000000000000000000000000", 1234
        )
        blob = Blob()
        blob.data = pointer.to_bytes()

        # Checkout should return the pointer as-is when object is missing
        checked_out = self.normalizer.checkout_normalize(blob, b"missing.bin")
        self.assertEqual(checked_out.data, blob.data)
