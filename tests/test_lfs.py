# test_lfs.py -- tests for LFS
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for LFS support."""

import shutil
import tempfile

from dulwich.lfs import LFSFilterDriver, LFSPointer, LFSStore

from . import TestCase


class LFSTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.lfs = LFSStore.create(self.test_dir)

    def test_create(self) -> None:
        sha = self.lfs.write_object([b"a", b"b"])
        with self.lfs.open_object(sha) as f:
            self.assertEqual(b"ab", f.read())

    def test_missing(self) -> None:
        self.assertRaises(KeyError, self.lfs.open_object, "abcdeabcdeabcdeabcde")

    def test_write_object_empty(self) -> None:
        """Test writing an empty object."""
        sha = self.lfs.write_object([])
        with self.lfs.open_object(sha) as f:
            self.assertEqual(b"", f.read())

    def test_write_object_multiple_chunks(self) -> None:
        """Test writing an object with multiple chunks."""
        chunks = [b"chunk1", b"chunk2", b"chunk3"]
        sha = self.lfs.write_object(chunks)
        with self.lfs.open_object(sha) as f:
            self.assertEqual(b"".join(chunks), f.read())

    def test_sha_path_calculation(self) -> None:
        """Test the internal sha path calculation."""
        # The implementation splits the sha into parts for directory structure
        # Write and verify we can read it back
        sha = self.lfs.write_object([b"test data"])
        self.assertEqual(len(sha), 64)  # SHA-256 is 64 hex chars

        # Open should succeed, which verifies the path calculation works
        with self.lfs.open_object(sha) as f:
            self.assertEqual(b"test data", f.read())

    def test_create_lfs_dir(self) -> None:
        """Test creating an LFS directory when it doesn't exist."""
        import os

        # Create a temporary directory for the test
        lfs_parent_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, lfs_parent_dir)

        # Create a path for the LFS directory
        lfs_dir = os.path.join(lfs_parent_dir, "lfs")

        # Create the LFS store
        LFSStore.create(lfs_dir)

        # Verify the directories were created
        self.assertTrue(os.path.isdir(lfs_dir))
        self.assertTrue(os.path.isdir(os.path.join(lfs_dir, "tmp")))
        self.assertTrue(os.path.isdir(os.path.join(lfs_dir, "objects")))


class LFSPointerTests(TestCase):
    def test_from_bytes_valid(self) -> None:
        """Test parsing a valid LFS pointer."""
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size 0\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNotNone(pointer)
        self.assertEqual(
            pointer.oid,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        self.assertEqual(pointer.size, 0)

    def test_from_bytes_with_extra_fields(self) -> None:
        """Test parsing LFS pointer with extra fields (should still work)."""
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size 1234\n"
            b"x-custom-field value\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, 1234)

    def test_from_bytes_invalid_version(self) -> None:
        """Test parsing with invalid version line."""
        pointer_data = (
            b"version https://invalid.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size 0\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNone(pointer)

    def test_from_bytes_missing_oid(self) -> None:
        """Test parsing with missing OID."""
        pointer_data = b"version https://git-lfs.github.com/spec/v1\nsize 0\n"
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNone(pointer)

    def test_from_bytes_missing_size(self) -> None:
        """Test parsing with missing size."""
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNone(pointer)

    def test_from_bytes_invalid_size(self) -> None:
        """Test parsing with invalid size."""
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size not_a_number\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNone(pointer)

    def test_from_bytes_binary_data(self) -> None:
        """Test parsing binary data (not an LFS pointer)."""
        binary_data = b"\x00\x01\x02\x03\x04"
        pointer = LFSPointer.from_bytes(binary_data)
        self.assertIsNone(pointer)

    def test_to_bytes(self) -> None:
        """Test converting LFS pointer to bytes."""
        pointer = LFSPointer(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 1234
        )
        data = pointer.to_bytes()
        expected = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size 1234\n"
        )
        self.assertEqual(data, expected)

    def test_round_trip(self) -> None:
        """Test converting to bytes and back."""
        original = LFSPointer(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 9876
        )
        data = original.to_bytes()
        parsed = LFSPointer.from_bytes(data)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.oid, original.oid)
        self.assertEqual(parsed.size, original.size)

    def test_is_valid_oid(self) -> None:
        """Test OID validation."""
        # Valid SHA256
        valid_pointer = LFSPointer(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0
        )
        self.assertTrue(valid_pointer.is_valid_oid())

        # Too short
        short_pointer = LFSPointer("e3b0c44298fc1c14", 0)
        self.assertFalse(short_pointer.is_valid_oid())

        # Invalid hex characters
        invalid_pointer = LFSPointer(
            "g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0
        )
        self.assertFalse(invalid_pointer.is_valid_oid())


class LFSFilterDriverTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.lfs_store = LFSStore.create(self.test_dir)
        self.filter_driver = LFSFilterDriver(self.lfs_store)

    def test_clean_new_file(self) -> None:
        """Test clean filter on new file content."""
        content = b"This is a test file content"
        result = self.filter_driver.clean(content)

        # Result should be an LFS pointer
        pointer = LFSPointer.from_bytes(result)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, len(content))

        # Content should be stored in LFS
        with self.lfs_store.open_object(pointer.oid) as f:
            self.assertEqual(f.read(), content)

    def test_clean_existing_pointer(self) -> None:
        """Test clean filter on already-pointer content."""
        # Create a pointer
        pointer = LFSPointer(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 1234
        )
        pointer_data = pointer.to_bytes()

        # Clean should return the pointer unchanged
        result = self.filter_driver.clean(pointer_data)
        self.assertEqual(result, pointer_data)

    def test_smudge_valid_pointer(self) -> None:
        """Test smudge filter with valid pointer."""
        # Store some content
        content = b"This is the actual file content"
        sha = self.lfs_store.write_object([content])

        # Create pointer
        pointer = LFSPointer(sha, len(content))
        pointer_data = pointer.to_bytes()

        # Smudge should return the actual content
        result = self.filter_driver.smudge(pointer_data)
        self.assertEqual(result, content)

    def test_smudge_missing_object(self) -> None:
        """Test smudge filter with missing LFS object."""
        # Create pointer to non-existent object
        pointer = LFSPointer(
            "0000000000000000000000000000000000000000000000000000000000000000", 1234
        )
        pointer_data = pointer.to_bytes()

        # Smudge should return the pointer as-is when object is missing
        result = self.filter_driver.smudge(pointer_data)
        self.assertEqual(result, pointer_data)

    def test_smudge_non_pointer(self) -> None:
        """Test smudge filter on non-pointer content."""
        content = b"This is not an LFS pointer"

        # Smudge should return content unchanged
        result = self.filter_driver.smudge(content)
        self.assertEqual(result, content)

    def test_round_trip(self) -> None:
        """Test clean followed by smudge."""
        original_content = b"Round trip test content"

        # Clean (working tree -> repo)
        pointer_data = self.filter_driver.clean(original_content)

        # Verify it's a pointer
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNotNone(pointer)

        # Smudge (repo -> working tree)
        restored_content = self.filter_driver.smudge(pointer_data)

        # Should get back the original content
        self.assertEqual(restored_content, original_content)
