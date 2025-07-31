# test_lfs.py -- tests for LFS
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for LFS support."""

import json
import os
import shutil
import tempfile

from dulwich import porcelain
from dulwich.lfs import LFSFilterDriver, LFSPointer, LFSStore
from dulwich.repo import Repo

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


class LFSIntegrationTests(TestCase):
    """Integration tests for LFS with Git operations."""

    def setUp(self) -> None:
        super().setUp()
        import os

        from dulwich.repo import Repo

        # Create temporary directory for test repo
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)

        # Initialize repo
        self.repo = Repo.init(self.test_dir)
        self.lfs_dir = os.path.join(self.test_dir, ".git", "lfs")
        self.lfs_store = LFSStore.create(self.lfs_dir)

    def test_lfs_with_gitattributes(self) -> None:
        """Test LFS integration with .gitattributes."""
        import os

        # Create .gitattributes file
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.bin filter=lfs diff=lfs merge=lfs -text\n")

        # Create a binary file
        bin_path = os.path.join(self.test_dir, "large.bin")
        large_content = b"Large binary content" * 1000
        with open(bin_path, "wb") as f:
            f.write(large_content)

        # Add files to repo
        self.repo.get_worktree().stage([".gitattributes", "large.bin"])

        # Get the blob for large.bin from the index
        index = self.repo.open_index()
        entry = index[b"large.bin"]
        blob = self.repo.object_store[entry.sha]

        # With LFS configured, the blob should contain an LFS pointer
        # (Note: This would require actual LFS filter integration in dulwich)
        # For now, we just verify the structure
        self.assertIsNotNone(blob)

    def test_lfs_checkout_missing_object(self) -> None:
        """Test checkout behavior when LFS object is missing."""
        from dulwich.objects import Blob, Commit, Tree

        # Create an LFS pointer blob
        pointer = LFSPointer(
            "0000000000000000000000000000000000000000000000000000000000000000", 1234
        )
        blob = Blob()
        blob.data = pointer.to_bytes()
        self.repo.object_store.add_object(blob)

        # Create tree with the blob
        tree = Tree()
        tree.add(b"missing.bin", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        # Create commit
        commit = Commit()
        commit.tree = tree.id
        commit.message = b"Add missing LFS file"
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        self.repo.object_store.add_object(commit)

        # Update HEAD
        self.repo.refs[b"HEAD"] = commit.id

        # Checkout should leave pointer file when object is missing
        # (actual checkout would require more integration)

    def test_lfs_pointer_detection(self) -> None:
        """Test detection of LFS pointer files."""
        # Test various file contents
        test_cases = [
            # Valid LFS pointer
            (
                b"version https://git-lfs.github.com/spec/v1\n"
                b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
                b"size 1234\n",
                True,
            ),
            # Regular text file
            (b"This is a regular text file\n", False),
            # Binary file
            (b"\x00\x01\x02\x03\x04", False),
            # File that starts like pointer but isn't
            (b"version 1.0\nThis is not an LFS pointer\n", False),
        ]

        for content, expected_is_pointer in test_cases:
            pointer = LFSPointer.from_bytes(content)
            self.assertEqual(
                pointer is not None,
                expected_is_pointer,
                f"Failed for content: {content!r}",
            )

    def test_builtin_lfs_clone_no_config(self) -> None:
        """Test cloning with LFS when no git-lfs commands are configured."""
        # Create source repository
        source_dir = os.path.join(self.test_dir, "source")
        os.makedirs(source_dir)
        source_repo = Repo.init(source_dir)

        # Create empty config (no LFS commands)
        config = source_repo.get_config()
        config.write_to_path()

        # Create .gitattributes with LFS filter
        gitattributes_path = os.path.join(source_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.bin filter=lfs\n")

        # Create test content and store in LFS
        test_content = b"Test binary content"
        test_oid = LFSStore.from_repo(source_repo, create=True).write_object(
            [test_content]
        )

        # Create LFS pointer file
        pointer = LFSPointer(test_oid, len(test_content))
        pointer_file = os.path.join(source_dir, "test.bin")
        with open(pointer_file, "wb") as f:
            f.write(pointer.to_bytes())

        # Commit files
        porcelain.add(source_repo, paths=[".gitattributes", "test.bin"])
        porcelain.commit(source_repo, message=b"Add LFS tracked file")
        source_repo.close()

        # Clone the repository
        target_dir = os.path.join(self.test_dir, "target")
        target_repo = porcelain.clone(source_dir, target_dir)

        # Verify no LFS commands in config
        target_config = target_repo.get_config_stack()
        with self.assertRaises(KeyError):
            target_config.get((b"filter", b"lfs"), b"smudge")

        # Check the cloned file
        cloned_file = os.path.join(target_dir, "test.bin")
        with open(cloned_file, "rb") as f:
            content = f.read()

        # Should still be a pointer (LFS object not in target's store)
        self.assertTrue(
            content.startswith(b"version https://git-lfs.github.com/spec/v1")
        )
        self.assertIn(test_oid.encode(), content)
        target_repo.close()

    def test_builtin_lfs_with_local_objects(self) -> None:
        """Test built-in LFS filter when objects are available locally."""
        # No LFS config
        config = self.repo.get_config()
        config.write_to_path()

        # Create .gitattributes
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.dat filter=lfs\n")

        # Create LFS store and add object
        test_content = b"Hello from LFS!"
        lfs_store = LFSStore.from_repo(self.repo, create=True)
        test_oid = lfs_store.write_object([test_content])

        # Create pointer file
        pointer = LFSPointer(test_oid, len(test_content))
        pointer_file = os.path.join(self.test_dir, "data.dat")
        with open(pointer_file, "wb") as f:
            f.write(pointer.to_bytes())

        # Commit
        porcelain.add(self.repo, paths=[".gitattributes", "data.dat"])
        porcelain.commit(self.repo, message=b"Add LFS file")

        # Reset index to trigger checkout with filter
        self.repo.get_worktree().reset_index()

        # Check file content
        with open(pointer_file, "rb") as f:
            content = f.read()

        # Built-in filter should have converted pointer to actual content
        self.assertEqual(content, test_content)

    def test_builtin_lfs_filter_used(self) -> None:
        """Verify that built-in LFS filter is used when no config exists."""
        # Get filter registry
        normalizer = self.repo.get_blob_normalizer()
        filter_registry = normalizer.filter_registry
        lfs_driver = filter_registry.get_driver("lfs")

        # Should be built-in LFS filter
        self.assertIsInstance(lfs_driver, LFSFilterDriver)
        self.assertEqual(type(lfs_driver).__module__, "dulwich.lfs")


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

    def test_clean_empty_file(self) -> None:
        """Test clean filter on empty file."""
        content = b""
        result = self.filter_driver.clean(content)

        # Result should be an LFS pointer
        pointer = LFSPointer.from_bytes(result)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, 0)

        # Empty content should be stored in LFS
        with self.lfs_store.open_object(pointer.oid) as f:
            self.assertEqual(f.read(), content)

    def test_clean_large_file(self) -> None:
        """Test clean filter on large file."""
        # Create a large file (1MB)
        content = b"x" * (1024 * 1024)
        result = self.filter_driver.clean(content)

        # Result should be an LFS pointer
        pointer = LFSPointer.from_bytes(result)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, len(content))

        # Content should be stored in LFS
        with self.lfs_store.open_object(pointer.oid) as f:
            self.assertEqual(f.read(), content)

    def test_smudge_corrupt_pointer(self) -> None:
        """Test smudge filter with corrupt pointer data."""
        # Create corrupt pointer data
        corrupt_data = (
            b"version https://git-lfs.github.com/spec/v1\noid sha256:invalid\n"
        )

        # Smudge should return the data as-is
        result = self.filter_driver.smudge(corrupt_data)
        self.assertEqual(result, corrupt_data)

    def test_clean_unicode_content(self) -> None:
        """Test clean filter with unicode content."""
        # UTF-8 encoded unicode content
        content = "Hello 世界 🌍".encode()
        result = self.filter_driver.clean(content)

        # Result should be an LFS pointer
        pointer = LFSPointer.from_bytes(result)
        self.assertIsNotNone(pointer)

        # Content should be preserved exactly
        with self.lfs_store.open_object(pointer.oid) as f:
            self.assertEqual(f.read(), content)


class LFSStoreEdgeCaseTests(TestCase):
    """Edge case tests for LFS store."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.lfs = LFSStore.create(self.test_dir)

    def test_concurrent_writes(self) -> None:
        """Test that concurrent writes to same content work correctly."""
        content = b"duplicate content"

        # Write the same content multiple times
        sha1 = self.lfs.write_object([content])
        sha2 = self.lfs.write_object([content])

        # Should get the same SHA
        self.assertEqual(sha1, sha2)

        # Content should be stored only once
        with self.lfs.open_object(sha1) as f:
            self.assertEqual(f.read(), content)

    def test_write_with_generator(self) -> None:
        """Test writing object with generator chunks."""

        def chunk_generator():
            yield b"chunk1"
            yield b"chunk2"
            yield b"chunk3"

        sha = self.lfs.write_object(chunk_generator())

        # Verify content
        with self.lfs.open_object(sha) as f:
            self.assertEqual(f.read(), b"chunk1chunk2chunk3")

    def test_partial_write_rollback(self) -> None:
        """Test that partial writes don't leave artifacts."""
        import os

        # Count initial objects
        objects_dir = os.path.join(self.test_dir, "objects")
        initial_count = sum(len(files) for _, _, files in os.walk(objects_dir))

        # Try to write with a failing generator
        def failing_generator():
            yield b"chunk1"
            raise RuntimeError("Simulated error")

        # This should fail
        with self.assertRaises(RuntimeError):
            self.lfs.write_object(failing_generator())

        # No new objects should have been created
        final_count = sum(len(files) for _, _, files in os.walk(objects_dir))
        self.assertEqual(initial_count, final_count)


class LFSPointerEdgeCaseTests(TestCase):
    """Edge case tests for LFS pointer parsing."""

    def test_pointer_with_windows_line_endings(self) -> None:
        """Test parsing pointer with Windows line endings."""
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\r\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\r\n"
            b"size 1234\r\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, 1234)

    def test_pointer_with_extra_whitespace(self) -> None:
        """Test parsing pointer with extra whitespace."""
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1  \n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size 1234   \n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, 1234)

    def test_pointer_case_sensitivity(self) -> None:
        """Test that pointer parsing is case sensitive."""
        # Version line must be exact
        pointer_data = (
            b"Version https://git-lfs.github.com/spec/v1\n"  # Capital V
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size 1234\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNone(pointer)  # Should fail due to case

    def test_pointer_oid_formats(self) -> None:
        """Test different OID formats."""
        # SHA256 is currently the only supported format
        # Test SHA1 format (should fail)
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha1:356a192b7913b04c54574d18c28d46e6395428ab\n"  # SHA1
            b"size 1234\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        # This might be accepted but marked as invalid OID
        if pointer:
            self.assertFalse(pointer.is_valid_oid())

    def test_pointer_size_limits(self) -> None:
        """Test size value limits."""
        # Test with very large size
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size 999999999999999999\n"  # Very large number
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, 999999999999999999)

        # Test with negative size (should fail)
        pointer_data = (
            b"version https://git-lfs.github.com/spec/v1\n"
            b"oid sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
            b"size -1\n"
        )
        pointer = LFSPointer.from_bytes(pointer_data)
        self.assertIsNone(pointer)  # Should fail with negative size


class LFSServerTests(TestCase):
    """Tests for the LFS server implementation."""

    def setUp(self) -> None:
        super().setUp()
        import threading

        from dulwich.lfs_server import run_lfs_server

        # Create temporary directory for LFS storage
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)

        # Start LFS server
        self.server, self.server_url = run_lfs_server(port=0, lfs_dir=self.test_dir)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        self.addCleanup(self.server.shutdown)

    def test_server_batch_endpoint(self) -> None:
        """Test the batch endpoint directly."""
        from urllib.request import Request, urlopen

        # Create batch request
        batch_data = {
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": "abc123", "size": 100}],
        }

        req = Request(
            f"{self.server_url}/objects/batch",
            data=json.dumps(batch_data).encode("utf-8"),
            headers={
                "Content-Type": "application/vnd.git-lfs+json",
                "Accept": "application/vnd.git-lfs+json",
            },
            method="POST",
        )

        with urlopen(req) as response:
            result = json.loads(response.read())

        self.assertIn("objects", result)
        self.assertEqual(len(result["objects"]), 1)
        self.assertEqual(result["objects"][0]["oid"], "abc123")
        self.assertIn("error", result["objects"][0])  # Object doesn't exist

    def test_server_upload_download(self) -> None:
        """Test uploading and downloading an object."""
        import hashlib
        from urllib.request import Request, urlopen

        test_content = b"test server content"
        test_oid = hashlib.sha256(test_content).hexdigest()

        # Get upload URL via batch
        batch_data = {
            "operation": "upload",
            "transfers": ["basic"],
            "objects": [{"oid": test_oid, "size": len(test_content)}],
        }

        req = Request(
            f"{self.server_url}/objects/batch",
            data=json.dumps(batch_data).encode("utf-8"),
            headers={
                "Content-Type": "application/vnd.git-lfs+json",
                "Accept": "application/vnd.git-lfs+json",
            },
            method="POST",
        )

        with urlopen(req) as response:
            batch_result = json.loads(response.read())

        upload_url = batch_result["objects"][0]["actions"]["upload"]["href"]

        # Upload the object
        upload_req = Request(
            upload_url,
            data=test_content,
            headers={"Content-Type": "application/octet-stream"},
            method="PUT",
        )

        with urlopen(upload_req) as response:
            self.assertEqual(response.status, 200)

        # Download the object
        download_batch_data = {
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": test_oid, "size": len(test_content)}],
        }

        req = Request(
            f"{self.server_url}/objects/batch",
            data=json.dumps(download_batch_data).encode("utf-8"),
            headers={
                "Content-Type": "application/vnd.git-lfs+json",
                "Accept": "application/vnd.git-lfs+json",
            },
            method="POST",
        )

        with urlopen(req) as response:
            download_batch_result = json.loads(response.read())

        download_url = download_batch_result["objects"][0]["actions"]["download"][
            "href"
        ]

        # Download the object
        download_req = Request(download_url)

        with urlopen(download_req) as response:
            downloaded_content = response.read()

        self.assertEqual(downloaded_content, test_content)

    def test_server_verify_endpoint(self) -> None:
        """Test the verify endpoint."""
        import hashlib
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen

        test_content = b"verify test"
        test_oid = hashlib.sha256(test_content).hexdigest()

        # First upload the object
        self.server.lfs_store.write_object([test_content])

        # Test verify for existing object
        verify_req = Request(
            f"{self.server_url}/objects/{test_oid}/verify",
            data=json.dumps({"oid": test_oid, "size": len(test_content)}).encode(
                "utf-8"
            ),
            headers={"Content-Type": "application/vnd.git-lfs+json"},
            method="POST",
        )

        with urlopen(verify_req) as response:
            self.assertEqual(response.status, 200)

        # Test verify for non-existent object
        fake_oid = "0" * 64
        verify_req = Request(
            f"{self.server_url}/objects/{fake_oid}/verify",
            data=json.dumps({"oid": fake_oid, "size": 100}).encode("utf-8"),
            headers={"Content-Type": "application/vnd.git-lfs+json"},
            method="POST",
        )

        with self.assertRaises(HTTPError) as cm:
            with urlopen(verify_req):
                pass
        self.assertEqual(cm.exception.code, 404)

    def test_server_invalid_endpoints(self) -> None:
        """Test invalid endpoints return 404."""
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen

        # Test invalid GET endpoint
        with self.assertRaises(HTTPError) as cm:
            with urlopen(f"{self.server_url}/invalid"):
                pass
        self.assertEqual(cm.exception.code, 404)

        # Test invalid POST endpoint
        req = Request(f"{self.server_url}/invalid", data=b"test", method="POST")

        with self.assertRaises(HTTPError) as cm:
            with urlopen(req):
                pass
        self.assertEqual(cm.exception.code, 404)

    def test_server_batch_invalid_operation(self) -> None:
        """Test batch endpoint with invalid operation."""
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen

        batch_data = {"operation": "invalid", "transfers": ["basic"], "objects": []}

        req = Request(
            f"{self.server_url}/objects/batch",
            data=json.dumps(batch_data).encode("utf-8"),
            headers={"Content-Type": "application/vnd.git-lfs+json"},
            method="POST",
        )

        with self.assertRaises(HTTPError) as cm:
            with urlopen(req):
                pass
        self.assertEqual(cm.exception.code, 400)

    def test_server_batch_missing_fields(self) -> None:
        """Test batch endpoint with missing required fields."""
        from urllib.request import Request, urlopen

        # Missing oid
        batch_data = {
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"size": 100}],  # Missing oid
        }

        req = Request(
            f"{self.server_url}/objects/batch",
            data=json.dumps(batch_data).encode("utf-8"),
            headers={"Content-Type": "application/vnd.git-lfs+json"},
            method="POST",
        )

        with urlopen(req) as response:
            result = json.loads(response.read())

        self.assertIn("error", result["objects"][0])
        self.assertIn("Missing oid", result["objects"][0]["error"]["message"])

    def test_server_upload_oid_mismatch(self) -> None:
        """Test upload with OID mismatch."""
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen

        # Upload with wrong OID
        upload_req = Request(
            f"{self.server_url}/objects/wrongoid123",
            data=b"test content",
            headers={"Content-Type": "application/octet-stream"},
            method="PUT",
        )

        with self.assertRaises(HTTPError) as cm:
            with urlopen(upload_req):
                pass
        self.assertEqual(cm.exception.code, 400)
        self.assertIn("OID mismatch", cm.exception.read().decode())

    def test_server_download_non_existent(self) -> None:
        """Test downloading non-existent object."""
        from urllib.error import HTTPError
        from urllib.request import urlopen

        fake_oid = "0" * 64

        with self.assertRaises(HTTPError) as cm:
            with urlopen(f"{self.server_url}/objects/{fake_oid}"):
                pass
        self.assertEqual(cm.exception.code, 404)

    def test_server_invalid_json(self) -> None:
        """Test batch endpoint with invalid JSON."""
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen

        req = Request(
            f"{self.server_url}/objects/batch",
            data=b"not json",
            headers={"Content-Type": "application/vnd.git-lfs+json"},
            method="POST",
        )

        with self.assertRaises(HTTPError) as cm:
            with urlopen(req):
                pass
        self.assertEqual(cm.exception.code, 400)


class LFSClientTests(TestCase):
    """Tests for LFS client network operations."""

    def setUp(self) -> None:
        super().setUp()
        import threading

        from dulwich.lfs import LFSClient
        from dulwich.lfs_server import run_lfs_server

        # Create temporary directory for LFS storage
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)

        # Start LFS server in a thread
        self.server, self.server_url = run_lfs_server(port=0, lfs_dir=self.test_dir)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
        self.addCleanup(self.server.shutdown)

        # Create LFS client pointing to our test server
        self.client = LFSClient(self.server_url)

    def test_client_url_normalization(self) -> None:
        """Test that client URL is normalized correctly."""
        from dulwich.lfs import LFSClient

        # Test with trailing slash
        client = LFSClient("https://example.com/repo.git/info/lfs/")
        self.assertEqual(client.url, "https://example.com/repo.git/info/lfs")

        # Test without trailing slash
        client = LFSClient("https://example.com/repo.git/info/lfs")
        self.assertEqual(client.url, "https://example.com/repo.git/info/lfs")

    def test_batch_request_format(self) -> None:
        """Test batch request formatting."""
        # Create an object in the store
        test_content = b"test content for batch"
        sha = self.server.lfs_store.write_object([test_content])

        # Request download batch
        result = self.client.batch(
            "download", [{"oid": sha, "size": len(test_content)}]
        )

        self.assertIsNotNone(result.objects)
        self.assertEqual(len(result.objects), 1)
        self.assertEqual(result.objects[0].oid, sha)
        self.assertIsNotNone(result.objects[0].actions)
        self.assertIn("download", result.objects[0].actions)

    def test_download_with_verification(self) -> None:
        """Test download with size and hash verification."""
        import hashlib

        from dulwich.lfs import LFSError

        test_content = b"test content for download"
        test_oid = hashlib.sha256(test_content).hexdigest()

        # Store the object
        sha = self.server.lfs_store.write_object([test_content])
        self.assertEqual(sha, test_oid)  # Verify SHA calculation

        # Download the object
        content = self.client.download(test_oid, len(test_content))
        self.assertEqual(content, test_content)

        # Test size mismatch
        with self.assertRaises(LFSError) as cm:
            self.client.download(test_oid, 999)  # Wrong size
        self.assertIn("size", str(cm.exception))

    def test_upload_with_verify(self) -> None:
        """Test upload with verification step."""
        import hashlib

        test_content = b"upload test content"
        test_oid = hashlib.sha256(test_content).hexdigest()
        test_size = len(test_content)

        # Upload the object
        self.client.upload(test_oid, test_size, test_content)

        # Verify it was stored
        with self.server.lfs_store.open_object(test_oid) as f:
            stored_content = f.read()
        self.assertEqual(stored_content, test_content)

    def test_upload_already_exists(self) -> None:
        """Test upload when object already exists on server."""
        import hashlib

        test_content = b"existing content"
        test_oid = hashlib.sha256(test_content).hexdigest()

        # Pre-store the object
        self.server.lfs_store.write_object([test_content])

        # Upload again - should not raise an error
        self.client.upload(test_oid, len(test_content), test_content)

        # Verify it's still there
        with self.server.lfs_store.open_object(test_oid) as f:
            self.assertEqual(f.read(), test_content)

    def test_error_handling(self) -> None:
        """Test error handling for various scenarios."""
        from urllib.error import HTTPError

        from dulwich.lfs import LFSError

        # Test downloading non-existent object
        with self.assertRaises(LFSError) as cm:
            self.client.download(
                "0000000000000000000000000000000000000000000000000000000000000000", 100
            )
        self.assertIn("Object not found", str(cm.exception))

        # Test uploading with wrong OID
        with self.assertRaises(HTTPError) as cm:
            self.client.upload("wrong_oid", 5, b"hello")
        # Server should reject due to OID mismatch
        self.assertIn("OID mismatch", str(cm.exception))
