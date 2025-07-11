# test_porcelain_filters.py -- Tests for porcelain filter integration
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

"""Tests for porcelain filter integration."""

import os
import tempfile
from io import BytesIO

from dulwich import porcelain
from dulwich.repo import Repo

from . import TestCase
from .compat.utils import rmtree_ro


class PorcelainFilterTests(TestCase):
    """Test filter integration in porcelain commands."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def test_add_with_autocrlf(self) -> None:
        """Test adding files with autocrlf enabled."""
        # Configure autocrlf
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Create a file with CRLF line endings
        test_file = os.path.join(self.test_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"line1\r\nline2\r\nline3\r\n")

        # Add the file
        porcelain.add(self.repo, paths=["test.txt"])

        # Check that the blob in the index has LF line endings
        index = self.repo.open_index()
        entry = index[b"test.txt"]
        blob = self.repo.object_store[entry.sha]
        self.assertEqual(blob.data, b"line1\nline2\nline3\n")

    def test_checkout_with_autocrlf(self) -> None:
        """Test checkout with autocrlf enabled."""
        # First, add a file with LF line endings to the repo
        test_file = os.path.join(self.test_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"line1\nline2\nline3\n")

        porcelain.add(self.repo, paths=["test.txt"])
        porcelain.commit(self.repo, message=b"Add test file")

        # Remove the file
        os.remove(test_file)

        # Configure autocrlf
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Checkout the file
        porcelain.checkout(self.repo, paths=["test.txt"])

        # On Windows or with autocrlf=true, file should have CRLF line endings
        with open(test_file, "rb") as f:
            content = f.read()
            # The checkout should apply the smudge filter
            self.assertEqual(content, b"line1\r\nline2\r\nline3\r\n")

    def test_status_with_filters(self) -> None:
        """Test status command with filters applied."""
        # Configure autocrlf
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"input")
        config.write_to_path()

        # Create a file with CRLF line endings
        test_file = os.path.join(self.test_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"line1\r\nline2\r\n")

        # Add and commit with normalized line endings
        porcelain.add(self.repo, paths=["test.txt"])
        porcelain.commit(self.repo, message=b"Initial commit")

        # Modify the file with CRLF line endings
        with open(test_file, "wb") as f:
            f.write(b"line1\r\nline2\r\nline3\r\n")

        # Status should detect the change after normalizing
        results = porcelain.status(self.repo)
        self.assertIn(b"test.txt", results.unstaged)

    def test_diff_with_filters(self) -> None:
        """Test diff command with filters applied."""
        # Configure autocrlf
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Create and commit a file
        test_file = os.path.join(self.test_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"line1\r\nline2\r\n")

        porcelain.add(self.repo, paths=["test.txt"])
        porcelain.commit(self.repo, message=b"Initial commit")

        # Modify the file
        with open(test_file, "wb") as f:
            f.write(b"line1\r\nmodified\r\nline3\r\n")

        # Get diff - should normalize line endings for comparison
        outstream = BytesIO()
        porcelain.diff(self.repo, outstream=outstream)
        diff_output = outstream.getvalue()
        self.assertIn(b"-line2", diff_output)
        self.assertIn(b"+modified", diff_output)
        self.assertIn(b"+line3", diff_output)

    def test_add_with_gitattributes(self) -> None:
        """Test adding files with gitattributes filters."""
        # Create .gitattributes with text attribute
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.txt text\n")
            f.write(b"*.bin -text\n")

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create text file with CRLF
        text_file = os.path.join(self.test_dir, "test.txt")
        with open(text_file, "wb") as f:
            f.write(b"text\r\nfile\r\n")

        # Create binary file with CRLF (should not be converted)
        bin_file = os.path.join(self.test_dir, "test.bin")
        with open(bin_file, "wb") as f:
            f.write(b"binary\r\nfile\r\n")

        # Add both files
        porcelain.add(self.repo, paths=["test.txt", "test.bin"])

        # Check text file was normalized
        index = self.repo.open_index()
        text_entry = index[b"test.txt"]
        text_blob = self.repo.object_store[text_entry.sha]
        self.assertEqual(text_blob.data, b"text\nfile\n")

        # Check binary file was not normalized
        bin_entry = index[b"test.bin"]
        bin_blob = self.repo.object_store[bin_entry.sha]
        self.assertEqual(bin_blob.data, b"binary\r\nfile\r\n")

    def test_clone_with_filters(self) -> None:
        """Test cloning a repository with filters."""
        # Create a source repository
        source_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, source_dir)
        source_repo = Repo.init(source_dir)
        self.addCleanup(source_repo.close)

        # Add a file with LF endings
        test_file = os.path.join(source_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"line1\nline2\n")

        porcelain.add(source_repo, paths=["test.txt"])
        porcelain.commit(source_repo, message=b"Initial commit")

        # Clone the repository without checkout
        target_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, target_dir)

        # Clone without checkout first
        target_repo = porcelain.clone(source_dir, target_dir, checkout=False)
        self.addCleanup(target_repo.close)

        # Configure autocrlf in target repo
        target_config = target_repo.get_config()
        target_config.set((b"core",), b"autocrlf", b"true")
        target_config.write_to_path()

        # Now checkout the files with autocrlf enabled
        target_repo.reset_index()

        # Check that the working tree file has CRLF endings
        target_file = os.path.join(target_dir, "test.txt")
        with open(target_file, "rb") as f:
            content = f.read()
            # The checkout should apply the smudge filter
            self.assertIn(b"\r\n", content)

    def test_commit_with_clean_filter(self) -> None:
        """Test committing with a clean filter."""
        # Set up a custom filter in git config
        config = self.repo.get_config()
        config.set((b"filter", b"testfilter"), b"clean", b"sed 's/SECRET/REDACTED/g'")
        config.write_to_path()

        # Create .gitattributes to use the filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.secret filter=testfilter\n")

        porcelain.add(self.repo, paths=[".gitattributes"])
        porcelain.commit(self.repo, message=b"Add gitattributes")

        # Create a file with sensitive content
        secret_file = os.path.join(self.test_dir, "config.secret")
        with open(secret_file, "wb") as f:
            f.write(b"password=SECRET123\n")

        # Add the file
        porcelain.add(self.repo, paths=["config.secret"])

        # The committed blob should have filtered content
        # (Note: actual filter execution requires process filter support)

    def test_ls_files_with_filters(self) -> None:
        """Test ls-files respects filter settings."""
        # Configure autocrlf
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Create files with different line endings
        file1 = os.path.join(self.test_dir, "unix.txt")
        with open(file1, "wb") as f:
            f.write(b"unix\nfile\n")

        file2 = os.path.join(self.test_dir, "windows.txt")
        with open(file2, "wb") as f:
            f.write(b"windows\r\nfile\r\n")

        # Add files
        porcelain.add(self.repo, paths=["unix.txt", "windows.txt"])

        # List files
        files = list(porcelain.ls_files(self.repo))
        self.assertIn(b"unix.txt", files)
        self.assertIn(b"windows.txt", files)

        # Both files should be normalized in the index
        index = self.repo.open_index()
        for filename in [b"unix.txt", b"windows.txt"]:
            entry = index[filename]
            blob = self.repo.object_store[entry.sha]
            # Both should have LF line endings in the repository
            self.assertNotIn(b"\r\n", blob.data)


class PorcelainLFSIntegrationTests(TestCase):
    """Test LFS integration in porcelain commands."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

        # Set up LFS
        lfs_dir = os.path.join(self.test_dir, ".git", "lfs")
        os.makedirs(lfs_dir, exist_ok=True)

    def test_add_large_file_with_lfs(self) -> None:
        """Test adding large files with LFS filter."""
        # Configure LFS filter
        config = self.repo.get_config()
        config.set((b"filter", b"lfs"), b"clean", b"git-lfs clean -- %f")
        config.set((b"filter", b"lfs"), b"smudge", b"git-lfs smudge -- %f")
        config.set((b"filter", b"lfs"), b"process", b"git-lfs filter-process")
        config.set((b"filter", b"lfs"), b"required", b"true")
        config.write_to_path()

        # Create .gitattributes for LFS
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.bin filter=lfs diff=lfs merge=lfs -text\n")

        porcelain.add(self.repo, paths=[".gitattributes"])
        porcelain.commit(self.repo, message=b"Add LFS attributes")

        # Create a large binary file
        large_file = os.path.join(self.test_dir, "large.bin")
        content = b"X" * (1024 * 1024)  # 1MB file
        with open(large_file, "wb") as f:
            f.write(content)

        # Add the large file
        # Note: actual LFS handling requires git-lfs to be installed
        # This test verifies the filter infrastructure is in place
        porcelain.add(self.repo, paths=["large.bin"])

        # Check that something was added to the index
        index = self.repo.open_index()
        self.assertIn(b"large.bin", index)

    def test_status_with_lfs_files(self) -> None:
        """Test status command with LFS files."""
        # Set up LFS attributes
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.bin filter=lfs diff=lfs merge=lfs -text\n")

        porcelain.add(self.repo, paths=[".gitattributes"])
        porcelain.commit(self.repo, message=b"Add LFS attributes")

        # Create an LFS pointer file manually
        from dulwich.lfs import LFSPointer

        pointer = LFSPointer(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 1024
        )
        lfs_file = os.path.join(self.test_dir, "data.bin")
        with open(lfs_file, "wb") as f:
            f.write(pointer.to_bytes())

        # Add and commit the pointer
        porcelain.add(self.repo, paths=["data.bin"])
        porcelain.commit(self.repo, message=b"Add LFS file")

        # Modify the pointer file
        with open(lfs_file, "ab") as f:
            f.write(b"modified\n")

        # Status should detect the change
        results = porcelain.status(self.repo)
        self.assertIn(b"data.bin", results.unstaged)


class FilterEdgeCaseTests(TestCase):
    """Test edge cases in filter handling."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def test_mixed_line_endings(self) -> None:
        """Test handling files with mixed line endings."""
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Create file with mixed line endings
        mixed_file = os.path.join(self.test_dir, "mixed.txt")
        with open(mixed_file, "wb") as f:
            f.write(b"line1\r\nline2\nline3\r\nline4")

        porcelain.add(self.repo, paths=["mixed.txt"])

        # Check normalization
        index = self.repo.open_index()
        entry = index[b"mixed.txt"]
        blob = self.repo.object_store[entry.sha]
        # Should normalize all to LF
        self.assertEqual(blob.data, b"line1\nline2\nline3\nline4")

    def test_binary_detection(self) -> None:
        """Test binary file detection in filters."""
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Create a file with binary content
        binary_file = os.path.join(self.test_dir, "binary.dat")
        with open(binary_file, "wb") as f:
            f.write(b"\x00\x01\x02\r\n\x03\x04\r\n")

        porcelain.add(self.repo, paths=["binary.dat"])

        # Binary files should not be converted
        index = self.repo.open_index()
        entry = index[b"binary.dat"]
        blob = self.repo.object_store[entry.sha]
        self.assertEqual(blob.data, b"\x00\x01\x02\r\n\x03\x04\r\n")

    def test_empty_file_handling(self) -> None:
        """Test filter handling of empty files."""
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Create empty file
        empty_file = os.path.join(self.test_dir, "empty.txt")
        with open(empty_file, "wb") as f:
            f.write(b"")

        porcelain.add(self.repo, paths=["empty.txt"])

        # Empty files should pass through unchanged
        index = self.repo.open_index()
        entry = index[b"empty.txt"]
        blob = self.repo.object_store[entry.sha]
        self.assertEqual(blob.data, b"")

    def test_gitattributes_precedence(self) -> None:
        """Test that gitattributes takes precedence over config."""
        # Set autocrlf=false in config
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"false")
        config.write_to_path()

        # But force text conversion via gitattributes
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.txt text\n")

        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create file with CRLF
        text_file = os.path.join(self.test_dir, "test.txt")
        with open(text_file, "wb") as f:
            f.write(b"line1\r\nline2\r\n")

        porcelain.add(self.repo, paths=["test.txt"])

        # Should be normalized despite autocrlf=false
        index = self.repo.open_index()
        entry = index[b"test.txt"]
        blob = self.repo.object_store[entry.sha]
        self.assertEqual(blob.data, b"line1\nline2\n")
