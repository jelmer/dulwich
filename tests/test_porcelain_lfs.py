# test_porcelain_lfs.py -- Tests for LFS porcelain functions
# Copyright (C) 2024 Jelmer Vernooij
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

"""Tests for LFS porcelain functions."""

import os
import tempfile
import unittest

from dulwich import porcelain
from dulwich.lfs import LFSPointer, LFSStore
from dulwich.repo import Repo
from tests import TestCase


class LFSPorcelainTestCase(TestCase):
    """Test case for LFS porcelain functions."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup_test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def _cleanup_test_dir(self):
        """Clean up test directory recursively."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_lfs_init(self):
        """Test LFS initialization."""
        porcelain.lfs_init(self.repo)

        # Check that LFS store was created
        lfs_dir = os.path.join(self.repo.controldir(), "lfs")
        self.assertTrue(os.path.exists(lfs_dir))
        self.assertTrue(os.path.exists(os.path.join(lfs_dir, "objects")))
        self.assertTrue(os.path.exists(os.path.join(lfs_dir, "tmp")))

        # Check that config was set
        config = self.repo.get_config()
        self.assertEqual(
            config.get((b"filter", b"lfs"), b"process"), b"git-lfs filter-process"
        )
        self.assertEqual(config.get((b"filter", b"lfs"), b"required"), b"true")

    def test_lfs_track(self):
        """Test tracking patterns with LFS."""
        # Track some patterns
        patterns = ["*.bin", "*.pdf"]
        tracked = porcelain.lfs_track(self.repo, patterns)

        self.assertEqual(set(tracked), set(patterns))

        # Check .gitattributes was created
        gitattributes_path = os.path.join(self.repo.path, ".gitattributes")
        self.assertTrue(os.path.exists(gitattributes_path))

        # Read and verify content
        with open(gitattributes_path, "rb") as f:
            content = f.read()

        self.assertIn(b"*.bin diff=lfs filter=lfs merge=lfs -text", content)
        self.assertIn(b"*.pdf diff=lfs filter=lfs merge=lfs -text", content)

        # Test listing tracked patterns
        tracked = porcelain.lfs_track(self.repo)
        self.assertEqual(set(tracked), set(patterns))

    def test_lfs_untrack(self):
        """Test untracking patterns from LFS."""
        # First track some patterns
        patterns = ["*.bin", "*.pdf", "*.zip"]
        porcelain.lfs_track(self.repo, patterns)

        # Untrack one pattern
        remaining = porcelain.lfs_untrack(self.repo, ["*.pdf"])
        self.assertEqual(set(remaining), {"*.bin", "*.zip"})

        # Verify .gitattributes
        with open(os.path.join(self.repo.path, ".gitattributes"), "rb") as f:
            content = f.read()

        self.assertIn(b"*.bin diff=lfs filter=lfs merge=lfs -text", content)
        self.assertNotIn(b"*.pdf diff=lfs filter=lfs merge=lfs -text", content)
        self.assertIn(b"*.zip diff=lfs filter=lfs merge=lfs -text", content)

    def test_lfs_clean(self):
        """Test cleaning a file to LFS pointer."""
        # Initialize LFS
        porcelain.lfs_init(self.repo)

        # Create a test file
        test_content = b"This is test content for LFS"
        test_file = os.path.join(self.repo.path, "test.bin")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Clean the file
        pointer_content = porcelain.lfs_clean(self.repo, "test.bin")

        # Verify it's a valid LFS pointer
        pointer = LFSPointer.from_bytes(pointer_content)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, len(test_content))

        # Verify the content was stored in LFS
        lfs_store = LFSStore.from_repo(self.repo)
        with lfs_store.open_object(pointer.oid) as f:
            stored_content = f.read()
        self.assertEqual(stored_content, test_content)

    def test_lfs_smudge(self):
        """Test smudging an LFS pointer to content."""
        # Initialize LFS
        porcelain.lfs_init(self.repo)

        # Create test content and store it
        test_content = b"This is test content for smudging"
        lfs_store = LFSStore.from_repo(self.repo)
        oid = lfs_store.write_object([test_content])

        # Create LFS pointer
        pointer = LFSPointer(oid, len(test_content))
        pointer_content = pointer.to_bytes()

        # Smudge the pointer
        smudged_content = porcelain.lfs_smudge(self.repo, pointer_content)

        self.assertEqual(smudged_content, test_content)

    def test_lfs_ls_files(self):
        """Test listing LFS files."""
        # Initialize repo with some LFS files
        porcelain.lfs_init(self.repo)

        # Create a test file and convert to LFS
        test_content = b"Large file content"
        test_file = os.path.join(self.repo.path, "large.bin")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Clean to LFS pointer
        pointer_content = porcelain.lfs_clean(self.repo, "large.bin")
        with open(test_file, "wb") as f:
            f.write(pointer_content)

        # Add and commit
        porcelain.add(self.repo, paths=["large.bin"])
        porcelain.commit(self.repo, message=b"Add LFS file")

        # List LFS files
        lfs_files = porcelain.lfs_ls_files(self.repo)

        self.assertEqual(len(lfs_files), 1)
        path, oid, size = lfs_files[0]
        self.assertEqual(path, "large.bin")
        self.assertEqual(size, len(test_content))

    def test_lfs_migrate(self):
        """Test migrating files to LFS."""
        # Create some files
        files = {
            "small.txt": b"Small file",
            "large1.bin": b"X" * 1000,
            "large2.dat": b"Y" * 2000,
            "exclude.bin": b"Z" * 1500,
        }

        for filename, content in files.items():
            path = os.path.join(self.repo.path, filename)
            with open(path, "wb") as f:
                f.write(content)

        # Add files to index
        porcelain.add(self.repo, paths=list(files.keys()))

        # Migrate with patterns
        count = porcelain.lfs_migrate(
            self.repo, include=["*.bin", "*.dat"], exclude=["exclude.*"]
        )

        self.assertEqual(count, 2)  # large1.bin and large2.dat

        # Verify files were converted to LFS pointers
        for filename in ["large1.bin", "large2.dat"]:
            path = os.path.join(self.repo.path, filename)
            with open(path, "rb") as f:
                content = f.read()
            pointer = LFSPointer.from_bytes(content)
            self.assertIsNotNone(pointer)

    def test_lfs_pointer_check(self):
        """Test checking if files are LFS pointers."""
        # Initialize LFS
        porcelain.lfs_init(self.repo)

        # Create an LFS pointer file
        test_content = b"LFS content"
        lfs_file = os.path.join(self.repo.path, "lfs.bin")
        # First create the file
        with open(lfs_file, "wb") as f:
            f.write(test_content)
        pointer_content = porcelain.lfs_clean(self.repo, "lfs.bin")
        with open(lfs_file, "wb") as f:
            f.write(pointer_content)

        # Create a regular file
        regular_file = os.path.join(self.repo.path, "regular.txt")
        with open(regular_file, "wb") as f:
            f.write(b"Regular content")

        # Check both files
        results = porcelain.lfs_pointer_check(
            self.repo, paths=["lfs.bin", "regular.txt", "nonexistent.txt"]
        )

        self.assertIsNotNone(results["lfs.bin"])
        self.assertIsNone(results["regular.txt"])
        self.assertIsNone(results["nonexistent.txt"])


if __name__ == "__main__":
    unittest.main()
