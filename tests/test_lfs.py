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

from dulwich.lfs import LFSStore

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
