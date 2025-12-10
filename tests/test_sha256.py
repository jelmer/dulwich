# test_sha256.py -- Tests for SHA256 support
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

"""Tests for SHA256 support in Dulwich."""

import os
import shutil
import tempfile
import unittest

from dulwich.object_format import SHA1, SHA256, get_object_format
from dulwich.objects import Blob, Tree, valid_hexsha
from dulwich.repo import MemoryRepo, Repo


class HashAlgorithmTests(unittest.TestCase):
    """Tests for the hash algorithm abstraction."""

    def test_sha1_properties(self):
        """Test SHA1 algorithm properties."""
        alg = SHA1
        self.assertEqual(alg.name, "sha1")
        self.assertEqual(alg.oid_length, 20)
        self.assertEqual(alg.hex_length, 40)

    def test_sha256_properties(self):
        """Test SHA256 algorithm properties."""
        alg = SHA256
        self.assertEqual(alg.name, "sha256")
        self.assertEqual(alg.oid_length, 32)
        self.assertEqual(alg.hex_length, 64)

    def test_get_hash_algorithm(self):
        """Test getting hash algorithms by name."""
        self.assertEqual(get_object_format("sha1"), SHA1)
        self.assertEqual(get_object_format("sha256"), SHA256)
        self.assertEqual(get_object_format(None), SHA1)  # Default

        with self.assertRaises(ValueError):
            get_object_format("invalid")


class ObjectHashingTests(unittest.TestCase):
    """Tests for object hashing with different algorithms."""

    def test_blob_sha1(self):
        """Test blob hashing with SHA1."""
        blob = Blob()
        blob.data = b"Hello, World!"

        # Default should be SHA1
        sha1_id = blob.id
        self.assertEqual(len(sha1_id), 40)
        self.assertTrue(valid_hexsha(sha1_id))

    def test_blob_sha256(self):
        """Test blob hashing with SHA256."""
        blob = Blob()
        blob.data = b"Hello, World!"

        # Get SHA256 hash
        sha256_id = blob.get_id(SHA256)
        self.assertEqual(len(sha256_id), 64)
        self.assertTrue(valid_hexsha(sha256_id))

        # SHA256 ID should be different from SHA1
        sha1_id = blob.id
        self.assertNotEqual(sha1_id, sha256_id)

        # Verify .id property returns SHA1 for backward compatibility
        self.assertEqual(blob.id, sha1_id)
        self.assertEqual(blob.get_id(), sha1_id)  # Default should be SHA1

    def test_tree_sha256(self):
        """Test tree hashing with SHA256."""
        tree = Tree()
        tree.add(b"file.txt", 0o100644, b"a" * 40)  # SHA1 hex

        # Get SHA1 (default)
        sha1_id = tree.id
        self.assertEqual(len(sha1_id), 40)

        # Get SHA256
        sha256_id = tree.get_id(SHA256)
        self.assertEqual(len(sha256_id), 64)

        # Verify they're different
        self.assertNotEqual(sha1_id, sha256_id)

    def test_valid_hexsha(self):
        """Test hex SHA validation for both algorithms."""
        # Valid SHA1
        self.assertTrue(valid_hexsha(b"1234567890abcdef1234567890abcdef12345678"))

        # Valid SHA256
        self.assertTrue(
            valid_hexsha(
                b"1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            )
        )

        # Invalid lengths
        self.assertFalse(valid_hexsha(b"1234"))
        self.assertFalse(
            valid_hexsha(b"1234567890abcdef1234567890abcdef123456")
        )  # 38 chars

        # Invalid characters
        self.assertFalse(valid_hexsha(b"123456789gabcdef1234567890abcdef12345678"))


class RepositorySHA256Tests(unittest.TestCase):
    """Tests for SHA256 repository support."""

    def setUp(self):
        """Set up test repository directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test repository."""
        shutil.rmtree(self.test_dir)

    def test_init_sha256_repo(self):
        """Test initializing a SHA256 repository."""
        repo_path = os.path.join(self.test_dir, "sha256_repo")
        repo = Repo.init(repo_path, mkdir=True, object_format="sha256")

        # Check repository format version
        config = repo.get_config()
        self.assertEqual(config.get(("core",), "repositoryformatversion"), b"1")

        # Check object format extension
        self.assertEqual(config.get(("extensions",), "objectformat"), b"sha256")

        # Check hash algorithm detection
        self.assertEqual(repo.object_format, SHA256)

        repo.close()

    def test_init_sha1_repo(self):
        """Test initializing a SHA1 repository (default)."""
        repo_path = os.path.join(self.test_dir, "sha1_repo")
        repo = Repo.init(repo_path, mkdir=True)

        # Check repository format version
        config = repo.get_config()
        self.assertEqual(config.get(("core",), "repositoryformatversion"), b"0")

        # Object format extension should not exist
        with self.assertRaises(KeyError):
            config.get(("extensions",), "objectformat")

        # Check hash algorithm detection
        self.assertEqual(repo.object_format, SHA1)

        repo.close()

    def test_format_version_validation(self):
        """Test format version validation for SHA256."""
        repo_path = os.path.join(self.test_dir, "invalid_repo")

        # SHA256 with format version 0 should fail
        with self.assertRaises(ValueError) as cm:
            Repo.init(repo_path, mkdir=True, format=0, object_format="sha256")
        self.assertIn("SHA256", str(cm.exception))

    def test_memory_repo_sha256(self):
        """Test SHA256 support in memory repository."""
        repo = MemoryRepo.init_bare([], {}, object_format="sha256")

        # Check hash algorithm
        self.assertEqual(repo.object_format, SHA256)


if __name__ == "__main__":
    unittest.main()
