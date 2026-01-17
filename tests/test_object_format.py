# test_object_format.py -- tests for object_format.py
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

"""Tests for object_format module."""

from dulwich.object_format import (
    DEFAULT_OBJECT_FORMAT,
    OBJECT_FORMAT_TYPE_NUMS,
    OBJECT_FORMATS,
    SHA1,
    SHA256,
    get_object_format,
    verify_same_object_format,
)

from . import TestCase


class ObjectFormatTests(TestCase):
    """Tests for ObjectFormat class."""

    def test_sha1_attributes(self) -> None:
        """Test SHA1 object format attributes."""
        self.assertEqual("sha1", SHA1.name)
        self.assertEqual(1, SHA1.type_num)
        self.assertEqual(20, SHA1.oid_length)
        self.assertEqual(40, SHA1.hex_length)

    def test_sha256_attributes(self) -> None:
        """Test SHA256 object format attributes."""
        self.assertEqual("sha256", SHA256.name)
        self.assertEqual(20, SHA256.type_num)
        self.assertEqual(32, SHA256.oid_length)
        self.assertEqual(64, SHA256.hex_length)

    def test_str_representation(self) -> None:
        """Test __str__ method returns format name."""
        self.assertEqual("sha1", str(SHA1))
        self.assertEqual("sha256", str(SHA256))

    def test_repr_representation(self) -> None:
        """Test __repr__ method."""
        self.assertEqual("ObjectFormat('sha1')", repr(SHA1))
        self.assertEqual("ObjectFormat('sha256')", repr(SHA256))

    def test_new_hash(self) -> None:
        """Test new_hash creates a hash object."""
        sha1_hash = SHA1.new_hash()
        self.assertEqual("sha1", sha1_hash.name)

        sha256_hash = SHA256.new_hash()
        self.assertEqual("sha256", sha256_hash.name)

    def test_hash_object(self) -> None:
        """Test hash_object returns binary digest."""
        data = b"test data"

        # Test SHA1
        sha1_digest = SHA1.hash_object(data)
        self.assertIsInstance(sha1_digest, bytes)
        self.assertEqual(20, len(sha1_digest))

        # Verify it matches expected hash
        import hashlib

        expected_sha1 = hashlib.sha1(data).digest()
        self.assertEqual(expected_sha1, sha1_digest)

        # Test SHA256
        sha256_digest = SHA256.hash_object(data)
        self.assertIsInstance(sha256_digest, bytes)
        self.assertEqual(32, len(sha256_digest))

        expected_sha256 = hashlib.sha256(data).digest()
        self.assertEqual(expected_sha256, sha256_digest)

    def test_hash_object_hex(self) -> None:
        """Test hash_object_hex returns hexadecimal digest."""
        data = b"test data"

        # Test SHA1
        sha1_hex = SHA1.hash_object_hex(data)
        self.assertIsInstance(sha1_hex, bytes)
        self.assertEqual(40, len(sha1_hex))

        # Verify it matches expected hash
        import hashlib

        expected_sha1_hex = hashlib.sha1(data).hexdigest().encode("ascii")
        self.assertEqual(expected_sha1_hex, sha1_hex)

        # Test SHA256
        sha256_hex = SHA256.hash_object_hex(data)
        self.assertIsInstance(sha256_hex, bytes)
        self.assertEqual(64, len(sha256_hex))

        expected_sha256_hex = hashlib.sha256(data).hexdigest().encode("ascii")
        self.assertEqual(expected_sha256_hex, sha256_hex)


class ObjectFormatMappingTests(TestCase):
    """Tests for object format mappings."""

    def test_object_formats_dict(self) -> None:
        """Test OBJECT_FORMATS dictionary."""
        self.assertEqual(SHA1, OBJECT_FORMATS["sha1"])
        self.assertEqual(SHA256, OBJECT_FORMATS["sha256"])

    def test_object_format_type_nums_dict(self) -> None:
        """Test OBJECT_FORMAT_TYPE_NUMS dictionary."""
        self.assertEqual(SHA1, OBJECT_FORMAT_TYPE_NUMS[1])
        self.assertEqual(SHA256, OBJECT_FORMAT_TYPE_NUMS[2])

    def test_default_object_format(self) -> None:
        """Test DEFAULT_OBJECT_FORMAT is SHA1."""
        self.assertEqual(SHA1, DEFAULT_OBJECT_FORMAT)


class GetObjectFormatTests(TestCase):
    """Tests for get_object_format function."""

    def test_get_object_format_none(self) -> None:
        """Test get_object_format with None returns default."""
        result = get_object_format(None)
        self.assertEqual(DEFAULT_OBJECT_FORMAT, result)

    def test_get_object_format_sha1(self) -> None:
        """Test get_object_format with 'sha1'."""
        result = get_object_format("sha1")
        self.assertEqual(SHA1, result)

    def test_get_object_format_sha256(self) -> None:
        """Test get_object_format with 'sha256'."""
        result = get_object_format("sha256")
        self.assertEqual(SHA256, result)

    def test_get_object_format_case_insensitive(self) -> None:
        """Test get_object_format is case insensitive."""
        self.assertEqual(SHA1, get_object_format("SHA1"))
        self.assertEqual(SHA1, get_object_format("Sha1"))
        self.assertEqual(SHA256, get_object_format("SHA256"))
        self.assertEqual(SHA256, get_object_format("Sha256"))

    def test_get_object_format_invalid(self) -> None:
        """Test get_object_format with invalid name raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            get_object_format("md5")

        self.assertEqual("Unsupported object format: md5", str(cm.exception))


class VerifySameObjectFormatTests(TestCase):
    """Tests for verify_same_object_format function."""

    def test_verify_single_format(self) -> None:
        """Test verify_same_object_format with single format."""
        result = verify_same_object_format(SHA1)
        self.assertEqual(SHA1, result)

    def test_verify_multiple_same_formats(self) -> None:
        """Test verify_same_object_format with multiple same formats."""
        result = verify_same_object_format(SHA1, SHA1, SHA1)
        self.assertEqual(SHA1, result)

        result = verify_same_object_format(SHA256, SHA256)
        self.assertEqual(SHA256, result)

    def test_verify_no_formats(self) -> None:
        """Test verify_same_object_format with no formats raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            verify_same_object_format()

        self.assertEqual(
            "At least one object format must be provided", str(cm.exception)
        )

    def test_verify_different_formats(self) -> None:
        """Test verify_same_object_format with different formats raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            verify_same_object_format(SHA1, SHA256)

        self.assertEqual("Object format mismatch: sha1 != sha256", str(cm.exception))

    def test_verify_multiple_different_formats(self) -> None:
        """Test verify_same_object_format fails on first mismatch."""
        with self.assertRaises(ValueError) as cm:
            verify_same_object_format(SHA1, SHA1, SHA256, SHA1)

        self.assertEqual("Object format mismatch: sha1 != sha256", str(cm.exception))
