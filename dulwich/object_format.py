# hash.py -- Object format abstraction layer for Git
# Copyright (C) 2024 The Dulwich contributors
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

"""Object format abstraction for Git objects.

This module provides an abstraction layer for different object formats
used in Git repositories (SHA-1 and SHA-256).
"""

from collections.abc import Callable
from hashlib import sha1, sha256
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _hashlib import HASH


class ObjectFormat:
    """Object format (hash algorithm) used in Git."""

    def __init__(
        self,
        name: str,
        type_num: int,
        oid_length: int,
        hex_length: int,
        hash_func: Callable[[], "HASH"],
    ) -> None:
        """Initialize an object format.

        Args:
            name: Name of the format (e.g., "sha1", "sha256")
            type_num: Format type number used in Git
            oid_length: Length of the binary object ID in bytes
            hex_length: Length of the hexadecimal object ID in characters
            hash_func: Hash function from hashlib
        """
        self.name = name
        self.type_num = type_num
        self.oid_length = oid_length
        self.hex_length = hex_length
        self.hash_func = hash_func

    def __str__(self) -> str:
        """Return string representation."""
        return self.name

    def __repr__(self) -> str:
        """Return repr."""
        return f"ObjectFormat({self.name!r})"

    def new_hash(self) -> "HASH":
        """Create a new hash object."""
        return self.hash_func()

    def hash_object(self, data: bytes) -> bytes:
        """Hash data and return the digest.

        Args:
            data: Data to hash

        Returns:
            Binary digest
        """
        h = self.new_hash()
        h.update(data)
        return h.digest()

    def hash_object_hex(self, data: bytes) -> bytes:
        """Hash data and return the hexadecimal digest.

        Args:
            data: Data to hash

        Returns:
            Hexadecimal digest as bytes
        """
        h = self.new_hash()
        h.update(data)
        return h.hexdigest().encode("ascii")


# Define the supported object formats
SHA1 = ObjectFormat("sha1", type_num=1, oid_length=20, hex_length=40, hash_func=sha1)
SHA256 = ObjectFormat(
    "sha256", type_num=20, oid_length=32, hex_length=64, hash_func=sha256
)

# Map of format names to ObjectFormat instances
OBJECT_FORMATS = {
    "sha1": SHA1,
    "sha256": SHA256,
}

# Map of format numbers to ObjectFormat instances
OBJECT_FORMAT_TYPE_NUMS = {
    1: SHA1,
    2: SHA256,
}

# Default format for backward compatibility
DEFAULT_OBJECT_FORMAT = SHA1


def get_object_format(name: str | None = None) -> ObjectFormat:
    """Get an object format by name.

    Args:
        name: Format name ("sha1" or "sha256"). If None, returns default.

    Returns:
        ObjectFormat instance

    Raises:
        ValueError: If the format name is not supported
    """
    if name is None:
        return DEFAULT_OBJECT_FORMAT
    try:
        return OBJECT_FORMATS[name.lower()]
    except KeyError:
        raise ValueError(f"Unsupported object format: {name}")


def verify_same_object_format(*formats: ObjectFormat) -> ObjectFormat:
    """Verify that all provided object formats are the same.

    Args:
        *formats: Object format instances to verify

    Returns:
        The common object format

    Raises:
        ValueError: If formats don't match or no formats provided
    """
    if not formats:
        raise ValueError("At least one object format must be provided")

    first = formats[0]
    for fmt in formats[1:]:
        if fmt != first:
            raise ValueError(f"Object format mismatch: {first.name} != {fmt.name}")

    return first
