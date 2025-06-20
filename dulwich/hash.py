# hash.py -- Hash algorithm abstraction layer for Git
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

"""Hash algorithm abstraction for Git objects.

This module provides an abstraction layer for different hash algorithms
used in Git repositories (SHA-1 and SHA-256).
"""

from hashlib import sha1, sha256
from typing import Callable, Optional


class HashAlgorithm:
    """Base class for hash algorithms used in Git."""

    def __init__(
        self, name: str, oid_length: int, hex_length: int, hash_func: Callable
    ) -> None:
        """Initialize a hash algorithm.

        Args:
            name: Name of the algorithm (e.g., "sha1", "sha256")
            oid_length: Length of the binary object ID in bytes
            hex_length: Length of the hexadecimal object ID in characters
            hash_func: Hash function from hashlib
        """
        self.name = name
        self.oid_length = oid_length
        self.hex_length = hex_length
        self.hash_func = hash_func
        self.zero_oid = b"0" * hex_length
        self.zero_oid_bin = b"\x00" * oid_length

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"HashAlgorithm({self.name!r})"

    def new_hash(self):
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


# Define the supported hash algorithms
SHA1 = HashAlgorithm("sha1", 20, 40, sha1)
SHA256 = HashAlgorithm("sha256", 32, 64, sha256)

# Map of algorithm names to HashAlgorithm instances
HASH_ALGORITHMS = {
    "sha1": SHA1,
    "sha256": SHA256,
}

# Default algorithm for backward compatibility
DEFAULT_HASH_ALGORITHM = SHA1


def get_hash_algorithm(name: Optional[str] = None) -> HashAlgorithm:
    """Get a hash algorithm by name.

    Args:
        name: Algorithm name ("sha1" or "sha256"). If None, returns default.

    Returns:
        HashAlgorithm instance

    Raises:
        ValueError: If the algorithm name is not supported
    """
    if name is None:
        return DEFAULT_HASH_ALGORITHM
    try:
        return HASH_ALGORITHMS[name.lower()]
    except KeyError:
        raise ValueError(f"Unsupported hash algorithm: {name}")
