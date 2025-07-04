# lfs.py -- Implementation of the LFS
# Copyright (C) 2020 Jelmer Vernooij
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

import hashlib
import os
import tempfile
from collections.abc import Iterable
from typing import TYPE_CHECKING, BinaryIO, Optional

if TYPE_CHECKING:
    from .repo import Repo


class LFSStore:
    """Stores objects on disk, indexed by SHA256."""

    def __init__(self, path: str) -> None:
        self.path = path

    @classmethod
    def create(cls, lfs_dir: str) -> "LFSStore":
        if not os.path.isdir(lfs_dir):
            os.mkdir(lfs_dir)
        os.mkdir(os.path.join(lfs_dir, "tmp"))
        os.mkdir(os.path.join(lfs_dir, "objects"))
        return cls(lfs_dir)

    @classmethod
    def from_repo(cls, repo: "Repo", create: bool = False) -> "LFSStore":
        lfs_dir = os.path.join(repo.controldir(), "lfs")
        if create:
            return cls.create(lfs_dir)
        return cls(lfs_dir)

    def _sha_path(self, sha: str) -> str:
        return os.path.join(self.path, "objects", sha[0:2], sha[2:4], sha)

    def open_object(self, sha: str) -> BinaryIO:
        """Open an object by sha."""
        try:
            return open(self._sha_path(sha), "rb")
        except FileNotFoundError as exc:
            raise KeyError(sha) from exc

    def write_object(self, chunks: Iterable[bytes]) -> str:
        """Write an object.

        Returns: object SHA
        """
        sha = hashlib.sha256()
        tmpdir = os.path.join(self.path, "tmp")
        with tempfile.NamedTemporaryFile(dir=tmpdir, mode="wb", delete=False) as f:
            for chunk in chunks:
                sha.update(chunk)
                f.write(chunk)
            f.flush()
            tmppath = f.name
        path = self._sha_path(sha.hexdigest())
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        os.rename(tmppath, path)
        return sha.hexdigest()


class LFSPointer:
    """Represents an LFS pointer file."""

    def __init__(self, oid: str, size: int) -> None:
        self.oid = oid
        self.size = size

    @classmethod
    def from_bytes(cls, data: bytes) -> Optional["LFSPointer"]:
        """Parse LFS pointer from bytes.

        Returns None if data is not a valid LFS pointer.
        """
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return None

        # LFS pointer files have a specific format
        lines = text.strip().split("\n")
        if len(lines) < 3:
            return None

        # Must start with version
        if not lines[0].startswith("version https://git-lfs.github.com/spec/v1"):
            return None

        oid = None
        size = None

        for line in lines[1:]:
            if line.startswith("oid sha256:"):
                oid = line[11:].strip()
            elif line.startswith("size "):
                try:
                    size = int(line[5:].strip())
                except ValueError:
                    return None

        if oid is None or size is None:
            return None

        return cls(oid, size)

    def to_bytes(self) -> bytes:
        """Convert LFS pointer to bytes."""
        return (
            f"version https://git-lfs.github.com/spec/v1\n"
            f"oid sha256:{self.oid}\n"
            f"size {self.size}\n"
        ).encode()

    def is_valid_oid(self) -> bool:
        """Check if the OID is valid SHA256."""
        if len(self.oid) != 64:
            return False
        try:
            int(self.oid, 16)
            return True
        except ValueError:
            return False


class LFSFilterDriver:
    """LFS filter driver implementation."""

    def __init__(self, lfs_store: "LFSStore") -> None:
        self.lfs_store = lfs_store

    def clean(self, data: bytes) -> bytes:
        """Convert file content to LFS pointer (clean filter)."""
        # Check if data is already an LFS pointer
        pointer = LFSPointer.from_bytes(data)
        if pointer is not None:
            return data

        # Store the file content in LFS
        sha = self.lfs_store.write_object([data])

        # Create and return LFS pointer
        pointer = LFSPointer(sha, len(data))
        return pointer.to_bytes()

    def smudge(self, data: bytes) -> bytes:
        """Convert LFS pointer to file content (smudge filter)."""
        # Try to parse as LFS pointer
        pointer = LFSPointer.from_bytes(data)
        if pointer is None:
            # Not an LFS pointer, return as-is
            return data

        # Validate the pointer
        if not pointer.is_valid_oid():
            return data

        try:
            # Read the actual content from LFS store
            with self.lfs_store.open_object(pointer.oid) as f:
                return f.read()
        except KeyError:
            # Object not found in LFS store, return pointer as-is
            # This matches Git LFS behavior when object is missing
            return data
