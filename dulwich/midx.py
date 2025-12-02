# midx.py -- Multi-Pack-Index (MIDX) support
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

"""Multi-Pack-Index (MIDX) support.

A multi-pack-index (MIDX) provides a single index that covers multiple pack files,
enabling fast object lookup across all packs without opening each pack index.

The MIDX file format consists of:
- A header with signature, version, and hash algorithm
- A chunk lookup table
- Multiple chunks containing pack names, OID fanout, OID lookup, and object offsets
- A trailer with checksum

This module provides:
- Reading MIDX files
- Writing MIDX files
- Integration with pack-based object stores

Limitations:
- Incremental MIDX chains are not yet supported (base_midx_files must be 0)
- BTMP (bitmapped packfiles) chunk is not yet implemented
- RIDX (reverse index) chunk is not yet implemented

Note: Incremental MIDX chains were introduced in Git 2.47 as an experimental
feature, where multiple MIDX files can be chained together. The format includes
a base_midx_files field in the header and uses a multi-pack-index.d/ directory
with a multi-pack-index-chain file. This feature is not yet supported by Dulwich
as the specification is still evolving.
"""

__all__ = [
    "CHUNK_BTMP",
    "CHUNK_LOFF",
    "CHUNK_OIDF",
    "CHUNK_OIDL",
    "CHUNK_OOFF",
    "CHUNK_PNAM",
    "CHUNK_RIDX",
    "HASH_ALGORITHM_SHA1",
    "HASH_ALGORITHM_SHA256",
    "MIDX_SIGNATURE",
    "MIDX_VERSION",
    "MultiPackIndex",
    "load_midx",
    "load_midx_file",
    "write_midx",
]

import os
import struct
from collections.abc import Iterator
from io import UnsupportedOperation
from typing import IO, Any

try:
    import mmap
except ImportError:
    has_mmap = False
else:
    has_mmap = True

from .file import GitFile, _GitFile
from .objects import ObjectID, RawObjectID
from .pack import SHA1Writer

# MIDX signature
MIDX_SIGNATURE = b"MIDX"

# MIDX version
MIDX_VERSION = 1

# Chunk identifiers (4 bytes each)
CHUNK_PNAM = b"PNAM"  # Packfile names
CHUNK_OIDF = b"OIDF"  # OID fanout table
CHUNK_OIDL = b"OIDL"  # OID lookup table
CHUNK_OOFF = b"OOFF"  # Object offsets
CHUNK_LOFF = b"LOFF"  # Large offsets (optional)
CHUNK_BTMP = b"BTMP"  # Bitmapped packfiles (optional)
CHUNK_RIDX = b"RIDX"  # Reverse index (optional)

# Hash algorithm identifiers
HASH_ALGORITHM_SHA1 = 1
HASH_ALGORITHM_SHA256 = 2


class MultiPackIndex:
    """Multi-pack-index for efficient object lookup across multiple pack files."""

    def __init__(
        self,
        filename: str | os.PathLike[str],
        file: IO[bytes] | _GitFile | None = None,
        contents: bytes | None = None,
        size: int | None = None,
    ) -> None:
        """Initialize a MultiPackIndex.

        Args:
            filename: Path to the MIDX file
            file: Optional file object
            contents: Optional mmap'd contents
            size: Optional size of the MIDX file
        """
        self._filename = os.fspath(filename)
        self._file = file
        self._size = size

        # Instance variables that will be set during parsing
        self.version: int
        self.hash_algorithm: int
        self.hash_size: int
        self.chunk_count: int
        self.base_midx_files: int
        self.pack_count: int
        self.pack_names: list[str]
        self.object_count: int
        self._chunks: dict[bytes, int]
        self._fanout_table: list[int]
        self._oidl_offset: int
        self._ooff_offset: int
        self._loff_offset: int

        # Load file contents
        if contents is None:
            if file is None:
                with GitFile(filename, "rb") as f:
                    self._contents, self._size = self._load_file_contents(f, size)
            else:
                self._contents, self._size = self._load_file_contents(file, size)
        else:
            self._contents = contents

        # Parse header
        self._parse_header()

        # Parse chunk lookup table
        self._parse_chunk_table()

    def _load_file_contents(
        self, f: IO[bytes] | _GitFile, size: int | None = None
    ) -> tuple[bytes | Any, int]:
        """Load contents from a file, preferring mmap when possible.

        Args:
            f: File-like object to load
            size: Expected size, or None to determine from file

        Returns:
            Tuple of (contents, size)
        """
        try:
            fd = f.fileno()
        except (UnsupportedOperation, AttributeError):
            fd = None

        # Attempt to use mmap if possible
        if fd is not None:
            if size is None:
                size = os.fstat(fd).st_size
            if has_mmap:
                try:
                    contents = mmap.mmap(fd, size, access=mmap.ACCESS_READ)
                except (OSError, ValueError):
                    # Can't mmap - perhaps a socket or invalid file descriptor
                    pass
                else:
                    return contents, size

        # Fall back to reading entire file into memory
        contents_bytes = f.read()
        size = len(contents_bytes)
        return contents_bytes, size

    def _parse_header(self) -> None:
        """Parse the MIDX header."""
        if len(self._contents) < 12:
            raise ValueError("MIDX file too small")

        # Check signature
        signature = self._contents[0:4]
        if signature != MIDX_SIGNATURE:
            raise ValueError(f"Invalid MIDX signature: {signature!r}")

        # Read version
        self.version = self._contents[4]
        if self.version != MIDX_VERSION:
            raise ValueError(f"Unsupported MIDX version: {self.version}")

        # Read object ID version (hash algorithm)
        self.hash_algorithm = self._contents[5]
        if self.hash_algorithm == HASH_ALGORITHM_SHA1:
            self.hash_size = 20
        elif self.hash_algorithm == HASH_ALGORITHM_SHA256:
            self.hash_size = 32
        else:
            raise ValueError(f"Unknown hash algorithm: {self.hash_algorithm}")

        # Read chunk count
        self.chunk_count = self._contents[6]

        # Read base MIDX files count (currently always 0)
        self.base_midx_files = self._contents[7]
        if self.base_midx_files != 0:
            raise ValueError("Incremental MIDX not yet supported")

        # Read pack file count
        (self.pack_count,) = struct.unpack(">L", self._contents[8:12])

    def _parse_chunk_table(self) -> None:
        """Parse the chunk lookup table."""
        self._chunks = {}

        # Chunk table starts at offset 12
        offset = 12

        # Each chunk entry is 12 bytes (4-byte ID + 8-byte offset)
        for i in range(self.chunk_count + 1):  # +1 for terminator
            chunk_id = self._contents[offset : offset + 4]
            (chunk_offset,) = struct.unpack(
                ">Q", self._contents[offset + 4 : offset + 12]
            )

            if chunk_id == b"\x00\x00\x00\x00":
                # Terminator entry
                break

            self._chunks[chunk_id] = chunk_offset
            offset += 12

        # Parse required chunks
        self._parse_pnam_chunk()
        self._parse_oidf_chunk()
        self._parse_oidl_chunk()
        self._parse_ooff_chunk()

        # Parse optional chunks
        if CHUNK_LOFF in self._chunks:
            self._parse_loff_chunk()

    def _parse_pnam_chunk(self) -> None:
        """Parse the Packfile Names (PNAM) chunk."""
        if CHUNK_PNAM not in self._chunks:
            raise ValueError("Required PNAM chunk not found")

        offset = self._chunks[CHUNK_PNAM]
        self.pack_names = []

        # Find the end of the PNAM chunk (next chunk or end of chunks section)
        next_offset = min(
            (o for o in self._chunks.values() if o > offset),
            default=len(self._contents),
        )

        # Parse null-terminated pack names
        current = offset
        while current < next_offset:
            # Find the next null terminator
            null_pos = self._contents.find(b"\x00", current, next_offset)
            if null_pos == -1:
                break

            pack_name = self._contents[current:null_pos].decode("utf-8")
            if pack_name:  # Skip empty strings (padding)
                self.pack_names.append(pack_name)
            current = null_pos + 1

    def _parse_oidf_chunk(self) -> None:
        """Parse the OID Fanout (OIDF) chunk."""
        if CHUNK_OIDF not in self._chunks:
            raise ValueError("Required OIDF chunk not found")

        offset = self._chunks[CHUNK_OIDF]
        self._fanout_table = []

        # Read 256 4-byte entries
        for i in range(256):
            (count,) = struct.unpack(
                ">L", self._contents[offset + i * 4 : offset + i * 4 + 4]
            )
            self._fanout_table.append(count)

        # Total object count is the last entry
        self.object_count = self._fanout_table[255]

    def _parse_oidl_chunk(self) -> None:
        """Parse the OID Lookup (OIDL) chunk."""
        if CHUNK_OIDL not in self._chunks:
            raise ValueError("Required OIDL chunk not found")

        self._oidl_offset = self._chunks[CHUNK_OIDL]

    def _parse_ooff_chunk(self) -> None:
        """Parse the Object Offsets (OOFF) chunk."""
        if CHUNK_OOFF not in self._chunks:
            raise ValueError("Required OOFF chunk not found")

        self._ooff_offset = self._chunks[CHUNK_OOFF]

    def _parse_loff_chunk(self) -> None:
        """Parse the Large Offsets (LOFF) chunk."""
        self._loff_offset = self._chunks[CHUNK_LOFF]

    def __len__(self) -> int:
        """Return the number of objects in this MIDX."""
        return self.object_count

    def _get_oid(self, index: int) -> RawObjectID:
        """Get the object ID at the given index.

        Args:
            index: Index of the object

        Returns:
            Binary object ID
        """
        if index < 0 or index >= self.object_count:
            raise IndexError(f"Index {index} out of range")

        offset = self._oidl_offset + index * self.hash_size
        return RawObjectID(self._contents[offset : offset + self.hash_size])

    def _get_pack_info(self, index: int) -> tuple[int, int]:
        """Get pack ID and offset for object at the given index.

        Args:
            index: Index of the object

        Returns:
            Tuple of (pack_id, offset)
        """
        if index < 0 or index >= self.object_count:
            raise IndexError(f"Index {index} out of range")

        # Each entry is 8 bytes (4-byte pack ID + 4-byte offset)
        offset = self._ooff_offset + index * 8

        (pack_id,) = struct.unpack(">L", self._contents[offset : offset + 4])
        (pack_offset,) = struct.unpack(">L", self._contents[offset + 4 : offset + 8])

        # Check if this is a large offset (MSB set)
        if pack_offset & 0x80000000:
            # Look up in LOFF chunk
            if CHUNK_LOFF not in self._chunks:
                raise ValueError("Large offset found but no LOFF chunk")

            large_index = pack_offset & 0x7FFFFFFF
            large_offset_pos = self._loff_offset + large_index * 8
            (pack_offset,) = struct.unpack(
                ">Q", self._contents[large_offset_pos : large_offset_pos + 8]
            )

        return pack_id, pack_offset

    def object_offset(self, sha: ObjectID | RawObjectID) -> tuple[str, int] | None:
        """Return the pack name and offset for the given object.

        Args:
            sha: Binary SHA-1 or SHA-256 hash

        Returns:
            Tuple of (pack_name, offset) or None if not found
        """
        if len(sha) != self.hash_size:
            raise ValueError(
                f"SHA size mismatch: expected {self.hash_size}, got {len(sha)}"
            )

        # Use fanout table to narrow search range
        first_byte = sha[0]
        start_idx = 0 if first_byte == 0 else self._fanout_table[first_byte - 1]
        end_idx = self._fanout_table[first_byte]

        # Binary search within the range
        while start_idx < end_idx:
            mid = (start_idx + end_idx) // 2
            mid_sha = self._get_oid(mid)

            if mid_sha == sha:
                # Found it!
                pack_id, offset = self._get_pack_info(mid)
                return self.pack_names[pack_id], offset
            elif mid_sha < sha:
                start_idx = mid + 1
            else:
                end_idx = mid

        return None

    def __contains__(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if the given object SHA is in this MIDX.

        Args:
            sha: Binary SHA hash

        Returns:
            True if the object is in this MIDX
        """
        return self.object_offset(sha) is not None

    def iterentries(self) -> Iterator[tuple[RawObjectID, str, int]]:
        """Iterate over all entries in this MIDX.

        Yields:
            Tuples of (sha, pack_name, offset)
        """
        for i in range(self.object_count):
            sha = self._get_oid(i)
            pack_id, offset = self._get_pack_info(i)
            pack_name = self.pack_names[pack_id]
            yield sha, pack_name, offset

    def close(self) -> None:
        """Close the MIDX file and release mmap resources."""
        # Close mmap'd contents first if it's an mmap object
        if self._contents is not None and has_mmap:
            if isinstance(self._contents, mmap.mmap):
                self._contents.close()
        self._contents = None

        # Close file handle
        if self._file is not None:
            self._file.close()
            self._file = None


def load_midx(path: str | os.PathLike[str]) -> MultiPackIndex:
    """Load a multi-pack-index file by path.

    Args:
        path: Path to the MIDX file

    Returns:
        A MultiPackIndex loaded from the given path
    """
    with GitFile(path, "rb") as f:
        return load_midx_file(path, f)


def load_midx_file(
    path: str | os.PathLike[str], f: IO[bytes] | _GitFile
) -> MultiPackIndex:
    """Load a multi-pack-index from a file-like object.

    Args:
        path: Path for the MIDX file
        f: File-like object

    Returns:
        A MultiPackIndex loaded from the given file
    """
    return MultiPackIndex(path, file=f)


def write_midx(
    f: IO[bytes],
    pack_index_entries: list[tuple[str, list[tuple[RawObjectID, int, int | None]]]],
    hash_algorithm: int = HASH_ALGORITHM_SHA1,
) -> bytes:
    """Write a multi-pack-index file.

    Args:
        f: File-like object to write to
        pack_index_entries: List of (pack_name, entries) tuples where entries are
                          (sha, offset, crc32) tuples, sorted by SHA
        hash_algorithm: Hash algorithm to use (1=SHA-1, 2=SHA-256)

    Returns:
        SHA-1 checksum of the written MIDX file
    """
    if hash_algorithm == HASH_ALGORITHM_SHA1:
        hash_size = 20
    elif hash_algorithm == HASH_ALGORITHM_SHA256:
        hash_size = 32
    else:
        raise ValueError(f"Unknown hash algorithm: {hash_algorithm}")

    # Wrap file in SHA1Writer to compute checksum
    writer = SHA1Writer(f)

    # Sort pack entries by pack name (required by Git)
    pack_index_entries_sorted = sorted(pack_index_entries, key=lambda x: x[0])

    # Collect all objects from all packs
    all_objects: list[tuple[RawObjectID, int, int]] = []  # (sha, pack_id, offset)
    pack_names: list[str] = []

    for pack_id, (pack_name, entries) in enumerate(pack_index_entries_sorted):
        pack_names.append(pack_name)
        for sha, offset, _crc32 in entries:
            all_objects.append((sha, pack_id, offset))

    # Sort all objects by SHA
    all_objects.sort(key=lambda x: x[0])

    # Calculate offsets for chunks
    num_packs = len(pack_names)
    num_objects = len(all_objects)

    # Header: 12 bytes
    header_size = 12

    # Chunk count: PNAM, OIDF, OIDL, OOFF, and optionally LOFF
    # We'll determine if LOFF is needed later
    chunk_count = 4  # PNAM, OIDF, OIDL, OOFF

    # Check if we need LOFF chunk (for offsets >= 2^31)
    need_loff = any(offset >= 2**31 for _sha, _pack_id, offset in all_objects)
    if need_loff:
        chunk_count += 1

    # Chunk table: (chunk_count + 1) * 12 bytes (including terminator)
    chunk_table_size = (chunk_count + 1) * 12

    # Calculate chunk offsets
    current_offset = header_size + chunk_table_size

    # PNAM chunk: pack names as null-terminated strings, padded to 4-byte boundary
    pnam_data = b"".join(name.encode("utf-8") + b"\x00" for name in pack_names)
    # Pad to 4-byte boundary
    pnam_padding = (4 - len(pnam_data) % 4) % 4
    pnam_data += b"\x00" * pnam_padding
    pnam_offset = current_offset
    current_offset += len(pnam_data)

    # OIDF chunk: 256 * 4 bytes
    oidf_offset = current_offset
    oidf_size = 256 * 4
    current_offset += oidf_size

    # OIDL chunk: num_objects * hash_size bytes
    oidl_offset = current_offset
    oidl_size = num_objects * hash_size
    current_offset += oidl_size

    # OOFF chunk: num_objects * 8 bytes (4 for pack_id + 4 for offset)
    ooff_offset = current_offset
    ooff_size = num_objects * 8
    current_offset += ooff_size

    # LOFF chunk (if needed): variable size
    # We'll calculate the exact size when we know how many large offsets we have
    loff_offset = current_offset if need_loff else 0
    large_offsets: list[int] = []

    # Calculate trailer offset (where checksum starts)
    # We need to pre-calculate large offset count for accurate trailer offset
    if need_loff:
        # Count large offsets
        large_offset_count = sum(1 for _, _, offset in all_objects if offset >= 2**31)
        loff_size = large_offset_count * 8
        trailer_offset = current_offset + loff_size
    else:
        trailer_offset = current_offset

    # Write header
    writer.write(MIDX_SIGNATURE)  # 4 bytes: signature
    writer.write(bytes([MIDX_VERSION]))  # 1 byte: version
    writer.write(bytes([hash_algorithm]))  # 1 byte: hash algorithm
    writer.write(bytes([chunk_count]))  # 1 byte: chunk count
    writer.write(bytes([0]))  # 1 byte: base MIDX files (always 0)
    writer.write(struct.pack(">L", num_packs))  # 4 bytes: pack count

    # Write chunk table
    chunk_table = [
        (CHUNK_PNAM, pnam_offset),
        (CHUNK_OIDF, oidf_offset),
        (CHUNK_OIDL, oidl_offset),
        (CHUNK_OOFF, ooff_offset),
    ]
    if need_loff:
        chunk_table.append((CHUNK_LOFF, loff_offset))

    for chunk_id, chunk_offset in chunk_table:
        writer.write(chunk_id)  # 4 bytes
        writer.write(struct.pack(">Q", chunk_offset))  # 8 bytes

    # Write terminator (points to where trailer/checksum starts)
    writer.write(b"\x00\x00\x00\x00")  # 4 bytes
    writer.write(struct.pack(">Q", trailer_offset))  # 8 bytes

    # Write PNAM chunk
    writer.write(pnam_data)

    # Write OIDF chunk (fanout table)
    fanout: list[int] = [0] * 256
    for sha, _pack_id, _offset in all_objects:
        first_byte = sha[0]
        fanout[first_byte] += 1

    # Convert counts to cumulative
    cumulative = 0
    for i in range(256):
        cumulative += fanout[i]
        writer.write(struct.pack(">L", cumulative))

    # Write OIDL chunk (object IDs)
    for sha, _pack_id, _offset in all_objects:
        writer.write(sha)

    # Write OOFF chunk (pack ID and offset for each object)
    for _sha, pack_id, offset in all_objects:
        writer.write(struct.pack(">L", pack_id))

        if offset >= 2**31:
            # Use large offset table
            large_offset_index = len(large_offsets)
            large_offsets.append(offset)
            # Set MSB to indicate large offset
            writer.write(struct.pack(">L", 0x80000000 | large_offset_index))
        else:
            writer.write(struct.pack(">L", offset))

    # Write LOFF chunk if needed
    if need_loff:
        for large_offset in large_offsets:
            writer.write(struct.pack(">Q", large_offset))

    # Write checksum
    return writer.write_sha()


def write_midx_file(
    path: str | os.PathLike[str],
    pack_index_entries: list[tuple[str, list[tuple[RawObjectID, int, int | None]]]],
    hash_algorithm: int = HASH_ALGORITHM_SHA1,
) -> bytes:
    """Write a multi-pack-index file to disk.

    Args:
        path: Path where to write the MIDX file
        pack_index_entries: List of (pack_name, entries) tuples where entries are
                          (sha, offset, crc32) tuples, sorted by SHA
        hash_algorithm: Hash algorithm to use (1=SHA-1, 2=SHA-256)

    Returns:
        SHA-1 checksum of the written MIDX file
    """
    with GitFile(path, "wb") as f:
        return write_midx(f, pack_index_entries, hash_algorithm)


# TODO: Add support for incremental MIDX chains
# TODO: Add support for BTMP and RIDX chunks for bitmap integration
