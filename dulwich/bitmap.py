# bitmap.py -- Packfile bitmap support for git
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

"""Support for Git packfile bitmaps.

Bitmaps store reachability information for packfiles, enabling faster
object counting and enumeration operations without full graph traversal.

The bitmap format uses EWAH (Enhanced Word-Aligned Hybrid) compression
for efficient storage and fast bitwise operations.
"""

import os
import struct
from collections.abc import Iterator
from io import BytesIO
from typing import IO, TYPE_CHECKING, Optional, Union

from .file import GitFile

if TYPE_CHECKING:
    from .pack import PackIndex

# Bitmap file signature
BITMAP_SIGNATURE = b"BITM"

# Bitmap format version
BITMAP_VERSION = 1

# Bitmap flags
BITMAP_OPT_FULL_DAG = 0x1  # Full closure
BITMAP_OPT_HASH_CACHE = 0x4  # Name-hash cache
BITMAP_OPT_LOOKUP_TABLE = 0x10  # Lookup table for random access
BITMAP_OPT_PSEUDO_MERGES = 0x20  # Pseudo-merge bitmaps


def _encode_ewah_words(words: list[int]) -> list[int]:
    """Encode a list of 64-bit words using EWAH run-length compression.

    Args:
        words: List of 64-bit words to encode

    Returns:
        List of compressed words (RLWs followed by literals)
    """
    compressed_words = []
    i = 0

    while i < len(words):
        # Check for runs of all zeros or all ones
        if words[i] == 0 or words[i] == 0xFFFFFFFFFFFFFFFF:
            # Count consecutive identical words
            run_value = words[i]
            run_length = 0
            while i < len(words) and words[i] == run_value:
                run_length += 1
                i += 1

            # Collect following literal words
            literals = []
            while i < len(words) and words[i] != 0 and words[i] != 0xFFFFFFFFFFFFFFFF:
                literals.append(words[i])
                i += 1
                if len(literals) >= 0x7FFFFFFF:  # Max literal count in RLW
                    break

            # Create RLW with correct bit layout:
            # [literal_words(31 bits)][running_len(32 bits)][running_bit(1 bit)]
            running_bit = 1 if run_value == 0xFFFFFFFFFFFFFFFF else 0
            rlw = (len(literals) << 33) | (run_length << 1) | running_bit
            compressed_words.append(rlw)
            compressed_words.extend(literals)
        else:
            # Collect literal words
            literals = []
            while i < len(words) and words[i] != 0 and words[i] != 0xFFFFFFFFFFFFFFFF:
                literals.append(words[i])
                i += 1
                if len(literals) >= 0x7FFFFFFF:  # Max literal count
                    break

            # RLW with no run, just literals
            # [literal_words(31 bits)][running_len(32 bits)][running_bit(1 bit)]
            rlw = (len(literals) << 33) | (0 << 1) | 0
            compressed_words.append(rlw)
            compressed_words.extend(literals)

    return compressed_words


class EWAHBitmap:
    """EWAH (Enhanced Word-Aligned Hybrid) compressed bitmap.

    EWAH uses run-length encoding for efficient bitmap storage.
    Each bitmap consists of:
    - Uncompressed bit count (4 bytes)
    - Compressed word count (4 bytes)
    - Compressed words (8 bytes each)
    - Current RLW position (4 bytes)

    Each Run Length Word (RLW) 64-bit layout (LSB to MSB):
    - Bit 0: running_bit (1 bit) - value of repeated words (0 or 1)
    - Bits 1-32: running_len (32 bits) - count of repeated words
    - Bits 33-63: literal_words (31 bits) - count of literal words following this RLW
    """

    def __init__(self, data: Optional[bytes] = None) -> None:
        """Initialize EWAH bitmap.

        Args:
            data: Optional compressed bitmap data to decode
        """
        self.bits: set[int] = set()
        self.bit_count = 0

        if data:
            self._decode(data)

    def _decode(self, data: bytes) -> None:
        """Decode EWAH compressed bitmap data.

        Args:
            data: Compressed bitmap data (EWAH format with header + words + RLW position)
        """
        f = BytesIO(data)

        # Read header
        bit_count_bytes = f.read(4)
        word_count_bytes = f.read(4)

        if len(bit_count_bytes) < 4 or len(word_count_bytes) < 4:
            return

        bit_count = struct.unpack(">I", bit_count_bytes)[0]
        word_count = struct.unpack(">I", word_count_bytes)[0]

        self.bit_count = bit_count
        current_bit = 0

        # Read all words first
        words = []
        for _ in range(word_count):
            word_bytes = f.read(8)
            if len(word_bytes) < 8:
                break
            word = struct.unpack(">Q", word_bytes)[0]
            words.append(word)

        # Process EWAH chunks: RLW followed by literal words
        idx = 0
        while idx < len(words):
            # This is an RLW
            # Bit layout: [literal_words(31 bits)][running_len(32 bits)][running_bit(1 bit)]
            rlw = words[idx]
            running_bit = rlw & 1
            running_len = (rlw >> 1) & 0xFFFFFFFF
            literal_words = rlw >> 33
            idx += 1

            # Process running bits
            if running_len > 0:
                if running_bit == 1:
                    # Add all bits in the repeated section
                    for i in range(running_len * 64):
                        self.bits.add(current_bit + i)
                current_bit += running_len * 64

            # Process literal words
            for _ in range(literal_words):
                if idx >= len(words):
                    break

                literal = words[idx]
                idx += 1

                # Extract set bits from literal word
                for i in range(64):
                    if literal & (1 << i):
                        self.bits.add(current_bit + i)
                current_bit += 64

        # Read RLW position (we don't use it currently, but it's part of the format)
        f.read(4)

    def encode(self) -> bytes:
        """Encode bitmap to EWAH compressed format.

        Returns:
            Compressed bitmap data including header, words, and RLW position
        """
        if not self.bits:
            # Empty bitmap: bit_count=0, word_count=0, rlw_pos=0
            return struct.pack(">III", 0, 0, 0)

        max_bit = max(self.bits) if self.bits else 0
        bit_count = max_bit + 1
        word_count = (bit_count + 63) // 64

        # Create literal words
        words = [0] * word_count
        for bit in self.bits:
            word_idx = bit // 64
            bit_idx = bit % 64
            words[word_idx] |= 1 << bit_idx

        # Compress using EWAH run-length encoding
        compressed_words = _encode_ewah_words(words)

        # Build EWAH data
        f = BytesIO()

        # Header
        f.write(struct.pack(">I", bit_count))
        f.write(struct.pack(">I", len(compressed_words)))

        # Write compressed words
        for word in compressed_words:
            f.write(struct.pack(">Q", word))

        # Write RLW position (position of last RLW in the compressed words)
        # For now, we'll use 0 as we don't track this during encoding
        # This could be improved in the future if needed
        f.write(struct.pack(">I", 0))

        return f.getvalue()

    def __contains__(self, bit: int) -> bool:
        """Check if a bit is set.

        Args:
            bit: Bit position to check

        Returns:
            True if bit is set, False otherwise
        """
        return bit in self.bits

    def __len__(self) -> int:
        """Return the number of set bits.

        Returns:
            Count of set bits
        """
        return len(self.bits)

    def __or__(self, other: "EWAHBitmap") -> "EWAHBitmap":
        """Bitwise OR operation.

        Args:
            other: Other bitmap to OR with

        Returns:
            New bitmap with OR result
        """
        result = EWAHBitmap()
        result.bits = self.bits | other.bits
        result.bit_count = max(self.bit_count, other.bit_count)
        return result

    def __and__(self, other: "EWAHBitmap") -> "EWAHBitmap":
        """Bitwise AND operation.

        Args:
            other: Other bitmap to AND with

        Returns:
            New bitmap with AND result
        """
        result = EWAHBitmap()
        result.bits = self.bits & other.bits
        result.bit_count = max(self.bit_count, other.bit_count)
        return result

    def __xor__(self, other: "EWAHBitmap") -> "EWAHBitmap":
        """Bitwise XOR operation.

        Args:
            other: Other bitmap to XOR with

        Returns:
            New bitmap with XOR result
        """
        result = EWAHBitmap()
        result.bits = self.bits ^ other.bits
        result.bit_count = max(self.bit_count, other.bit_count)
        return result

    def add(self, bit: int) -> None:
        """Set a bit.

        Args:
            bit: Bit position to set
        """
        self.bits.add(bit)
        self.bit_count = max(self.bit_count, bit + 1)


class BitmapEntry:
    """A single bitmap entry for a commit."""

    def __init__(
        self,
        object_pos: int,
        xor_offset: int,
        flags: int,
        bitmap: EWAHBitmap,
    ) -> None:
        """Initialize a bitmap entry.

        Args:
            object_pos: Position of object in pack index
            xor_offset: XOR offset for compression (0-160)
            flags: Entry flags
            bitmap: The EWAH bitmap data
        """
        self.object_pos = object_pos
        self.xor_offset = xor_offset
        self.flags = flags
        self.bitmap = bitmap


class PackBitmap:
    """A pack bitmap index.

    Bitmaps store reachability information for commits in a packfile,
    allowing fast object enumeration without graph traversal.
    """

    def __init__(
        self,
        version: int = BITMAP_VERSION,
        flags: int = BITMAP_OPT_FULL_DAG,
    ) -> None:
        """Initialize a pack bitmap.

        Args:
            version: Bitmap format version
            flags: Bitmap flags
        """
        self.version = version
        self.flags = flags
        self.pack_checksum: Optional[bytes] = None

        # Type bitmaps for commits, trees, blobs, tags
        self.commit_bitmap = EWAHBitmap()
        self.tree_bitmap = EWAHBitmap()
        self.blob_bitmap = EWAHBitmap()
        self.tag_bitmap = EWAHBitmap()

        # Bitmap entries indexed by commit SHA
        self.entries: dict[bytes, BitmapEntry] = {}

        # List of entries in order (for XOR offset resolution)
        self.entries_list: list[tuple[bytes, BitmapEntry]] = []

        # Optional lookup table for random access
        self.lookup_table: Optional[list[tuple[int, int, int]]] = None

        # Optional name-hash cache
        self.name_hash_cache: Optional[list[int]] = None

    def get_bitmap(self, commit_sha: bytes) -> Optional[EWAHBitmap]:
        """Get the bitmap for a commit.

        Args:
            commit_sha: SHA-1 of the commit

        Returns:
            EWAH bitmap or None if not found
        """
        entry = self.entries.get(commit_sha)
        if entry is None:
            return None

        # Decompress using XOR if needed
        if entry.xor_offset > 0:
            # Find the entry at the XOR offset
            # The XOR offset tells us how many entries back to look
            # We need to find this entry in the ordered list
            try:
                current_idx = next(
                    i
                    for i, (sha, _) in enumerate(self.entries_list)
                    if sha == commit_sha
                )
            except StopIteration:
                # Entry not found in list, return as-is
                return entry.bitmap

            # XOR offset is how many positions back to look (max 160)
            if current_idx >= entry.xor_offset:
                base_sha, _base_entry = self.entries_list[
                    current_idx - entry.xor_offset
                ]
                # Get the base bitmap (recursively if it also uses XOR)
                base_bitmap = self.get_bitmap(base_sha)
                if base_bitmap is not None:
                    # XOR the current bitmap with the base
                    return entry.bitmap ^ base_bitmap

        return entry.bitmap

    def has_commit(self, commit_sha: bytes) -> bool:
        """Check if a commit has a bitmap.

        Args:
            commit_sha: SHA-1 of the commit

        Returns:
            True if bitmap exists for this commit
        """
        return commit_sha in self.entries

    def iter_commits(self) -> Iterator[bytes]:
        """Iterate over all commits with bitmaps.

        Returns:
            Iterator of commit SHAs
        """
        return iter(self.entries.keys())


def read_bitmap(
    filename: Union[str, os.PathLike[str]],
    pack_index: Optional["PackIndex"] = None,
) -> PackBitmap:
    """Read a bitmap index file.

    Args:
        filename: Path to the .bitmap file
        pack_index: Optional PackIndex to resolve object positions to SHAs

    Returns:
        Loaded PackBitmap

    Raises:
        ValueError: If file format is invalid
        ChecksumMismatch: If checksum verification fails
    """
    with GitFile(filename, "rb") as f:
        return read_bitmap_file(f, pack_index=pack_index)


def read_bitmap_file(
    f: IO[bytes], pack_index: Optional["PackIndex"] = None
) -> PackBitmap:
    """Read bitmap data from a file object.

    Args:
        f: File object to read from
        pack_index: Optional PackIndex to resolve object positions to SHAs

    Returns:
        Loaded PackBitmap

    Raises:
        ValueError: If file format is invalid
    """
    # Read header
    signature = f.read(4)
    if signature != BITMAP_SIGNATURE:
        raise ValueError(
            f"Invalid bitmap signature: {signature!r}, expected {BITMAP_SIGNATURE!r}"
        )

    version_bytes = f.read(2)
    flags_bytes = f.read(2)

    if len(version_bytes) < 2 or len(flags_bytes) < 2:
        raise ValueError("Incomplete bitmap header")

    version = struct.unpack(">H", version_bytes)[0]
    flags = struct.unpack(">H", flags_bytes)[0]

    if version != BITMAP_VERSION:
        raise ValueError(f"Unsupported bitmap version: {version}")

    # Read entry count
    entry_count_bytes = f.read(4)
    if len(entry_count_bytes) < 4:
        raise ValueError("Missing entry count")
    entry_count = struct.unpack(">I", entry_count_bytes)[0]

    # Read pack checksum
    pack_checksum = f.read(20)
    if len(pack_checksum) < 20:
        raise ValueError("Missing pack checksum")

    bitmap = PackBitmap(version=version, flags=flags)
    bitmap.pack_checksum = pack_checksum

    # Read type bitmaps (EWAH bitmaps are self-describing)
    for i, type_bitmap in enumerate(
        [
            bitmap.commit_bitmap,
            bitmap.tree_bitmap,
            bitmap.blob_bitmap,
            bitmap.tag_bitmap,
        ]
    ):
        # EWAH format:
        # 4 bytes: bit count
        # 4 bytes: word count
        # N x 8 bytes: compressed words
        # 4 bytes: RLW position

        # Read header to determine size
        bit_count_bytes = f.read(4)
        word_count_bytes = f.read(4)

        if len(bit_count_bytes) < 4 or len(word_count_bytes) < 4:
            raise ValueError(f"Missing type bitmap {i} header")

        word_count = struct.unpack(">I", word_count_bytes)[0]

        # Read compressed words
        words_data = f.read(word_count * 8)
        if len(words_data) < word_count * 8:
            raise ValueError(f"Incomplete type bitmap {i} data")

        # Read RLW position
        rlw_pos_bytes = f.read(4)
        if len(rlw_pos_bytes) < 4:
            raise ValueError(f"Missing type bitmap {i} RLW position")

        # Reconstruct the full EWAH data to pass to _decode
        ewah_data = bit_count_bytes + word_count_bytes + words_data + rlw_pos_bytes
        type_bitmap._decode(ewah_data)

    # Read bitmap entries
    for _ in range(entry_count):
        # Read object position (4 bytes)
        obj_pos_bytes = f.read(4)
        if len(obj_pos_bytes) < 4:
            raise ValueError("Incomplete bitmap entry")
        obj_pos = struct.unpack(">I", obj_pos_bytes)[0]

        # Read XOR offset (1 byte)
        xor_offset_bytes = f.read(1)
        if len(xor_offset_bytes) < 1:
            raise ValueError("Missing XOR offset")
        xor_offset = xor_offset_bytes[0]

        # Read flags (1 byte)
        flags_bytes = f.read(1)
        if len(flags_bytes) < 1:
            raise ValueError("Missing entry flags")
        entry_flags = flags_bytes[0]

        # Read self-describing EWAH bitmap
        # EWAH format: bit_count (4) + word_count (4) + words (word_count * 8) + rlw_pos (4)
        bit_count_bytes = f.read(4)
        word_count_bytes = f.read(4)

        if len(bit_count_bytes) < 4 or len(word_count_bytes) < 4:
            raise ValueError("Incomplete bitmap entry EWAH header")

        word_count = struct.unpack(">I", word_count_bytes)[0]

        # Read compressed words
        words_data = f.read(word_count * 8)
        if len(words_data) < word_count * 8:
            raise ValueError("Incomplete bitmap entry EWAH words")

        # Read RLW position
        rlw_pos_bytes = f.read(4)
        if len(rlw_pos_bytes) < 4:
            raise ValueError("Missing bitmap entry EWAH RLW position")

        # Reconstruct full EWAH data
        bitmap_data = bit_count_bytes + word_count_bytes + words_data + rlw_pos_bytes

        # Create bitmap entry
        ewah_bitmap = EWAHBitmap(bitmap_data) if word_count > 0 else EWAHBitmap()
        entry = BitmapEntry(
            object_pos=obj_pos,
            xor_offset=xor_offset,
            flags=entry_flags,
            bitmap=ewah_bitmap,
        )

        # Resolve object position to SHA if we have a pack index
        if pack_index is not None:
            # Get the SHA at the given position in the sorted index
            sha = None
            for idx, (entry_sha, _offset, _crc32) in enumerate(
                pack_index.iterentries()
            ):
                if idx == obj_pos:
                    sha = entry_sha
                    break

            if sha is not None:
                bitmap.entries[sha] = entry
                bitmap.entries_list.append((sha, entry))
        else:
            # Without pack index, use position as temporary key
            temp_key = obj_pos.to_bytes(4, byteorder="big")
            bitmap.entries[temp_key] = entry
            bitmap.entries_list.append((temp_key, entry))

    # Read optional lookup table
    if flags & BITMAP_OPT_LOOKUP_TABLE:
        # Lookup table contains triplets: (commit_pos, offset, xor_row)
        # Number of entries matches the bitmap entry count
        lookup_table = []
        for _ in range(entry_count):
            # Read commit position (4 bytes)
            commit_pos_bytes = f.read(4)
            if len(commit_pos_bytes) < 4:
                break
            commit_pos = struct.unpack(">I", commit_pos_bytes)[0]

            # Read file offset (8 bytes)
            offset_bytes = f.read(8)
            if len(offset_bytes) < 8:
                break
            offset = struct.unpack(">Q", offset_bytes)[0]

            # Read XOR row (4 bytes)
            xor_row_bytes = f.read(4)
            if len(xor_row_bytes) < 4:
                break
            xor_row = struct.unpack(">I", xor_row_bytes)[0]

            lookup_table.append((commit_pos, offset, xor_row))

        bitmap.lookup_table = lookup_table

    # Read optional name-hash cache
    if flags & BITMAP_OPT_HASH_CACHE:
        # Name-hash cache contains one 32-bit hash per object in the pack
        # The number of hashes depends on the total number of objects
        # For now, we'll read what's available
        name_hash_cache = []
        while True:
            hash_bytes = f.read(4)
            if len(hash_bytes) < 4:
                break
            hash_value = struct.unpack(">I", hash_bytes)[0]
            name_hash_cache.append(hash_value)

        if name_hash_cache:
            bitmap.name_hash_cache = name_hash_cache

    return bitmap


def write_bitmap(
    filename: Union[str, os.PathLike[str]],
    bitmap: PackBitmap,
) -> None:
    """Write a bitmap index file.

    Args:
        filename: Path to write the .bitmap file
        bitmap: PackBitmap to write
    """
    with GitFile(filename, "wb") as f:
        write_bitmap_file(f, bitmap)


def write_bitmap_file(f: IO[bytes], bitmap: PackBitmap) -> None:
    """Write bitmap data to a file object.

    Args:
        f: File object to write to
        bitmap: PackBitmap to write
    """
    # Write header
    f.write(BITMAP_SIGNATURE)
    f.write(struct.pack(">H", bitmap.version))
    f.write(struct.pack(">H", bitmap.flags))

    # Write entry count
    f.write(struct.pack(">I", len(bitmap.entries)))

    # Write pack checksum
    if bitmap.pack_checksum:
        f.write(bitmap.pack_checksum)
    else:
        f.write(b"\x00" * 20)

    # Write type bitmaps (self-describing EWAH format, no size prefix needed)
    for type_bitmap in [
        bitmap.commit_bitmap,
        bitmap.tree_bitmap,
        bitmap.blob_bitmap,
        bitmap.tag_bitmap,
    ]:
        data = type_bitmap.encode()
        f.write(data)

    # Write bitmap entries
    for _sha, entry in bitmap.entries.items():
        # Write object position (4 bytes)
        f.write(struct.pack(">I", entry.object_pos))

        # Write XOR offset (1 byte)
        f.write(bytes([entry.xor_offset]))

        # Write flags (1 byte)
        f.write(bytes([entry.flags]))

        # Write compressed bitmap data (self-describing EWAH format, no size prefix)
        bitmap_data = entry.bitmap.encode()
        f.write(bitmap_data)

    # Write optional lookup table
    if bitmap.flags & BITMAP_OPT_LOOKUP_TABLE and bitmap.lookup_table:
        for commit_pos, offset, xor_row in bitmap.lookup_table:
            f.write(struct.pack(">I", commit_pos))  # 4 bytes
            f.write(struct.pack(">Q", offset))  # 8 bytes
            f.write(struct.pack(">I", xor_row))  # 4 bytes

    # Write optional name-hash cache
    if bitmap.flags & BITMAP_OPT_HASH_CACHE and bitmap.name_hash_cache:
        for hash_value in bitmap.name_hash_cache:
            f.write(struct.pack(">I", hash_value))
