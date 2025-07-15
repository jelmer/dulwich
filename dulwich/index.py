# index.py -- File parser/writer for the git index file
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Parser for the git index file format."""

import errno
import os
import shutil
import stat
import struct
import sys
import types
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Optional,
    Union,
    cast,
)

if TYPE_CHECKING:
    from .config import Config
    from .diff_tree import TreeChange
    from .file import _GitFile
    from .line_ending import BlobNormalizer
    from .repo import Repo

from .file import GitFile
from .object_store import iter_tree_contents
from .objects import (
    S_IFGITLINK,
    S_ISGITLINK,
    Blob,
    ObjectID,
    Tree,
    hex_to_sha,
    sha_to_hex,
)
from .pack import ObjectContainer, SHA1Reader, SHA1Writer

# 2-bit stage (during merge)
FLAG_STAGEMASK = 0x3000
FLAG_STAGESHIFT = 12
FLAG_NAMEMASK = 0x0FFF

# assume-valid
FLAG_VALID = 0x8000

# extended flag (must be zero in version 2)
FLAG_EXTENDED = 0x4000

# used by sparse checkout
EXTENDED_FLAG_SKIP_WORKTREE = 0x4000

# used by "git add -N"
EXTENDED_FLAG_INTEND_TO_ADD = 0x2000

DEFAULT_VERSION = 2

# Index extension signatures
TREE_EXTENSION = b"TREE"
REUC_EXTENSION = b"REUC"
UNTR_EXTENSION = b"UNTR"
EOIE_EXTENSION = b"EOIE"
IEOT_EXTENSION = b"IEOT"


def _encode_varint(value: int) -> bytes:
    """Encode an integer using variable-width encoding.

    Same format as used for OFS_DELTA pack entries and index v4 path compression.
    Uses 7 bits per byte, with the high bit indicating continuation.

    Args:
      value: Integer to encode
    Returns:
      Encoded bytes
    """
    if value == 0:
        return b"\x00"

    result = []
    while value > 0:
        byte = value & 0x7F  # Take lower 7 bits
        value >>= 7
        if value > 0:
            byte |= 0x80  # Set continuation bit
        result.append(byte)

    return bytes(result)


def _decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Decode a variable-width encoded integer.

    Args:
      data: Bytes to decode from
      offset: Starting offset in data
    Returns:
      tuple of (decoded_value, new_offset)
    """
    value = 0
    shift = 0
    pos = offset

    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):  # No continuation bit
            break

    return value, pos


def _compress_path(path: bytes, previous_path: bytes) -> bytes:
    """Compress a path relative to the previous path for index version 4.

    Args:
      path: Path to compress
      previous_path: Previous path for comparison
    Returns:
      Compressed path data (varint prefix_len + suffix)
    """
    # Find the common prefix length
    common_len = 0
    min_len = min(len(path), len(previous_path))

    for i in range(min_len):
        if path[i] == previous_path[i]:
            common_len += 1
        else:
            break

    # The number of bytes to remove from the end of previous_path
    # to get the common prefix
    remove_len = len(previous_path) - common_len

    # The suffix to append
    suffix = path[common_len:]

    # Encode: varint(remove_len) + suffix + NUL
    return _encode_varint(remove_len) + suffix + b"\x00"


def _decompress_path(
    data: bytes, offset: int, previous_path: bytes
) -> tuple[bytes, int]:
    """Decompress a path from index version 4 compressed format.

    Args:
      data: Raw data containing compressed path
      offset: Starting offset in data
      previous_path: Previous path for decompression
    Returns:
      tuple of (decompressed_path, new_offset)
    """
    # Decode the number of bytes to remove from previous path
    remove_len, new_offset = _decode_varint(data, offset)

    # Find the NUL terminator for the suffix
    suffix_start = new_offset
    suffix_end = suffix_start
    while suffix_end < len(data) and data[suffix_end] != 0:
        suffix_end += 1

    if suffix_end >= len(data):
        raise ValueError("Unterminated path suffix in compressed entry")

    suffix = data[suffix_start:suffix_end]
    new_offset = suffix_end + 1  # Skip the NUL terminator

    # Reconstruct the path
    if remove_len > len(previous_path):
        raise ValueError(
            f"Invalid path compression: trying to remove {remove_len} bytes from {len(previous_path)}-byte path"
        )

    prefix = previous_path[:-remove_len] if remove_len > 0 else previous_path
    path = prefix + suffix

    return path, new_offset


def _decompress_path_from_stream(
    f: BinaryIO, previous_path: bytes
) -> tuple[bytes, int]:
    """Decompress a path from index version 4 compressed format, reading from stream.

    Args:
      f: File-like object to read from
      previous_path: Previous path for decompression
    Returns:
      tuple of (decompressed_path, bytes_consumed)
    """
    # Decode the varint for remove_len by reading byte by byte
    remove_len = 0
    shift = 0
    bytes_consumed = 0

    while True:
        byte_data = f.read(1)
        if not byte_data:
            raise ValueError("Unexpected end of file while reading varint")
        byte = byte_data[0]
        bytes_consumed += 1
        remove_len |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):  # No continuation bit
            break

    # Read the suffix until NUL terminator
    suffix = b""
    while True:
        byte_data = f.read(1)
        if not byte_data:
            raise ValueError("Unexpected end of file while reading path suffix")
        byte = byte_data[0]
        bytes_consumed += 1
        if byte == 0:  # NUL terminator
            break
        suffix += bytes([byte])

    # Reconstruct the path
    if remove_len > len(previous_path):
        raise ValueError(
            f"Invalid path compression: trying to remove {remove_len} bytes from {len(previous_path)}-byte path"
        )

    prefix = previous_path[:-remove_len] if remove_len > 0 else previous_path
    path = prefix + suffix

    return path, bytes_consumed


class Stage(Enum):
    NORMAL = 0
    MERGE_CONFLICT_ANCESTOR = 1
    MERGE_CONFLICT_THIS = 2
    MERGE_CONFLICT_OTHER = 3


@dataclass
class SerializedIndexEntry:
    name: bytes
    ctime: Union[int, float, tuple[int, int]]
    mtime: Union[int, float, tuple[int, int]]
    dev: int
    ino: int
    mode: int
    uid: int
    gid: int
    size: int
    sha: bytes
    flags: int
    extended_flags: int

    def stage(self) -> Stage:
        return Stage((self.flags & FLAG_STAGEMASK) >> FLAG_STAGESHIFT)


@dataclass
class IndexExtension:
    """Base class for index extensions."""

    signature: bytes
    data: bytes

    @classmethod
    def from_raw(cls, signature: bytes, data: bytes) -> "IndexExtension":
        """Create an extension from raw data.

        Args:
          signature: 4-byte extension signature
          data: Extension data
        Returns:
          Parsed extension object
        """
        if signature == TREE_EXTENSION:
            return TreeExtension.from_bytes(data)
        elif signature == REUC_EXTENSION:
            return ResolveUndoExtension.from_bytes(data)
        elif signature == UNTR_EXTENSION:
            return UntrackedExtension.from_bytes(data)
        else:
            # Unknown extension - just store raw data
            return cls(signature, data)

    def to_bytes(self) -> bytes:
        """Serialize extension to bytes."""
        return self.data


class TreeExtension(IndexExtension):
    """Tree cache extension."""

    def __init__(self, entries: list[tuple[bytes, bytes, int]]) -> None:
        self.entries = entries
        super().__init__(TREE_EXTENSION, b"")

    @classmethod
    def from_bytes(cls, data: bytes) -> "TreeExtension":
        # TODO: Implement tree cache parsing
        return cls([])

    def to_bytes(self) -> bytes:
        # TODO: Implement tree cache serialization
        return b""


class ResolveUndoExtension(IndexExtension):
    """Resolve undo extension for recording merge conflicts."""

    def __init__(self, entries: list[tuple[bytes, list[tuple[int, bytes]]]]) -> None:
        self.entries = entries
        super().__init__(REUC_EXTENSION, b"")

    @classmethod
    def from_bytes(cls, data: bytes) -> "ResolveUndoExtension":
        # TODO: Implement resolve undo parsing
        return cls([])

    def to_bytes(self) -> bytes:
        # TODO: Implement resolve undo serialization
        return b""


class UntrackedExtension(IndexExtension):
    """Untracked cache extension."""

    def __init__(self, data: bytes) -> None:
        super().__init__(UNTR_EXTENSION, data)

    @classmethod
    def from_bytes(cls, data: bytes) -> "UntrackedExtension":
        return cls(data)


@dataclass
class IndexEntry:
    ctime: Union[int, float, tuple[int, int]]
    mtime: Union[int, float, tuple[int, int]]
    dev: int
    ino: int
    mode: int
    uid: int
    gid: int
    size: int
    sha: bytes
    flags: int = 0
    extended_flags: int = 0

    @classmethod
    def from_serialized(cls, serialized: SerializedIndexEntry) -> "IndexEntry":
        return cls(
            ctime=serialized.ctime,
            mtime=serialized.mtime,
            dev=serialized.dev,
            ino=serialized.ino,
            mode=serialized.mode,
            uid=serialized.uid,
            gid=serialized.gid,
            size=serialized.size,
            sha=serialized.sha,
            flags=serialized.flags,
            extended_flags=serialized.extended_flags,
        )

    def serialize(self, name: bytes, stage: Stage) -> SerializedIndexEntry:
        # Clear out any existing stage bits, then set them from the Stage.
        new_flags = self.flags & ~FLAG_STAGEMASK
        new_flags |= stage.value << FLAG_STAGESHIFT
        return SerializedIndexEntry(
            name=name,
            ctime=self.ctime,
            mtime=self.mtime,
            dev=self.dev,
            ino=self.ino,
            mode=self.mode,
            uid=self.uid,
            gid=self.gid,
            size=self.size,
            sha=self.sha,
            flags=new_flags,
            extended_flags=self.extended_flags,
        )

    def stage(self) -> Stage:
        return Stage((self.flags & FLAG_STAGEMASK) >> FLAG_STAGESHIFT)

    @property
    def skip_worktree(self) -> bool:
        """Return True if the skip-worktree bit is set in extended_flags."""
        return bool(self.extended_flags & EXTENDED_FLAG_SKIP_WORKTREE)

    def set_skip_worktree(self, skip: bool = True) -> None:
        """Helper method to set or clear the skip-worktree bit in extended_flags.
        Also sets FLAG_EXTENDED in self.flags if needed.
        """
        if skip:
            # Turn on the skip-worktree bit
            self.extended_flags |= EXTENDED_FLAG_SKIP_WORKTREE
            # Also ensure the main 'extended' bit is set in flags
            self.flags |= FLAG_EXTENDED
        else:
            # Turn off the skip-worktree bit
            self.extended_flags &= ~EXTENDED_FLAG_SKIP_WORKTREE
            # Optionally unset the main extended bit if no extended flags remain
            if self.extended_flags == 0:
                self.flags &= ~FLAG_EXTENDED


class ConflictedIndexEntry:
    """Index entry that represents a conflict."""

    ancestor: Optional[IndexEntry]
    this: Optional[IndexEntry]
    other: Optional[IndexEntry]

    def __init__(
        self,
        ancestor: Optional[IndexEntry] = None,
        this: Optional[IndexEntry] = None,
        other: Optional[IndexEntry] = None,
    ) -> None:
        self.ancestor = ancestor
        self.this = this
        self.other = other


class UnmergedEntries(Exception):
    """Unmerged entries exist in the index."""


def pathsplit(path: bytes) -> tuple[bytes, bytes]:
    """Split a /-delimited path into a directory part and a basename.

    Args:
      path: The path to split.

    Returns:
      Tuple with directory name and basename
    """
    try:
        (dirname, basename) = path.rsplit(b"/", 1)
    except ValueError:
        return (b"", path)
    else:
        return (dirname, basename)


def pathjoin(*args: bytes) -> bytes:
    """Join a /-delimited path."""
    return b"/".join([p for p in args if p])


def read_cache_time(f: BinaryIO) -> tuple[int, int]:
    """Read a cache time.

    Args:
      f: File-like object to read from
    Returns:
      Tuple with seconds and nanoseconds
    """
    return struct.unpack(">LL", f.read(8))


def write_cache_time(f: BinaryIO, t: Union[int, float, tuple[int, int]]) -> None:
    """Write a cache time.

    Args:
      f: File-like object to write to
      t: Time to write (as int, float or tuple with secs and nsecs)
    """
    if isinstance(t, int):
        t = (t, 0)
    elif isinstance(t, float):
        (secs, nsecs) = divmod(t, 1.0)
        t = (int(secs), int(nsecs * 1000000000))
    elif not isinstance(t, tuple):
        raise TypeError(t)
    f.write(struct.pack(">LL", *t))


def read_cache_entry(
    f: BinaryIO, version: int, previous_path: bytes = b""
) -> SerializedIndexEntry:
    """Read an entry from a cache file.

    Args:
      f: File-like object to read from
      version: Index version
      previous_path: Previous entry's path (for version 4 compression)
    """
    beginoffset = f.tell()
    ctime = read_cache_time(f)
    mtime = read_cache_time(f)
    (
        dev,
        ino,
        mode,
        uid,
        gid,
        size,
        sha,
        flags,
    ) = struct.unpack(">LLLLLL20sH", f.read(20 + 4 * 6 + 2))
    if flags & FLAG_EXTENDED:
        if version < 3:
            raise AssertionError("extended flag set in index with version < 3")
        (extended_flags,) = struct.unpack(">H", f.read(2))
    else:
        extended_flags = 0

    if version >= 4:
        # Version 4: paths are always compressed (name_len should be 0)
        name, consumed = _decompress_path_from_stream(f, previous_path)
    else:
        # Versions < 4: regular name reading
        name = f.read(flags & FLAG_NAMEMASK)

    # Padding:
    if version < 4:
        real_size = (f.tell() - beginoffset + 8) & ~7
        f.read((beginoffset + real_size) - f.tell())

    return SerializedIndexEntry(
        name,
        ctime,
        mtime,
        dev,
        ino,
        mode,
        uid,
        gid,
        size,
        sha_to_hex(sha),
        flags & ~FLAG_NAMEMASK,
        extended_flags,
    )


def write_cache_entry(
    f: BinaryIO, entry: SerializedIndexEntry, version: int, previous_path: bytes = b""
) -> None:
    """Write an index entry to a file.

    Args:
      f: File object
      entry: IndexEntry to write
      version: Index format version
      previous_path: Previous entry's path (for version 4 compression)
    """
    beginoffset = f.tell()
    write_cache_time(f, entry.ctime)
    write_cache_time(f, entry.mtime)

    if version >= 4:
        # Version 4: use compression but set name_len to actual filename length
        # This matches how C Git implements index v4 flags
        compressed_path = _compress_path(entry.name, previous_path)
        flags = len(entry.name) | (entry.flags & ~FLAG_NAMEMASK)
    else:
        # Versions < 4: include actual name length
        flags = len(entry.name) | (entry.flags & ~FLAG_NAMEMASK)

    if entry.extended_flags:
        flags |= FLAG_EXTENDED
    if flags & FLAG_EXTENDED and version is not None and version < 3:
        raise AssertionError("unable to use extended flags in version < 3")

    f.write(
        struct.pack(
            b">LLLLLL20sH",
            entry.dev & 0xFFFFFFFF,
            entry.ino & 0xFFFFFFFF,
            entry.mode,
            entry.uid,
            entry.gid,
            entry.size,
            hex_to_sha(entry.sha),
            flags,
        )
    )
    if flags & FLAG_EXTENDED:
        f.write(struct.pack(b">H", entry.extended_flags))

    if version >= 4:
        # Version 4: always write compressed path
        f.write(compressed_path)
    else:
        # Versions < 4: write regular path and padding
        f.write(entry.name)
        real_size = (f.tell() - beginoffset + 8) & ~7
        f.write(b"\0" * ((beginoffset + real_size) - f.tell()))


class UnsupportedIndexFormat(Exception):
    """An unsupported index format was encountered."""

    def __init__(self, version: int) -> None:
        self.index_format_version = version


def read_index_header(f: BinaryIO) -> tuple[int, int]:
    """Read an index header from a file.

    Returns:
      tuple of (version, num_entries)
    """
    header = f.read(4)
    if header != b"DIRC":
        raise AssertionError(f"Invalid index file header: {header!r}")
    (version, num_entries) = struct.unpack(b">LL", f.read(4 * 2))
    if version not in (1, 2, 3, 4):
        raise UnsupportedIndexFormat(version)
    return version, num_entries


def write_index_extension(f: BinaryIO, extension: IndexExtension) -> None:
    """Write an index extension.

    Args:
      f: File-like object to write to
      extension: Extension to write
    """
    data = extension.to_bytes()
    f.write(extension.signature)
    f.write(struct.pack(">I", len(data)))
    f.write(data)


def read_index(f: BinaryIO) -> Iterator[SerializedIndexEntry]:
    """Read an index file, yielding the individual entries."""
    version, num_entries = read_index_header(f)
    previous_path = b""
    for i in range(num_entries):
        entry = read_cache_entry(f, version, previous_path)
        previous_path = entry.name
        yield entry


def read_index_dict_with_version(
    f: BinaryIO,
) -> tuple[
    dict[bytes, Union[IndexEntry, ConflictedIndexEntry]], int, list[IndexExtension]
]:
    """Read an index file and return it as a dictionary along with the version.

    Returns:
      tuple of (entries_dict, version, extensions)
    """
    version, num_entries = read_index_header(f)

    ret: dict[bytes, Union[IndexEntry, ConflictedIndexEntry]] = {}
    previous_path = b""
    for i in range(num_entries):
        entry = read_cache_entry(f, version, previous_path)
        previous_path = entry.name
        stage = entry.stage()
        if stage == Stage.NORMAL:
            ret[entry.name] = IndexEntry.from_serialized(entry)
        else:
            existing = ret.setdefault(entry.name, ConflictedIndexEntry())
            if isinstance(existing, IndexEntry):
                raise AssertionError(f"Non-conflicted entry for {entry.name!r} exists")
            if stage == Stage.MERGE_CONFLICT_ANCESTOR:
                existing.ancestor = IndexEntry.from_serialized(entry)
            elif stage == Stage.MERGE_CONFLICT_THIS:
                existing.this = IndexEntry.from_serialized(entry)
            elif stage == Stage.MERGE_CONFLICT_OTHER:
                existing.other = IndexEntry.from_serialized(entry)

    # Read extensions
    extensions = []
    while True:
        # Check if we're at the end (20 bytes before EOF for SHA checksum)
        current_pos = f.tell()
        f.seek(0, 2)  # EOF
        eof_pos = f.tell()
        f.seek(current_pos)

        if current_pos >= eof_pos - 20:
            break

        # Try to read extension signature
        signature = f.read(4)
        if len(signature) < 4:
            break

        # Check if it's a valid extension signature (4 uppercase letters)
        if not all(65 <= b <= 90 for b in signature):
            # Not an extension, seek back
            f.seek(-4, 1)
            break

        # Read extension size
        size_data = f.read(4)
        if len(size_data) < 4:
            break
        size = struct.unpack(">I", size_data)[0]

        # Read extension data
        data = f.read(size)
        if len(data) < size:
            break

        extension = IndexExtension.from_raw(signature, data)
        extensions.append(extension)

    return ret, version, extensions


def read_index_dict(
    f: BinaryIO,
) -> dict[bytes, Union[IndexEntry, ConflictedIndexEntry]]:
    """Read an index file and return it as a dictionary.
       Dict Key is tuple of path and stage number, as
            path alone is not unique
    Args:
      f: File object to read fromls.
    """
    ret: dict[bytes, Union[IndexEntry, ConflictedIndexEntry]] = {}
    for entry in read_index(f):
        stage = entry.stage()
        if stage == Stage.NORMAL:
            ret[entry.name] = IndexEntry.from_serialized(entry)
        else:
            existing = ret.setdefault(entry.name, ConflictedIndexEntry())
            if isinstance(existing, IndexEntry):
                raise AssertionError(f"Non-conflicted entry for {entry.name!r} exists")
            if stage == Stage.MERGE_CONFLICT_ANCESTOR:
                existing.ancestor = IndexEntry.from_serialized(entry)
            elif stage == Stage.MERGE_CONFLICT_THIS:
                existing.this = IndexEntry.from_serialized(entry)
            elif stage == Stage.MERGE_CONFLICT_OTHER:
                existing.other = IndexEntry.from_serialized(entry)
    return ret


def write_index(
    f: BinaryIO,
    entries: list[SerializedIndexEntry],
    version: Optional[int] = None,
    extensions: Optional[list[IndexExtension]] = None,
) -> None:
    """Write an index file.

    Args:
      f: File-like object to write to
      version: Version number to write
      entries: Iterable over the entries to write
      extensions: Optional list of extensions to write
    """
    if version is None:
        version = DEFAULT_VERSION
    # STEP 1: check if any extended_flags are set
    uses_extended_flags = any(e.extended_flags != 0 for e in entries)
    if uses_extended_flags and version < 3:
        # Force or bump the version to 3
        version = 3
    # The rest is unchanged, but you might insert a final check:
    if version < 3:
        # Double-check no extended flags appear
        for e in entries:
            if e.extended_flags != 0:
                raise AssertionError("Attempt to use extended flags in index < v3")
    # Proceed with the existing code to write the header and entries.
    f.write(b"DIRC")
    f.write(struct.pack(b">LL", version, len(entries)))
    previous_path = b""
    for entry in entries:
        write_cache_entry(f, entry, version=version, previous_path=previous_path)
        previous_path = entry.name

    # Write extensions
    if extensions:
        for extension in extensions:
            write_index_extension(f, extension)


def write_index_dict(
    f: BinaryIO,
    entries: dict[bytes, Union[IndexEntry, ConflictedIndexEntry]],
    version: Optional[int] = None,
    extensions: Optional[list[IndexExtension]] = None,
) -> None:
    """Write an index file based on the contents of a dictionary.
    being careful to sort by path and then by stage.
    """
    entries_list = []
    for key in sorted(entries):
        value = entries[key]
        if isinstance(value, ConflictedIndexEntry):
            if value.ancestor is not None:
                entries_list.append(
                    value.ancestor.serialize(key, Stage.MERGE_CONFLICT_ANCESTOR)
                )
            if value.this is not None:
                entries_list.append(
                    value.this.serialize(key, Stage.MERGE_CONFLICT_THIS)
                )
            if value.other is not None:
                entries_list.append(
                    value.other.serialize(key, Stage.MERGE_CONFLICT_OTHER)
                )
        else:
            entries_list.append(value.serialize(key, Stage.NORMAL))

    write_index(f, entries_list, version=version, extensions=extensions)


def cleanup_mode(mode: int) -> int:
    """Cleanup a mode value.

    This will return a mode that can be stored in a tree object.

    Args:
      mode: Mode to clean up.

    Returns:
      mode
    """
    if stat.S_ISLNK(mode):
        return stat.S_IFLNK
    elif stat.S_ISDIR(mode):
        return stat.S_IFDIR
    elif S_ISGITLINK(mode):
        return S_IFGITLINK
    ret = stat.S_IFREG | 0o644
    if mode & 0o100:
        ret |= 0o111
    return ret


class Index:
    """A Git Index file."""

    _byname: dict[bytes, Union[IndexEntry, ConflictedIndexEntry]]

    def __init__(
        self,
        filename: Union[bytes, str, os.PathLike],
        read: bool = True,
        skip_hash: bool = False,
        version: Optional[int] = None,
    ) -> None:
        """Create an index object associated with the given filename.

        Args:
          filename: Path to the index file
          read: Whether to initialize the index from the given file, should it exist.
          skip_hash: Whether to skip SHA1 hash when writing (for manyfiles feature)
          version: Index format version to use (None = auto-detect from file or use default)
        """
        self._filename = os.fspath(filename)
        # TODO(jelmer): Store the version returned by read_index
        self._version = version
        self._skip_hash = skip_hash
        self._extensions: list[IndexExtension] = []
        self.clear()
        if read:
            self.read()

    @property
    def path(self) -> Union[bytes, str]:
        return self._filename

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._filename!r})"

    def write(self) -> None:
        """Write current contents of index to disk."""
        from typing import BinaryIO, cast

        f = GitFile(self._filename, "wb")
        try:
            # Filter out extensions with no meaningful data
            meaningful_extensions = []
            for ext in self._extensions:
                # Skip extensions that have empty data
                ext_data = ext.to_bytes()
                if ext_data:
                    meaningful_extensions.append(ext)

            if self._skip_hash:
                # When skipHash is enabled, write the index without computing SHA1
                write_index_dict(
                    cast(BinaryIO, f),
                    self._byname,
                    version=self._version,
                    extensions=meaningful_extensions,
                )
                # Write 20 zero bytes instead of SHA1
                f.write(b"\x00" * 20)
                f.close()
            else:
                sha1_writer = SHA1Writer(cast(BinaryIO, f))
                write_index_dict(
                    cast(BinaryIO, sha1_writer),
                    self._byname,
                    version=self._version,
                    extensions=meaningful_extensions,
                )
                sha1_writer.close()
        except:
            f.close()
            raise

    def read(self) -> None:
        """Read current contents of index from disk."""
        if not os.path.exists(self._filename):
            return
        f = GitFile(self._filename, "rb")
        try:
            sha1_reader = SHA1Reader(f)
            entries, version, extensions = read_index_dict_with_version(
                cast(BinaryIO, sha1_reader)
            )
            self._version = version
            self._extensions = extensions
            self.update(entries)
            # Extensions have already been read by read_index_dict_with_version
            sha1_reader.check_sha(allow_empty=True)
        finally:
            f.close()

    def __len__(self) -> int:
        """Number of entries in this index file."""
        return len(self._byname)

    def __getitem__(self, key: bytes) -> Union[IndexEntry, ConflictedIndexEntry]:
        """Retrieve entry by relative path and stage.

        Returns: Either a IndexEntry or a ConflictedIndexEntry
        Raises KeyError: if the entry does not exist
        """
        return self._byname[key]

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over the paths and stages in this index."""
        return iter(self._byname)

    def __contains__(self, key: bytes) -> bool:
        return key in self._byname

    def get_sha1(self, path: bytes) -> bytes:
        """Return the (git object) SHA1 for the object at a path."""
        value = self[path]
        if isinstance(value, ConflictedIndexEntry):
            raise UnmergedEntries
        return value.sha

    def get_mode(self, path: bytes) -> int:
        """Return the POSIX file mode for the object at a path."""
        value = self[path]
        if isinstance(value, ConflictedIndexEntry):
            raise UnmergedEntries
        return value.mode

    def iterobjects(self) -> Iterable[tuple[bytes, bytes, int]]:
        """Iterate over path, sha, mode tuples for use with commit_tree."""
        for path in self:
            entry = self[path]
            if isinstance(entry, ConflictedIndexEntry):
                raise UnmergedEntries
            yield path, entry.sha, cleanup_mode(entry.mode)

    def has_conflicts(self) -> bool:
        for value in self._byname.values():
            if isinstance(value, ConflictedIndexEntry):
                return True
        return False

    def clear(self) -> None:
        """Remove all contents from this index."""
        self._byname = {}

    def __setitem__(
        self, name: bytes, value: Union[IndexEntry, ConflictedIndexEntry]
    ) -> None:
        assert isinstance(name, bytes)
        self._byname[name] = value

    def __delitem__(self, name: bytes) -> None:
        del self._byname[name]

    def iteritems(
        self,
    ) -> Iterator[tuple[bytes, Union[IndexEntry, ConflictedIndexEntry]]]:
        return iter(self._byname.items())

    def items(self) -> Iterator[tuple[bytes, Union[IndexEntry, ConflictedIndexEntry]]]:
        return iter(self._byname.items())

    def update(
        self, entries: dict[bytes, Union[IndexEntry, ConflictedIndexEntry]]
    ) -> None:
        for key, value in entries.items():
            self[key] = value

    def paths(self) -> Generator[bytes, None, None]:
        yield from self._byname.keys()

    def changes_from_tree(
        self,
        object_store: ObjectContainer,
        tree: ObjectID,
        want_unchanged: bool = False,
    ) -> Generator[
        tuple[
            tuple[Optional[bytes], Optional[bytes]],
            tuple[Optional[int], Optional[int]],
            tuple[Optional[bytes], Optional[bytes]],
        ],
        None,
        None,
    ]:
        """Find the differences between the contents of this index and a tree.

        Args:
          object_store: Object store to use for retrieving tree contents
          tree: SHA1 of the root tree
          want_unchanged: Whether unchanged files should be reported
        Returns: Iterator over tuples with (oldpath, newpath), (oldmode,
            newmode), (oldsha, newsha)
        """

        def lookup_entry(path: bytes) -> tuple[bytes, int]:
            entry = self[path]
            if hasattr(entry, "sha") and hasattr(entry, "mode"):
                return entry.sha, cleanup_mode(entry.mode)
            else:
                # Handle ConflictedIndexEntry case
                return b"", 0

        yield from changes_from_tree(
            self.paths(),
            lookup_entry,
            object_store,
            tree,
            want_unchanged=want_unchanged,
        )

    def commit(self, object_store: ObjectContainer) -> bytes:
        """Create a new tree from an index.

        Args:
          object_store: Object store to save the tree in
        Returns:
          Root tree SHA
        """
        return commit_tree(object_store, self.iterobjects())


def commit_tree(
    object_store: ObjectContainer, blobs: Iterable[tuple[bytes, bytes, int]]
) -> bytes:
    """Commit a new tree.

    Args:
      object_store: Object store to add trees to
      blobs: Iterable over blob path, sha, mode entries
    Returns:
      SHA1 of the created tree.
    """
    trees: dict[bytes, Any] = {b"": {}}

    def add_tree(path: bytes) -> dict[bytes, Any]:
        if path in trees:
            return trees[path]
        dirname, basename = pathsplit(path)
        t = add_tree(dirname)
        assert isinstance(basename, bytes)
        newtree: dict[bytes, Any] = {}
        t[basename] = newtree
        trees[path] = newtree
        return newtree

    for path, sha, mode in blobs:
        tree_path, basename = pathsplit(path)
        tree = add_tree(tree_path)
        tree[basename] = (mode, sha)

    def build_tree(path: bytes) -> bytes:
        tree = Tree()
        for basename, entry in trees[path].items():
            if isinstance(entry, dict):
                mode = stat.S_IFDIR
                sha = build_tree(pathjoin(path, basename))
            else:
                (mode, sha) = entry
            tree.add(basename, mode, sha)
        object_store.add_object(tree)
        return tree.id

    return build_tree(b"")


def commit_index(object_store: ObjectContainer, index: Index) -> bytes:
    """Create a new tree from an index.

    Args:
      object_store: Object store to save the tree in
      index: Index file
    Note: This function is deprecated, use index.commit() instead.
    Returns: Root tree sha.
    """
    return commit_tree(object_store, index.iterobjects())


def changes_from_tree(
    names: Iterable[bytes],
    lookup_entry: Callable[[bytes], tuple[bytes, int]],
    object_store: ObjectContainer,
    tree: Optional[bytes],
    want_unchanged: bool = False,
) -> Iterable[
    tuple[
        tuple[Optional[bytes], Optional[bytes]],
        tuple[Optional[int], Optional[int]],
        tuple[Optional[bytes], Optional[bytes]],
    ]
]:
    """Find the differences between the contents of a tree and
    a working copy.

    Args:
      names: Iterable of names in the working copy
      lookup_entry: Function to lookup an entry in the working copy
      object_store: Object store to use for retrieving tree contents
      tree: SHA1 of the root tree, or None for an empty tree
      want_unchanged: Whether unchanged files should be reported
    Returns: Iterator over tuples with (oldpath, newpath), (oldmode, newmode),
        (oldsha, newsha)
    """
    # TODO(jelmer): Support a include_trees option
    other_names = set(names)

    if tree is not None:
        for name, mode, sha in iter_tree_contents(object_store, tree):
            try:
                (other_sha, other_mode) = lookup_entry(name)
            except KeyError:
                # Was removed
                yield ((name, None), (mode, None), (sha, None))
            else:
                other_names.remove(name)
                if want_unchanged or other_sha != sha or other_mode != mode:
                    yield ((name, name), (mode, other_mode), (sha, other_sha))

    # Mention added files
    for name in other_names:
        try:
            (other_sha, other_mode) = lookup_entry(name)
        except KeyError:
            pass
        else:
            yield ((None, name), (None, other_mode), (None, other_sha))


def index_entry_from_stat(
    stat_val: os.stat_result,
    hex_sha: bytes,
    mode: Optional[int] = None,
) -> IndexEntry:
    """Create a new index entry from a stat value.

    Args:
      stat_val: POSIX stat_result instance
      hex_sha: Hex sha of the object
    """
    if mode is None:
        mode = cleanup_mode(stat_val.st_mode)

    return IndexEntry(
        ctime=stat_val.st_ctime,
        mtime=stat_val.st_mtime,
        dev=stat_val.st_dev,
        ino=stat_val.st_ino,
        mode=mode,
        uid=stat_val.st_uid,
        gid=stat_val.st_gid,
        size=stat_val.st_size,
        sha=hex_sha,
        flags=0,
        extended_flags=0,
    )


if sys.platform == "win32":
    # On Windows, creating symlinks either requires administrator privileges
    # or developer mode. Raise a more helpful error when we're unable to
    # create symlinks

    # https://github.com/jelmer/dulwich/issues/1005

    class WindowsSymlinkPermissionError(PermissionError):
        def __init__(self, errno: int, msg: str, filename: Optional[str]) -> None:
            super(PermissionError, self).__init__(
                errno,
                f"Unable to create symlink; do you have developer mode enabled? {msg}",
                filename,
            )

    def symlink(
        src: Union[str, bytes],
        dst: Union[str, bytes],
        target_is_directory: bool = False,
        *,
        dir_fd: Optional[int] = None,
    ) -> None:
        try:
            return os.symlink(
                src, dst, target_is_directory=target_is_directory, dir_fd=dir_fd
            )
        except PermissionError as e:
            raise WindowsSymlinkPermissionError(
                e.errno or 0, e.strerror or "", e.filename
            ) from e
else:
    symlink = os.symlink


def build_file_from_blob(
    blob: Blob,
    mode: int,
    target_path: bytes,
    *,
    honor_filemode: bool = True,
    tree_encoding: str = "utf-8",
    symlink_fn: Optional[Callable] = None,
) -> os.stat_result:
    """Build a file or symlink on disk based on a Git object.

    Args:
      blob: The git object
      mode: File mode
      target_path: Path to write to
      honor_filemode: An optional flag to honor core.filemode setting in
        config file, default is core.filemode=True, change executable bit
      symlink_fn: Function to use for creating symlinks
    Returns: stat object for the file
    """
    try:
        oldstat = os.lstat(target_path)
    except FileNotFoundError:
        oldstat = None
    contents = blob.as_raw_string()
    if stat.S_ISLNK(mode):
        if oldstat:
            _remove_file_with_readonly_handling(target_path)
        if sys.platform == "win32":
            # os.readlink on Python3 on Windows requires a unicode string.
            contents_str = contents.decode(tree_encoding)
            target_path_str = target_path.decode(tree_encoding)
            (symlink_fn or symlink)(contents_str, target_path_str)
        else:
            (symlink_fn or symlink)(contents, target_path)
    else:
        if oldstat is not None and oldstat.st_size == len(contents):
            with open(target_path, "rb") as f:
                if f.read() == contents:
                    return oldstat

        with open(target_path, "wb") as f:
            # Write out file
            f.write(contents)

        if honor_filemode:
            os.chmod(target_path, mode)

    return os.lstat(target_path)


INVALID_DOTNAMES = (b".git", b".", b"..", b"")


def _normalize_path_element_default(element: bytes) -> bytes:
    """Normalize path element for default case-insensitive comparison."""
    return element.lower()


def _normalize_path_element_ntfs(element: bytes) -> bytes:
    """Normalize path element for NTFS filesystem."""
    return element.rstrip(b". ").lower()


def _normalize_path_element_hfs(element: bytes) -> bytes:
    """Normalize path element for HFS+ filesystem."""
    import unicodedata

    # Decode to Unicode (let UnicodeDecodeError bubble up)
    element_str = element.decode("utf-8", errors="strict")

    # Remove HFS+ ignorable characters
    filtered = "".join(c for c in element_str if ord(c) not in HFS_IGNORABLE_CHARS)
    # Normalize to NFD
    normalized = unicodedata.normalize("NFD", filtered)
    return normalized.lower().encode("utf-8", errors="strict")


def get_path_element_normalizer(config) -> Callable[[bytes], bytes]:
    """Get the appropriate path element normalization function based on config.

    Args:
        config: Repository configuration object

    Returns:
        Function that normalizes path elements for the configured filesystem
    """
    import os
    import sys

    if config.get_boolean(b"core", b"protectNTFS", os.name == "nt"):
        return _normalize_path_element_ntfs
    elif config.get_boolean(b"core", b"protectHFS", sys.platform == "darwin"):
        return _normalize_path_element_hfs
    else:
        return _normalize_path_element_default


def validate_path_element_default(element: bytes) -> bool:
    return _normalize_path_element_default(element) not in INVALID_DOTNAMES


def validate_path_element_ntfs(element: bytes) -> bool:
    normalized = _normalize_path_element_ntfs(element)
    if normalized in INVALID_DOTNAMES:
        return False
    if normalized == b"git~1":
        return False
    return True


# HFS+ ignorable Unicode codepoints (from Git's utf8.c)
HFS_IGNORABLE_CHARS = {
    0x200C,  # ZERO WIDTH NON-JOINER
    0x200D,  # ZERO WIDTH JOINER
    0x200E,  # LEFT-TO-RIGHT MARK
    0x200F,  # RIGHT-TO-LEFT MARK
    0x202A,  # LEFT-TO-RIGHT EMBEDDING
    0x202B,  # RIGHT-TO-LEFT EMBEDDING
    0x202C,  # POP DIRECTIONAL FORMATTING
    0x202D,  # LEFT-TO-RIGHT OVERRIDE
    0x202E,  # RIGHT-TO-LEFT OVERRIDE
    0x206A,  # INHIBIT SYMMETRIC SWAPPING
    0x206B,  # ACTIVATE SYMMETRIC SWAPPING
    0x206C,  # INHIBIT ARABIC FORM SHAPING
    0x206D,  # ACTIVATE ARABIC FORM SHAPING
    0x206E,  # NATIONAL DIGIT SHAPES
    0x206F,  # NOMINAL DIGIT SHAPES
    0xFEFF,  # ZERO WIDTH NO-BREAK SPACE
}


def validate_path_element_hfs(element: bytes) -> bool:
    """Validate path element for HFS+ filesystem.

    Equivalent to Git's is_hfs_dotgit and related checks.
    Uses NFD normalization and ignores HFS+ ignorable characters.
    """
    try:
        normalized = _normalize_path_element_hfs(element)
    except UnicodeDecodeError:
        # Malformed UTF-8 - be conservative and reject
        return False

    # Check against invalid names
    if normalized in INVALID_DOTNAMES:
        return False

    # Also check for 8.3 short name
    if normalized == b"git~1":
        return False

    return True


def validate_path(
    path: bytes,
    element_validator: Callable[[bytes], bool] = validate_path_element_default,
) -> bool:
    """Default path validator that just checks for .git/."""
    parts = path.split(b"/")
    for p in parts:
        if not element_validator(p):
            return False
    else:
        return True


def build_index_from_tree(
    root_path: Union[str, bytes],
    index_path: Union[str, bytes],
    object_store: ObjectContainer,
    tree_id: bytes,
    honor_filemode: bool = True,
    validate_path_element: Callable[[bytes], bool] = validate_path_element_default,
    symlink_fn: Optional[Callable] = None,
    blob_normalizer: Optional["BlobNormalizer"] = None,
    tree_encoding: str = "utf-8",
) -> None:
    """Generate and materialize index from a tree.

    Args:
      tree_id: Tree to materialize
      root_path: Target dir for materialized index files
      index_path: Target path for generated index
      object_store: Non-empty object store holding tree contents
      honor_filemode: An optional flag to honor core.filemode setting in
        config file, default is core.filemode=True, change executable bit
      validate_path_element: Function to validate path elements to check
        out; default just refuses .git and .. directories.
      blob_normalizer: An optional BlobNormalizer to use for converting line
        endings when writing blobs to the working directory.
      tree_encoding: Encoding used for tree paths (default: utf-8)

    Note: existing index is wiped and contents are not merged
        in a working dir. Suitable only for fresh clones.
    """
    index = Index(index_path, read=False)
    if not isinstance(root_path, bytes):
        root_path = os.fsencode(root_path)

    for entry in iter_tree_contents(object_store, tree_id):
        if not validate_path(entry.path, validate_path_element):
            continue
        full_path = _tree_to_fs_path(root_path, entry.path, tree_encoding)

        if not os.path.exists(os.path.dirname(full_path)):
            os.makedirs(os.path.dirname(full_path))

        # TODO(jelmer): Merge new index into working tree
        if S_ISGITLINK(entry.mode):
            if not os.path.isdir(full_path):
                os.mkdir(full_path)
            st = os.lstat(full_path)
            # TODO(jelmer): record and return submodule paths
        else:
            obj = object_store[entry.sha]
            assert isinstance(obj, Blob)
            # Apply blob normalization for checkout if normalizer is provided
            if blob_normalizer is not None:
                obj = blob_normalizer.checkout_normalize(obj, entry.path)
            st = build_file_from_blob(
                obj,
                entry.mode,
                full_path,
                honor_filemode=honor_filemode,
                tree_encoding=tree_encoding,
                symlink_fn=symlink_fn,
            )

        # Add file to index
        if not honor_filemode or S_ISGITLINK(entry.mode):
            # we can not use tuple slicing to build a new tuple,
            # because on windows that will convert the times to
            # longs, which causes errors further along
            st_tuple = (
                entry.mode,
                st.st_ino,
                st.st_dev,
                st.st_nlink,
                st.st_uid,
                st.st_gid,
                st.st_size,
                st.st_atime,
                st.st_mtime,
                st.st_ctime,
            )
            st = st.__class__(st_tuple)
            # default to a stage 0 index entry (normal)
            # when reading from the filesystem
        index[entry.path] = index_entry_from_stat(st, entry.sha)

    index.write()


def blob_from_path_and_mode(
    fs_path: bytes, mode: int, tree_encoding: str = "utf-8"
) -> Blob:
    """Create a blob from a path and a stat object.

    Args:
      fs_path: Full file system path to file
      mode: File mode
    Returns: A `Blob` object
    """
    assert isinstance(fs_path, bytes)
    blob = Blob()
    if stat.S_ISLNK(mode):
        if sys.platform == "win32":
            # os.readlink on Python3 on Windows requires a unicode string.
            blob.data = os.readlink(os.fsdecode(fs_path)).encode(tree_encoding)
        else:
            blob.data = os.readlink(fs_path)
    else:
        with open(fs_path, "rb") as f:
            blob.data = f.read()
    return blob


def blob_from_path_and_stat(
    fs_path: bytes, st: os.stat_result, tree_encoding: str = "utf-8"
) -> Blob:
    """Create a blob from a path and a stat object.

    Args:
      fs_path: Full file system path to file
      st: A stat object
    Returns: A `Blob` object
    """
    return blob_from_path_and_mode(fs_path, st.st_mode, tree_encoding)


def read_submodule_head(path: Union[str, bytes]) -> Optional[bytes]:
    """Read the head commit of a submodule.

    Args:
      path: path to the submodule
    Returns: HEAD sha, None if not a valid head/repository
    """
    from .errors import NotGitRepository
    from .repo import Repo

    # Repo currently expects a "str", so decode if necessary.
    # TODO(jelmer): Perhaps move this into Repo() ?
    if not isinstance(path, str):
        path = os.fsdecode(path)
    try:
        repo = Repo(path)
    except NotGitRepository:
        return None
    try:
        return repo.head()
    except KeyError:
        return None


def _has_directory_changed(tree_path: bytes, entry: IndexEntry) -> bool:
    """Check if a directory has changed after getting an error.

    When handling an error trying to create a blob from a path, call this
    function. It will check if the path is a directory. If it's a directory
    and a submodule, check the submodule head to see if it's has changed. If
    not, consider the file as changed as Git tracked a file and not a
    directory.

    Return true if the given path should be considered as changed and False
    otherwise or if the path is not a directory.
    """
    # This is actually a directory
    if os.path.exists(os.path.join(tree_path, b".git")):
        # Submodule
        head = read_submodule_head(tree_path)
        if entry.sha != head:
            return True
    else:
        # The file was changed to a directory, so consider it removed.
        return True

    return False


os_sep_bytes = os.sep.encode("ascii")


def _ensure_parent_dir_exists(full_path: bytes) -> None:
    """Ensure parent directory exists, checking no parent is a file."""
    parent_dir = os.path.dirname(full_path)
    if parent_dir and not os.path.exists(parent_dir):
        # Check if any parent in the path is a file
        parts = parent_dir.split(os_sep_bytes)
        for i in range(len(parts)):
            partial_path = os_sep_bytes.join(parts[: i + 1])
            if (
                partial_path
                and os.path.exists(partial_path)
                and not os.path.isdir(partial_path)
            ):
                # Parent path is a file, this is an error
                raise OSError(
                    f"Cannot create directory, parent path is a file: {partial_path!r}"
                )
        os.makedirs(parent_dir)


def _remove_file_with_readonly_handling(path: bytes) -> None:
    """Remove a file, handling read-only files on Windows.

    Args:
      path: Path to the file to remove
    """
    try:
        os.unlink(path)
    except PermissionError:
        # On Windows, remove read-only attribute and retry
        if sys.platform == "win32":
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            os.unlink(path)
        else:
            raise


def _remove_empty_parents(path: bytes, stop_at: bytes) -> None:
    """Remove empty parent directories up to stop_at."""
    parent = os.path.dirname(path)
    while parent and parent != stop_at:
        try:
            os.rmdir(parent)
            parent = os.path.dirname(parent)
        except FileNotFoundError:
            # Directory doesn't exist - stop trying
            break
        except OSError as e:
            if e.errno == errno.ENOTEMPTY:
                # Directory not empty - stop trying
                break
            raise


def _check_symlink_matches(
    full_path: bytes, repo_object_store, entry_sha: bytes
) -> bool:
    """Check if symlink target matches expected target.

    Returns True if symlink matches, False if it doesn't match.
    """
    try:
        current_target = os.readlink(full_path)
        blob_obj = repo_object_store[entry_sha]
        expected_target = blob_obj.as_raw_string()
        if isinstance(current_target, str):
            current_target = current_target.encode()
        return current_target == expected_target
    except FileNotFoundError:
        # Symlink doesn't exist
        return False
    except OSError as e:
        if e.errno == errno.EINVAL:
            # Not a symlink
            return False
        raise


def _check_file_matches(
    repo_object_store,
    full_path: bytes,
    entry_sha: bytes,
    entry_mode: int,
    current_stat: os.stat_result,
    honor_filemode: bool,
    blob_normalizer: Optional["BlobNormalizer"] = None,
    tree_path: Optional[bytes] = None,
) -> bool:
    """Check if a file on disk matches the expected git object.

    Returns True if file matches, False if it doesn't match.
    """
    # Check mode first (if honor_filemode is True)
    if honor_filemode:
        current_mode = stat.S_IMODE(current_stat.st_mode)
        expected_mode = stat.S_IMODE(entry_mode)

        # For regular files, only check the user executable bit, not group/other permissions
        # This matches Git's behavior where umask differences don't count as modifications
        if stat.S_ISREG(current_stat.st_mode):
            # Normalize regular file modes to ignore group/other write permissions
            current_mode_normalized = (
                current_mode & 0o755
            )  # Keep only user rwx and all read+execute
            expected_mode_normalized = expected_mode & 0o755

            # For Git compatibility, regular files should be either 644 or 755
            if expected_mode_normalized not in (0o644, 0o755):
                expected_mode_normalized = 0o644  # Default for regular files
            if current_mode_normalized not in (0o644, 0o755):
                # Determine if it should be executable based on user execute bit
                if current_mode & 0o100:  # User execute bit is set
                    current_mode_normalized = 0o755
                else:
                    current_mode_normalized = 0o644

            if current_mode_normalized != expected_mode_normalized:
                return False
        else:
            # For non-regular files (symlinks, etc.), check mode exactly
            if current_mode != expected_mode:
                return False

    # If mode matches (or we don't care), check content via size first
    blob_obj = repo_object_store[entry_sha]
    if current_stat.st_size != blob_obj.raw_length():
        return False

    # Size matches, check actual content
    try:
        with open(full_path, "rb") as f:
            current_content = f.read()
            expected_content = blob_obj.as_raw_string()
            if blob_normalizer and tree_path is not None:
                normalized_blob = blob_normalizer.checkout_normalize(
                    blob_obj, tree_path
                )
                expected_content = normalized_blob.as_raw_string()
            return current_content == expected_content
    except (FileNotFoundError, PermissionError, IsADirectoryError):
        return False


def _transition_to_submodule(repo, path, full_path, current_stat, entry, index):
    """Transition any type to submodule."""
    from .submodule import ensure_submodule_placeholder

    if current_stat is not None and stat.S_ISDIR(current_stat.st_mode):
        # Already a directory, just ensure .git file exists
        ensure_submodule_placeholder(repo, path)
    else:
        # Remove whatever is there and create submodule
        if current_stat is not None:
            _remove_file_with_readonly_handling(full_path)
        ensure_submodule_placeholder(repo, path)

    st = os.lstat(full_path)
    index[path] = index_entry_from_stat(st, entry.sha)


def _transition_to_file(
    object_store,
    path,
    full_path,
    current_stat,
    entry,
    index,
    honor_filemode,
    symlink_fn,
    blob_normalizer,
    tree_encoding="utf-8",
):
    """Transition any type to regular file or symlink."""
    # Check if we need to update
    if (
        current_stat is not None
        and stat.S_ISREG(current_stat.st_mode)
        and not stat.S_ISLNK(entry.mode)
    ):
        # File to file - check if update needed
        file_matches = _check_file_matches(
            object_store,
            full_path,
            entry.sha,
            entry.mode,
            current_stat,
            honor_filemode,
            blob_normalizer,
            path,
        )
        needs_update = not file_matches
    elif (
        current_stat is not None
        and stat.S_ISLNK(current_stat.st_mode)
        and stat.S_ISLNK(entry.mode)
    ):
        # Symlink to symlink - check if update needed
        symlink_matches = _check_symlink_matches(full_path, object_store, entry.sha)
        needs_update = not symlink_matches
    else:
        needs_update = True

    if not needs_update:
        # Just update index - current_stat should always be valid here since we're not updating
        index[path] = index_entry_from_stat(current_stat, entry.sha)
        return

    # Remove existing entry if needed
    if current_stat is not None and stat.S_ISDIR(current_stat.st_mode):
        # Remove directory
        dir_contents = set(os.listdir(full_path))
        git_file_name = b".git" if isinstance(full_path, bytes) else ".git"

        if git_file_name in dir_contents:
            if dir_contents != {git_file_name}:
                raise IsADirectoryError(
                    f"Cannot replace submodule with untracked files: {full_path!r}"
                )
            shutil.rmtree(full_path)
        else:
            try:
                os.rmdir(full_path)
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    raise IsADirectoryError(
                        f"Cannot replace non-empty directory with file: {full_path!r}"
                    )
                raise
    elif current_stat is not None:
        _remove_file_with_readonly_handling(full_path)

    # Ensure parent directory exists
    _ensure_parent_dir_exists(full_path)

    # Write the file
    blob_obj = object_store[entry.sha]
    assert isinstance(blob_obj, Blob)
    if blob_normalizer:
        blob_obj = blob_normalizer.checkout_normalize(blob_obj, path)
    st = build_file_from_blob(
        blob_obj,
        entry.mode,
        full_path,
        honor_filemode=honor_filemode,
        tree_encoding=tree_encoding,
        symlink_fn=symlink_fn,
    )
    index[path] = index_entry_from_stat(st, entry.sha)


def _transition_to_absent(repo, path, full_path, current_stat, index):
    """Remove any type of entry."""
    if current_stat is None:
        return

    if stat.S_ISDIR(current_stat.st_mode):
        # Check if it's a submodule directory
        dir_contents = set(os.listdir(full_path))
        git_file_name = b".git" if isinstance(full_path, bytes) else ".git"

        if git_file_name in dir_contents and dir_contents == {git_file_name}:
            shutil.rmtree(full_path)
        else:
            try:
                os.rmdir(full_path)
            except OSError as e:
                if e.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                    raise
    else:
        _remove_file_with_readonly_handling(full_path)

    try:
        del index[path]
    except KeyError:
        pass

    # Try to remove empty parent directories
    _remove_empty_parents(
        full_path, repo.path if isinstance(repo.path, bytes) else repo.path.encode()
    )


def detect_case_only_renames(
    changes: list["TreeChange"],
    config: "Config",
) -> list["TreeChange"]:
    """Detect and transform case-only renames in a list of tree changes.

    This function identifies file renames that only differ in case (e.g.,
    README.txt -> readme.txt) and transforms matching ADD/DELETE pairs into
    CHANGE_RENAME operations. It uses filesystem-appropriate path normalization
    based on the repository configuration.

    Args:
      changes: List of TreeChange objects representing file changes
      config: Repository configuration object

    Returns:
      New list of TreeChange objects with case-only renames converted to CHANGE_RENAME
    """
    from .diff_tree import (
        CHANGE_ADD,
        CHANGE_COPY,
        CHANGE_DELETE,
        CHANGE_MODIFY,
        CHANGE_RENAME,
        TreeChange,
    )

    # Build dictionaries of old and new paths with their normalized forms
    old_paths_normalized = {}
    new_paths_normalized = {}
    old_changes = {}  # Map from old path to change object
    new_changes = {}  # Map from new path to change object

    # Get the appropriate normalizer based on config
    normalize_func = get_path_element_normalizer(config)

    def normalize_path(path: bytes) -> bytes:
        """Normalize entire path using element normalization."""
        return b"/".join(normalize_func(part) for part in path.split(b"/"))

    # Pre-normalize all paths once to avoid repeated normalization
    for change in changes:
        if change.type == CHANGE_DELETE and change.old:
            try:
                normalized = normalize_path(change.old.path)
            except UnicodeDecodeError:
                import logging

                logging.warning(
                    "Skipping case-only rename detection for path with invalid UTF-8: %r",
                    change.old.path,
                )
            else:
                old_paths_normalized[normalized] = change.old.path
                old_changes[change.old.path] = change
        elif change.type == CHANGE_RENAME and change.old:
            # Treat RENAME as DELETE + ADD for case-only detection
            try:
                normalized = normalize_path(change.old.path)
            except UnicodeDecodeError:
                import logging

                logging.warning(
                    "Skipping case-only rename detection for path with invalid UTF-8: %r",
                    change.old.path,
                )
            else:
                old_paths_normalized[normalized] = change.old.path
                old_changes[change.old.path] = change

        if (
            change.type in (CHANGE_ADD, CHANGE_MODIFY, CHANGE_RENAME, CHANGE_COPY)
            and change.new
        ):
            try:
                normalized = normalize_path(change.new.path)
            except UnicodeDecodeError:
                import logging

                logging.warning(
                    "Skipping case-only rename detection for path with invalid UTF-8: %r",
                    change.new.path,
                )
            else:
                new_paths_normalized[normalized] = change.new.path
                new_changes[change.new.path] = change

    # Find case-only renames and transform changes
    case_only_renames = set()
    new_rename_changes = []

    for norm_path, old_path in old_paths_normalized.items():
        if norm_path in new_paths_normalized:
            new_path = new_paths_normalized[norm_path]
            if old_path != new_path:
                # Found a case-only rename
                old_change = old_changes[old_path]
                new_change = new_changes[new_path]

                # Create a CHANGE_RENAME to replace the DELETE and ADD/MODIFY pair
                if new_change.type == CHANGE_ADD:
                    # Simple case: DELETE + ADD becomes RENAME
                    rename_change = TreeChange(
                        CHANGE_RENAME, old_change.old, new_change.new
                    )
                else:
                    # Complex case: DELETE + MODIFY becomes RENAME
                    # Use the old file from DELETE and new file from MODIFY
                    rename_change = TreeChange(
                        CHANGE_RENAME, old_change.old, new_change.new
                    )

                new_rename_changes.append(rename_change)

                # Mark the old changes for removal
                case_only_renames.add(old_change)
                case_only_renames.add(new_change)

    # Return new list with original ADD/DELETE changes replaced by renames
    result = [change for change in changes if change not in case_only_renames]
    result.extend(new_rename_changes)
    return result


def update_working_tree(
    repo: "Repo",
    old_tree_id: Optional[bytes],
    new_tree_id: bytes,
    change_iterator: Iterator["TreeChange"],
    honor_filemode: bool = True,
    validate_path_element: Optional[Callable[[bytes], bool]] = None,
    symlink_fn: Optional[Callable] = None,
    force_remove_untracked: bool = False,
    blob_normalizer: Optional["BlobNormalizer"] = None,
    tree_encoding: str = "utf-8",
    allow_overwrite_modified: bool = False,
) -> None:
    """Update the working tree and index to match a new tree.

    This function handles:
    - Adding new files
    - Updating modified files
    - Removing deleted files
    - Cleaning up empty directories

    Args:
      repo: Repository object
      old_tree_id: SHA of the tree before the update
      new_tree_id: SHA of the tree to update to
      change_iterator: Iterator of TreeChange objects to apply
      honor_filemode: An optional flag to honor core.filemode setting
      validate_path_element: Function to validate path elements to check out
      symlink_fn: Function to use for creating symlinks
      force_remove_untracked: If True, remove files that exist in working
        directory but not in target tree, even if old_tree_id is None
      blob_normalizer: An optional BlobNormalizer to use for converting line
        endings when writing blobs to the working directory.
      tree_encoding: Encoding used for tree paths (default: utf-8)
      allow_overwrite_modified: If False, raise an error when attempting to
        overwrite files that have been modified compared to old_tree_id
    """
    if validate_path_element is None:
        validate_path_element = validate_path_element_default

    from .diff_tree import (
        CHANGE_ADD,
        CHANGE_COPY,
        CHANGE_DELETE,
        CHANGE_MODIFY,
        CHANGE_RENAME,
        CHANGE_UNCHANGED,
    )

    repo_path = repo.path if isinstance(repo.path, bytes) else repo.path.encode()
    index = repo.open_index()

    # Convert iterator to list since we need multiple passes
    changes = list(change_iterator)

    # Transform case-only renames on case-insensitive filesystems
    import platform

    default_ignore_case = platform.system() in ("Windows", "Darwin")
    config = repo.get_config()
    ignore_case = config.get_boolean((b"core",), b"ignorecase", default_ignore_case)

    if ignore_case:
        config = repo.get_config()
        changes = detect_case_only_renames(changes, config)

    # Check for path conflicts where files need to become directories
    paths_becoming_dirs = set()
    for change in changes:
        if change.type in (CHANGE_ADD, CHANGE_MODIFY, CHANGE_RENAME, CHANGE_COPY):
            path = change.new.path
            if b"/" in path:  # This is a file inside a directory
                # Check if any parent path exists as a file in the old tree or changes
                parts = path.split(b"/")
                for i in range(1, len(parts)):
                    parent = b"/".join(parts[:i])
                    # See if this parent path is being deleted (was a file, becoming a dir)
                    for other_change in changes:
                        if (
                            other_change.type == CHANGE_DELETE
                            and other_change.old
                            and other_change.old.path == parent
                        ):
                            paths_becoming_dirs.add(parent)

    # Check if any path that needs to become a directory has been modified
    for path in paths_becoming_dirs:
        full_path = _tree_to_fs_path(repo_path, path, tree_encoding)
        try:
            current_stat = os.lstat(full_path)
        except FileNotFoundError:
            continue  # File doesn't exist, nothing to check
        except OSError as e:
            raise OSError(
                f"Cannot access {path.decode('utf-8', errors='replace')}: {e}"
            ) from e

        if stat.S_ISREG(current_stat.st_mode):
            # Find the old entry for this path
            old_change = None
            for change in changes:
                if (
                    change.type == CHANGE_DELETE
                    and change.old
                    and change.old.path == path
                ):
                    old_change = change
                    break

            if old_change:
                # Check if file has been modified
                file_matches = _check_file_matches(
                    repo.object_store,
                    full_path,
                    old_change.old.sha,
                    old_change.old.mode,
                    current_stat,
                    honor_filemode,
                    blob_normalizer,
                    path,
                )
                if not file_matches:
                    raise OSError(
                        f"Cannot replace modified file with directory: {path!r}"
                    )

    # Check for uncommitted modifications before making any changes
    if not allow_overwrite_modified and old_tree_id:
        for change in changes:
            # Only check files that are being modified or deleted
            if change.type in (CHANGE_MODIFY, CHANGE_DELETE) and change.old:
                path = change.old.path
                if path.startswith(b".git") or not validate_path(
                    path, validate_path_element
                ):
                    continue

                full_path = _tree_to_fs_path(repo_path, path, tree_encoding)
                try:
                    current_stat = os.lstat(full_path)
                except FileNotFoundError:
                    continue  # File doesn't exist, nothing to check
                except OSError as e:
                    raise OSError(
                        f"Cannot access {path.decode('utf-8', errors='replace')}: {e}"
                    ) from e

                if stat.S_ISREG(current_stat.st_mode):
                    # Check if working tree file differs from old tree
                    file_matches = _check_file_matches(
                        repo.object_store,
                        full_path,
                        change.old.sha,
                        change.old.mode,
                        current_stat,
                        honor_filemode,
                        blob_normalizer,
                        path,
                    )
                    if not file_matches:
                        from .errors import WorkingTreeModifiedError

                        raise WorkingTreeModifiedError(
                            f"Your local changes to '{path.decode('utf-8', errors='replace')}' "
                            f"would be overwritten by checkout. "
                            f"Please commit your changes or stash them before you switch branches."
                        )

    # Apply the changes
    for change in changes:
        if change.type in (CHANGE_DELETE, CHANGE_RENAME):
            # Remove file/directory
            path = change.old.path
            if path.startswith(b".git") or not validate_path(
                path, validate_path_element
            ):
                continue

            full_path = _tree_to_fs_path(repo_path, path, tree_encoding)
            try:
                delete_stat: Optional[os.stat_result] = os.lstat(full_path)
            except FileNotFoundError:
                delete_stat = None
            except OSError as e:
                raise OSError(
                    f"Cannot access {path.decode('utf-8', errors='replace')}: {e}"
                ) from e

            _transition_to_absent(repo, path, full_path, delete_stat, index)

        if change.type in (
            CHANGE_ADD,
            CHANGE_MODIFY,
            CHANGE_UNCHANGED,
            CHANGE_COPY,
            CHANGE_RENAME,
        ):
            # Add or modify file
            path = change.new.path
            if path.startswith(b".git") or not validate_path(
                path, validate_path_element
            ):
                continue

            full_path = _tree_to_fs_path(repo_path, path, tree_encoding)
            try:
                modify_stat: Optional[os.stat_result] = os.lstat(full_path)
            except FileNotFoundError:
                modify_stat = None
            except OSError as e:
                raise OSError(
                    f"Cannot access {path.decode('utf-8', errors='replace')}: {e}"
                ) from e

            if S_ISGITLINK(change.new.mode):
                _transition_to_submodule(
                    repo, path, full_path, modify_stat, change.new, index
                )
            else:
                _transition_to_file(
                    repo.object_store,
                    path,
                    full_path,
                    modify_stat,
                    change.new,
                    index,
                    honor_filemode,
                    symlink_fn,
                    blob_normalizer,
                    tree_encoding,
                )

    index.write()


def get_unstaged_changes(
    index: Index,
    root_path: Union[str, bytes],
    filter_blob_callback: Optional[Callable] = None,
) -> Generator[bytes, None, None]:
    """Walk through an index and check for differences against working tree.

    Args:
      index: index to check
      root_path: path in which to find files
    Returns: iterator over paths with unstaged changes
    """
    # For each entry in the index check the sha1 & ensure not staged
    if not isinstance(root_path, bytes):
        root_path = os.fsencode(root_path)

    for tree_path, entry in index.iteritems():
        full_path = _tree_to_fs_path(root_path, tree_path)
        if isinstance(entry, ConflictedIndexEntry):
            # Conflicted files are always unstaged
            yield tree_path
            continue

        try:
            st = os.lstat(full_path)
            if stat.S_ISDIR(st.st_mode):
                if _has_directory_changed(tree_path, entry):
                    yield tree_path
                continue

            if not stat.S_ISREG(st.st_mode) and not stat.S_ISLNK(st.st_mode):
                continue

            blob = blob_from_path_and_stat(full_path, st)

            if filter_blob_callback is not None:
                blob = filter_blob_callback(blob, tree_path)
        except FileNotFoundError:
            # The file was removed, so we assume that counts as
            # different from whatever file used to exist.
            yield tree_path
        else:
            if blob.id != entry.sha:
                yield tree_path


def _tree_to_fs_path(
    root_path: bytes, tree_path: bytes, tree_encoding: str = "utf-8"
) -> bytes:
    """Convert a git tree path to a file system path.

    Args:
      root_path: Root filesystem path
      tree_path: Git tree path as bytes (encoded with tree_encoding)
      tree_encoding: Encoding used for tree paths (default: utf-8)

    Returns: File system path.
    """
    assert isinstance(tree_path, bytes)
    if os_sep_bytes != b"/":
        sep_corrected_path = tree_path.replace(b"/", os_sep_bytes)
    else:
        sep_corrected_path = tree_path

    # On Windows, we need to handle tree path encoding properly
    if sys.platform == "win32":
        # Decode from tree encoding, then re-encode for filesystem
        try:
            tree_path_str = sep_corrected_path.decode(tree_encoding)
            sep_corrected_path = os.fsencode(tree_path_str)
        except UnicodeDecodeError:
            # If decoding fails, use the original bytes
            pass

    return os.path.join(root_path, sep_corrected_path)


def _fs_to_tree_path(fs_path: Union[str, bytes], tree_encoding: str = "utf-8") -> bytes:
    """Convert a file system path to a git tree path.

    Args:
      fs_path: File system path.
      tree_encoding: Encoding to use for tree paths (default: utf-8)

    Returns:  Git tree path as bytes (encoded with tree_encoding)
    """
    if not isinstance(fs_path, bytes):
        fs_path_bytes = os.fsencode(fs_path)
    else:
        fs_path_bytes = fs_path

    # On Windows, we need to ensure tree paths are properly encoded
    if sys.platform == "win32":
        try:
            # Decode from filesystem encoding, then re-encode with tree encoding
            fs_path_str = os.fsdecode(fs_path_bytes)
            fs_path_bytes = fs_path_str.encode(tree_encoding)
        except UnicodeDecodeError:
            # If filesystem decoding fails, use the original bytes
            pass

    if os_sep_bytes != b"/":
        tree_path = fs_path_bytes.replace(os_sep_bytes, b"/")
    else:
        tree_path = fs_path_bytes
    return tree_path


def index_entry_from_directory(st: os.stat_result, path: bytes) -> Optional[IndexEntry]:
    if os.path.exists(os.path.join(path, b".git")):
        head = read_submodule_head(path)
        if head is None:
            return None
        return index_entry_from_stat(st, head, mode=S_IFGITLINK)
    return None


def index_entry_from_path(
    path: bytes, object_store: Optional[ObjectContainer] = None
) -> Optional[IndexEntry]:
    """Create an index from a filesystem path.

    This returns an index value for files, symlinks
    and tree references. for directories and
    non-existent files it returns None

    Args:
      path: Path to create an index entry for
      object_store: Optional object store to
        save new blobs in
    Returns: An index entry; None for directories
    """
    assert isinstance(path, bytes)
    st = os.lstat(path)
    if stat.S_ISDIR(st.st_mode):
        return index_entry_from_directory(st, path)

    if stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode):
        blob = blob_from_path_and_stat(path, st)
        if object_store is not None:
            object_store.add_object(blob)
        return index_entry_from_stat(st, blob.id)

    return None


def iter_fresh_entries(
    paths: Iterable[bytes],
    root_path: bytes,
    object_store: Optional[ObjectContainer] = None,
) -> Iterator[tuple[bytes, Optional[IndexEntry]]]:
    """Iterate over current versions of index entries on disk.

    Args:
      paths: Paths to iterate over
      root_path: Root path to access from
      object_store: Optional store to save new blobs in
    Returns: Iterator over path, index_entry
    """
    for path in paths:
        p = _tree_to_fs_path(root_path, path)
        try:
            entry = index_entry_from_path(p, object_store=object_store)
        except (FileNotFoundError, IsADirectoryError):
            entry = None
        yield path, entry


def iter_fresh_objects(
    paths: Iterable[bytes],
    root_path: bytes,
    include_deleted: bool = False,
    object_store: Optional[ObjectContainer] = None,
) -> Iterator[tuple[bytes, Optional[bytes], Optional[int]]]:
    """Iterate over versions of objects on disk referenced by index.

    Args:
      root_path: Root path to access from
      include_deleted: Include deleted entries with sha and
        mode set to None
      object_store: Optional object store to report new items to
    Returns: Iterator over path, sha, mode
    """
    for path, entry in iter_fresh_entries(paths, root_path, object_store=object_store):
        if entry is None:
            if include_deleted:
                yield path, None, None
        else:
            yield path, entry.sha, cleanup_mode(entry.mode)


def refresh_index(index: Index, root_path: bytes) -> None:
    """Refresh the contents of an index.

    This is the equivalent to running 'git commit -a'.

    Args:
      index: Index to update
      root_path: Root filesystem path
    """
    for path, entry in iter_fresh_entries(index, root_path):
        if entry:
            index[path] = entry


class locked_index:
    """Lock the index while making modifications.

    Works as a context manager.
    """

    _file: "_GitFile"

    def __init__(self, path: Union[bytes, str]) -> None:
        self._path = path

    def __enter__(self) -> Index:
        self._file = GitFile(self._path, "wb")
        self._index = Index(self._path)
        return self._index

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[types.TracebackType],
    ) -> None:
        if exc_type is not None:
            self._file.abort()
            return
        try:
            from typing import BinaryIO, cast

            f = SHA1Writer(cast(BinaryIO, self._file))
            write_index_dict(cast(BinaryIO, f), self._index._byname)
        except BaseException:
            self._file.abort()
        else:
            f.close()
