# pack.py -- For dealing with packed git objects.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
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

"""Classes for dealing with packed git objects.

A pack is a compact representation of a bunch of objects, stored
using deltas where possible.

They have two parts, the pack file, which stores the data, and an index
that tells you where the data is.

To find an object you look in all of the index files 'til you find a
match for the object name. You then use the pointer got from this as
a pointer in to the corresponding packfile.
"""

__all__ = [
    "DEFAULT_PACK_DELTA_WINDOW_SIZE",
    "DEFAULT_PACK_INDEX_VERSION",
    "DELTA_TYPES",
    "OFS_DELTA",
    "PACK_SPOOL_FILE_MAX_SIZE",
    "REF_DELTA",
    "DeltaChainIterator",
    "FilePackIndex",
    "MemoryPackIndex",
    "ObjectContainer",
    "Pack",
    "PackChunkGenerator",
    "PackData",
    "PackFileDisappeared",
    "PackHint",
    "PackIndex",
    "PackIndex1",
    "PackIndex2",
    "PackIndex3",
    "PackIndexEntry",
    "PackIndexer",
    "PackInflater",
    "PackStreamCopier",
    "PackStreamReader",
    "PackedObjectContainer",
    "SHA1Reader",
    "SHA1Writer",
    "UnpackedObject",
    "UnpackedObjectIterator",
    "UnpackedObjectStream",
    "UnresolvedDeltas",
    "apply_delta",
    "bisect_find_sha",
    "chunks_length",
    "compute_file_sha",
    "deltas_from_sorted_objects",
    "deltify_pack_objects",
    "extend_pack",
    "find_reusable_deltas",
    "full_unpacked_object",
    "generate_unpacked_objects",
    "iter_sha1",
    "load_pack_index",
    "load_pack_index_file",
    "obj_sha",
    "pack_header_chunks",
    "pack_object_chunks",
    "pack_object_header",
    "pack_objects_to_data",
    "read_pack_header",
    "read_zlib_chunks",
    "sort_objects_for_delta",
    "take_msb_bytes",
    "unpack_object",
    "write_pack",
    "write_pack_data",
    "write_pack_from_container",
    "write_pack_header",
    "write_pack_index",
    "write_pack_object",
    "write_pack_objects",
]

import binascii
from collections import defaultdict, deque
from contextlib import suppress
from io import BytesIO, UnsupportedOperation

try:
    from cdifflib import CSequenceMatcher as SequenceMatcher
except ModuleNotFoundError:
    from difflib import SequenceMatcher

import os
import struct
import sys
import warnings
import zlib
from collections.abc import Callable, Iterable, Iterator, Sequence, Set
from hashlib import sha1, sha256
from itertools import chain
from os import SEEK_CUR, SEEK_END
from struct import unpack_from
from types import TracebackType
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Generic,
    Protocol,
    TypeVar,
)

try:
    import mmap
except ImportError:
    has_mmap = False
else:
    has_mmap = True

if TYPE_CHECKING:
    from _hashlib import HASH as HashObject

    from .bitmap import PackBitmap
    from .commit_graph import CommitGraph
    from .object_store import BaseObjectStore
    from .ref import Ref

# For some reason the above try, except fails to set has_mmap = False for plan9
if sys.platform == "Plan9":
    has_mmap = False

from . import replace_me
from .errors import ApplyDeltaError, ChecksumMismatch
from .file import GitFile, _GitFile
from .lru_cache import LRUSizeCache
from .object_format import OBJECT_FORMAT_TYPE_NUMS, SHA1, ObjectFormat
from .objects import (
    ObjectID,
    RawObjectID,
    ShaFile,
    hex_to_sha,
    object_header,
    sha_to_hex,
)

OFS_DELTA = 6
REF_DELTA = 7

DELTA_TYPES = (OFS_DELTA, REF_DELTA)


DEFAULT_PACK_DELTA_WINDOW_SIZE = 10

# Keep pack files under 16Mb in memory, otherwise write them out to disk
PACK_SPOOL_FILE_MAX_SIZE = 16 * 1024 * 1024

# Default pack index version to use when none is specified
DEFAULT_PACK_INDEX_VERSION = 2


OldUnpackedObject = tuple[bytes | int, list[bytes]] | list[bytes]
ResolveExtRefFn = Callable[[bytes], tuple[int, OldUnpackedObject]]
ProgressFn = Callable[[int, str], None]
PackHint = tuple[int, bytes | None]


class UnresolvedDeltas(Exception):
    """Delta objects could not be resolved."""

    def __init__(self, shas: list[bytes]) -> None:
        """Initialize UnresolvedDeltas exception.

        Args:
            shas: List of SHA hashes for unresolved delta objects
        """
        self.shas = shas


class ObjectContainer(Protocol):
    """Protocol for objects that can contain git objects."""

    def add_object(self, obj: ShaFile) -> None:
        """Add a single object to this object store."""

    def add_objects(
        self,
        objects: Sequence[tuple[ShaFile, str | None]],
        progress: Callable[..., None] | None = None,
    ) -> "Pack | None":
        """Add a set of objects to this object store.

        Args:
          objects: Iterable over a list of (object, path) tuples
          progress: Progress callback for object insertion
        Returns: Optional Pack object of the objects written.
        """

    def __contains__(self, sha1: "ObjectID") -> bool:
        """Check if a hex sha is present."""

    def __getitem__(self, sha1: "ObjectID | RawObjectID") -> ShaFile:
        """Retrieve an object."""

    def get_commit_graph(self) -> "CommitGraph | None":
        """Get the commit graph for this object store.

        Returns:
          CommitGraph object if available, None otherwise
        """
        return None


class PackedObjectContainer(ObjectContainer):
    """Container for objects packed in a pack file."""

    def get_unpacked_object(
        self, sha1: "ObjectID | RawObjectID", *, include_comp: bool = False
    ) -> "UnpackedObject":
        """Get a raw unresolved object.

        Args:
            sha1: SHA-1 hash of the object
            include_comp: Whether to include compressed data

        Returns:
            UnpackedObject instance
        """
        raise NotImplementedError(self.get_unpacked_object)

    def iterobjects_subset(
        self, shas: Iterable["ObjectID"], *, allow_missing: bool = False
    ) -> Iterator[ShaFile]:
        """Iterate over a subset of objects.

        Args:
            shas: Iterable of object SHAs to retrieve
            allow_missing: If True, skip missing objects

        Returns:
            Iterator of ShaFile objects
        """
        raise NotImplementedError(self.iterobjects_subset)

    def iter_unpacked_subset(
        self,
        shas: Iterable["ObjectID | RawObjectID"],
        *,
        include_comp: bool = False,
        allow_missing: bool = False,
        convert_ofs_delta: bool = True,
    ) -> Iterator["UnpackedObject"]:
        """Iterate over unpacked objects from a subset of SHAs.

        Args:
          shas: Set of object SHAs to retrieve
          include_comp: Include compressed data if True
          allow_missing: If True, skip missing objects
          convert_ofs_delta: If True, convert offset deltas to ref deltas

        Returns:
          Iterator of UnpackedObject instances
        """
        raise NotImplementedError(self.iter_unpacked_subset)


class UnpackedObjectStream:
    """Abstract base class for a stream of unpacked objects."""

    def __iter__(self) -> Iterator["UnpackedObject"]:
        """Iterate over unpacked objects."""
        raise NotImplementedError(self.__iter__)

    def __len__(self) -> int:
        """Return the number of objects in the stream."""
        raise NotImplementedError(self.__len__)


def take_msb_bytes(
    read: Callable[[int], bytes], crc32: int | None = None
) -> tuple[list[int], int | None]:
    """Read bytes marked with most significant bit.

    Args:
      read: Read function
      crc32: Optional CRC32 checksum to update

    Returns:
      Tuple of (list of bytes read, updated CRC32 or None)
    """
    ret: list[int] = []
    while len(ret) == 0 or ret[-1] & 0x80:
        b = read(1)
        if crc32 is not None:
            crc32 = binascii.crc32(b, crc32)
        ret.append(ord(b[:1]))
    return ret, crc32


class PackFileDisappeared(Exception):
    """Raised when a pack file unexpectedly disappears."""

    def __init__(self, obj: object) -> None:
        """Initialize PackFileDisappeared exception.

        Args:
            obj: The object that triggered the exception
        """
        self.obj = obj


class UnpackedObject:
    """Class encapsulating an object unpacked from a pack file.

    These objects should only be created from within unpack_object. Most
    members start out as empty and are filled in at various points by
    read_zlib_chunks, unpack_object, DeltaChainIterator, etc.

    End users of this object should take care that the function they're getting
    this object from is guaranteed to set the members they need.
    """

    __slots__ = [
        "_sha",  # Cached binary SHA.
        "comp_chunks",  # Compressed object chunks.
        "crc32",  # CRC32.
        "decomp_chunks",  # Decompressed object chunks.
        "decomp_len",  # Decompressed length of this object.
        "delta_base",  # Delta base offset or SHA.
        "hash_func",  # Hash function to use for computing object IDs.
        "obj_chunks",  # Decompressed and delta-resolved chunks.
        "obj_type_num",  # Type of this object.
        "offset",  # Offset in its pack.
        "pack_type_num",  # Type of this object in the pack (may be a delta).
    ]

    obj_type_num: int | None
    obj_chunks: list[bytes] | None
    delta_base: None | bytes | int
    decomp_chunks: list[bytes]
    comp_chunks: list[bytes] | None
    decomp_len: int | None
    crc32: int | None
    offset: int | None
    pack_type_num: int
    _sha: bytes | None
    hash_func: Callable[[], "HashObject"]

    # TODO(dborowitz): read_zlib_chunks and unpack_object could very well be
    # methods of this object.
    def __init__(
        self,
        pack_type_num: int,
        *,
        delta_base: None | bytes | int = None,
        decomp_len: int | None = None,
        crc32: int | None = None,
        sha: bytes | None = None,
        decomp_chunks: list[bytes] | None = None,
        offset: int | None = None,
        hash_func: Callable[[], "HashObject"] = sha1,
    ) -> None:
        """Initialize an UnpackedObject.

        Args:
            pack_type_num: Type number of this object in the pack
            delta_base: Delta base (offset or SHA) if this is a delta object
            decomp_len: Decompressed length of this object
            crc32: CRC32 checksum
            sha: SHA hash of the object
            decomp_chunks: Decompressed chunks
            offset: Offset in the pack file
            hash_func: Hash function to use (defaults to sha1)
        """
        self.offset = offset
        self._sha = sha
        self.pack_type_num = pack_type_num
        self.delta_base = delta_base
        self.comp_chunks = None
        self.decomp_chunks: list[bytes] = decomp_chunks or []
        if decomp_chunks is not None and decomp_len is None:
            self.decomp_len = sum(map(len, decomp_chunks))
        else:
            self.decomp_len = decomp_len
        self.crc32 = crc32
        self.hash_func = hash_func

        if pack_type_num in DELTA_TYPES:
            self.obj_type_num = None
            self.obj_chunks = None
        else:
            self.obj_type_num = pack_type_num
            self.obj_chunks = self.decomp_chunks
            self.delta_base = delta_base

    def sha(self) -> RawObjectID:
        """Return the binary SHA of this object."""
        if self._sha is None:
            assert self.obj_type_num is not None and self.obj_chunks is not None
            self._sha = obj_sha(self.obj_type_num, self.obj_chunks, self.hash_func)
        return RawObjectID(self._sha)

    def sha_file(self) -> ShaFile:
        """Return a ShaFile from this object."""
        assert self.obj_type_num is not None and self.obj_chunks is not None
        return ShaFile.from_raw_chunks(self.obj_type_num, self.obj_chunks)

    # Only provided for backwards compatibility with code that expects either
    # chunks or a delta tuple.
    def _obj(self) -> OldUnpackedObject:
        """Return the decompressed chunks, or (delta base, delta chunks)."""
        if self.pack_type_num in DELTA_TYPES:
            assert isinstance(self.delta_base, (bytes, int))
            return (self.delta_base, self.decomp_chunks)
        else:
            return self.decomp_chunks

    def __eq__(self, other: object) -> bool:
        """Check equality with another UnpackedObject."""
        if not isinstance(other, UnpackedObject):
            return False
        for slot in self.__slots__:
            if getattr(self, slot) != getattr(other, slot):
                return False
        return True

    def __ne__(self, other: object) -> bool:
        """Check inequality with another UnpackedObject."""
        return not (self == other)

    def __repr__(self) -> str:
        """Return string representation of this UnpackedObject."""
        data = [f"{s}={getattr(self, s)!r}" for s in self.__slots__]
        return "{}({})".format(self.__class__.__name__, ", ".join(data))


_ZLIB_BUFSIZE = 65536  # 64KB buffer for better I/O performance


def read_zlib_chunks(
    read_some: Callable[[int], bytes],
    unpacked: UnpackedObject,
    include_comp: bool = False,
    buffer_size: int = _ZLIB_BUFSIZE,
) -> bytes:
    """Read zlib data from a buffer.

    This function requires that the buffer have additional data following the
    compressed data, which is guaranteed to be the case for git pack files.

    Args:
      read_some: Read function that returns at least one byte, but may
        return less than the requested size.
      unpacked: An UnpackedObject to write result data to. If its crc32
        attr is not None, the CRC32 of the compressed bytes will be computed
        using this starting CRC32.
        After this function, will have the following attrs set:
        * comp_chunks    (if include_comp is True)
        * decomp_chunks
        * decomp_len
        * crc32
      include_comp: If True, include compressed data in the result.
      buffer_size: Size of the read buffer.
    Returns: Leftover unused data from the decompression.

    Raises:
      zlib.error: if a decompression error occurred.
    """
    if unpacked.decomp_len is None or unpacked.decomp_len <= -1:
        raise ValueError("non-negative zlib data stream size expected")
    decomp_obj = zlib.decompressobj()

    comp_chunks = []
    decomp_chunks = unpacked.decomp_chunks
    decomp_len = 0
    crc32 = unpacked.crc32

    while True:
        add = read_some(buffer_size)
        if not add:
            raise zlib.error("EOF before end of zlib stream")
        comp_chunks.append(add)
        decomp = decomp_obj.decompress(add)
        decomp_len += len(decomp)
        decomp_chunks.append(decomp)
        unused = decomp_obj.unused_data
        if unused:
            left = len(unused)
            if crc32 is not None:
                crc32 = binascii.crc32(add[:-left], crc32)
            if include_comp:
                comp_chunks[-1] = add[:-left]
            break
        elif crc32 is not None:
            crc32 = binascii.crc32(add, crc32)
    if crc32 is not None:
        crc32 &= 0xFFFFFFFF

    if decomp_len != unpacked.decomp_len:
        raise zlib.error("decompressed data does not match expected size")

    unpacked.crc32 = crc32
    if include_comp:
        unpacked.comp_chunks = comp_chunks
    return unused


def iter_sha1(iter: Iterable[bytes]) -> bytes:
    """Return the hexdigest of the SHA1 over a set of names.

    Args:
      iter: Iterator over string objects
    Returns: 40-byte hex sha1 digest
    """
    sha = sha1()
    for name in iter:
        sha.update(name)
    return sha.hexdigest().encode("ascii")


def load_pack_index(
    path: str | os.PathLike[str], object_format: ObjectFormat
) -> "PackIndex":
    """Load an index file by path.

    Args:
      path: Path to the index file
      object_format: Hash algorithm used by the repository
    Returns: A PackIndex loaded from the given path
    """
    with GitFile(path, "rb") as f:
        return load_pack_index_file(path, f, object_format)


def _load_file_contents(
    f: IO[bytes] | _GitFile, size: int | None = None
) -> tuple[bytes | Any, int]:
    """Load contents from a file, preferring mmap when possible.

    Args:
      f: File-like object to load
      size: Expected size, or None to determine from file
    Returns: Tuple of (contents, size)
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
    contents_bytes = f.read()
    size = len(contents_bytes)
    return contents_bytes, size


def load_pack_index_file(
    path: str | os.PathLike[str],
    f: IO[bytes] | _GitFile,
    object_format: ObjectFormat,
) -> "PackIndex":
    """Load an index file from a file-like object.

    Args:
      path: Path for the index file
      f: File-like object
      object_format: Hash algorithm used by the repository
    Returns: A PackIndex loaded from the given file
    """
    contents, size = _load_file_contents(f)
    if contents[:4] == b"\377tOc":
        version = struct.unpack(b">L", contents[4:8])[0]
        if version == 2:
            return PackIndex2(
                path,
                object_format,
                file=f,
                contents=contents,
                size=size,
            )
        elif version == 3:
            return PackIndex3(path, object_format, file=f, contents=contents, size=size)
        else:
            raise KeyError(f"Unknown pack index format {version}")
    else:
        return PackIndex1(path, object_format, file=f, contents=contents, size=size)


def bisect_find_sha(
    start: int, end: int, sha: bytes, unpack_name: Callable[[int], bytes]
) -> int | None:
    """Find a SHA in a data blob with sorted SHAs.

    Args:
      start: Start index of range to search
      end: End index of range to search
      sha: Sha to find
      unpack_name: Callback to retrieve SHA by index
    Returns: Index of the SHA, or None if it wasn't found
    """
    assert start <= end
    while start <= end:
        i = (start + end) // 2
        file_sha = unpack_name(i)
        if file_sha < sha:
            start = i + 1
        elif file_sha > sha:
            end = i - 1
        else:
            return i
    return None


PackIndexEntry = tuple[RawObjectID, int, int | None]


class PackIndex:
    """An index in to a packfile.

    Given a sha id of an object a pack index can tell you the location in the
    packfile of that object if it has it.
    """

    object_format: "ObjectFormat"

    def __eq__(self, other: object) -> bool:
        """Check equality with another PackIndex."""
        if not isinstance(other, PackIndex):
            return False

        for (name1, _, _), (name2, _, _) in zip(
            self.iterentries(), other.iterentries()
        ):
            if name1 != name2:
                return False
        return True

    def __ne__(self, other: object) -> bool:
        """Check if this pack index is not equal to another."""
        return not self.__eq__(other)

    def __len__(self) -> int:
        """Return the number of entries in this pack index."""
        raise NotImplementedError(self.__len__)

    def __iter__(self) -> Iterator[ObjectID]:
        """Iterate over the SHAs in this pack."""
        return map(lambda sha: sha_to_hex(RawObjectID(sha)), self._itersha())

    def iterentries(self) -> Iterator[PackIndexEntry]:
        """Iterate over the entries in this pack index.

        Returns: iterator over tuples with object name, offset in packfile and
            crc32 checksum.
        """
        raise NotImplementedError(self.iterentries)

    def get_pack_checksum(self) -> bytes | None:
        """Return the SHA1 checksum stored for the corresponding packfile.

        Returns: 20-byte binary digest, or None if not available
        """
        raise NotImplementedError(self.get_pack_checksum)

    @replace_me(since="0.21.0", remove_in="0.23.0")
    def object_index(self, sha: ObjectID | RawObjectID) -> int:
        """Return the index for the given SHA.

        Args:
            sha: SHA-1 hash

        Returns:
            Index position
        """
        return self.object_offset(sha)

    def object_offset(self, sha: ObjectID | RawObjectID) -> int:
        """Return the offset in to the corresponding packfile for the object.

        Given the name of an object it will return the offset that object
        lives at within the corresponding pack file. If the pack file doesn't
        have the object then None will be returned.
        """
        raise NotImplementedError(self.object_offset)

    def object_sha1(self, index: int) -> bytes:
        """Return the SHA1 corresponding to the index in the pack file."""
        for name, offset, _crc32 in self.iterentries():
            if offset == index:
                return name
        else:
            raise KeyError(index)

    def _object_offset(self, sha: bytes) -> int:
        """See object_offset.

        Args:
          sha: A *binary* SHA string. (20 characters long)_
        """
        raise NotImplementedError(self._object_offset)

    def objects_sha1(self) -> bytes:
        """Return the hex SHA1 over all the shas of all objects in this pack.

        Note: This is used for the filename of the pack.
        """
        return iter_sha1(self._itersha())

    def _itersha(self) -> Iterator[bytes]:
        """Yield all the SHA1's of the objects in the index, sorted."""
        raise NotImplementedError(self._itersha)

    def iter_prefix(self, prefix: bytes) -> Iterator[RawObjectID]:
        """Iterate over all SHA1s with the given prefix.

        Args:
            prefix: Binary prefix to match
        Returns: Iterator of matching SHA1s
        """
        # Default implementation for PackIndex classes that don't override
        for sha, _, _ in self.iterentries():
            if sha.startswith(prefix):
                yield RawObjectID(sha)

    def close(self) -> None:
        """Close any open files."""

    def check(self) -> None:
        """Check the consistency of this pack index."""


class MemoryPackIndex(PackIndex):
    """Pack index that is stored entirely in memory."""

    def __init__(
        self,
        entries: list[PackIndexEntry],
        object_format: ObjectFormat,
        pack_checksum: bytes | None = None,
    ) -> None:
        """Create a new MemoryPackIndex.

        Args:
          entries: Sequence of name, idx, crc32 (sorted)
          object_format: Object format used by this index
          pack_checksum: Optional pack checksum
        """
        self._by_sha = {}
        self._by_offset = {}
        for name, offset, _crc32 in entries:
            self._by_sha[name] = offset
            self._by_offset[offset] = name
        self._entries = entries
        self._pack_checksum = pack_checksum
        self.object_format = object_format

    def get_pack_checksum(self) -> bytes | None:
        """Return the SHA checksum stored for the corresponding packfile."""
        return self._pack_checksum

    def __len__(self) -> int:
        """Return the number of entries in this pack index."""
        return len(self._entries)

    def object_offset(self, sha: ObjectID | RawObjectID) -> int:
        """Return the offset for the given SHA.

        Args:
          sha: SHA to look up (binary or hex)
        Returns: Offset in the pack file
        """
        lookup_sha: RawObjectID
        if len(sha) == self.object_format.hex_length:
            lookup_sha = hex_to_sha(ObjectID(sha))
        else:
            lookup_sha = RawObjectID(sha)
        return self._by_sha[lookup_sha]

    def object_sha1(self, offset: int) -> bytes:
        """Return the SHA1 for the object at the given offset."""
        return self._by_offset[offset]

    def _itersha(self) -> Iterator[bytes]:
        """Iterate over all SHA1s in the index."""
        return iter(self._by_sha)

    def iterentries(self) -> Iterator[PackIndexEntry]:
        """Iterate over all index entries."""
        return iter(self._entries)

    @classmethod
    def for_pack(cls, pack_data: "PackData") -> "MemoryPackIndex":
        """Create a MemoryPackIndex from a PackData object."""
        return MemoryPackIndex(
            list(pack_data.sorted_entries()),
            pack_checksum=pack_data.get_stored_checksum(),
            object_format=pack_data.object_format,
        )

    @classmethod
    def clone(cls, other_index: "PackIndex") -> "MemoryPackIndex":
        """Create a copy of another PackIndex in memory."""
        return cls(
            list(other_index.iterentries()),
            other_index.object_format,
            other_index.get_pack_checksum(),
        )


class FilePackIndex(PackIndex):
    """Pack index that is based on a file.

    To do the loop it opens the file, and indexes first 256 4 byte groups
    with the first byte of the sha id. The value in the four byte group indexed
    is the end of the group that shares the same starting byte. Subtract one
    from the starting byte and index again to find the start of the group.
    The values are sorted by sha id within the group, so do the math to find
    the start and end offset and then bisect in to find if the value is
    present.
    """

    _fan_out_table: list[int]
    _file: IO[bytes] | _GitFile

    def __init__(
        self,
        filename: str | os.PathLike[str],
        file: IO[bytes] | _GitFile | None = None,
        contents: "bytes | mmap.mmap | None" = None,
        size: int | None = None,
    ) -> None:
        """Create a pack index object.

        Provide it with the name of the index file to consider, and it will map
        it whenever required.
        """
        self._filename = filename
        # Take the size now, so it can be checked each time we map the file to
        # ensure that it hasn't changed.
        if file is None:
            self._file = GitFile(filename, "rb")
        else:
            self._file = file
        if contents is None:
            self._contents, self._size = _load_file_contents(self._file, size)
        else:
            self._contents = contents
            self._size = size if size is not None else len(contents)

    @property
    def path(self) -> str:
        """Return the path to this index file."""
        return os.fspath(self._filename)

    def __eq__(self, other: object) -> bool:
        """Check equality with another FilePackIndex."""
        # Quick optimization:
        if (
            isinstance(other, FilePackIndex)
            and self._fan_out_table != other._fan_out_table
        ):
            return False

        return super().__eq__(other)

    def close(self) -> None:
        """Close the underlying file and any mmap."""
        self._file.close()
        close_fn = getattr(self._contents, "close", None)
        if close_fn is not None:
            close_fn()

    def __len__(self) -> int:
        """Return the number of entries in this pack index."""
        return self._fan_out_table[-1]

    def _unpack_entry(self, i: int) -> PackIndexEntry:
        """Unpack the i-th entry in the index file.

        Returns: Tuple with object name (SHA), offset in pack file and CRC32
            checksum (if known).
        """
        raise NotImplementedError(self._unpack_entry)

    def _unpack_name(self, i: int) -> bytes:
        """Unpack the i-th name from the index file."""
        raise NotImplementedError(self._unpack_name)

    def _unpack_offset(self, i: int) -> int:
        """Unpack the i-th object offset from the index file."""
        raise NotImplementedError(self._unpack_offset)

    def _unpack_crc32_checksum(self, i: int) -> int | None:
        """Unpack the crc32 checksum for the ith object from the index file."""
        raise NotImplementedError(self._unpack_crc32_checksum)

    def _itersha(self) -> Iterator[bytes]:
        """Iterate over all SHA1s in the index."""
        for i in range(len(self)):
            yield self._unpack_name(i)

    def iterentries(self) -> Iterator[PackIndexEntry]:
        """Iterate over the entries in this pack index.

        Returns: iterator over tuples with object name, offset in packfile and
            crc32 checksum.
        """
        for i in range(len(self)):
            yield self._unpack_entry(i)

    def _read_fan_out_table(self, start_offset: int) -> list[int]:
        """Read the fan-out table from the index.

        The fan-out table contains 256 entries mapping first byte values
        to the number of objects with SHA1s less than or equal to that byte.

        Args:
          start_offset: Offset in the file where the fan-out table starts
        Returns: List of 256 integers
        """
        ret = []
        for i in range(0x100):
            fanout_entry = self._contents[
                start_offset + i * 4 : start_offset + (i + 1) * 4
            ]
            ret.append(struct.unpack(">L", fanout_entry)[0])
        return ret

    def check(self) -> None:
        """Check that the stored checksum matches the actual checksum."""
        actual = self.calculate_checksum()
        stored = self.get_stored_checksum()
        if actual != stored:
            raise ChecksumMismatch(stored, actual)

    def calculate_checksum(self) -> bytes:
        """Calculate the SHA1 checksum over this pack index.

        Returns: This is a 20-byte binary digest
        """
        return sha1(self._contents[:-20]).digest()

    def get_pack_checksum(self) -> bytes:
        """Return the SHA1 checksum stored for the corresponding packfile.

        Returns: 20-byte binary digest
        """
        return bytes(self._contents[-40:-20])

    def get_stored_checksum(self) -> bytes:
        """Return the SHA1 checksum stored for this index.

        Returns: 20-byte binary digest
        """
        return bytes(self._contents[-20:])

    def object_offset(self, sha: ObjectID | RawObjectID) -> int:
        """Return the offset in to the corresponding packfile for the object.

        Given the name of an object it will return the offset that object
        lives at within the corresponding pack file. If the pack file doesn't
        have the object then None will be returned.
        """
        lookup_sha: RawObjectID
        if len(sha) == self.object_format.hex_length:  # hex string
            lookup_sha = hex_to_sha(ObjectID(sha))
        else:
            lookup_sha = RawObjectID(sha)
        try:
            return self._object_offset(lookup_sha)
        except ValueError as exc:
            closed = getattr(self._contents, "closed", None)
            if closed in (None, True):
                raise PackFileDisappeared(self) from exc
            raise

    def _object_offset(self, sha: bytes) -> int:
        """See object_offset.

        Args:
          sha: A *binary* SHA string. (20 characters long)_
        """
        hash_size = getattr(self, "hash_size", 20)  # Default to SHA1 for v1
        assert len(sha) == hash_size
        idx = ord(sha[:1])
        if idx == 0:
            start = 0
        else:
            start = self._fan_out_table[idx - 1]
        end = self._fan_out_table[idx]
        i = bisect_find_sha(start, end, sha, self._unpack_name)
        if i is None:
            raise KeyError(sha)
        return self._unpack_offset(i)

    def iter_prefix(self, prefix: bytes) -> Iterator[RawObjectID]:
        """Iterate over all SHA1s with the given prefix."""
        start = ord(prefix[:1])
        if start == 0:
            start = 0
        else:
            start = self._fan_out_table[start - 1]
        end = ord(prefix[:1]) + 1
        if end == 0x100:
            end = len(self)
        else:
            end = self._fan_out_table[end]
        assert start <= end
        started = False
        for i in range(start, end):
            name: bytes = self._unpack_name(i)
            if name.startswith(prefix):
                yield RawObjectID(name)
                started = True
            elif started:
                break


class PackIndex1(FilePackIndex):
    """Version 1 Pack Index file."""

    object_format = SHA1

    def __init__(
        self,
        filename: str | os.PathLike[str],
        object_format: ObjectFormat,
        file: IO[bytes] | _GitFile | None = None,
        contents: bytes | None = None,
        size: int | None = None,
    ) -> None:
        """Initialize a version 1 pack index.

        Args:
            filename: Path to the index file
            object_format: Object format used by the repository
            file: Optional file object
            contents: Optional mmap'd contents
            size: Optional size of the index
        """
        super().__init__(filename, file, contents, size)

        # PackIndex1 only supports SHA1
        if object_format != SHA1:
            raise AssertionError(
                f"PackIndex1 only supports SHA1, not {object_format.name}"
            )

        self.object_format = object_format
        self.version = 1
        self._fan_out_table = self._read_fan_out_table(0)
        self.hash_size = self.object_format.oid_length
        self._entry_size = 4 + self.hash_size

    def _unpack_entry(self, i: int) -> tuple[RawObjectID, int, None]:
        base_offset = (0x100 * 4) + (i * self._entry_size)
        offset = unpack_from(">L", self._contents, base_offset)[0]
        name = self._contents[base_offset + 4 : base_offset + 4 + self.hash_size]
        return (RawObjectID(name), offset, None)

    def _unpack_name(self, i: int) -> bytes:
        offset = (0x100 * 4) + (i * self._entry_size) + 4
        return self._contents[offset : offset + self.hash_size]

    def _unpack_offset(self, i: int) -> int:
        offset = (0x100 * 4) + (i * self._entry_size)
        return int(unpack_from(">L", self._contents, offset)[0])

    def _unpack_crc32_checksum(self, i: int) -> None:
        # Not stored in v1 index files
        return None


class PackIndex2(FilePackIndex):
    """Version 2 Pack Index file."""

    object_format = SHA1

    def __init__(
        self,
        filename: str | os.PathLike[str],
        object_format: ObjectFormat,
        file: IO[bytes] | _GitFile | None = None,
        contents: bytes | None = None,
        size: int | None = None,
    ) -> None:
        """Initialize a version 2 pack index.

        Args:
            filename: Path to the index file
            object_format: Object format used by the repository
            file: Optional file object
            contents: Optional mmap'd contents
            size: Optional size of the index
        """
        super().__init__(filename, file, contents, size)
        self.object_format = object_format
        if self._contents[:4] != b"\377tOc":
            raise AssertionError("Not a v2 pack index file")
        (self.version,) = unpack_from(b">L", self._contents, 4)
        if self.version != 2:
            raise AssertionError(f"Version was {self.version}")
        self._fan_out_table = self._read_fan_out_table(8)
        self.hash_size = self.object_format.oid_length
        self._name_table_offset = 8 + 0x100 * 4
        self._crc32_table_offset = self._name_table_offset + self.hash_size * len(self)
        self._pack_offset_table_offset = self._crc32_table_offset + 4 * len(self)
        self._pack_offset_largetable_offset = self._pack_offset_table_offset + 4 * len(
            self
        )

    def _unpack_entry(self, i: int) -> tuple[RawObjectID, int, int]:
        return (
            RawObjectID(self._unpack_name(i)),
            self._unpack_offset(i),
            self._unpack_crc32_checksum(i),
        )

    def _unpack_name(self, i: int) -> bytes:
        offset = self._name_table_offset + i * self.hash_size
        return self._contents[offset : offset + self.hash_size]

    def _unpack_offset(self, i: int) -> int:
        offset = self._pack_offset_table_offset + i * 4
        offset_val = int(unpack_from(">L", self._contents, offset)[0])
        if offset_val & (2**31):
            offset = (
                self._pack_offset_largetable_offset + (offset_val & (2**31 - 1)) * 8
            )
            offset_val = int(unpack_from(">Q", self._contents, offset)[0])
        return offset_val

    def _unpack_crc32_checksum(self, i: int) -> int:
        return int(
            unpack_from(">L", self._contents, self._crc32_table_offset + i * 4)[0]
        )

    def get_pack_checksum(self) -> bytes:
        """Return the checksum stored for the corresponding packfile.

        Returns: binary digest (size depends on hash algorithm)
        """
        # Index ends with: pack_checksum + index_checksum
        # Each checksum is hash_size bytes
        checksum_size = self.hash_size
        return bytes(self._contents[-2 * checksum_size : -checksum_size])

    def get_stored_checksum(self) -> bytes:
        """Return the checksum stored for this index.

        Returns: binary digest (size depends on hash algorithm)
        """
        checksum_size = self.hash_size
        return bytes(self._contents[-checksum_size:])

    def calculate_checksum(self) -> bytes:
        """Calculate the checksum over this pack index.

        Returns: binary digest (size depends on hash algorithm)
        """
        # Determine hash function based on hash_size
        if self.hash_size == 20:
            hash_func = sha1
        elif self.hash_size == 32:
            hash_func = sha256
        else:
            raise ValueError(f"Unsupported hash size: {self.hash_size}")

        return hash_func(self._contents[: -self.hash_size]).digest()


class PackIndex3(FilePackIndex):
    """Version 3 Pack Index file.

    Supports variable hash sizes for SHA-1 (20 bytes) and SHA-256 (32 bytes).
    """

    def __init__(
        self,
        filename: str | os.PathLike[str],
        object_format: ObjectFormat,
        file: IO[bytes] | _GitFile | None = None,
        contents: bytes | None = None,
        size: int | None = None,
    ) -> None:
        """Initialize a version 3 pack index.

        Args:
            filename: Path to the index file
            object_format: Object format used by the repository
            file: Optional file object
            contents: Optional mmap'd contents
            size: Optional size of the index
        """
        super().__init__(filename, file, contents, size)
        if self._contents[:4] != b"\377tOc":
            raise AssertionError("Not a v3 pack index file")
        (self.version,) = unpack_from(b">L", self._contents, 4)
        if self.version != 3:
            raise AssertionError(f"Version was {self.version}")

        # Read hash algorithm identifier (1 = SHA-1, 2 = SHA-256)
        (self.hash_format,) = unpack_from(b">L", self._contents, 8)
        file_object_format = OBJECT_FORMAT_TYPE_NUMS[self.hash_format]

        # Verify provided object_format matches what's in the file
        if object_format != file_object_format:
            raise AssertionError(
                f"Object format mismatch: provided {object_format.name}, "
                f"but file contains {file_object_format.name}"
            )

        self.object_format = object_format
        self.hash_size = self.object_format.oid_length

        # Read length of shortened object names
        (self.shortened_oid_len,) = unpack_from(b">L", self._contents, 12)

        # Calculate offsets based on variable hash size
        self._fan_out_table = self._read_fan_out_table(
            16
        )  # After header (4 + 4 + 4 + 4)
        self._name_table_offset = 16 + 0x100 * 4
        self._crc32_table_offset = self._name_table_offset + self.hash_size * len(self)
        self._pack_offset_table_offset = self._crc32_table_offset + 4 * len(self)
        self._pack_offset_largetable_offset = self._pack_offset_table_offset + 4 * len(
            self
        )

    def _unpack_entry(self, i: int) -> tuple[RawObjectID, int, int]:
        return (
            RawObjectID(self._unpack_name(i)),
            self._unpack_offset(i),
            self._unpack_crc32_checksum(i),
        )

    def _unpack_name(self, i: int) -> bytes:
        offset = self._name_table_offset + i * self.hash_size
        return self._contents[offset : offset + self.hash_size]

    def _unpack_offset(self, i: int) -> int:
        offset_pos = self._pack_offset_table_offset + i * 4
        offset = unpack_from(">L", self._contents, offset_pos)[0]
        assert isinstance(offset, int)
        if offset & (2**31):
            large_offset_pos = (
                self._pack_offset_largetable_offset + (offset & (2**31 - 1)) * 8
            )
            offset = unpack_from(">Q", self._contents, large_offset_pos)[0]
            assert isinstance(offset, int)
        return offset

    def _unpack_crc32_checksum(self, i: int) -> int:
        result = unpack_from(">L", self._contents, self._crc32_table_offset + i * 4)[0]
        assert isinstance(result, int)
        return result


def read_pack_header(read: Callable[[int], bytes]) -> tuple[int, int]:
    """Read the header of a pack file.

    Args:
      read: Read function
    Returns: Tuple of (pack version, number of objects). If no data is
        available to read, returns (None, None).
    """
    header = read(12)
    if not header:
        raise AssertionError("file too short to contain pack")
    if header[:4] != b"PACK":
        raise AssertionError(f"Invalid pack header {header!r}")
    (version,) = unpack_from(b">L", header, 4)
    if version not in (2, 3):
        raise AssertionError(f"Version was {version}")
    (num_objects,) = unpack_from(b">L", header, 8)
    return (version, num_objects)


def chunks_length(chunks: bytes | Iterable[bytes]) -> int:
    """Get the total length of a sequence of chunks.

    Args:
      chunks: Either a single bytes object or an iterable of bytes
    Returns: Total length in bytes
    """
    if isinstance(chunks, bytes):
        return len(chunks)
    else:
        return sum(map(len, chunks))


def unpack_object(
    read_all: Callable[[int], bytes],
    hash_func: Callable[[], "HashObject"],
    read_some: Callable[[int], bytes] | None = None,
    compute_crc32: bool = False,
    include_comp: bool = False,
    zlib_bufsize: int = _ZLIB_BUFSIZE,
) -> tuple[UnpackedObject, bytes]:
    """Unpack a Git object.

    Args:
      read_all: Read function that blocks until the number of requested
        bytes are read.
      hash_func: Hash function to use for computing object IDs.
      read_some: Read function that returns at least one byte, but may not
        return the number of bytes requested.
      compute_crc32: If True, compute the CRC32 of the compressed data. If
        False, the returned CRC32 will be None.
      include_comp: If True, include compressed data in the result.
      zlib_bufsize: An optional buffer size for zlib operations.
    Returns: A tuple of (unpacked, unused), where unused is the unused data
        leftover from decompression, and unpacked in an UnpackedObject with
        the following attrs set:

        * obj_chunks     (for non-delta types)
        * pack_type_num
        * delta_base     (for delta types)
        * comp_chunks    (if include_comp is True)
        * decomp_chunks
        * decomp_len
        * crc32          (if compute_crc32 is True)
    """
    if read_some is None:
        read_some = read_all
    if compute_crc32:
        crc32 = 0
    else:
        crc32 = None

    raw, crc32 = take_msb_bytes(read_all, crc32=crc32)
    type_num = (raw[0] >> 4) & 0x07
    size = raw[0] & 0x0F
    for i, byte in enumerate(raw[1:]):
        size += (byte & 0x7F) << ((i * 7) + 4)

    delta_base: int | bytes | None
    raw_base = len(raw)
    if type_num == OFS_DELTA:
        raw, crc32 = take_msb_bytes(read_all, crc32=crc32)
        raw_base += len(raw)
        if raw[-1] & 0x80:
            raise AssertionError
        delta_base_offset = raw[0] & 0x7F
        for byte in raw[1:]:
            delta_base_offset += 1
            delta_base_offset <<= 7
            delta_base_offset += byte & 0x7F
        delta_base = delta_base_offset
    elif type_num == REF_DELTA:
        # Determine hash size from hash_func
        hash_size = len(hash_func().digest())
        delta_base_obj = read_all(hash_size)
        if crc32 is not None:
            crc32 = binascii.crc32(delta_base_obj, crc32)
        delta_base = delta_base_obj
        raw_base += hash_size
    else:
        delta_base = None

    unpacked = UnpackedObject(
        type_num,
        delta_base=delta_base,
        decomp_len=size,
        crc32=crc32,
        hash_func=hash_func,
    )
    unused = read_zlib_chunks(
        read_some,
        unpacked,
        buffer_size=zlib_bufsize,
        include_comp=include_comp,
    )
    return unpacked, unused


def _compute_object_size(value: tuple[int, Any]) -> int:
    """Compute the size of a unresolved object for use with LRUSizeCache."""
    (num, obj) = value
    if num in DELTA_TYPES:
        return chunks_length(obj[1])
    return chunks_length(obj)


class PackStreamReader:
    """Class to read a pack stream.

    The pack is read from a ReceivableProtocol using read() or recv() as
    appropriate.
    """

    def __init__(
        self,
        hash_func: Callable[[], "HashObject"],
        read_all: Callable[[int], bytes],
        read_some: Callable[[int], bytes] | None = None,
        zlib_bufsize: int = _ZLIB_BUFSIZE,
    ) -> None:
        """Initialize pack stream reader.

        Args:
            hash_func: Hash function to use for computing object IDs
            read_all: Function to read all requested bytes
            read_some: Function to read some bytes (optional)
            zlib_bufsize: Buffer size for zlib decompression
        """
        self.read_all = read_all
        if read_some is None:
            self.read_some = read_all
        else:
            self.read_some = read_some
        self.hash_func = hash_func
        self.sha = hash_func()
        self._hash_size = len(hash_func().digest())
        self._offset = 0
        self._rbuf = BytesIO()
        # trailer is a deque to avoid memory allocation on small reads
        self._trailer: deque[int] = deque()
        self._zlib_bufsize = zlib_bufsize

    def _read(self, read: Callable[[int], bytes], size: int) -> bytes:
        """Read up to size bytes using the given callback.

        As a side effect, update the verifier's hash (excluding the last
        hash_size bytes read, which is the pack checksum).

        Args:
          read: The read callback to read from.
          size: The maximum number of bytes to read; the particular
            behavior is callback-specific.
        Returns: Bytes read
        """
        data = read(size)

        # maintain a trailer of the last hash_size bytes we've read
        n = len(data)
        self._offset += n
        tn = len(self._trailer)
        if n >= self._hash_size:
            to_pop = tn
            to_add = self._hash_size
        else:
            to_pop = max(n + tn - self._hash_size, 0)
            to_add = n
        self.sha.update(
            bytes(bytearray([self._trailer.popleft() for _ in range(to_pop)]))
        )
        self._trailer.extend(data[-to_add:])

        # hash everything but the trailer
        self.sha.update(data[:-to_add])
        return data

    def _buf_len(self) -> int:
        buf = self._rbuf
        start = buf.tell()
        buf.seek(0, SEEK_END)
        end = buf.tell()
        buf.seek(start)
        return end - start

    @property
    def offset(self) -> int:
        """Return current offset in the stream."""
        return self._offset - self._buf_len()

    def read(self, size: int) -> bytes:
        """Read, blocking until size bytes are read."""
        buf_len = self._buf_len()
        if buf_len >= size:
            return self._rbuf.read(size)
        buf_data = self._rbuf.read()
        self._rbuf = BytesIO()
        return buf_data + self._read(self.read_all, size - buf_len)

    def recv(self, size: int) -> bytes:
        """Read up to size bytes, blocking until one byte is read."""
        buf_len = self._buf_len()
        if buf_len:
            data = self._rbuf.read(size)
            if size >= buf_len:
                self._rbuf = BytesIO()
            return data
        return self._read(self.read_some, size)

    def __len__(self) -> int:
        """Return the number of objects in this pack."""
        return self._num_objects

    def read_objects(self, compute_crc32: bool = False) -> Iterator[UnpackedObject]:
        """Read the objects in this pack file.

        Args:
          compute_crc32: If True, compute the CRC32 of the compressed
            data. If False, the returned CRC32 will be None.
        Returns: Iterator over UnpackedObjects with the following members set:
            offset
            obj_type_num
            obj_chunks (for non-delta types)
            delta_base (for delta types)
            decomp_chunks
            decomp_len
            crc32 (if compute_crc32 is True)

        Raises:
          ChecksumMismatch: if the checksum of the pack contents does not
            match the checksum in the pack trailer.
          zlib.error: if an error occurred during zlib decompression.
          IOError: if an error occurred writing to the output file.
        """
        _pack_version, self._num_objects = read_pack_header(self.read)

        for _ in range(self._num_objects):
            offset = self.offset
            unpacked, unused = unpack_object(
                self.read,
                self.hash_func,
                read_some=self.recv,
                compute_crc32=compute_crc32,
                zlib_bufsize=self._zlib_bufsize,
            )
            unpacked.offset = offset

            # prepend any unused data to current read buffer
            buf = BytesIO()
            buf.write(unused)
            buf.write(self._rbuf.read())
            buf.seek(0)
            self._rbuf = buf

            yield unpacked

        if self._buf_len() < self._hash_size:
            # If the read buffer is full, then the last read() got the whole
            # trailer off the wire. If not, it means there is still some of the
            # trailer to read. We need to read() all hash_size bytes; N come from the
            # read buffer and (hash_size - N) come from the wire.
            self.read(self._hash_size)

        pack_sha = bytearray(self._trailer)
        if pack_sha != self.sha.digest():
            raise ChecksumMismatch(
                sha_to_hex(RawObjectID(bytes(pack_sha))), self.sha.hexdigest()
            )


class PackStreamCopier(PackStreamReader):
    """Class to verify a pack stream as it is being read.

    The pack is read from a ReceivableProtocol using read() or recv() as
    appropriate and written out to the given file-like object.
    """

    def __init__(
        self,
        hash_func: Callable[[], "HashObject"],
        read_all: Callable[[int], bytes],
        read_some: Callable[[int], bytes] | None,
        outfile: IO[bytes],
        delta_iter: "DeltaChainIterator[UnpackedObject] | None" = None,
    ) -> None:
        """Initialize the copier.

        Args:
          hash_func: Hash function to use for computing object IDs
          read_all: Read function that blocks until the number of
            requested bytes are read.
          read_some: Read function that returns at least one byte, but may
            not return the number of bytes requested.
          outfile: File-like object to write output through.
          delta_iter: Optional DeltaChainIterator to record deltas as we
            read them.
        """
        super().__init__(hash_func, read_all, read_some=read_some)
        self.outfile = outfile
        self._delta_iter = delta_iter

    def _read(self, read: Callable[[int], bytes], size: int) -> bytes:
        """Read data from the read callback and write it to the file."""
        data = super()._read(read, size)
        self.outfile.write(data)
        return data

    def verify(self, progress: Callable[..., None] | None = None) -> None:
        """Verify a pack stream and write it to the output file.

        See PackStreamReader.iterobjects for a list of exceptions this may
        throw.
        """
        i = 0  # default count of entries if read_objects() is empty
        for i, unpacked in enumerate(self.read_objects()):
            if self._delta_iter:
                self._delta_iter.record(unpacked)
            if progress is not None:
                progress(f"copying pack entries: {i}/{len(self)}\r".encode("ascii"))
        if progress is not None:
            progress(f"copied {i} pack entries\n".encode("ascii"))


def obj_sha(
    type: int,
    chunks: bytes | Iterable[bytes],
    hash_func: Callable[[], "HashObject"] = sha1,
) -> bytes:
    """Compute the SHA for a numeric type and object chunks.

    Args:
        type: Object type number
        chunks: Object data chunks
        hash_func: Hash function to use (defaults to sha1)

    Returns:
        Binary hash digest
    """
    sha = hash_func()
    sha.update(object_header(type, chunks_length(chunks)))
    if isinstance(chunks, bytes):
        sha.update(chunks)
    else:
        for chunk in chunks:
            sha.update(chunk)
    return sha.digest()


def compute_file_sha(
    f: IO[bytes],
    hash_func: Callable[[], "HashObject"],
    start_ofs: int = 0,
    end_ofs: int = 0,
    buffer_size: int = 1 << 16,
) -> "HashObject":
    """Hash a portion of a file into a new SHA.

    Args:
      f: A file-like object to read from that supports seek().
      hash_func: A callable that returns a new HashObject.
      start_ofs: The offset in the file to start reading at.
      end_ofs: The offset in the file to end reading at, relative to the
        end of the file.
      buffer_size: A buffer size for reading.
    Returns: A new SHA object updated with data read from the file.
    """
    sha = hash_func()
    f.seek(0, SEEK_END)
    length = f.tell()
    if start_ofs < 0:
        raise AssertionError(f"start_ofs cannot be negative: {start_ofs}")
    if (end_ofs < 0 and length + end_ofs < start_ofs) or end_ofs > length:
        raise AssertionError(
            f"Attempt to read beyond file length. start_ofs: {start_ofs}, end_ofs: {end_ofs}, file length: {length}"
        )
    todo = length + end_ofs - start_ofs
    f.seek(start_ofs)
    while todo:
        data = f.read(min(todo, buffer_size))
        sha.update(data)
        todo -= len(data)
    return sha


class PackData:
    """The data contained in a packfile.

    Pack files can be accessed both sequentially for exploding a pack, and
    directly with the help of an index to retrieve a specific object.

    The objects within are either complete or a delta against another.

    The header is variable length. If the MSB of each byte is set then it
    indicates that the subsequent byte is still part of the header.
    For the first byte the next MS bits are the type, which tells you the type
    of object, and whether it is a delta. The LS byte is the lowest bits of the
    size. For each subsequent byte the LS 7 bits are the next MS bits of the
    size, i.e. the last byte of the header contains the MS bits of the size.

    For the complete objects the data is stored as zlib deflated data.
    The size in the header is the uncompressed object size, so to uncompress
    you need to just keep feeding data to zlib until you get an object back,
    or it errors on bad data. This is done here by just giving the complete
    buffer from the start of the deflated object on. This is bad, but until I
    get mmap sorted out it will have to do.

    Currently there are no integrity checks done. Also no attempt is made to
    try and detect the delta case, or a request for an object at the wrong
    position.  It will all just throw a zlib or KeyError.
    """

    def __init__(
        self,
        filename: str | os.PathLike[str],
        object_format: ObjectFormat,
        file: IO[bytes] | None = None,
        size: int | None = None,
        *,
        delta_window_size: int | None = None,
        window_memory: int | None = None,
        delta_cache_size: int | None = None,
        depth: int | None = None,
        threads: int | None = None,
        big_file_threshold: int | None = None,
    ) -> None:
        """Create a PackData object representing the pack in the given filename.

        The file must exist and stay readable until the object is disposed of.
        It must also stay the same size. It will be mapped whenever needed.

        Currently there is a restriction on the size of the pack as the python
        mmap implementation is flawed.
        """
        self._filename = filename
        self.object_format = object_format
        self._size = size
        self._header_size = 12
        self.delta_window_size = delta_window_size
        self.window_memory = window_memory
        self.delta_cache_size = delta_cache_size
        self.depth = depth
        self.threads = threads
        self.big_file_threshold = big_file_threshold
        self._file: IO[bytes]

        if file is None:
            self._file = GitFile(self._filename, "rb")
        else:
            self._file = file
        (_version, self._num_objects) = read_pack_header(self._file.read)

        # Use delta_cache_size config if available, otherwise default
        cache_size = delta_cache_size or (1024 * 1024 * 20)
        self._offset_cache = LRUSizeCache[int, tuple[int, OldUnpackedObject]](
            cache_size, compute_size=_compute_object_size
        )

    @property
    def filename(self) -> str:
        """Get the filename of the pack file.

        Returns:
          Base filename without directory path
        """
        return os.path.basename(self._filename)

    @property
    def path(self) -> str | os.PathLike[str]:
        """Get the full path of the pack file.

        Returns:
          Full path to the pack file
        """
        return self._filename

    @classmethod
    def from_file(
        cls,
        file: IO[bytes],
        object_format: ObjectFormat,
        size: int | None = None,
    ) -> "PackData":
        """Create a PackData object from an open file.

        Args:
          file: Open file object
          object_format: Object format
          size: Optional file size

        Returns:
          PackData instance
        """
        return cls(str(file), object_format, file=file, size=size)

    @classmethod
    def from_path(
        cls,
        path: str | os.PathLike[str],
        object_format: ObjectFormat,
    ) -> "PackData":
        """Create a PackData object from a file path.

        Args:
          path: Path to the pack file
          object_format: Object format

        Returns:
          PackData instance
        """
        return cls(filename=path, object_format=object_format)

    def close(self) -> None:
        """Close the underlying pack file."""
        self._file.close()

    def __enter__(self) -> "PackData":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager."""
        self.close()

    def __eq__(self, other: object) -> bool:
        """Check equality with another object."""
        if isinstance(other, PackData):
            return self.get_stored_checksum() == other.get_stored_checksum()
        return False

    def _get_size(self) -> int:
        if self._size is not None:
            return self._size
        self._size = os.path.getsize(self._filename)
        if self._size < self._header_size:
            errmsg = f"{self._filename} is too small for a packfile ({self._size} < {self._header_size})"
            raise AssertionError(errmsg)
        return self._size

    def __len__(self) -> int:
        """Returns the number of objects in this pack."""
        return self._num_objects

    def calculate_checksum(self) -> bytes:
        """Calculate the checksum for this pack.

        Returns: Binary digest (size depends on hash algorithm)
        """
        return compute_file_sha(
            self._file,
            hash_func=self.object_format.hash_func,
            end_ofs=-self.object_format.oid_length,
        ).digest()

    def iter_unpacked(self, *, include_comp: bool = False) -> Iterator[UnpackedObject]:
        """Iterate over unpacked objects in the pack."""
        self._file.seek(self._header_size)

        if self._num_objects is None:
            return

        for _ in range(self._num_objects):
            offset = self._file.tell()
            unpacked, unused = unpack_object(
                self._file.read,
                self.object_format.hash_func,
                compute_crc32=False,
                include_comp=include_comp,
            )
            unpacked.offset = offset
            yield unpacked
            # Back up over unused data.
            self._file.seek(-len(unused), SEEK_CUR)

    def iterentries(
        self,
        progress: Callable[[int, int], None] | None = None,
        resolve_ext_ref: ResolveExtRefFn | None = None,
    ) -> Iterator[PackIndexEntry]:
        """Yield entries summarizing the contents of this pack.

        Args:
          progress: Progress function, called with current and total
            object count.
          resolve_ext_ref: Optional function to resolve external references
        Returns: iterator of tuples with (sha, offset, crc32)
        """
        num_objects = self._num_objects
        indexer = PackIndexer.for_pack_data(self, resolve_ext_ref=resolve_ext_ref)
        for i, result in enumerate(indexer):
            if progress is not None:
                progress(i, num_objects)
            yield result

    def sorted_entries(
        self,
        progress: Callable[[int, int], None] | None = None,
        resolve_ext_ref: ResolveExtRefFn | None = None,
    ) -> list[tuple[RawObjectID, int, int]]:
        """Return entries in this pack, sorted by SHA.

        Args:
          progress: Progress function, called with current and total
            object count
          resolve_ext_ref: Optional function to resolve external references
        Returns: Iterator of tuples with (sha, offset, crc32)
        """
        return sorted(
            self.iterentries(progress=progress, resolve_ext_ref=resolve_ext_ref)  # type: ignore
        )

    def create_index_v1(
        self,
        filename: str,
        progress: Callable[..., None] | None = None,
        resolve_ext_ref: ResolveExtRefFn | None = None,
    ) -> bytes:
        """Create a version 1 file for this data file.

        Args:
          filename: Index filename.
          progress: Progress report function
          resolve_ext_ref: Optional function to resolve external references
        Returns: Checksum of index file
        """
        entries = self.sorted_entries(
            progress=progress, resolve_ext_ref=resolve_ext_ref
        )
        checksum = self.calculate_checksum()
        with GitFile(filename, "wb") as f:
            write_pack_index_v1(
                f,
                entries,
                checksum,
            )
        return checksum

    def create_index_v2(
        self,
        filename: str,
        progress: Callable[..., None] | None = None,
        resolve_ext_ref: ResolveExtRefFn | None = None,
    ) -> bytes:
        """Create a version 2 index file for this data file.

        Args:
          filename: Index filename.
          progress: Progress report function
          resolve_ext_ref: Optional function to resolve external references
        Returns: Checksum of index file
        """
        entries = self.sorted_entries(
            progress=progress, resolve_ext_ref=resolve_ext_ref
        )
        with GitFile(filename, "wb") as f:
            return write_pack_index_v2(f, entries, self.calculate_checksum())

    def create_index_v3(
        self,
        filename: str,
        progress: Callable[..., None] | None = None,
        resolve_ext_ref: ResolveExtRefFn | None = None,
        hash_format: int | None = None,
    ) -> bytes:
        """Create a version 3 index file for this data file.

        Args:
          filename: Index filename.
          progress: Progress report function
          resolve_ext_ref: Function to resolve external references
          hash_format: Hash algorithm identifier (1 = SHA-1, 2 = SHA-256)
        Returns: Checksum of index file
        """
        entries = self.sorted_entries(
            progress=progress, resolve_ext_ref=resolve_ext_ref
        )
        with GitFile(filename, "wb") as f:
            if hash_format is None:
                hash_format = 1  # Default to SHA-1
            return write_pack_index_v3(
                f, entries, self.calculate_checksum(), hash_format=hash_format
            )

    def create_index(
        self,
        filename: str,
        progress: Callable[..., None] | None = None,
        version: int = 2,
        resolve_ext_ref: ResolveExtRefFn | None = None,
        hash_format: int | None = None,
    ) -> bytes:
        """Create an  index file for this data file.

        Args:
          filename: Index filename.
          progress: Progress report function
          version: Index version (1, 2, or 3)
          resolve_ext_ref: Function to resolve external references
          hash_format: Hash algorithm identifier for v3 (1 = SHA-1, 2 = SHA-256)
        Returns: Checksum of index file
        """
        if version == 1:
            return self.create_index_v1(
                filename, progress, resolve_ext_ref=resolve_ext_ref
            )
        elif version == 2:
            return self.create_index_v2(
                filename, progress, resolve_ext_ref=resolve_ext_ref
            )
        elif version == 3:
            return self.create_index_v3(
                filename,
                progress,
                resolve_ext_ref=resolve_ext_ref,
                hash_format=hash_format,
            )
        else:
            raise ValueError(f"unknown index format {version}")

    def get_stored_checksum(self) -> bytes:
        """Return the expected checksum stored in this pack."""
        checksum_size = self.object_format.oid_length
        self._file.seek(-checksum_size, SEEK_END)
        return self._file.read(checksum_size)

    def check(self) -> None:
        """Check the consistency of this pack."""
        actual = self.calculate_checksum()
        stored = self.get_stored_checksum()
        if actual != stored:
            raise ChecksumMismatch(stored, actual)

    def get_unpacked_object_at(
        self, offset: int, *, include_comp: bool = False
    ) -> UnpackedObject:
        """Given offset in the packfile return a UnpackedObject."""
        assert offset >= self._header_size
        self._file.seek(offset)
        unpacked, _ = unpack_object(
            self._file.read, self.object_format.hash_func, include_comp=include_comp
        )
        unpacked.offset = offset
        return unpacked

    def get_object_at(self, offset: int) -> tuple[int, OldUnpackedObject]:
        """Given an offset in to the packfile return the object that is there.

        Using the associated index the location of an object can be looked up,
        and then the packfile can be asked directly for that object using this
        function.
        """
        try:
            return self._offset_cache[offset]
        except KeyError:
            pass
        unpacked = self.get_unpacked_object_at(offset, include_comp=False)
        return (unpacked.pack_type_num, unpacked._obj())


T = TypeVar("T")


class DeltaChainIterator(Generic[T]):
    """Abstract iterator over pack data based on delta chains.

    Each object in the pack is guaranteed to be inflated exactly once,
    regardless of how many objects reference it as a delta base. As a result,
    memory usage is proportional to the length of the longest delta chain.

    Subclasses can override _result to define the result type of the iterator.
    By default, results are UnpackedObjects with the following members set:

    * offset
    * obj_type_num
    * obj_chunks
    * pack_type_num
    * delta_base     (for delta types)
    * comp_chunks    (if _include_comp is True)
    * decomp_chunks
    * decomp_len
    * crc32          (if _compute_crc32 is True)
    """

    _compute_crc32 = False
    _include_comp = False

    def __init__(
        self,
        file_obj: IO[bytes] | None,
        hash_func: Callable[[], "HashObject"],
        *,
        resolve_ext_ref: ResolveExtRefFn | None = None,
    ) -> None:
        """Initialize DeltaChainIterator.

        Args:
            file_obj: File object to read pack data from
            hash_func: Hash function to use for computing object IDs
            resolve_ext_ref: Optional function to resolve external references
        """
        self._file = file_obj
        self.hash_func = hash_func
        self._resolve_ext_ref = resolve_ext_ref
        self._pending_ofs: dict[int, list[int]] = defaultdict(list)
        self._pending_ref: dict[bytes, list[int]] = defaultdict(list)
        self._full_ofs: list[tuple[int, int]] = []
        self._ext_refs: list[RawObjectID] = []

    @classmethod
    def for_pack_data(
        cls, pack_data: PackData, resolve_ext_ref: ResolveExtRefFn | None = None
    ) -> "DeltaChainIterator[T]":
        """Create a DeltaChainIterator from pack data.

        Args:
          pack_data: PackData object to iterate
          resolve_ext_ref: Optional function to resolve external refs

        Returns:
          DeltaChainIterator instance
        """
        walker = cls(
            None, pack_data.object_format.hash_func, resolve_ext_ref=resolve_ext_ref
        )
        walker.set_pack_data(pack_data)
        for unpacked in pack_data.iter_unpacked(include_comp=False):
            walker.record(unpacked)
        return walker

    @classmethod
    def for_pack_subset(
        cls,
        pack: "Pack",
        shas: Iterable[ObjectID | RawObjectID],
        *,
        allow_missing: bool = False,
        resolve_ext_ref: ResolveExtRefFn | None = None,
    ) -> "DeltaChainIterator[T]":
        """Create a DeltaChainIterator for a subset of objects.

        Args:
          pack: Pack object containing the data
          shas: Iterable of object SHAs to include
          allow_missing: If True, skip missing objects
          resolve_ext_ref: Optional function to resolve external refs

        Returns:
          DeltaChainIterator instance
        """
        walker = cls(
            None, pack.object_format.hash_func, resolve_ext_ref=resolve_ext_ref
        )
        walker.set_pack_data(pack.data)
        todo = set()
        for sha in shas:
            try:
                off = pack.index.object_offset(sha)
            except KeyError:
                if not allow_missing:
                    raise
            else:
                todo.add(off)
        done = set()
        while todo:
            off = todo.pop()
            unpacked = pack.data.get_unpacked_object_at(off)
            walker.record(unpacked)
            done.add(off)
            base_ofs = None
            if unpacked.pack_type_num == OFS_DELTA:
                assert unpacked.offset is not None
                assert unpacked.delta_base is not None
                assert isinstance(unpacked.delta_base, int)
                base_ofs = unpacked.offset - unpacked.delta_base
            elif unpacked.pack_type_num == REF_DELTA:
                with suppress(KeyError):
                    assert isinstance(unpacked.delta_base, bytes)
                    base_ofs = pack.index.object_index(RawObjectID(unpacked.delta_base))
            if base_ofs is not None and base_ofs not in done:
                todo.add(base_ofs)
        return walker

    def record(self, unpacked: UnpackedObject) -> None:
        """Record an unpacked object for later processing.

        Args:
          unpacked: UnpackedObject to record
        """
        type_num = unpacked.pack_type_num
        offset = unpacked.offset
        assert offset is not None
        if type_num == OFS_DELTA:
            assert unpacked.delta_base is not None
            assert isinstance(unpacked.delta_base, int)
            base_offset = offset - unpacked.delta_base
            self._pending_ofs[base_offset].append(offset)
        elif type_num == REF_DELTA:
            assert isinstance(unpacked.delta_base, bytes)
            self._pending_ref[unpacked.delta_base].append(offset)
        else:
            self._full_ofs.append((offset, type_num))

    def set_pack_data(self, pack_data: PackData) -> None:
        """Set the pack data for iteration.

        Args:
          pack_data: PackData object to use
        """
        self._file = pack_data._file

    def _walk_all_chains(self) -> Iterator[T]:
        for offset, type_num in self._full_ofs:
            yield from self._follow_chain(offset, type_num, None)
        yield from self._walk_ref_chains()
        assert not self._pending_ofs, repr(self._pending_ofs)

    def _ensure_no_pending(self) -> None:
        if self._pending_ref:
            raise UnresolvedDeltas(
                [sha_to_hex(RawObjectID(s)) for s in self._pending_ref]
            )

    def _walk_ref_chains(self) -> Iterator[T]:
        if not self._resolve_ext_ref:
            self._ensure_no_pending()
            return

        for base_sha, pending in sorted(self._pending_ref.items()):
            if base_sha not in self._pending_ref:
                continue
            try:
                type_num, chunks = self._resolve_ext_ref(base_sha)
            except KeyError:
                # Not an external ref, but may depend on one. Either it will
                # get popped via a _follow_chain call, or we will raise an
                # error below.
                continue
            self._ext_refs.append(RawObjectID(base_sha))
            self._pending_ref.pop(base_sha)
            for new_offset in pending:
                yield from self._follow_chain(new_offset, type_num, chunks)  # type: ignore[arg-type]

        self._ensure_no_pending()

    def _result(self, unpacked: UnpackedObject) -> T:
        raise NotImplementedError

    def _resolve_object(
        self, offset: int, obj_type_num: int, base_chunks: list[bytes] | None
    ) -> UnpackedObject:
        assert self._file is not None
        self._file.seek(offset)
        unpacked, _ = unpack_object(
            self._file.read,
            self.hash_func,
            read_some=None,
            compute_crc32=self._compute_crc32,
            include_comp=self._include_comp,
        )
        unpacked.offset = offset
        if base_chunks is None:
            assert unpacked.pack_type_num == obj_type_num
        else:
            assert unpacked.pack_type_num in DELTA_TYPES
            unpacked.obj_type_num = obj_type_num
            unpacked.obj_chunks = apply_delta(base_chunks, unpacked.decomp_chunks)
        return unpacked

    def _follow_chain(
        self, offset: int, obj_type_num: int, base_chunks: list[bytes] | None
    ) -> Iterator[T]:
        # Unlike PackData.get_object_at, there is no need to cache offsets as
        # this approach by design inflates each object exactly once.
        todo = [(offset, obj_type_num, base_chunks)]
        while todo:
            (offset, obj_type_num, base_chunks) = todo.pop()
            unpacked = self._resolve_object(offset, obj_type_num, base_chunks)
            yield self._result(unpacked)

            assert unpacked.offset is not None
            unblocked = chain(
                self._pending_ofs.pop(unpacked.offset, []),
                self._pending_ref.pop(unpacked.sha(), []),
            )
            todo.extend(
                (new_offset, unpacked.obj_type_num, unpacked.obj_chunks)  # type: ignore
                for new_offset in unblocked
            )

    def __iter__(self) -> Iterator[T]:
        """Iterate over objects in the pack."""
        return self._walk_all_chains()

    def ext_refs(self) -> list[RawObjectID]:
        """Return external references."""
        return self._ext_refs


class UnpackedObjectIterator(DeltaChainIterator[UnpackedObject]):
    """Delta chain iterator that yield unpacked objects."""

    def _result(self, unpacked: UnpackedObject) -> UnpackedObject:
        """Return the unpacked object.

        Args:
            unpacked: The unpacked object

        Returns:
            The unpacked object unchanged
        """
        return unpacked


class PackIndexer(DeltaChainIterator[PackIndexEntry]):
    """Delta chain iterator that yields index entries."""

    _compute_crc32 = True

    def _result(self, unpacked: UnpackedObject) -> PackIndexEntry:
        """Convert unpacked object to pack index entry.

        Args:
            unpacked: The unpacked object

        Returns:
            Tuple of (sha, offset, crc32) for index entry
        """
        assert unpacked.offset is not None
        return unpacked.sha(), unpacked.offset, unpacked.crc32


class PackInflater(DeltaChainIterator[ShaFile]):
    """Delta chain iterator that yields ShaFile objects."""

    def _result(self, unpacked: UnpackedObject) -> ShaFile:
        """Convert unpacked object to ShaFile.

        Args:
            unpacked: The unpacked object

        Returns:
            ShaFile object from the unpacked data
        """
        return unpacked.sha_file()


class SHA1Reader(BinaryIO):
    """Wrapper for file-like object that remembers the SHA1 of its data."""

    def __init__(self, f: IO[bytes]) -> None:
        """Initialize SHA1Reader.

        Args:
            f: File-like object to wrap
        """
        self.f = f
        self.sha1 = sha1(b"")

    def read(self, size: int = -1) -> bytes:
        """Read bytes and update SHA1.

        Args:
            size: Number of bytes to read, -1 for all

        Returns:
            Bytes read from file
        """
        data = self.f.read(size)
        self.sha1.update(data)
        return data

    def check_sha(self, allow_empty: bool = False) -> None:
        """Check if the SHA1 matches the expected value.

        Args:
            allow_empty: Allow empty SHA1 hash

        Raises:
            ChecksumMismatch: If SHA1 doesn't match
        """
        stored = self.f.read(20)
        # If git option index.skipHash is set the index will be empty
        if stored != self.sha1.digest() and (
            not allow_empty
            or (
                len(stored) == 20
                and sha_to_hex(RawObjectID(stored))
                != b"0000000000000000000000000000000000000000"
            )
        ):
            raise ChecksumMismatch(
                self.sha1.hexdigest(),
                sha_to_hex(RawObjectID(stored)) if stored else b"",
            )

    def close(self) -> None:
        """Close the underlying file."""
        return self.f.close()

    def tell(self) -> int:
        """Return current file position."""
        return self.f.tell()

    # BinaryIO abstract methods
    def readable(self) -> bool:
        """Check if file is readable."""
        return True

    def writable(self) -> bool:
        """Check if file is writable."""
        return False

    def seekable(self) -> bool:
        """Check if file is seekable."""
        return getattr(self.f, "seekable", lambda: False)()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position in file.

        Args:
            offset: Position offset
            whence: Reference point (0=start, 1=current, 2=end)

        Returns:
            New file position
        """
        return self.f.seek(offset, whence)

    def flush(self) -> None:
        """Flush the file buffer."""
        if hasattr(self.f, "flush"):
            self.f.flush()

    def readline(self, size: int = -1) -> bytes:
        """Read a line from the file.

        Args:
            size: Maximum bytes to read

        Returns:
            Line read from file
        """
        return self.f.readline(size)

    def readlines(self, hint: int = -1) -> list[bytes]:
        """Read all lines from the file.

        Args:
            hint: Approximate number of bytes to read

        Returns:
            List of lines
        """
        return self.f.readlines(hint)

    def writelines(self, lines: Iterable[bytes], /) -> None:  # type: ignore[override]
        """Write multiple lines to the file (not supported)."""
        raise UnsupportedOperation("writelines")

    def write(self, data: bytes, /) -> int:  # type: ignore[override]
        """Write data to the file (not supported)."""
        raise UnsupportedOperation("write")

    def __enter__(self) -> "SHA1Reader":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        type: type | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit context manager and close file."""
        self.close()

    def __iter__(self) -> "SHA1Reader":
        """Return iterator for reading file lines."""
        return self

    def __next__(self) -> bytes:
        """Get next line from file.

        Returns:
            Next line

        Raises:
            StopIteration: When no more lines
        """
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def fileno(self) -> int:
        """Return file descriptor number."""
        return self.f.fileno()

    def isatty(self) -> bool:
        """Check if file is a terminal."""
        return getattr(self.f, "isatty", lambda: False)()

    def truncate(self, size: int | None = None) -> int:
        """Not supported for read-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("truncate")


class SHA1Writer(BinaryIO):
    """Wrapper for file-like object that remembers the SHA1 of its data."""

    def __init__(self, f: BinaryIO | IO[bytes]) -> None:
        """Initialize SHA1Writer.

        Args:
            f: File-like object to wrap
        """
        self.f = f
        self.length = 0
        self.sha1 = sha1(b"")
        self.digest: bytes | None = None

    def write(self, data: bytes | bytearray | memoryview, /) -> int:  # type: ignore[override]
        """Write data and update SHA1.

        Args:
            data: Data to write

        Returns:
            Number of bytes written
        """
        self.sha1.update(data)
        written = self.f.write(data)
        self.length += written
        return written

    def write_sha(self) -> bytes:
        """Write the SHA1 digest to the file.

        Returns:
            The SHA1 digest bytes
        """
        sha = self.sha1.digest()
        assert len(sha) == 20
        self.f.write(sha)
        self.length += len(sha)
        return sha

    def close(self) -> None:
        """Close the pack file and finalize the SHA."""
        self.digest = self.write_sha()
        self.f.close()

    def offset(self) -> int:
        """Get the total number of bytes written.

        Returns:
            Total bytes written
        """
        return self.length

    def tell(self) -> int:
        """Return current file position."""
        return self.f.tell()

    # BinaryIO abstract methods
    def readable(self) -> bool:
        """Check if file is readable."""
        return False

    def writable(self) -> bool:
        """Check if file is writable."""
        return True

    def seekable(self) -> bool:
        """Check if file is seekable."""
        return getattr(self.f, "seekable", lambda: False)()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position in file.

        Args:
            offset: Position offset
            whence: Reference point (0=start, 1=current, 2=end)

        Returns:
            New file position
        """
        return self.f.seek(offset, whence)

    def flush(self) -> None:
        """Flush the file buffer."""
        if hasattr(self.f, "flush"):
            self.f.flush()

    def readline(self, size: int = -1) -> bytes:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("readline")

    def readlines(self, hint: int = -1) -> list[bytes]:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("readlines")

    def writelines(self, lines: Iterable[bytes], /) -> None:  # type: ignore[override]
        """Write multiple lines to the file.

        Args:
            lines: Iterable of lines to write
        """
        for line in lines:
            self.write(line)

    def read(self, size: int = -1) -> bytes:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("read")

    def __enter__(self) -> "SHA1Writer":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        type: type | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit context manager and close file."""
        self.f.close()

    def __iter__(self) -> "SHA1Writer":
        """Return iterator."""
        return self

    def __next__(self) -> bytes:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("__next__")

    def fileno(self) -> int:
        """Return file descriptor number."""
        return self.f.fileno()

    def isatty(self) -> bool:
        """Check if file is a terminal."""
        return getattr(self.f, "isatty", lambda: False)()

    def truncate(self, size: int | None = None) -> int:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("truncate")


class HashWriter(BinaryIO):
    """Wrapper for file-like object that computes hash of its data.

    This is a generic version that works with any hash algorithm.
    """

    def __init__(
        self, f: BinaryIO | IO[bytes], hash_func: Callable[[], "HashObject"]
    ) -> None:
        """Initialize HashWriter.

        Args:
            f: File-like object to wrap
            hash_func: Hash function (e.g., sha1, sha256)
        """
        self.f = f
        self.length = 0
        self.hash_obj = hash_func()
        self.digest: bytes | None = None

    def write(self, data: bytes | bytearray | memoryview, /) -> int:  # type: ignore[override]
        """Write data and update hash.

        Args:
            data: Data to write

        Returns:
            Number of bytes written
        """
        self.hash_obj.update(data)
        written = self.f.write(data)
        self.length += written
        return written

    def write_hash(self) -> bytes:
        """Write the hash digest to the file.

        Returns:
            The hash digest bytes
        """
        digest = self.hash_obj.digest()
        self.f.write(digest)
        self.length += len(digest)
        return digest

    def close(self) -> None:
        """Close the pack file and finalize the hash."""
        self.digest = self.write_hash()
        self.f.close()

    def offset(self) -> int:
        """Get the total number of bytes written.

        Returns:
            Total bytes written
        """
        return self.length

    def tell(self) -> int:
        """Return current file position."""
        return self.f.tell()

    # BinaryIO abstract methods
    def readable(self) -> bool:
        """Check if file is readable."""
        return False

    def writable(self) -> bool:
        """Check if file is writable."""
        return True

    def seekable(self) -> bool:
        """Check if file is seekable."""
        return getattr(self.f, "seekable", lambda: False)()

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position in file.

        Args:
            offset: Position offset
            whence: Reference point (0=start, 1=current, 2=end)

        Returns:
            New file position
        """
        return self.f.seek(offset, whence)

    def flush(self) -> None:
        """Flush the file buffer."""
        if hasattr(self.f, "flush"):
            self.f.flush()

    def readline(self, size: int = -1) -> bytes:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("readline")

    def readlines(self, hint: int = -1) -> list[bytes]:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("readlines")

    def writelines(self, lines: Iterable[bytes], /) -> None:  # type: ignore[override]
        """Write multiple lines to the file.

        Args:
            lines: Iterable of lines to write
        """
        for line in lines:
            self.write(line)

    def read(self, size: int = -1) -> bytes:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("read")

    def __enter__(self) -> "HashWriter":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        type: type | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit context manager and close file."""
        self.close()

    def __iter__(self) -> "HashWriter":
        """Return iterator."""
        return self

    def __next__(self) -> bytes:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("__next__")

    def fileno(self) -> int:
        """Return file descriptor number."""
        return self.f.fileno()

    def isatty(self) -> bool:
        """Check if file is a terminal."""
        return getattr(self.f, "isatty", lambda: False)()

    def truncate(self, size: int | None = None) -> int:
        """Not supported for write-only file.

        Raises:
            UnsupportedOperation: Always raised
        """
        raise UnsupportedOperation("truncate")


def pack_object_header(
    type_num: int,
    delta_base: bytes | int | None,
    size: int,
    object_format: "ObjectFormat",
) -> bytearray:
    """Create a pack object header for the given object info.

    Args:
      type_num: Numeric type of the object.
      delta_base: Delta base offset or ref, or None for whole objects.
      size: Uncompressed object size.
      object_format: Object format (hash algorithm) to use.
    Returns: A header for a packed object.
    """
    header = []
    c = (type_num << 4) | (size & 15)
    size >>= 4
    while size:
        header.append(c | 0x80)
        c = size & 0x7F
        size >>= 7
    header.append(c)
    if type_num == OFS_DELTA:
        assert isinstance(delta_base, int)
        ret = [delta_base & 0x7F]
        delta_base >>= 7
        while delta_base:
            delta_base -= 1
            ret.insert(0, 0x80 | (delta_base & 0x7F))
            delta_base >>= 7
        header.extend(ret)
    elif type_num == REF_DELTA:
        assert isinstance(delta_base, bytes)
        assert len(delta_base) == object_format.oid_length
        header += delta_base
    return bytearray(header)


def pack_object_chunks(
    type: int,
    object: list[bytes] | tuple[bytes | int, list[bytes]],
    object_format: "ObjectFormat",
    *,
    compression_level: int = -1,
) -> Iterator[bytes]:
    """Generate chunks for a pack object.

    Args:
      type: Numeric type of the object
      object: Object to write
      object_format: Object format (hash algorithm) to use
      compression_level: the zlib compression level
    Returns: Chunks
    """
    if type in DELTA_TYPES:
        if isinstance(object, tuple):
            delta_base, object = object
        else:
            raise TypeError("Delta types require a tuple of (delta_base, object)")
    else:
        delta_base = None

    # Convert object to list of bytes chunks
    if isinstance(object, bytes):
        chunks = [object]
    elif isinstance(object, list):
        chunks = object
    elif isinstance(object, ShaFile):
        chunks = object.as_raw_chunks()
    else:
        # Shouldn't reach here with proper typing
        raise TypeError(f"Unexpected object type: {object.__class__.__name__}")

    yield bytes(
        pack_object_header(
            type, delta_base, sum(map(len, chunks)), object_format=object_format
        )
    )
    compressor = zlib.compressobj(level=compression_level)
    for data in chunks:
        yield compressor.compress(data)
    yield compressor.flush()


def write_pack_object(
    write: Callable[[bytes], int],
    type: int,
    object: list[bytes] | tuple[bytes | int, list[bytes]],
    object_format: "ObjectFormat",
    *,
    sha: "HashObject | None" = None,
    compression_level: int = -1,
) -> int:
    """Write pack object to a file.

    Args:
      write: Write function to use
      type: Numeric type of the object
      object: Object to write
      object_format: Object format (hash algorithm) to use
      sha: Optional SHA-1 hasher to update
      compression_level: the zlib compression level
    Returns: CRC32 checksum of the written object
    """
    crc32 = 0
    for chunk in pack_object_chunks(
        type, object, compression_level=compression_level, object_format=object_format
    ):
        write(chunk)
        if sha is not None:
            sha.update(chunk)
        crc32 = binascii.crc32(chunk, crc32)
    return crc32 & 0xFFFFFFFF


def write_pack(
    filename: str,
    objects: Sequence[ShaFile] | Sequence[tuple[ShaFile, bytes | None]],
    object_format: "ObjectFormat",
    *,
    deltify: bool | None = None,
    delta_window_size: int | None = None,
    compression_level: int = -1,
) -> tuple[bytes, bytes]:
    """Write a new pack data file.

    Args:
      filename: Path to the new pack file (without .pack extension)
      objects: Objects to write to the pack
      object_format: Object format
      delta_window_size: Delta window size
      deltify: Whether to deltify pack objects
      compression_level: the zlib compression level
    Returns: Tuple with checksum of pack file and index file
    """
    with GitFile(filename + ".pack", "wb") as f:
        entries, data_sum = write_pack_objects(
            f,
            objects,
            delta_window_size=delta_window_size,
            deltify=deltify,
            compression_level=compression_level,
            object_format=object_format,
        )
    entries_list = sorted([(k, v[0], v[1]) for (k, v) in entries.items()])
    with GitFile(filename + ".idx", "wb") as f:
        idx_sha = write_pack_index(f, entries_list, data_sum)
    return data_sum, idx_sha


def pack_header_chunks(num_objects: int) -> Iterator[bytes]:
    """Yield chunks for a pack header."""
    yield b"PACK"  # Pack header
    yield struct.pack(b">L", 2)  # Pack version
    yield struct.pack(b">L", num_objects)  # Number of objects in pack


def write_pack_header(
    write: Callable[[bytes], int] | IO[bytes], num_objects: int
) -> None:
    """Write a pack header for the given number of objects."""
    write_fn: Callable[[bytes], int]
    if hasattr(write, "write"):
        write_fn = write.write
        warnings.warn(
            "write_pack_header() now takes a write rather than file argument",
            DeprecationWarning,
            stacklevel=2,
        )
    else:
        write_fn = write
    for chunk in pack_header_chunks(num_objects):
        write_fn(chunk)


def find_reusable_deltas(
    container: PackedObjectContainer,
    object_ids: Set[ObjectID],
    *,
    other_haves: Set[ObjectID] | None = None,
    progress: Callable[..., None] | None = None,
) -> Iterator[UnpackedObject]:
    """Find deltas in a pack that can be reused.

    Args:
      container: Pack container to search for deltas
      object_ids: Set of object IDs to find deltas for
      other_haves: Set of other object IDs we have
      progress: Optional progress reporting callback

    Returns:
      Iterator of UnpackedObject entries that can be reused
    """
    if other_haves is None:
        other_haves = set()
    reused = 0
    for i, unpacked in enumerate(
        container.iter_unpacked_subset(
            object_ids, allow_missing=True, convert_ofs_delta=True
        )
    ):
        if progress is not None and i % 1000 == 0:
            progress(f"checking for reusable deltas: {i}/{len(object_ids)}\r".encode())
        if unpacked.pack_type_num == REF_DELTA:
            hexsha = sha_to_hex(unpacked.delta_base)  # type: ignore
            if hexsha in object_ids or hexsha in other_haves:
                yield unpacked
                reused += 1
    if progress is not None:
        progress((f"found {reused} deltas to reuse\n").encode())


def deltify_pack_objects(
    objects: Iterator[ShaFile] | Iterator[tuple[ShaFile, bytes | None]],
    *,
    window_size: int | None = None,
    progress: Callable[..., None] | None = None,
) -> Iterator[UnpackedObject]:
    """Generate deltas for pack objects.

    Args:
      objects: An iterable of (object, path) tuples to deltify.
      window_size: Window size; None for default
      progress: Optional progress reporting callback
    Returns: Iterator over type_num, object id, delta_base, content
        delta_base is None for full text entries
    """

    def objects_with_hints() -> Iterator[tuple[ShaFile, tuple[int, bytes | None]]]:
        for e in objects:
            if isinstance(e, ShaFile):
                yield (e, (e.type_num, None))
            else:
                yield (e[0], (e[0].type_num, e[1]))

    sorted_objs = sort_objects_for_delta(objects_with_hints())
    yield from deltas_from_sorted_objects(
        sorted_objs,
        window_size=window_size,
        progress=progress,
    )


def sort_objects_for_delta(
    objects: Iterator[ShaFile] | Iterator[tuple[ShaFile, PackHint | None]],
) -> Iterator[tuple[ShaFile, bytes | None]]:
    """Sort objects for optimal delta compression.

    Args:
      objects: Iterator of objects or (object, hint) tuples

    Returns:
      Iterator of sorted (ShaFile, path) tuples
    """
    magic = []
    for entry in objects:
        if isinstance(entry, tuple):
            obj, hint = entry
            if hint is None:
                type_num = None
                path = None
            else:
                (type_num, path) = hint
        else:
            obj = entry
            type_num = None
            path = None
        magic.append((type_num, path, -obj.raw_length(), obj))
    # Build a list of objects ordered by the magic Linus heuristic
    # This helps us find good objects to diff against us
    magic.sort()
    return ((x[3], x[1]) for x in magic)


def deltas_from_sorted_objects(
    objects: Iterator[tuple[ShaFile, bytes | None]],
    window_size: int | None = None,
    progress: Callable[..., None] | None = None,
) -> Iterator[UnpackedObject]:
    """Create deltas from sorted objects.

    Args:
      objects: Iterator of sorted objects to deltify
      window_size: Delta window size; None for default
      progress: Optional progress reporting callback

    Returns:
      Iterator of UnpackedObject entries
    """
    # TODO(jelmer): Use threads
    if window_size is None:
        window_size = DEFAULT_PACK_DELTA_WINDOW_SIZE

    possible_bases: deque[tuple[bytes, int, list[bytes]]] = deque()
    for i, (o, path) in enumerate(objects):
        if progress is not None and i % 1000 == 0:
            progress((f"generating deltas: {i}\r").encode())
        raw = o.as_raw_chunks()
        winner = raw
        winner_len = sum(map(len, winner))
        winner_base = None
        for base_id, base_type_num, base in possible_bases:
            if base_type_num != o.type_num:
                continue
            delta_len = 0
            delta = []
            for chunk in create_delta(b"".join(base), b"".join(raw)):
                delta_len += len(chunk)
                if delta_len >= winner_len:
                    break
                delta.append(chunk)
            else:
                winner_base = base_id
                winner = delta
                winner_len = sum(map(len, winner))
        yield UnpackedObject(
            o.type_num,
            sha=o.sha().digest(),
            delta_base=winner_base,
            decomp_len=winner_len,
            decomp_chunks=winner,
        )
        possible_bases.appendleft((o.sha().digest(), o.type_num, raw))
        while len(possible_bases) > window_size:
            possible_bases.pop()


def pack_objects_to_data(
    objects: Sequence[ShaFile]
    | Sequence[tuple[ShaFile, bytes | None]]
    | Sequence[tuple[ShaFile, PackHint | None]],
    *,
    deltify: bool | None = None,
    delta_window_size: int | None = None,
    ofs_delta: bool = True,
    progress: Callable[..., None] | None = None,
) -> tuple[int, Iterator[UnpackedObject]]:
    """Create pack data from objects.

    Args:
      objects: Pack objects
      deltify: Whether to deltify pack objects
      delta_window_size: Delta window size
      ofs_delta: Whether to use offset deltas
      progress: Optional progress reporting callback
    Returns: Tuples with (type_num, hexdigest, delta base, object chunks)
    """
    count = len(objects)
    if deltify is None:
        # PERFORMANCE/TODO(jelmer): This should be enabled but the python
        # implementation is *much* too slow at the moment.
        # Maybe consider enabling it just if the rust extension is available?
        deltify = False
    if deltify:
        return (
            count,
            deltify_pack_objects(
                iter(objects),  # type: ignore
                window_size=delta_window_size,
                progress=progress,
            ),
        )
    else:

        def iter_without_path() -> Iterator[UnpackedObject]:
            for o in objects:
                if isinstance(o, tuple):
                    yield full_unpacked_object(o[0])
                else:
                    yield full_unpacked_object(o)

        return (count, iter_without_path())


def generate_unpacked_objects(
    container: PackedObjectContainer,
    object_ids: Sequence[tuple[ObjectID, PackHint | None]],
    delta_window_size: int | None = None,
    deltify: bool | None = None,
    reuse_deltas: bool = True,
    ofs_delta: bool = True,
    other_haves: set[ObjectID] | None = None,
    progress: Callable[..., None] | None = None,
) -> Iterator[UnpackedObject]:
    """Create pack data from objects.

    Returns: Tuples with (type_num, hexdigest, delta base, object chunks)
    """
    todo = dict(object_ids)
    if reuse_deltas:
        for unpack in find_reusable_deltas(
            container, set(todo), other_haves=other_haves, progress=progress
        ):
            del todo[sha_to_hex(RawObjectID(unpack.sha()))]
            yield unpack
    if deltify is None:
        # PERFORMANCE/TODO(jelmer): This should be enabled but is *much* too
        # slow at the moment.
        deltify = False
    if deltify:
        objects_to_delta = container.iterobjects_subset(
            todo.keys(), allow_missing=False
        )
        sorted_objs = sort_objects_for_delta((o, todo[o.id]) for o in objects_to_delta)
        yield from deltas_from_sorted_objects(
            sorted_objs,
            window_size=delta_window_size,
            progress=progress,
        )
    else:
        for oid in todo:
            yield full_unpacked_object(container[oid])


def full_unpacked_object(o: ShaFile) -> UnpackedObject:
    """Create an UnpackedObject from a ShaFile.

    Args:
      o: ShaFile object to convert

    Returns:
      UnpackedObject with full object data
    """
    return UnpackedObject(
        o.type_num,
        delta_base=None,
        crc32=None,
        decomp_chunks=o.as_raw_chunks(),
        sha=o.sha().digest(),
    )


def write_pack_from_container(
    write: Callable[[bytes], None]
    | Callable[[bytes | bytearray | memoryview], int]
    | IO[bytes],
    container: PackedObjectContainer,
    object_ids: Sequence[tuple[ObjectID, PackHint | None]],
    object_format: "ObjectFormat",
    *,
    delta_window_size: int | None = None,
    deltify: bool | None = None,
    reuse_deltas: bool = True,
    compression_level: int = -1,
    other_haves: set[ObjectID] | None = None,
) -> tuple[dict[bytes, tuple[int, int]], bytes]:
    """Write a new pack data file.

    Args:
      write: write function to use
      container: PackedObjectContainer
      object_ids: Sequence of (object_id, hint) tuples to write
      object_format: Object format (hash algorithm) to use
      delta_window_size: Sliding window size for searching for deltas;
                         Set to None for default window size.
      deltify: Whether to deltify objects
      reuse_deltas: Whether to reuse existing deltas
      compression_level: the zlib compression level to use
      other_haves: Set of additional object IDs the receiver has
    Returns: Dict mapping id -> (offset, crc32 checksum), pack checksum
    """
    pack_contents_count = len(object_ids)
    pack_contents = generate_unpacked_objects(
        container,
        object_ids,
        delta_window_size=delta_window_size,
        deltify=deltify,
        reuse_deltas=reuse_deltas,
        other_haves=other_haves,
    )

    return write_pack_data(
        write,
        pack_contents,
        num_records=pack_contents_count,
        compression_level=compression_level,
        object_format=object_format,
    )


def write_pack_objects(
    write: Callable[[bytes], None] | IO[bytes],
    objects: Sequence[ShaFile] | Sequence[tuple[ShaFile, bytes | None]],
    object_format: "ObjectFormat",
    *,
    delta_window_size: int | None = None,
    deltify: bool | None = None,
    compression_level: int = -1,
) -> tuple[dict[bytes, tuple[int, int]], bytes]:
    """Write a new pack data file.

    Args:
      write: write function to use
      objects: Sequence of (object, path) tuples to write
      object_format: Object format (hash algorithm) to use
      delta_window_size: Sliding window size for searching for deltas;
                         Set to None for default window size.
      deltify: Whether to deltify objects
      compression_level: the zlib compression level to use
    Returns: Dict mapping id -> (offset, crc32 checksum), pack checksum
    """
    pack_contents_count, pack_contents = pack_objects_to_data(objects, deltify=deltify)

    return write_pack_data(
        write,
        pack_contents,
        num_records=pack_contents_count,
        compression_level=compression_level,
        object_format=object_format,
    )


class PackChunkGenerator:
    """Generator for pack data chunks."""

    def __init__(
        self,
        object_format: "ObjectFormat",
        num_records: int | None = None,
        records: Iterator[UnpackedObject] | None = None,
        progress: Callable[..., None] | None = None,
        compression_level: int = -1,
        reuse_compressed: bool = True,
    ) -> None:
        """Initialize PackChunkGenerator.

        Args:
            num_records: Expected number of records
            records: Iterator of pack records
            progress: Optional progress callback
            compression_level: Compression level (-1 for default)
            reuse_compressed: Whether to reuse compressed chunks
            object_format: Object format (hash algorithm) to use
        """
        self.object_format = object_format
        self.cs = object_format.new_hash()
        self.entries: dict[bytes, tuple[int, int]] = {}
        if records is None:
            records = iter([])  # Empty iterator if None
        self._it = self._pack_data_chunks(
            records=records,
            num_records=num_records,
            progress=progress,
            compression_level=compression_level,
            reuse_compressed=reuse_compressed,
        )

    def sha1digest(self) -> bytes:
        """Return the SHA1 digest of the pack data."""
        return self.cs.digest()

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over pack data chunks."""
        return self._it

    def _pack_data_chunks(
        self,
        records: Iterator[UnpackedObject],
        *,
        num_records: int | None = None,
        progress: Callable[..., None] | None = None,
        compression_level: int = -1,
        reuse_compressed: bool = True,
    ) -> Iterator[bytes]:
        """Iterate pack data file chunks.

        Args:
          records: Iterator over UnpackedObject
          num_records: Number of records (defaults to len(records) if not specified)
          progress: Function to report progress to
          compression_level: the zlib compression level
          reuse_compressed: Whether to reuse compressed chunks
        Returns: Dict mapping id -> (offset, crc32 checksum), pack checksum
        """
        # Write the pack
        if num_records is None:
            num_records = len(records)  # type: ignore
        offset = 0
        for chunk in pack_header_chunks(num_records):
            yield chunk
            self.cs.update(chunk)
            offset += len(chunk)
        actual_num_records = 0
        for i, unpacked in enumerate(records):
            type_num = unpacked.pack_type_num
            if progress is not None and i % 1000 == 0:
                progress((f"writing pack data: {i}/{num_records}\r").encode("ascii"))
            raw: list[bytes] | tuple[int, list[bytes]] | tuple[bytes, list[bytes]]
            if unpacked.delta_base is not None:
                assert isinstance(unpacked.delta_base, bytes), (
                    f"Expected bytes, got {type(unpacked.delta_base)}"
                )
                try:
                    base_offset, _base_crc32 = self.entries[unpacked.delta_base]
                except KeyError:
                    type_num = REF_DELTA
                    assert isinstance(unpacked.delta_base, bytes)
                    raw = (unpacked.delta_base, unpacked.decomp_chunks)
                else:
                    type_num = OFS_DELTA
                    raw = (offset - base_offset, unpacked.decomp_chunks)
            else:
                raw = unpacked.decomp_chunks
            chunks: list[bytes] | Iterator[bytes]
            if unpacked.comp_chunks is not None and reuse_compressed:
                chunks = unpacked.comp_chunks
            else:
                chunks = pack_object_chunks(
                    type_num,
                    raw,
                    compression_level=compression_level,
                    object_format=self.object_format,
                )
            crc32 = 0
            object_size = 0
            for chunk in chunks:
                yield chunk
                crc32 = binascii.crc32(chunk, crc32)
                self.cs.update(chunk)
                object_size += len(chunk)
            actual_num_records += 1
            self.entries[unpacked.sha()] = (offset, crc32)
            offset += object_size
        if actual_num_records != num_records:
            raise AssertionError(
                f"actual records written differs: {actual_num_records} != {num_records}"
            )

        yield self.cs.digest()


def write_pack_data(
    write: Callable[[bytes], None]
    | Callable[[bytes | bytearray | memoryview], int]
    | IO[bytes],
    records: Iterator[UnpackedObject],
    object_format: "ObjectFormat",
    *,
    num_records: int | None = None,
    progress: Callable[..., None] | None = None,
    compression_level: int = -1,
) -> tuple[dict[bytes, tuple[int, int]], bytes]:
    """Write a new pack data file.

    Args:
      write: Write function to use
      num_records: Number of records (defaults to len(records) if None)
      records: Iterator over type_num, object_id, delta_base, raw
      object_format: Object format (hash algorithm) to use
      progress: Function to report progress to
      compression_level: the zlib compression level
    Returns: Dict mapping id -> (offset, crc32 checksum), pack checksum
    """
    chunk_generator = PackChunkGenerator(
        num_records=num_records,
        records=records,
        progress=progress,
        compression_level=compression_level,
        object_format=object_format,
    )
    for chunk in chunk_generator:
        if callable(write):
            write(chunk)
        else:
            write.write(chunk)
    return chunk_generator.entries, chunk_generator.sha1digest()


def write_pack_index_v1(
    f: IO[bytes],
    entries: Iterable[tuple[bytes, int, int | None]],
    pack_checksum: bytes,
) -> bytes:
    """Write a new pack index file.

    Args:
      f: A file-like object to write to
      entries: List of tuples with object name (sha), offset_in_pack,
        and crc32_checksum.
      pack_checksum: Checksum of the pack file.
    Returns: The SHA of the written index file
    """
    f = SHA1Writer(f)
    fan_out_table: dict[int, int] = defaultdict(lambda: 0)
    for name, _offset, _entry_checksum in entries:
        fan_out_table[ord(name[:1])] += 1
    # Fan-out table
    for i in range(0x100):
        f.write(struct.pack(">L", fan_out_table[i]))
        fan_out_table[i + 1] += fan_out_table[i]
    for name, offset, _entry_checksum in entries:
        if len(name) != 20:
            raise TypeError("pack index v1 only supports SHA-1 names")
        if not (offset <= 0xFFFFFFFF):
            raise TypeError("pack format 1 only supports offsets < 2Gb")
        f.write(struct.pack(">L20s", offset, name))
    assert len(pack_checksum) == 20
    f.write(pack_checksum)
    return f.write_sha()


def _delta_encode_size(size: int) -> bytes:
    ret = bytearray()
    c = size & 0x7F
    size >>= 7
    while size:
        ret.append(c | 0x80)
        c = size & 0x7F
        size >>= 7
    ret.append(c)
    return bytes(ret)


# The length of delta compression copy operations in version 2 packs is limited
# to 64K.  To copy more, we use several copy operations.  Version 3 packs allow
# 24-bit lengths in copy operations, but we always make version 2 packs.
_MAX_COPY_LEN = 0xFFFF


def _encode_copy_operation(start: int, length: int) -> bytes:
    scratch = bytearray([0x80])
    for i in range(4):
        if start & 0xFF << i * 8:
            scratch.append((start >> i * 8) & 0xFF)
            scratch[0] |= 1 << i
    for i in range(2):
        if length & 0xFF << i * 8:
            scratch.append((length >> i * 8) & 0xFF)
            scratch[0] |= 1 << (4 + i)
    return bytes(scratch)


def _create_delta_py(base_buf: bytes, target_buf: bytes) -> Iterator[bytes]:
    """Use python difflib to work out how to transform base_buf to target_buf.

    Args:
      base_buf: Base buffer
      target_buf: Target buffer
    """
    if isinstance(base_buf, list):
        base_buf = b"".join(base_buf)
    if isinstance(target_buf, list):
        target_buf = b"".join(target_buf)
    assert isinstance(base_buf, bytes)
    assert isinstance(target_buf, bytes)
    # write delta header
    yield _delta_encode_size(len(base_buf))
    yield _delta_encode_size(len(target_buf))
    # write out delta opcodes
    seq = SequenceMatcher(isjunk=None, a=base_buf, b=target_buf)
    for opcode, i1, i2, j1, j2 in seq.get_opcodes():
        # Git patch opcodes don't care about deletes!
        # if opcode == 'replace' or opcode == 'delete':
        #    pass
        if opcode == "equal":
            # If they are equal, unpacker will use data from base_buf
            # Write out an opcode that says what range to use
            copy_start = i1
            copy_len = i2 - i1
            while copy_len > 0:
                to_copy = min(copy_len, _MAX_COPY_LEN)
                yield _encode_copy_operation(copy_start, to_copy)
                copy_start += to_copy
                copy_len -= to_copy
        if opcode == "replace" or opcode == "insert":
            # If we are replacing a range or adding one, then we just
            # output it to the stream (prefixed by its size)
            s = j2 - j1
            o = j1
            while s > 127:
                yield bytes([127])
                yield bytes(memoryview(target_buf)[o : o + 127])
                s -= 127
                o += 127
            yield bytes([s])
            yield bytes(memoryview(target_buf)[o : o + s])


# Default to pure Python implementation
create_delta = _create_delta_py


def apply_delta(
    src_buf: bytes | list[bytes], delta: bytes | list[bytes]
) -> list[bytes]:
    """Based on the similar function in git's patch-delta.c.

    Args:
      src_buf: Source buffer
      delta: Delta instructions
    """
    if not isinstance(src_buf, bytes):
        src_buf = b"".join(src_buf)
    if not isinstance(delta, bytes):
        delta = b"".join(delta)
    out = []
    index = 0
    delta_length = len(delta)

    def get_delta_header_size(delta: bytes, index: int) -> tuple[int, int]:
        size = 0
        i = 0
        while delta:
            cmd = ord(delta[index : index + 1])
            index += 1
            size |= (cmd & ~0x80) << i
            i += 7
            if not cmd & 0x80:
                break
        return size, index

    src_size, index = get_delta_header_size(delta, index)
    dest_size, index = get_delta_header_size(delta, index)
    if src_size != len(src_buf):
        raise ApplyDeltaError(
            f"Unexpected source buffer size: {src_size} vs {len(src_buf)}"
        )
    while index < delta_length:
        cmd = ord(delta[index : index + 1])
        index += 1
        if cmd & 0x80:
            cp_off = 0
            for i in range(4):
                if cmd & (1 << i):
                    x = ord(delta[index : index + 1])
                    index += 1
                    cp_off |= x << (i * 8)
            cp_size = 0
            # Version 3 packs can contain copy sizes larger than 64K.
            for i in range(3):
                if cmd & (1 << (4 + i)):
                    x = ord(delta[index : index + 1])
                    index += 1
                    cp_size |= x << (i * 8)
            if cp_size == 0:
                cp_size = 0x10000
            if (
                cp_off + cp_size < cp_size
                or cp_off + cp_size > src_size
                or cp_size > dest_size
            ):
                break
            out.append(src_buf[cp_off : cp_off + cp_size])
        elif cmd != 0:
            out.append(delta[index : index + cmd])
            index += cmd
        else:
            raise ApplyDeltaError("Invalid opcode 0")

    if index != delta_length:
        raise ApplyDeltaError(f"delta not empty: {delta[index:]!r}")

    if dest_size != chunks_length(out):
        raise ApplyDeltaError("dest size incorrect")

    return out


def write_pack_index_v2(
    f: IO[bytes],
    entries: Iterable[tuple[bytes, int, int | None]],
    pack_checksum: bytes,
) -> bytes:
    """Write a new pack index file.

    Args:
      f: File-like object to write to
      entries: List of tuples with object name (sha), offset_in_pack, and
        crc32_checksum.
      pack_checksum: Checksum of the pack file.
    Returns: The checksum of the index file written
    """
    # Determine hash algorithm from pack_checksum length
    if len(pack_checksum) == 20:
        hash_func = sha1
    elif len(pack_checksum) == 32:
        hash_func = sha256
    else:
        raise ValueError(f"Unsupported pack checksum length: {len(pack_checksum)}")

    f_writer = HashWriter(f, hash_func)
    f_writer.write(b"\377tOc")  # Magic!
    f_writer.write(struct.pack(">L", 2))

    # Convert to list to allow multiple iterations
    entries_list = list(entries)

    fan_out_table: dict[int, int] = defaultdict(lambda: 0)
    for name, offset, entry_checksum in entries_list:
        fan_out_table[ord(name[:1])] += 1

    if entries_list:
        hash_size = len(entries_list[0][0])
    else:
        hash_size = len(pack_checksum)  # Use pack_checksum length as hash size

    # Fan-out table
    largetable: list[int] = []
    for i in range(0x100):
        f_writer.write(struct.pack(b">L", fan_out_table[i]))
        fan_out_table[i + 1] += fan_out_table[i]
    for name, offset, entry_checksum in entries_list:
        if len(name) != hash_size:
            raise TypeError(
                f"Object name has wrong length: expected {hash_size}, got {len(name)}"
            )
        f_writer.write(name)
    for name, offset, entry_checksum in entries_list:
        f_writer.write(struct.pack(b">L", entry_checksum))
    for name, offset, entry_checksum in entries_list:
        if offset < 2**31:
            f_writer.write(struct.pack(b">L", offset))
        else:
            f_writer.write(struct.pack(b">L", 2**31 + len(largetable)))
            largetable.append(offset)
    for offset in largetable:
        f_writer.write(struct.pack(b">Q", offset))
    f_writer.write(pack_checksum)
    return f_writer.write_hash()


def write_pack_index_v3(
    f: IO[bytes],
    entries: Iterable[tuple[bytes, int, int | None]],
    pack_checksum: bytes,
    hash_format: int = 1,
) -> bytes:
    """Write a new pack index file in v3 format.

    Args:
      f: File-like object to write to
      entries: List of tuples with object name (sha), offset_in_pack, and
        crc32_checksum.
      pack_checksum: Checksum of the pack file.
      hash_format: Hash algorithm identifier (1 = SHA-1, 2 = SHA-256)
    Returns: The SHA of the index file written
    """
    if hash_format == 1:
        hash_size = 20  # SHA-1
        writer_cls = SHA1Writer
    elif hash_format == 2:
        hash_size = 32  # SHA-256
        # TODO: Add SHA256Writer when SHA-256 support is implemented
        raise NotImplementedError("SHA-256 support not yet implemented")
    else:
        raise ValueError(f"Unknown hash algorithm {hash_format}")

    # Convert entries to list to allow multiple iterations
    entries_list = list(entries)

    # Calculate shortest unambiguous prefix length for object names
    # For now, use full hash size (this could be optimized)
    shortened_oid_len = hash_size

    f = writer_cls(f)
    f.write(b"\377tOc")  # Magic!
    f.write(struct.pack(">L", 3))  # Version 3
    f.write(struct.pack(">L", hash_format))  # Hash algorithm
    f.write(struct.pack(">L", shortened_oid_len))  # Shortened OID length

    fan_out_table: dict[int, int] = defaultdict(lambda: 0)
    for name, offset, entry_checksum in entries_list:
        if len(name) != hash_size:
            raise ValueError(
                f"Object name has wrong length: expected {hash_size}, got {len(name)}"
            )
        fan_out_table[ord(name[:1])] += 1

    # Fan-out table
    largetable: list[int] = []
    for i in range(0x100):
        f.write(struct.pack(b">L", fan_out_table[i]))
        fan_out_table[i + 1] += fan_out_table[i]

    # Object names table
    for name, offset, entry_checksum in entries_list:
        f.write(name)

    # CRC32 checksums table
    for name, offset, entry_checksum in entries_list:
        f.write(struct.pack(b">L", entry_checksum))

    # Offset table
    for name, offset, entry_checksum in entries_list:
        if offset < 2**31:
            f.write(struct.pack(b">L", offset))
        else:
            f.write(struct.pack(b">L", 2**31 + len(largetable)))
            largetable.append(offset)

    # Large offset table
    for offset in largetable:
        f.write(struct.pack(b">Q", offset))

    assert len(pack_checksum) == hash_size, (
        f"Pack checksum has wrong length: expected {hash_size}, got {len(pack_checksum)}"
    )
    f.write(pack_checksum)
    return f.write_sha()


def write_pack_index(
    f: IO[bytes],
    entries: Iterable[tuple[bytes, int, int | None]],
    pack_checksum: bytes,
    progress: Callable[..., None] | None = None,
    version: int | None = None,
) -> bytes:
    """Write a pack index file.

    Args:
      f: File-like object to write to.
      entries: List of (checksum, offset, crc32) tuples
      pack_checksum: Checksum of the pack file.
      progress: Progress function (not currently used)
      version: Pack index version to use (1, 2, or 3). If None, defaults to DEFAULT_PACK_INDEX_VERSION.

    Returns:
      SHA of the written index file

    Raises:
      ValueError: If an unsupported version is specified
    """
    if version is None:
        version = DEFAULT_PACK_INDEX_VERSION

    if version == 1:
        return write_pack_index_v1(f, entries, pack_checksum)
    elif version == 2:
        return write_pack_index_v2(f, entries, pack_checksum)
    elif version == 3:
        return write_pack_index_v3(f, entries, pack_checksum)
    else:
        raise ValueError(f"Unsupported pack index version: {version}")


class Pack:
    """A Git pack object."""

    _data_load: Callable[[], PackData] | None
    _idx_load: Callable[[], PackIndex] | None

    _data: PackData | None
    _idx: PackIndex | None
    _bitmap: "PackBitmap | None"

    def __init__(
        self,
        basename: str,
        *,
        object_format: ObjectFormat,
        resolve_ext_ref: ResolveExtRefFn | None = None,
        delta_window_size: int | None = None,
        window_memory: int | None = None,
        delta_cache_size: int | None = None,
        depth: int | None = None,
        threads: int | None = None,
        big_file_threshold: int | None = None,
    ) -> None:
        """Initialize a Pack object.

        Args:
          basename: Base path for pack files (without .pack/.idx extension)
          object_format: Hash algorithm used by the repository
          resolve_ext_ref: Optional function to resolve external references
          delta_window_size: Size of the delta compression window
          window_memory: Memory limit for delta compression window
          delta_cache_size: Size of the delta cache
          depth: Maximum depth for delta chains
          threads: Number of threads to use for operations
          big_file_threshold: Size threshold for big file handling
        """
        self._basename = basename
        self.object_format = object_format
        self._data = None
        self._idx = None
        self._bitmap = None
        self._idx_path = self._basename + ".idx"
        self._data_path = self._basename + ".pack"
        self._bitmap_path = self._basename + ".bitmap"
        self.delta_window_size = delta_window_size
        self.window_memory = window_memory
        self.delta_cache_size = delta_cache_size
        self.depth = depth
        self.threads = threads
        self.big_file_threshold = big_file_threshold
        self._idx_load = lambda: load_pack_index(self._idx_path, object_format)
        self._data_load = lambda: PackData(
            self._data_path,
            delta_window_size=delta_window_size,
            window_memory=window_memory,
            delta_cache_size=delta_cache_size,
            depth=depth,
            threads=threads,
            big_file_threshold=big_file_threshold,
            object_format=object_format,
        )
        self.resolve_ext_ref = resolve_ext_ref

    @classmethod
    def from_lazy_objects(
        cls,
        data_fn: Callable[[], PackData],
        idx_fn: Callable[[], PackIndex],
    ) -> "Pack":
        """Create a new pack object from callables to load pack data and index objects."""
        # Load index to get object format
        idx = idx_fn()
        ret = cls("", object_format=idx.object_format)
        ret._data_load = data_fn
        ret._idx = idx
        ret._idx_load = None
        return ret

    @classmethod
    def from_objects(cls, data: PackData, idx: PackIndex) -> "Pack":
        """Create a new pack object from pack data and index objects."""
        ret = cls("", object_format=idx.object_format)
        ret._data = data
        ret._data_load = None
        ret._idx = idx
        ret._idx_load = None
        ret.check_length_and_checksum()
        return ret

    def name(self) -> bytes:
        """The SHA over the SHAs of the objects in this pack."""
        return self.index.objects_sha1()

    @property
    def data(self) -> PackData:
        """The pack data object being used."""
        if self._data is None:
            assert self._data_load
            self._data = self._data_load()
            self.check_length_and_checksum()
        return self._data

    @property
    def index(self) -> PackIndex:
        """The index being used.

        Note: This may be an in-memory index
        """
        if self._idx is None:
            assert self._idx_load
            self._idx = self._idx_load()
        return self._idx

    @property
    def bitmap(self) -> "PackBitmap | None":
        """The bitmap being used, if available.

        Returns:
            PackBitmap instance or None if no bitmap exists

        Raises:
            ValueError: If bitmap file is invalid or corrupt
        """
        if self._bitmap is None:
            from .bitmap import read_bitmap

            self._bitmap = read_bitmap(self._bitmap_path, pack_index=self.index)
        return self._bitmap

    def ensure_bitmap(
        self,
        object_store: "BaseObjectStore",
        refs: dict["Ref", "ObjectID"],
        commit_interval: int | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> "PackBitmap":
        """Ensure a bitmap exists for this pack, generating one if needed.

        Args:
          object_store: Object store to read objects from
          refs: Dictionary of ref names to commit SHAs
          commit_interval: Include every Nth commit in bitmap index
          progress: Optional progress reporting callback

        Returns:
          PackBitmap instance (either existing or newly generated)
        """
        from .bitmap import generate_bitmap, write_bitmap

        # Check if bitmap already exists
        try:
            existing = self.bitmap
            if existing is not None:
                return existing
        except FileNotFoundError:
            pass  # No bitmap, we'll generate one

        # Generate new bitmap
        if progress:
            progress(f"Generating bitmap for {self.name().decode('utf-8')}...\n")

        pack_bitmap = generate_bitmap(
            self.index,
            object_store,
            refs,
            self.get_stored_checksum(),
            commit_interval=commit_interval,
            progress=progress,
        )

        # Write bitmap file
        write_bitmap(self._bitmap_path, pack_bitmap)

        if progress:
            progress(f"Wrote {self._bitmap_path}\n")

        # Update cached bitmap
        self._bitmap = pack_bitmap

        return pack_bitmap

    def close(self) -> None:
        """Close the pack file and index."""
        if self._data is not None:
            self._data.close()
        if self._idx is not None:
            self._idx.close()

    def __enter__(self) -> "Pack":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager."""
        self.close()

    def __eq__(self, other: object) -> bool:
        """Check equality with another pack."""
        if not isinstance(other, Pack):
            return False
        return self.index == other.index

    def __len__(self) -> int:
        """Number of entries in this pack."""
        return len(self.index)

    def __repr__(self) -> str:
        """Return string representation of this pack."""
        return f"{self.__class__.__name__}({self._basename!r})"

    def __iter__(self) -> Iterator[ObjectID]:
        """Iterate over all the sha1s of the objects in this pack."""
        return iter(self.index)

    def check_length_and_checksum(self) -> None:
        """Sanity check the length and checksum of the pack index and data."""
        assert len(self.index) == len(self.data), (
            f"Length mismatch: {len(self.index)} (index) != {len(self.data)} (data)"
        )
        idx_stored_checksum = self.index.get_pack_checksum()
        data_stored_checksum = self.data.get_stored_checksum()
        if (
            idx_stored_checksum is not None
            and idx_stored_checksum != data_stored_checksum
        ):
            raise ChecksumMismatch(
                sha_to_hex(RawObjectID(idx_stored_checksum)),
                sha_to_hex(RawObjectID(data_stored_checksum)),
            )

    def check(self) -> None:
        """Check the integrity of this pack.

        Raises:
          ChecksumMismatch: if a checksum for the index or data is wrong
        """
        self.index.check()
        self.data.check()
        for obj in self.iterobjects():
            obj.check()
        # TODO: object connectivity checks

    def get_stored_checksum(self) -> bytes:
        """Return the stored checksum of the pack data."""
        return self.data.get_stored_checksum()

    def pack_tuples(self) -> list[tuple[ShaFile, None]]:
        """Return pack tuples for all objects in pack."""
        return [(o, None) for o in self.iterobjects()]

    def __contains__(self, sha1: ObjectID | RawObjectID) -> bool:
        """Check whether this pack contains a particular SHA1."""
        try:
            self.index.object_offset(sha1)
            return True
        except KeyError:
            return False

    def get_raw(self, sha1: RawObjectID | ObjectID) -> tuple[int, bytes]:
        """Get raw object data by SHA1."""
        offset = self.index.object_offset(sha1)
        obj_type, obj = self.data.get_object_at(offset)
        type_num, chunks = self.resolve_object(offset, obj_type, obj)
        return type_num, b"".join(chunks)  # type: ignore[arg-type]

    def __getitem__(self, sha1: "ObjectID | RawObjectID") -> ShaFile:
        """Retrieve the specified SHA1."""
        type, uncomp = self.get_raw(sha1)
        return ShaFile.from_raw_string(type, uncomp, sha=sha1)

    def iterobjects(self) -> Iterator[ShaFile]:
        """Iterate over the objects in this pack."""
        return iter(
            PackInflater.for_pack_data(self.data, resolve_ext_ref=self.resolve_ext_ref)
        )

    def iterobjects_subset(
        self, shas: Iterable[ObjectID], *, allow_missing: bool = False
    ) -> Iterator[ShaFile]:
        """Iterate over a subset of objects in this pack."""
        return (
            uo
            for uo in PackInflater.for_pack_subset(
                self,
                shas,
                allow_missing=allow_missing,
                resolve_ext_ref=self.resolve_ext_ref,
            )
            if uo.id in shas
        )

    def iter_unpacked_subset(
        self,
        shas: Iterable[ObjectID | RawObjectID],
        *,
        include_comp: bool = False,
        allow_missing: bool = False,
        convert_ofs_delta: bool = False,
    ) -> Iterator[UnpackedObject]:
        """Iterate over unpacked objects in subset."""
        ofs_pending: dict[int, list[UnpackedObject]] = defaultdict(list)
        ofs: dict[int, bytes] = {}
        todo: set[ObjectID | RawObjectID] = set(shas)
        for unpacked in self.iter_unpacked(include_comp=include_comp):
            sha = unpacked.sha()
            if unpacked.offset is not None:
                ofs[unpacked.offset] = sha
            hexsha = sha_to_hex(RawObjectID(sha))
            if hexsha in todo:
                if unpacked.pack_type_num == OFS_DELTA:
                    assert isinstance(unpacked.delta_base, int)
                    assert unpacked.offset is not None
                    base_offset = unpacked.offset - unpacked.delta_base
                    try:
                        unpacked.delta_base = ofs[base_offset]
                    except KeyError:
                        ofs_pending[base_offset].append(unpacked)
                        continue
                    else:
                        unpacked.pack_type_num = REF_DELTA
                yield unpacked
                todo.remove(hexsha)
            if unpacked.offset is not None:
                for child in ofs_pending.pop(unpacked.offset, []):
                    child.pack_type_num = REF_DELTA
                    child.delta_base = sha
                    yield child
        assert not ofs_pending
        if not allow_missing and todo:
            raise UnresolvedDeltas(list(todo))

    def iter_unpacked(self, include_comp: bool = False) -> Iterator[UnpackedObject]:
        """Iterate over all unpacked objects in this pack."""
        ofs_to_entries = {
            ofs: (sha, crc32) for (sha, ofs, crc32) in self.index.iterentries()
        }
        for unpacked in self.data.iter_unpacked(include_comp=include_comp):
            assert unpacked.offset is not None
            (sha, crc32) = ofs_to_entries[unpacked.offset]
            unpacked._sha = sha
            unpacked.crc32 = crc32
            yield unpacked

    def keep(self, msg: bytes | None = None) -> str:
        """Add a .keep file for the pack, preventing git from garbage collecting it.

        Args:
          msg: A message written inside the .keep file; can be used later
            to determine whether or not a .keep file is obsolete.
        Returns: The path of the .keep file, as a string.
        """
        keepfile_name = f"{self._basename}.keep"
        with GitFile(keepfile_name, "wb") as keepfile:
            if msg:
                keepfile.write(msg)
                keepfile.write(b"\n")
        return keepfile_name

    def get_ref(
        self, sha: RawObjectID | ObjectID
    ) -> tuple[int | None, int, OldUnpackedObject]:
        """Get the object for a ref SHA, only looking in this pack."""
        # TODO: cache these results
        try:
            offset = self.index.object_offset(sha)
        except KeyError:
            offset = None
        if offset:
            type, obj = self.data.get_object_at(offset)
        elif self.resolve_ext_ref:
            type, obj = self.resolve_ext_ref(sha)
        else:
            raise KeyError(sha)
        return offset, type, obj

    def resolve_object(
        self,
        offset: int,
        type: int,
        obj: OldUnpackedObject,
        get_ref: Callable[
            [RawObjectID | ObjectID], tuple[int | None, int, OldUnpackedObject]
        ]
        | None = None,
    ) -> tuple[int, OldUnpackedObject]:
        """Resolve an object, possibly resolving deltas when necessary.

        Returns: Tuple with object type and contents.
        """
        # Walk down the delta chain, building a stack of deltas to reach
        # the requested object.
        base_offset: int | None = offset
        base_type = type
        base_obj = obj
        delta_stack = []
        while base_type in DELTA_TYPES:
            prev_offset = base_offset
            if get_ref is None:
                get_ref = self.get_ref
            if base_type == OFS_DELTA:
                (delta_offset, delta) = base_obj
                # TODO: clean up asserts and replace with nicer error messages
                assert isinstance(delta_offset, int), (
                    f"Expected int, got {delta_offset.__class__}"
                )
                assert base_offset is not None
                base_offset = base_offset - delta_offset
                base_type, base_obj = self.data.get_object_at(base_offset)
                assert isinstance(base_type, int)
            elif base_type == REF_DELTA:
                (basename, delta) = base_obj
                assert (
                    isinstance(basename, bytes)
                    and len(basename) == self.object_format.oid_length
                )
                base_offset_temp, base_type, base_obj = get_ref(RawObjectID(basename))
                assert isinstance(base_type, int)
                # base_offset_temp can be None for thin packs (external references)
                base_offset = base_offset_temp
                if base_offset == prev_offset:  # object is based on itself
                    raise UnresolvedDeltas([basename])
            delta_stack.append((prev_offset, base_type, delta))

        # Now grab the base object (mustn't be a delta) and apply the
        # deltas all the way up the stack.
        chunks = base_obj
        for prev_offset, _delta_type, delta in reversed(delta_stack):
            # Convert chunks to bytes for apply_delta if needed
            if isinstance(chunks, list):
                chunks_bytes = b"".join(chunks)
            elif isinstance(chunks, tuple):
                # For tuple type, second element is the actual data
                _, chunk_data = chunks
                if isinstance(chunk_data, list):
                    chunks_bytes = b"".join(chunk_data)
                else:
                    chunks_bytes = chunk_data
            else:
                chunks_bytes = chunks

            # Apply delta and get result as list
            chunks = apply_delta(chunks_bytes, delta)

            if prev_offset is not None:
                self.data._offset_cache[prev_offset] = base_type, chunks
        return base_type, chunks

    def entries(
        self, progress: Callable[[int, int], None] | None = None
    ) -> Iterator[PackIndexEntry]:
        """Yield entries summarizing the contents of this pack.

        Args:
          progress: Progress function, called with current and total
            object count.
        Returns: iterator of tuples with (sha, offset, crc32)
        """
        return self.data.iterentries(
            progress=progress, resolve_ext_ref=self.resolve_ext_ref
        )

    def sorted_entries(
        self, progress: Callable[[int, int], None] | None = None
    ) -> Iterator[PackIndexEntry]:
        """Return entries in this pack, sorted by SHA.

        Args:
          progress: Progress function, called with current and total
            object count
        Returns: Iterator of tuples with (sha, offset, crc32)
        """
        return iter(
            self.data.sorted_entries(
                progress=progress, resolve_ext_ref=self.resolve_ext_ref
            )
        )

    def get_unpacked_object(
        self,
        sha: ObjectID | RawObjectID,
        *,
        include_comp: bool = False,
        convert_ofs_delta: bool = True,
    ) -> UnpackedObject:
        """Get the unpacked object for a sha.

        Args:
          sha: SHA of object to fetch
          include_comp: Whether to include compression data in UnpackedObject
          convert_ofs_delta: Whether to convert offset deltas to ref deltas
        """
        offset = self.index.object_offset(sha)
        unpacked = self.data.get_unpacked_object_at(offset, include_comp=include_comp)
        if unpacked.pack_type_num == OFS_DELTA and convert_ofs_delta:
            assert isinstance(unpacked.delta_base, int)
            unpacked.delta_base = self.index.object_sha1(offset - unpacked.delta_base)
            unpacked.pack_type_num = REF_DELTA
        return unpacked


def extend_pack(
    f: BinaryIO,
    object_ids: Set["RawObjectID"],
    get_raw: Callable[["RawObjectID | ObjectID"], tuple[int, bytes]],
    object_format: "ObjectFormat",
    *,
    compression_level: int = -1,
    progress: Callable[[bytes], None] | None = None,
) -> tuple[bytes, list[tuple[RawObjectID, int, int]]]:
    """Extend a pack file with more objects.

    The caller should make sure that object_ids does not contain any objects
    that are already in the pack
    """
    # Update the header with the new number of objects.
    f.seek(0)
    _version, num_objects = read_pack_header(f.read)

    if object_ids:
        f.seek(0)
        write_pack_header(f.write, num_objects + len(object_ids))

        # Must flush before reading (http://bugs.python.org/issue3207)
        f.flush()

    # Rescan the rest of the pack, computing the SHA with the new header.
    new_sha = compute_file_sha(
        f, hash_func=object_format.hash_func, end_ofs=-object_format.oid_length
    )

    # Must reposition before writing (http://bugs.python.org/issue3207)
    f.seek(0, os.SEEK_CUR)

    extra_entries = []

    # Complete the pack.
    for i, object_id in enumerate(object_ids):
        if progress is not None:
            progress(
                (f"writing extra base objects: {i}/{len(object_ids)}\r").encode("ascii")
            )
        assert len(object_id) == object_format.oid_length
        type_num, data = get_raw(object_id)
        offset = f.tell()
        crc32 = write_pack_object(
            f.write,
            type_num,
            [data],  # Convert bytes to list[bytes]
            sha=new_sha,
            compression_level=compression_level,
            object_format=object_format,
        )
        extra_entries.append((object_id, offset, crc32))
    pack_sha = new_sha.digest()
    f.write(pack_sha)
    return pack_sha, extra_entries


try:
    from dulwich._pack import (  # type: ignore
        apply_delta,
        bisect_find_sha,
    )
except ImportError:
    pass

# Try to import the Rust version of create_delta
try:
    from dulwich._pack import create_delta as _create_delta_rs
except ImportError:
    pass
else:
    # Wrap the Rust version to match the Python API (returns bytes instead of Iterator)
    def _create_delta_rs_wrapper(base_buf: bytes, target_buf: bytes) -> Iterator[bytes]:
        """Wrapper for Rust create_delta to match Python API."""
        yield _create_delta_rs(base_buf, target_buf)

    create_delta = _create_delta_rs_wrapper
