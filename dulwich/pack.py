# pack.py -- For dealing with packed git objects.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

"""Classes for dealing with packed git objects.

A pack is a compact representation of a bunch of objects, stored
using deltas where possible.

They have two parts, the pack file, which stores the data, and an index
that tells you where the data is.

To find an object you look in all of the index files 'til you find a
match for the object name. You then use the pointer got from this as
a pointer in to the corresponding packfile.
"""

try:
    from collections import defaultdict
except ImportError:
    from dulwich._compat import defaultdict

from cStringIO import (
    StringIO,
    )
from collections import (
    deque,
    )
import difflib
from itertools import (
    chain,
    imap,
    izip,
    )
try:
    import mmap
except ImportError:
    has_mmap = False
else:
    has_mmap = True
import os
import struct
try:
    from struct import unpack_from
except ImportError:
    from dulwich._compat import unpack_from
import sys
import zlib

from dulwich.errors import (
    ApplyDeltaError,
    ChecksumMismatch,
    )
from dulwich.file import GitFile
from dulwich.lru_cache import (
    LRUSizeCache,
    )
from dulwich._compat import (
    make_sha,
    SEEK_END,
    )
from dulwich.objects import (
    ShaFile,
    hex_to_sha,
    sha_to_hex,
    object_header,
    )

supports_mmap_offset = (sys.version_info[0] >= 3 or
        (sys.version_info[0] == 2 and sys.version_info[1] >= 6))


OFS_DELTA = 6
REF_DELTA = 7

DELTA_TYPES = (OFS_DELTA, REF_DELTA)


def take_msb_bytes(read):
    """Read bytes marked with most significant bit.

    :param read: Read function
    """
    ret = []
    while len(ret) == 0 or ret[-1] & 0x80:
        ret.append(ord(read(1)))
    return ret


def read_zlib_chunks(read_some, dec_size, buffer_size=4096):
    """Read zlib data from a buffer.

    This function requires that the buffer have additional data following the
    compressed data, which is guaranteed to be the case for git pack files.

    :param read_some: Read function that returns at least one byte, but may
        return less than the requested size
    :param dec_size: Expected size of the decompressed buffer
    :param buffer_size: Size of the read buffer
    :return: Tuple with list of chunks, length of compressed data length and
        and unused read data.
    :raise zlib.error: if a decompression error occurred.
    """
    if dec_size <= -1:
        raise ValueError("non-negative zlib data stream size expected")
    obj = zlib.decompressobj()
    ret = []
    fed = 0
    size = 0
    while obj.unused_data == "":
        add = read_some(buffer_size)
        if not add:
            raise zlib.error("EOF before end of zlib stream")
        fed += len(add)
        decomp = obj.decompress(add)
        size += len(decomp)
        ret.append(decomp)
    if size != dec_size:
        raise zlib.error("decompressed data does not match expected size")
    comp_len = fed - len(obj.unused_data)
    return ret, comp_len, obj.unused_data


def iter_sha1(iter):
    """Return the hexdigest of the SHA1 over a set of names.

    :param iter: Iterator over string objects
    :return: 40-byte hex sha1 digest
    """
    sha1 = make_sha()
    for name in iter:
        sha1.update(name)
    return sha1.hexdigest()


def load_pack_index(path):
    """Load an index file by path.

    :param filename: Path to the index file
    :return: A PackIndex loaded from the given path
    """
    f = GitFile(path, 'rb')
    try:
        return load_pack_index_file(path, f)
    finally:
        f.close()


def _load_file_contents(f, size=None):
    fileno = getattr(f, 'fileno', None)
    # Attempt to use mmap if possible
    if fileno is not None:
        fd = f.fileno()
        if size is None:
            size = os.fstat(fd).st_size
        if has_mmap:
            try:
                contents = mmap.mmap(fd, size, access=mmap.ACCESS_READ)
            except mmap.error:
                # Perhaps a socket?
                pass
            else:
                return contents, size
    contents = f.read()
    size = len(contents)
    return contents, size


def load_pack_index_file(path, f):
    """Load an index file from a file-like object.

    :param path: Path for the index file
    :param f: File-like object
    :return: A PackIndex loaded from the given file
    """
    contents, size = _load_file_contents(f)
    if contents[:4] == '\377tOc':
        version = struct.unpack(">L", contents[4:8])[0]
        if version == 2:
            return PackIndex2(path, file=f, contents=contents,
                size=size)
        else:
            raise KeyError("Unknown pack index format %d" % version)
    else:
        return PackIndex1(path, file=f, contents=contents, size=size)


def bisect_find_sha(start, end, sha, unpack_name):
    """Find a SHA in a data blob with sorted SHAs.

    :param start: Start index of range to search
    :param end: End index of range to search
    :param sha: Sha to find
    :param unpack_name: Callback to retrieve SHA by index
    :return: Index of the SHA, or None if it wasn't found
    """
    assert start <= end
    while start <= end:
        i = (start + end)/2
        file_sha = unpack_name(i)
        x = cmp(file_sha, sha)
        if x < 0:
            start = i + 1
        elif x > 0:
            end = i - 1
        else:
            return i
    return None


class PackIndex(object):
    """An index in to a packfile.

    Given a sha id of an object a pack index can tell you the location in the
    packfile of that object if it has it.
    """

    def __eq__(self, other):
        if not isinstance(other, PackIndex):
            return False

        for (name1, _, _), (name2, _, _) in izip(self.iterentries(),
                                                 other.iterentries()):
            if name1 != name2:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        """Return the number of entries in this pack index."""
        raise NotImplementedError(self.__len__)

    def __iter__(self):
        """Iterate over the SHAs in this pack."""
        return imap(sha_to_hex, self._itersha())

    def iterentries(self):
        """Iterate over the entries in this pack index.

        :return: iterator over tuples with object name, offset in packfile and
            crc32 checksum.
        """
        raise NotImplementedError(self.iterentries)

    def get_pack_checksum(self):
        """Return the SHA1 checksum stored for the corresponding packfile.

        :return: 20-byte binary digest
        """
        raise NotImplementedError(self.get_pack_checksum)

    def object_index(self, sha):
        """Return the index in to the corresponding packfile for the object.

        Given the name of an object it will return the offset that object
        lives at within the corresponding pack file. If the pack file doesn't
        have the object then None will be returned.
        """
        if len(sha) == 40:
            sha = hex_to_sha(sha)
        return self._object_index(sha)

    def _object_index(self, sha):
        """See object_index.

        :param sha: A *binary* SHA string. (20 characters long)_
        """
        raise NotImplementedError(self._object_index)

    def objects_sha1(self):
        """Return the hex SHA1 over all the shas of all objects in this pack.

        :note: This is used for the filename of the pack.
        """
        return iter_sha1(self._itersha())

    def _itersha(self):
        """Yield all the SHA1's of the objects in the index, sorted."""
        raise NotImplementedError(self._itersha)


class MemoryPackIndex(PackIndex):
    """Pack index that is stored entirely in memory."""

    def __init__(self, entries, pack_checksum=None):
        """Create a new MemoryPackIndex.

        :param entries: Sequence of name, idx, crc32 (sorted)
        :param pack_checksum: Optional pack checksum
        """
        self._by_sha = {}
        for name, idx, crc32 in entries:
            self._by_sha[name] = idx
        self._entries = entries
        self._pack_checksum = pack_checksum

    def get_pack_checksum(self):
        return self._pack_checksum

    def __len__(self):
        return len(self._entries)

    def _object_index(self, sha):
        return self._by_sha[sha][0]

    def _itersha(self):
        return iter(self._by_sha)

    def iterentries(self):
        return iter(self._entries)


class FilePackIndex(PackIndex):
    """Pack index that is based on a file.

    To do the loop it opens the file, and indexes first 256 4 byte groups
    with the first byte of the sha id. The value in the four byte group indexed
    is the end of the group that shares the same starting byte. Subtract one
    from the starting byte and index again to find the start of the group.
    The values are sorted by sha id within the group, so do the math to find
    the start and end offset and then bisect in to find if the value is present.
    """

    def __init__(self, filename, file=None, contents=None, size=None):
        """Create a pack index object.

        Provide it with the name of the index file to consider, and it will map
        it whenever required.
        """
        self._filename = filename
        # Take the size now, so it can be checked each time we map the file to
        # ensure that it hasn't changed.
        if file is None:
            self._file = GitFile(filename, 'rb')
        else:
            self._file = file
        if contents is None:
            self._contents, self._size = _load_file_contents(self._file, size)
        else:
            self._contents, self._size = (contents, size)

    def __eq__(self, other):
        # Quick optimization:
        if (isinstance(other, FilePackIndex) and
            self._fan_out_table != other._fan_out_table):
            return False

        return super(FilePackIndex, self).__eq__(other)

    def close(self):
        self._file.close()
        if getattr(self._contents, "close", None) is not None:
            self._contents.close()

    def __len__(self):
        """Return the number of entries in this pack index."""
        return self._fan_out_table[-1]

    def _unpack_entry(self, i):
        """Unpack the i-th entry in the index file.

        :return: Tuple with object name (SHA), offset in pack file and CRC32
            checksum (if known).
        """
        raise NotImplementedError(self._unpack_entry)

    def _unpack_name(self, i):
        """Unpack the i-th name from the index file."""
        raise NotImplementedError(self._unpack_name)

    def _unpack_offset(self, i):
        """Unpack the i-th object offset from the index file."""
        raise NotImplementedError(self._unpack_offset)

    def _unpack_crc32_checksum(self, i):
        """Unpack the crc32 checksum for the i-th object from the index file."""
        raise NotImplementedError(self._unpack_crc32_checksum)

    def _itersha(self):
        for i in range(len(self)):
            yield self._unpack_name(i)

    def iterentries(self):
        """Iterate over the entries in this pack index.

        :return: iterator over tuples with object name, offset in packfile and
            crc32 checksum.
        """
        for i in range(len(self)):
            yield self._unpack_entry(i)

    def _read_fan_out_table(self, start_offset):
        ret = []
        for i in range(0x100):
            fanout_entry = self._contents[start_offset+i*4:start_offset+(i+1)*4]
            ret.append(struct.unpack(">L", fanout_entry)[0])
        return ret

    def check(self):
        """Check that the stored checksum matches the actual checksum."""
        actual = self.calculate_checksum()
        stored = self.get_stored_checksum()
        if actual != stored:
            raise ChecksumMismatch(stored, actual)

    def calculate_checksum(self):
        """Calculate the SHA1 checksum over this pack index.

        :return: This is a 20-byte binary digest
        """
        return make_sha(self._contents[:-20]).digest()

    def get_pack_checksum(self):
        """Return the SHA1 checksum stored for the corresponding packfile.

        :return: 20-byte binary digest
        """
        return str(self._contents[-40:-20])

    def get_stored_checksum(self):
        """Return the SHA1 checksum stored for this index.

        :return: 20-byte binary digest
        """
        return str(self._contents[-20:])

    def _object_index(self, sha):
        """See object_index.

        :param sha: A *binary* SHA string. (20 characters long)_
        """
        assert len(sha) == 20
        idx = ord(sha[0])
        if idx == 0:
            start = 0
        else:
            start = self._fan_out_table[idx-1]
        end = self._fan_out_table[idx]
        i = bisect_find_sha(start, end, sha, self._unpack_name)
        if i is None:
            raise KeyError(sha)
        return self._unpack_offset(i)


class PackIndex1(FilePackIndex):
    """Version 1 Pack Index file."""

    def __init__(self, filename, file=None, contents=None, size=None):
        super(PackIndex1, self).__init__(filename, file, contents, size)
        self.version = 1
        self._fan_out_table = self._read_fan_out_table(0)

    def _unpack_entry(self, i):
        (offset, name) = unpack_from(">L20s", self._contents,
                                     (0x100 * 4) + (i * 24))
        return (name, offset, None)

    def _unpack_name(self, i):
        offset = (0x100 * 4) + (i * 24) + 4
        return self._contents[offset:offset+20]

    def _unpack_offset(self, i):
        offset = (0x100 * 4) + (i * 24)
        return unpack_from(">L", self._contents, offset)[0]

    def _unpack_crc32_checksum(self, i):
        # Not stored in v1 index files
        return None


class PackIndex2(FilePackIndex):
    """Version 2 Pack Index file."""

    def __init__(self, filename, file=None, contents=None, size=None):
        super(PackIndex2, self).__init__(filename, file, contents, size)
        assert self._contents[:4] == '\377tOc', "Not a v2 pack index file"
        (self.version, ) = unpack_from(">L", self._contents, 4)
        assert self.version == 2, "Version was %d" % self.version
        self._fan_out_table = self._read_fan_out_table(8)
        self._name_table_offset = 8 + 0x100 * 4
        self._crc32_table_offset = self._name_table_offset + 20 * len(self)
        self._pack_offset_table_offset = (self._crc32_table_offset +
                                          4 * len(self))

    def _unpack_entry(self, i):
        return (self._unpack_name(i), self._unpack_offset(i),
                self._unpack_crc32_checksum(i))

    def _unpack_name(self, i):
        offset = self._name_table_offset + i * 20
        return self._contents[offset:offset+20]

    def _unpack_offset(self, i):
        offset = self._pack_offset_table_offset + i * 4
        return unpack_from(">L", self._contents, offset)[0]

    def _unpack_crc32_checksum(self, i):
        return unpack_from(">L", self._contents,
                          self._crc32_table_offset + i * 4)[0]


def read_pack_header(read):
    """Read the header of a pack file.

    :param read: Read function
    :return: Tuple with pack version and number of objects
    """
    header = read(12)
    assert header[:4] == "PACK"
    (version,) = unpack_from(">L", header, 4)
    assert version in (2, 3), "Version was %d" % version
    (num_objects,) = unpack_from(">L", header, 8)
    return (version, num_objects)


def chunks_length(chunks):
    return sum(imap(len, chunks))


def unpack_object(read_all, read_some=None):
    """Unpack a Git object.

    :param read_all: Read function that blocks until the number of requested
        bytes are read.
    :param read_some: Read function that returns at least one byte, but may not
        return the number of bytes requested.
    :return: tuple with type, uncompressed data, compressed size and tail data.
    """
    if read_some is None:
        read_some = read_all
    bytes = take_msb_bytes(read_all)
    type = (bytes[0] >> 4) & 0x07
    size = bytes[0] & 0x0f
    for i, byte in enumerate(bytes[1:]):
        size += (byte & 0x7f) << ((i * 7) + 4)
    raw_base = len(bytes)
    if type == OFS_DELTA:
        bytes = take_msb_bytes(read_all)
        raw_base += len(bytes)
        assert not (bytes[-1] & 0x80)
        delta_base_offset = bytes[0] & 0x7f
        for byte in bytes[1:]:
            delta_base_offset += 1
            delta_base_offset <<= 7
            delta_base_offset += (byte & 0x7f)
        uncomp, comp_len, unused = read_zlib_chunks(read_some, size)
        assert size == chunks_length(uncomp)
        return type, (delta_base_offset, uncomp), comp_len+raw_base, unused
    elif type == REF_DELTA:
        basename = read_all(20)
        raw_base += 20
        uncomp, comp_len, unused = read_zlib_chunks(read_some, size)
        assert size == chunks_length(uncomp)
        return type, (basename, uncomp), comp_len+raw_base, unused
    else:
        uncomp, comp_len, unused = read_zlib_chunks(read_some, size)
        assert chunks_length(uncomp) == size
        return type, uncomp, comp_len+raw_base, unused


def _compute_object_size((num, obj)):
    """Compute the size of a unresolved object for use with LRUSizeCache."""
    if num in DELTA_TYPES:
        return chunks_length(obj[1])
    return chunks_length(obj)


class PackStreamReader(object):
    """Class to read a pack stream.

    The pack is read from a ReceivableProtocol using read() or recv() as
    appropriate.
    """

    def __init__(self, read_all, read_some=None):
        self.read_all = read_all
        if read_some is None:
            self.read_some = read_all
        else:
            self.read_some = read_some
        self.sha = make_sha()
        self._offset = 0
        self._rbuf = StringIO()
        # trailer is a deque to avoid memory allocation on small reads
        self._trailer = deque()

    def _read(self, read, size):
        """Read up to size bytes using the given callback.

        As a side effect, update the verifier's hash (excluding the last 20
        bytes read) and write through to the output file.

        :param read: The read callback to read from.
        :param size: The maximum number of bytes to read; the particular
            behavior is callback-specific.
        """
        data = read(size)

        # maintain a trailer of the last 20 bytes we've read
        n = len(data)
        self._offset += n
        tn = len(self._trailer)
        if n >= 20:
            to_pop = tn
            to_add = 20
        else:
            to_pop = max(n + tn - 20, 0)
            to_add = n
        for _ in xrange(to_pop):
            self.sha.update(self._trailer.popleft())
        self._trailer.extend(data[-to_add:])

        # hash everything but the trailer
        self.sha.update(data[:-to_add])
        return data

    def _buf_len(self):
        buf = self._rbuf
        start = buf.tell()
        buf.seek(0, SEEK_END)
        end = buf.tell()
        buf.seek(start)
        return end - start

    @property
    def offset(self):
        return self._offset - self._buf_len()

    def read(self, size):
        """Read, blocking until size bytes are read."""
        buf_len = self._buf_len()
        if buf_len >= size:
            return self._rbuf.read(size)
        buf_data = self._rbuf.read()
        self._rbuf = StringIO()
        return buf_data + self._read(self.read_all, size - buf_len)

    def recv(self, size):
        """Read up to size bytes, blocking until one byte is read."""
        buf_len = self._buf_len()
        if buf_len:
            data = self._rbuf.read(size)
            if size >= buf_len:
                self._rbuf = StringIO()
            return data
        return self._read(self.read_some, size)

    def __len__(self):
        return self._num_objects

    def read_objects(self):
        """Read the objects in this pack file.

        :raise AssertionError: if there is an error in the pack format.
        :raise ChecksumMismatch: if the checksum of the pack contents does not
            match the checksum in the pack trailer.
        :raise zlib.error: if an error occurred during zlib decompression.
        :raise IOError: if an error occurred writing to the output file.
        """
        pack_version, self._num_objects = read_pack_header(self.read)
        for i in xrange(self._num_objects):
            type, uncomp, comp_len, unused = unpack_object(self.read, self.recv)
            yield type, uncomp, comp_len

            # prepend any unused data to current read buffer
            buf = StringIO()
            buf.write(unused)
            buf.write(self._rbuf.read())
            buf.seek(0)
            self._rbuf = buf

        pack_sha = sha_to_hex(''.join([c for c in self._trailer]))
        calculated_sha = self.sha.hexdigest()
        if pack_sha != calculated_sha:
            raise ChecksumMismatch(pack_sha, calculated_sha)


class PackObjectIterator(object):

    def __init__(self, pack, progress=None):
        self.i = 0
        self.offset = pack._header_size
        self.num = len(pack)
        self.map = pack._file
        self._progress = progress

    def __iter__(self):
        return self

    def __len__(self):
        return self.num

    def next(self):
        if self.i == self.num:
            raise StopIteration
        self.map.seek(self.offset)
        (type, obj, total_size, unused) = unpack_object(self.map.read)
        self.map.seek(self.offset)
        crc32 = zlib.crc32(self.map.read(total_size)) & 0xffffffff
        ret = (self.offset, type, obj, crc32)
        self.offset += total_size
        if self._progress is not None:
            self._progress(self.i, self.num)
        self.i+=1
        return ret

def obj_sha(type, chunks):
    """Compute the SHA for a numeric type and object chunks."""
    sha = make_sha()
    sha.update(object_header(type, chunks_length(chunks)))
    for chunk in chunks:
        sha.update(chunk)
    return sha.digest()


class PackData(object):
    """The data contained in a packfile.

    Pack files can be accessed both sequentially for exploding a pack, and
    directly with the help of an index to retrieve a specific object.

    The objects within are either complete or a delta aginst another.

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

    def __init__(self, filename, file=None, size=None):
        """Create a PackData object representing the pack in the given filename.

        The file must exist and stay readable until the object is disposed of. It
        must also stay the same size. It will be mapped whenever needed.

        Currently there is a restriction on the size of the pack as the python
        mmap implementation is flawed.
        """
        self._filename = filename
        self._size = size
        self._header_size = 12
        if file is None:
            self._file = GitFile(self._filename, 'rb')
        else:
            self._file = file
        (version, self._num_objects) = read_pack_header(self._file.read)
        self._offset_cache = LRUSizeCache(1024*1024*20,
            compute_size=_compute_object_size)
        self.pack = None

    @classmethod
    def from_file(cls, file, size):
        return cls(str(file), file=file, size=size)

    @classmethod
    def from_path(cls, path):
        return cls(filename=path)

    def close(self):
        self._file.close()

    def _get_size(self):
        if self._size is not None:
            return self._size
        self._size = os.path.getsize(self._filename)
        if self._size < self._header_size:
            errmsg = ("%s is too small for a packfile (%d < %d)" %
                      (self._filename, self._size, self._header_size))
            raise AssertionError(errmsg)
        return self._size

    def __len__(self):
        """Returns the number of objects in this pack."""
        return self._num_objects

    def calculate_checksum(self):
        """Calculate the checksum for this pack.

        :return: 20-byte binary SHA1 digest
        """
        s = make_sha()
        self._file.seek(0)
        todo = self._get_size() - 20
        while todo > 0:
            x = self._file.read(min(todo, 1<<16))
            s.update(x)
            todo -= len(x)
        return s.digest()

    def get_ref(self, sha):
        """Get the object for a ref SHA, only looking in this pack."""
        # TODO: cache these results
        if self.pack is None:
            raise KeyError(sha)
        offset = self.pack.index.object_index(sha)
        if not offset:
            raise KeyError(sha)
        type, obj = self.get_object_at(offset)
        return offset, type, obj

    def resolve_object(self, offset, type, obj, get_ref=None):
        """Resolve an object, possibly resolving deltas when necessary.

        :return: Tuple with object type and contents.
        """
        if type not in DELTA_TYPES:
            return type, obj

        if get_ref is None:
            get_ref = self.get_ref
        if type == OFS_DELTA:
            (delta_offset, delta) = obj
            # TODO: clean up asserts and replace with nicer error messages
            assert isinstance(offset, int)
            assert isinstance(delta_offset, int)
            base_offset = offset-delta_offset
            type, base_obj = self.get_object_at(base_offset)
            assert isinstance(type, int)
        elif type == REF_DELTA:
            (basename, delta) = obj
            assert isinstance(basename, str) and len(basename) == 20
            base_offset, type, base_obj = get_ref(basename)
            assert isinstance(type, int)
        type, base_chunks = self.resolve_object(base_offset, type, base_obj)
        chunks = apply_delta(base_chunks, delta)
        # TODO(dborowitz): This can result in poor performance if large base
        # objects are separated from deltas in the pack. We should reorganize
        # so that we apply deltas to all objects in a chain one after the other
        # to optimize cache performance.
        if offset is not None:
            self._offset_cache[offset] = type, chunks
        return type, chunks

    def iterobjects(self, progress=None):
        return PackObjectIterator(self, progress)

    def iterentries(self, progress=None):
        """Yield entries summarizing the contents of this pack.

        :param progress: Progress function, called with current and total
            object count.
        :return: iterator of tuples with (sha, offset, crc32)
        """
        for offset, type, obj, crc32 in self.iterobjects(progress=progress):
            assert isinstance(offset, int)
            assert isinstance(type, int)
            assert isinstance(obj, list) or isinstance(obj, tuple)
            type, obj = self.resolve_object(offset, type, obj)
            yield obj_sha(type, obj), offset, crc32

    def sorted_entries(self, progress=None):
        """Return entries in this pack, sorted by SHA.

        :param progress: Progress function, called with current and total
            object count
        :return: List of tuples with (sha, offset, crc32)
        """
        ret = list(self.iterentries(progress=progress))
        ret.sort()
        return ret

    def create_index_v1(self, filename, progress=None):
        """Create a version 1 file for this data file.

        :param filename: Index filename.
        :param progress: Progress report function
        :return: Checksum of index file
        """
        entries = self.sorted_entries(progress=progress)
        f = GitFile(filename, 'wb')
        try:
            return write_pack_index_v1(f, entries, self.calculate_checksum())
        finally:
            f.close()

    def create_index_v2(self, filename, progress=None):
        """Create a version 2 index file for this data file.

        :param filename: Index filename.
        :param progress: Progress report function
        :return: Checksum of index file
        """
        entries = self.sorted_entries(progress=progress)
        f = GitFile(filename, 'wb')
        try:
            return write_pack_index_v2(f, entries, self.calculate_checksum())
        finally:
            f.close()

    def create_index(self, filename, progress=None,
                     version=2):
        """Create an  index file for this data file.

        :param filename: Index filename.
        :param progress: Progress report function
        :return: Checksum of index file
        """
        if version == 1:
            return self.create_index_v1(filename, progress)
        elif version == 2:
            return self.create_index_v2(filename, progress)
        else:
            raise ValueError("unknown index format %d" % version)

    def get_stored_checksum(self):
        """Return the expected checksum stored in this pack."""
        self._file.seek(self._get_size()-20)
        return self._file.read(20)

    def check(self):
        """Check the consistency of this pack."""
        actual = self.calculate_checksum()
        stored = self.get_stored_checksum()
        if actual != stored:
            raise ChecksumMismatch(stored, actual)

    def get_object_at(self, offset):
        """Given an offset in to the packfile return the object that is there.

        Using the associated index the location of an object can be looked up,
        and then the packfile can be asked directly for that object using this
        function.
        """
        if offset in self._offset_cache:
            return self._offset_cache[offset]
        assert isinstance(offset, long) or isinstance(offset, int),\
                "offset was %r" % offset
        assert offset >= self._header_size
        self._file.seek(offset)
        return unpack_object(self._file.read)[:2]


class ThinPackData(PackData):
    """PackData for thin packs, which require an ObjectStore for resolving."""

    def __init__(self, resolve_ext_ref, *args, **kwargs):
        super(ThinPackData, self).__init__(*args, **kwargs)
        self.resolve_ext_ref = resolve_ext_ref

    @classmethod
    def from_file(cls, resolve_ext_ref, file, size):
        return cls(resolve_ext_ref, str(file), file=file, size=size)

    def get_ref(self, sha):
        """Resolve a reference looking in both this pack and the store."""
        try:
            # As part of completing a pack we create a Pack object with a
            # ThinPackData and a full PackIndex, so check in the index first if
            # possible.
            # TODO(dborowitz): reevaluate this when the pack completion code is
            # rewritten.
            return super(ThinPackData, self).get_ref(sha)
        except KeyError:
            type, obj = self.resolve_ext_ref(sha)
            return None, type, obj

    def iterentries(self, progress=None):
        """Yield entries summarizing the contents of this pack.

        :param progress: Progress function, called with current and
            total object count.

        This will yield tuples with (sha, offset, crc32)
        """
        found = {}
        postponed = defaultdict(list)

        class Postpone(Exception):
            """Raised to postpone delta resolving."""

            def __init__(self, sha):
                self.sha = sha

        def get_ref_text(sha):
            assert len(sha) == 20
            if sha in found:
                offset = found[sha]
                type, obj = self.get_object_at(offset)
                return offset, type, obj
            try:
                return self.get_ref(sha)
            except KeyError:
                raise Postpone(sha)

        extra = []
        todo = chain(self.iterobjects(progress=progress), extra)
        for (offset, type, obj, crc32) in todo:
            assert isinstance(offset, int)
            if obj is None:
                # Inflate postponed delta
                obj, type = self.get_object_at(offset)
            assert isinstance(type, int)
            assert isinstance(obj, list) or isinstance(obj, tuple)
            try:
                type, obj = self.resolve_object(offset, type, obj, get_ref_text)
            except Postpone, e:
                # Save memory by not storing the inflated obj in postponed
                postponed[e.sha].append((offset, type, None, crc32))
            else:
                sha = obj_sha(type, obj)
                found[sha] = offset
                yield sha, offset, crc32
                extra.extend(postponed.pop(sha, []))
        if postponed:
            raise KeyError([sha_to_hex(h) for h in postponed.keys()])


class SHA1Reader(object):
    """Wrapper around a file-like object that remembers the SHA1 of its data."""

    def __init__(self, f):
        self.f = f
        self.sha1 = make_sha("")

    def read(self, num=None):
        data = self.f.read(num)
        self.sha1.update(data)
        return data

    def check_sha(self):
        stored = self.f.read(20)
        if stored != self.sha1.digest():
            raise ChecksumMismatch(self.sha1.hexdigest(), sha_to_hex(stored))

    def close(self):
        return self.f.close()

    def tell(self):
        return self.f.tell()


class SHA1Writer(object):
    """Wrapper around a file-like object that remembers the SHA1 of its data."""

    def __init__(self, f):
        self.f = f
        self.sha1 = make_sha("")

    def write(self, data):
        self.sha1.update(data)
        self.f.write(data)

    def write_sha(self):
        sha = self.sha1.digest()
        assert len(sha) == 20
        self.f.write(sha)
        return sha

    def close(self):
        sha = self.write_sha()
        self.f.close()
        return sha

    def tell(self):
        return self.f.tell()


def write_pack_object(f, type, object):
    """Write pack object to a file.

    :param f: File to write to
    :param type: Numeric type of the object
    :param object: Object to write
    :return: Tuple with offset at which the object was written, and crc32
    """
    offset = f.tell()
    packed_data_hdr = ""
    if type == OFS_DELTA:
        (delta_base_offset, object) = object
    elif type == REF_DELTA:
        (basename, object) = object
    size = len(object)
    c = (type << 4) | (size & 15)
    size >>= 4
    while size:
        packed_data_hdr += (chr(c | 0x80))
        c = size & 0x7f
        size >>= 7
    packed_data_hdr += chr(c)
    if type == OFS_DELTA:
        ret = [delta_base_offset & 0x7f]
        delta_base_offset >>= 7
        while delta_base_offset:
            delta_base_offset -= 1
            ret.insert(0, 0x80 | (delta_base_offset & 0x7f))
            delta_base_offset >>= 7
        packed_data_hdr += "".join([chr(x) for x in ret])
    elif type == REF_DELTA:
        assert len(basename) == 20
        packed_data_hdr += basename
    packed_data = packed_data_hdr + zlib.compress(object)
    f.write(packed_data)
    return (offset, (zlib.crc32(packed_data) & 0xffffffff))


def write_pack(filename, objects, num_objects):
    """Write a new pack data file.

    :param filename: Path to the new pack file (without .pack extension)
    :param objects: Iterable over (object, path) tuples to write
    :param num_objects: Number of objects to write
    :return: Tuple with checksum of pack file and index file
    """
    f = GitFile(filename + ".pack", 'wb')
    try:
        entries, data_sum = write_pack_data(f, objects, num_objects)
    finally:
        f.close()
    entries.sort()
    f = GitFile(filename + ".idx", 'wb')
    try:
        return data_sum, write_pack_index_v2(f, entries, data_sum)
    finally:
        f.close()


def write_pack_header(f, num_objects):
    """Write a pack header for the given number of objects."""
    f.write('PACK')                          # Pack header
    f.write(struct.pack('>L', 2))            # Pack version
    f.write(struct.pack('>L', num_objects))  # Number of objects in pack


def write_pack_data(f, objects, num_objects, window=10):
    """Write a new pack data file.

    :param f: File to write to
    :param objects: Iterable over (object, path) tuples to write
    :param num_objects: Number of objects to write
    :param window: Sliding window size for searching for deltas; currently
                   unimplemented
    :return: List with (name, offset, crc32 checksum) entries, pack checksum
    """
    recency = list(objects)
    # FIXME: Somehow limit delta depth
    # FIXME: Make thin-pack optional (its not used when cloning a pack)
    # Build a list of objects ordered by the magic Linus heuristic
    # This helps us find good objects to diff against us
    magic = []
    for obj, path in recency:
        magic.append( (obj.type_num, path, 1, -obj.raw_length(), obj) )
    magic.sort()
    # Build a map of objects and their index in magic - so we can find
    # preceeding objects to diff against
    offs = {}
    for i in range(len(magic)):
        offs[magic[i][4]] = i
    # Write the pack
    entries = []
    f = SHA1Writer(f)
    write_pack_header(f, num_objects)
    for o, path in recency:
        sha1 = o.sha().digest()
        orig_t = o.type_num
        raw = o.as_raw_string()
        winner = raw
        t = orig_t
        #for i in range(offs[o]-window, window):
        #    if i < 0 or i >= len(offs): continue
        #    b = magic[i][4]
        #    if b.type_num != orig_t: continue
        #    base = b.as_raw_string()
        #    delta = create_delta(base, raw)
        #    if len(delta) < len(winner):
        #        winner = delta
        #        t = 6 if magic[i][2] == 1 else 7
        offset, crc32 = write_pack_object(f, t, winner)
        entries.append((sha1, offset, crc32))
    return entries, f.write_sha()


def write_pack_index_v1(f, entries, pack_checksum):
    """Write a new pack index file.

    :param f: A file-like object to write to
    :param entries: List of tuples with object name (sha), offset_in_pack,
        and crc32_checksum.
    :param pack_checksum: Checksum of the pack file.
    :return: The SHA of the written index file
    """
    f = SHA1Writer(f)
    fan_out_table = defaultdict(lambda: 0)
    for (name, offset, entry_checksum) in entries:
        fan_out_table[ord(name[0])] += 1
    # Fan-out table
    for i in range(0x100):
        f.write(struct.pack(">L", fan_out_table[i]))
        fan_out_table[i+1] += fan_out_table[i]
    for (name, offset, entry_checksum) in entries:
        f.write(struct.pack(">L20s", offset, name))
    assert len(pack_checksum) == 20
    f.write(pack_checksum)
    return f.write_sha()


def create_delta(base_buf, target_buf):
    """Use python difflib to work out how to transform base_buf to target_buf.

    :param base_buf: Base buffer
    :param target_buf: Target buffer
    """
    assert isinstance(base_buf, str)
    assert isinstance(target_buf, str)
    out_buf = ""
    # write delta header
    def encode_size(size):
        ret = ""
        c = size & 0x7f
        size >>= 7
        while size:
            ret += chr(c | 0x80)
            c = size & 0x7f
            size >>= 7
        ret += chr(c)
        return ret
    out_buf += encode_size(len(base_buf))
    out_buf += encode_size(len(target_buf))
    # write out delta opcodes
    seq = difflib.SequenceMatcher(a=base_buf, b=target_buf)
    for opcode, i1, i2, j1, j2 in seq.get_opcodes():
        # Git patch opcodes don't care about deletes!
        #if opcode == "replace" or opcode == "delete":
        #    pass
        if opcode == "equal":
            # If they are equal, unpacker will use data from base_buf
            # Write out an opcode that says what range to use
            scratch = ""
            op = 0x80
            o = i1
            for i in range(4):
                if o & 0xff << i*8:
                    scratch += chr((o >> i*8) & 0xff)
                    op |= 1 << i
            s = i2 - i1
            for i in range(2):
                if s & 0xff << i*8:
                    scratch += chr((s >> i*8) & 0xff)
                    op |= 1 << (4+i)
            out_buf += chr(op)
            out_buf += scratch
        if opcode == "replace" or opcode == "insert":
            # If we are replacing a range or adding one, then we just
            # output it to the stream (prefixed by its size)
            s = j2 - j1
            o = j1
            while s > 127:
                out_buf += chr(127)
                out_buf += target_buf[o:o+127]
                s -= 127
                o += 127
            out_buf += chr(s)
            out_buf += target_buf[o:o+s]
    return out_buf


def apply_delta(src_buf, delta):
    """Based on the similar function in git's patch-delta.c.

    :param src_buf: Source buffer
    :param delta: Delta instructions
    """
    if type(src_buf) != str:
        src_buf = "".join(src_buf)
    if type(delta) != str:
        delta = "".join(delta)
    out = []
    index = 0
    delta_length = len(delta)
    def get_delta_header_size(delta, index):
        size = 0
        i = 0
        while delta:
            cmd = ord(delta[index])
            index += 1
            size |= (cmd & ~0x80) << i
            i += 7
            if not cmd & 0x80:
                break
        return size, index
    src_size, index = get_delta_header_size(delta, index)
    dest_size, index = get_delta_header_size(delta, index)
    assert src_size == len(src_buf), "%d vs %d" % (src_size, len(src_buf))
    while index < delta_length:
        cmd = ord(delta[index])
        index += 1
        if cmd & 0x80:
            cp_off = 0
            for i in range(4):
                if cmd & (1 << i):
                    x = ord(delta[index])
                    index += 1
                    cp_off |= x << (i * 8)
            cp_size = 0
            for i in range(3):
                if cmd & (1 << (4+i)):
                    x = ord(delta[index])
                    index += 1
                    cp_size |= x << (i * 8)
            if cp_size == 0:
                cp_size = 0x10000
            if (cp_off + cp_size < cp_size or
                cp_off + cp_size > src_size or
                cp_size > dest_size):
                break
            out.append(src_buf[cp_off:cp_off+cp_size])
        elif cmd != 0:
            out.append(delta[index:index+cmd])
            index += cmd
        else:
            raise ApplyDeltaError("Invalid opcode 0")

    if index != delta_length:
        raise ApplyDeltaError("delta not empty: %r" % delta[index:])

    if dest_size != chunks_length(out):
        raise ApplyDeltaError("dest size incorrect")

    return out


def write_pack_index_v2(f, entries, pack_checksum):
    """Write a new pack index file.

    :param f: File-like object to write to
    :param entries: List of tuples with object name (sha), offset_in_pack, and
        crc32_checksum.
    :param pack_checksum: Checksum of the pack file.
    :return: The SHA of the index file written
    """
    f = SHA1Writer(f)
    f.write('\377tOc') # Magic!
    f.write(struct.pack(">L", 2))
    fan_out_table = defaultdict(lambda: 0)
    for (name, offset, entry_checksum) in entries:
        fan_out_table[ord(name[0])] += 1
    # Fan-out table
    for i in range(0x100):
        f.write(struct.pack(">L", fan_out_table[i]))
        fan_out_table[i+1] += fan_out_table[i]
    for (name, offset, entry_checksum) in entries:
        f.write(name)
    for (name, offset, entry_checksum) in entries:
        f.write(struct.pack(">L", entry_checksum))
    for (name, offset, entry_checksum) in entries:
        # FIXME: handle if MSBit is set in offset
        f.write(struct.pack(">L", offset))
    # FIXME: handle table for pack files > 8 Gb
    assert len(pack_checksum) == 20
    f.write(pack_checksum)
    return f.write_sha()


class Pack(object):
    """A Git pack object."""

    def __init__(self, basename):
        self._basename = basename
        self._data = None
        self._idx = None
        self._idx_path = self._basename + ".idx"
        self._data_path = self._basename + ".pack"
        self._data_load = lambda: PackData(self._data_path)
        self._idx_load = lambda: load_pack_index(self._idx_path)

    @classmethod
    def from_lazy_objects(self, data_fn, idx_fn):
        """Create a new pack object from callables to load pack data and
        index objects."""
        ret = Pack("")
        ret._data_load = data_fn
        ret._idx_load = idx_fn
        return ret

    @classmethod
    def from_objects(self, data, idx):
        """Create a new pack object from pack data and index objects."""
        ret = Pack("")
        ret._data_load = lambda: data
        ret._idx_load = lambda: idx
        return ret

    def name(self):
        """The SHA over the SHAs of the objects in this pack."""
        return self.index.objects_sha1()

    @property
    def data(self):
        """The pack data object being used."""
        if self._data is None:
            self._data = self._data_load()
            self._data.pack = self
            assert len(self.index) == len(self._data)
            idx_stored_checksum = self.index.get_pack_checksum()
            data_stored_checksum = self._data.get_stored_checksum()
            if idx_stored_checksum != data_stored_checksum:
                raise ChecksumMismatch(sha_to_hex(idx_stored_checksum),
                                       sha_to_hex(data_stored_checksum))
        return self._data

    @property
    def index(self):
        """The index being used.

        :note: This may be an in-memory index
        """
        if self._idx is None:
            self._idx = self._idx_load()
        return self._idx

    def close(self):
        if self._data is not None:
            self._data.close()
        self.index.close()

    def __eq__(self, other):
        return type(self) == type(other) and self.index == other.index

    def __len__(self):
        """Number of entries in this pack."""
        return len(self.index)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._basename)

    def __iter__(self):
        """Iterate over all the sha1s of the objects in this pack."""
        return iter(self.index)

    def check(self):
        """Check the integrity of this pack.

        :raise ChecksumMismatch: if a checksum for the index or data is wrong
        """
        self.index.check()
        self.data.check()
        for obj in self.iterobjects():
            obj.check()
        # TODO: object connectivity checks

    def get_stored_checksum(self):
        return self.data.get_stored_checksum()

    def __contains__(self, sha1):
        """Check whether this pack contains a particular SHA1."""
        try:
            self.index.object_index(sha1)
            return True
        except KeyError:
            return False

    def get_raw(self, sha1):
        offset = self.index.object_index(sha1)
        obj_type, obj = self.data.get_object_at(offset)
        if type(offset) is long:
          offset = int(offset)
        type_num, chunks = self.data.resolve_object(offset, obj_type, obj)
        return type_num, "".join(chunks)

    def __getitem__(self, sha1):
        """Retrieve the specified SHA1."""
        type, uncomp = self.get_raw(sha1)
        return ShaFile.from_raw_string(type, uncomp)

    def iterobjects(self):
        """Iterate over the objects in this pack."""
        for offset, type, obj, crc32 in self.data.iterobjects():
            assert isinstance(offset, int)
            yield ShaFile.from_raw_chunks(
              *self.data.resolve_object(offset, type, obj))


try:
    from dulwich._pack import apply_delta, bisect_find_sha
except ImportError:
    pass
