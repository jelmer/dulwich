# pack.py -- For dealing wih packed git objects.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copryight (C) 2008 Jelmer Vernooij <jelmer@samba.org>
# The code is loosely based on that in the sha1_file.c file from git itself,
# which is Copyright (C) Linus Torvalds, 2005 and distributed under the
# GPL version 2.
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License.
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

from collections import defaultdict
import hashlib
from itertools import imap, izip
import mmap
import os
import sha
import struct
import sys
import zlib

from objects import (
        ShaFile,
        )
from errors import ApplyDeltaError

supports_mmap_offset = (sys.version_info[0] >= 3 or 
        (sys.version_info[0] == 2 and sys.version_info[1] >= 6))


def take_msb_bytes(map, offset):
    ret = []
    while len(ret) == 0 or ret[-1] & 0x80:
        ret.append(ord(map[offset]))
        offset += 1
    return ret


def read_zlib(data, offset, dec_size):
    obj = zlib.decompressobj()
    x = ""
    fed = 0
    while obj.unused_data == "":
        base = offset+fed
        add = data[base:base+1024]
        fed += len(add)
        x += obj.decompress(add)
    assert len(x) == dec_size
    comp_len = fed-len(obj.unused_data)
    return x, comp_len


def iter_sha1(iter):
    sha = hashlib.sha1()
    for name in iter:
        sha.update(name)
    return sha.hexdigest()


def hex_to_sha(hex):
  """Convert a hex string to a binary sha string."""
  ret = ""
  for i in range(0, len(hex), 2):
    ret += chr(int(hex[i:i+2], 16))
  return ret


def sha_to_hex(sha):
  """Convert a binary sha string to a hex sha string."""
  ret = ""
  for i in sha:
      ret += "%02x" % ord(i)
  return ret


MAX_MMAP_SIZE = 256 * 1024 * 1024

def simple_mmap(f, offset, size, access=mmap.ACCESS_READ):
    """Simple wrapper for mmap() which always supports the offset parameter.

    :param f: File object.
    :param offset: Offset in the file, from the beginning of the file.
    :param size: Size of the mmap'ed area
    :param access: Access mechanism.
    :return: MMAP'd area.
    """
    if offset+size > MAX_MMAP_SIZE and not supports_mmap_offset:
        raise AssertionError("%s is larger than 256 meg, and this version "
            "of Python does not support the offset argument to mmap().")
    if supports_mmap_offset:
        return mmap.mmap(f.fileno(), size, access=access, offset=offset)
    else:
        class ArraySkipper(object):

            def __init__(self, array, offset):
                self.array = array
                self.offset = offset

            def __getslice__(self, i, j):
                return self.array[i+self.offset:j+self.offset]

            def __getitem__(self, i):
                return self.array[i+self.offset]

            def __len__(self):
                return len(self.array) - self.offset

            def __str__(self):
                return str(self.array[self.offset:])

        mem = mmap.mmap(f.fileno(), size+offset, access=access)
        if offset == 0:
            return mem
        return ArraySkipper(mem, offset)


def resolve_object(offset, type, obj, get_ref, get_offset):
  """Resolve an object, possibly resolving deltas when necessary."""
  if not type in (6, 7): # Not a delta
     return type, obj

  if type == 6: # offset delta
     (delta_offset, delta) = obj
     assert isinstance(delta_offset, int)
     assert isinstance(delta, str)
     offset = offset-delta_offset
     type, base_obj = get_offset(offset)
     assert isinstance(type, int)
  elif type == 7: # ref delta
     (basename, delta) = obj
     assert isinstance(basename, str) and len(basename) == 20
     assert isinstance(delta, str)
     type, base_obj = get_ref(basename)
     assert isinstance(type, int)
  type, base_text = resolve_object(offset, type, base_obj, get_ref, get_offset)
  return type, apply_delta(base_text, delta)


class PackIndex(object):
  """An index in to a packfile.

  Given a sha id of an object a pack index can tell you the location in the
  packfile of that object if it has it.

  To do the loop it opens the file, and indexes first 256 4 byte groups
  with the first byte of the sha id. The value in the four byte group indexed
  is the end of the group that shares the same starting byte. Subtract one
  from the starting byte and index again to find the start of the group.
  The values are sorted by sha id within the group, so do the math to find
  the start and end offset and then bisect in to find if the value is present.
  """

  def __init__(self, filename):
    """Create a pack index object.

    Provide it with the name of the index file to consider, and it will map
    it whenever required.
    """
    self._filename = filename
    # Take the size now, so it can be checked each time we map the file to
    # ensure that it hasn't changed.
    self._size = os.path.getsize(filename)
    self._file = open(filename, 'r')
    self._contents = simple_mmap(self._file, 0, self._size)
    if self._contents[:4] != '\377tOc':
        self.version = 1
        self._fan_out_table = self._read_fan_out_table(0)
    else:
        (self.version, ) = struct.unpack_from(">L", self._contents, 4)
        assert self.version in (2,), "Version was %d" % self.version
        self._fan_out_table = self._read_fan_out_table(8)
        self._name_table_offset = 8 + 0x100 * 4
        self._crc32_table_offset = self._name_table_offset + 20 * len(self)
        self._pack_offset_table_offset = self._crc32_table_offset + 4 * len(self)

  def __eq__(self, other):
    if type(self) != type(other):
        return False

    if self._fan_out_table != other._fan_out_table:
        return False

    for (name1, _, _), (name2, _, _) in izip(self.iterentries(), other.iterentries()):
        if name1 != name2:
            return False
    return True

  def close(self):
    self._file.close()

  def __len__(self):
    """Return the number of entries in this pack index."""
    return self._fan_out_table[-1]

  def _unpack_entry(self, i):
    """Unpack the i-th entry in the index file.

    :return: Tuple with object name (SHA), offset in pack file and 
          CRC32 checksum (if known)."""
    if self.version == 1:
        (offset, name) = struct.unpack_from(">L20s", self._contents, 
            (0x100 * 4) + (i * 24))
        return (name, offset, None)
    else:
        return (self._unpack_name(i), self._unpack_offset(i), 
                self._unpack_crc32_checksum(i))

  def _unpack_name(self, i):
    if self.version == 1:
        return self._unpack_entry(i)[0]
    else:
        return struct.unpack_from("20s", self._contents, 
                                  self._name_table_offset + i * 20)[0]

  def _unpack_offset(self, i):
    if self.version == 1:
        return self._unpack_entry(i)[1]
    else:
        return struct.unpack_from(">L", self._contents, 
                                  self._pack_offset_table_offset + i * 4)[0]

  def _unpack_crc32_checksum(self, i):
    if self.version == 1:
        return None
    else:
        return struct.unpack_from(">L", self._contents, 
                                  self._crc32_table_offset + i * 4)[0]

  def __iter__(self):
      return imap(sha_to_hex, self._itersha())

  def _itersha(self):
    for i in range(len(self)):
        yield self._unpack_name(i)

  def objects_sha1(self):
    return iter_sha1(self._itersha())

  def iterentries(self):
    """Iterate over the entries in this pack index.
   
    Will yield tuples with object name, offset in packfile and crc32 checksum.
    """
    for i in range(len(self)):
        yield self._unpack_entry(i)

  def _read_fan_out_table(self, start_offset):
    ret = []
    for i in range(0x100):
        ret.append(struct.unpack(">L", self._contents[start_offset+i*4:start_offset+(i+1)*4])[0])
    return ret

  def check(self):
    """Check that the stored checksum matches the actual checksum."""
    return self.calculate_checksum() == self.get_stored_checksums()[1]

  def calculate_checksum(self):
    f = open(self._filename, 'r')
    try:
        return hashlib.sha1(self._contents[:-20]).digest()
    finally:
        f.close()

  def get_stored_checksums(self):
    """Return the SHA1 checksums stored for the corresponding packfile and 
    this header file itself."""
    return str(self._contents[-40:-20]), str(self._contents[-20:])

  def object_index(self, sha):
    """Return the index in to the corresponding packfile for the object.

    Given the name of an object it will return the offset that object lives
    at within the corresponding pack file. If the pack file doesn't have the
    object then None will be returned.
    """
    size = os.path.getsize(self._filename)
    assert size == self._size, "Pack index %s has changed size, I don't " \
         "like that" % self._filename
    if len(sha) == 40:
        sha = hex_to_sha(sha)
    return self._object_index(sha)

  def _object_index(self, sha):
      """See object_index"""
      idx = ord(sha[0])
      if idx == 0:
          start = 0
      else:
          start = self._fan_out_table[idx-1]
      end = self._fan_out_table[idx]
      assert start <= end
      while start <= end:
        i = (start + end)/2
        file_sha = self._unpack_name(i)
        if file_sha < sha:
          start = i + 1
        elif file_sha > sha:
          end = i - 1
        else:
          return self._unpack_offset(i)
      return None


def read_pack_header(f):
    header = f.read(12)
    assert header[:4] == "PACK"
    (version,) = struct.unpack_from(">L", header, 4)
    assert version in (2, 3), "Version was %d" % version
    (num_objects,) = struct.unpack_from(">L", header, 8)
    return (version, num_objects)


def read_pack_tail(f):
    return (f.read(20),)


def _unpack_object(map):
    bytes = take_msb_bytes(map, 0)
    type = (bytes[0] >> 4) & 0x07
    size = bytes[0] & 0x0f
    for i, byte in enumerate(bytes[1:]):
      size += (byte & 0x7f) << ((i * 7) + 4)
    raw_base = len(bytes)
    if type == 6: # offset delta
        bytes = take_msb_bytes(map, raw_base)
        assert not (bytes[-1] & 0x80)
        delta_base_offset = bytes[0] & 0x7f
        for byte in bytes[1:]:
            delta_base_offset += 1
            delta_base_offset <<= 7
            delta_base_offset += (byte & 0x7f)
        raw_base+=len(bytes)
        uncomp, comp_len = read_zlib(map, raw_base, size)
        assert size == len(uncomp)
        return type, (delta_base_offset, uncomp), comp_len+raw_base
    elif type == 7: # ref delta
        basename = map[raw_base:raw_base+20]
        uncomp, comp_len = read_zlib(map, raw_base+20, size)
        assert size == len(uncomp)
        return type, (basename, uncomp), comp_len+raw_base+20
    else:
        uncomp, comp_len = read_zlib(map, raw_base, size)
        assert len(uncomp) == size
        return type, uncomp, comp_len+raw_base


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

  Currently there are no integrity checks done. Also no attempt is made to try
  and detect the delta case, or a request for an object at the wrong position.
  It will all just throw a zlib or KeyError.
  """

  def __init__(self, filename):
    """Create a PackData object that represents the pack in the given filename.

    The file must exist and stay readable until the object is disposed of. It
    must also stay the same size. It will be mapped whenever needed.

    Currently there is a restriction on the size of the pack as the python
    mmap implementation is flawed.
    """
    self._filename = filename
    assert os.path.exists(filename), "%s is not a packfile" % filename
    self._size = os.path.getsize(filename)
    self._header_size = 12
    assert self._size >= self._header_size, "%s is too small for a packfile" % filename
    self._read_header()

  def _read_header(self):
    f = open(self._filename, 'rb')
    try:
        (version, self._num_objects) = \
                read_pack_header(f)
        f.seek(self._size-20)
        (self._stored_checksum,) = read_pack_tail(f)
    finally:
        f.close()

  def __len__(self):
      """Returns the number of objects in this pack."""
      return self._num_objects

  def calculate_checksum(self):
    f = open(self._filename, 'rb')
    try:
        map = simple_mmap(f, 0, self._size)
        return hashlib.sha1(map[:-20]).digest()
    finally:
        f.close()

  def iterobjects(self):
    offset = self._header_size
    f = open(self._filename, 'rb')
    for i in range(len(self)):
        map = simple_mmap(f, offset, self._size-offset)
        (type, obj, total_size) = _unpack_object(map)
        yield offset, type, obj
        offset += total_size
    f.close()

  def iterentries(self, ext_resolve_ref=None):
    found = {}
    at = {}
    postponed = defaultdict(list)
    class Postpone(Exception):
        """Raised to postpone delta resolving."""
        
    def get_ref_text(sha):
        if sha in found:
            return found[sha]
        if ext_resolve_ref:
            try:
                return ext_resolve_ref(sha)
            except KeyError:
                pass
        raise Postpone, (sha, )
    todo = list(self.iterobjects())
    while todo:
      (offset, type, obj) = todo.pop(0)
      at[offset] = (type, obj)
      assert isinstance(offset, int)
      assert isinstance(type, int)
      assert isinstance(obj, tuple) or isinstance(obj, str)
      try:
        type, obj = resolve_object(offset, type, obj, get_ref_text,
            at.__getitem__)
      except Postpone, (sha, ):
        postponed[sha].append((offset, type, obj))
      else:
        shafile = ShaFile.from_raw_string(type, obj)
        sha = shafile.sha().digest()
        found[sha] = (type, obj)
        yield sha, offset, shafile.crc32()
        todo += postponed.get(sha, [])
    if postponed:
        raise KeyError([sha_to_hex(h) for h in postponed.keys()])

  def sorted_entries(self, resolve_ext_ref=None):
    ret = list(self.iterentries(resolve_ext_ref))
    ret.sort()
    return ret

  def create_index_v1(self, filename):
    entries = self.sorted_entries()
    write_pack_index_v1(filename, entries, self.calculate_checksum())

  def create_index_v2(self, filename):
    entries = self.sorted_entries()
    write_pack_index_v2(filename, entries, self.calculate_checksum())

  def get_stored_checksum(self):
    return self._stored_checksum

  def check(self):
    return (self.calculate_checksum() == self.get_stored_checksum())

  def get_object_at(self, offset):
    """Given an offset in to the packfile return the object that is there.

    Using the associated index the location of an object can be looked up, and
    then the packfile can be asked directly for that object using this
    function.
    """
    assert isinstance(offset, long) or isinstance(offset, int),\
            "offset was %r" % offset
    assert offset >= self._header_size
    size = os.path.getsize(self._filename)
    assert size == self._size, "Pack data %s has changed size, I don't " \
         "like that" % self._filename
    f = open(self._filename, 'rb')
    try:
      map = simple_mmap(f, offset, size-offset)
      return _unpack_object(map)[:2]
    finally:
      f.close()


class SHA1Writer(object):
    
    def __init__(self, f):
        self.f = f
        self.sha1 = hashlib.sha1("")

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
    :param o: Object to write
    """
    ret = f.tell()
    if type == 6: # ref delta
        (delta_base_offset, object) = object
    elif type == 7: # offset delta
        (basename, object) = object
    size = len(object)
    c = (type << 4) | (size & 15)
    size >>= 4
    while size:
        f.write(chr(c | 0x80))
        c = size & 0x7f
        size >>= 7
    f.write(chr(c))
    if type == 6: # offset delta
        ret = [delta_base_offset & 0x7f]
        delta_base_offset >>= 7
        while delta_base_offset:
            delta_base_offset -= 1
            ret.insert(0, 0x80 | (delta_base_offset & 0x7f))
            delta_base_offset >>= 7
        f.write("".join([chr(x) for x in ret]))
    elif type == 7: # ref delta
        assert len(basename) == 20
        f.write(basename)
    f.write(zlib.compress(object))
    return f.tell()


def write_pack(filename, objects, num_objects):
    f = open(filename + ".pack", 'w')
    try:
        entries, data_sum = write_pack_data(f, objects, num_objects)
    except:
        f.close()
    entries.sort()
    write_pack_index_v2(filename + ".idx", entries, data_sum)


def write_pack_data(f, objects, num_objects):
    """Write a new pack file.

    :param filename: The filename of the new pack file.
    :param objects: List of objects to write.
    :return: List with (name, offset, crc32 checksum) entries, pack checksum
    """
    entries = []
    f = SHA1Writer(f)
    f.write("PACK")               # Pack header
    f.write(struct.pack(">L", 2)) # Pack version
    f.write(struct.pack(">L", num_objects)) # Number of objects in pack
    for o in objects:
        sha1 = o.sha().digest()
        crc32 = o.crc32()
        # FIXME: Delta !
        t, o = o.as_raw_string()
        offset = write_pack_object(f, t, o)
        entries.append((sha1, offset, crc32))
    return entries, f.write_sha()


def write_pack_index_v1(filename, entries, pack_checksum):
    """Write a new pack index file.

    :param filename: The filename of the new pack index file.
    :param entries: List of tuples with object name (sha), offset_in_pack,  and
            crc32_checksum.
    :param pack_checksum: Checksum of the pack file.
    """
    f = open(filename, 'w')
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
    f.close()


def apply_delta(src_buf, delta):
    """Based on the similar function in git's patch-delta.c."""
    assert isinstance(src_buf, str), "was %r" % (src_buf,)
    assert isinstance(delta, str)
    out = ""
    def pop(delta):
        ret = delta[0]
        delta = delta[1:]
        return ord(ret), delta
    def get_delta_header_size(delta):
        size = 0
        i = 0
        while delta:
            cmd, delta = pop(delta)
            size |= (cmd & ~0x80) << i
            i += 7
            if not cmd & 0x80:
                break
        return size, delta
    src_size, delta = get_delta_header_size(delta)
    dest_size, delta = get_delta_header_size(delta)
    assert src_size == len(src_buf)
    while delta:
        cmd, delta = pop(delta)
        if cmd & 0x80:
            cp_off = 0
            for i in range(4):
                if cmd & (1 << i): 
                    x, delta = pop(delta)
                    cp_off |= x << (i * 8)
            cp_size = 0
            for i in range(3):
                if cmd & (1 << (4+i)): 
                    x, delta = pop(delta)
                    cp_size |= x << (i * 8)
            if cp_size == 0: 
                cp_size = 0x10000
            if (cp_off + cp_size < cp_size or
                cp_off + cp_size > src_size or
                cp_size > dest_size):
                break
            out += src_buf[cp_off:cp_off+cp_size]
        elif cmd != 0:
            out += delta[:cmd]
            delta = delta[cmd:]
        else:
            raise ApplyDeltaError("Invalid opcode 0")
    
    if delta != "":
        raise ApplyDeltaError("delta not empty: %r" % delta)

    if dest_size != len(out):
        raise ApplyDeltaError("dest size incorrect")

    return out


def write_pack_index_v2(filename, entries, pack_checksum):
    """Write a new pack index file.

    :param filename: The filename of the new pack index file.
    :param entries: List of tuples with object name (sha), offset_in_pack,  and
            crc32_checksum.
    :param pack_checksum: Checksum of the pack file.
    """
    f = open(filename, 'w')
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
        f.write(struct.pack(">l", entry_checksum))
    for (name, offset, entry_checksum) in entries:
        # FIXME: handle if MSBit is set in offset
        f.write(struct.pack(">L", offset))
    # FIXME: handle table for pack files > 8 Gb
    assert len(pack_checksum) == 20
    f.write(pack_checksum)
    f.close()


class Pack(object):

    def __init__(self, basename):
        self._basename = basename
        self._data_path = self._basename + ".pack"
        self._idx_path = self._basename + ".idx"
        self._data = None
        self._idx = None

    def name(self):
        return self.idx.objects_sha1()

    @property
    def data(self):
        if self._data is None:
            self._data = PackData(self._data_path)
            assert len(self.idx) == len(self._data)
            assert self.idx.get_stored_checksums()[0] == self._data.get_stored_checksum()
        return self._data

    @property
    def idx(self):
        if self._idx is None:
            self._idx = PackIndex(self._idx_path)
        return self._idx

    def close(self):
        if self._data is not None:
            self._data.close()
        self.idx.close()

    def __eq__(self, other):
        return type(self) == type(other) and self.idx == other.idx

    def __len__(self):
        """Number of entries in this pack."""
        return len(self.idx)

    def __repr__(self):
        return "Pack(%r)" % self._basename

    def __iter__(self):
        """Iterate over all the sha1s of the objects in this pack."""
        return iter(self.idx)

    def check(self):
        return self.idx.check() and self.data.check()

    def get_stored_checksum(self):
        return self.data.get_stored_checksum()

    def __contains__(self, sha1):
        """Check whether this pack contains a particular SHA1."""
        return (self.idx.object_index(sha1) is not None)

    def get_raw(self, sha1, resolve_ref=None):
        if resolve_ref is None:
            resolve_ref = self.get_raw
        offset = self.idx.object_index(sha1)
        if offset is None:
            raise KeyError(sha1)

        type, obj = self.data.get_object_at(offset)
        assert isinstance(offset, int)
        return resolve_object(offset, type, obj, resolve_ref,
            self.data.get_object_at)

    def __getitem__(self, sha1):
        """Retrieve the specified SHA1."""
        type, uncomp = self.get_raw(sha1)
        return ShaFile.from_raw_string(type, uncomp)

    def iterobjects(self):
        for offset, type, obj in self.data.iterobjects():
            assert isinstance(offset, int)
            yield ShaFile.from_raw_string(
                    *resolve_object(offset, type, obj, self.get_raw, 
                self.data.get_object_at))


def load_packs(path):
    if not os.path.exists(path):
        return
    for name in os.listdir(path):
        if name.startswith("pack-") and name.endswith(".pack"):
            yield Pack(os.path.join(path, name[:-len(".pack")]))

