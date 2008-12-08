# pack.py -- For dealing wih packed git objects.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
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

import mmap
import os

from objects import (ShaFile,
                     _decompress,
                     )

def hex_to_sha(hex):
  """Converts a hex value to the number it represents"""
  mapping = { '0' : 0, '1' : 1, '2' : 2, '3' : 3, '4' : 4, '5' : 5, '6' : 6,
              '7' : 7, '8' : 8, '9' : 9, 'a' : 10, 'b' : 11, 'c' : 12,
              'd' : 13, 'e' : 14, 'f' : 15}
  value = 0
  for c in hex:
    value = (16 * value) + mapping[c]
  return value

def multi_ord(map, start, count):
  value = 0
  for i in range(count):
    value = value * 256 + ord(map[start+i])
  return value

max_size = 256 * 1024 * 1024

class PackIndex(object):
  """An index in to a packfile.

  Given a sha id of an object a pack index can tell you the location in the
  packfile of that object if it has it.

  To do the looup it opens the file, and indexes first 256 4 byte groups
  with the first byte of the sha id. The value in the four byte group indexed
  is the end of the group that shares the same starting byte. Subtract one
  from the starting byte and index again to find the start of the group.
  The values are sorted by sha id within the group, so do the math to find
  the start and end offset and then bisect in to find if the value is present.
  """

  header_record_size = 4
  header_size = 256 * header_record_size
  index_size = 4
  sha_bytes = 20
  record_size = sha_bytes + index_size

  def __init__(self, filename):
    """Create a pack index object.

    Provide it with the name of the index file to consider, and it will map
    it whenever required.
    """
    self._filename = filename
    assert os.path.exists(filename), "%s is not a pack index" % filename
    # Take the size now, so it can be checked each time we map the file to
    # ensure that it hasn't changed.
    self._size = os.path.getsize(filename)
    assert self._size > self.header_size, "%s is too small to be a packfile" % \
        filename
    assert self._size < max_size, "%s is larger than 256 meg, and it " \
        "might not be a good idea to mmap it. If you want to go ahead " \
        "delete this check, or get python to support mmap offsets so that " \
        "I can map the files sensibly"

  def object_index(self, sha):
    """Return the index in to the corresponding packfile for the object.

    Given the name of an object it will return the offset that object lives
    at within the corresponding pack file. If the pack file doesn't have the
    object then None will be returned.
    """
    size = os.path.getsize(self._filename)
    assert size == self._size, "Pack index %s has changed size, I don't " \
         "like that" % self._filename
    f = open(self._filename, 'rb')
    try:
      map = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
      return self._object_index(map, sha)
    finally:
      f.close()

  def _object_index(self, map, hexsha):
    """See object_index"""
    first_byte = hex_to_sha(hexsha[:2])
    header_offset = self.header_record_size * first_byte
    start = multi_ord(map, header_offset-self.header_record_size, self.header_record_size)
    end = multi_ord(map, header_offset, self.header_record_size)
    sha = hex_to_sha(hexsha)
    while start < end:
      i = (start + end)/2
      offset = self.header_size + (i * self.record_size)
      file_sha = multi_ord(map, offset + self.index_size, self.sha_bytes)
      if file_sha == sha:
        return multi_ord(map, offset, self.index_size)
      elif file_sha < sha:
        start = offset + 1
      else:
        end = offset - 1
    return None


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
    assert self._size < max_size, "%s is larger than 256 meg, and it " \
        "might not be a good idea to mmap it. If you want to go ahead " \
        "delete this check, or get python to support mmap offsets so that " \
        "I can map the files sensibly"

  def get_object_at(self, offset):
    """Given an offset in to the packfile return the object that is there.

    Using the associated index the location of an object can be looked up, and
    then the packfile can be asked directly for that object using this
    function.

    Currently only non-delta objects are supported.
    """
    size = os.path.getsize(self._filename)
    assert size == self._size, "Pack data %s has changed size, I don't " \
         "like that" % self._filename
    f = open(self._filename, 'rb')
    try:
      map = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
      return self._get_object_at(map, offset)
    finally:
      f.close()

  def _get_object_at(self, map, offset):
    first_byte = ord(map[offset])
    sign_extend = first_byte & 0x80
    type = (first_byte >> 4) & 0x07
    size = first_byte & 0x0f
    cur_offset = 0
    while sign_extend > 0:
      byte = ord(map[offset+cur_offset+1])
      sign_extend = byte & 0x80
      size_part = byte & 0x7f
      size += size_part << ((cur_offset * 7) + 4)
      cur_offset += 1
    raw_base = offset+cur_offset+1
    # The size is the inflated size, so we have no idea what the deflated size
    # is, so for now give it as much as we have. It should really iterate
    # feeding it more data if it doesn't decompress, but as we have the whole
    # thing then just use it.
    raw = map[raw_base:]
    uncomp = _decompress(raw)
    obj = ShaFile.from_raw_string(type, uncomp)
    return obj

