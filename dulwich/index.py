# index.py -- File parser/write for the git index file
# Copryight (C) 2008 Jelmer Vernooij <jelmer@samba.org>
 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your opinion) any later version of the license.
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

"""Parser for the git index file format."""

import struct

def read_cache_time(f):
    return struct.unpack(">LL", f.read(8))


def read_cache_entry(f):
    beginoffset = f.tell()
    ctime = read_cache_time(f)
    mtime = read_cache_time(f)
    (ino, dev, mode, uid, gid, size, sha, flags, ) = \
        struct.unpack(">LLLLLL20sH", f.read(20 + 4 * 6 + 2))
    name = ""
    char = ord(f.read(1))
    while char != 0:
        name += chr(char)
        char = ord(f.read(1))
    # Padding:
    real_size = ((f.tell() - beginoffset + 8) & ~7)
    f.seek(beginoffset + real_size)
    return (name, ctime, mtime, ino, dev, mode, uid, gid, size, sha, flags)

class Index(object):

    def __init__(self, filename):
        f = open(filename, 'r')
        try:
            assert f.read(4) == "DIRC"
            (version, self._num_entries) = struct.unpack(">LL", f.read(4 * 2))
            assert version in (1, 2)
            self._entries = []
            for i in range(len(self)):
                self._entries.append(read_cache_entry(f))
        finally:
            f.close()

    def __len__(self):
        return self._num_entries

    def items(self):
        return list(self._entries)

    def __iter__(self):
        return iter(self._entries)
