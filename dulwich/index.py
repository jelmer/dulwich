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


def write_cache_time(f, t):
    f.write(struct.pack(">LL", *t))


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


def write_cache_entry(f, entry):
    beginoffset = f.tell()
    (name, ctime, mtime, ino, dev, mode, uid, gid, size, sha, flags) = entry
    write_cache_time(f, ctime)
    write_cache_time(f, mtime)
    f.write(struct.pack(">LLLLLL20sH", ino, dev, mode, uid, gid, size, sha, flags))
    f.write(name)
    f.write(chr(0))
    real_size = ((f.tell() - beginoffset + 8) & ~7)
    f.write(chr(0) * (f.tell() - (beginoffset + real_size)))
    return 

def read_index(f):
    assert f.read(4) == "DIRC"
    (version, num_entries) = struct.unpack(">LL", f.read(4 * 2))
    assert version in (1, 2)
    for i in range(num_entries):
        yield read_cache_entry(f)


def write_index(f, entries):
    f.write("DIRC")
    f.write(struct.pack(">LL", 2, len(entries)))
    for x in entries:
        write_cache_entry(f, x)


class Index(object):

    def __init__(self, filename):
        self._entries = []
        f = open(filename, 'r')
        self._byname = {}
        try:
            for x in read_index(f):
                self._entries.append(x)
                self._byname[x[0]] = x
        finally:
            f.close()

    def __len__(self):
        return len(self._entries)

    def items(self):
        return list(self._entries)

    def __iter__(self):
        return iter(self._entries)

    def __getitem__(self, name):
        return self._byname[name]
