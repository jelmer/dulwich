# archive.py -- Creating an archive from a tarball
# Copyright (C) 2015 Jonas Haag <jonas@lophus.org>
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
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

"""Generates tarballs for Git trees.

"""

import posixpath
import stat
import tarfile
from io import BytesIO
from contextlib import closing


class ChunkedBytesIO(object):
    """Turn a list of bytestrings into a file-like object.

    This is similar to creating a `BytesIO` from a concatenation of the
    bytestring list, but saves memory by NOT creating one giant bytestring first::

        BytesIO(b''.join(list_of_bytestrings)) =~= ChunkedBytesIO(list_of_bytestrings)
    """
    def __init__(self, contents):
        self.contents = contents
        self.pos = (0, 0)

    def read(self, maxbytes=None):
        if maxbytes < 0:
            maxbytes = float('inf')

        buf = []
        chunk, cursor = self.pos

        while chunk < len(self.contents):
            if maxbytes < len(self.contents[chunk]) - cursor:
                buf.append(self.contents[chunk][cursor:cursor+maxbytes])
                cursor += maxbytes
                self.pos = (chunk, cursor)
                break
            else:
                buf.append(self.contents[chunk][cursor:])
                maxbytes -= len(self.contents[chunk]) - cursor
                chunk += 1
                cursor = 0
                self.pos = (chunk, cursor)
        return b''.join(buf)


def tar_stream(store, tree, mtime, format=''):
    """Generate a tar stream for the contents of a Git tree.

    Returns a generator that lazily assembles a .tar.gz archive, yielding it in
    pieces (bytestrings). To obtain the complete .tar.gz binary file, simply
    concatenate these chunks.

    :param store: Object store to retrieve objects from
    :param tree: Tree object for the tree root
    :param mtime: UNIX timestamp that is assigned as the modification time for
        all files
    :param format: Optional compression format for tarball
    :return: Bytestrings
    """
    buf = BytesIO()
    with closing(tarfile.open(None, "w:%s" % format, buf)) as tar:
        for entry_abspath, entry in _walk_tree(store, tree):
            try:
                blob = store[entry.sha]
            except KeyError:
                # Entry probably refers to a submodule, which we don't yet support.
                continue
            data = ChunkedBytesIO(blob.chunked)

            info = tarfile.TarInfo()
            info.name = entry_abspath.decode('ascii') # tarfile only works with ascii.
            info.size = blob.raw_length()
            info.mode = entry.mode
            info.mtime = mtime

            tar.addfile(info, data)
            yield buf.getvalue()
            buf.truncate(0)
            buf.seek(0)
    yield buf.getvalue()


def _walk_tree(store, tree, root=b''):
    """Recursively walk a dulwich Tree, yielding tuples of
    (absolute path, TreeEntry) along the way.
    """
    for entry in tree.iteritems():
        entry_abspath = posixpath.join(root, entry.path)
        if stat.S_ISDIR(entry.mode):
            for _ in _walk_tree(store, store[entry.sha], entry_abspath):
                yield _
        else:
            yield (entry_abspath, entry)
