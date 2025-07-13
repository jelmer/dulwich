# archive.py -- Creating an archive from a tarball
# Copyright (C) 2015 Jonas Haag <jonas@lophus.org>
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Generates tarballs for Git trees."""

import posixpath
import stat
import struct
import tarfile
from collections.abc import Generator
from contextlib import closing
from io import BytesIO
from os import SEEK_END
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .object_store import BaseObjectStore
    from .objects import TreeEntry

from .objects import Tree


class ChunkedBytesIO:
    """Turn a list of bytestrings into a file-like object.

    This is similar to creating a `BytesIO` from a concatenation of the
    bytestring list, but saves memory by NOT creating one giant bytestring
    first::

        BytesIO(b''.join(list_of_bytestrings)) =~= ChunkedBytesIO(
            list_of_bytestrings)
    """

    def __init__(self, contents: list[bytes]) -> None:
        self.contents = contents
        self.pos = (0, 0)

    def read(self, maxbytes: Optional[int] = None) -> bytes:
        if maxbytes is None or maxbytes < 0:
            remaining = None
        else:
            remaining = maxbytes

        buf = []
        chunk, cursor = self.pos

        while chunk < len(self.contents):
            chunk_remainder = len(self.contents[chunk]) - cursor
            if remaining is not None and remaining < chunk_remainder:
                buf.append(self.contents[chunk][cursor : cursor + remaining])
                cursor += remaining
                self.pos = (chunk, cursor)
                break
            else:
                buf.append(self.contents[chunk][cursor:])
                if remaining is not None:
                    remaining -= chunk_remainder
                chunk += 1
                cursor = 0
                self.pos = (chunk, cursor)
        return b"".join(buf)


def tar_stream(
    store: "BaseObjectStore",
    tree: "Tree",
    mtime: int,
    prefix: bytes = b"",
    format: str = "",
) -> Generator[bytes, None, None]:
    """Generate a tar stream for the contents of a Git tree.

    Returns a generator that lazily assembles a .tar.gz archive, yielding it in
    pieces (bytestrings). To obtain the complete .tar.gz binary file, simply
    concatenate these chunks.

    Args:
      store: Object store to retrieve objects from
      tree: Tree object for the tree root
      mtime: UNIX timestamp that is assigned as the modification time for
        all files, and the gzip header modification time if format='gz'
      format: Optional compression format for tarball
    Returns:
      Bytestrings
    """
    buf = BytesIO()
    mode = "w:" + format if format else "w"
    from typing import Any, cast

    # The tarfile.open overloads are complex; cast to Any to avoid issues
    with closing(cast(Any, tarfile.open)(name=None, mode=mode, fileobj=buf)) as tar:
        if format == "gz":
            # Manually correct the gzip header file modification time so that
            # archives created from the same Git tree are always identical.
            # The gzip header file modification time is not currently
            # accessible from the tarfile API, see:
            # https://bugs.python.org/issue31526
            buf.seek(0)
            assert buf.read(2) == b"\x1f\x8b", "Invalid gzip header"
            buf.seek(4)
            buf.write(struct.pack("<L", mtime))
            buf.seek(0, SEEK_END)

        for entry_abspath, entry in _walk_tree(store, tree, prefix):
            try:
                blob = store[entry.sha]
            except KeyError:
                # Entry probably refers to a submodule, which we don't yet
                # support.
                continue
            if hasattr(blob, "chunked"):
                data = ChunkedBytesIO(blob.chunked)
            else:
                # Fallback for objects without chunked attribute
                data = ChunkedBytesIO([blob.as_raw_string()])

            info = tarfile.TarInfo()
            # tarfile only works with ascii.
            info.name = entry_abspath.decode("utf-8", "surrogateescape")
            info.size = blob.raw_length()
            info.mode = entry.mode
            info.mtime = mtime

            tar.addfile(info, data)
            yield buf.getvalue()
            buf.truncate(0)
            buf.seek(0)
    yield buf.getvalue()


def _walk_tree(
    store: "BaseObjectStore", tree: "Tree", root: bytes = b""
) -> Generator[tuple[bytes, "TreeEntry"], None, None]:
    """Recursively walk a dulwich Tree, yielding tuples of
    (absolute path, TreeEntry) along the way.
    """
    for entry in tree.iteritems():
        entry_abspath = posixpath.join(root, entry.path)
        if stat.S_ISDIR(entry.mode):
            subtree = store[entry.sha]
            if isinstance(subtree, Tree):
                yield from _walk_tree(store, subtree, entry_abspath)
        else:
            yield (entry_abspath, entry)
