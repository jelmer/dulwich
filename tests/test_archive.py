# test_archive.py -- tests for archive
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

"""Tests for archive support."""

import struct
import tarfile
from io import BytesIO
from typing import Optional
from unittest.mock import patch

from dulwich.archive import tar_stream
from dulwich.object_store import MemoryObjectStore
from dulwich.objects import Blob, Tree
from dulwich.tests.utils import build_commit_graph

from . import TestCase


class ArchiveTests(TestCase):
    def test_empty(self) -> None:
        store = MemoryObjectStore()
        _c1, _c2, c3 = build_commit_graph(store, [[1], [2, 1], [3, 1, 2]])
        tree = store[c3.tree]
        stream = b"".join(tar_stream(store, tree, 10))
        out = BytesIO(stream)
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEqual([], tf.getnames())

    def _get_example_tar_stream(
        self, mtime: int, prefix: bytes = b"", format: str = ""
    ) -> BytesIO:
        store = MemoryObjectStore()
        b1 = Blob.from_string(b"somedata")
        store.add_object(b1)
        t1 = Tree()
        t1.add(b"somename", 0o100644, b1.id)
        store.add_object(t1)
        stream = b"".join(tar_stream(store, t1, mtime, prefix, format))
        return BytesIO(stream)

    def test_simple(self) -> None:
        stream = self._get_example_tar_stream(mtime=0)
        tf = tarfile.TarFile(fileobj=stream)
        self.addCleanup(tf.close)
        self.assertEqual(["somename"], tf.getnames())

    def test_unicode(self) -> None:
        store = MemoryObjectStore()
        b1 = Blob.from_string(b"somedata")
        store.add_object(b1)
        t1 = Tree()
        t1.add("ő".encode(), 0o100644, b1.id)
        store.add_object(t1)
        stream = b"".join(tar_stream(store, t1, mtime=0))
        tf = tarfile.TarFile(fileobj=BytesIO(stream))
        self.addCleanup(tf.close)
        self.assertEqual(["ő"], tf.getnames())

    def test_prefix(self) -> None:
        stream = self._get_example_tar_stream(mtime=0, prefix=b"blah")
        tf = tarfile.TarFile(fileobj=stream)
        self.addCleanup(tf.close)
        self.assertEqual(["blah/somename"], tf.getnames())

    def test_gzip_mtime(self) -> None:
        stream = self._get_example_tar_stream(mtime=1234, format="gz")
        expected_mtime = struct.pack("<L", 1234)
        self.assertEqual(stream.getvalue()[4:8], expected_mtime)

    def test_same_file(self) -> None:
        contents: list[Optional[bytes]] = [None, None]
        for format in ["", "gz", "bz2"]:
            for i in [0, 1]:
                with patch("time.time", return_value=i):
                    stream = self._get_example_tar_stream(mtime=0, format=format)
                    contents[i] = stream.getvalue()
            self.assertEqual(
                contents[0],
                contents[1],
                f"Different file contents for format {format!r}",
            )
