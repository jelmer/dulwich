# test_archive.py -- tests for archive
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for archive support."""

from io import BytesIO
import sys
import tarfile

from dulwich.archive import tar_stream
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    Tree,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    build_commit_graph,
    )


class ArchiveTests(TestCase):

    def test_empty(self):
        if sys.version_info[:2] <= (2, 6):
            self.skipTest("archive creation known failing on Python2.6")
        store = MemoryObjectStore()
        c1, c2, c3 = build_commit_graph(store, [[1], [2, 1], [3, 1, 2]])
        tree = store[c3.tree]
        stream = b''.join(tar_stream(store, tree, 10))
        out = BytesIO(stream)
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEqual([], tf.getnames())

    def test_simple(self):
        self.skipTest("known to fail on python2.6 and 3.4; needs debugging")
        store = MemoryObjectStore()
        b1 = Blob.from_string(b"somedata")
        store.add_object(b1)
        t1 = Tree()
        t1.add(b"somename", 0o100644, b1.id)
        store.add_object(t1)
        stream = b''.join(tar_stream(store, t1, 10))
        out = BytesIO(stream)
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEqual(["somename"], tf.getnames())
