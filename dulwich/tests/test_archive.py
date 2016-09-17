# test_archive.py -- tests for archive
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
#
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

from io import BytesIO
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
