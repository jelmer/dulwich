# test_fastexport.py -- Fast export/import functionality
# Copyright (C) 2010 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
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

from cStringIO import StringIO
import stat
from unittest import TestCase

from dulwich.fastexport import (
    FastExporter,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    Commit,
    Tree,
    )


class FastExporterTests(TestCase):

    def setUp(self):
        super(FastExporterTests, self).setUp()
        self.store = MemoryObjectStore()
        self.stream = StringIO()
        self.fastexporter = FastExporter(self.stream, self.store)

    def test_export_blob(self):
        b = Blob()
        b.data = "fooBAR"
        self.assertEquals(1, self.fastexporter.export_blob(b))
        self.assertEquals('blob\nmark :1\ndata 6\nfooBAR\n',
            self.stream.getvalue())

    def test_export_commit(self):
        b = Blob()
        b.data = "FOO"
        t = Tree()
        t.add(stat.S_IFREG | 0644, "foo", b.id)
        c = Commit()
        c.committer = c.author = "Jelmer <jelmer@host>"
        c.author_time = c.commit_time = 1271345553.47
        c.author_timezone = c.commit_timezone = 0
        c.message = "msg"
        c.tree = t.id
        self.store.add_objects([(b, None), (t, None), (c, None)])
        self.assertEquals(2,
                self.fastexporter.export_commit(c, "refs/heads/master"))
        self.assertEquals("""blob
mark :1
data 3
FOO
commit refs/heads/master
mark :2
author Jelmer <jelmer@host> 1271345553.47 +0000
committer Jelmer <jelmer@host> 1271345553.47 +0000
data 3
msg
M 100644 :1 foo

""", self.stream.getvalue())
