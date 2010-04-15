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

from dulwich.fastexport import FastExporter
from dulwich.object_store import MemoryObjectStore
from dulwich.objects import Blob

from unittest import TestCase


class FastExporterTests(TestCase):

    def setUp(self):
        super(FastExporterTests, self).setUp()
        self.store = MemoryObjectStore()
        self.stream = StringIO()
        self.fastexporter = FastExporter(self.stream, self.store)

    def test_export_blob(self):
        b = Blob()
        b.data = "fooBAR"
        self.fastexporter.export_blob(b, 0)
        self.assertEquals('blob\nmark :0\ndata 6\nfooBAR\n',
            self.stream.getvalue())
