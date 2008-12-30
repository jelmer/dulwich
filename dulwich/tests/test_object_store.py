# test_object_store.py -- tests for object_store.py
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
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

from dulwich.object_store import ObjectStore
from unittest import TestCase

class ObjectStoreTests(TestCase):

    def test_pack_dir(self):
        o = ObjectStore("foo")
        self.assertEquals("foo/pack", o.pack_dir())

    def test_empty_packs(self):
        o = ObjectStore("foo")
        self.assertEquals([], o.packs)



