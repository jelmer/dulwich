# test_object_store.py -- tests for object_store.py
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# or (at your option) any later version of the License.
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


"""Tests for the object store interface."""


from unittest import TestCase

from dulwich.objects import (
    Blob,
    )
from dulwich.object_store import (
    DiskObjectStore,
    MemoryObjectStore,
    )
import os
import shutil


testobject = Blob()
testobject.data = "yummy data"


class SpecificDiskObjectStoreTests(TestCase):

    def test_pack_dir(self):
        o = DiskObjectStore("foo")
        self.assertEquals(os.path.join("foo", "pack"), o.pack_dir)

    def test_empty_packs(self):
        o = DiskObjectStore("foo")
        self.assertEquals([], o.packs)



class ObjectStoreTests(object):

    def test_iter(self):
        self.assertEquals([], list(self.store))

    def test_get_nonexistant(self):
        self.assertRaises(KeyError, self.store.__getitem__, "a" * 40)

    def test_contains_nonexistant(self):
        self.assertFalse(self.store.__contains__("a" * 40))

    def test_add_objects_empty(self):
        self.store.add_objects([])

    def test_add_commit(self):
        # TODO: Argh, no way to construct Git commit objects without 
        # access to a serialized form.
        self.store.add_objects([])

    def test_add_object(self):
        self.store.add_object(testobject)
        self.assertEquals(set([testobject.id]), set(self.store))
        self.assertTrue(self.store.__contains__(testobject.id))
        r = self.store[testobject.id]
        self.assertEquals(r, testobject)

    def test_add_objects(self):
        data = [(testobject, "mypath")]
        self.store.add_objects(data)
        self.assertEquals(set([testobject.id]), set(self.store))
        self.assertTrue(self.store.__contains__(testobject.id))
        r = self.store[testobject.id]
        self.assertEquals(r, testobject)


class MemoryObjectStoreTests(ObjectStoreTests,TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.store = MemoryObjectStore()


class DiskObjectStoreTests(ObjectStoreTests,TestCase):

    def setUp(self):
        TestCase.setUp(self)
        if os.path.exists("foo"):
            shutil.rmtree("foo")
        os.makedirs(os.path.join("foo", "pack"))
        self.store = DiskObjectStore("foo")
