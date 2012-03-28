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


from cStringIO import StringIO
import os
import shutil
import tempfile

from dulwich.index import (
    commit_tree,
    )
from dulwich.errors import (
    NotTreeError,
    )
from dulwich.objects import (
    sha_to_hex,
    object_class,
    Blob,
    Tag,
    Tree,
    TreeEntry,
    )
from dulwich.object_store import (
    DiskObjectStore,
    MemoryObjectStore,
    ObjectStoreGraphWalker,
    tree_lookup_path,
    )
from dulwich.pack import (
    REF_DELTA,
    write_pack_objects,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    make_object,
    build_pack,
    )


testobject = make_object(Blob, data="yummy data")


class ObjectStoreTests(object):

    def test_determine_wants_all(self):
        self.assertEqual(["1" * 40],
            self.store.determine_wants_all({"refs/heads/foo": "1" * 40}))

    def test_determine_wants_all_zero(self):
        self.assertEqual([],
            self.store.determine_wants_all({"refs/heads/foo": "0" * 40}))

    def test_iter(self):
        self.assertEqual([], list(self.store))

    def test_get_nonexistant(self):
        self.assertRaises(KeyError, lambda: self.store["a" * 40])

    def test_contains_nonexistant(self):
        self.assertFalse(("a" * 40) in self.store)

    def test_add_objects_empty(self):
        self.store.add_objects([])

    def test_add_commit(self):
        # TODO: Argh, no way to construct Git commit objects without 
        # access to a serialized form.
        self.store.add_objects([])

    def test_add_object(self):
        self.store.add_object(testobject)
        self.assertEqual(set([testobject.id]), set(self.store))
        self.assertTrue(testobject.id in self.store)
        r = self.store[testobject.id]
        self.assertEqual(r, testobject)

    def test_add_objects(self):
        data = [(testobject, "mypath")]
        self.store.add_objects(data)
        self.assertEqual(set([testobject.id]), set(self.store))
        self.assertTrue(testobject.id in self.store)
        r = self.store[testobject.id]
        self.assertEqual(r, testobject)

    def test_tree_changes(self):
        blob_a1 = make_object(Blob, data='a1')
        blob_a2 = make_object(Blob, data='a2')
        blob_b = make_object(Blob, data='b')
        for blob in [blob_a1, blob_a2, blob_b]:
            self.store.add_object(blob)

        blobs_1 = [('a', blob_a1.id, 0100644), ('b', blob_b.id, 0100644)]
        tree1_id = commit_tree(self.store, blobs_1)
        blobs_2 = [('a', blob_a2.id, 0100644), ('b', blob_b.id, 0100644)]
        tree2_id = commit_tree(self.store, blobs_2)
        change_a = (('a', 'a'), (0100644, 0100644), (blob_a1.id, blob_a2.id))
        self.assertEqual([change_a],
                          list(self.store.tree_changes(tree1_id, tree2_id)))
        self.assertEqual(
          [change_a, (('b', 'b'), (0100644, 0100644), (blob_b.id, blob_b.id))],
          list(self.store.tree_changes(tree1_id, tree2_id,
                                       want_unchanged=True)))

    def test_iter_tree_contents(self):
        blob_a = make_object(Blob, data='a')
        blob_b = make_object(Blob, data='b')
        blob_c = make_object(Blob, data='c')
        for blob in [blob_a, blob_b, blob_c]:
            self.store.add_object(blob)

        blobs = [
          ('a', blob_a.id, 0100644),
          ('ad/b', blob_b.id, 0100644),
          ('ad/bd/c', blob_c.id, 0100755),
          ('ad/c', blob_c.id, 0100644),
          ('c', blob_c.id, 0100644),
          ]
        tree_id = commit_tree(self.store, blobs)
        self.assertEqual([TreeEntry(p, m, h) for (p, h, m) in blobs],
                          list(self.store.iter_tree_contents(tree_id)))

    def test_iter_tree_contents_include_trees(self):
        blob_a = make_object(Blob, data='a')
        blob_b = make_object(Blob, data='b')
        blob_c = make_object(Blob, data='c')
        for blob in [blob_a, blob_b, blob_c]:
            self.store.add_object(blob)

        blobs = [
          ('a', blob_a.id, 0100644),
          ('ad/b', blob_b.id, 0100644),
          ('ad/bd/c', blob_c.id, 0100755),
          ]
        tree_id = commit_tree(self.store, blobs)
        tree = self.store[tree_id]
        tree_ad = self.store[tree['ad'][1]]
        tree_bd = self.store[tree_ad['bd'][1]]

        expected = [
          TreeEntry('', 0040000, tree_id),
          TreeEntry('a', 0100644, blob_a.id),
          TreeEntry('ad', 0040000, tree_ad.id),
          TreeEntry('ad/b', 0100644, blob_b.id),
          TreeEntry('ad/bd', 0040000, tree_bd.id),
          TreeEntry('ad/bd/c', 0100755, blob_c.id),
          ]
        actual = self.store.iter_tree_contents(tree_id, include_trees=True)
        self.assertEqual(expected, list(actual))

    def make_tag(self, name, obj):
        tag = make_object(Tag, name=name, message='',
                          tag_time=12345, tag_timezone=0,
                          tagger='Test Tagger <test@example.com>',
                          object=(object_class(obj.type_name), obj.id))
        self.store.add_object(tag)
        return tag

    def test_peel_sha(self):
        self.store.add_object(testobject)
        tag1 = self.make_tag('1', testobject)
        tag2 = self.make_tag('2', testobject)
        tag3 = self.make_tag('3', testobject)
        for obj in [testobject, tag1, tag2, tag3]:
            self.assertEqual(testobject, self.store.peel_sha(obj.id))

    def test_get_raw(self):
        self.store.add_object(testobject)
        self.assertEqual((Blob.type_num, 'yummy data'),
                         self.store.get_raw(testobject.id))


class MemoryObjectStoreTests(ObjectStoreTests, TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.store = MemoryObjectStore()


class PackBasedObjectStoreTests(ObjectStoreTests):

    def tearDown(self):
        for pack in self.store.packs:
            pack.close()

    def test_empty_packs(self):
        self.assertEqual([], self.store.packs)

    def test_pack_loose_objects(self):
        b1 = make_object(Blob, data="yummy data")
        self.store.add_object(b1)
        b2 = make_object(Blob, data="more yummy data")
        self.store.add_object(b2)
        self.assertEqual([], self.store.packs)
        self.assertEqual(2, self.store.pack_loose_objects())
        self.assertNotEquals([], self.store.packs)
        self.assertEqual(0, self.store.pack_loose_objects())


class DiskObjectStoreTests(PackBasedObjectStoreTests, TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.store_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.store_dir)
        self.store = DiskObjectStore.init(self.store_dir)

    def tearDown(self):
        TestCase.tearDown(self)
        PackBasedObjectStoreTests.tearDown(self)

    def test_alternates(self):
        alternate_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, alternate_dir)
        alternate_store = DiskObjectStore(alternate_dir)
        b2 = make_object(Blob, data="yummy data")
        alternate_store.add_object(b2)
        store = DiskObjectStore(self.store_dir)
        self.assertRaises(KeyError, store.__getitem__, b2.id)
        store.add_alternate_path(alternate_dir)
        self.assertEqual(b2, store[b2.id])

    def test_add_alternate_path(self):
        store = DiskObjectStore(self.store_dir)
        self.assertEqual([], store._read_alternate_paths())
        store.add_alternate_path("/foo/path")
        self.assertEqual(["/foo/path"], store._read_alternate_paths())
        store.add_alternate_path("/bar/path")
        self.assertEqual(
            ["/foo/path", "/bar/path"],
            store._read_alternate_paths())

    def test_pack_dir(self):
        o = DiskObjectStore(self.store_dir)
        self.assertEqual(os.path.join(self.store_dir, "pack"), o.pack_dir)

    def test_add_pack(self):
        o = DiskObjectStore(self.store_dir)
        f, commit = o.add_pack()
        b = make_object(Blob, data="more yummy data")
        write_pack_objects(f, [(b, None)])
        commit()

    def test_add_thin_pack(self):
        o = DiskObjectStore(self.store_dir)
        blob = make_object(Blob, data='yummy data')
        o.add_object(blob)

        f = StringIO()
        entries = build_pack(f, [
          (REF_DELTA, (blob.id, 'more yummy data')),
          ], store=o)
        pack = o.add_thin_pack(f.read, None)
        try:
            packed_blob_sha = sha_to_hex(entries[0][3])
            pack.check_length_and_checksum()
            self.assertEqual(sorted([blob.id, packed_blob_sha]), list(pack))
            self.assertTrue(o.contains_packed(packed_blob_sha))
            self.assertTrue(o.contains_packed(blob.id))
            self.assertEqual((Blob.type_num, 'more yummy data'),
                             o.get_raw(packed_blob_sha))
        finally:
            # FIXME: DiskObjectStore should have close() which do the following:
            for p in o._pack_cache or []:
                p.close()

            pack.close()

class TreeLookupPathTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.store = MemoryObjectStore()
        blob_a = make_object(Blob, data='a')
        blob_b = make_object(Blob, data='b')
        blob_c = make_object(Blob, data='c')
        for blob in [blob_a, blob_b, blob_c]:
            self.store.add_object(blob)

        blobs = [
          ('a', blob_a.id, 0100644),
          ('ad/b', blob_b.id, 0100644),
          ('ad/bd/c', blob_c.id, 0100755),
          ('ad/c', blob_c.id, 0100644),
          ('c', blob_c.id, 0100644),
          ]
        self.tree_id = commit_tree(self.store, blobs)

    def get_object(self, sha):
        return self.store[sha]

    def test_lookup_blob(self):
        o_id = tree_lookup_path(self.get_object, self.tree_id, 'a')[1]
        self.assertTrue(isinstance(self.store[o_id], Blob))

    def test_lookup_tree(self):
        o_id = tree_lookup_path(self.get_object, self.tree_id, 'ad')[1]
        self.assertTrue(isinstance(self.store[o_id], Tree))
        o_id = tree_lookup_path(self.get_object, self.tree_id, 'ad/bd')[1]
        self.assertTrue(isinstance(self.store[o_id], Tree))
        o_id = tree_lookup_path(self.get_object, self.tree_id, 'ad/bd/')[1]
        self.assertTrue(isinstance(self.store[o_id], Tree))

    def test_lookup_nonexistent(self):
        self.assertRaises(KeyError, tree_lookup_path, self.get_object, self.tree_id, 'j')

    def test_lookup_not_tree(self):
        self.assertRaises(NotTreeError, tree_lookup_path, self.get_object, self.tree_id, 'ad/b/j')

# TODO: MissingObjectFinderTests

class ObjectStoreGraphWalkerTests(TestCase):

    def get_walker(self, heads, parent_map):
        return ObjectStoreGraphWalker(heads,
            parent_map.__getitem__)

    def test_empty(self):
        gw = self.get_walker([], {})
        self.assertIs(None, gw.next())
        gw.ack("aa" * 20)
        self.assertIs(None, gw.next())

    def test_descends(self):
        gw = self.get_walker(["a"], {"a": ["b"], "b": []})
        self.assertEqual("a", gw.next())
        self.assertEqual("b", gw.next())

    def test_present(self):
        gw = self.get_walker(["a"], {"a": ["b"], "b": []})
        gw.ack("a")
        self.assertIs(None, gw.next())

    def test_parent_present(self):
        gw = self.get_walker(["a"], {"a": ["b"], "b": []})
        self.assertEqual("a", gw.next())
        gw.ack("a")
        self.assertIs(None, gw.next())

    def test_child_ack_later(self):
        gw = self.get_walker(["a"], {"a": ["b"], "b": ["c"], "c": []})
        self.assertEqual("a", gw.next())
        self.assertEqual("b", gw.next())
        gw.ack("a")
        self.assertIs(None, gw.next())

    def test_only_once(self):
        # a  b
        # |  |
        # c  d
        # \ /
        #  e
        gw = self.get_walker(["a", "b"], {
                "a": ["c"],
                "b": ["d"],
                "c": ["e"],
                "d": ["e"],
                "e": [],
                })
        self.assertEqual("a", gw.next())
        self.assertEqual("c", gw.next())
        gw.ack("a")
        self.assertEqual("b", gw.next())
        self.assertEqual("d", gw.next())
        self.assertIs(None, gw.next())
