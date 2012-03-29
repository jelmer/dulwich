# test_index.py -- Tests for the git index
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
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

"""Tests for the index."""


from cStringIO import (
    StringIO,
    )
import os
import shutil
import stat
import struct
import sys
import tempfile

from dulwich.index import (
    Index,
    build_index_from_tree,
    cleanup_mode,
    commit_tree,
    index_entry_from_stat,
    read_index,
    write_cache_time,
    write_index,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    Tree,
    )
from dulwich.repo import Repo
from dulwich.tests import TestCase


class IndexTestCase(TestCase):

    datadir = os.path.join(os.path.dirname(__file__), 'data/indexes')

    def get_simple_index(self, name):
        return Index(os.path.join(self.datadir, name))


class SimpleIndexTestCase(IndexTestCase):

    def test_len(self):
        self.assertEqual(1, len(self.get_simple_index("index")))

    def test_iter(self):
        self.assertEqual(['bla'], list(self.get_simple_index("index")))

    def test_getitem(self):
        self.assertEqual(((1230680220, 0), (1230680220, 0), 2050, 3761020,
                           33188, 1000, 1000, 0,
                           'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', 0),
                          self.get_simple_index("index")["bla"])

    def test_empty(self):
        i = self.get_simple_index("notanindex")
        self.assertEqual(0, len(i))
        self.assertFalse(os.path.exists(i._filename))


class SimpleIndexWriterTestCase(IndexTestCase):

    def setUp(self):
        IndexTestCase.setUp(self)
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        IndexTestCase.tearDown(self)
        shutil.rmtree(self.tempdir)

    def test_simple_write(self):
        entries = [('barbla', (1230680220, 0), (1230680220, 0), 2050, 3761020,
                    33188, 1000, 1000, 0,
                    'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', 0)]
        filename = os.path.join(self.tempdir, 'test-simple-write-index')
        x = open(filename, 'w+')
        try:
            write_index(x, entries)
        finally:
            x.close()
        x = open(filename, 'r')
        try:
            self.assertEqual(entries, list(read_index(x)))
        finally:
            x.close()


class CommitTreeTests(TestCase):

    def setUp(self):
        super(CommitTreeTests, self).setUp()
        self.store = MemoryObjectStore()

    def test_single_blob(self):
        blob = Blob()
        blob.data = "foo"
        self.store.add_object(blob)
        blobs = [("bla", blob.id, stat.S_IFREG)]
        rootid = commit_tree(self.store, blobs)
        self.assertEqual(rootid, "1a1e80437220f9312e855c37ac4398b68e5c1d50")
        self.assertEqual((stat.S_IFREG, blob.id), self.store[rootid]["bla"])
        self.assertEqual(set([rootid, blob.id]), set(self.store._data.keys()))

    def test_nested(self):
        blob = Blob()
        blob.data = "foo"
        self.store.add_object(blob)
        blobs = [("bla/bar", blob.id, stat.S_IFREG)]
        rootid = commit_tree(self.store, blobs)
        self.assertEqual(rootid, "d92b959b216ad0d044671981196781b3258fa537")
        dirid = self.store[rootid]["bla"][1]
        self.assertEqual(dirid, "c1a1deb9788150829579a8b4efa6311e7b638650")
        self.assertEqual((stat.S_IFDIR, dirid), self.store[rootid]["bla"])
        self.assertEqual((stat.S_IFREG, blob.id), self.store[dirid]["bar"])
        self.assertEqual(set([rootid, dirid, blob.id]),
                          set(self.store._data.keys()))


class CleanupModeTests(TestCase):

    def test_file(self):
        self.assertEqual(0100644, cleanup_mode(0100000))

    def test_executable(self):
        self.assertEqual(0100755, cleanup_mode(0100711))

    def test_symlink(self):
        self.assertEqual(0120000, cleanup_mode(0120711))

    def test_dir(self):
        self.assertEqual(0040000, cleanup_mode(040531))

    def test_submodule(self):
        self.assertEqual(0160000, cleanup_mode(0160744))


class WriteCacheTimeTests(TestCase):

    def test_write_string(self):
        f = StringIO()
        self.assertRaises(TypeError, write_cache_time, f, "foo")

    def test_write_int(self):
        f = StringIO()
        write_cache_time(f, 434343)
        self.assertEqual(struct.pack(">LL", 434343, 0), f.getvalue())

    def test_write_tuple(self):
        f = StringIO()
        write_cache_time(f, (434343, 21))
        self.assertEqual(struct.pack(">LL", 434343, 21), f.getvalue())

    def test_write_float(self):
        f = StringIO()
        write_cache_time(f, 434343.000000021)
        self.assertEqual(struct.pack(">LL", 434343, 21), f.getvalue())


class IndexEntryFromStatTests(TestCase):

    def test_simple(self):
        st = os.stat_result((16877, 131078, 64769L,
                154, 1000, 1000, 12288,
                1323629595, 1324180496, 1324180496))
        entry = index_entry_from_stat(st, "22" * 20, 0)
        self.assertEqual(entry, (
            1324180496,
            1324180496,
            64769L,
            131078,
            16384,
            1000,
            1000,
            12288,
            '2222222222222222222222222222222222222222',
            0))

    def test_override_mode(self):
        st = os.stat_result((stat.S_IFREG + 0644, 131078, 64769L,
                154, 1000, 1000, 12288,
                1323629595, 1324180496, 1324180496))
        entry = index_entry_from_stat(st, "22" * 20, 0,
                mode=stat.S_IFREG + 0755)
        self.assertEqual(entry, (
            1324180496,
            1324180496,
            64769L,
            131078,
            33261,
            1000,
            1000,
            12288,
            '2222222222222222222222222222222222222222',
            0))


class BuildIndexTests(TestCase):

    def assertReasonableIndexEntry(self, index_entry, 
            time, mode, filesize, sha):
        delta = 1000000
        self.assertEquals(index_entry[0], index_entry[1])  # ctime and atime
        self.assertTrue(index_entry[0] > time - delta)
        self.assertEquals(index_entry[4], mode)  # mode
        if sys.platform != 'nt':
            self.assertEquals(index_entry[5], os.getuid())
            self.assertEquals(index_entry[6], os.getgid())
        self.assertEquals(index_entry[7], filesize)  # filesize
        self.assertEquals(index_entry[8], sha)  # sha

    def assertFileContents(self, path, contents, symlink=False):
        if symlink:
            self.assertEquals(os.readlink(path), contents)
        else:
            f = open(path, 'rb')
            try:
                self.assertEquals(f.read(), contents)
            finally:
                f.close()

    def test_empty(self):
        repo_dir = tempfile.mkdtemp()
        repo = Repo.init(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)

        tree = Tree()
        repo.object_store.add_object(tree)

        build_index_from_tree(repo.path, repo.index_path(),
                repo.object_store, tree.id)

        # Verify index entries
        index = repo.open_index()
        self.assertEquals(len(index), 0)

        # Verify no files
        self.assertEquals(['.git'], os.listdir(repo.path))

    def test_nonempty(self):
        if os.name != 'posix':
            self.skip("test depends on POSIX shell")

        repo_dir = tempfile.mkdtemp()
        repo = Repo.init(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)

        # Populate repo
        filea = Blob.from_string('file a')
        fileb = Blob.from_string('file b')
        filed = Blob.from_string('file d')
        filee = Blob.from_string('d')

        tree = Tree()
        tree['a'] = (stat.S_IFREG | 0644, filea.id)
        tree['b'] = (stat.S_IFREG | 0644, fileb.id)
        tree['c/d'] = (stat.S_IFREG | 0644, filed.id)
        tree['c/e'] = (stat.S_IFLNK, filee.id)  # symlink

        repo.object_store.add_objects([(o, None)
            for o in [filea, fileb, filed, filee, tree]])

        build_index_from_tree(repo.path, repo.index_path(),
                repo.object_store, tree.id)

        # Verify index entries
        import time
        ctime = time.time()
        index = repo.open_index()
        self.assertEquals(len(index), 4)

        # filea
        apath = os.path.join(repo.path, 'a')
        self.assertTrue(os.path.exists(apath))
        self.assertReasonableIndexEntry(index['a'],
            ctime, stat.S_IFREG | 0644, 6, filea.id)
        self.assertFileContents(apath, 'file a')

        # fileb
        bpath = os.path.join(repo.path, 'b')
        self.assertTrue(os.path.exists(bpath))
        self.assertReasonableIndexEntry(index['b'],
            ctime, stat.S_IFREG | 0644, 6, fileb.id)
        self.assertFileContents(bpath, 'file b')

        # filed
        dpath = os.path.join(repo.path, 'c', 'd')
        self.assertTrue(os.path.exists(dpath))
        self.assertReasonableIndexEntry(index['c/d'], 
            ctime, stat.S_IFREG | 0644, 6, filed.id)
        self.assertFileContents(dpath, 'file d')

        # symlink to d
        epath = os.path.join(repo.path, 'c', 'e')
        self.assertTrue(os.path.exists(epath))
        self.assertReasonableIndexEntry(index['c/e'], 
            ctime, stat.S_IFLNK, 1, filee.id)
        self.assertFileContents(epath, 'd', symlink=True)

        # Verify no extra files
        self.assertEquals(['.git', 'a', 'b', 'c'],
            sorted(os.listdir(repo.path)))
        self.assertEquals(['d', 'e'], 
            sorted(os.listdir(os.path.join(repo.path, 'c'))))
