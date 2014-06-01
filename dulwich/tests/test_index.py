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


from io import BytesIO
import os
import shutil
import stat
import struct
import tempfile

from dulwich.index import (
    Index,
    build_index_from_tree,
    cleanup_mode,
    commit_tree,
    get_unstaged_changes,
    index_entry_from_stat,
    read_index,
    read_index_dict,
    write_cache_time,
    write_index,
    write_index_dict,
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

    def test_against_empty_tree(self):
        i = self.get_simple_index("index")
        changes = list(i.changes_from_tree(MemoryObjectStore(), None))
        self.assertEqual(1, len(changes))
        (oldname, newname), (oldmode, newmode), (oldsha, newsha) = changes[0]
        self.assertEqual('bla', newname)
        self.assertEqual('e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', newsha)


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


class ReadIndexDictTests(IndexTestCase):

    def setUp(self):
        IndexTestCase.setUp(self)
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        IndexTestCase.tearDown(self)
        shutil.rmtree(self.tempdir)

    def test_simple_write(self):
        entries = {'barbla': ((1230680220, 0), (1230680220, 0), 2050, 3761020,
                    33188, 1000, 1000, 0,
                    'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', 0)}
        filename = os.path.join(self.tempdir, 'test-simple-write-index')
        x = open(filename, 'w+')
        try:
            write_index_dict(x, entries)
        finally:
            x.close()
        x = open(filename, 'r')
        try:
            self.assertEqual(entries, read_index_dict(x))
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
        self.assertEqual(0o100644, cleanup_mode(0o100000))

    def test_executable(self):
        self.assertEqual(0o100755, cleanup_mode(0o100711))

    def test_symlink(self):
        self.assertEqual(0o120000, cleanup_mode(0o120711))

    def test_dir(self):
        self.assertEqual(0o040000, cleanup_mode(0o40531))

    def test_submodule(self):
        self.assertEqual(0o160000, cleanup_mode(0o160744))


class WriteCacheTimeTests(TestCase):

    def test_write_string(self):
        f = BytesIO()
        self.assertRaises(TypeError, write_cache_time, f, "foo")

    def test_write_int(self):
        f = BytesIO()
        write_cache_time(f, 434343)
        self.assertEqual(struct.pack(">LL", 434343, 0), f.getvalue())

    def test_write_tuple(self):
        f = BytesIO()
        write_cache_time(f, (434343, 21))
        self.assertEqual(struct.pack(">LL", 434343, 21), f.getvalue())

    def test_write_float(self):
        f = BytesIO()
        write_cache_time(f, 434343.000000021)
        self.assertEqual(struct.pack(">LL", 434343, 21), f.getvalue())


class IndexEntryFromStatTests(TestCase):

    def test_simple(self):
        st = os.stat_result((16877, 131078, 64769,
                154, 1000, 1000, 12288,
                1323629595, 1324180496, 1324180496))
        entry = index_entry_from_stat(st, "22" * 20, 0)
        self.assertEqual(entry, (
            1324180496,
            1324180496,
            64769,
            131078,
            16384,
            1000,
            1000,
            12288,
            '2222222222222222222222222222222222222222',
            0))

    def test_override_mode(self):
        st = os.stat_result((stat.S_IFREG + 0o644, 131078, 64769,
                154, 1000, 1000, 12288,
                1323629595, 1324180496, 1324180496))
        entry = index_entry_from_stat(st, "22" * 20, 0,
                mode=stat.S_IFREG + 0o755)
        self.assertEqual(entry, (
            1324180496,
            1324180496,
            64769,
            131078,
            33261,
            1000,
            1000,
            12288,
            '2222222222222222222222222222222222222222',
            0))


class BuildIndexTests(TestCase):

    def assertReasonableIndexEntry(self, index_entry, mode, filesize, sha):
        self.assertEqual(index_entry[4], mode)  # mode
        self.assertEqual(index_entry[7], filesize)  # filesize
        self.assertEqual(index_entry[8], sha)  # sha

    def assertFileContents(self, path, contents, symlink=False):
        if symlink:
            self.assertEqual(os.readlink(path), contents)
        else:
            f = open(path, 'rb')
            try:
                self.assertEqual(f.read(), contents)
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
        self.assertEqual(len(index), 0)

        # Verify no files
        self.assertEqual(['.git'], os.listdir(repo.path))

    def test_nonempty(self):
        if os.name != 'posix':
            self.skipTest("test depends on POSIX shell")

        repo_dir = tempfile.mkdtemp()
        repo = Repo.init(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)

        # Populate repo
        filea = Blob.from_string('file a')
        fileb = Blob.from_string('file b')
        filed = Blob.from_string('file d')
        filee = Blob.from_string('d')

        tree = Tree()
        tree['a'] = (stat.S_IFREG | 0o644, filea.id)
        tree['b'] = (stat.S_IFREG | 0o644, fileb.id)
        tree['c/d'] = (stat.S_IFREG | 0o644, filed.id)
        tree['c/e'] = (stat.S_IFLNK, filee.id)  # symlink

        repo.object_store.add_objects([(o, None)
            for o in [filea, fileb, filed, filee, tree]])

        build_index_from_tree(repo.path, repo.index_path(),
                repo.object_store, tree.id)

        # Verify index entries
        index = repo.open_index()
        self.assertEqual(len(index), 4)

        # filea
        apath = os.path.join(repo.path, 'a')
        self.assertTrue(os.path.exists(apath))
        self.assertReasonableIndexEntry(index['a'],
            stat.S_IFREG | 0o644, 6, filea.id)
        self.assertFileContents(apath, 'file a')

        # fileb
        bpath = os.path.join(repo.path, 'b')
        self.assertTrue(os.path.exists(bpath))
        self.assertReasonableIndexEntry(index['b'],
            stat.S_IFREG | 0o644, 6, fileb.id)
        self.assertFileContents(bpath, 'file b')

        # filed
        dpath = os.path.join(repo.path, 'c', 'd')
        self.assertTrue(os.path.exists(dpath))
        self.assertReasonableIndexEntry(index['c/d'],
            stat.S_IFREG | 0o644, 6, filed.id)
        self.assertFileContents(dpath, 'file d')

        # symlink to d
        epath = os.path.join(repo.path, 'c', 'e')
        self.assertTrue(os.path.exists(epath))
        self.assertReasonableIndexEntry(index['c/e'],
            stat.S_IFLNK, 1, filee.id)
        self.assertFileContents(epath, 'd', symlink=True)

        # Verify no extra files
        self.assertEqual(['.git', 'a', 'b', 'c'],
            sorted(os.listdir(repo.path)))
        self.assertEqual(['d', 'e'],
            sorted(os.listdir(os.path.join(repo.path, 'c'))))


class GetUnstagedChangesTests(TestCase):

    def test_get_unstaged_changes(self):
        """Unit test for get_unstaged_changes."""

        repo_dir = tempfile.mkdtemp()
        repo = Repo.init(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)

        # Commit a dummy file then modify it
        foo1_fullpath = os.path.join(repo_dir, 'foo1')
        with open(foo1_fullpath, 'w') as f:
            f.write('origstuff')

        foo2_fullpath = os.path.join(repo_dir, 'foo2')
        with open(foo2_fullpath, 'w') as f:
            f.write('origstuff')

        repo.stage(['foo1', 'foo2'])
        repo.do_commit('test status', author='', committer='')

        with open(foo1_fullpath, 'w') as f:
            f.write('newstuff')

        # modify access and modify time of path
        os.utime(foo1_fullpath, (0, 0))

        changes = get_unstaged_changes(repo.open_index(), repo_dir)

        self.assertEqual(list(changes), ['foo1'])
