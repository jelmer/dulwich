# -*- coding: utf-8 -*-
# test_index.py -- Tests for the git index
# encoding: utf-8
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
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

"""Tests for the index."""


from contextlib import closing
from io import BytesIO
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
    get_unstaged_changes,
    index_entry_from_stat,
    read_index,
    read_index_dict,
    validate_path_element_default,
    validate_path_element_ntfs,
    write_cache_time,
    write_index,
    write_index_dict,
    _tree_to_fs_path,
    _fs_to_tree_path,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    Tree,
    )
from dulwich.repo import Repo
from dulwich.tests import (
    TestCase,
    skipIf,
    )

class IndexTestCase(TestCase):

    datadir = os.path.join(os.path.dirname(__file__), 'data/indexes')

    def get_simple_index(self, name):
        return Index(os.path.join(self.datadir, name))


class SimpleIndexTestCase(IndexTestCase):

    def test_len(self):
        self.assertEqual(1, len(self.get_simple_index("index")))

    def test_iter(self):
        self.assertEqual([b'bla'], list(self.get_simple_index("index")))

    def test_getitem(self):
        self.assertEqual(((1230680220, 0), (1230680220, 0), 2050, 3761020,
                           33188, 1000, 1000, 0,
                           b'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', 0),
                          self.get_simple_index("index")[b"bla"])

    def test_empty(self):
        i = self.get_simple_index("notanindex")
        self.assertEqual(0, len(i))
        self.assertFalse(os.path.exists(i._filename))

    def test_against_empty_tree(self):
        i = self.get_simple_index("index")
        changes = list(i.changes_from_tree(MemoryObjectStore(), None))
        self.assertEqual(1, len(changes))
        (oldname, newname), (oldmode, newmode), (oldsha, newsha) = changes[0]
        self.assertEqual(b'bla', newname)
        self.assertEqual(b'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', newsha)

class SimpleIndexWriterTestCase(IndexTestCase):

    def setUp(self):
        IndexTestCase.setUp(self)
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        IndexTestCase.tearDown(self)
        shutil.rmtree(self.tempdir)

    def test_simple_write(self):
        entries = [(b'barbla', (1230680220, 0), (1230680220, 0), 2050, 3761020,
                    33188, 1000, 1000, 0,
                    b'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', 0)]
        filename = os.path.join(self.tempdir, 'test-simple-write-index')
        with open(filename, 'wb+') as x:
            write_index(x, entries)

        with open(filename, 'rb') as x:
            self.assertEqual(entries, list(read_index(x)))


class ReadIndexDictTests(IndexTestCase):

    def setUp(self):
        IndexTestCase.setUp(self)
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        IndexTestCase.tearDown(self)
        shutil.rmtree(self.tempdir)

    def test_simple_write(self):
        entries = {b'barbla': ((1230680220, 0), (1230680220, 0), 2050, 3761020,
                    33188, 1000, 1000, 0,
                    b'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391', 0)}
        filename = os.path.join(self.tempdir, 'test-simple-write-index')
        with open(filename, 'wb+') as x:
            write_index_dict(x, entries)

        with open(filename, 'rb') as x:
            self.assertEqual(entries, read_index_dict(x))


class CommitTreeTests(TestCase):

    def setUp(self):
        super(CommitTreeTests, self).setUp()
        self.store = MemoryObjectStore()

    def test_single_blob(self):
        blob = Blob()
        blob.data = b"foo"
        self.store.add_object(blob)
        blobs = [(b"bla", blob.id, stat.S_IFREG)]
        rootid = commit_tree(self.store, blobs)
        self.assertEqual(rootid, b"1a1e80437220f9312e855c37ac4398b68e5c1d50")
        self.assertEqual((stat.S_IFREG, blob.id), self.store[rootid][b"bla"])
        self.assertEqual(set([rootid, blob.id]), set(self.store._data.keys()))

    def test_nested(self):
        blob = Blob()
        blob.data = b"foo"
        self.store.add_object(blob)
        blobs = [(b"bla/bar", blob.id, stat.S_IFREG)]
        rootid = commit_tree(self.store, blobs)
        self.assertEqual(rootid, b"d92b959b216ad0d044671981196781b3258fa537")
        dirid = self.store[rootid][b"bla"][1]
        self.assertEqual(dirid, b"c1a1deb9788150829579a8b4efa6311e7b638650")
        self.assertEqual((stat.S_IFDIR, dirid), self.store[rootid][b"bla"])
        self.assertEqual((stat.S_IFREG, blob.id), self.store[dirid][b"bar"])
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
            with open(path, 'rb') as f:
                self.assertEqual(f.read(), contents)

    def test_empty(self):
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with closing(Repo.init(repo_dir)) as repo:
            tree = Tree()
            repo.object_store.add_object(tree)

            build_index_from_tree(repo.path, repo.index_path(),
                    repo.object_store, tree.id)

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 0)

            # Verify no files
            self.assertEqual(['.git'], os.listdir(repo.path))

    def test_git_dir(self):
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with closing(Repo.init(repo_dir)) as repo:

            # Populate repo
            filea = Blob.from_string(b'file a')
            filee = Blob.from_string(b'd')

            tree = Tree()
            tree[b'.git/a'] = (stat.S_IFREG | 0o644, filea.id)
            tree[b'c/e'] = (stat.S_IFREG | 0o644, filee.id)

            repo.object_store.add_objects([(o, None)
                for o in [filea, filee, tree]])

            build_index_from_tree(repo.path, repo.index_path(),
                    repo.object_store, tree.id)

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 1)

            # filea
            apath = os.path.join(repo.path, '.git', 'a')
            self.assertFalse(os.path.exists(apath))

            # filee
            epath = os.path.join(repo.path, 'c', 'e')
            self.assertTrue(os.path.exists(epath))
            self.assertReasonableIndexEntry(index[b'c/e'],
                stat.S_IFREG | 0o644, 1, filee.id)
            self.assertFileContents(epath, b'd')

    def test_nonempty(self):
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with closing(Repo.init(repo_dir)) as repo:

            # Populate repo
            filea = Blob.from_string(b'file a')
            fileb = Blob.from_string(b'file b')
            filed = Blob.from_string(b'file d')

            tree = Tree()
            tree[b'a'] = (stat.S_IFREG | 0o644, filea.id)
            tree[b'b'] = (stat.S_IFREG | 0o644, fileb.id)
            tree[b'c/d'] = (stat.S_IFREG | 0o644, filed.id)

            repo.object_store.add_objects([(o, None)
                for o in [filea, fileb, filed, tree]])

            build_index_from_tree(repo.path, repo.index_path(),
                    repo.object_store, tree.id)

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 3)

            # filea
            apath = os.path.join(repo.path, 'a')
            self.assertTrue(os.path.exists(apath))
            self.assertReasonableIndexEntry(index[b'a'],
                stat.S_IFREG | 0o644, 6, filea.id)
            self.assertFileContents(apath, b'file a')

            # fileb
            bpath = os.path.join(repo.path, 'b')
            self.assertTrue(os.path.exists(bpath))
            self.assertReasonableIndexEntry(index[b'b'],
                stat.S_IFREG | 0o644, 6, fileb.id)
            self.assertFileContents(bpath, b'file b')

            # filed
            dpath = os.path.join(repo.path, 'c', 'd')
            self.assertTrue(os.path.exists(dpath))
            self.assertReasonableIndexEntry(index[b'c/d'],
                stat.S_IFREG | 0o644, 6, filed.id)
            self.assertFileContents(dpath, b'file d')

            # Verify no extra files
            self.assertEqual(['.git', 'a', 'b', 'c'],
                sorted(os.listdir(repo.path)))
            self.assertEqual(['d'],
                sorted(os.listdir(os.path.join(repo.path, 'c'))))

    @skipIf(not getattr(os, 'symlink', None), 'Requires symlink support')
    def test_symlink(self):
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with closing(Repo.init(repo_dir)) as repo:

            # Populate repo
            filed = Blob.from_string(b'file d')
            filee = Blob.from_string(b'd')

            tree = Tree()
            tree[b'c/d'] = (stat.S_IFREG | 0o644, filed.id)
            tree[b'c/e'] = (stat.S_IFLNK, filee.id)  # symlink

            repo.object_store.add_objects([(o, None)
                for o in [filed, filee, tree]])

            build_index_from_tree(repo.path, repo.index_path(),
                    repo.object_store, tree.id)

            # Verify index entries
            index = repo.open_index()

            # symlink to d
            epath = os.path.join(repo.path, 'c', 'e')
            self.assertTrue(os.path.exists(epath))
            self.assertReasonableIndexEntry(
                index[b'c/e'], stat.S_IFLNK,
                0 if sys.platform == 'win32' else 1,
                filee.id)
            self.assertFileContents(epath, 'd', symlink=True)

    def test_no_decode_encode(self):
        repo_dir = tempfile.mkdtemp()
        repo_dir_bytes = repo_dir.encode(sys.getfilesystemencoding())
        self.addCleanup(shutil.rmtree, repo_dir)
        with closing(Repo.init(repo_dir)) as repo:

            # Populate repo
            file = Blob.from_string(b'foo')

            tree = Tree()
            latin1_name = u'À'.encode('latin1')
            utf8_name = u'À'.encode('utf8')
            tree[latin1_name] = (stat.S_IFREG | 0o644, file.id)
            tree[utf8_name] = (stat.S_IFREG | 0o644, file.id)

            repo.object_store.add_objects(
                [(o, None) for o in [file, tree]])

            build_index_from_tree(
                repo.path, repo.index_path(),
                repo.object_store, tree.id)

            # Verify index entries
            index = repo.open_index()

            latin1_path = os.path.join(repo_dir_bytes, latin1_name)
            self.assertTrue(os.path.exists(latin1_path))

            utf8_path = os.path.join(repo_dir_bytes, utf8_name)
            self.assertTrue(os.path.exists(utf8_path))


class GetUnstagedChangesTests(TestCase):

    def test_get_unstaged_changes(self):
        """Unit test for get_unstaged_changes."""

        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with closing(Repo.init(repo_dir)) as repo:

            # Commit a dummy file then modify it
            foo1_fullpath = os.path.join(repo_dir, 'foo1')
            with open(foo1_fullpath, 'wb') as f:
                f.write(b'origstuff')

            foo2_fullpath = os.path.join(repo_dir, 'foo2')
            with open(foo2_fullpath, 'wb') as f:
                f.write(b'origstuff')

            repo.stage(['foo1', 'foo2'])
            repo.do_commit(b'test status', author=b'', committer=b'')

            with open(foo1_fullpath, 'wb') as f:
                f.write(b'newstuff')

            # modify access and modify time of path
            os.utime(foo1_fullpath, (0, 0))

            changes = get_unstaged_changes(repo.open_index(), repo_dir)

            self.assertEqual(list(changes), [b'foo1'])


class TestValidatePathElement(TestCase):

    def test_default(self):
        self.assertTrue(validate_path_element_default(b"bla"))
        self.assertTrue(validate_path_element_default(b".bla"))
        self.assertFalse(validate_path_element_default(b".git"))
        self.assertFalse(validate_path_element_default(b".giT"))
        self.assertFalse(validate_path_element_default(b".."))
        self.assertTrue(validate_path_element_default(b"git~1"))

    def test_ntfs(self):
        self.assertTrue(validate_path_element_ntfs(b"bla"))
        self.assertTrue(validate_path_element_ntfs(b".bla"))
        self.assertFalse(validate_path_element_ntfs(b".git"))
        self.assertFalse(validate_path_element_ntfs(b".giT"))
        self.assertFalse(validate_path_element_ntfs(b".."))
        self.assertFalse(validate_path_element_ntfs(b"git~1"))


class TestTreeFSPathConversion(TestCase):

    def test_tree_to_fs_path(self):
        tree_path = u'délwíçh/foo'.encode('utf8')
        fs_path = _tree_to_fs_path(b'/prefix/path', tree_path)
        self.assertEqual(
            fs_path,
            os.path.join(u'/prefix/path', u'délwíçh', u'foo').encode('utf8'))

    def test_fs_to_tree_path_str(self):
        fs_path = os.path.join(os.path.join(u'délwíçh', u'foo'))
        tree_path = _fs_to_tree_path(fs_path, "utf-8")
        self.assertEqual(tree_path, u'délwíçh/foo'.encode("utf-8"))

    def test_fs_to_tree_path_bytes(self):
        fs_path = os.path.join(os.path.join(u'délwíçh', u'foo').encode('utf8'))
        tree_path = _fs_to_tree_path(fs_path, "utf-8")
        self.assertEqual(tree_path, u'délwíçh/foo'.encode('utf8'))
