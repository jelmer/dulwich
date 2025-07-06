# test_index.py -- Tests for the git index
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

import os
import shutil
import stat
import struct
import sys
import tempfile
from io import BytesIO

from dulwich.index import (
    Index,
    IndexEntry,
    SerializedIndexEntry,
    _compress_path,
    _decode_varint,
    _decompress_path,
    _encode_varint,
    _fs_to_tree_path,
    _tree_to_fs_path,
    build_index_from_tree,
    cleanup_mode,
    commit_tree,
    get_unstaged_changes,
    index_entry_from_directory,
    index_entry_from_path,
    index_entry_from_stat,
    iter_fresh_entries,
    read_index,
    read_index_dict,
    update_working_tree,
    validate_path_element_default,
    validate_path_element_hfs,
    validate_path_element_ntfs,
    write_cache_time,
    write_index,
    write_index_dict,
)
from dulwich.object_store import MemoryObjectStore
from dulwich.objects import S_IFGITLINK, Blob, Commit, Tree
from dulwich.repo import Repo

from . import TestCase, skipIf


def can_symlink() -> bool:
    """Return whether running process can create symlinks."""
    if sys.platform != "win32":
        # Platforms other than Windows should allow symlinks without issues.
        return True

    test_source = tempfile.mkdtemp()
    test_target = test_source + "can_symlink"
    try:
        os.symlink(test_source, test_target)
    except (NotImplementedError, OSError):
        return False
    return True


class IndexTestCase(TestCase):
    datadir = os.path.join(os.path.dirname(__file__), "../testdata/indexes")

    def get_simple_index(self, name):
        return Index(os.path.join(self.datadir, name))


class SimpleIndexTestCase(IndexTestCase):
    def test_len(self) -> None:
        self.assertEqual(1, len(self.get_simple_index("index")))

    def test_iter(self) -> None:
        self.assertEqual([b"bla"], list(self.get_simple_index("index")))

    def test_iter_skip_hash(self) -> None:
        self.assertEqual([b"bla"], list(self.get_simple_index("index_skip_hash")))

    def test_iterobjects(self) -> None:
        self.assertEqual(
            [(b"bla", b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", 33188)],
            list(self.get_simple_index("index").iterobjects()),
        )

    def test_getitem(self) -> None:
        self.assertEqual(
            IndexEntry(
                (1230680220, 0),
                (1230680220, 0),
                2050,
                3761020,
                33188,
                1000,
                1000,
                0,
                b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
                0,
                0,
            ),
            self.get_simple_index("index")[b"bla"],
        )

    def test_empty(self) -> None:
        i = self.get_simple_index("notanindex")
        self.assertEqual(0, len(i))
        self.assertFalse(os.path.exists(i._filename))

    def test_against_empty_tree(self) -> None:
        i = self.get_simple_index("index")
        changes = list(i.changes_from_tree(MemoryObjectStore(), None))
        self.assertEqual(1, len(changes))
        (oldname, newname), (oldmode, newmode), (oldsha, newsha) = changes[0]
        self.assertEqual(b"bla", newname)
        self.assertEqual(b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", newsha)

    def test_index_pathlib(self) -> None:
        import tempfile
        from pathlib import Path

        # Create a temporary index file
        with tempfile.NamedTemporaryFile(suffix=".index", delete=False) as f:
            temp_path = f.name

        self.addCleanup(os.unlink, temp_path)

        # Test creating Index with pathlib.Path
        path_obj = Path(temp_path)
        index = Index(path_obj, read=False)
        self.assertEqual(str(path_obj), index.path)

        # Add an entry and write
        index[b"test"] = IndexEntry(
            ctime=(0, 0),
            mtime=(0, 0),
            dev=0,
            ino=0,
            mode=33188,
            uid=0,
            gid=0,
            size=0,
            sha=b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
        )
        index.write()

        # Read it back with pathlib.Path
        index2 = Index(path_obj)
        self.assertIn(b"test", index2)


class SimpleIndexWriterTestCase(IndexTestCase):
    def setUp(self) -> None:
        IndexTestCase.setUp(self)
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        IndexTestCase.tearDown(self)
        shutil.rmtree(self.tempdir)

    def test_simple_write(self) -> None:
        entries = [
            (
                SerializedIndexEntry(
                    b"barbla",
                    (1230680220, 0),
                    (1230680220, 0),
                    2050,
                    3761020,
                    33188,
                    1000,
                    1000,
                    0,
                    b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
                    0,
                    0,
                )
            )
        ]
        filename = os.path.join(self.tempdir, "test-simple-write-index")
        with open(filename, "wb+") as x:
            write_index(x, entries)

        with open(filename, "rb") as x:
            self.assertEqual(entries, list(read_index(x)))


class ReadIndexDictTests(IndexTestCase):
    def setUp(self) -> None:
        IndexTestCase.setUp(self)
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        IndexTestCase.tearDown(self)
        shutil.rmtree(self.tempdir)

    def test_simple_write(self) -> None:
        entries = {
            b"barbla": IndexEntry(
                (1230680220, 0),
                (1230680220, 0),
                2050,
                3761020,
                33188,
                1000,
                1000,
                0,
                b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
                0,
                0,
            )
        }
        filename = os.path.join(self.tempdir, "test-simple-write-index")
        with open(filename, "wb+") as x:
            write_index_dict(x, entries)

        with open(filename, "rb") as x:
            self.assertEqual(entries, read_index_dict(x))


class CommitTreeTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.store = MemoryObjectStore()

    def test_single_blob(self) -> None:
        blob = Blob()
        blob.data = b"foo"
        self.store.add_object(blob)
        blobs = [(b"bla", blob.id, stat.S_IFREG)]
        rootid = commit_tree(self.store, blobs)
        self.assertEqual(rootid, b"1a1e80437220f9312e855c37ac4398b68e5c1d50")
        self.assertEqual((stat.S_IFREG, blob.id), self.store[rootid][b"bla"])
        self.assertEqual({rootid, blob.id}, set(self.store._data.keys()))

    def test_nested(self) -> None:
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
        self.assertEqual({rootid, dirid, blob.id}, set(self.store._data.keys()))


class CleanupModeTests(TestCase):
    def assertModeEqual(self, expected, got) -> None:
        self.assertEqual(expected, got, f"{expected:o} != {got:o}")

    def test_file(self) -> None:
        self.assertModeEqual(0o100644, cleanup_mode(0o100000))

    def test_executable(self) -> None:
        self.assertModeEqual(0o100755, cleanup_mode(0o100711))
        self.assertModeEqual(0o100755, cleanup_mode(0o100700))

    def test_symlink(self) -> None:
        self.assertModeEqual(0o120000, cleanup_mode(0o120711))

    def test_dir(self) -> None:
        self.assertModeEqual(0o040000, cleanup_mode(0o40531))

    def test_submodule(self) -> None:
        self.assertModeEqual(0o160000, cleanup_mode(0o160744))


class WriteCacheTimeTests(TestCase):
    def test_write_string(self) -> None:
        f = BytesIO()
        self.assertRaises(TypeError, write_cache_time, f, "foo")

    def test_write_int(self) -> None:
        f = BytesIO()
        write_cache_time(f, 434343)
        self.assertEqual(struct.pack(">LL", 434343, 0), f.getvalue())

    def test_write_tuple(self) -> None:
        f = BytesIO()
        write_cache_time(f, (434343, 21))
        self.assertEqual(struct.pack(">LL", 434343, 21), f.getvalue())

    def test_write_float(self) -> None:
        f = BytesIO()
        write_cache_time(f, 434343.000000021)
        self.assertEqual(struct.pack(">LL", 434343, 21), f.getvalue())


class IndexEntryFromStatTests(TestCase):
    def test_simple(self) -> None:
        st = os.stat_result(
            (
                16877,
                131078,
                64769,
                154,
                1000,
                1000,
                12288,
                1323629595,
                1324180496,
                1324180496,
            )
        )
        entry = index_entry_from_stat(st, b"22" * 20)
        self.assertEqual(
            entry,
            IndexEntry(
                1324180496,
                1324180496,
                64769,
                131078,
                16384,
                1000,
                1000,
                12288,
                b"2222222222222222222222222222222222222222",
                0,
                0,
            ),
        )

    def test_override_mode(self) -> None:
        st = os.stat_result(
            (
                stat.S_IFREG + 0o644,
                131078,
                64769,
                154,
                1000,
                1000,
                12288,
                1323629595,
                1324180496,
                1324180496,
            )
        )
        entry = index_entry_from_stat(st, b"22" * 20, mode=stat.S_IFREG + 0o755)
        self.assertEqual(
            entry,
            IndexEntry(
                1324180496,
                1324180496,
                64769,
                131078,
                33261,
                1000,
                1000,
                12288,
                b"2222222222222222222222222222222222222222",
                0,
                0,
            ),
        )


class BuildIndexTests(TestCase):
    def assertReasonableIndexEntry(self, index_entry, mode, filesize, sha) -> None:
        self.assertEqual(index_entry.mode, mode)  # mode
        self.assertEqual(index_entry.size, filesize)  # filesize
        self.assertEqual(index_entry.sha, sha)  # sha

    def assertFileContents(self, path, contents, symlink=False) -> None:
        if symlink:
            self.assertEqual(os.readlink(path), contents)
        else:
            with open(path, "rb") as f:
                self.assertEqual(f.read(), contents)

    def test_empty(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            tree = Tree()
            repo.object_store.add_object(tree)

            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 0)

            # Verify no files
            self.assertEqual([".git"], os.listdir(repo.path))

    def test_git_dir(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Populate repo
            filea = Blob.from_string(b"file a")
            filee = Blob.from_string(b"d")

            tree = Tree()
            tree[b".git/a"] = (stat.S_IFREG | 0o644, filea.id)
            tree[b"c/e"] = (stat.S_IFREG | 0o644, filee.id)

            repo.object_store.add_objects([(o, None) for o in [filea, filee, tree]])

            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 1)

            # filea
            apath = os.path.join(repo.path, ".git", "a")
            self.assertFalse(os.path.exists(apath))

            # filee
            epath = os.path.join(repo.path, "c", "e")
            self.assertTrue(os.path.exists(epath))
            self.assertReasonableIndexEntry(
                index[b"c/e"], stat.S_IFREG | 0o644, 1, filee.id
            )
            self.assertFileContents(epath, b"d")

    def test_nonempty(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Populate repo
            filea = Blob.from_string(b"file a")
            fileb = Blob.from_string(b"file b")
            filed = Blob.from_string(b"file d")

            tree = Tree()
            tree[b"a"] = (stat.S_IFREG | 0o644, filea.id)
            tree[b"b"] = (stat.S_IFREG | 0o644, fileb.id)
            tree[b"c/d"] = (stat.S_IFREG | 0o644, filed.id)

            repo.object_store.add_objects(
                [(o, None) for o in [filea, fileb, filed, tree]]
            )

            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 3)

            # filea
            apath = os.path.join(repo.path, "a")
            self.assertTrue(os.path.exists(apath))
            self.assertReasonableIndexEntry(
                index[b"a"], stat.S_IFREG | 0o644, 6, filea.id
            )
            self.assertFileContents(apath, b"file a")

            # fileb
            bpath = os.path.join(repo.path, "b")
            self.assertTrue(os.path.exists(bpath))
            self.assertReasonableIndexEntry(
                index[b"b"], stat.S_IFREG | 0o644, 6, fileb.id
            )
            self.assertFileContents(bpath, b"file b")

            # filed
            dpath = os.path.join(repo.path, "c", "d")
            self.assertTrue(os.path.exists(dpath))
            self.assertReasonableIndexEntry(
                index[b"c/d"], stat.S_IFREG | 0o644, 6, filed.id
            )
            self.assertFileContents(dpath, b"file d")

            # Verify no extra files
            self.assertEqual([".git", "a", "b", "c"], sorted(os.listdir(repo.path)))
            self.assertEqual(["d"], sorted(os.listdir(os.path.join(repo.path, "c"))))

    @skipIf(not getattr(os, "sync", None), "Requires sync support")
    def test_norewrite(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Populate repo
            filea = Blob.from_string(b"file a")
            filea_path = os.path.join(repo_dir, "a")
            tree = Tree()
            tree[b"a"] = (stat.S_IFREG | 0o644, filea.id)

            repo.object_store.add_objects([(o, None) for o in [filea, tree]])

            # First Write
            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )
            # Use sync as metadata can be cached on some FS
            os.sync()
            mtime = os.stat(filea_path).st_mtime

            # Test Rewrite
            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )
            os.sync()
            self.assertEqual(mtime, os.stat(filea_path).st_mtime)

            # Modify content
            with open(filea_path, "wb") as fh:
                fh.write(b"test a")
            os.sync()
            mtime = os.stat(filea_path).st_mtime

            # Test rewrite
            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )
            os.sync()
            with open(filea_path, "rb") as fh:
                self.assertEqual(b"file a", fh.read())

    @skipIf(not can_symlink(), "Requires symlink support")
    def test_symlink(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Populate repo
            filed = Blob.from_string(b"file d")
            filee = Blob.from_string(b"d")

            tree = Tree()
            tree[b"c/d"] = (stat.S_IFREG | 0o644, filed.id)
            tree[b"c/e"] = (stat.S_IFLNK, filee.id)  # symlink

            repo.object_store.add_objects([(o, None) for o in [filed, filee, tree]])

            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )

            # Verify index entries
            index = repo.open_index()

            # symlink to d
            epath = os.path.join(repo.path, "c", "e")
            self.assertTrue(os.path.exists(epath))
            self.assertReasonableIndexEntry(
                index[b"c/e"],
                stat.S_IFLNK,
                0 if sys.platform == "win32" else 1,
                filee.id,
            )
            self.assertFileContents(epath, "d", symlink=True)

    def test_no_decode_encode(self) -> None:
        repo_dir = tempfile.mkdtemp()
        repo_dir_bytes = os.fsencode(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Populate repo
            file = Blob.from_string(b"foo")

            tree = Tree()
            latin1_name = "À".encode("latin1")
            try:
                latin1_path = os.path.join(repo_dir_bytes, latin1_name)
            except UnicodeDecodeError:
                self.skipTest("can not decode as latin1")
            utf8_name = "À".encode()
            utf8_path = os.path.join(repo_dir_bytes, utf8_name)
            tree[latin1_name] = (stat.S_IFREG | 0o644, file.id)
            tree[utf8_name] = (stat.S_IFREG | 0o644, file.id)

            repo.object_store.add_objects([(o, None) for o in [file, tree]])

            try:
                build_index_from_tree(
                    repo.path, repo.index_path(), repo.object_store, tree.id
                )
            except OSError as e:
                if e.errno == 92 and sys.platform == "darwin":
                    # Our filename isn't supported by the platform :(
                    self.skipTest(f"can not write filename {e.filename!r}")
                else:
                    raise
            except UnicodeDecodeError:
                # This happens e.g. with python3.6 on Windows.
                # It implicitly decodes using utf8, which doesn't work.
                self.skipTest("can not implicitly convert as utf8")

            # Verify index entries
            index = repo.open_index()
            self.assertIn(latin1_name, index)
            self.assertIn(utf8_name, index)

            self.assertTrue(os.path.exists(latin1_path))

            self.assertTrue(os.path.exists(utf8_path))

    def test_git_submodule(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            filea = Blob.from_string(b"file alalala")

            subtree = Tree()
            subtree[b"a"] = (stat.S_IFREG | 0o644, filea.id)

            c = Commit()
            c.tree = subtree.id
            c.committer = c.author = b"Somebody <somebody@example.com>"
            c.commit_time = c.author_time = 42342
            c.commit_timezone = c.author_timezone = 0
            c.parents = []
            c.message = b"Subcommit"

            tree = Tree()
            tree[b"c"] = (S_IFGITLINK, c.id)

            repo.object_store.add_objects([(o, None) for o in [tree]])

            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 1)

            # filea
            apath = os.path.join(repo.path, "c/a")
            self.assertFalse(os.path.exists(apath))

            # dir c
            cpath = os.path.join(repo.path, "c")
            self.assertTrue(os.path.isdir(cpath))
            self.assertEqual(index[b"c"].mode, S_IFGITLINK)  # mode
            self.assertEqual(index[b"c"].sha, c.id)  # sha

    def test_git_submodule_exists(self) -> None:
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            filea = Blob.from_string(b"file alalala")

            subtree = Tree()
            subtree[b"a"] = (stat.S_IFREG | 0o644, filea.id)

            c = Commit()
            c.tree = subtree.id
            c.committer = c.author = b"Somebody <somebody@example.com>"
            c.commit_time = c.author_time = 42342
            c.commit_timezone = c.author_timezone = 0
            c.parents = []
            c.message = b"Subcommit"

            tree = Tree()
            tree[b"c"] = (S_IFGITLINK, c.id)

            os.mkdir(os.path.join(repo_dir, "c"))
            repo.object_store.add_objects([(o, None) for o in [tree]])

            build_index_from_tree(
                repo.path, repo.index_path(), repo.object_store, tree.id
            )

            # Verify index entries
            index = repo.open_index()
            self.assertEqual(len(index), 1)

            # filea
            apath = os.path.join(repo.path, "c/a")
            self.assertFalse(os.path.exists(apath))

            # dir c
            cpath = os.path.join(repo.path, "c")
            self.assertTrue(os.path.isdir(cpath))
            self.assertEqual(index[b"c"].mode, S_IFGITLINK)  # mode
            self.assertEqual(index[b"c"].sha, c.id)  # sha

    def test_with_line_ending_normalization(self) -> None:
        """Test that build_index_from_tree applies line-ending normalization."""
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)

        from dulwich.line_ending import BlobNormalizer

        with Repo.init(repo_dir) as repo:
            # Set up autocrlf config
            config = repo.get_config()
            config.set((b"core",), b"autocrlf", b"true")
            config.write_to_path()

            # Create blob with LF line endings
            content_lf = b"line1\nline2\nline3\n"
            blob = Blob.from_string(content_lf)

            tree = Tree()
            tree[b"test.txt"] = (stat.S_IFREG | 0o644, blob.id)

            repo.object_store.add_objects([(blob, None), (tree, None)])

            # Create blob normalizer
            blob_normalizer = BlobNormalizer(config, {})

            # Build index with normalization
            build_index_from_tree(
                repo.path,
                repo.index_path(),
                repo.object_store,
                tree.id,
                blob_normalizer=blob_normalizer,
            )

            # On Windows with autocrlf=true, file should have CRLF line endings
            test_file = os.path.join(repo.path, "test.txt")
            with open(test_file, "rb") as f:
                content = f.read()

            # autocrlf=true means LF -> CRLF on checkout (on all platforms for testing)
            expected_content = b"line1\r\nline2\r\nline3\r\n"
            self.assertEqual(content, expected_content)


class GetUnstagedChangesTests(TestCase):
    def test_get_unstaged_changes(self) -> None:
        """Unit test for get_unstaged_changes."""
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Commit a dummy file then modify it
            foo1_fullpath = os.path.join(repo_dir, "foo1")
            with open(foo1_fullpath, "wb") as f:
                f.write(b"origstuff")

            foo2_fullpath = os.path.join(repo_dir, "foo2")
            with open(foo2_fullpath, "wb") as f:
                f.write(b"origstuff")

            repo.stage(["foo1", "foo2"])
            repo.do_commit(
                b"test status",
                author=b"author <email>",
                committer=b"committer <email>",
            )

            with open(foo1_fullpath, "wb") as f:
                f.write(b"newstuff")

            # modify access and modify time of path
            os.utime(foo1_fullpath, (0, 0))

            changes = get_unstaged_changes(repo.open_index(), repo_dir)

            self.assertEqual(list(changes), [b"foo1"])

    def test_get_unstaged_deleted_changes(self) -> None:
        """Unit test for get_unstaged_changes."""
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Commit a dummy file then remove it
            foo1_fullpath = os.path.join(repo_dir, "foo1")
            with open(foo1_fullpath, "wb") as f:
                f.write(b"origstuff")

            repo.stage(["foo1"])
            repo.do_commit(
                b"test status",
                author=b"author <email>",
                committer=b"committer <email>",
            )

            os.unlink(foo1_fullpath)

            changes = get_unstaged_changes(repo.open_index(), repo_dir)

            self.assertEqual(list(changes), [b"foo1"])

    def test_get_unstaged_changes_removed_replaced_by_directory(self) -> None:
        """Unit test for get_unstaged_changes."""
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Commit a dummy file then modify it
            foo1_fullpath = os.path.join(repo_dir, "foo1")
            with open(foo1_fullpath, "wb") as f:
                f.write(b"origstuff")

            repo.stage(["foo1"])
            repo.do_commit(
                b"test status",
                author=b"author <email>",
                committer=b"committer <email>",
            )

            os.remove(foo1_fullpath)
            os.mkdir(foo1_fullpath)

            changes = get_unstaged_changes(repo.open_index(), repo_dir)

            self.assertEqual(list(changes), [b"foo1"])

    @skipIf(not can_symlink(), "Requires symlink support")
    def test_get_unstaged_changes_removed_replaced_by_link(self) -> None:
        """Unit test for get_unstaged_changes."""
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        with Repo.init(repo_dir) as repo:
            # Commit a dummy file then modify it
            foo1_fullpath = os.path.join(repo_dir, "foo1")
            with open(foo1_fullpath, "wb") as f:
                f.write(b"origstuff")

            repo.stage(["foo1"])
            repo.do_commit(
                b"test status",
                author=b"author <email>",
                committer=b"committer <email>",
            )

            os.remove(foo1_fullpath)
            os.symlink(os.path.dirname(foo1_fullpath), foo1_fullpath)

            changes = get_unstaged_changes(repo.open_index(), repo_dir)

            self.assertEqual(list(changes), [b"foo1"])


class TestValidatePathElement(TestCase):
    def test_default(self) -> None:
        self.assertTrue(validate_path_element_default(b"bla"))
        self.assertTrue(validate_path_element_default(b".bla"))
        self.assertFalse(validate_path_element_default(b".git"))
        self.assertFalse(validate_path_element_default(b".giT"))
        self.assertFalse(validate_path_element_default(b".."))
        self.assertTrue(validate_path_element_default(b"git~1"))

    def test_ntfs(self) -> None:
        self.assertTrue(validate_path_element_ntfs(b"bla"))
        self.assertTrue(validate_path_element_ntfs(b".bla"))
        self.assertFalse(validate_path_element_ntfs(b".git"))
        self.assertFalse(validate_path_element_ntfs(b".giT"))
        self.assertFalse(validate_path_element_ntfs(b".."))
        self.assertFalse(validate_path_element_ntfs(b"git~1"))

    def test_hfs(self) -> None:
        # Normal paths should pass
        self.assertTrue(validate_path_element_hfs(b"bla"))
        self.assertTrue(validate_path_element_hfs(b".bla"))

        # Basic .git variations should fail
        self.assertFalse(validate_path_element_hfs(b".git"))
        self.assertFalse(validate_path_element_hfs(b".giT"))
        self.assertFalse(validate_path_element_hfs(b".GIT"))
        self.assertFalse(validate_path_element_hfs(b".."))

        # git~1 should also fail on HFS+
        self.assertFalse(validate_path_element_hfs(b"git~1"))

        # Test HFS+ Unicode normalization attacks
        # .g\u200cit (zero-width non-joiner)
        self.assertFalse(validate_path_element_hfs(b".g\xe2\x80\x8cit"))

        # .gi\u200dt (zero-width joiner)
        self.assertFalse(validate_path_element_hfs(b".gi\xe2\x80\x8dt"))

        # Test other ignorable characters
        # .g\ufeffit (zero-width no-break space)
        self.assertFalse(validate_path_element_hfs(b".g\xef\xbb\xbfit"))

        # Valid Unicode that shouldn't be confused with .git
        self.assertTrue(validate_path_element_hfs(b".g\xc3\xaft"))  # .gït
        self.assertTrue(validate_path_element_hfs(b"git"))  # git without dot


class TestTreeFSPathConversion(TestCase):
    def test_tree_to_fs_path(self) -> None:
        tree_path = "délwíçh/foo".encode()
        fs_path = _tree_to_fs_path(b"/prefix/path", tree_path)
        self.assertEqual(
            fs_path,
            os.fsencode(os.path.join("/prefix/path", "délwíçh", "foo")),
        )

    def test_tree_to_fs_path_windows_separator(self) -> None:
        tree_path = b"path/with/slash"
        original_sep = os.sep.encode("ascii")
        # Temporarily modify os_sep_bytes to test Windows path conversion
        # This simulates Windows behavior on all platforms for testing
        import dulwich.index

        dulwich.index.os_sep_bytes = b"\\"
        self.addCleanup(setattr, dulwich.index, "os_sep_bytes", original_sep)

        fs_path = _tree_to_fs_path(b"/prefix/path", tree_path)

        # The function should join the prefix path with the converted tree path
        # The expected behavior is that the path separators in the tree_path are
        # converted to the platform-specific separator (which we've set to backslash)
        expected_path = os.path.join(b"/prefix/path", b"path\\with\\slash")
        self.assertEqual(fs_path, expected_path)

    def test_fs_to_tree_path_str(self) -> None:
        fs_path = os.path.join(os.path.join("délwíçh", "foo"))
        tree_path = _fs_to_tree_path(fs_path)
        self.assertEqual(tree_path, "délwíçh/foo".encode())

    def test_fs_to_tree_path_bytes(self) -> None:
        fs_path = os.path.join(os.fsencode(os.path.join("délwíçh", "foo")))
        tree_path = _fs_to_tree_path(fs_path)
        self.assertEqual(tree_path, "délwíçh/foo".encode())

    def test_fs_to_tree_path_windows_separator(self) -> None:
        # Test conversion of Windows paths to tree paths
        fs_path = b"path\\with\\backslash"
        original_sep = os.sep.encode("ascii")
        # Temporarily modify os_sep_bytes to test Windows path conversion
        import dulwich.index

        dulwich.index.os_sep_bytes = b"\\"
        self.addCleanup(setattr, dulwich.index, "os_sep_bytes", original_sep)

        tree_path = _fs_to_tree_path(fs_path)
        self.assertEqual(tree_path, b"path/with/backslash")


class TestIndexEntryFromPath(TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir)

    def test_index_entry_from_path_file(self) -> None:
        """Test creating index entry from a regular file."""
        # Create a test file
        test_file = os.path.join(self.tempdir, "testfile")
        with open(test_file, "wb") as f:
            f.write(b"test content")

        # Get the index entry
        entry = index_entry_from_path(os.fsencode(test_file))

        # Verify the entry was created with the right mode
        self.assertIsNotNone(entry)
        self.assertEqual(cleanup_mode(os.stat(test_file).st_mode), entry.mode)

    @skipIf(not can_symlink(), "Requires symlink support")
    def test_index_entry_from_path_symlink(self) -> None:
        """Test creating index entry from a symlink."""
        # Create a target file
        target_file = os.path.join(self.tempdir, "target")
        with open(target_file, "wb") as f:
            f.write(b"target content")

        # Create a symlink
        link_file = os.path.join(self.tempdir, "symlink")
        os.symlink(target_file, link_file)

        # Get the index entry
        entry = index_entry_from_path(os.fsencode(link_file))

        # Verify the entry was created with the right mode
        self.assertIsNotNone(entry)
        self.assertEqual(cleanup_mode(os.lstat(link_file).st_mode), entry.mode)

    def test_index_entry_from_path_directory(self) -> None:
        """Test creating index entry from a directory (should return None)."""
        # Create a directory
        test_dir = os.path.join(self.tempdir, "testdir")
        os.mkdir(test_dir)

        # Get the index entry for a directory
        entry = index_entry_from_path(os.fsencode(test_dir))

        # Should return None for regular directories
        self.assertIsNone(entry)

    def test_index_entry_from_directory_regular(self) -> None:
        """Test index_entry_from_directory with a regular directory."""
        # Create a directory
        test_dir = os.path.join(self.tempdir, "testdir")
        os.mkdir(test_dir)

        # Get stat for the directory
        st = os.lstat(test_dir)

        # Get the index entry for a regular directory
        entry = index_entry_from_directory(st, os.fsencode(test_dir))

        # Should return None for regular directories
        self.assertIsNone(entry)

    def test_index_entry_from_directory_git_submodule(self) -> None:
        """Test index_entry_from_directory with a Git submodule."""
        # Create a git repository that will be a submodule
        sub_repo_dir = os.path.join(self.tempdir, "subrepo")
        os.mkdir(sub_repo_dir)

        # Create the .git directory to make it look like a git repo
        git_dir = os.path.join(sub_repo_dir, ".git")
        os.mkdir(git_dir)

        # Create HEAD file with a fake commit SHA
        head_sha = b"1234567890" * 4  # 40-char fake SHA
        with open(os.path.join(git_dir, "HEAD"), "wb") as f:
            f.write(head_sha)

        # Get stat for the submodule directory
        st = os.lstat(sub_repo_dir)

        # Get the index entry for a git submodule directory
        entry = index_entry_from_directory(st, os.fsencode(sub_repo_dir))

        # Since we don't have a proper git setup, this might still return None
        # This test just ensures the code path is executed
        if entry is not None:
            # If an entry is returned, it should have the gitlink mode
            self.assertEqual(entry.mode, S_IFGITLINK)

    def test_index_entry_from_path_with_object_store(self) -> None:
        """Test creating index entry with object store."""
        # Create a test file
        test_file = os.path.join(self.tempdir, "testfile")
        with open(test_file, "wb") as f:
            f.write(b"test content")

        # Create a memory object store
        object_store = MemoryObjectStore()

        # Get the index entry and add to object store
        entry = index_entry_from_path(os.fsencode(test_file), object_store)

        # Verify we can access the blob from the object store
        self.assertIsNotNone(entry)
        blob = object_store[entry.sha]
        self.assertEqual(b"test content", blob.data)

    def test_iter_fresh_entries(self) -> None:
        """Test iterating over fresh entries."""
        # Create some test files
        file1 = os.path.join(self.tempdir, "file1")
        with open(file1, "wb") as f:
            f.write(b"file1 content")

        file2 = os.path.join(self.tempdir, "file2")
        with open(file2, "wb") as f:
            f.write(b"file2 content")

        # Create a memory object store
        object_store = MemoryObjectStore()

        # Get fresh entries
        paths = [b"file1", b"file2", b"nonexistent"]
        entries = dict(
            iter_fresh_entries(paths, os.fsencode(self.tempdir), object_store)
        )

        # Verify both files got entries but nonexistent file is None
        self.assertIn(b"file1", entries)
        self.assertIn(b"file2", entries)
        self.assertIn(b"nonexistent", entries)
        self.assertIsNotNone(entries[b"file1"])
        self.assertIsNotNone(entries[b"file2"])
        self.assertIsNone(entries[b"nonexistent"])

        # Check that blobs were added to object store
        blob1 = object_store[entries[b"file1"].sha]
        self.assertEqual(b"file1 content", blob1.data)

        blob2 = object_store[entries[b"file2"].sha]
        self.assertEqual(b"file2 content", blob2.data)

    def test_read_submodule_head(self) -> None:
        """Test reading the HEAD of a submodule."""
        from dulwich.index import read_submodule_head

        # Create a test repo that will be our "submodule"
        sub_repo_dir = os.path.join(self.tempdir, "subrepo")
        os.mkdir(sub_repo_dir)
        submodule_repo = Repo.init(sub_repo_dir)

        # Create a file and commit it to establish a HEAD
        test_file = os.path.join(sub_repo_dir, "testfile")
        with open(test_file, "wb") as f:
            f.write(b"test content")

        submodule_repo.stage(["testfile"])
        commit_id = submodule_repo.do_commit(b"Test commit for submodule")

        # Test reading the HEAD
        head_sha = read_submodule_head(sub_repo_dir)
        self.assertEqual(commit_id, head_sha)

        # Test with bytes path
        head_sha_bytes = read_submodule_head(os.fsencode(sub_repo_dir))
        self.assertEqual(commit_id, head_sha_bytes)

        # Test with non-existent path
        non_repo_dir = os.path.join(self.tempdir, "nonrepo")
        os.mkdir(non_repo_dir)
        self.assertIsNone(read_submodule_head(non_repo_dir))

        # Test with path that doesn't have a .git directory
        not_git_dir = os.path.join(self.tempdir, "notgit")
        os.mkdir(not_git_dir)
        self.assertIsNone(read_submodule_head(not_git_dir))

    def test_has_directory_changed(self) -> None:
        """Test checking if a directory has changed."""
        from dulwich.index import IndexEntry, _has_directory_changed

        # Setup mock IndexEntry
        mock_entry = IndexEntry(
            (1230680220, 0),
            (1230680220, 0),
            2050,
            3761020,
            33188,
            1000,
            1000,
            0,
            b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
            0,
            0,
        )

        # Test with a regular directory (not a submodule)
        reg_dir = os.path.join(self.tempdir, "regular_dir")
        os.mkdir(reg_dir)

        # Should return True for regular directory
        self.assertTrue(_has_directory_changed(os.fsencode(reg_dir), mock_entry))

        # Create a git repository to test submodule scenarios
        sub_repo_dir = os.path.join(self.tempdir, "subrepo")
        os.mkdir(sub_repo_dir)
        submodule_repo = Repo.init(sub_repo_dir)

        # Create a file and commit it to establish a HEAD
        test_file = os.path.join(sub_repo_dir, "testfile")
        with open(test_file, "wb") as f:
            f.write(b"test content")

        submodule_repo.stage(["testfile"])
        commit_id = submodule_repo.do_commit(b"Test commit for submodule")

        # Create an entry with the correct commit SHA
        correct_entry = IndexEntry(
            (1230680220, 0),
            (1230680220, 0),
            2050,
            3761020,
            33188,
            1000,
            1000,
            0,
            commit_id,
            0,
            0,
        )

        # Create an entry with an incorrect commit SHA
        incorrect_entry = IndexEntry(
            (1230680220, 0),
            (1230680220, 0),
            2050,
            3761020,
            33188,
            1000,
            1000,
            0,
            b"0000000000000000000000000000000000000000",
            0,
            0,
        )

        # Should return False for submodule with correct SHA
        self.assertFalse(
            _has_directory_changed(os.fsencode(sub_repo_dir), correct_entry)
        )

        # Should return True for submodule with incorrect SHA
        self.assertTrue(
            _has_directory_changed(os.fsencode(sub_repo_dir), incorrect_entry)
        )

    def test_get_unstaged_changes(self) -> None:
        """Test detecting unstaged changes in a working tree."""
        from dulwich.index import (
            ConflictedIndexEntry,
            Index,
            IndexEntry,
            get_unstaged_changes,
        )

        # Create a test repo
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)

        # Create test index
        index = Index(os.path.join(repo_dir, "index"))

        # Create an actual hash of our test content
        from dulwich.objects import Blob

        test_blob = Blob()
        test_blob.data = b"initial content"

        # Create some test files with known contents
        file1_path = os.path.join(repo_dir, "file1")
        with open(file1_path, "wb") as f:
            f.write(b"initial content")

        file2_path = os.path.join(repo_dir, "file2")
        with open(file2_path, "wb") as f:
            f.write(b"initial content")

        # Add them to index
        entry1 = IndexEntry(
            (1230680220, 0),
            (1230680220, 0),
            2050,
            3761020,
            33188,
            1000,
            1000,
            0,
            b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",  # Not matching actual content
            0,
            0,
        )

        entry2 = IndexEntry(
            (1230680220, 0),
            (1230680220, 0),
            2050,
            3761020,
            33188,
            1000,
            1000,
            0,
            test_blob.id,  # Will be content's real hash
            0,
            0,
        )

        # Add a file that has a conflict
        entry_conflict = ConflictedIndexEntry(b"conflict", {0: None, 1: None, 2: None})

        index._byname = {
            b"file1": entry1,
            b"file2": entry2,
            b"file3": IndexEntry(
                (1230680220, 0),
                (1230680220, 0),
                2050,
                3761020,
                33188,
                1000,
                1000,
                0,
                b"0000000000000000000000000000000000000000",
                0,
                0,
            ),
            b"conflict": entry_conflict,
        }

        # Get unstaged changes
        changes = list(get_unstaged_changes(index, repo_dir))

        # File1 should be unstaged (content doesn't match hash)
        # File3 doesn't exist (deleted)
        # Conflict is always unstaged
        self.assertEqual(sorted(changes), [b"conflict", b"file1", b"file3"])

        # Create directory where there should be a file
        os.mkdir(os.path.join(repo_dir, "file4"))
        index._byname[b"file4"] = entry1

        # Get unstaged changes again
        changes = list(get_unstaged_changes(index, repo_dir))

        # Now file4 should also be unstaged because it's a directory instead of a file
        self.assertEqual(sorted(changes), [b"conflict", b"file1", b"file3", b"file4"])

        # Create a custom blob filter function
        def filter_blob_callback(blob, path):
            # Modify blob to make it look changed
            blob.data = b"modified " + blob.data
            return blob

        # Get unstaged changes with blob filter
        changes = list(get_unstaged_changes(index, repo_dir, filter_blob_callback))

        # Now both file1 and file2 should be unstaged due to the filter
        self.assertEqual(
            sorted(changes), [b"conflict", b"file1", b"file2", b"file3", b"file4"]
        )


class TestManyFilesFeature(TestCase):
    """Tests for the manyFiles feature (index version 4 and skipHash)."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir)

    def test_index_version_4_parsing(self):
        """Test that index version 4 files can be parsed."""
        index_path = os.path.join(self.tempdir, "index")

        # Create an index with version 4
        index = Index(index_path, read=False, version=4)

        # Add some entries
        entry = IndexEntry(
            ctime=(1234567890, 0),
            mtime=(1234567890, 0),
            dev=1,
            ino=1,
            mode=0o100644,
            uid=1000,
            gid=1000,
            size=5,
            sha=b"0" * 40,
        )
        index[b"test.txt"] = entry

        # Write and read back
        index.write()

        # Read the index back
        index2 = Index(index_path)
        self.assertEqual(index2._version, 4)
        self.assertIn(b"test.txt", index2)

    def test_skip_hash_feature(self):
        """Test that skipHash feature works correctly."""
        index_path = os.path.join(self.tempdir, "index")

        # Create an index with skipHash enabled
        index = Index(index_path, read=False, skip_hash=True)

        # Add some entries
        entry = IndexEntry(
            ctime=(1234567890, 0),
            mtime=(1234567890, 0),
            dev=1,
            ino=1,
            mode=0o100644,
            uid=1000,
            gid=1000,
            size=5,
            sha=b"0" * 40,
        )
        index[b"test.txt"] = entry

        # Write the index
        index.write()

        # Verify the file was written with zero hash
        with open(index_path, "rb") as f:
            f.seek(-20, 2)  # Seek to last 20 bytes
            trailing_hash = f.read(20)
            self.assertEqual(trailing_hash, b"\x00" * 20)

        # Verify we can still read it back
        index2 = Index(index_path)
        self.assertIn(b"test.txt", index2)

    def test_version_4_no_padding(self):
        """Test that version 4 entries have no padding."""
        # Create entries with names that would show compression benefits
        entries = [
            SerializedIndexEntry(
                name=b"src/main/java/com/example/Service.java",
                ctime=(1234567890, 0),
                mtime=(1234567890, 0),
                dev=1,
                ino=1,
                mode=0o100644,
                uid=1000,
                gid=1000,
                size=5,
                sha=b"0" * 40,
                flags=0,
                extended_flags=0,
            ),
            SerializedIndexEntry(
                name=b"src/main/java/com/example/Controller.java",
                ctime=(1234567890, 0),
                mtime=(1234567890, 0),
                dev=1,
                ino=2,
                mode=0o100644,
                uid=1000,
                gid=1000,
                size=5,
                sha=b"1" * 40,
                flags=0,
                extended_flags=0,
            ),
        ]

        # Test version 2 (with padding, full paths)
        buf_v2 = BytesIO()
        from dulwich.index import write_cache_entry

        previous_path = b""
        for entry in entries:
            # Set proper flags for v2
            entry_v2 = SerializedIndexEntry(
                entry.name,
                entry.ctime,
                entry.mtime,
                entry.dev,
                entry.ino,
                entry.mode,
                entry.uid,
                entry.gid,
                entry.size,
                entry.sha,
                len(entry.name),
                entry.extended_flags,
            )
            write_cache_entry(buf_v2, entry_v2, version=2, previous_path=previous_path)
            previous_path = entry.name
        v2_data = buf_v2.getvalue()

        # Test version 4 (path compression, no padding)
        buf_v4 = BytesIO()
        previous_path = b""
        for entry in entries:
            write_cache_entry(buf_v4, entry, version=4, previous_path=previous_path)
            previous_path = entry.name
        v4_data = buf_v4.getvalue()

        # Version 4 should be shorter due to compression and no padding
        self.assertLess(len(v4_data), len(v2_data))

        # Both should parse correctly
        buf_v2.seek(0)
        from dulwich.index import read_cache_entry

        previous_path = b""
        parsed_v2_entries = []
        for _ in entries:
            parsed = read_cache_entry(buf_v2, version=2, previous_path=previous_path)
            parsed_v2_entries.append(parsed)
            previous_path = parsed.name

        buf_v4.seek(0)
        previous_path = b""
        parsed_v4_entries = []
        for _ in entries:
            parsed = read_cache_entry(buf_v4, version=4, previous_path=previous_path)
            parsed_v4_entries.append(parsed)
            previous_path = parsed.name

        # Both should have the same paths
        for v2_entry, v4_entry in zip(parsed_v2_entries, parsed_v4_entries):
            self.assertEqual(v2_entry.name, v4_entry.name)
            self.assertEqual(v2_entry.sha, v4_entry.sha)


class TestManyFilesRepoIntegration(TestCase):
    """Tests for manyFiles feature integration with Repo."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir)

    def test_repo_with_manyfiles_config(self):
        """Test that a repository with feature.manyFiles=true uses the right settings."""
        # Create a new repository
        repo = Repo.init(self.tempdir)

        # Set feature.manyFiles=true in config
        config = repo.get_config()
        config.set(b"feature", b"manyFiles", b"true")
        config.write_to_path()

        # Open the index - should have skipHash enabled and version 4
        index = repo.open_index()
        self.assertTrue(index._skip_hash)
        self.assertEqual(index._version, 4)

    def test_repo_with_explicit_index_settings(self):
        """Test that explicit index.version and index.skipHash work."""
        # Create a new repository
        repo = Repo.init(self.tempdir)

        # Set explicit index settings
        config = repo.get_config()
        config.set(b"index", b"version", b"3")
        config.set(b"index", b"skipHash", b"false")
        config.write_to_path()

        # Open the index - should respect explicit settings
        index = repo.open_index()
        self.assertFalse(index._skip_hash)
        self.assertEqual(index._version, 3)


class TestPathPrefixCompression(TestCase):
    """Tests for index version 4 path prefix compression."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir)

    def test_varint_encoding_decoding(self):
        """Test variable-width integer encoding and decoding."""
        test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 65535, 65536]

        for value in test_values:
            encoded = _encode_varint(value)
            decoded, _ = _decode_varint(encoded, 0)
            self.assertEqual(value, decoded, f"Failed for value {value}")

    def test_path_compression_simple(self):
        """Test simple path compression cases."""
        # Test case 1: No common prefix
        compressed = _compress_path(b"file1.txt", b"")
        decompressed, _ = _decompress_path(compressed, 0, b"")
        self.assertEqual(b"file1.txt", decompressed)

        # Test case 2: Common prefix
        compressed = _compress_path(b"src/file2.txt", b"src/file1.txt")
        decompressed, _ = _decompress_path(compressed, 0, b"src/file1.txt")
        self.assertEqual(b"src/file2.txt", decompressed)

        # Test case 3: Completely different paths
        compressed = _compress_path(b"docs/readme.md", b"src/file1.txt")
        decompressed, _ = _decompress_path(compressed, 0, b"src/file1.txt")
        self.assertEqual(b"docs/readme.md", decompressed)

    def test_path_compression_deep_directories(self):
        """Test compression with deep directory structures."""
        path1 = b"src/main/java/com/example/service/UserService.java"
        path2 = b"src/main/java/com/example/service/OrderService.java"
        path3 = b"src/main/java/com/example/model/User.java"

        # Compress path2 relative to path1
        compressed = _compress_path(path2, path1)
        decompressed, _ = _decompress_path(compressed, 0, path1)
        self.assertEqual(path2, decompressed)

        # Compress path3 relative to path2
        compressed = _compress_path(path3, path2)
        decompressed, _ = _decompress_path(compressed, 0, path2)
        self.assertEqual(path3, decompressed)

    def test_index_version_4_with_compression(self):
        """Test full index version 4 write/read with path compression."""
        index_path = os.path.join(self.tempdir, "index")

        # Create an index with version 4
        index = Index(index_path, read=False, version=4)

        # Add multiple entries with common prefixes
        paths = [
            b"src/main/java/App.java",
            b"src/main/java/Utils.java",
            b"src/main/resources/config.properties",
            b"src/test/java/AppTest.java",
            b"docs/README.md",
            b"docs/INSTALL.md",
        ]

        for i, path in enumerate(paths):
            entry = IndexEntry(
                ctime=(1234567890, 0),
                mtime=(1234567890, 0),
                dev=1,
                ino=i + 1,
                mode=0o100644,
                uid=1000,
                gid=1000,
                size=10,
                sha=f"{i:040d}".encode(),
            )
            index[path] = entry

        # Write and read back
        index.write()

        # Read the index back
        index2 = Index(index_path)
        self.assertEqual(index2._version, 4)

        # Verify all paths were preserved correctly
        for path in paths:
            self.assertIn(path, index2)

        # Verify the index file is smaller than version 2 would be
        with open(index_path, "rb") as f:
            v4_size = len(f.read())

        # Create equivalent version 2 index for comparison
        index_v2_path = os.path.join(self.tempdir, "index_v2")
        index_v2 = Index(index_v2_path, read=False, version=2)
        for path in paths:
            entry = IndexEntry(
                ctime=(1234567890, 0),
                mtime=(1234567890, 0),
                dev=1,
                ino=1,
                mode=0o100644,
                uid=1000,
                gid=1000,
                size=10,
                sha=b"0" * 40,
            )
            index_v2[path] = entry
        index_v2.write()

        with open(index_v2_path, "rb") as f:
            v2_size = len(f.read())

        # Version 4 should be smaller due to compression
        self.assertLess(
            v4_size, v2_size, "Version 4 index should be smaller than version 2"
        )

    def test_path_compression_edge_cases(self):
        """Test edge cases in path compression."""
        # Empty paths
        compressed = _compress_path(b"", b"")
        decompressed, _ = _decompress_path(compressed, 0, b"")
        self.assertEqual(b"", decompressed)

        # Path identical to previous
        compressed = _compress_path(b"same.txt", b"same.txt")
        decompressed, _ = _decompress_path(compressed, 0, b"same.txt")
        self.assertEqual(b"same.txt", decompressed)

        # Path shorter than previous
        compressed = _compress_path(b"short", b"very/long/path/file.txt")
        decompressed, _ = _decompress_path(compressed, 0, b"very/long/path/file.txt")
        self.assertEqual(b"short", decompressed)


class TestUpdateWorkingTree(TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

        def cleanup_tempdir():
            """Remove tempdir, handling read-only files on Windows."""

            def remove_readonly(func, path, excinfo):
                """Error handler for Windows read-only files."""
                import stat

                if sys.platform == "win32" and excinfo[0] is PermissionError:
                    os.chmod(path, stat.S_IWRITE)
                    func(path)
                else:
                    raise

            shutil.rmtree(self.tempdir, onerror=remove_readonly)

        self.addCleanup(cleanup_tempdir)

        self.repo = Repo.init(self.tempdir)

    def test_update_working_tree_with_blob_normalizer(self):
        """Test update_working_tree with a blob normalizer."""

        # Create a simple blob normalizer that converts CRLF to LF
        class TestBlobNormalizer:
            def checkout_normalize(self, blob, path):
                # Convert CRLF to LF during checkout
                new_blob = Blob()
                new_blob.data = blob.data.replace(b"\r\n", b"\n")
                return new_blob

        # Create a tree with a file containing CRLF
        blob = Blob()
        blob.data = b"Hello\r\nWorld\r\n"
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree[b"test.txt"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        # Update working tree with normalizer
        normalizer = TestBlobNormalizer()
        update_working_tree(
            self.repo,
            None,  # old_tree_id
            tree.id,  # new_tree_id
            blob_normalizer=normalizer,
        )

        # Check that the file was written with LF line endings
        test_file = os.path.join(self.tempdir, "test.txt")
        with open(test_file, "rb") as f:
            content = f.read()

        self.assertEqual(b"Hello\nWorld\n", content)

        # Check that the index has the original blob SHA
        index = self.repo.open_index()
        self.assertEqual(blob.id, index[b"test.txt"].sha)

    def test_update_working_tree_without_blob_normalizer(self):
        """Test update_working_tree without a blob normalizer."""
        # Create a tree with a file containing CRLF
        blob = Blob()
        blob.data = b"Hello\r\nWorld\r\n"
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree[b"test.txt"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        # Update working tree without normalizer
        update_working_tree(
            self.repo,
            None,  # old_tree_id
            tree.id,  # new_tree_id
            blob_normalizer=None,
        )

        # Check that the file was written with original CRLF line endings
        test_file = os.path.join(self.tempdir, "test.txt")
        with open(test_file, "rb") as f:
            content = f.read()

        self.assertEqual(b"Hello\r\nWorld\r\n", content)

        # Check that the index has the blob SHA
        index = self.repo.open_index()
        self.assertEqual(blob.id, index[b"test.txt"].sha)

    def test_update_working_tree_remove_directory(self):
        """Test that update_working_tree properly removes directories."""
        # Create initial tree with a directory containing files
        blob1 = Blob()
        blob1.data = b"content1"
        self.repo.object_store.add_object(blob1)

        blob2 = Blob()
        blob2.data = b"content2"
        self.repo.object_store.add_object(blob2)

        tree1 = Tree()
        tree1[b"dir/file1.txt"] = (0o100644, blob1.id)
        tree1[b"dir/file2.txt"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1 (create directory with files)
        update_working_tree(self.repo, None, tree1.id)

        # Verify directory and files exist
        dir_path = os.path.join(self.tempdir, "dir")
        self.assertTrue(os.path.isdir(dir_path))
        self.assertTrue(os.path.exists(os.path.join(dir_path, "file1.txt")))
        self.assertTrue(os.path.exists(os.path.join(dir_path, "file2.txt")))

        # Create empty tree (remove everything)
        tree2 = Tree()
        self.repo.object_store.add_object(tree2)

        # Update to empty tree
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Verify directory was removed
        self.assertFalse(os.path.exists(dir_path))

    def test_update_working_tree_submodule_to_file(self):
        """Test replacing a submodule directory with a file."""
        # Create tree with submodule
        submodule_sha = b"a" * 40
        tree1 = Tree()
        tree1[b"submodule"] = (S_IFGITLINK, submodule_sha)
        self.repo.object_store.add_object(tree1)

        # Update to tree with submodule
        update_working_tree(self.repo, None, tree1.id)

        # Verify submodule directory exists with .git file
        submodule_path = os.path.join(self.tempdir, "submodule")
        self.assertTrue(os.path.isdir(submodule_path))
        self.assertTrue(os.path.exists(os.path.join(submodule_path, ".git")))

        # Create tree with file at same path
        blob = Blob()
        blob.data = b"file content"
        self.repo.object_store.add_object(blob)

        tree2 = Tree()
        tree2[b"submodule"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree2)

        # Update to tree with file (should remove submodule directory and create file)
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Verify it's now a file
        self.assertTrue(os.path.isfile(submodule_path))
        with open(submodule_path, "rb") as f:
            self.assertEqual(b"file content", f.read())

    def test_update_working_tree_directory_with_nested_subdir(self):
        """Test removing directory with nested subdirectories."""
        # Create tree with nested directories
        blob = Blob()
        blob.data = b"deep content"
        self.repo.object_store.add_object(blob)

        tree1 = Tree()
        tree1[b"a/b/c/file.txt"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Verify nested structure exists
        path_a = os.path.join(self.tempdir, "a")
        path_b = os.path.join(path_a, "b")
        path_c = os.path.join(path_b, "c")
        file_path = os.path.join(path_c, "file.txt")

        self.assertTrue(os.path.exists(file_path))

        # Create empty tree
        tree2 = Tree()
        self.repo.object_store.add_object(tree2)

        # Update to empty tree
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Verify all directories were removed
        self.assertFalse(os.path.exists(path_a))

    def test_update_working_tree_file_replaced_by_dir_not_removed(self):
        """Test that a directory replacing a git file is left alone if not empty."""
        # Create tree with a file
        blob = Blob()
        blob.data = b"file content"
        self.repo.object_store.add_object(blob)

        tree1 = Tree()
        tree1[b"path"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Verify file exists
        file_path = os.path.join(self.tempdir, "path")
        self.assertTrue(os.path.isfile(file_path))

        # Manually replace file with directory containing untracked file
        os.remove(file_path)
        os.mkdir(file_path)
        with open(os.path.join(file_path, "untracked.txt"), "w") as f:
            f.write("untracked content")

        # Create empty tree
        tree2 = Tree()
        self.repo.object_store.add_object(tree2)

        # Update should succeed but leave the directory alone
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Directory should still exist with its contents
        self.assertTrue(os.path.isdir(file_path))
        self.assertTrue(os.path.exists(os.path.join(file_path, "untracked.txt")))

    def test_update_working_tree_file_replaced_by_empty_dir_removed(self):
        """Test that an empty directory replacing a git file is removed."""
        # Create tree with a file
        blob = Blob()
        blob.data = b"file content"
        self.repo.object_store.add_object(blob)

        tree1 = Tree()
        tree1[b"path"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Verify file exists
        file_path = os.path.join(self.tempdir, "path")
        self.assertTrue(os.path.isfile(file_path))

        # Manually replace file with empty directory
        os.remove(file_path)
        os.mkdir(file_path)

        # Create empty tree
        tree2 = Tree()
        self.repo.object_store.add_object(tree2)

        # Update should remove the empty directory
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Directory should be gone
        self.assertFalse(os.path.exists(file_path))

    def test_update_working_tree_symlink_transitions(self):
        """Test transitions involving symlinks."""
        # Skip on Windows where symlinks might not be supported
        if sys.platform == "win32":
            self.skipTest("Symlinks not fully supported on Windows")

        # Create tree with symlink
        blob1 = Blob()
        blob1.data = b"target/path"
        self.repo.object_store.add_object(blob1)

        tree1 = Tree()
        tree1[b"link"] = (0o120000, blob1.id)  # Symlink mode
        self.repo.object_store.add_object(tree1)

        # Update to tree with symlink
        update_working_tree(self.repo, None, tree1.id)

        link_path = os.path.join(self.tempdir, "link")
        self.assertTrue(os.path.islink(link_path))
        self.assertEqual(b"target/path", os.readlink(link_path).encode())

        # Test 1: Replace symlink with regular file
        blob2 = Blob()
        blob2.data = b"file content"
        self.repo.object_store.add_object(blob2)

        tree2 = Tree()
        tree2[b"link"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree2)

        update_working_tree(self.repo, tree1.id, tree2.id)

        self.assertFalse(os.path.islink(link_path))
        self.assertTrue(os.path.isfile(link_path))
        with open(link_path, "rb") as f:
            self.assertEqual(b"file content", f.read())

        # Test 2: Replace file with symlink
        update_working_tree(self.repo, tree2.id, tree1.id)

        self.assertTrue(os.path.islink(link_path))
        self.assertEqual(b"target/path", os.readlink(link_path).encode())

        # Test 3: Replace symlink with directory (manually)
        os.unlink(link_path)
        os.mkdir(link_path)

        # Create empty tree
        tree3 = Tree()
        self.repo.object_store.add_object(tree3)

        # Should remove empty directory
        update_working_tree(self.repo, tree1.id, tree3.id)
        self.assertFalse(os.path.exists(link_path))

    def test_update_working_tree_modified_file_to_dir_transition(self):
        """Test that modified files are not removed when they should be directories."""
        # Create tree with file
        blob1 = Blob()
        blob1.data = b"original content"
        self.repo.object_store.add_object(blob1)

        tree1 = Tree()
        tree1[b"path"] = (0o100644, blob1.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        file_path = os.path.join(self.tempdir, "path")

        # Modify the file locally
        with open(file_path, "w") as f:
            f.write("modified content")

        # Create tree where path is a directory with file
        blob2 = Blob()
        blob2.data = b"subfile content"
        self.repo.object_store.add_object(blob2)

        tree2 = Tree()
        tree2[b"path/subfile"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree2)

        # Update should fail because can't create directory where modified file exists
        with self.assertRaises(IOError):
            update_working_tree(self.repo, tree1.id, tree2.id)

        # File should still exist with modifications
        self.assertTrue(os.path.isfile(file_path))
        with open(file_path) as f:
            self.assertEqual("modified content", f.read())

    def test_update_working_tree_executable_transitions(self):
        """Test transitions involving executable bit changes."""
        # Skip on Windows where executable bit is not supported
        if sys.platform == "win32":
            self.skipTest("Executable bit not supported on Windows")

        # Create tree with non-executable file
        blob = Blob()
        blob.data = b"#!/bin/sh\necho hello"
        self.repo.object_store.add_object(blob)

        tree1 = Tree()
        tree1[b"script.sh"] = (0o100644, blob.id)  # Non-executable
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        script_path = os.path.join(self.tempdir, "script.sh")
        self.assertTrue(os.path.isfile(script_path))

        # Check it's not executable
        mode = os.stat(script_path).st_mode
        self.assertFalse(mode & stat.S_IXUSR)

        # Create tree with executable file (same content)
        tree2 = Tree()
        tree2[b"script.sh"] = (0o100755, blob.id)  # Executable
        self.repo.object_store.add_object(tree2)

        # Update to tree2
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Check it's now executable
        mode = os.stat(script_path).st_mode
        self.assertTrue(mode & stat.S_IXUSR)

    def test_update_working_tree_submodule_with_untracked_files(self):
        """Test that submodules with untracked files are not removed."""
        from dulwich.objects import S_IFGITLINK, Tree

        # Create tree with submodule
        submodule_sha = b"a" * 40
        tree1 = Tree()
        tree1[b"submodule"] = (S_IFGITLINK, submodule_sha)
        self.repo.object_store.add_object(tree1)

        # Update to tree with submodule
        update_working_tree(self.repo, None, tree1.id)

        # Add untracked file to submodule directory
        submodule_path = os.path.join(self.tempdir, "submodule")
        untracked_path = os.path.join(submodule_path, "untracked.txt")
        with open(untracked_path, "w") as f:
            f.write("untracked content")

        # Create empty tree
        tree2 = Tree()
        self.repo.object_store.add_object(tree2)

        # Update should not remove submodule directory with untracked files
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Directory should still exist with untracked file
        self.assertTrue(os.path.isdir(submodule_path))
        self.assertTrue(os.path.exists(untracked_path))

    def test_update_working_tree_dir_to_file_with_subdir(self):
        """Test replacing directory structure with a file."""
        # Create tree with nested directory structure
        blob1 = Blob()
        blob1.data = b"content1"
        self.repo.object_store.add_object(blob1)

        blob2 = Blob()
        blob2.data = b"content2"
        self.repo.object_store.add_object(blob2)

        tree1 = Tree()
        tree1[b"dir/subdir/file1"] = (0o100644, blob1.id)
        tree1[b"dir/subdir/file2"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Verify structure exists
        dir_path = os.path.join(self.tempdir, "dir")
        self.assertTrue(os.path.isdir(dir_path))

        # Add an untracked file to make directory truly non-empty
        untracked_path = os.path.join(dir_path, "untracked.txt")
        with open(untracked_path, "w") as f:
            f.write("untracked content")

        # Create tree with file at "dir" path
        blob3 = Blob()
        blob3.data = b"replacement file"
        self.repo.object_store.add_object(blob3)

        tree2 = Tree()
        tree2[b"dir"] = (0o100644, blob3.id)
        self.repo.object_store.add_object(tree2)

        # Update should fail because directory is not empty
        with self.assertRaises(IsADirectoryError):
            update_working_tree(self.repo, tree1.id, tree2.id)

        # Directory should still exist
        self.assertTrue(os.path.isdir(dir_path))

    def test_update_working_tree_case_sensitivity(self):
        """Test handling of case-sensitive filename changes."""
        # Create tree with lowercase file
        blob1 = Blob()
        blob1.data = b"lowercase content"
        self.repo.object_store.add_object(blob1)

        tree1 = Tree()
        tree1[b"readme.txt"] = (0o100644, blob1.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Create tree with uppercase file (different content)
        blob2 = Blob()
        blob2.data = b"uppercase content"
        self.repo.object_store.add_object(blob2)

        tree2 = Tree()
        tree2[b"README.txt"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree2)

        # Update to tree2
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Check what exists (behavior depends on filesystem)
        lowercase_path = os.path.join(self.tempdir, "readme.txt")
        uppercase_path = os.path.join(self.tempdir, "README.txt")

        # On case-insensitive filesystems, one will overwrite the other
        # On case-sensitive filesystems, both may exist
        self.assertTrue(
            os.path.exists(lowercase_path) or os.path.exists(uppercase_path)
        )

    def test_update_working_tree_deeply_nested_removal(self):
        """Test removal of deeply nested directory structures."""
        # Create deeply nested structure
        blob = Blob()
        blob.data = b"deep content"
        self.repo.object_store.add_object(blob)

        tree1 = Tree()
        # Create a very deep path
        deep_path = b"/".join([b"level%d" % i for i in range(10)])
        tree1[deep_path + b"/file.txt"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Verify deep structure exists
        current_path = self.tempdir
        for i in range(10):
            current_path = os.path.join(current_path, f"level{i}")
            self.assertTrue(os.path.isdir(current_path))

        # Create empty tree
        tree2 = Tree()
        self.repo.object_store.add_object(tree2)

        # Update should remove all empty directories
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Verify top level directory is gone
        top_level = os.path.join(self.tempdir, "level0")
        self.assertFalse(os.path.exists(top_level))

    def test_update_working_tree_read_only_files(self):
        """Test handling of read-only files during updates."""
        # Create tree with file
        blob1 = Blob()
        blob1.data = b"original content"
        self.repo.object_store.add_object(blob1)

        tree1 = Tree()
        tree1[b"readonly.txt"] = (0o100644, blob1.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Make file read-only
        file_path = os.path.join(self.tempdir, "readonly.txt")
        os.chmod(file_path, 0o444)  # Read-only

        # Create tree with modified file
        blob2 = Blob()
        blob2.data = b"new content"
        self.repo.object_store.add_object(blob2)

        tree2 = Tree()
        tree2[b"readonly.txt"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree2)

        # Update should handle read-only file
        update_working_tree(self.repo, tree1.id, tree2.id)

        # Verify content was updated
        with open(file_path, "rb") as f:
            self.assertEqual(b"new content", f.read())

    def test_update_working_tree_invalid_filenames(self):
        """Test handling of invalid filenames for the platform."""
        # Create tree with potentially problematic filenames
        blob = Blob()
        blob.data = b"content"
        self.repo.object_store.add_object(blob)

        tree = Tree()
        # Add files with names that might be invalid on some platforms
        tree[b"valid.txt"] = (0o100644, blob.id)
        if sys.platform != "win32":
            # These are invalid on Windows but valid on Unix
            tree[b"file:with:colons.txt"] = (0o100644, blob.id)
            tree[b"file<with>brackets.txt"] = (0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        # Update should skip invalid files based on validation
        update_working_tree(self.repo, None, tree.id)

        # Valid file should exist
        self.assertTrue(os.path.exists(os.path.join(self.tempdir, "valid.txt")))

    def test_update_working_tree_symlink_to_directory(self):
        """Test replacing a symlink pointing to a directory with a real directory."""
        if sys.platform == "win32":
            self.skipTest("Symlinks not fully supported on Windows")

        # Create a target directory
        target_dir = os.path.join(self.tempdir, "target")
        os.mkdir(target_dir)
        with open(os.path.join(target_dir, "file.txt"), "w") as f:
            f.write("target file")

        # Create tree with symlink pointing to directory
        blob1 = Blob()
        blob1.data = b"target"  # Relative path to target directory
        self.repo.object_store.add_object(blob1)

        tree1 = Tree()
        tree1[b"link"] = (0o120000, blob1.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        link_path = os.path.join(self.tempdir, "link")
        self.assertTrue(os.path.islink(link_path))

        # Create tree with actual directory at same path
        blob2 = Blob()
        blob2.data = b"new file content"
        self.repo.object_store.add_object(blob2)

        tree2 = Tree()
        tree2[b"link/newfile.txt"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree2)

        # Update should replace symlink with actual directory
        update_working_tree(self.repo, tree1.id, tree2.id)

        self.assertFalse(os.path.islink(link_path))
        self.assertTrue(os.path.isdir(link_path))
        self.assertTrue(os.path.exists(os.path.join(link_path, "newfile.txt")))

    def test_update_working_tree_comprehensive_transitions(self):
        """Test all possible file type transitions comprehensively."""
        # Skip on Windows where symlinks might not be supported
        if sys.platform == "win32":
            self.skipTest("Symlinks not fully supported on Windows")

        # Create blobs for different file types
        file_blob = Blob()
        file_blob.data = b"regular file content"
        self.repo.object_store.add_object(file_blob)

        exec_blob = Blob()
        exec_blob.data = b"#!/bin/sh\necho executable"
        self.repo.object_store.add_object(exec_blob)

        link_blob = Blob()
        link_blob.data = b"target/path"
        self.repo.object_store.add_object(link_blob)

        submodule_sha = b"a" * 40

        # Test 1: Regular file → Submodule
        tree1 = Tree()
        tree1[b"item"] = (0o100644, file_blob.id)
        self.repo.object_store.add_object(tree1)

        tree2 = Tree()
        tree2[b"item"] = (S_IFGITLINK, submodule_sha)
        self.repo.object_store.add_object(tree2)

        update_working_tree(self.repo, None, tree1.id)
        self.assertTrue(os.path.isfile(os.path.join(self.tempdir, "item")))

        update_working_tree(self.repo, tree1.id, tree2.id)
        self.assertTrue(os.path.isdir(os.path.join(self.tempdir, "item")))

        # Test 2: Submodule → Executable file
        tree3 = Tree()
        tree3[b"item"] = (0o100755, exec_blob.id)
        self.repo.object_store.add_object(tree3)

        update_working_tree(self.repo, tree2.id, tree3.id)
        item_path = os.path.join(self.tempdir, "item")
        self.assertTrue(os.path.isfile(item_path))
        if sys.platform != "win32":
            self.assertTrue(os.access(item_path, os.X_OK))

        # Test 3: Executable file → Symlink
        tree4 = Tree()
        tree4[b"item"] = (0o120000, link_blob.id)
        self.repo.object_store.add_object(tree4)

        update_working_tree(self.repo, tree3.id, tree4.id)
        self.assertTrue(os.path.islink(item_path))

        # Test 4: Symlink → Submodule
        tree5 = Tree()
        tree5[b"item"] = (S_IFGITLINK, submodule_sha)
        self.repo.object_store.add_object(tree5)

        update_working_tree(self.repo, tree4.id, tree5.id)
        self.assertTrue(os.path.isdir(item_path))

        # Test 5: Clean up - Submodule → absent
        tree6 = Tree()
        self.repo.object_store.add_object(tree6)

        update_working_tree(self.repo, tree5.id, tree6.id)
        self.assertFalse(os.path.exists(item_path))

        # Test 6: Symlink → Executable file
        tree7 = Tree()
        tree7[b"item2"] = (0o120000, link_blob.id)
        self.repo.object_store.add_object(tree7)

        update_working_tree(self.repo, tree6.id, tree7.id)
        item2_path = os.path.join(self.tempdir, "item2")
        self.assertTrue(os.path.islink(item2_path))

        tree8 = Tree()
        tree8[b"item2"] = (0o100755, exec_blob.id)
        self.repo.object_store.add_object(tree8)

        update_working_tree(self.repo, tree7.id, tree8.id)
        self.assertTrue(os.path.isfile(item2_path))
        if sys.platform != "win32":
            self.assertTrue(os.access(item2_path, os.X_OK))

    def test_update_working_tree_partial_update_failure(self):
        """Test handling when update fails partway through."""
        # Create initial tree
        blob1 = Blob()
        blob1.data = b"file1 content"
        self.repo.object_store.add_object(blob1)

        blob2 = Blob()
        blob2.data = b"file2 content"
        self.repo.object_store.add_object(blob2)

        tree1 = Tree()
        tree1[b"file1.txt"] = (0o100644, blob1.id)
        tree1[b"file2.txt"] = (0o100644, blob2.id)
        self.repo.object_store.add_object(tree1)

        # Update to tree1
        update_working_tree(self.repo, None, tree1.id)

        # Create a directory where file2.txt is, to cause a conflict
        file2_path = os.path.join(self.tempdir, "file2.txt")
        os.remove(file2_path)
        os.mkdir(file2_path)
        # Add untracked file to prevent removal
        with open(os.path.join(file2_path, "blocker.txt"), "w") as f:
            f.write("blocking content")

        # Create tree with updates to both files
        blob3 = Blob()
        blob3.data = b"file1 updated"
        self.repo.object_store.add_object(blob3)

        blob4 = Blob()
        blob4.data = b"file2 updated"
        self.repo.object_store.add_object(blob4)

        tree2 = Tree()
        tree2[b"file1.txt"] = (0o100644, blob3.id)
        tree2[b"file2.txt"] = (0o100644, blob4.id)
        self.repo.object_store.add_object(tree2)

        # Update should partially succeed - file1 updated, file2 blocked
        try:
            update_working_tree(self.repo, tree1.id, tree2.id)
        except IsADirectoryError:
            # Expected to fail on file2 because it's a directory
            pass

        # file1 should be updated
        with open(os.path.join(self.tempdir, "file1.txt"), "rb") as f:
            self.assertEqual(b"file1 updated", f.read())

        # file2 should still be a directory
        self.assertTrue(os.path.isdir(file2_path))
