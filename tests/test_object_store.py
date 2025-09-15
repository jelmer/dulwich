# test_object_store.py -- tests for object_store.py
# Copyright (C) 2008 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
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

"""Tests for the object store interface."""

import os
import shutil
import stat
import sys
import tempfile
from contextlib import closing
from io import BytesIO

from dulwich.errors import NotTreeError
from dulwich.index import commit_tree
from dulwich.object_store import (
    DiskObjectStore,
    MemoryObjectStore,
    ObjectStoreGraphWalker,
    OverlayObjectStore,
    commit_tree_changes,
    read_packs_file,
    tree_lookup_path,
)
from dulwich.objects import (
    S_IFGITLINK,
    Blob,
    EmptyFileException,
    SubmoduleEncountered,
    Tree,
    TreeEntry,
    sha_to_hex,
)
from dulwich.pack import REF_DELTA, write_pack_objects
from dulwich.tests.test_object_store import ObjectStoreTests, PackBasedObjectStoreTests
from dulwich.tests.utils import build_pack, make_object

from . import TestCase

testobject = make_object(Blob, data=b"yummy data")


class OverlayObjectStoreTests(ObjectStoreTests, TestCase):
    def setUp(self) -> None:
        TestCase.setUp(self)
        self.bases = [MemoryObjectStore(), MemoryObjectStore()]
        self.store = OverlayObjectStore(self.bases, self.bases[0])


class MemoryObjectStoreTests(ObjectStoreTests, TestCase):
    def setUp(self) -> None:
        TestCase.setUp(self)
        self.store = MemoryObjectStore()

    def test_add_pack(self) -> None:
        o = MemoryObjectStore()
        f, commit, abort = o.add_pack()
        try:
            b = make_object(Blob, data=b"more yummy data")
            write_pack_objects(f.write, [(b, None)])
        except BaseException:
            abort()
            raise
        else:
            commit()

    def test_add_pack_emtpy(self) -> None:
        o = MemoryObjectStore()
        _f, commit, _abort = o.add_pack()
        commit()

    def test_add_thin_pack(self) -> None:
        o = MemoryObjectStore()
        blob = make_object(Blob, data=b"yummy data")
        o.add_object(blob)

        f = BytesIO()
        entries = build_pack(
            f,
            [
                (REF_DELTA, (blob.id, b"more yummy data")),
            ],
            store=o,
        )
        o.add_thin_pack(f.read, None)
        packed_blob_sha = sha_to_hex(entries[0][3])
        self.assertEqual(
            (Blob.type_num, b"more yummy data"), o.get_raw(packed_blob_sha)
        )

    def test_add_thin_pack_empty(self) -> None:
        o = MemoryObjectStore()

        f = BytesIO()
        entries = build_pack(f, [], store=o)
        self.assertEqual([], entries)
        o.add_thin_pack(f.read, None)

    def test_add_pack_data_with_deltas(self) -> None:
        """Test that add_pack_data properly handles delta objects.

        This test verifies that MemoryObjectStore.add_pack_data can handle
        pack data containing delta objects. Before the fix for issue #1179,
        this would fail with AssertionError when trying to call sha_file()
        on unresolved delta objects.

        The fix routes through add_pack() which properly resolves deltas.
        """
        o1 = MemoryObjectStore()
        o2 = MemoryObjectStore()
        base_blob = make_object(Blob, data=b"base data")
        o1.add_object(base_blob)

        # Create a pack with a delta object
        f = BytesIO()
        entries = build_pack(
            f,
            [
                (REF_DELTA, (base_blob.id, b"more data")),
            ],
            store=o1,
        )

        # Use add_thin_pack which internally calls add_pack_data
        # This demonstrates the scenario where delta resolution is needed
        f.seek(0)
        o2.add_object(base_blob)  # Need base object for thin pack
        o2.add_thin_pack(f.read, None)

        # Verify the delta object was properly resolved and added
        packed_blob_sha = sha_to_hex(entries[0][3])
        self.assertIn(packed_blob_sha, o2)
        self.assertEqual((Blob.type_num, b"more data"), o2.get_raw(packed_blob_sha))


class DiskObjectStoreTests(PackBasedObjectStoreTests, TestCase):
    def setUp(self) -> None:
        TestCase.setUp(self)
        self.store_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.store_dir)
        self.store = DiskObjectStore.init(self.store_dir)

    def tearDown(self) -> None:
        TestCase.tearDown(self)
        PackBasedObjectStoreTests.tearDown(self)

    def test_loose_compression_level(self) -> None:
        alternate_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, alternate_dir)
        alternate_store = DiskObjectStore(alternate_dir, loose_compression_level=6)
        b2 = make_object(Blob, data=b"yummy data")
        alternate_store.add_object(b2)

    def test_alternates(self) -> None:
        alternate_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, alternate_dir)
        alternate_store = DiskObjectStore(alternate_dir)
        b2 = make_object(Blob, data=b"yummy data")
        alternate_store.add_object(b2)
        store = DiskObjectStore(self.store_dir)
        self.assertRaises(KeyError, store.__getitem__, b2.id)
        store.add_alternate_path(alternate_dir)
        self.assertIn(b2.id, store)
        self.assertEqual(b2, store[b2.id])

    def test_read_alternate_paths(self) -> None:
        store = DiskObjectStore(self.store_dir)

        abs_path = os.path.abspath(os.path.normpath("/abspath"))
        # ensures in particular existence of the alternates file
        store.add_alternate_path(abs_path)
        self.assertEqual(set(store._read_alternate_paths()), {abs_path})

        store.add_alternate_path("relative-path")
        self.assertIn(
            os.path.join(store.path, "relative-path"),
            set(store._read_alternate_paths()),
        )

        # arguably, add_alternate_path() could strip comments.
        # Meanwhile it's more convenient to use it than to import INFODIR
        store.add_alternate_path("# comment")
        for alt_path in store._read_alternate_paths():
            self.assertNotIn("#", alt_path)

    def test_file_modes(self) -> None:
        self.store.add_object(testobject)
        path = self.store._get_shafile_path(testobject.id)
        mode = os.stat(path).st_mode

        packmode = "0o100444" if sys.platform != "win32" else "0o100666"
        self.assertEqual(oct(mode), packmode)

    def test_corrupted_object_raise_exception(self) -> None:
        """Corrupted sha1 disk file should raise specific exception."""
        self.store.add_object(testobject)
        self.assertEqual(
            (Blob.type_num, b"yummy data"), self.store.get_raw(testobject.id)
        )
        self.assertTrue(self.store.contains_loose(testobject.id))
        self.assertIsNotNone(self.store._get_loose_object(testobject.id))

        path = self.store._get_shafile_path(testobject.id)
        old_mode = os.stat(path).st_mode
        os.chmod(path, 0o600)
        with open(path, "wb") as f:  # corrupt the file
            f.write(b"")
        os.chmod(path, old_mode)

        expected_error_msg = "Corrupted empty file detected"
        try:
            self.store.contains_loose(testobject.id)
        except EmptyFileException as e:
            self.assertEqual(str(e), expected_error_msg)

        try:
            self.store._get_loose_object(testobject.id)
        except EmptyFileException as e:
            self.assertEqual(str(e), expected_error_msg)

        # this does not change iteration on loose objects though
        self.assertEqual([testobject.id], list(self.store._iter_loose_objects()))

    def test_tempfile_in_loose_store(self) -> None:
        self.store.add_object(testobject)
        self.assertEqual([testobject.id], list(self.store._iter_loose_objects()))

        # add temporary files to the loose store
        for i in range(256):
            dirname = os.path.join(self.store_dir, f"{i:02x}")
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            fd, _n = tempfile.mkstemp(prefix="tmp_obj_", dir=dirname)
            os.close(fd)

        self.assertEqual([testobject.id], list(self.store._iter_loose_objects()))

    def test_add_alternate_path(self) -> None:
        store = DiskObjectStore(self.store_dir)
        self.assertEqual([], list(store._read_alternate_paths()))
        store.add_alternate_path(os.path.abspath("/foo/path"))
        self.assertEqual(
            [os.path.abspath("/foo/path")], list(store._read_alternate_paths())
        )
        if sys.platform == "win32":
            store.add_alternate_path("D:\\bar\\path")
        else:
            store.add_alternate_path("/bar/path")

        if sys.platform == "win32":
            self.assertEqual(
                [os.path.abspath("/foo/path"), "D:\\bar\\path"],
                list(store._read_alternate_paths()),
            )
        else:
            self.assertEqual(
                [os.path.abspath("/foo/path"), "/bar/path"],
                list(store._read_alternate_paths()),
            )

    def test_rel_alternative_path(self) -> None:
        alternate_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, alternate_dir)
        alternate_store = DiskObjectStore(alternate_dir)
        b2 = make_object(Blob, data=b"yummy data")
        alternate_store.add_object(b2)
        store = DiskObjectStore(self.store_dir)
        self.assertRaises(KeyError, store.__getitem__, b2.id)
        store.add_alternate_path(os.path.relpath(alternate_dir, self.store_dir))
        self.assertEqual(list(alternate_store), list(store.alternates[0]))
        self.assertIn(b2.id, store)
        self.assertEqual(b2, store[b2.id])

    def test_pack_dir(self) -> None:
        o = DiskObjectStore(self.store_dir)
        self.assertEqual(os.path.join(self.store_dir, "pack"), o.pack_dir)

    def test_add_pack(self) -> None:
        o = DiskObjectStore(self.store_dir)
        self.addCleanup(o.close)
        f, commit, abort = o.add_pack()
        try:
            b = make_object(Blob, data=b"more yummy data")
            write_pack_objects(f.write, [(b, None)])
        except BaseException:
            abort()
            raise
        else:
            commit()

    def test_add_thin_pack(self) -> None:
        o = DiskObjectStore(self.store_dir)
        self.addCleanup(o.close)

        blob = make_object(Blob, data=b"yummy data")
        o.add_object(blob)

        f = BytesIO()
        entries = build_pack(
            f,
            [
                (REF_DELTA, (blob.id, b"more yummy data")),
            ],
            store=o,
        )

        with o.add_thin_pack(f.read, None) as pack:
            packed_blob_sha = sha_to_hex(entries[0][3])
            pack.check_length_and_checksum()
            self.assertEqual(sorted([blob.id, packed_blob_sha]), list(pack))
            self.assertTrue(o.contains_packed(packed_blob_sha))
            self.assertTrue(o.contains_packed(blob.id))
            self.assertEqual(
                (Blob.type_num, b"more yummy data"),
                o.get_raw(packed_blob_sha),
            )

    def test_add_thin_pack_empty(self) -> None:
        with closing(DiskObjectStore(self.store_dir)) as o:
            f = BytesIO()
            entries = build_pack(f, [], store=o)
            self.assertEqual([], entries)
            o.add_thin_pack(f.read, None)

    def test_pack_index_version_config(self) -> None:
        # Test that pack.indexVersion configuration is respected
        from dulwich.config import ConfigDict
        from dulwich.pack import load_pack_index

        # Create config with pack.indexVersion = 1
        config = ConfigDict()
        config[(b"pack",)] = {b"indexVersion": b"1"}

        # Create object store with config
        store_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, store_dir)
        os.makedirs(os.path.join(store_dir, "pack"))
        store = DiskObjectStore.from_config(store_dir, config)
        self.addCleanup(store.close)

        # Create some objects to pack
        b1 = make_object(Blob, data=b"blob1")
        b2 = make_object(Blob, data=b"blob2")
        store.add_objects([(b1, None), (b2, None)])

        # Add a pack
        f, commit, abort = store.add_pack()
        try:
            # build_pack expects (type_num, data) tuples
            objects_spec = [
                (b1.type_num, b1.as_raw_string()),
                (b2.type_num, b2.as_raw_string()),
            ]
            build_pack(f, objects_spec, store=store)
            commit()
        except:
            abort()
            raise

        # Find the created pack index
        pack_dir = os.path.join(store_dir, "pack")
        idx_files = [f for f in os.listdir(pack_dir) if f.endswith(".idx")]
        self.assertEqual(1, len(idx_files))

        # Load and verify it's version 1
        idx_path = os.path.join(pack_dir, idx_files[0])
        idx = load_pack_index(idx_path)
        self.assertEqual(1, idx.version)

        # Test version 3
        config[(b"pack",)] = {b"indexVersion": b"3"}
        store_dir2 = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, store_dir2)
        os.makedirs(os.path.join(store_dir2, "pack"))
        store2 = DiskObjectStore.from_config(store_dir2, config)
        self.addCleanup(store2.close)

        b3 = make_object(Blob, data=b"blob3")
        store2.add_objects([(b3, None)])

        f2, commit2, abort2 = store2.add_pack()
        try:
            objects_spec2 = [(b3.type_num, b3.as_raw_string())]
            build_pack(f2, objects_spec2, store=store2)
            commit2()
        except:
            abort2()
            raise

        # Find and verify version 3 index
        pack_dir2 = os.path.join(store_dir2, "pack")
        idx_files2 = [f for f in os.listdir(pack_dir2) if f.endswith(".idx")]
        self.assertEqual(1, len(idx_files2))

        idx_path2 = os.path.join(pack_dir2, idx_files2[0])
        idx2 = load_pack_index(idx_path2)
        self.assertEqual(3, idx2.version)

    def test_prune_orphaned_tempfiles(self) -> None:
        import time

        # Create an orphaned temporary pack file in the repository directory
        tmp_pack_path = os.path.join(self.store_dir, "tmp_pack_test123")
        with open(tmp_pack_path, "wb") as f:
            f.write(b"temporary pack data")

        # Create an orphaned .pack file without .idx in pack directory
        pack_dir = os.path.join(self.store_dir, "pack")
        orphaned_pack_path = os.path.join(pack_dir, "pack-orphaned.pack")
        with open(orphaned_pack_path, "wb") as f:
            f.write(b"orphaned pack data")

        # Make files appear old by modifying mtime (older than grace period)
        from dulwich.object_store import DEFAULT_TEMPFILE_GRACE_PERIOD

        old_time = time.time() - (
            DEFAULT_TEMPFILE_GRACE_PERIOD + 3600
        )  # grace period + 1 hour
        os.utime(tmp_pack_path, (old_time, old_time))
        os.utime(orphaned_pack_path, (old_time, old_time))

        # Create a recent temporary file that should NOT be cleaned
        recent_tmp_path = os.path.join(self.store_dir, "tmp_pack_recent")
        with open(recent_tmp_path, "wb") as f:
            f.write(b"recent temp data")

        # Run prune
        self.store.prune()

        # Check that old orphaned files were removed
        self.assertFalse(os.path.exists(tmp_pack_path))
        self.assertFalse(os.path.exists(orphaned_pack_path))

        # Check that recent file was NOT removed
        self.assertTrue(os.path.exists(recent_tmp_path))

        # Cleanup the recent file
        os.remove(recent_tmp_path)

    def test_prune_with_custom_grace_period(self) -> None:
        """Test that prune respects custom grace period."""
        import time

        # Create a temporary file that's 1 hour old
        tmp_pack_path = os.path.join(self.store_dir, "tmp_pack_1hour")
        with open(tmp_pack_path, "wb") as f:
            f.write(b"1 hour old data")

        # Make it 1 hour old
        old_time = time.time() - 3600  # 1 hour ago
        os.utime(tmp_pack_path, (old_time, old_time))

        # Prune with default grace period (2 weeks) - should NOT remove
        self.store.prune()
        self.assertTrue(os.path.exists(tmp_pack_path))

        # Prune with 30 minute grace period - should remove
        self.store.prune(grace_period=1800)  # 30 minutes
        self.assertFalse(os.path.exists(tmp_pack_path))

    def test_gc_prunes_tempfiles(self) -> None:
        """Test that garbage collection prunes temporary files."""
        import time

        from dulwich.gc import garbage_collect
        from dulwich.repo import Repo

        # Create a repository with the store
        repo = Repo.init(self.store_dir)

        # Create an old orphaned temporary file in the objects directory
        tmp_pack_path = os.path.join(repo.object_store.path, "tmp_pack_old")
        with open(tmp_pack_path, "wb") as f:
            f.write(b"old temporary data")

        # Make it old (older than grace period)
        from dulwich.object_store import DEFAULT_TEMPFILE_GRACE_PERIOD

        old_time = time.time() - (
            DEFAULT_TEMPFILE_GRACE_PERIOD + 3600
        )  # grace period + 1 hour
        os.utime(tmp_pack_path, (old_time, old_time))

        # Run garbage collection
        garbage_collect(repo)

        # Verify the orphaned file was cleaned up
        self.assertFalse(os.path.exists(tmp_pack_path))

    def test_commit_graph_enabled_by_default(self) -> None:
        """Test that commit graph is enabled by default."""
        from dulwich.config import ConfigDict

        config = ConfigDict()
        store = DiskObjectStore.from_config(self.store_dir, config)
        self.addCleanup(store.close)

        # Should be enabled by default
        self.assertTrue(store._use_commit_graph)

    def test_commit_graph_disabled_by_config(self) -> None:
        """Test that commit graph can be disabled via config."""
        from dulwich.config import ConfigDict

        config = ConfigDict()
        config[(b"core",)] = {b"commitGraph": b"false"}
        store = DiskObjectStore.from_config(self.store_dir, config)
        self.addCleanup(store.close)

        # Should be disabled
        self.assertFalse(store._use_commit_graph)

        # get_commit_graph should return None when disabled
        self.assertIsNone(store.get_commit_graph())

    def test_commit_graph_enabled_by_config(self) -> None:
        """Test that commit graph can be explicitly enabled via config."""
        from dulwich.config import ConfigDict

        config = ConfigDict()
        config[(b"core",)] = {b"commitGraph": b"true"}
        store = DiskObjectStore.from_config(self.store_dir, config)
        self.addCleanup(store.close)

        # Should be enabled
        self.assertTrue(store._use_commit_graph)

    def test_commit_graph_usage_in_find_shallow(self) -> None:
        """Test that find_shallow uses commit graph when available."""
        import time

        from dulwich.object_store import find_shallow
        from dulwich.objects import Commit

        # Create a simple commit chain: c1 -> c2 -> c3
        ts = int(time.time())
        c1 = make_object(
            Commit,
            message=b"commit 1",
            tree=b"1" * 40,
            parents=[],
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
            commit_time=ts,
            commit_timezone=0,
            author_time=ts,
            author_timezone=0,
        )

        c2 = make_object(
            Commit,
            message=b"commit 2",
            tree=b"2" * 40,
            parents=[c1.id],
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
            commit_time=ts + 1,
            commit_timezone=0,
            author_time=ts + 1,
            author_timezone=0,
        )

        c3 = make_object(
            Commit,
            message=b"commit 3",
            tree=b"3" * 40,
            parents=[c2.id],
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
            commit_time=ts + 2,
            commit_timezone=0,
            author_time=ts + 2,
            author_timezone=0,
        )

        self.store.add_objects([(c1, None), (c2, None), (c3, None)])

        # Write a commit graph
        self.store.write_commit_graph([c1.id, c2.id, c3.id])

        # Verify commit graph was written
        commit_graph = self.store.get_commit_graph()
        self.assertIsNotNone(commit_graph)
        self.assertEqual(3, len(commit_graph))

        # Test find_shallow with depth
        # With depth 2 starting from c3:
        # - depth 1 includes c3 itself (not shallow)
        # - depth 2 includes c3 and c2 (not shallow)
        # - c1 is at depth 3, so it's marked as shallow
        shallow, not_shallow = find_shallow(self.store, [c3.id], 2)

        # c2 should be marked as shallow since it's at the depth boundary
        self.assertEqual({c2.id}, shallow)
        self.assertEqual({c3.id}, not_shallow)

    def test_commit_graph_end_to_end(self) -> None:
        """Test end-to-end commit graph generation and usage."""
        import os
        import time

        from dulwich.object_store import get_depth
        from dulwich.objects import Blob, Commit, Tree

        # Create a more complex commit history:
        #   c1 -- c2 -- c4
        #     \        /
        #      \-- c3 -/

        ts = int(time.time())

        # Create some blobs and trees for the commits
        blob1 = make_object(Blob, data=b"content 1")
        blob2 = make_object(Blob, data=b"content 2")
        blob3 = make_object(Blob, data=b"content 3")
        blob4 = make_object(Blob, data=b"content 4")

        tree1 = make_object(Tree)
        tree1[b"file1.txt"] = (0o100644, blob1.id)

        tree2 = make_object(Tree)
        tree2[b"file1.txt"] = (0o100644, blob1.id)
        tree2[b"file2.txt"] = (0o100644, blob2.id)

        tree3 = make_object(Tree)
        tree3[b"file1.txt"] = (0o100644, blob1.id)
        tree3[b"file3.txt"] = (0o100644, blob3.id)

        tree4 = make_object(Tree)
        tree4[b"file1.txt"] = (0o100644, blob1.id)
        tree4[b"file2.txt"] = (0o100644, blob2.id)
        tree4[b"file3.txt"] = (0o100644, blob3.id)
        tree4[b"file4.txt"] = (0o100644, blob4.id)

        # Add all objects to store
        self.store.add_objects(
            [
                (blob1, None),
                (blob2, None),
                (blob3, None),
                (blob4, None),
                (tree1, None),
                (tree2, None),
                (tree3, None),
                (tree4, None),
            ]
        )

        # Create commits
        c1 = make_object(
            Commit,
            message=b"Initial commit",
            tree=tree1.id,
            parents=[],
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
            commit_time=ts,
            commit_timezone=0,
            author_time=ts,
            author_timezone=0,
        )

        c2 = make_object(
            Commit,
            message=b"Second commit",
            tree=tree2.id,
            parents=[c1.id],
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
            commit_time=ts + 10,
            commit_timezone=0,
            author_time=ts + 10,
            author_timezone=0,
        )

        c3 = make_object(
            Commit,
            message=b"Branch commit",
            tree=tree3.id,
            parents=[c1.id],
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
            commit_time=ts + 20,
            commit_timezone=0,
            author_time=ts + 20,
            author_timezone=0,
        )

        c4 = make_object(
            Commit,
            message=b"Merge commit",
            tree=tree4.id,
            parents=[c2.id, c3.id],
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <test@example.com>",
            commit_time=ts + 30,
            commit_timezone=0,
            author_time=ts + 30,
            author_timezone=0,
        )

        self.store.add_objects([(c1, None), (c2, None), (c3, None), (c4, None)])

        # First, verify operations work without commit graph
        # Check depth calculation
        depth_before = get_depth(self.store, c4.id)
        self.assertEqual(3, depth_before)  # c4 -> c2/c3 -> c1

        # Generate commit graph
        self.store.write_commit_graph([c1.id, c2.id, c3.id, c4.id])

        # Verify commit graph file was created
        graph_path = os.path.join(self.store.path, "info", "commit-graph")
        self.assertTrue(os.path.exists(graph_path))

        # Load and verify commit graph
        commit_graph = self.store.get_commit_graph()
        self.assertIsNotNone(commit_graph)
        self.assertEqual(4, len(commit_graph))

        # Verify commit graph contains correct parent information
        c1_entry = commit_graph.get_entry_by_oid(c1.id)
        self.assertIsNotNone(c1_entry)
        self.assertEqual([], c1_entry.parents)

        c2_entry = commit_graph.get_entry_by_oid(c2.id)
        self.assertIsNotNone(c2_entry)
        self.assertEqual([c1.id], c2_entry.parents)

        c3_entry = commit_graph.get_entry_by_oid(c3.id)
        self.assertIsNotNone(c3_entry)
        self.assertEqual([c1.id], c3_entry.parents)

        c4_entry = commit_graph.get_entry_by_oid(c4.id)
        self.assertIsNotNone(c4_entry)
        self.assertEqual([c2.id, c3.id], c4_entry.parents)

        # Test that operations now use the commit graph
        # Check depth calculation again - should use commit graph
        depth_after = get_depth(self.store, c4.id)
        self.assertEqual(3, depth_after)

        # Test with commit graph disabled
        self.store._use_commit_graph = False
        self.assertIsNone(self.store.get_commit_graph())

        # Operations should still work without commit graph
        depth_disabled = get_depth(self.store, c4.id)
        self.assertEqual(3, depth_disabled)


class TreeLookupPathTests(TestCase):
    def setUp(self) -> None:
        TestCase.setUp(self)
        self.store = MemoryObjectStore()
        blob_a = make_object(Blob, data=b"a")
        blob_b = make_object(Blob, data=b"b")
        blob_c = make_object(Blob, data=b"c")
        for blob in [blob_a, blob_b, blob_c]:
            self.store.add_object(blob)

        blobs = [
            (b"a", blob_a.id, 0o100644),
            (b"ad/b", blob_b.id, 0o100644),
            (b"ad/bd/c", blob_c.id, 0o100755),
            (b"ad/c", blob_c.id, 0o100644),
            (b"c", blob_c.id, 0o100644),
            (b"d", blob_c.id, S_IFGITLINK),
        ]
        self.tree_id = commit_tree(self.store, blobs)

    def get_object(self, sha):
        return self.store[sha]

    def test_lookup_blob(self) -> None:
        o_id = tree_lookup_path(self.get_object, self.tree_id, b"a")[1]
        self.assertIsInstance(self.store[o_id], Blob)

    def test_lookup_tree(self) -> None:
        o_id = tree_lookup_path(self.get_object, self.tree_id, b"ad")[1]
        self.assertIsInstance(self.store[o_id], Tree)
        o_id = tree_lookup_path(self.get_object, self.tree_id, b"ad/bd")[1]
        self.assertIsInstance(self.store[o_id], Tree)
        o_id = tree_lookup_path(self.get_object, self.tree_id, b"ad/bd/")[1]
        self.assertIsInstance(self.store[o_id], Tree)

    def test_lookup_submodule(self) -> None:
        tree_lookup_path(self.get_object, self.tree_id, b"d")[1]
        self.assertRaises(
            SubmoduleEncountered,
            tree_lookup_path,
            self.get_object,
            self.tree_id,
            b"d/a",
        )

    def test_lookup_nonexistent(self) -> None:
        self.assertRaises(
            KeyError, tree_lookup_path, self.get_object, self.tree_id, b"j"
        )

    def test_lookup_not_tree(self) -> None:
        self.assertRaises(
            NotTreeError,
            tree_lookup_path,
            self.get_object,
            self.tree_id,
            b"ad/b/j",
        )

    def test_lookup_empty_path(self) -> None:
        # Empty path should return the tree itself
        mode, sha = tree_lookup_path(self.get_object, self.tree_id, b"")
        self.assertEqual(sha, self.tree_id)
        self.assertEqual(mode, stat.S_IFDIR)


class ObjectStoreGraphWalkerTests(TestCase):
    def get_walker(self, heads, parent_map):
        new_parent_map = {
            k * 40: [(p * 40) for p in ps] for (k, ps) in parent_map.items()
        }
        return ObjectStoreGraphWalker(
            [x * 40 for x in heads], new_parent_map.__getitem__
        )

    def test_ack_invalid_value(self) -> None:
        gw = self.get_walker([], {})
        self.assertRaises(ValueError, gw.ack, "tooshort")

    def test_empty(self) -> None:
        gw = self.get_walker([], {})
        self.assertIs(None, next(gw))
        gw.ack(b"a" * 40)
        self.assertIs(None, next(gw))

    def test_descends(self) -> None:
        gw = self.get_walker([b"a"], {b"a": [b"b"], b"b": []})
        self.assertEqual(b"a" * 40, next(gw))
        self.assertEqual(b"b" * 40, next(gw))

    def test_present(self) -> None:
        gw = self.get_walker([b"a"], {b"a": [b"b"], b"b": []})
        gw.ack(b"a" * 40)
        self.assertIs(None, next(gw))

    def test_parent_present(self) -> None:
        gw = self.get_walker([b"a"], {b"a": [b"b"], b"b": []})
        self.assertEqual(b"a" * 40, next(gw))
        gw.ack(b"a" * 40)
        self.assertIs(None, next(gw))

    def test_child_ack_later(self) -> None:
        gw = self.get_walker([b"a"], {b"a": [b"b"], b"b": [b"c"], b"c": []})
        self.assertEqual(b"a" * 40, next(gw))
        self.assertEqual(b"b" * 40, next(gw))
        gw.ack(b"a" * 40)
        self.assertIs(None, next(gw))

    def test_only_once(self) -> None:
        # a  b
        # |  |
        # c  d
        # \ /
        #  e
        gw = self.get_walker(
            [b"a", b"b"],
            {
                b"a": [b"c"],
                b"b": [b"d"],
                b"c": [b"e"],
                b"d": [b"e"],
                b"e": [],
            },
        )
        walk = []
        acked = False
        walk.append(next(gw))
        walk.append(next(gw))
        # A branch (a, c) or (b, d) may be done after 2 steps or 3 depending on
        # the order walked: 3-step walks include (a, b, c) and (b, a, d), etc.
        if walk == [b"a" * 40, b"c" * 40] or walk == [b"b" * 40, b"d" * 40]:
            gw.ack(walk[0])
            acked = True

        walk.append(next(gw))
        if not acked and walk[2] == b"c" * 40:
            gw.ack(b"a" * 40)
        elif not acked and walk[2] == b"d" * 40:
            gw.ack(b"b" * 40)
        walk.append(next(gw))
        self.assertIs(None, next(gw))

        self.assertEqual([b"a" * 40, b"b" * 40, b"c" * 40, b"d" * 40], sorted(walk))
        self.assertLess(walk.index(b"a" * 40), walk.index(b"c" * 40))
        self.assertLess(walk.index(b"b" * 40), walk.index(b"d" * 40))


class CommitTreeChangesTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.store = MemoryObjectStore()
        self.blob_a = make_object(Blob, data=b"a")
        self.blob_b = make_object(Blob, data=b"b")
        self.blob_c = make_object(Blob, data=b"c")
        for blob in [self.blob_a, self.blob_b, self.blob_c]:
            self.store.add_object(blob)

        blobs = [
            (b"a", self.blob_a.id, 0o100644),
            (b"ad/b", self.blob_b.id, 0o100644),
            (b"ad/bd/c", self.blob_c.id, 0o100755),
            (b"ad/c", self.blob_c.id, 0o100644),
            (b"c", self.blob_c.id, 0o100644),
        ]
        self.tree_id = commit_tree(self.store, blobs)

    def test_no_changes(self) -> None:
        self.assertEqual(
            self.store[self.tree_id],
            commit_tree_changes(self.store, self.store[self.tree_id], []),
        )

    def test_add_blob(self) -> None:
        blob_d = make_object(Blob, data=b"d")
        new_tree = commit_tree_changes(
            self.store, self.store[self.tree_id], [(b"d", 0o100644, blob_d.id)]
        )
        self.assertEqual(
            new_tree[b"d"],
            (33188, b"c59d9b6344f1af00e504ba698129f07a34bbed8d"),
        )

    def test_add_blob_in_dir(self) -> None:
        blob_d = make_object(Blob, data=b"d")
        new_tree = commit_tree_changes(
            self.store,
            self.store[self.tree_id],
            [(b"e/f/d", 0o100644, blob_d.id)],
        )
        self.assertEqual(
            new_tree.items(),
            [
                TreeEntry(path=b"a", mode=stat.S_IFREG | 0o100644, sha=self.blob_a.id),
                TreeEntry(
                    path=b"ad",
                    mode=stat.S_IFDIR,
                    sha=b"0e2ce2cd7725ff4817791be31ccd6e627e801f4a",
                ),
                TreeEntry(path=b"c", mode=stat.S_IFREG | 0o100644, sha=self.blob_c.id),
                TreeEntry(
                    path=b"e",
                    mode=stat.S_IFDIR,
                    sha=b"6ab344e288724ac2fb38704728b8896e367ed108",
                ),
            ],
        )
        e_tree = self.store[new_tree[b"e"][1]]
        self.assertEqual(
            e_tree.items(),
            [
                TreeEntry(
                    path=b"f",
                    mode=stat.S_IFDIR,
                    sha=b"24d2c94d8af232b15a0978c006bf61ef4479a0a5",
                )
            ],
        )
        f_tree = self.store[e_tree[b"f"][1]]
        self.assertEqual(
            f_tree.items(),
            [TreeEntry(path=b"d", mode=stat.S_IFREG | 0o100644, sha=blob_d.id)],
        )

    def test_delete_blob(self) -> None:
        new_tree = commit_tree_changes(
            self.store, self.store[self.tree_id], [(b"ad/bd/c", None, None)]
        )
        self.assertEqual(set(new_tree), {b"a", b"ad", b"c"})
        ad_tree = self.store[new_tree[b"ad"][1]]
        self.assertEqual(set(ad_tree), {b"b", b"c"})


class TestReadPacksFile(TestCase):
    def test_read_packs(self) -> None:
        self.assertEqual(
            ["pack-1.pack"],
            list(
                read_packs_file(
                    BytesIO(
                        b"""P pack-1.pack
"""
                    )
                )
            ),
        )
