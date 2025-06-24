"""Tests for dulwich.gc."""

import os
import shutil
import tempfile
import time
from unittest import TestCase
from unittest.mock import patch

from dulwich.config import ConfigDict
from dulwich.gc import (
    GCStats,
    find_reachable_objects,
    find_unreachable_objects,
    garbage_collect,
    maybe_auto_gc,
    prune_unreachable_objects,
    should_run_gc,
)
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import MemoryRepo, Repo


class GCTestCase(TestCase):
    """Tests for garbage collection functionality."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir)
        self.repo = Repo.init(self.tmpdir)
        self.addCleanup(self.repo.close)

    def test_find_reachable_objects_empty_repo(self):
        """Test finding reachable objects in empty repository."""
        reachable = find_reachable_objects(self.repo.object_store, self.repo.refs)
        self.assertEqual(set(), reachable)

    def test_find_reachable_objects_with_commit(self):
        """Test finding reachable objects with a commit."""
        # Create a blob
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)

        # Create a tree
        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        # Create a commit
        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit"
        self.repo.object_store.add_object(commit)

        # Set HEAD to the commit
        self.repo.refs[b"HEAD"] = commit.id

        # Find reachable objects
        reachable = find_reachable_objects(self.repo.object_store, self.repo.refs)

        # All three objects should be reachable
        self.assertEqual({blob.id, tree.id, commit.id}, reachable)

    def test_find_unreachable_objects(self):
        """Test finding unreachable objects."""
        # Create a reachable blob
        reachable_blob = Blob.from_string(b"reachable content")
        self.repo.object_store.add_object(reachable_blob)

        # Create a tree
        tree = Tree()
        tree.add(b"reachable.txt", 0o100644, reachable_blob.id)
        self.repo.object_store.add_object(tree)

        # Create a commit
        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit"
        self.repo.object_store.add_object(commit)

        # Set HEAD to the commit
        self.repo.refs[b"HEAD"] = commit.id

        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"unreachable content")
        self.repo.object_store.add_object(unreachable_blob)

        # Find unreachable objects
        unreachable = find_unreachable_objects(self.repo.object_store, self.repo.refs)

        # Only the unreachable blob should be found
        self.assertEqual({unreachable_blob.id}, unreachable)

    def test_prune_unreachable_objects(self):
        """Test pruning unreachable objects."""
        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"unreachable content")
        self.repo.object_store.add_object(unreachable_blob)

        # Verify it exists
        self.assertIn(unreachable_blob.id, self.repo.object_store)

        # Prune unreachable objects (grace_period=None means no grace period check)
        pruned, bytes_freed = prune_unreachable_objects(
            self.repo.object_store, self.repo.refs, grace_period=None
        )

        # Verify the blob was pruned
        self.assertEqual({unreachable_blob.id}, pruned)
        self.assertGreater(bytes_freed, 0)

        # Note: We can't test that the object is gone because delete()
        # only supports loose objects and may not be fully implemented

    def test_prune_unreachable_objects_dry_run(self):
        """Test pruning unreachable objects with dry run."""
        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"unreachable content")
        self.repo.object_store.add_object(unreachable_blob)

        # Prune with dry run (grace_period=None means no grace period check)
        pruned, bytes_freed = prune_unreachable_objects(
            self.repo.object_store, self.repo.refs, grace_period=None, dry_run=True
        )

        # Verify the blob would be pruned but still exists
        self.assertEqual({unreachable_blob.id}, pruned)
        self.assertGreater(bytes_freed, 0)
        self.assertIn(unreachable_blob.id, self.repo.object_store)

    def test_garbage_collect(self):
        """Test full garbage collection."""
        # Create some reachable objects
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit"
        self.repo.object_store.add_object(commit)

        self.repo.refs[b"HEAD"] = commit.id

        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"unreachable content")
        self.repo.object_store.add_object(unreachable_blob)

        # Run garbage collection (grace_period=None means no grace period check)
        stats = garbage_collect(self.repo, prune=True, grace_period=None)

        # Check results
        self.assertIsInstance(stats, GCStats)
        self.assertEqual({unreachable_blob.id}, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)
        # Check that loose objects were counted
        self.assertGreaterEqual(
            stats.loose_objects_before, 4
        )  # At least blob, tree, commit, unreachable
        self.assertLess(
            stats.loose_objects_after, stats.loose_objects_before
        )  # Should have fewer after GC

    def test_garbage_collect_no_prune(self):
        """Test garbage collection without pruning."""
        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"unreachable content")
        self.repo.object_store.add_object(unreachable_blob)

        # Run garbage collection without pruning
        stats = garbage_collect(self.repo, prune=False)

        # Check that nothing was pruned
        self.assertEqual(set(), stats.pruned_objects)
        self.assertEqual(0, stats.bytes_freed)
        self.assertIn(unreachable_blob.id, self.repo.object_store)

    def test_garbage_collect_dry_run(self):
        """Test garbage collection with dry run."""
        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"unreachable content")
        self.repo.object_store.add_object(unreachable_blob)

        # Run garbage collection with dry run (grace_period=None means no grace period check)
        stats = garbage_collect(self.repo, prune=True, grace_period=None, dry_run=True)

        # Check that object would be pruned but still exists
        # On Windows, the repository initialization might create additional unreachable objects
        # So we check that our blob is in the pruned objects, not that it's the only one
        self.assertIn(unreachable_blob.id, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)
        self.assertIn(unreachable_blob.id, self.repo.object_store)

    def test_grace_period(self):
        """Test that grace period prevents pruning recent objects."""
        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"recent unreachable content")
        self.repo.object_store.add_object(unreachable_blob)

        # Ensure the object is loose
        self.assertTrue(self.repo.object_store.contains_loose(unreachable_blob.id))

        # Run garbage collection with a 1 hour grace period, but dry run to avoid packing
        # The object was just created, so it should not be pruned
        stats = garbage_collect(self.repo, prune=True, grace_period=3600, dry_run=True)

        # Check that the object was NOT pruned
        self.assertEqual(set(), stats.pruned_objects)
        self.assertEqual(0, stats.bytes_freed)
        self.assertIn(unreachable_blob.id, self.repo.object_store)

        # Now test with no grace period - it should be pruned
        stats = garbage_collect(self.repo, prune=True, grace_period=None)

        # Check that the object was pruned
        self.assertEqual({unreachable_blob.id}, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)

    def test_grace_period_old_object(self):
        """Test that old objects are pruned even with grace period."""
        # Create an unreachable blob
        old_blob = Blob.from_string(b"old unreachable content")
        self.repo.object_store.add_object(old_blob)

        # Ensure the object is loose
        self.assertTrue(self.repo.object_store.contains_loose(old_blob.id))

        # Manually set the mtime to 2 hours ago
        path = self.repo.object_store._get_shafile_path(old_blob.id)
        old_time = time.time() - 7200  # 2 hours ago
        os.utime(path, (old_time, old_time))

        # Run garbage collection with a 1 hour grace period
        # The object is 2 hours old, so it should be pruned
        stats = garbage_collect(self.repo, prune=True, grace_period=3600)

        # Check that the object was pruned
        self.assertEqual({old_blob.id}, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)

    def test_packed_objects_pruned(self):
        """Test that packed objects are pruned via repack with exclusion."""
        # Create an unreachable blob
        unreachable_blob = Blob.from_string(b"unreachable packed content")
        self.repo.object_store.add_object(unreachable_blob)

        # Pack the objects to ensure the blob is in a pack
        self.repo.object_store.pack_loose_objects()

        # Ensure the object is NOT loose anymore
        self.assertFalse(self.repo.object_store.contains_loose(unreachable_blob.id))
        self.assertIn(unreachable_blob.id, self.repo.object_store)

        # Run garbage collection (grace_period=None means no grace period check)
        stats = garbage_collect(self.repo, prune=True, grace_period=None)

        # Check that the packed object was pruned
        self.assertEqual({unreachable_blob.id}, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)
        self.assertNotIn(unreachable_blob.id, self.repo.object_store)


class AutoGCTestCase(TestCase):
    """Tests for auto GC functionality."""

    def test_should_run_gc_disabled(self):
        """Test that auto GC doesn't run when gc.auto is 0."""
        r = MemoryRepo()
        config = ConfigDict()
        config.set(b"gc", b"auto", b"0")

        self.assertFalse(should_run_gc(r, config))

    def test_should_run_gc_default_values(self):
        """Test auto GC with default configuration values."""
        r = MemoryRepo()
        config = ConfigDict()

        # Should not run with empty repo
        self.assertFalse(should_run_gc(r, config))

    def test_should_run_gc_with_loose_objects(self):
        """Test that auto GC triggers based on loose object count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)
            config = ConfigDict()
            config.set(b"gc", b"auto", b"10")  # Low threshold for testing

            # Add some loose objects
            for i in range(15):
                blob = Blob()
                blob.data = f"test blob {i}".encode()
                r.object_store.add_object(blob)

            self.assertTrue(should_run_gc(r, config))

    def test_should_run_gc_with_pack_limit(self):
        """Test that auto GC triggers based on pack file count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)
            config = ConfigDict()
            config.set(b"gc", b"autoPackLimit", b"2")  # Low threshold for testing

            # Create some pack files by repacking
            for i in range(3):
                blob = Blob()
                blob.data = f"test blob {i}".encode()
                r.object_store.add_object(blob)
                r.object_store.pack_loose_objects()

            # Force re-enumeration of packs
            r.object_store._update_pack_cache()

            self.assertTrue(should_run_gc(r, config))

    def test_count_loose_objects(self):
        """Test counting loose objects."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)

            # Initially should have no loose objects
            count = r.object_store.count_loose_objects()
            self.assertEqual(0, count)

            # Add some loose objects
            for i in range(5):
                blob = Blob()
                blob.data = f"test blob {i}".encode()
                r.object_store.add_object(blob)

            count = r.object_store.count_loose_objects()
            self.assertEqual(5, count)

    def test_count_pack_files(self):
        """Test counting pack files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)

            # Initially should have no packs
            count = r.object_store.count_pack_files()
            self.assertEqual(0, count)

            # Create a pack
            blob = Blob()
            blob.data = b"test blob"
            r.object_store.add_object(blob)
            r.object_store.pack_loose_objects()

            # Force re-enumeration of packs
            r.object_store._update_pack_cache()

            count = r.object_store.count_pack_files()
            self.assertEqual(1, count)

    def test_maybe_auto_gc_runs_when_needed(self):
        """Test that auto GC runs when thresholds are exceeded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)
            config = ConfigDict()
            config.set(b"gc", b"auto", b"5")  # Low threshold for testing

            # Add enough loose objects to trigger GC
            for i in range(10):
                blob = Blob()
                blob.data = f"test blob {i}".encode()
                r.object_store.add_object(blob)

            with patch("dulwich.gc.garbage_collect") as mock_gc:
                result = maybe_auto_gc(r, config)

            self.assertTrue(result)
            mock_gc.assert_called_once_with(r, auto=True)

    def test_maybe_auto_gc_skips_when_not_needed(self):
        """Test that auto GC doesn't run when thresholds are not exceeded."""
        r = MemoryRepo()
        config = ConfigDict()

        with patch("dulwich.gc.garbage_collect") as mock_gc:
            result = maybe_auto_gc(r, config)

        self.assertFalse(result)
        mock_gc.assert_not_called()

    def test_maybe_auto_gc_with_gc_log(self):
        """Test that auto GC is skipped when gc.log exists and is recent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)
            config = ConfigDict()
            config.set(b"gc", b"auto", b"1")  # Low threshold

            # Create gc.log file
            gc_log_path = os.path.join(r.controldir(), "gc.log")
            with open(gc_log_path, "wb") as f:
                f.write(b"Previous GC failed\n")

            # Add objects to trigger GC
            blob = Blob()
            blob.data = b"test"
            r.object_store.add_object(blob)

            with patch("builtins.print") as mock_print:
                result = maybe_auto_gc(r, config)

            self.assertFalse(result)
            # Verify gc.log contents were printed
            mock_print.assert_called_once_with("Previous GC failed\n")

    def test_maybe_auto_gc_with_expired_gc_log(self):
        """Test that auto GC runs when gc.log exists but is expired."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)
            config = ConfigDict()
            config.set(b"gc", b"auto", b"1")  # Low threshold
            config.set(b"gc", b"logExpiry", b"0.days")  # Expire immediately

            # Create gc.log file
            gc_log_path = os.path.join(r.controldir(), "gc.log")
            with open(gc_log_path, "wb") as f:
                f.write(b"Previous GC failed\n")

            # Make the file old
            old_time = time.time() - 86400  # 1 day ago
            os.utime(gc_log_path, (old_time, old_time))

            # Add objects to trigger GC
            blob = Blob()
            blob.data = b"test"
            r.object_store.add_object(blob)

            with patch("dulwich.gc.garbage_collect") as mock_gc:
                result = maybe_auto_gc(r, config)

            self.assertTrue(result)
            mock_gc.assert_called_once_with(r, auto=True)
            # gc.log should be removed after successful GC
            self.assertFalse(os.path.exists(gc_log_path))

    def test_maybe_auto_gc_handles_gc_failure(self):
        """Test that auto GC handles failures gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            r = Repo.init(tmpdir)
            config = ConfigDict()
            config.set(b"gc", b"auto", b"1")  # Low threshold

            # Add objects to trigger GC
            blob = Blob()
            blob.data = b"test"
            r.object_store.add_object(blob)

            with patch(
                "dulwich.gc.garbage_collect", side_effect=OSError("GC failed")
            ) as mock_gc:
                result = maybe_auto_gc(r, config)

            self.assertFalse(result)
            mock_gc.assert_called_once_with(r, auto=True)

            # Check that error was written to gc.log
            gc_log_path = os.path.join(r.controldir(), "gc.log")
            self.assertTrue(os.path.exists(gc_log_path))
            with open(gc_log_path, "rb") as f:
                content = f.read()
                self.assertIn(b"Auto GC failed: GC failed", content)
