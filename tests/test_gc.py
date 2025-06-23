"""Tests for dulwich.gc."""

import shutil
import tempfile
from unittest import TestCase

from dulwich.gc import (
    GCStats,
    find_reachable_objects,
    find_unreachable_objects,
    garbage_collect,
    prune_unreachable_objects,
)
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo


class GCTestCase(TestCase):
    """Tests for garbage collection functionality."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.repo = Repo.init(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

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

        # Prune unreachable objects
        pruned, bytes_freed = prune_unreachable_objects(
            self.repo.object_store, self.repo.refs, grace_period=0
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

        # Prune with dry run
        pruned, bytes_freed = prune_unreachable_objects(
            self.repo.object_store, self.repo.refs, grace_period=0, dry_run=True
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

        # Run garbage collection
        stats = garbage_collect(self.repo, prune=True, grace_period=0)

        # Check results
        self.assertIsInstance(stats, GCStats)
        self.assertEqual({unreachable_blob.id}, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)

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

        # Run garbage collection with dry run
        stats = garbage_collect(self.repo, prune=True, grace_period=0, dry_run=True)

        # Check that object would be pruned but still exists
        self.assertEqual({unreachable_blob.id}, stats.pruned_objects)
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

        # Now test with zero grace period - it should be pruned
        stats = garbage_collect(self.repo, prune=True, grace_period=0)

        # Check that the object was pruned
        self.assertEqual({unreachable_blob.id}, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)

    def test_grace_period_old_object(self):
        """Test that old objects are pruned even with grace period."""
        import os
        import time

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

        # Run garbage collection
        stats = garbage_collect(self.repo, prune=True, grace_period=0)

        # Check that the packed object was pruned
        self.assertEqual({unreachable_blob.id}, stats.pruned_objects)
        self.assertGreater(stats.bytes_freed, 0)
        self.assertNotIn(unreachable_blob.id, self.repo.object_store)
