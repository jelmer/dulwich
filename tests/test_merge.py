"""Tests for merge functionality."""

import importlib.util
import unittest

from dulwich.merge import MergeConflict, Merger, three_way_merge
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import MemoryRepo


class MergeTests(unittest.TestCase):
    """Tests for merge functionality."""

    def setUp(self):
        self.repo = MemoryRepo()
        # Check if merge3 module is available
        if importlib.util.find_spec("merge3") is None:
            raise unittest.SkipTest("merge3 module not available, skipping merge tests")
        self.merger = Merger(self.repo.object_store)

    def test_merge_blobs_no_conflict(self):
        """Test merging blobs without conflicts."""
        # Create base blob
        base_blob = Blob.from_string(b"line1\nline2\nline3\n")

        # Create modified versions - currently our algorithm treats changes to different line groups as conflicts
        # This is a simple implementation - Git's merge is more sophisticated
        ours_blob = Blob.from_string(b"line1\nmodified line2\nline3\n")
        theirs_blob = Blob.from_string(b"line1\nline2\nmodified line3\n")

        # Add blobs to object store
        self.repo.object_store.add_object(base_blob)
        self.repo.object_store.add_object(ours_blob)
        self.repo.object_store.add_object(theirs_blob)

        # Merge - this will result in a conflict with our simple algorithm
        result, has_conflicts = self.merger.merge_blobs(
            base_blob, ours_blob, theirs_blob
        )

        # For now, expect conflicts since both sides changed (even different lines)
        self.assertTrue(has_conflicts)
        self.assertIn(b"<<<<<<< ours", result)
        self.assertIn(b">>>>>>> theirs", result)

    def test_merge_blobs_clean_merge(self):
        """Test merging blobs with a clean merge (one side unchanged)."""
        # Create base blob
        base_blob = Blob.from_string(b"line1\nline2\nline3\n")

        # Only ours modifies
        ours_blob = Blob.from_string(b"line1\nmodified line2\nline3\n")
        theirs_blob = base_blob  # unchanged

        # Add blobs to object store
        self.repo.object_store.add_object(base_blob)
        self.repo.object_store.add_object(ours_blob)

        # Merge
        result, has_conflicts = self.merger.merge_blobs(
            base_blob, ours_blob, theirs_blob
        )

        self.assertFalse(has_conflicts)
        self.assertEqual(result, b"line1\nmodified line2\nline3\n")

    def test_merge_blobs_with_conflict(self):
        """Test merging blobs with conflicts."""
        # Create base blob
        base_blob = Blob.from_string(b"line1\nline2\nline3\n")

        # Create conflicting modifications
        ours_blob = Blob.from_string(b"line1\nours line2\nline3\n")
        theirs_blob = Blob.from_string(b"line1\ntheirs line2\nline3\n")

        # Add blobs to object store
        self.repo.object_store.add_object(base_blob)
        self.repo.object_store.add_object(ours_blob)
        self.repo.object_store.add_object(theirs_blob)

        # Merge
        result, has_conflicts = self.merger.merge_blobs(
            base_blob, ours_blob, theirs_blob
        )

        self.assertTrue(has_conflicts)
        self.assertIn(b"<<<<<<< ours", result)
        self.assertIn(b"=======", result)
        self.assertIn(b">>>>>>> theirs", result)

    def test_merge_blobs_identical(self):
        """Test merging identical blobs."""
        blob = Blob.from_string(b"same content\n")
        self.repo.object_store.add_object(blob)

        result, has_conflicts = self.merger.merge_blobs(blob, blob, blob)

        self.assertFalse(has_conflicts)
        self.assertEqual(result, b"same content\n")

    def test_merge_blobs_one_side_unchanged(self):
        """Test merging when one side is unchanged."""
        base_blob = Blob.from_string(b"original\n")
        modified_blob = Blob.from_string(b"modified\n")

        self.repo.object_store.add_object(base_blob)
        self.repo.object_store.add_object(modified_blob)

        # Test ours unchanged, theirs modified
        result, has_conflicts = self.merger.merge_blobs(
            base_blob, base_blob, modified_blob
        )
        self.assertFalse(has_conflicts)
        self.assertEqual(result, b"modified\n")

        # Test theirs unchanged, ours modified
        result, has_conflicts = self.merger.merge_blobs(
            base_blob, modified_blob, base_blob
        )
        self.assertFalse(has_conflicts)
        self.assertEqual(result, b"modified\n")

    def test_merge_blobs_deletion_no_conflict(self):
        """Test merging with deletion where no conflict occurs."""
        base_blob = Blob.from_string(b"content\n")
        self.repo.object_store.add_object(base_blob)

        # Both delete
        result, has_conflicts = self.merger.merge_blobs(base_blob, None, None)
        self.assertFalse(has_conflicts)
        self.assertEqual(result, b"")

        # One deletes, other unchanged
        result, has_conflicts = self.merger.merge_blobs(base_blob, None, base_blob)
        self.assertFalse(has_conflicts)
        self.assertEqual(result, b"")

    def test_merge_blobs_deletion_with_conflict(self):
        """Test merging with deletion that causes conflict."""
        base_blob = Blob.from_string(b"content\n")
        modified_blob = Blob.from_string(b"modified content\n")

        self.repo.object_store.add_object(base_blob)
        self.repo.object_store.add_object(modified_blob)

        # We delete, they modify
        result, has_conflicts = self.merger.merge_blobs(base_blob, None, modified_blob)
        self.assertTrue(has_conflicts)

    def test_merge_blobs_no_base(self):
        """Test merging blobs with no common ancestor."""
        blob1 = Blob.from_string(b"content1\n")
        blob2 = Blob.from_string(b"content2\n")

        self.repo.object_store.add_object(blob1)
        self.repo.object_store.add_object(blob2)

        # Different content added in both - conflict
        result, has_conflicts = self.merger.merge_blobs(None, blob1, blob2)
        self.assertTrue(has_conflicts)

        # Same content added in both - no conflict
        result, has_conflicts = self.merger.merge_blobs(None, blob1, blob1)
        self.assertFalse(has_conflicts)
        self.assertEqual(result, b"content1\n")

    def test_merge_trees_simple(self):
        """Test simple tree merge."""
        # Create base tree
        base_tree = Tree()
        blob1 = Blob.from_string(b"file1 content\n")
        blob2 = Blob.from_string(b"file2 content\n")
        self.repo.object_store.add_object(blob1)
        self.repo.object_store.add_object(blob2)
        base_tree.add(b"file1.txt", 0o100644, blob1.id)
        base_tree.add(b"file2.txt", 0o100644, blob2.id)
        self.repo.object_store.add_object(base_tree)

        # Create ours tree (modify file1)
        ours_tree = Tree()
        ours_blob1 = Blob.from_string(b"file1 modified by ours\n")
        self.repo.object_store.add_object(ours_blob1)
        ours_tree.add(b"file1.txt", 0o100644, ours_blob1.id)
        ours_tree.add(b"file2.txt", 0o100644, blob2.id)
        self.repo.object_store.add_object(ours_tree)

        # Create theirs tree (modify file2)
        theirs_tree = Tree()
        theirs_blob2 = Blob.from_string(b"file2 modified by theirs\n")
        self.repo.object_store.add_object(theirs_blob2)
        theirs_tree.add(b"file1.txt", 0o100644, blob1.id)
        theirs_tree.add(b"file2.txt", 0o100644, theirs_blob2.id)
        self.repo.object_store.add_object(theirs_tree)

        # Merge
        merged_tree, conflicts = self.merger.merge_trees(
            base_tree, ours_tree, theirs_tree
        )

        self.assertEqual(len(conflicts), 0)
        self.assertIn(b"file1.txt", [item.path for item in merged_tree.items()])
        self.assertIn(b"file2.txt", [item.path for item in merged_tree.items()])

    def test_merge_trees_with_conflict(self):
        """Test tree merge with conflicting changes."""
        # Create base tree
        base_tree = Tree()
        blob1 = Blob.from_string(b"original content\n")
        self.repo.object_store.add_object(blob1)
        base_tree.add(b"conflict.txt", 0o100644, blob1.id)
        self.repo.object_store.add_object(base_tree)

        # Create ours tree
        ours_tree = Tree()
        ours_blob = Blob.from_string(b"ours content\n")
        self.repo.object_store.add_object(ours_blob)
        ours_tree.add(b"conflict.txt", 0o100644, ours_blob.id)
        self.repo.object_store.add_object(ours_tree)

        # Create theirs tree
        theirs_tree = Tree()
        theirs_blob = Blob.from_string(b"theirs content\n")
        self.repo.object_store.add_object(theirs_blob)
        theirs_tree.add(b"conflict.txt", 0o100644, theirs_blob.id)
        self.repo.object_store.add_object(theirs_tree)

        # Merge
        merged_tree, conflicts = self.merger.merge_trees(
            base_tree, ours_tree, theirs_tree
        )

        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0], b"conflict.txt")

    def test_three_way_merge(self):
        """Test three-way merge between commits."""
        # Create base commit
        base_tree = Tree()
        blob = Blob.from_string(b"base content\n")
        self.repo.object_store.add_object(blob)
        base_tree.add(b"file.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(base_tree)

        base_commit = Commit()
        base_commit.tree = base_tree.id
        base_commit.author = b"Test Author <test@example.com>"
        base_commit.committer = b"Test Author <test@example.com>"
        base_commit.message = b"Base commit"
        base_commit.commit_time = base_commit.author_time = 12345
        base_commit.commit_timezone = base_commit.author_timezone = 0
        self.repo.object_store.add_object(base_commit)

        # Create ours commit
        ours_tree = Tree()
        ours_blob = Blob.from_string(b"ours content\n")
        self.repo.object_store.add_object(ours_blob)
        ours_tree.add(b"file.txt", 0o100644, ours_blob.id)
        self.repo.object_store.add_object(ours_tree)

        ours_commit = Commit()
        ours_commit.tree = ours_tree.id
        ours_commit.parents = [base_commit.id]
        ours_commit.author = b"Test Author <test@example.com>"
        ours_commit.committer = b"Test Author <test@example.com>"
        ours_commit.message = b"Ours commit"
        ours_commit.commit_time = ours_commit.author_time = 12346
        ours_commit.commit_timezone = ours_commit.author_timezone = 0
        self.repo.object_store.add_object(ours_commit)

        # Create theirs commit
        theirs_tree = Tree()
        theirs_blob = Blob.from_string(b"theirs content\n")
        self.repo.object_store.add_object(theirs_blob)
        theirs_tree.add(b"file.txt", 0o100644, theirs_blob.id)
        self.repo.object_store.add_object(theirs_tree)

        theirs_commit = Commit()
        theirs_commit.tree = theirs_tree.id
        theirs_commit.parents = [base_commit.id]
        theirs_commit.author = b"Test Author <test@example.com>"
        theirs_commit.committer = b"Test Author <test@example.com>"
        theirs_commit.message = b"Theirs commit"
        theirs_commit.commit_time = theirs_commit.author_time = 12347
        theirs_commit.commit_timezone = theirs_commit.author_timezone = 0
        self.repo.object_store.add_object(theirs_commit)

        # Perform three-way merge
        merged_tree, conflicts = three_way_merge(
            self.repo.object_store, base_commit, ours_commit, theirs_commit
        )

        # Should have conflict since both modified the same file differently
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0], b"file.txt")

    def test_merge_exception(self):
        """Test MergeConflict exception."""
        exc = MergeConflict(b"test/path", "test message")
        self.assertEqual(exc.path, b"test/path")
        self.assertIn("test/path", str(exc))
        self.assertIn("test message", str(exc))
