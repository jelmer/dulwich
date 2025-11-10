"""Tests for rerere functionality."""

import os
import tempfile
import unittest

from dulwich.rerere import (
    RerereCache,
    _extract_conflict_regions,
    _has_conflict_markers,
    _normalize_conflict_markers,
    _remove_conflict_markers,
    is_rerere_autoupdate,
    is_rerere_enabled,
)


class NormalizeConflictMarkersTests(unittest.TestCase):
    """Tests for _normalize_conflict_markers function."""

    def test_normalize_basic_conflict(self) -> None:
        """Test normalizing a basic conflict."""
        content = b"""line 1
<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
line 2
"""
        expected = b"""line 1
<<<<<<<
our change
=======
their change
>>>>>>>
line 2
"""
        result = _normalize_conflict_markers(content)
        self.assertEqual(expected, result)

    def test_normalize_with_branch_names(self) -> None:
        """Test normalizing conflict with branch names."""
        content = b"""<<<<<<< HEAD
content from HEAD
=======
content from feature
>>>>>>> feature
"""
        expected = b"""<<<<<<<
content from HEAD
=======
content from feature
>>>>>>>
"""
        result = _normalize_conflict_markers(content)
        self.assertEqual(expected, result)


class ExtractConflictRegionsTests(unittest.TestCase):
    """Tests for _extract_conflict_regions function."""

    def test_extract_single_conflict(self) -> None:
        """Test extracting a single conflict region."""
        content = b"""line 1
<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
line 2
"""
        regions = _extract_conflict_regions(content)
        self.assertEqual(1, len(regions))
        ours, sep, theirs = regions[0]
        self.assertEqual(b"our change", ours)
        self.assertEqual(b"=======", sep)
        self.assertEqual(b"their change", theirs)

    def test_extract_multiple_conflicts(self) -> None:
        """Test extracting multiple conflict regions."""
        content = b"""<<<<<<< ours
change 1
=======
change 2
>>>>>>> theirs
middle line
<<<<<<< ours
change 3
=======
change 4
>>>>>>> theirs
"""
        regions = _extract_conflict_regions(content)
        self.assertEqual(2, len(regions))


class HasConflictMarkersTests(unittest.TestCase):
    """Tests for _has_conflict_markers function."""

    def test_has_conflict_markers(self) -> None:
        """Test detecting conflict markers."""
        content = b"""<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
"""
        self.assertTrue(_has_conflict_markers(content))

    def test_no_conflict_markers(self) -> None:
        """Test content without conflict markers."""
        content = b"""line 1
line 2
line 3
"""
        self.assertFalse(_has_conflict_markers(content))

    def test_partial_conflict_markers(self) -> None:
        """Test content with only some conflict markers."""
        content = b"""<<<<<<< ours
our change
line 3
"""
        self.assertFalse(_has_conflict_markers(content))


class RemoveConflictMarkersTests(unittest.TestCase):
    """Tests for _remove_conflict_markers function."""

    def test_remove_conflict_markers(self) -> None:
        """Test removing conflict markers from resolved content."""
        content = b"""line 1
<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
line 2
"""
        # This is a simplified test - in reality the resolved content
        # would have the user's chosen resolution
        result = _remove_conflict_markers(content)
        # The function keeps only lines outside conflict blocks
        self.assertNotIn(b"<<<<<<<", result)
        self.assertNotIn(b"=======", result)
        self.assertNotIn(b">>>>>>>", result)


class RerereCacheTests(unittest.TestCase):
    """Tests for RerereCache class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.tempdir = tempfile.mkdtemp()
        self.cache = RerereCache(self.tempdir)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_record_conflict(self) -> None:
        """Test recording a conflict."""
        content = b"""line 1
<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
line 2
"""
        conflict_id = self.cache.record_conflict(b"test.txt", content)
        self.assertIsNotNone(conflict_id)
        self.assertEqual(40, len(conflict_id))  # SHA-1 hash length

    def test_record_conflict_no_markers(self) -> None:
        """Test recording content without conflict markers."""
        content = b"line 1\nline 2\n"
        conflict_id = self.cache.record_conflict(b"test.txt", content)
        self.assertIsNone(conflict_id)

    def test_status_empty(self) -> None:
        """Test status with no conflicts."""
        status = self.cache.status()
        self.assertEqual([], status)

    def test_status_with_conflict(self) -> None:
        """Test status with a recorded conflict."""
        content = b"""<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
"""
        conflict_id = self.cache.record_conflict(b"test.txt", content)
        status = self.cache.status()
        self.assertEqual(1, len(status))
        cid, has_resolution = status[0]
        self.assertEqual(conflict_id, cid)
        self.assertFalse(has_resolution)

    def test_has_resolution(self) -> None:
        """Test checking for resolution."""
        content = b"""<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
"""
        conflict_id = self.cache.record_conflict(b"test.txt", content)
        self.assertIsNotNone(conflict_id)
        self.assertFalse(self.cache.has_resolution(conflict_id))

    def test_diff(self) -> None:
        """Test getting diff for a conflict."""
        content = b"""<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
"""
        conflict_id = self.cache.record_conflict(b"test.txt", content)
        self.assertIsNotNone(conflict_id)

        preimage, postimage = self.cache.diff(conflict_id)
        self.assertIsNotNone(preimage)
        self.assertIsNone(postimage)  # No resolution recorded yet

    def test_clear(self) -> None:
        """Test clearing all conflicts."""
        content = b"""<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
"""
        self.cache.record_conflict(b"test.txt", content)
        status = self.cache.status()
        self.assertEqual(1, len(status))

        self.cache.clear()
        status = self.cache.status()
        self.assertEqual([], status)

    def test_forget(self) -> None:
        """Test forgetting a specific conflict."""
        content = b"""<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
"""
        conflict_id = self.cache.record_conflict(b"test.txt", content)
        self.assertIsNotNone(conflict_id)

        self.cache.forget(conflict_id)
        status = self.cache.status()
        self.assertEqual([], status)


class ConfigTests(unittest.TestCase):
    """Tests for rerere configuration functions."""

    def test_is_rerere_enabled_false_by_default(self) -> None:
        """Test that rerere is disabled by default."""
        from dulwich.config import ConfigDict

        config = ConfigDict()
        self.assertFalse(is_rerere_enabled(config))

    def test_is_rerere_enabled_true(self) -> None:
        """Test rerere enabled config."""
        from dulwich.config import ConfigDict

        config = ConfigDict()
        config.set((b"rerere",), b"enabled", b"true")
        self.assertTrue(is_rerere_enabled(config))

    def test_is_rerere_autoupdate_false_by_default(self) -> None:
        """Test that rerere.autoupdate is disabled by default."""
        from dulwich.config import ConfigDict

        config = ConfigDict()
        self.assertFalse(is_rerere_autoupdate(config))

    def test_is_rerere_autoupdate_true(self) -> None:
        """Test rerere.autoupdate enabled config."""
        from dulwich.config import ConfigDict

        config = ConfigDict()
        config.set((b"rerere",), b"autoupdate", b"true")
        self.assertTrue(is_rerere_autoupdate(config))


class RerereAutoTests(unittest.TestCase):
    """Tests for rerere_auto functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""

        from dulwich.repo import Repo

        self.tempdir = tempfile.mkdtemp()
        self.repo = Repo.init(self.tempdir)

        # Enable rerere
        config = self.repo.get_config()
        config.set((b"rerere",), b"enabled", b"true")
        config.write_to_path()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_rerere_auto_disabled(self) -> None:
        """Test that rerere_auto does nothing when disabled."""
        from dulwich.rerere import rerere_auto

        # Disable rerere
        config = self.repo.get_config()
        config.set((b"rerere",), b"enabled", b"false")
        config.write_to_path()

        # Create a fake conflicted file
        conflict_file = os.path.join(self.tempdir, "test.txt")
        with open(conflict_file, "wb") as f:
            f.write(
                b"""<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
"""
            )

        recorded, resolved = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual([], recorded)
        self.assertEqual([], resolved)

    def test_rerere_auto_records_conflicts(self) -> None:
        """Test that rerere_auto records conflicts from working tree."""
        from dulwich.rerere import rerere_auto

        # Create a conflicted file in the working tree
        conflict_file = os.path.join(self.tempdir, "test.txt")
        with open(conflict_file, "wb") as f:
            f.write(
                b"""line 1
<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
line 2
"""
            )

        recorded, resolved = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual(1, len(recorded))
        self.assertEqual(0, len(resolved))

        path, conflict_id = recorded[0]
        self.assertEqual(b"test.txt", path)
        self.assertEqual(40, len(conflict_id))  # SHA-1 hash length

    def test_rerere_auto_skips_non_conflicted_files(self) -> None:
        """Test that rerere_auto skips files without conflict markers."""
        from dulwich.rerere import rerere_auto

        # Create a non-conflicted file
        file_path = os.path.join(self.tempdir, "test.txt")
        with open(file_path, "wb") as f:
            f.write(b"line 1\nline 2\n")

        recorded, resolved = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual([], recorded)
        self.assertEqual([], resolved)

    def test_rerere_auto_handles_missing_files(self) -> None:
        """Test that rerere_auto handles deleted files gracefully."""
        from dulwich.rerere import rerere_auto

        # Don't create the file
        recorded, resolved = rerere_auto(self.repo, self.tempdir, [b"missing.txt"])
        self.assertEqual([], recorded)
        self.assertEqual([], resolved)

    def test_rerere_auto_applies_known_resolution(self) -> None:
        """Test that rerere_auto applies known resolutions when autoupdate is enabled."""
        from dulwich.rerere import RerereCache, rerere_auto

        # Enable autoupdate
        config = self.repo.get_config()
        config.set((b"rerere",), b"autoupdate", b"true")
        config.write_to_path()

        # Create a conflicted file
        conflict_file = os.path.join(self.tempdir, "test.txt")
        conflict_content = b"""line 1
<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
line 2
"""
        with open(conflict_file, "wb") as f:
            f.write(conflict_content)

        # Record the conflict first time
        recorded, resolved = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual(1, len(recorded))
        self.assertEqual(0, len(resolved))  # No resolution yet

        conflict_id = recorded[0][1]

        # Manually record a resolution
        cache = RerereCache.from_repo(self.repo)
        resolution = b"line 1\nresolved change\nline 2\n"
        cache.record_resolution(conflict_id, resolution)

        # Create the same conflict again
        with open(conflict_file, "wb") as f:
            f.write(conflict_content)

        # rerere_auto should now apply the resolution
        recorded2, resolved2 = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual(1, len(recorded2))
        self.assertEqual(1, len(resolved2))
        self.assertEqual(b"test.txt", resolved2[0])

        # Verify the file was resolved
        with open(conflict_file, "rb") as f:
            actual = f.read()
        self.assertEqual(resolution, actual)

    def test_rerere_auto_no_apply_without_autoupdate(self) -> None:
        """Test that rerere_auto doesn't apply resolutions when autoupdate is disabled."""
        from dulwich.rerere import RerereCache, rerere_auto

        # autoupdate is disabled by default

        # Create a conflicted file
        conflict_file = os.path.join(self.tempdir, "test.txt")
        conflict_content = b"""line 1
<<<<<<< ours
our change
=======
their change
>>>>>>> theirs
line 2
"""
        with open(conflict_file, "wb") as f:
            f.write(conflict_content)

        # Record the conflict first time
        recorded, _resolved = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        conflict_id = recorded[0][1]

        # Manually record a resolution
        cache = RerereCache.from_repo(self.repo)
        resolution = b"line 1\nresolved change\nline 2\n"
        cache.record_resolution(conflict_id, resolution)

        # Create the same conflict again
        with open(conflict_file, "wb") as f:
            f.write(conflict_content)

        # rerere_auto should NOT apply the resolution (autoupdate disabled)
        recorded2, resolved2 = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual(1, len(recorded2))
        self.assertEqual(0, len(resolved2))  # Should not auto-apply

        # Verify the file still has conflicts
        with open(conflict_file, "rb") as f:
            actual = f.read()
        self.assertEqual(conflict_content, actual)


class RerereEndToEndTests(unittest.TestCase):
    """End-to-end tests for rerere with real merge operations."""

    def setUp(self) -> None:
        """Set up test fixtures."""

        from dulwich.objects import Blob, Commit, Tree
        from dulwich.repo import Repo

        self.tempdir = tempfile.mkdtemp()
        self.repo = Repo.init(self.tempdir)

        # Enable rerere
        config = self.repo.get_config()
        config.set((b"rerere",), b"enabled", b"true")
        config.write_to_path()

        # Create initial commit on master
        blob1 = Blob.from_string(b"line 1\noriginal line\nline 3\n")
        self.repo.object_store.add_object(blob1)

        tree1 = Tree()
        tree1.add(b"file.txt", 0o100644, blob1.id)
        self.repo.object_store.add_object(tree1)

        commit1 = Commit()
        commit1.tree = tree1.id
        commit1.author = commit1.committer = b"Test User <test@example.com>"
        commit1.author_time = commit1.commit_time = 1234567890
        commit1.author_timezone = commit1.commit_timezone = 0
        commit1.encoding = b"UTF-8"
        commit1.message = b"Initial commit"
        self.repo.object_store.add_object(commit1)

        self.repo.refs[b"refs/heads/master"] = commit1.id
        self.repo.refs[b"HEAD"] = commit1.id

        # Write file to working tree
        with open(os.path.join(self.tempdir, "file.txt"), "wb") as f:
            f.write(b"line 1\noriginal line\nline 3\n")

        self.initial_commit = commit1.id

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        import shutil

        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_rerere_full_workflow(self) -> None:
        """Test complete rerere workflow with real merge conflicts."""
        from dulwich.objects import Blob, Commit, Tree
        from dulwich.porcelain import merge, rerere

        # Create branch1: change "original line" to "branch1 change"
        blob_branch1 = Blob.from_string(b"line 1\nbranch1 change\nline 3\n")
        self.repo.object_store.add_object(blob_branch1)

        tree_branch1 = Tree()
        tree_branch1.add(b"file.txt", 0o100644, blob_branch1.id)
        self.repo.object_store.add_object(tree_branch1)

        commit_branch1 = Commit()
        commit_branch1.tree = tree_branch1.id
        commit_branch1.parents = [self.initial_commit]
        commit_branch1.author = commit_branch1.committer = (
            b"Test User <test@example.com>"
        )
        commit_branch1.author_time = commit_branch1.commit_time = 1234567891
        commit_branch1.author_timezone = commit_branch1.commit_timezone = 0
        commit_branch1.encoding = b"UTF-8"
        commit_branch1.message = b"Branch1 changes"
        self.repo.object_store.add_object(commit_branch1)
        self.repo.refs[b"refs/heads/branch1"] = commit_branch1.id

        # Create branch2: change "original line" to "branch2 change"
        blob_branch2 = Blob.from_string(b"line 1\nbranch2 change\nline 3\n")
        self.repo.object_store.add_object(blob_branch2)

        tree_branch2 = Tree()
        tree_branch2.add(b"file.txt", 0o100644, blob_branch2.id)
        self.repo.object_store.add_object(tree_branch2)

        commit_branch2 = Commit()
        commit_branch2.tree = tree_branch2.id
        commit_branch2.parents = [self.initial_commit]
        commit_branch2.author = commit_branch2.committer = (
            b"Test User <test@example.com>"
        )
        commit_branch2.author_time = commit_branch2.commit_time = 1234567892
        commit_branch2.author_timezone = commit_branch2.commit_timezone = 0
        commit_branch2.encoding = b"UTF-8"
        commit_branch2.message = b"Branch2 changes"
        self.repo.object_store.add_object(commit_branch2)
        self.repo.refs[b"refs/heads/branch2"] = commit_branch2.id

        # Checkout branch1
        self.repo.refs[b"HEAD"] = commit_branch1.id
        with open(os.path.join(self.tempdir, "file.txt"), "wb") as f:
            f.write(b"line 1\nbranch1 change\nline 3\n")

        # Merge branch2 into branch1 - should create conflict
        merge_result, conflicts = merge(self.repo, b"branch2", no_commit=True)

        # Should have conflicts
        self.assertIsNone(merge_result)  # No commit created due to conflicts
        self.assertEqual([b"file.txt"], conflicts)

        # File should have conflict markers
        with open(os.path.join(self.tempdir, "file.txt"), "rb") as f:
            content = f.read()
        self.assertIn(b"<<<<<<<", content)
        self.assertIn(b"branch1 change", content)
        self.assertIn(b"branch2 change", content)

        # Record the conflict with rerere
        recorded, resolved = rerere(self.repo)
        self.assertEqual(1, len(recorded))
        self.assertEqual(0, len(resolved))  # No resolution yet

        conflict_id = recorded[0][1]

        # User manually resolves the conflict
        resolved_content = b"line 1\nmerged change\nline 3\n"
        with open(os.path.join(self.tempdir, "file.txt"), "wb") as f:
            f.write(resolved_content)

        # Record the resolution
        from dulwich.rerere import RerereCache

        cache = RerereCache.from_repo(self.repo)
        cache.record_resolution(conflict_id, resolved_content)

        # Reset to initial state and try the merge again
        self.repo.refs[b"HEAD"] = commit_branch1.id
        with open(os.path.join(self.tempdir, "file.txt"), "wb") as f:
            f.write(b"line 1\nbranch1 change\nline 3\n")

        # Merge again - should create same conflict
        _merge_result2, conflicts2 = merge(self.repo, b"branch2", no_commit=True)
        self.assertEqual([b"file.txt"], conflicts2)

        # Now rerere should recognize the conflict
        recorded2, resolved2 = rerere(self.repo)
        self.assertEqual(1, len(recorded2))

        # With autoupdate disabled, it shouldn't auto-apply
        self.assertEqual(0, len(resolved2))

    def test_rerere_with_autoupdate(self) -> None:
        """Test rerere with autoupdate enabled."""
        from dulwich.objects import Blob, Commit, Tree
        from dulwich.porcelain import merge, rerere
        from dulwich.rerere import RerereCache

        # Enable autoupdate
        config = self.repo.get_config()
        config.set((b"rerere",), b"autoupdate", b"true")
        config.write_to_path()

        # Create branch1
        blob_branch1 = Blob.from_string(b"line 1\nbranch1 change\nline 3\n")
        self.repo.object_store.add_object(blob_branch1)

        tree_branch1 = Tree()
        tree_branch1.add(b"file.txt", 0o100644, blob_branch1.id)
        self.repo.object_store.add_object(tree_branch1)

        commit_branch1 = Commit()
        commit_branch1.tree = tree_branch1.id
        commit_branch1.parents = [self.initial_commit]
        commit_branch1.author = commit_branch1.committer = (
            b"Test User <test@example.com>"
        )
        commit_branch1.author_time = commit_branch1.commit_time = 1234567891
        commit_branch1.author_timezone = commit_branch1.commit_timezone = 0
        commit_branch1.encoding = b"UTF-8"
        commit_branch1.message = b"Branch1 changes"
        self.repo.object_store.add_object(commit_branch1)
        self.repo.refs[b"refs/heads/branch1"] = commit_branch1.id

        # Create branch2
        blob_branch2 = Blob.from_string(b"line 1\nbranch2 change\nline 3\n")
        self.repo.object_store.add_object(blob_branch2)

        tree_branch2 = Tree()
        tree_branch2.add(b"file.txt", 0o100644, blob_branch2.id)
        self.repo.object_store.add_object(tree_branch2)

        commit_branch2 = Commit()
        commit_branch2.tree = tree_branch2.id
        commit_branch2.parents = [self.initial_commit]
        commit_branch2.author = commit_branch2.committer = (
            b"Test User <test@example.com>"
        )
        commit_branch2.author_time = commit_branch2.commit_time = 1234567892
        commit_branch2.author_timezone = commit_branch2.commit_timezone = 0
        commit_branch2.encoding = b"UTF-8"
        commit_branch2.message = b"Branch2 changes"
        self.repo.object_store.add_object(commit_branch2)
        self.repo.refs[b"refs/heads/branch2"] = commit_branch2.id

        # Checkout branch1 and merge branch2
        self.repo.refs[b"HEAD"] = commit_branch1.id
        with open(os.path.join(self.tempdir, "file.txt"), "wb") as f:
            f.write(b"line 1\nbranch1 change\nline 3\n")

        merge(self.repo, b"branch2", no_commit=True)

        # Record conflict and resolution
        recorded, _ = rerere(self.repo)
        conflict_id = recorded[0][1]

        resolved_content = b"line 1\nmerged change\nline 3\n"
        with open(os.path.join(self.tempdir, "file.txt"), "wb") as f:
            f.write(resolved_content)

        cache = RerereCache.from_repo(self.repo)
        cache.record_resolution(conflict_id, resolved_content)

        # Reset and merge again
        self.repo.refs[b"HEAD"] = commit_branch1.id
        with open(os.path.join(self.tempdir, "file.txt"), "wb") as f:
            f.write(b"line 1\nbranch1 change\nline 3\n")

        merge(self.repo, b"branch2", no_commit=True)

        # With autoupdate, rerere should auto-apply the resolution
        recorded2, resolved2 = rerere(self.repo)
        self.assertEqual(1, len(recorded2))
        self.assertEqual(1, len(resolved2))
        self.assertEqual(b"file.txt", resolved2[0])

        # Verify the file was auto-resolved
        with open(os.path.join(self.tempdir, "file.txt"), "rb") as f:
            actual = f.read()
        self.assertEqual(resolved_content, actual)
