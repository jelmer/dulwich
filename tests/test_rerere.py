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

        result = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual([], result)

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

        result = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual(1, len(result))

        path, conflict_id = result[0]
        self.assertEqual(b"test.txt", path)
        self.assertEqual(40, len(conflict_id))  # SHA-1 hash length

    def test_rerere_auto_skips_non_conflicted_files(self) -> None:
        """Test that rerere_auto skips files without conflict markers."""
        from dulwich.rerere import rerere_auto

        # Create a non-conflicted file
        file_path = os.path.join(self.tempdir, "test.txt")
        with open(file_path, "wb") as f:
            f.write(b"line 1\nline 2\n")

        result = rerere_auto(self.repo, self.tempdir, [b"test.txt"])
        self.assertEqual([], result)

    def test_rerere_auto_handles_missing_files(self) -> None:
        """Test that rerere_auto handles deleted files gracefully."""
        from dulwich.rerere import rerere_auto

        # Don't create the file
        result = rerere_auto(self.repo, self.tempdir, [b"missing.txt"])
        self.assertEqual([], result)
