# test_diffstat.py -- Tests for diffstat
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Test Contributor
# All rights reserved.

"""Tests for dulwich.contrib.diffstat."""

import os
import tempfile
import unittest

from dulwich.contrib.diffstat import (
    _parse_patch,
    diffstat,
    main,
)


class ParsePatchTests(unittest.TestCase):
    """Tests for _parse_patch function."""

    def test_empty_input(self):
        """Test parsing an empty list of lines."""
        names, nametypes, counts = _parse_patch([])
        self.assertEqual(names, [])
        self.assertEqual(nametypes, [])
        self.assertEqual(counts, [])

    def test_basic_git_diff(self):
        """Test parsing a basic git diff with additions and deletions."""
        diff = [
            b"diff --git a/file.txt b/file.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file.txt",
            b"+++ b/file.txt",
            b"@@ -1,5 +1,7 @@",
            b" unchanged line",
            b"-deleted line",
            b"-another deleted line",
            b"+added line",
            b"+another added line",
            b"+third added line",
            b" unchanged line",
        ]
        names, nametypes, counts = _parse_patch(diff)
        self.assertEqual(names, [b"file.txt"])
        self.assertEqual(nametypes, [False])  # Not a binary file
        self.assertEqual(counts, [(3, 2)])  # 3 additions, 2 deletions

    def test_chunk_ending_with_nonstandard_line(self):
        """Test parsing a git diff where a chunk ends with a non-standard line.

        This tests the code path in line 103 of diffstat.py where the in_patch_chunk
        flag is set to False when encountering a line that doesn't start with
        the unchanged, added, or deleted indicators.
        """
        diff = [
            b"diff --git a/file.txt b/file.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file.txt",
            b"+++ b/file.txt",
            b"@@ -1,5 +1,7 @@",
            b" unchanged line",
            b"-deleted line",
            b"+added line",
            b"No leading space or indicator",  # Non-standard line
            b"diff --git a/file2.txt b/file2.txt",  # Next file's diff
            b"index 2345678..bcdefgh 100644",
            b"--- a/file2.txt",
            b"+++ b/file2.txt",
            b"@@ -1,3 +1,4 @@",
            b" unchanged in file2",
            b"+added in file2",
            b" another unchanged in file2",
        ]
        names, nametypes, counts = _parse_patch(diff)
        self.assertEqual(names, [b"file.txt", b"file2.txt"])
        self.assertEqual(nametypes, [False, False])
        self.assertEqual(
            counts, [(1, 1), (1, 0)]
        )  # file1: 1 add, 1 delete; file2: 1 add, 0 delete

    def test_binary_files(self):
        """Test parsing a git diff with binary files."""
        diff = [
            b"diff --git a/image.png b/image.png",
            b"index 1234567..abcdefg 100644",
            b"Binary files a/image.png and b/image.png differ",
        ]
        names, nametypes, counts = _parse_patch(diff)
        self.assertEqual(names, [b"image.png"])
        self.assertEqual(nametypes, [True])  # Is a binary file
        self.assertEqual(counts, [(0, 0)])  # No additions/deletions counted

    def test_renamed_file(self):
        """Test parsing a git diff with a renamed file."""
        diff = [
            b"diff --git a/oldname.txt b/newname.txt",
            b"similarity index 80%",
            b"rename from oldname.txt",
            b"rename to newname.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/oldname.txt",
            b"+++ b/newname.txt",
            b"@@ -1,3 +1,4 @@",
            b" unchanged line",
            b" another unchanged line",
            b"+added line",
            b" third unchanged line",
        ]
        names, nametypes, counts = _parse_patch(diff)
        # The name should include both old and new names
        self.assertEqual(names, [b"oldname.txt => newname.txt"])
        self.assertEqual(nametypes, [False])  # Not a binary file
        self.assertEqual(counts, [(1, 0)])  # 1 addition, 0 deletions

    def test_multiple_files(self):
        """Test parsing a git diff with multiple files."""
        diff = [
            # First file
            b"diff --git a/file1.txt b/file1.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file1.txt",
            b"+++ b/file1.txt",
            b"@@ -1,3 +1,4 @@",
            b" unchanged",
            b"+added",
            b" unchanged",
            b" unchanged",
            # Second file
            b"diff --git a/file2.txt b/file2.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file2.txt",
            b"+++ b/file2.txt",
            b"@@ -1,3 +1,2 @@",
            b" unchanged",
            b"-deleted",
            b" unchanged",
        ]
        names, nametypes, counts = _parse_patch(diff)
        self.assertEqual(names, [b"file1.txt", b"file2.txt"])
        self.assertEqual(nametypes, [False, False])
        self.assertEqual(
            counts, [(1, 0), (0, 1)]
        )  # 1 addition, 0 deletions for file1; 0 additions, 1 deletion for file2


class DiffstatTests(unittest.TestCase):
    """Tests for diffstat function."""

    def test_empty_diff(self):
        """Test generating diffstat for an empty diff."""
        result = diffstat([])
        self.assertEqual(result, b" 0 files changed, 0 insertions(+), 0 deletions(-)")

    def test_basic_diffstat(self):
        """Test generating a basic diffstat."""
        diff = [
            b"diff --git a/file.txt b/file.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file.txt",
            b"+++ b/file.txt",
            b"@@ -1,2 +1,3 @@",
            b" unchanged line",
            b"+added line",
            b" unchanged line",
        ]
        result = diffstat(diff)
        # Check that the output contains key elements
        self.assertIn(b"file.txt", result)
        self.assertIn(b"1 files changed", result)
        self.assertIn(b"1 insertions(+)", result)
        self.assertIn(b"0 deletions(-)", result)

    def test_binary_file_diffstat(self):
        """Test generating diffstat with binary files."""
        diff = [
            b"diff --git a/image.png b/image.png",
            b"index 1234567..abcdefg 100644",
            b"Binary files a/image.png and b/image.png differ",
        ]
        result = diffstat(diff)
        self.assertIn(b"image.png", result)
        self.assertIn(b"Bin", result)  # Binary file indicator
        self.assertIn(b"1 files changed", result)
        self.assertIn(b"0 insertions(+)", result)
        self.assertIn(b"0 deletions(-)", result)

    def test_multiple_files_diffstat(self):
        """Test generating diffstat with multiple files."""
        diff = [
            # First file
            b"diff --git a/file1.txt b/file1.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file1.txt",
            b"+++ b/file1.txt",
            b"@@ -1,3 +1,5 @@",
            b" unchanged",
            b"+added1",
            b"+added2",
            b" unchanged",
            b" unchanged",
            # Second file
            b"diff --git a/file2.txt b/file2.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file2.txt",
            b"+++ b/file2.txt",
            b"@@ -1,3 +1,2 @@",
            b" unchanged",
            b"-deleted",
            b" unchanged",
        ]
        result = diffstat(diff)
        self.assertIn(b"file1.txt", result)
        self.assertIn(b"file2.txt", result)
        self.assertIn(b"2 files changed", result)
        self.assertIn(b"2 insertions(+)", result)
        self.assertIn(b"1 deletions(-)", result)

    def test_custom_width(self):
        """Test diffstat with custom width parameter."""
        diff = [
            b"diff --git a/file.txt b/file.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file.txt",
            b"+++ b/file.txt",
            b"@@ -1,2 +1,5 @@",
            b" unchanged line",
            b"+added line 1",
            b"+added line 2",
            b"+added line 3",
            b" unchanged line",
        ]
        # Test with a very narrow width
        narrow_result = diffstat(diff, max_width=30)

        # Test with a wide width
        wide_result = diffstat(diff, max_width=120)

        # Both should contain the same file info but potentially different histogram widths
        self.assertIn(b"file.txt", narrow_result)
        self.assertIn(b"file.txt", wide_result)
        self.assertIn(b"1 files changed", narrow_result)
        self.assertIn(b"1 files changed", wide_result)
        self.assertIn(b"3 insertions(+)", narrow_result)
        self.assertIn(b"3 insertions(+)", wide_result)

    def test_histwidth_scaling(self):
        """Test histogram width scaling for various change sizes."""
        # Create a diff with a large number of changes to trigger the histogram scaling
        diff_lines = [
            b"diff --git a/file.txt b/file.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/file.txt",
            b"+++ b/file.txt",
            b"@@ -1,50 +1,50 @@",
        ]

        # Add a lot of added and deleted lines
        for i in range(30):
            diff_lines.append(b"+added line %d" % i)

        for i in range(20):
            diff_lines.append(b"-deleted line %d" % i)

        # Try with a narrow width to force scaling
        result = diffstat(diff_lines, max_width=40)
        self.assertIn(b"file.txt", result)
        self.assertIn(b"50", result)  # Should show 50 changes (30+20)

        # Make sure it has some + and - characters for the histogram
        plus_count = result.count(b"+")
        minus_count = result.count(b"-")
        self.assertGreater(plus_count, 0)
        self.assertGreater(minus_count, 0)

    def test_small_nonzero_changes(self):
        """Test with very small positive changes that would round to zero."""
        # Create a diff with a tiny number of changes and a large max_diff to trigger
        # the small ratio calculation
        normal_diff = [
            b"diff --git a/bigfile.txt b/bigfile.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/bigfile.txt",
            b"+++ b/bigfile.txt",
            b"@@ -1,1000 +1,1001 @@",
            b"+new line",  # Just one addition
        ]

        lot_of_changes_diff = [
            b"diff --git a/hugefile.txt b/hugefile.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/hugefile.txt",
            b"+++ b/hugefile.txt",
            b"@@ -1,1000 +1,2000 @@",
        ]

        # Add 1000 added lines to this one
        for i in range(1000):
            lot_of_changes_diff.append(b"+added line %d" % i)

        # Combine these diffs
        combined_diff = normal_diff + lot_of_changes_diff

        # Use a very large width to make the contrast obvious
        result = diffstat(combined_diff, max_width=200)

        # The small change should still have at least one '+' in the histogram
        self.assertIn(b"bigfile.txt", result)
        self.assertIn(b"hugefile.txt", result)
        self.assertIn(b"2 files changed", result)
        self.assertIn(b"1001 insertions(+)", result)

        # Get the line for bigfile.txt (should be the first file line)
        lines = result.split(b"\n")
        bigfile_line = next(line for line in lines if b"bigfile.txt" in line)

        # Make sure it has at least one + even though the ratio would be tiny
        self.assertIn(b"+", bigfile_line)

    def test_big_diff_histogram(self):
        """Test histogram creation with very large diffs."""
        # Create a large diff with many additions and deletions to test histogram width scaling
        diff_lines = [
            b"diff --git a/bigfile.txt b/bigfile.txt",
            b"index 1234567..abcdefg 100644",
            b"--- a/bigfile.txt",
            b"+++ b/bigfile.txt",
            b"@@ -1,1000 +1,2000 @@",
        ]

        # Add 1000 additions and 500 deletions
        for i in range(1000):
            diff_lines.append(b"+added line %d" % i)
        for i in range(500):
            diff_lines.append(b"-deleted line %d" % i)

        # Test with different widths
        narrow_result = diffstat(diff_lines, max_width=40)
        wide_result = diffstat(diff_lines, max_width=120)

        # Both should show the right number of changes
        for result in [narrow_result, wide_result]:
            self.assertIn(b"1 files changed", result)
            self.assertIn(b"1000 insertions(+)", result)
            self.assertIn(b"500 deletions(-)", result)

    def test_small_deletions_only(self):
        """Test histogram creation with only a few deletions."""
        # Create a diff with a huge maxdiff to force scaling, but only a few deletions
        diff1 = [
            b"diff --git a/file1.txt b/file1.txt",
            b"@@ -1,1000 +1,900 @@",
        ]
        for i in range(100):
            diff1.append(b"-deleted line %d" % i)

        # Create a second diff with many more changes to increase maxdiff
        diff2 = [
            b"diff --git a/file2.txt b/file2.txt",
            b"@@ -1,1000 +1,5000 @@",
        ]
        for i in range(4000):
            diff2.append(b"+added line %d" % i)

        # Combine the diffs
        diff = diff1 + diff2

        # Generate diffstat with a very wide display
        result = diffstat(diff, max_width=200)

        # Make sure both files are reported
        self.assertIn(b"file1.txt", result)
        self.assertIn(b"file2.txt", result)

        # Get the line for file1.txt
        lines = result.split(b"\n")
        file1_line = next(line for line in lines if b"file1.txt" in line)

        # Should show some - characters for the deletions
        self.assertIn(b"-", file1_line)

    def test_very_small_deletions_ratio(self):
        """Test histogram with tiny deletion ratio that would round to zero.

        This tests line 174 in diffstat.py where a small ratio between 0 and 1
        is forced to be at least 1 character wide in the histogram.
        """
        # Create a diff with a single deletion and a massive number of additions
        # to make the deletion ratio tiny
        diff = [
            b"diff --git a/file1.txt b/file1.txt",
            b"@@ -1,2 +1,1 @@",
            b"-single deleted line",  # Just one deletion
            b" unchanged line",
            b"diff --git a/file2.txt b/file2.txt",
            b"@@ -1,1 +1,10001 @@",
            b" unchanged line",
        ]

        # Add 10000 additions to file2 to create a huge maxdiff
        for i in range(10000):
            diff.append(b"+added line %d" % i)

        # Generate diffstat with a moderate display width
        result = diffstat(diff, max_width=80)

        # Make sure both files are reported
        self.assertIn(b"file1.txt", result)
        self.assertIn(b"file2.txt", result)

        # Get the line for file1.txt
        lines = result.split(b"\n")
        file1_line = next(line for line in lines if b"file1.txt" in line)

        # Should show at least one - character for the deletion
        # even though the ratio would be tiny (1/10001 ≈ 0.0001)
        self.assertIn(b"-", file1_line)

        # Confirm the summary stats are correct
        self.assertIn(b"2 files changed", result)
        self.assertIn(b"10000 insertions(+)", result)
        self.assertIn(b"1 deletions(-)", result)


class MainFunctionTests(unittest.TestCase):
    """Tests for the main() function."""

    def test_main_with_diff_file(self):
        """Test the main function with a diff file argument."""
        # Create a temporary diff file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            diff_content = b"""diff --git a/file.txt b/file.txt
index 1234567..abcdefg 100644
--- a/file.txt
+++ b/file.txt
@@ -1,3 +1,4 @@
 unchanged line
+added line
 another unchanged line
 third unchanged line
"""
            tmp.write(diff_content)
            tmp_path = tmp.name

        try:
            # Save the original sys.argv
            import sys

            orig_argv = sys.argv

            # Test with a file path argument
            sys.argv = ["diffstat.py", tmp_path]
            return_code = main()
            self.assertEqual(return_code, 0)

            # Test with no args to trigger the self-test
            sys.argv = ["diffstat.py"]
            return_code = main()
            self.assertEqual(return_code, 0)
        finally:
            # Restore original sys.argv
            sys.argv = orig_argv

            # Clean up the temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_main_self_test_failure(self):
        """Test the main function when the self-test fails."""
        import io
        import sys

        from dulwich.contrib.diffstat import diffstat as real_diffstat

        # Save original sys.argv, diffstat function, and stdout
        orig_argv = sys.argv
        orig_diffstat = real_diffstat
        orig_stdout = sys.stdout

        try:
            # Set up for testing self-test failure
            sys.argv = ["diffstat.py"]

            # Replace stdout with a StringIO object to capture output
            captured_output = io.StringIO()
            sys.stdout = captured_output

            # Mock the diffstat function to return a wrong result
            # This will trigger the self-test failure path
            from dulwich.contrib import diffstat as diffstat_module

            diffstat_module.diffstat = lambda lines, max_width=80: b"WRONG OUTPUT"

            # The main function should return -1 for self-test failure
            return_code = main()
            self.assertEqual(return_code, -1)

            # Check if the expected output is captured
            captured = captured_output.getvalue()
            self.assertIn("self test failed", captured)
            self.assertIn("Received:", captured)
            self.assertIn("WRONG OUTPUT", captured)
            self.assertIn("Expected:", captured)

        finally:
            # Restore original sys.argv, diffstat function, and stdout
            sys.argv = orig_argv
            diffstat_module.diffstat = orig_diffstat
            sys.stdout = orig_stdout


if __name__ == "__main__":
    unittest.main()
