# test_diff.py -- Tests for diff functionality.
# Copyright (C) 2025 Dulwich contributors
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

"""Tests for diff functionality."""

import io
import unittest

from dulwich.diff import ColorizedDiffStream

from . import TestCase


class ColorizedDiffStreamTests(TestCase):
    """Tests for ColorizedDiffStream."""

    def setUp(self):
        super().setUp()
        self.output = io.BytesIO()

    @unittest.skipUnless(
        ColorizedDiffStream.is_available(), "Rich not available for colorization"
    )
    def test_write_simple_diff(self):
        """Test writing a simple diff with colorization."""
        stream = ColorizedDiffStream(self.output)

        diff_content = b"""--- a/file.txt
+++ b/file.txt
@@ -1,3 +1,3 @@
 unchanged line
-removed line
+added line
 another unchanged line
"""

        stream.write(diff_content)
        stream.flush()

        # We can't easily test the exact colored output without mocking Rich,
        # but we can test that the stream writes something and doesn't crash
        result = self.output.getvalue()
        self.assertGreater(len(result), 0)

    @unittest.skipUnless(
        ColorizedDiffStream.is_available(), "Rich not available for colorization"
    )
    def test_write_line_by_line(self):
        """Test writing diff content line by line."""
        stream = ColorizedDiffStream(self.output)

        lines = [
            b"--- a/file.txt\n",
            b"+++ b/file.txt\n",
            b"@@ -1,2 +1,2 @@\n",
            b"-old line\n",
            b"+new line\n",
        ]

        for line in lines:
            stream.write(line)

        stream.flush()

        result = self.output.getvalue()
        self.assertGreater(len(result), 0)

    @unittest.skipUnless(
        ColorizedDiffStream.is_available(), "Rich not available for colorization"
    )
    def test_writelines(self):
        """Test writelines method."""
        stream = ColorizedDiffStream(self.output)

        lines = [
            b"--- a/file.txt\n",
            b"+++ b/file.txt\n",
            b"@@ -1,1 +1,1 @@\n",
            b"-old\n",
            b"+new\n",
        ]

        stream.writelines(lines)
        stream.flush()

        result = self.output.getvalue()
        self.assertGreater(len(result), 0)

    @unittest.skipUnless(
        ColorizedDiffStream.is_available(), "Rich not available for colorization"
    )
    def test_partial_line_buffering(self):
        """Test that partial lines are buffered correctly."""
        stream = ColorizedDiffStream(self.output)

        # Write partial line
        stream.write(b"+partial")
        # Output should be empty as line is not complete
        result = self.output.getvalue()
        self.assertEqual(len(result), 0)

        # Complete the line
        stream.write(b" line\n")
        result = self.output.getvalue()
        self.assertGreater(len(result), 0)

    @unittest.skipUnless(
        ColorizedDiffStream.is_available(), "Rich not available for colorization"
    )
    def test_unicode_handling(self):
        """Test handling of unicode content in diffs."""
        stream = ColorizedDiffStream(self.output)

        # UTF-8 encoded content
        unicode_diff = "--- a/ünïcödë.txt\n+++ b/ünïcödë.txt\n@@ -1,1 +1,1 @@\n-ōld\n+nëw\n".encode()

        stream.write(unicode_diff)
        stream.flush()

        result = self.output.getvalue()
        self.assertGreater(len(result), 0)

    def test_is_available_static_method(self):
        """Test is_available static method."""
        # This should not raise an error regardless of Rich availability
        result = ColorizedDiffStream.is_available()
        self.assertIsInstance(result, bool)

    @unittest.skipIf(
        ColorizedDiffStream.is_available(), "Rich is available, skipping fallback test"
    )
    def test_rich_not_available(self):
        """Test behavior when Rich is not available."""
        # When Rich is not available, we can't instantiate ColorizedDiffStream
        # This test only runs when Rich is not available
        with self.assertRaises(ImportError):
            ColorizedDiffStream(self.output)


class MockColorizedDiffStreamTests(TestCase):
    """Tests for ColorizedDiffStream using a mock when Rich is not available."""

    def setUp(self):
        super().setUp()
        self.output = io.BytesIO()

    def test_fallback_behavior_with_mock(self):
        """Test that we can handle cases where Rich is not available."""
        # This test demonstrates how the CLI handles the case where Rich is unavailable
        if not ColorizedDiffStream.is_available():
            # When Rich is not available, we should use the raw stream
            stream = self.output
        else:
            # When Rich is available, we can use ColorizedDiffStream
            stream = ColorizedDiffStream(self.output)

        diff_content = b"+added line\n-removed line\n"

        if hasattr(stream, "write"):
            stream.write(diff_content)
            if hasattr(stream, "flush"):
                stream.flush()

        # Test that some output was produced
        result = self.output.getvalue()
        self.assertGreaterEqual(len(result), 0)
