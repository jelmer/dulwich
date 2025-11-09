# test_stripspace.py -- Tests for stripspace functionality
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
#
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

"""Tests for stripspace functionality."""

import unittest

from dulwich.stripspace import stripspace


class StripspaceTests(unittest.TestCase):
    """Tests for the stripspace function."""

    def test_empty_input(self):
        """Test that empty input returns empty output."""
        self.assertEqual(stripspace(b""), b"")

    def test_simple_text(self):
        """Test simple text with no issues."""
        self.assertEqual(stripspace(b"hello\nworld\n"), b"hello\nworld\n")

    def test_trailing_whitespace(self):
        """Test that trailing whitespace is removed."""
        self.assertEqual(stripspace(b"hello  \nworld\t\n"), b"hello\nworld\n")

    def test_multiple_blank_lines(self):
        """Test that multiple blank lines are collapsed to one."""
        self.assertEqual(
            stripspace(b"hello\n\n\n\nworld\n"),
            b"hello\n\nworld\n",
        )

    def test_leading_blank_lines(self):
        """Test that leading blank lines are removed."""
        self.assertEqual(
            stripspace(b"\n\n\nhello\nworld\n"),
            b"hello\nworld\n",
        )

    def test_trailing_blank_lines(self):
        """Test that trailing blank lines are removed."""
        self.assertEqual(
            stripspace(b"hello\nworld\n\n\n"),
            b"hello\nworld\n",
        )

    def test_leading_and_trailing_blank_lines(self):
        """Test that both leading and trailing blank lines are removed."""
        self.assertEqual(
            stripspace(b"\n\nhello\nworld\n\n"),
            b"hello\nworld\n",
        )

    def test_adds_final_newline(self):
        """Test that a final newline is added if missing."""
        self.assertEqual(stripspace(b"hello\nworld"), b"hello\nworld\n")

    def test_complex_whitespace(self):
        """Test complex whitespace cleanup."""
        input_text = b"  hello  \n\n\n\n  world  \n\n"
        expected = b"hello\n\nworld\n"
        self.assertEqual(stripspace(input_text), expected)

    def test_strip_comments(self):
        """Test stripping lines that start with comment character."""
        input_text = b"# comment\nhello\n# another comment\nworld\n"
        expected = b"hello\nworld\n"
        self.assertEqual(stripspace(input_text, strip_comments=True), expected)

    def test_strip_comments_indented(self):
        """Test that indented comments are also stripped."""
        input_text = b"  # comment\nhello\n\t# another comment\nworld\n"
        expected = b"hello\nworld\n"
        self.assertEqual(stripspace(input_text, strip_comments=True), expected)

    def test_strip_comments_custom_char(self):
        """Test stripping comments with custom comment character."""
        input_text = b"; comment\nhello\n; another comment\nworld\n"
        expected = b"hello\nworld\n"
        self.assertEqual(
            stripspace(input_text, strip_comments=True, comment_char=b";"),
            expected,
        )

    def test_comment_lines(self):
        """Test prepending comment character to each line."""
        input_text = b"hello\nworld\n"
        expected = b"# hello\n# world\n"
        self.assertEqual(stripspace(input_text, comment_lines=True), expected)

    def test_comment_lines_custom_char(self):
        """Test prepending custom comment character to each line."""
        input_text = b"hello\nworld\n"
        expected = b"; hello\n; world\n"
        self.assertEqual(
            stripspace(input_text, comment_lines=True, comment_char=b";"),
            expected,
        )

    def test_only_whitespace(self):
        """Test input that contains only whitespace."""
        self.assertEqual(stripspace(b"  \n\t\n  \n"), b"")

    def test_only_blank_lines(self):
        """Test input that contains only blank lines."""
        self.assertEqual(stripspace(b"\n\n\n"), b"")

    def test_crlf_line_endings(self):
        """Test handling of Windows-style line endings."""
        self.assertEqual(
            stripspace(b"hello  \r\nworld\t\r\n"),
            b"hello\r\nworld\r\n",
        )

    def test_mixed_line_endings(self):
        """Test handling of mixed line endings."""
        self.assertEqual(
            stripspace(b"hello  \r\nworld\t\n"),
            b"hello\r\nworld\n",
        )

    def test_commit_message_example(self):
        """Test a realistic commit message cleanup."""
        input_text = b"""

Add new feature

This commit adds a new feature to the project.


Signed-off-by: Alice <alice@example.com>

"""
        expected = b"""Add new feature

This commit adds a new feature to the project.

Signed-off-by: Alice <alice@example.com>
"""
        self.assertEqual(stripspace(input_text), expected)

    def test_strip_comments_preserves_non_comment_hash(self):
        """Test that # in the middle of a line is not treated as a comment."""
        input_text = b"# comment\nhello # world\n"
        expected = b"hello # world\n"
        self.assertEqual(stripspace(input_text, strip_comments=True), expected)

    def test_comment_lines_then_strip_whitespace(self):
        """Test that comment_lines works correctly with whitespace stripping."""
        input_text = b"hello  \n\n\nworld  \n"
        expected = b"# hello\n\n# world\n"
        self.assertEqual(stripspace(input_text, comment_lines=True), expected)

    def test_only_comments(self):
        """Test input that contains only comments."""
        input_text = b"# comment 1\n# comment 2\n# comment 3\n"
        self.assertEqual(stripspace(input_text, strip_comments=True), b"")

    def test_blank_line_between_content(self):
        """Test that a single blank line between content is preserved."""
        input_text = b"hello\n\nworld\n"
        expected = b"hello\n\nworld\n"
        self.assertEqual(stripspace(input_text), expected)
