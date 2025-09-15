# test_whitespace.py -- Tests for whitespace error detection
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

"""Tests for whitespace error detection."""

from dulwich.whitespace import (
    DEFAULT_WHITESPACE_ERRORS,
    WhitespaceChecker,
    fix_whitespace_errors,
    parse_whitespace_config,
)

from . import TestCase


class WhitespaceConfigTests(TestCase):
    """Test core.whitespace configuration parsing."""

    def test_parse_default(self) -> None:
        """Test default whitespace configuration."""
        errors, tab_width = parse_whitespace_config(None)
        self.assertEqual(errors, DEFAULT_WHITESPACE_ERRORS)
        self.assertEqual(tab_width, 8)

    def test_parse_empty(self) -> None:
        """Test empty whitespace configuration."""
        errors, tab_width = parse_whitespace_config("")
        self.assertEqual(errors, set())
        self.assertEqual(tab_width, 8)

    def test_parse_single_error(self) -> None:
        """Test single error type."""
        errors, tab_width = parse_whitespace_config("blank-at-eol")
        self.assertEqual(errors, {"blank-at-eol"})
        self.assertEqual(tab_width, 8)

    def test_parse_multiple_errors(self) -> None:
        """Test multiple error types."""
        errors, tab_width = parse_whitespace_config(
            "blank-at-eol,space-before-tab,tab-in-indent"
        )
        self.assertEqual(errors, {"blank-at-eol", "space-before-tab", "tab-in-indent"})
        self.assertEqual(tab_width, 8)

    def test_parse_with_negation(self) -> None:
        """Test negation of default errors."""
        errors, tab_width = parse_whitespace_config("-blank-at-eol")
        # Should have defaults minus blank-at-eol
        expected = DEFAULT_WHITESPACE_ERRORS - {"blank-at-eol"}
        self.assertEqual(errors, expected)
        self.assertEqual(tab_width, 8)

    def test_parse_trailing_space_alias(self) -> None:
        """Test that trailing-space is an alias for blank-at-eol."""
        errors, tab_width = parse_whitespace_config("trailing-space")
        self.assertEqual(errors, {"blank-at-eol"})
        self.assertEqual(tab_width, 8)

    def test_parse_tabwidth(self) -> None:
        """Test tabwidth setting."""
        errors, tab_width = parse_whitespace_config("blank-at-eol,tabwidth=4")
        self.assertEqual(errors, {"blank-at-eol"})
        self.assertEqual(tab_width, 4)

    def test_parse_invalid_tabwidth(self) -> None:
        """Test invalid tabwidth defaults to 8."""
        _errors, tab_width = parse_whitespace_config("tabwidth=invalid")
        self.assertEqual(tab_width, 8)

        _errors, tab_width = parse_whitespace_config("tabwidth=0")
        self.assertEqual(tab_width, 8)


class WhitespaceCheckerTests(TestCase):
    """Test WhitespaceChecker functionality."""

    def test_blank_at_eol(self) -> None:
        """Test detection of trailing whitespace."""
        checker = WhitespaceChecker({"blank-at-eol"})

        # No trailing whitespace
        errors = checker.check_line(b"normal line", 1)
        self.assertEqual(errors, [])

        # Trailing space
        errors = checker.check_line(b"trailing space ", 1)
        self.assertEqual(errors, [("blank-at-eol", 1)])

        # Trailing tab
        errors = checker.check_line(b"trailing tab\t", 1)
        self.assertEqual(errors, [("blank-at-eol", 1)])

        # Multiple trailing whitespace
        errors = checker.check_line(b"multiple  \t ", 1)
        self.assertEqual(errors, [("blank-at-eol", 1)])

    def test_space_before_tab(self) -> None:
        """Test detection of space before tab in indentation."""
        checker = WhitespaceChecker({"space-before-tab"})

        # No space before tab
        errors = checker.check_line(b"\tindented", 1)
        self.assertEqual(errors, [])

        # Space before tab in indentation
        errors = checker.check_line(b" \tindented", 1)
        self.assertEqual(errors, [("space-before-tab", 1)])

        # Space before tab not in indentation (should not trigger)
        errors = checker.check_line(b"code \t comment", 1)
        self.assertEqual(errors, [])

    def test_indent_with_non_tab(self) -> None:
        """Test detection of 8+ spaces at start of line."""
        checker = WhitespaceChecker({"indent-with-non-tab"}, tab_width=8)

        # Less than 8 spaces
        errors = checker.check_line(b"    code", 1)
        self.assertEqual(errors, [])

        # Exactly 8 spaces
        errors = checker.check_line(b"        code", 1)
        self.assertEqual(errors, [("indent-with-non-tab", 1)])

        # More than 8 spaces
        errors = checker.check_line(b"         code", 1)
        self.assertEqual(errors, [("indent-with-non-tab", 1)])

        # Tab after spaces resets count
        errors = checker.check_line(b"    \t    code", 1)
        self.assertEqual(errors, [])

        # Custom tab width
        checker = WhitespaceChecker({"indent-with-non-tab"}, tab_width=4)
        errors = checker.check_line(b"    code", 1)
        self.assertEqual(errors, [("indent-with-non-tab", 1)])

    def test_tab_in_indent(self) -> None:
        """Test detection of tabs in indentation."""
        checker = WhitespaceChecker({"tab-in-indent"})

        # No tabs
        errors = checker.check_line(b"    code", 1)
        self.assertEqual(errors, [])

        # Tab in indentation
        errors = checker.check_line(b"\tcode", 1)
        self.assertEqual(errors, [("tab-in-indent", 1)])

        # Tab after non-whitespace (should not trigger)
        errors = checker.check_line(b"code\tcomment", 1)
        self.assertEqual(errors, [])

    def test_cr_at_eol(self) -> None:
        """Test detection of carriage return at end of line."""
        checker = WhitespaceChecker({"cr-at-eol"})

        # No CR
        errors = checker.check_line(b"normal line", 1)
        self.assertEqual(errors, [])

        # CR at end
        errors = checker.check_line(b"line\r", 1)
        self.assertEqual(errors, [("cr-at-eol", 1)])

    def test_blank_at_eof(self) -> None:
        """Test detection of blank lines at end of file."""
        checker = WhitespaceChecker({"blank-at-eof"})

        # No trailing blank lines
        content = b"line1\nline2\nline3"
        errors = checker.check_content(content)
        self.assertEqual(errors, [])

        # One trailing blank line (normal for files ending with newline)
        content = b"line1\nline2\nline3\n"
        errors = checker.check_content(content)
        self.assertEqual(errors, [])

        # Multiple trailing blank lines
        content = b"line1\nline2\n\n\n"
        errors = checker.check_content(content)
        self.assertEqual(errors, [("blank-at-eof", 5)])

        # Only blank lines
        content = b"\n\n\n"
        errors = checker.check_content(content)
        self.assertEqual(errors, [("blank-at-eof", 4)])

    def test_multiple_errors(self) -> None:
        """Test detection of multiple error types."""
        checker = WhitespaceChecker(
            {"blank-at-eol", "space-before-tab", "tab-in-indent"}
        )

        # Line with multiple errors
        errors = checker.check_line(b" \tcode  ", 1)
        error_types = {e[0] for e in errors}
        self.assertEqual(
            error_types, {"blank-at-eol", "space-before-tab", "tab-in-indent"}
        )

    def test_check_content_crlf(self) -> None:
        """Test content checking with CRLF line endings."""
        checker = WhitespaceChecker({"blank-at-eol", "cr-at-eol"})

        # CRLF line endings
        content = b"line1\r\nline2 \r\nline3\r\n"
        errors = checker.check_content(content)
        # Should detect trailing space on line 2 but not CR (since CRLF is handled)
        self.assertEqual(errors, [("blank-at-eol", 2)])


class WhitespaceFixTests(TestCase):
    """Test whitespace error fixing."""

    def test_fix_blank_at_eol(self) -> None:
        """Test fixing trailing whitespace."""
        content = b"line1  \nline2\t\nline3"
        errors = [("blank-at-eol", 1), ("blank-at-eol", 2)]
        fixed = fix_whitespace_errors(content, errors)
        self.assertEqual(fixed, b"line1\nline2\nline3")

    def test_fix_blank_at_eof(self) -> None:
        """Test fixing blank lines at end of file."""
        content = b"line1\nline2\n\n\n"
        errors = [("blank-at-eof", 4)]
        fixed = fix_whitespace_errors(content, errors)
        self.assertEqual(fixed, b"line1\nline2\n")

    def test_fix_cr_at_eol(self) -> None:
        """Test fixing carriage returns."""
        content = b"line1\r\nline2\r\nline3\r"
        errors = [("cr-at-eol", 1), ("cr-at-eol", 2), ("cr-at-eol", 3)]
        fixed = fix_whitespace_errors(content, errors)
        # Our fix function removes all CRs when cr-at-eol errors are fixed
        self.assertEqual(fixed, b"line1\nline2\nline3")

    def test_fix_specific_types(self) -> None:
        """Test fixing only specific error types."""
        content = b"line1  \nline2\n\n\n"
        errors = [("blank-at-eol", 1), ("blank-at-eof", 4)]

        # Fix only blank-at-eol
        fixed = fix_whitespace_errors(content, errors, fix_types={"blank-at-eol"})
        self.assertEqual(fixed, b"line1\nline2\n\n\n")

        # Fix only blank-at-eof
        fixed = fix_whitespace_errors(content, errors, fix_types={"blank-at-eof"})
        self.assertEqual(fixed, b"line1  \nline2\n")

    def test_fix_no_errors(self) -> None:
        """Test fixing with no errors returns original content."""
        content = b"line1\nline2\nline3"
        fixed = fix_whitespace_errors(content, [])
        self.assertEqual(fixed, content)
