# whitespace.py -- Whitespace error detection and fixing
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
"""Whitespace error detection and fixing functionality.

This module implements Git's core.whitespace configuration and related
whitespace error detection capabilities.
"""

__all__ = [
    "DEFAULT_WHITESPACE_ERRORS",
    "WHITESPACE_ERROR_TYPES",
    "WhitespaceChecker",
    "fix_whitespace_errors",
    "parse_whitespace_config",
]

from collections.abc import Sequence, Set

# Default whitespace errors Git checks for
DEFAULT_WHITESPACE_ERRORS = {
    "blank-at-eol",
    "space-before-tab",
    "blank-at-eof",
}

# All available whitespace error types
WHITESPACE_ERROR_TYPES = {
    "blank-at-eol",  # Trailing whitespace at end of line
    "space-before-tab",  # Space before tab in indentation
    "indent-with-non-tab",  # Indent with space when tabs expected (8+ spaces)
    "tab-in-indent",  # Tab in indentation when spaces expected
    "blank-at-eof",  # Blank lines at end of file
    "trailing-space",  # Trailing whitespace (same as blank-at-eol)
    "cr-at-eol",  # Carriage return at end of line
    "tabwidth",  # Special: sets tab width (not an error type)
}


def parse_whitespace_config(value: str | None) -> tuple[set[str], int]:
    """Parse core.whitespace configuration value.

    Args:
        value: The core.whitespace config value (e.g., "blank-at-eol,space-before-tab")

    Returns:
        Tuple of (enabled error types, tab width)
    """
    if value is None:
        return DEFAULT_WHITESPACE_ERRORS.copy(), 8

    if value == "":
        return set(), 8

    # Start with defaults if no explicit errors are specified or if negation is used
    parts = value.split(",")
    has_negation = any(p.strip().startswith("-") for p in parts)
    has_explicit_errors = any(p.strip() in WHITESPACE_ERROR_TYPES for p in parts)

    if has_negation or not has_explicit_errors:
        enabled = DEFAULT_WHITESPACE_ERRORS.copy()
    else:
        enabled = set()

    tab_width = 8

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Handle negation
        if part.startswith("-"):
            error_type = part[1:]
            if error_type in WHITESPACE_ERROR_TYPES:
                enabled.discard(error_type)
        elif part.startswith("tabwidth="):
            try:
                tab_width = int(part[9:])
                if tab_width < 1:
                    tab_width = 8
            except ValueError:
                tab_width = 8
        elif part in WHITESPACE_ERROR_TYPES:
            enabled.add(part)

    # Handle aliases
    if "trailing-space" in enabled:
        enabled.add("blank-at-eol")
        enabled.discard("trailing-space")

    return enabled, tab_width


class WhitespaceChecker:
    """Checks for whitespace errors in text content."""

    def __init__(self, enabled_errors: set[str], tab_width: int = 8):
        """Initialize whitespace checker.

        Args:
            enabled_errors: Set of error types to check for
            tab_width: Width of tab character for indentation checking
        """
        self.enabled_errors = enabled_errors
        self.tab_width = tab_width

    def check_line(self, line: bytes, line_num: int) -> list[tuple[str, int]]:
        """Check a single line for whitespace errors.

        Args:
            line: Line content (without newline)
            line_num: Line number (1-based)

        Returns:
            List of (error_type, line_number) tuples
        """
        errors = []

        # Check for trailing whitespace (blank-at-eol)
        if "blank-at-eol" in self.enabled_errors:
            if line and (line[-1:] == b" " or line[-1:] == b"\t"):
                # Find where trailing whitespace starts
                i = len(line) - 1
                while i >= 0 and line[i : i + 1] in (b" ", b"\t"):
                    i -= 1
                errors.append(("blank-at-eol", line_num))

        # Check for space before tab
        if "space-before-tab" in self.enabled_errors:
            # Check in indentation
            i = 0
            while i < len(line) and line[i : i + 1] in (b" ", b"\t"):
                if i > 0 and line[i - 1 : i] == b" " and line[i : i + 1] == b"\t":
                    errors.append(("space-before-tab", line_num))
                    break
                i += 1

        # Check for indent-with-non-tab (8+ spaces at start)
        if "indent-with-non-tab" in self.enabled_errors:
            space_count = 0
            for i in range(len(line)):
                if line[i : i + 1] == b" ":
                    space_count += 1
                    if space_count >= self.tab_width:
                        errors.append(("indent-with-non-tab", line_num))
                        break
                elif line[i : i + 1] == b"\t":
                    space_count = 0  # Reset on tab
                else:
                    break  # Non-whitespace character

        # Check for tab-in-indent
        if "tab-in-indent" in self.enabled_errors:
            for i in range(len(line)):
                if line[i : i + 1] == b"\t":
                    errors.append(("tab-in-indent", line_num))
                    break
                elif line[i : i + 1] not in (b" ", b"\t"):
                    break  # Non-whitespace character

        # Check for carriage return
        if "cr-at-eol" in self.enabled_errors:
            if line and line[-1:] == b"\r":
                errors.append(("cr-at-eol", line_num))

        return errors

    def check_content(self, content: bytes) -> list[tuple[str, int]]:
        """Check content for whitespace errors.

        Args:
            content: File content to check

        Returns:
            List of (error_type, line_number) tuples
        """
        errors = []
        lines = content.split(b"\n")

        # Handle CRLF line endings
        for i, line in enumerate(lines):
            if line.endswith(b"\r"):
                lines[i] = line[:-1]

        # Check each line
        for i, line in enumerate(lines):
            errors.extend(self.check_line(line, i + 1))

        # Check for blank lines at end of file
        if "blank-at-eof" in self.enabled_errors:
            # Skip the last empty line if content ends with newline
            check_lines = lines[:-1] if lines and lines[-1] == b"" else lines

            if check_lines:
                trailing_blank_count = 0
                for i in range(len(check_lines) - 1, -1, -1):
                    if check_lines[i] == b"":
                        trailing_blank_count += 1
                    else:
                        break

                if trailing_blank_count > 0:
                    # Report the line number of the last non-empty line + 1
                    errors.append(("blank-at-eof", len(check_lines) + 1))

        return errors


def fix_whitespace_errors(
    content: bytes,
    errors: Sequence[tuple[str, int]],
    fix_types: Set[str] | None = None,
) -> bytes:
    """Fix whitespace errors in content.

    Args:
        content: Original content
        errors: List of errors from WhitespaceChecker
        fix_types: Set of error types to fix (None means fix all)

    Returns:
        Fixed content
    """
    if not errors:
        return content

    lines = content.split(b"\n")

    # Handle CRLF line endings - we need to track which lines had them
    has_crlf = []
    for i, line in enumerate(lines):
        if line.endswith(b"\r"):
            has_crlf.append(i)
            lines[i] = line[:-1]

    # Group errors by line
    errors_by_line: dict[int, list[str]] = {}
    for error_type, line_num in errors:
        if fix_types is None or error_type in fix_types:
            if line_num not in errors_by_line:
                errors_by_line[line_num] = []
            errors_by_line[line_num].append(error_type)

    # Fix errors
    for line_num, error_types in errors_by_line.items():
        if line_num > len(lines):
            continue

        line_idx = line_num - 1
        line = lines[line_idx]

        # Fix trailing whitespace
        if "blank-at-eol" in error_types:
            # Remove trailing spaces and tabs
            while line and line[-1:] in (b" ", b"\t"):
                line = line[:-1]
            lines[line_idx] = line

        # Fix carriage return - since we already stripped CRs, we just don't restore them
        if "cr-at-eol" in error_types and line_idx in has_crlf:
            has_crlf.remove(line_idx)

    # Restore CRLF for lines that should keep them
    for idx in has_crlf:
        if idx < len(lines):
            lines[idx] = lines[idx] + b"\r"

    # Fix blank lines at end of file
    if fix_types is None or "blank-at-eof" in fix_types:
        # Remove trailing empty lines
        while len(lines) > 1 and lines[-1] == b"" and lines[-2] == b"":
            lines.pop()

    return b"\n".join(lines)
