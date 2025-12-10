# stripspace.py -- Git stripspace functionality
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Git stripspace functionality for cleaning up text and commit messages."""

__all__ = [
    "stripspace",
]


def stripspace(
    text: bytes,
    *,
    strip_comments: bool = False,
    comment_char: bytes = b"#",
    comment_lines: bool = False,
) -> bytes:
    """Strip unnecessary whitespace from text.

    This function mimics the behavior of ``git stripspace``, which is commonly
    used to clean up commit messages and other text content.

    Args:
      text: The text to process (as bytes)
      strip_comments: If True, remove lines that begin with comment_char
      comment_char: The comment character to use (default: b"#")
      comment_lines: If True, prepend comment_char to each line

    Returns:
      The processed text as bytes

    The function performs the following operations (in order):
      1. If comment_lines is True, prepend comment_char + space to each line
      2. Strip trailing whitespace from each line
      3. If strip_comments is True, remove lines starting with comment_char
      4. Collapse multiple consecutive blank lines into a single blank line
      5. Remove leading blank lines
      6. Remove trailing blank lines
      7. Ensure the text ends with a newline (unless empty)
    """
    if not text:
        return b""

    # Split into lines (preserving line endings for processing)
    lines = text.splitlines(keepends=True)

    # Step 1 & 2: Strip leading and trailing whitespace from each line (but keep newlines)
    processed_lines = []
    for line in lines:
        # Determine line ending
        line_ending = b""
        if line.endswith(b"\r\n"):
            line_ending = b"\r\n"
        elif line.endswith(b"\n"):
            line_ending = b"\n"
        elif line.endswith(b"\r"):
            line_ending = b"\r"

        # Strip all whitespace from the line content
        stripped_content = line.rstrip(b"\r\n\t ").lstrip()

        # If comment_lines is True, prepend comment char to non-empty lines only
        if comment_lines and stripped_content:
            stripped_content = comment_char + b" " + stripped_content

        # Reassemble with line ending
        processed_lines.append(stripped_content + line_ending)

    # Step 3: Strip comments if requested
    if strip_comments:
        processed_lines = [
            line
            for line in processed_lines
            if not line.lstrip().startswith(comment_char)
        ]

    if not processed_lines:
        return b""

    # Step 4 & 5 & 6: Collapse multiple blank lines, remove leading/trailing blank lines
    # First, identify blank lines (lines that are just whitespace/newline)
    def is_blank(line: bytes) -> bool:
        return line.strip() == b""

    # Remove leading blank lines
    while processed_lines and is_blank(processed_lines[0]):
        processed_lines.pop(0)

    # Remove trailing blank lines
    while processed_lines and is_blank(processed_lines[-1]):
        processed_lines.pop()

    # Collapse consecutive blank lines
    collapsed_lines = []
    prev_was_blank = False
    for line in processed_lines:
        is_current_blank = is_blank(line)
        if is_current_blank and prev_was_blank:
            # Skip this blank line
            continue
        collapsed_lines.append(line)
        prev_was_blank = is_current_blank

    if not collapsed_lines:
        return b""

    # Step 7: Ensure text ends with newline
    # Join all lines together
    result = b"".join(collapsed_lines)

    # If the result doesn't end with a newline, add one
    if not result.endswith((b"\n", b"\r\n", b"\r")):
        result += b"\n"

    return result
