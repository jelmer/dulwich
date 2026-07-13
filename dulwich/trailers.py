# trailers.py -- Git trailers parsing and manipulation
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

"""Git trailers parsing and manipulation.

This module provides functionality for parsing and manipulating Git trailers,
which are structured information blocks appended to commit messages.

Trailers follow the format:
    Token: value
    Token: value

They are similar to RFC 822 email headers and appear at the end of commit
messages after free-form content.
"""

__all__ = [
    "Trailer",
    "add_trailer_to_message",
    "format_trailers",
    "parse_trailers",
]


class Trailer:
    """Represents a single Git trailer.

    Args:
        key: The trailer key/token (e.g., "Signed-off-by")
        value: The trailer value
        separator: The separator character used (default ':')
    """

    def __init__(self, key: str, value: str, separator: str = ":") -> None:
        """Initialize a Trailer instance.

        Args:
            key: The trailer key/token
            value: The trailer value
            separator: The separator character (default ':')
        """
        self.key = key
        self.value = value
        self.separator = separator

    def __eq__(self, other: object) -> bool:
        """Compare two Trailer instances for equality.

        Args:
            other: The object to compare with

        Returns:
            True if trailers have the same key, value, and separator
        """
        if not isinstance(other, Trailer):
            return NotImplemented
        return (
            self.key == other.key
            and self.value == other.value
            and self.separator == other.separator
        )

    def __repr__(self) -> str:
        """Return a string representation suitable for debugging.

        Returns:
            A string showing the trailer's key, value, and separator
        """
        return f"Trailer(key={self.key!r}, value={self.value!r}, separator={self.separator!r})"

    def __str__(self) -> str:
        """Return the trailer formatted as it would appear in a commit message.

        Returns:
            The trailer in the format "key: value"
        """
        return f"{self.key}{self.separator} {self.value}"


def parse_trailers(
    message: bytes,
    separators: str = ":",
) -> tuple[bytes, list[Trailer]]:
    """Parse trailers from a commit message.

    Trailers are extracted from the input by looking for a group of one or more
    lines that (i) is all trailers, or (ii) contains at least one Git-generated
    or user-configured trailer and consists of at least 25% trailers.

    The group must be preceded by one or more empty (or whitespace-only) lines.
    The group must either be at the end of the input or be the last non-whitespace
    lines before a line that starts with '---'.

    Args:
        message: The commit message as bytes
        separators: Characters to recognize as trailer separators (default ':')

    Returns:
        A tuple of (message_without_trailers, list_of_trailers)
    """
    if not message:
        return (b"", [])

    # Decode message
    try:
        text = message.decode("utf-8")
    except UnicodeDecodeError:
        text = message.decode("latin-1")

    lines = text.splitlines(keepends=True)

    # Find the trailer block by searching backwards
    # Look for a blank line followed by trailer-like lines
    trailer_start = None
    cutoff_line = None

    # First, check if there's a "---" line that marks the end of the message
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].lstrip().startswith("---"):
            cutoff_line = i
            break

    # Determine the search range
    search_end = cutoff_line if cutoff_line is not None else len(lines)

    # Find the trailer block by scanning backward from the last non-blank line.
    # A trailer block is a run of trailer/continuation/blank lines that contains
    # at least one trailer and is preceded by a blank line. Because the block
    # only grows as the scan moves up, a non-blank line that is neither a
    # continuation nor a trailer invalidates every larger block, so the scan can
    # stop at it. Doing this in one pass keeps parsing linear in the message
    # size; the previous version re-sliced and re-checked the tail for every
    # blank line, which is cubic on a message made mostly of blank lines.
    content_end = search_end
    while content_end > 0 and not lines[content_end - 1].strip():
        content_end -= 1

    has_trailer = False
    for k in range(content_end - 1, 0, -1):
        line = lines[k].rstrip()
        if line and not line[0].isspace():
            # A non-blank, non-continuation line must be a trailer, otherwise no
            # block reaching this line (or starting above it) can be valid.
            if not _is_trailer_line(line, separators):
                break
            has_trailer = True
        # Blank and continuation lines are allowed inside the block. Stop at the
        # first preceding blank line once a trailer has been seen.
        if has_trailer and not lines[k - 1].strip():
            trailer_start = k
            break

    if trailer_start is None:
        # No trailer block found
        return (message, [])

    # Parse the trailers
    trailer_lines = lines[trailer_start:search_end]
    trailers = _parse_trailer_lines(trailer_lines, separators)

    # Reconstruct the message without trailers
    # Keep everything before the blank line that precedes the trailers
    message_lines = lines[:trailer_start]

    # Remove trailing blank lines from the message
    while message_lines and not message_lines[-1].strip():
        message_lines.pop()

    message_without_trailers = "".join(message_lines)
    if message_without_trailers and not message_without_trailers.endswith("\n"):
        message_without_trailers += "\n"

    return (message_without_trailers.encode("utf-8"), trailers)


def _is_trailer_line(line: str, separators: str) -> bool:
    """Check if a single line is a trailer line.

    A trailer line contains one of the separator characters and the token
    before the first separator is non-empty and free of whitespace.

    Args:
        line: The line to check (trailing whitespace already removed)
        separators: Valid separator characters

    Returns:
        True if the line is a trailer line
    """
    for sep in separators:
        if sep in line:
            key_part = line.split(sep, 1)[0]
            # Key must not contain whitespace
            if key_part and not any(c.isspace() for c in key_part):
                return True
    return False


def _parse_trailer_lines(lines: list[str], separators: str) -> list[Trailer]:
    """Parse individual trailer lines.

    Args:
        lines: The trailer lines to parse
        separators: Valid separator characters

    Returns:
        List of parsed Trailer objects
    """
    trailers: list[Trailer] = []
    current_trailer: Trailer | None = None

    for line in lines:
        stripped = line.rstrip()

        if not stripped:
            # Empty line - finalize current trailer if any
            if current_trailer:
                trailers.append(current_trailer)
                current_trailer = None
            continue

        # Check if this is a continuation line (starts with whitespace)
        if stripped[0].isspace():
            if current_trailer:
                # Append to the current trailer value
                continuation = stripped.lstrip()
                current_trailer.value += " " + continuation
            continue

        # Finalize the previous trailer if any
        if current_trailer:
            trailers.append(current_trailer)
            current_trailer = None

        # Try to parse as a new trailer
        for sep in separators:
            if sep in stripped:
                parts = stripped.split(sep, 1)
                key = parts[0]

                # Key must not contain whitespace
                if key and not any(c.isspace() for c in key):
                    value = parts[1].strip() if len(parts) > 1 else ""
                    current_trailer = Trailer(key, value, sep)
                    break

    # Don't forget the last trailer
    if current_trailer:
        trailers.append(current_trailer)

    return trailers


def format_trailers(trailers: list[Trailer]) -> bytes:
    """Format a list of trailers as bytes.

    Args:
        trailers: List of Trailer objects

    Returns:
        Formatted trailers as bytes
    """
    if not trailers:
        return b""

    lines = [str(trailer) for trailer in trailers]
    return "\n".join(lines).encode("utf-8") + b"\n"


def add_trailer_to_message(
    message: bytes,
    key: str,
    value: str,
    separator: str = ":",
    where: str = "end",
    if_exists: str = "addIfDifferentNeighbor",
    if_missing: str = "add",
) -> bytes:
    """Add a trailer to a commit message.

    Args:
        message: The original commit message
        key: The trailer key
        value: The trailer value
        separator: The separator to use
        where: Where to add the trailer ('end', 'start', 'after', 'before')
        if_exists: How to handle existing trailers with the same key
            - 'add': Always add
            - 'replace': Replace all existing
            - 'addIfDifferent': Add only if value is different from all existing
            - 'addIfDifferentNeighbor': Add only if value differs from neighbors
            - 'doNothing': Don't add if key exists
        if_missing: What to do if the key doesn't exist
            - 'add': Add the trailer
            - 'doNothing': Don't add the trailer

    Returns:
        The message with the trailer added
    """
    message_body, existing_trailers = parse_trailers(message, separator)

    new_trailer = Trailer(key, value, separator)

    # Check if the key exists
    key_exists = any(t.key == key for t in existing_trailers)

    if not key_exists:
        if if_missing == "doNothing":
            return message
        # Add the new trailer
        updated_trailers = [*existing_trailers, new_trailer]
    else:
        # Key exists - apply if_exists logic
        if if_exists == "doNothing":
            return message
        elif if_exists == "replace":
            # Replace all trailers with this key
            updated_trailers = [t for t in existing_trailers if t.key != key]
            updated_trailers.append(new_trailer)
        elif if_exists == "addIfDifferent":
            # Add only if no existing trailer has the same value
            has_same_value = any(
                t.key == key and t.value == value for t in existing_trailers
            )
            if has_same_value:
                return message
            updated_trailers = [*existing_trailers, new_trailer]
        elif if_exists == "addIfDifferentNeighbor":
            # Add only if adjacent trailers with same key have different values
            should_add = True

            # Check if there's a neighboring trailer with the same key and value
            for i, t in enumerate(existing_trailers):
                if t.key == key and t.value == value:
                    # Check if it's a neighbor (last trailer with this key)
                    is_neighbor = True
                    for j in range(i + 1, len(existing_trailers)):
                        if existing_trailers[j].key == key:
                            is_neighbor = False
                            break
                    if is_neighbor:
                        should_add = False
                        break

            if not should_add:
                return message
            updated_trailers = [*existing_trailers, new_trailer]
        else:  # 'add'
            updated_trailers = [*existing_trailers, new_trailer]

    # Apply where logic
    if where == "start":
        updated_trailers = [new_trailer] + [
            t for t in updated_trailers if t != new_trailer
        ]
    elif where == "before":
        # Insert before the first trailer with the same key
        result = []
        inserted = False
        for t in updated_trailers:
            if not inserted and t.key == key and t != new_trailer:
                result.append(new_trailer)
                inserted = True
            if t != new_trailer:
                result.append(t)
        if not inserted:
            result.append(new_trailer)
        updated_trailers = result
    elif where == "after":
        # Insert after the last trailer with the same key
        result = []
        last_key_index = -1
        for i, t in enumerate(updated_trailers):
            if t.key == key and t != new_trailer:
                last_key_index = len(result)
            if t != new_trailer:
                result.append(t)

        if last_key_index >= 0:
            result.insert(last_key_index + 1, new_trailer)
        else:
            result.append(new_trailer)
        updated_trailers = result
    # 'end' is the default - trailer is already at the end

    # Reconstruct the message
    result_message = message_body
    if result_message and not result_message.endswith(b"\n"):
        result_message += b"\n"

    if updated_trailers:
        result_message += b"\n"
        result_message += format_trailers(updated_trailers)

    return result_message
