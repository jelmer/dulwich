# attrs.py -- Git attributes for dulwich
# Copyright (C) 2019-2020 Collabora Ltd
# Copyright (C) 2019-2020 Andrej Shadura <andrew.shadura@collabora.co.uk>
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

"""Parse .gitattributes file."""

__all__ = [
    "AttributeValue",
    "GitAttributes",
    "Pattern",
    "match_path",
    "parse_git_attributes",
    "parse_gitattributes_file",
    "read_gitattributes",
]

import os
import re
from collections.abc import Generator, Iterator, Mapping, Sequence
from typing import IO

AttributeValue = bytes | bool | None


def _parse_attr(attr: bytes) -> tuple[bytes, AttributeValue]:
    """Parse a git attribute into its value.

    >>> _parse_attr(b'attr')
    (b'attr', True)
    >>> _parse_attr(b'-attr')
    (b'attr', False)
    >>> _parse_attr(b'!attr')
    (b'attr', None)
    >>> _parse_attr(b'attr=text')
    (b'attr', b'text')
    """
    if attr.startswith(b"!"):
        return attr[1:], None
    if attr.startswith(b"-"):
        return attr[1:], False
    if b"=" not in attr:
        return attr, True
    # Split only on first = to handle values with = in them
    name, _, value = attr.partition(b"=")
    return name, value


def parse_git_attributes(
    f: IO[bytes],
) -> Generator[tuple[bytes, Mapping[bytes, AttributeValue]], None, None]:
    """Parse a Git attributes string.

    Args:
      f: File-like object to read bytes from
    Returns:
      List of patterns and corresponding patterns in the order or them being encountered
    >>> from io import BytesIO
    >>> list(parse_git_attributes(BytesIO(b'''*.tar.* filter=lfs diff=lfs merge=lfs -text
    ...
    ... # store signatures in Git
    ... *.tar.*.asc -filter -diff merge=binary -text
    ...
    ... # store .dsc verbatim
    ... *.dsc -filter !diff merge=binary !text
    ... '''))) #doctest: +NORMALIZE_WHITESPACE
    [(b'*.tar.*', {'filter': 'lfs', 'diff': 'lfs', 'merge': 'lfs', 'text': False}),
     (b'*.tar.*.asc', {'filter': False, 'diff': False, 'merge': 'binary', 'text': False}),
     (b'*.dsc', {'filter': False, 'diff': None, 'merge': 'binary', 'text': None})]
    """
    for line in f:
        line = line.strip()

        # Ignore blank lines, they're used for readability.
        if not line:
            continue

        if line.startswith(b"#"):
            # Comment
            continue

        pattern, *attrs = line.split()

        yield (pattern, {k: v for k, v in (_parse_attr(a) for a in attrs)})


def _translate_pattern(pattern: bytes) -> bytes:
    """Translate a gitattributes pattern to a regular expression.

    Similar to gitignore patterns, but simpler as gitattributes doesn't support
    all the same features (e.g., no directory-only patterns with trailing /).
    """
    res = b""
    i = 0
    n = len(pattern)

    # If pattern doesn't contain /, it can match at any level
    if b"/" not in pattern:
        res = b"(?:.*/)??"
    elif pattern.startswith(b"/"):
        # Leading / means root of repository
        pattern = pattern[1:]
        n = len(pattern)

    while i < n:
        c = pattern[i : i + 1]
        i += 1

        if c == b"*":
            if i < n and pattern[i : i + 1] == b"*":
                # Double asterisk
                i += 1
                if i < n and pattern[i : i + 1] == b"/":
                    # **/ - match zero or more directories
                    res += b"(?:.*/)??"
                    i += 1
                elif i == n:
                    # ** at end - match everything
                    res += b".*"
                else:
                    # ** in middle
                    res += b".*"
            else:
                # Single * - match any character except /
                res += b"[^/]*"
        elif c == b"?":
            res += b"[^/]"
        elif c == b"[":
            # Character class
            j = i
            if j < n and pattern[j : j + 1] == b"!":
                j += 1
            if j < n and pattern[j : j + 1] == b"]":
                j += 1
            while j < n and pattern[j : j + 1] != b"]":
                j += 1
            if j >= n:
                res += b"\\["
            else:
                stuff = pattern[i:j].replace(b"\\", b"\\\\")
                i = j + 1
                if stuff.startswith(b"!"):
                    stuff = b"^" + stuff[1:]
                elif stuff.startswith(b"^"):
                    stuff = b"\\" + stuff
                res += b"[" + stuff + b"]"
        else:
            res += re.escape(c)

    return res


class Pattern:
    """A single gitattributes pattern."""

    def __init__(self, pattern: bytes):
        """Initialize GitAttributesPattern.

        Args:
            pattern: Attribute pattern as bytes
        """
        self.pattern = pattern
        self._regex: re.Pattern[bytes] | None = None
        self._compile()

    def _compile(self) -> None:
        """Compile the pattern to a regular expression."""
        regex_pattern = _translate_pattern(self.pattern)
        # Add anchors
        regex_pattern = b"^" + regex_pattern + b"$"
        self._regex = re.compile(regex_pattern)

    def match(self, path: bytes) -> bool:
        """Check if path matches this pattern.

        Args:
            path: Path to check (relative to repository root, using / separators)

        Returns:
            True if path matches this pattern
        """
        # Normalize path
        if path.startswith(b"/"):
            path = path[1:]

        # Try to match
        assert self._regex is not None  # Always set by _compile()
        return bool(self._regex.match(path))


def match_path(
    patterns: Sequence[tuple[Pattern, Mapping[bytes, AttributeValue]]], path: bytes
) -> dict[bytes, AttributeValue]:
    """Get attributes for a path by matching against patterns.

    Args:
        patterns: List of (Pattern, attributes) tuples
        path: Path to match (relative to repository root)

    Returns:
        Dictionary of attributes that apply to this path
    """
    attributes: dict[bytes, AttributeValue] = {}

    # Later patterns override earlier ones
    for pattern, attrs in patterns:
        if pattern.match(path):
            # Update attributes
            for name, value in attrs.items():
                if value is None:
                    # Unspecified - remove the attribute
                    attributes.pop(name, None)
                else:
                    attributes[name] = value

    return attributes


def parse_gitattributes_file(
    filename: str | bytes,
) -> list[tuple[Pattern, Mapping[bytes, AttributeValue]]]:
    """Parse a gitattributes file and return compiled patterns.

    Args:
        filename: Path to the .gitattributes file

    Returns:
        List of (Pattern, attributes) tuples
    """
    patterns = []

    if isinstance(filename, str):
        filename = filename.encode("utf-8")

    with open(filename, "rb") as f:
        for pattern_bytes, attrs in parse_git_attributes(f):
            pattern = Pattern(pattern_bytes)
            patterns.append((pattern, attrs))

    return patterns


def read_gitattributes(
    path: str | bytes,
) -> list[tuple[Pattern, Mapping[bytes, AttributeValue]]]:
    """Read .gitattributes from a directory.

    Args:
        path: Directory path to check for .gitattributes

    Returns:
        List of (Pattern, attributes) tuples
    """
    if isinstance(path, bytes):
        path = path.decode("utf-8")

    gitattributes_path = os.path.join(path, ".gitattributes")
    if os.path.exists(gitattributes_path):
        return parse_gitattributes_file(gitattributes_path)

    return []


class GitAttributes:
    """A collection of gitattributes patterns that can match paths."""

    def __init__(
        self,
        patterns: list[tuple[Pattern, Mapping[bytes, AttributeValue]]] | None = None,
    ):
        """Initialize GitAttributes.

        Args:
            patterns: Optional list of (Pattern, attributes) tuples
        """
        self._patterns = patterns or []

    def match_path(self, path: bytes) -> dict[bytes, AttributeValue]:
        """Get attributes for a path by matching against patterns.

        Args:
            path: Path to match (relative to repository root)

        Returns:
            Dictionary of attributes that apply to this path
        """
        return match_path(self._patterns, path)

    def add_patterns(
        self, patterns: Sequence[tuple[Pattern, Mapping[bytes, AttributeValue]]]
    ) -> None:
        """Add patterns to the collection.

        Args:
            patterns: List of (Pattern, attributes) tuples to add
        """
        self._patterns.extend(patterns)

    def __len__(self) -> int:
        """Return the number of patterns."""
        return len(self._patterns)

    def __iter__(self) -> Iterator[tuple["Pattern", Mapping[bytes, AttributeValue]]]:
        """Iterate over patterns."""
        return iter(self._patterns)

    @classmethod
    def from_file(cls, filename: str | bytes) -> "GitAttributes":
        """Create GitAttributes from a gitattributes file.

        Args:
            filename: Path to the .gitattributes file

        Returns:
            New GitAttributes instance
        """
        patterns = parse_gitattributes_file(filename)
        return cls(patterns)

    @classmethod
    def from_path(cls, path: str | bytes) -> "GitAttributes":
        """Create GitAttributes from .gitattributes in a directory.

        Args:
            path: Directory path to check for .gitattributes

        Returns:
            New GitAttributes instance
        """
        patterns = read_gitattributes(path)
        return cls(patterns)

    def set_attribute(self, pattern: bytes, name: bytes, value: AttributeValue) -> None:
        """Set an attribute for a pattern.

        Args:
            pattern: The file pattern
            name: Attribute name
            value: Attribute value (bytes, True, False, or None)
        """
        # Find existing pattern
        pattern_obj = None
        attrs_dict: dict[bytes, AttributeValue] | None = None
        pattern_index = -1

        for i, (p, attrs) in enumerate(self._patterns):
            if p.pattern == pattern:
                pattern_obj = p
                # Convert to mutable dict
                attrs_dict = dict(attrs)
                pattern_index = i
                break

        if pattern_obj is None:
            # Create new pattern
            pattern_obj = Pattern(pattern)
            attrs_dict = {name: value}
            self._patterns.append((pattern_obj, attrs_dict))
        else:
            # Update the existing pattern in the list
            assert pattern_index >= 0
            assert attrs_dict is not None
            self._patterns[pattern_index] = (pattern_obj, attrs_dict)

        # Update the attribute
        if attrs_dict is None:
            raise AssertionError("attrs_dict should not be None at this point")
        attrs_dict[name] = value

    def remove_pattern(self, pattern: bytes) -> None:
        """Remove all attributes for a pattern.

        Args:
            pattern: The file pattern to remove
        """
        self._patterns = [
            (p, attrs) for p, attrs in self._patterns if p.pattern != pattern
        ]

    def to_bytes(self) -> bytes:
        """Convert GitAttributes to bytes format suitable for writing to file.

        Returns:
            Bytes representation of the gitattributes file
        """
        lines = []
        for pattern_obj, attrs in self._patterns:
            pattern = pattern_obj.pattern
            attr_strs = []

            for name, value in sorted(attrs.items()):
                if value is True:
                    attr_strs.append(name)
                elif value is False:
                    attr_strs.append(b"-" + name)
                elif value is None:
                    attr_strs.append(b"!" + name)
                else:
                    # value is bytes
                    attr_strs.append(name + b"=" + value)

            if attr_strs:
                line = pattern + b" " + b" ".join(attr_strs)
                lines.append(line)

        return b"\n".join(lines) + b"\n" if lines else b""

    def write_to_file(self, filename: str | bytes) -> None:
        """Write GitAttributes to a file.

        Args:
            filename: Path to write the .gitattributes file
        """
        if isinstance(filename, str):
            filename = filename.encode("utf-8")

        content = self.to_bytes()
        with open(filename, "wb") as f:
            f.write(content)
