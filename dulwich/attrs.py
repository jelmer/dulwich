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
    "compile_gitattributes_patterns",
    "match_path",
    "parse_git_attributes",
    "parse_gitattributes_file",
    "read_gitattributes",
]

import errno
import logging
import os
import re
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping, Sequence
from itertools import chain
from typing import IO

from .file import open_nofollow_read
from .wildmatch import MalformedPattern
from .wildmatch import translate as translate_wildmatch

logger = logging.getLogger(__name__)

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


def _translate_pattern(pattern: bytes, base: bytes = b"") -> bytes:
    """Translate a gitattributes pattern to a regular expression.

    Similar to gitignore patterns, but simpler as gitattributes doesn't support
    all the same features (e.g., no directory-only patterns with trailing /).

    Args:
      pattern: The pattern to translate
      base: Directory of the .gitattributes file the pattern comes from,
        relative to the repository root; empty for the root

    Raises:
      MalformedPattern: if wildmatch() would refuse the pattern outright;
        see :func:`dulwich.wildmatch.translate`.
    """
    res = re.escape(base + b"/") if base else b""

    # If pattern doesn't contain /, it can match at any level
    if b"/" not in pattern:
        res += b"(?:.*/)??"
    elif pattern.startswith(b"/"):
        # Leading / means the directory of the .gitattributes file
        pattern = pattern[1:]

    return res + translate_wildmatch(pattern)


class Pattern:
    """A single gitattributes pattern."""

    def __init__(self, pattern: bytes, base: bytes = b""):
        """Initialize GitAttributesPattern.

        Args:
            pattern: Attribute pattern as bytes
            base: Directory of the .gitattributes file the pattern comes
              from, relative to the repository root; empty for the root
        """
        self.pattern = pattern
        self.base = base
        self._regex: re.Pattern[bytes] | None = None
        self._compile()

    def _compile(self) -> None:
        """Compile the pattern to a regular expression."""
        regex_pattern = _translate_pattern(self.pattern, self.base)
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
    patterns: Iterable[tuple[Pattern, Mapping[bytes, AttributeValue]]], path: bytes
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


def compile_gitattributes_patterns(
    entries: Iterable[tuple[bytes, Mapping[bytes, AttributeValue]]],
    source: str | bytes = b"<attributes>",
    base: bytes = b"",
) -> list[tuple[Pattern, Mapping[bytes, AttributeValue]]]:
    """Compile parsed gitattributes entries, skipping malformed patterns.

    Git's wildmatch() treats a malformed pattern as matching nothing rather
    than as a broken file, so one bad line is logged and dropped instead of
    aborting the load.

    Args:
        entries: (pattern, attributes) pairs, as from parse_git_attributes
        source: Where the entries came from, used in the warning
        base: Directory the patterns are relative to, relative to the
          repository root; empty for the root

    Returns:
        List of (Pattern, attributes) tuples
    """
    patterns = []
    for pattern_bytes, attrs in entries:
        try:
            pattern = Pattern(pattern_bytes, base)
        except MalformedPattern:
            logger.warning("Ignoring malformed pattern %r in %r", pattern_bytes, source)
            continue
        patterns.append((pattern, attrs))
    return patterns


def parse_gitattributes_file(
    filename: str | bytes,
) -> list[tuple[Pattern, Mapping[bytes, AttributeValue]]]:
    """Parse a gitattributes file and return compiled patterns.

    A malformed pattern is logged and skipped rather than raised, so one bad
    line doesn't stop the rest of the file from loading.

    Args:
        filename: Path to the .gitattributes file

    Returns:
        List of (Pattern, attributes) tuples
    """
    if isinstance(filename, str):
        filename = filename.encode("utf-8")

    with open(filename, "rb") as f:
        return compile_gitattributes_patterns(parse_git_attributes(f), filename)


def read_gitattributes(
    path: str | bytes,
) -> list[tuple[Pattern, Mapping[bytes, AttributeValue]]]:
    """Read .gitattributes from a directory.

    A ``.gitattributes`` that is a symlink is skipped with a warning rather
    than followed, as git does.

    Args:
        path: Directory path to check for .gitattributes

    Returns:
        List of (Pattern, attributes) tuples
    """
    if isinstance(path, bytes):
        path = path.decode("utf-8")

    gitattributes_path = os.path.join(path, ".gitattributes")
    try:
        f = open_nofollow_read(gitattributes_path)
    except FileNotFoundError:
        return []
    except OSError as e:
        if e.errno not in (errno.ELOOP, errno.EMLINK):
            raise
        logger.warning("Ignoring %s: it is a symbolic link", gitattributes_path)
        return []
    with f:
        return compile_gitattributes_patterns(
            parse_git_attributes(f), gitattributes_path.encode("utf-8")
        )


class GitAttributes:
    """A collection of gitattributes patterns that can match paths.

    Patterns are applied in the order git uses: ``patterns`` first, then
    those of each directory from the top down to the path's own directory
    (as returned by ``directory_loader``), then ``info_patterns``.
    """

    def __init__(
        self,
        patterns: list[tuple[Pattern, Mapping[bytes, AttributeValue]]] | None = None,
        *,
        directory_loader: Callable[
            [bytes], list[tuple[Pattern, Mapping[bytes, AttributeValue]]]
        ]
        | None = None,
        info_patterns: list[tuple[Pattern, Mapping[bytes, AttributeValue]]]
        | None = None,
    ):
        """Initialize GitAttributes.

        Args:
            patterns: Optional list of (Pattern, attributes) tuples
            directory_loader: Optional callable that returns the patterns
              of the .gitattributes file in a subdirectory, given its path
              relative to the repository root. Called at most once per
              directory, when a path below it is first matched.
            info_patterns: Optional list of (Pattern, attributes) tuples
              that take precedence over all others, as from
              ``$GIT_DIR/info/attributes``
        """
        self._patterns = patterns or []
        self._directory_loader = directory_loader
        self._directory_patterns: dict[
            bytes, list[tuple[Pattern, Mapping[bytes, AttributeValue]]]
        ] = {}
        self._info_patterns = info_patterns or []

    def _get_directory_patterns(
        self, dirpath: bytes
    ) -> list[tuple[Pattern, Mapping[bytes, AttributeValue]]]:
        try:
            return self._directory_patterns[dirpath]
        except KeyError:
            pass
        assert self._directory_loader is not None
        patterns = self._directory_loader(dirpath)
        self._directory_patterns[dirpath] = patterns
        return patterns

    def match_path(self, path: bytes) -> dict[bytes, AttributeValue]:
        """Get attributes for a path by matching against patterns.

        Args:
            path: Path to match (relative to repository root)

        Returns:
            Dictionary of attributes that apply to this path
        """
        directory_patterns: list[
            list[tuple[Pattern, Mapping[bytes, AttributeValue]]]
        ] = []
        if self._directory_loader is not None:
            parts = path.lstrip(b"/").split(b"/")[:-1]
            for i in range(1, len(parts) + 1):
                directory_patterns.append(
                    self._get_directory_patterns(b"/".join(parts[:i]))
                )
        return match_path(
            chain(self._patterns, *directory_patterns, self._info_patterns), path
        )

    def add_patterns(
        self, patterns: Sequence[tuple[Pattern, Mapping[bytes, AttributeValue]]]
    ) -> None:
        """Add patterns to the collection.

        Args:
            patterns: List of (Pattern, attributes) tuples to add
        """
        self._patterns.extend(patterns)

    def __len__(self) -> int:
        """Return the number of patterns.

        Patterns from subdirectories that have not been loaded yet are
        not counted.
        """
        return len(self._patterns) + len(self._info_patterns)

    def __iter__(self) -> Iterator[tuple["Pattern", Mapping[bytes, AttributeValue]]]:
        """Iterate over the top-level patterns and ``info_patterns``.

        Patterns from subdirectories are not included.
        """
        yield from self._patterns
        yield from self._info_patterns

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
