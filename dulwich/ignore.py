# Copyright (C) 2017 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Parsing of gitignore files.

For details for the matching rules, see https://git-scm.com/docs/gitignore

Important: When checking if directories are ignored, include a trailing slash in the path.
For example, use "dir/" instead of "dir" to check if a directory is ignored.
"""

import os.path
import re
from collections.abc import Iterable, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, BinaryIO, Optional, Union

if TYPE_CHECKING:
    from .repo import Repo

from .config import Config, get_xdg_config_home_path


def _pattern_to_str(pattern: Union["Pattern", bytes, str]) -> str:
    """Convert a pattern to string, handling both Pattern objects and raw patterns."""
    if isinstance(pattern, Pattern):
        pattern_data: Union[bytes, str] = pattern.pattern
    else:
        pattern_data = pattern
    return pattern_data.decode() if isinstance(pattern_data, bytes) else pattern_data


def _check_parent_exclusion(path: str, matching_patterns: Sequence["Pattern"]) -> bool:
    """Check if a parent directory exclusion prevents negation patterns from taking effect.

    Args:
        path: Path to check
        matching_patterns: List of Pattern objects that matched the path

    Returns:
        True if parent exclusion applies (negation should be ineffective), False otherwise
    """
    # Find the last negation pattern
    final_negation = next(
        (p for p in reversed(matching_patterns) if not p.is_exclude), None
    )
    if not final_negation:
        return False

    final_pattern_str = _pattern_to_str(final_negation)

    # Check if any exclusion pattern excludes a parent directory
    return any(
        pattern.is_exclude
        and _pattern_excludes_parent(_pattern_to_str(pattern), path, final_pattern_str)
        for pattern in matching_patterns
    )


def _pattern_excludes_parent(
    pattern_str: str, path: str, final_pattern_str: str
) -> bool:
    """Check if a pattern excludes a parent directory of the given path."""
    # Handle **/middle/** patterns
    if pattern_str.startswith("**/") and pattern_str.endswith("/**"):
        middle = pattern_str[3:-3]
        return f"/{middle}/" in f"/{path}" or path.startswith(f"{middle}/")

    # Handle dir/** patterns
    if pattern_str.endswith("/**") and not pattern_str.startswith("**/"):
        base_dir = pattern_str[:-3]
        if not path.startswith(base_dir + "/"):
            return False

        remaining = path[len(base_dir) + 1 :]

        # Special case: dir/** allows immediate child file negations
        if (
            not path.endswith("/")
            and final_pattern_str.startswith("!")
            and "/" not in remaining
        ):
            neg_pattern = final_pattern_str[1:]
            if neg_pattern == path or ("*" in neg_pattern and "**" not in neg_pattern):
                return False

        # Nested files with ** negation patterns
        if "**" in final_pattern_str and Pattern(final_pattern_str[1:].encode()).match(
            path.encode()
        ):
            return False

        return True

    # Directory patterns (ending with /) can exclude parent directories
    if pattern_str.endswith("/") and "/" in path:
        p = Pattern(pattern_str.encode())
        parts = path.split("/")
        return any(
            p.match(("/".join(parts[:i]) + "/").encode()) for i in range(1, len(parts))
        )

    return False


def _translate_segment(segment: bytes) -> bytes:
    """Translate a single path segment to regex, following Git rules exactly."""
    if segment == b"*":
        return b"[^/]+"

    res = b""
    i, n = 0, len(segment)
    while i < n:
        c = segment[i : i + 1]
        i += 1
        if c == b"*":
            res += b"[^/]*"
        elif c == b"?":
            res += b"[^/]"
        elif c == b"\\":
            if i < n:
                res += re.escape(segment[i : i + 1])
                i += 1
            else:
                res += re.escape(c)
        elif c == b"[":
            j = i
            if j < n and segment[j : j + 1] == b"!":
                j += 1
            if j < n and segment[j : j + 1] == b"]":
                j += 1
            while j < n and segment[j : j + 1] != b"]":
                j += 1
            if j >= n:
                res += b"\\["
            else:
                stuff = segment[i:j].replace(b"\\", b"\\\\")
                i = j + 1
                if stuff.startswith(b"!"):
                    stuff = b"^" + stuff[1:]
                elif stuff.startswith(b"^"):
                    stuff = b"\\" + stuff
                res += b"[" + stuff + b"]"
        else:
            res += re.escape(c)
    return res


def _handle_double_asterisk(segments: Sequence[bytes], i: int) -> tuple[bytes, bool]:
    """Handle ** segment processing, returns (regex_part, skip_next)."""
    # Check if ** is at end
    remaining = segments[i + 1 :]
    if all(s == b"" for s in remaining):
        # ** at end - matches everything
        return b".*", False

    # Check if next segment is also **
    if i + 1 < len(segments) and segments[i + 1] == b"**":
        # Consecutive ** segments
        # Check if this ends with a directory pattern (trailing /)
        remaining_after_next = segments[i + 2 :]
        is_dir_pattern = (
            len(remaining_after_next) == 1 and remaining_after_next[0] == b""
        )

        if is_dir_pattern:
            # Pattern like c/**/**/ - requires at least one intermediate directory
            return b"[^/]+/(?:[^/]+/)*", True
        else:
            # Pattern like c/**/**/d - allows zero intermediate directories
            return b"(?:[^/]+/)*", True
    else:
        # ** in middle - handle differently depending on what follows
        if i == 0:
            # ** at start - any prefix
            return b"(?:.*/)??", False
        else:
            # ** in middle - match zero or more complete directory segments
            return b"(?:[^/]+/)*", False


def _handle_leading_patterns(pat: bytes, res: bytes) -> tuple[bytes, bytes]:
    """Handle leading patterns like ``/**/``, ``**/``, or ``/``."""
    if pat.startswith(b"/**/"):
        # Leading /** is same as **
        return pat[4:], b"(.*/)?"
    elif pat.startswith(b"**/"):
        # Leading **/
        return pat[3:], b"(.*/)?"
    elif pat.startswith(b"/"):
        # Leading / means relative to .gitignore location
        return pat[1:], b""
    else:
        return pat, b""


def translate(pat: bytes) -> bytes:
    """Translate a gitignore pattern to a regular expression following Git rules exactly."""
    res = b"(?ms)"

    # Check for invalid patterns with // - Git treats these as broken patterns
    if b"//" in pat:
        # Pattern with // doesn't match anything in Git
        return b"(?!.*)"  # Negative lookahead - matches nothing

    # Don't normalize consecutive ** patterns - Git treats them specially
    # c/**/**/ requires at least one intermediate directory
    # So we keep the pattern as-is

    # Handle patterns with no slashes (match at any level)
    if b"/" not in pat[:-1]:  # No slash except possibly at end
        res += b"(.*/)?"

    # Handle leading patterns
    pat, prefix_added = _handle_leading_patterns(pat, res)
    if prefix_added:
        res += prefix_added

    # Process the rest of the pattern
    if pat == b"**":
        res += b".*"
    else:
        segments = pat.split(b"/")
        i = 0
        while i < len(segments):
            segment = segments[i]

            # Add slash separator (except for first segment)
            if i > 0 and segments[i - 1] != b"**":
                res += re.escape(b"/")

            if segment == b"**":
                regex_part, skip_next = _handle_double_asterisk(segments, i)
                res += regex_part
                if regex_part == b".*":  # End of pattern
                    break
                if skip_next:
                    i += 1
            else:
                res += _translate_segment(segment)

            i += 1

    # Add optional trailing slash for files
    if not pat.endswith(b"/"):
        res += b"/?"

    return res + b"\\Z"


def read_ignore_patterns(f: BinaryIO) -> Iterable[bytes]:
    """Read a git ignore file.

    Args:
      f: File-like object to read from
    Returns: List of patterns
    """
    for line in f:
        line = line.rstrip(b"\r\n")

        # Ignore blank lines, they're used for readability.
        if not line.strip():
            continue

        if line.startswith(b"#"):
            # Comment
            continue

        # Trailing spaces are ignored unless they are quoted with a backslash.
        while line.endswith(b" ") and not line.endswith(b"\\ "):
            line = line[:-1]
        line = line.replace(b"\\ ", b" ")

        yield line


def match_pattern(path: bytes, pattern: bytes, ignorecase: bool = False) -> bool:
    """Match a gitignore-style pattern against a path.

    Args:
      path: Path to match
      pattern: Pattern to match
      ignorecase: Whether to do case-sensitive matching
    Returns:
      bool indicating whether the pattern matched
    """
    return Pattern(pattern, ignorecase).match(path)


class Pattern:
    """A single ignore pattern."""

    def __init__(self, pattern: bytes, ignorecase: bool = False) -> None:
        """Initialize a Pattern object.

        Args:
            pattern: The gitignore pattern as bytes.
            ignorecase: Whether to perform case-insensitive matching.
        """
        self.pattern = pattern
        self.ignorecase = ignorecase

        # Handle negation
        if pattern.startswith(b"!"):
            self.is_exclude = False
            pattern = pattern[1:]
        else:
            # Handle escaping of ! and # at start only
            if (
                pattern.startswith(b"\\")
                and len(pattern) > 1
                and pattern[1:2] in (b"!", b"#")
            ):
                pattern = pattern[1:]
            self.is_exclude = True

        # Check if this is a directory-only pattern
        self.is_directory_only = pattern.endswith(b"/")

        flags = 0
        if self.ignorecase:
            flags = re.IGNORECASE
        self._re = re.compile(translate(pattern), flags)

    def __bytes__(self) -> bytes:
        """Return the pattern as bytes.

        Returns:
            The original pattern as bytes.
        """
        return self.pattern

    def __str__(self) -> str:
        """Return the pattern as a string.

        Returns:
            The pattern decoded as a string.
        """
        return os.fsdecode(self.pattern)

    def __eq__(self, other: object) -> bool:
        """Check equality with another Pattern object.

        Args:
            other: The object to compare with.

        Returns:
            True if patterns and ignorecase flags are equal, False otherwise.
        """
        return (
            isinstance(other, type(self))
            and self.pattern == other.pattern
            and self.ignorecase == other.ignorecase
        )

    def __repr__(self) -> str:
        """Return a string representation of the Pattern object.

        Returns:
            A string representation for debugging.
        """
        return f"{type(self).__name__}({self.pattern!r}, {self.ignorecase!r})"

    def match(self, path: bytes) -> bool:
        """Try to match a path against this ignore pattern.

        Args:
          path: Path to match (relative to ignore location)
        Returns: boolean
        """
        # For negation directory patterns (e.g., !dir/), only match directories
        if self.is_directory_only and not self.is_exclude and not path.endswith(b"/"):
            return False

        # Check if the regex matches
        if self._re.match(path):
            return True

        # For exclusion directory patterns, also match files under the directory
        if (
            self.is_directory_only
            and self.is_exclude
            and not path.endswith(b"/")
            and b"/" in path
        ):
            return bool(self._re.match(path.rsplit(b"/", 1)[0] + b"/"))

        return False


class IgnoreFilter:
    """Filter to apply gitignore patterns.

    Important: When checking if directories are ignored, include a trailing slash.
    For example, use is_ignored("dir/") instead of is_ignored("dir").
    """

    def __init__(
        self,
        patterns: Iterable[bytes],
        ignorecase: bool = False,
        path: Optional[str] = None,
    ) -> None:
        """Initialize an IgnoreFilter with a set of patterns.

        Args:
            patterns: An iterable of gitignore patterns as bytes.
            ignorecase: Whether to perform case-insensitive matching.
            path: Optional path to the ignore file for debugging purposes.
        """
        self._patterns: list[Pattern] = []
        self._ignorecase = ignorecase
        self._path = path
        for pattern in patterns:
            self.append_pattern(pattern)

    def append_pattern(self, pattern: bytes) -> None:
        """Add a pattern to the set."""
        self._patterns.append(Pattern(pattern, self._ignorecase))

    def find_matching(self, path: Union[bytes, str]) -> Iterable[Pattern]:
        """Yield all matching patterns for path.

        Args:
          path: Path to match
        Returns:
          Iterator over iterators
        """
        if not isinstance(path, bytes):
            path = os.fsencode(path)
        for pattern in self._patterns:
            if pattern.match(path):
                yield pattern

    def is_ignored(self, path: Union[bytes, str]) -> Optional[bool]:
        """Check whether a path is ignored using Git-compliant logic.

        For directories, include a trailing slash.

        Returns: status is None if file is not mentioned, True if it is
            included, False if it is explicitly excluded.
        """
        matching_patterns = list(self.find_matching(path))
        if not matching_patterns:
            return None

        # Basic rule: last matching pattern wins
        last_pattern = matching_patterns[-1]
        result = last_pattern.is_exclude

        # Apply Git's parent directory exclusion rule for negations
        if not result:  # Only applies to inclusions (negations)
            result = self._apply_parent_exclusion_rule(
                path.decode() if isinstance(path, bytes) else path, matching_patterns
            )

        return result

    def _apply_parent_exclusion_rule(
        self, path: str, matching_patterns: list[Pattern]
    ) -> bool:
        """Apply Git's parent directory exclusion rule.

        "It is not possible to re-include a file if a parent directory of that file is excluded."
        """
        return _check_parent_exclusion(path, matching_patterns)

    @classmethod
    def from_path(
        cls, path: Union[str, os.PathLike[str]], ignorecase: bool = False
    ) -> "IgnoreFilter":
        """Create an IgnoreFilter from a file path.

        Args:
            path: Path to the ignore file.
            ignorecase: Whether to perform case-insensitive matching.

        Returns:
            An IgnoreFilter instance with patterns loaded from the file.
        """
        with open(path, "rb") as f:
            return cls(read_ignore_patterns(f), ignorecase, path=str(path))

    def __repr__(self) -> str:
        """Return string representation of IgnoreFilter."""
        path = getattr(self, "_path", None)
        if path is not None:
            return f"{type(self).__name__}.from_path({path!r})"
        else:
            return f"<{type(self).__name__}>"


class IgnoreFilterStack:
    """Check for ignore status in multiple filters."""

    def __init__(self, filters: list[IgnoreFilter]) -> None:
        """Initialize an IgnoreFilterStack with multiple filters.

        Args:
            filters: A list of IgnoreFilter objects to check in order.
        """
        self._filters = filters

    def is_ignored(self, path: str) -> Optional[bool]:
        """Check whether a path is explicitly included or excluded in ignores.

        Args:
          path: Path to check
        Returns:
          None if the file is not mentioned, True if it is included,
          False if it is explicitly excluded.
        """
        for filter in self._filters:
            status = filter.is_ignored(path)
            if status is not None:
                return status
        return None

    def __repr__(self) -> str:
        """Return a string representation of the IgnoreFilterStack.

        Returns:
            A string representation for debugging.
        """
        return f"{type(self).__name__}({self._filters!r})"


def default_user_ignore_filter_path(config: Config) -> str:
    """Return default user ignore filter path.

    Args:
      config: A Config object
    Returns:
      Path to a global ignore file
    """
    try:
        value = config.get((b"core",), b"excludesFile")
        assert isinstance(value, bytes)
        return value.decode(encoding="utf-8")
    except KeyError:
        pass

    return get_xdg_config_home_path("git", "ignore")


class IgnoreFilterManager:
    """Ignore file manager with Git-compliant behavior.

    Important: When checking if directories are ignored, include a trailing slash.
    For example, use is_ignored("dir/") instead of is_ignored("dir").
    """

    def __init__(
        self,
        top_path: str,
        global_filters: list[IgnoreFilter],
        ignorecase: bool,
    ) -> None:
        """Initialize an IgnoreFilterManager.

        Args:
            top_path: The top-level directory path to manage ignores for.
            global_filters: List of global ignore filters to apply.
            ignorecase: Whether to perform case-insensitive matching.
        """
        self._path_filters: dict[str, Optional[IgnoreFilter]] = {}
        self._top_path = top_path
        self._global_filters = global_filters
        self._ignorecase = ignorecase

    def __repr__(self) -> str:
        """Return string representation of IgnoreFilterManager."""
        return f"{type(self).__name__}({self._top_path}, {self._global_filters!r}, {self._ignorecase!r})"

    def _load_path(self, path: str) -> Optional[IgnoreFilter]:
        try:
            return self._path_filters[path]
        except KeyError:
            pass

        p = os.path.join(self._top_path, path, ".gitignore")
        try:
            self._path_filters[path] = IgnoreFilter.from_path(p, self._ignorecase)
        except (FileNotFoundError, NotADirectoryError):
            self._path_filters[path] = None
        except OSError as e:
            # On Windows, opening a path that contains a symlink can fail with
            # errno 22 (Invalid argument) when the symlink points outside the repo
            if e.errno == 22:
                self._path_filters[path] = None
            else:
                raise
        return self._path_filters[path]

    def find_matching(self, path: str) -> Iterable[Pattern]:
        """Find matching patterns for path.

        Args:
          path: Path to check
        Returns:
          Iterator over Pattern instances
        """
        if os.path.isabs(path):
            raise ValueError(f"{path} is an absolute path")
        filters = [(0, f) for f in self._global_filters]
        if os.path.sep != "/":
            path = path.replace(os.path.sep, "/")
        parts = path.split("/")
        matches = []
        for i in range(len(parts) + 1):
            dirname = "/".join(parts[:i])
            for s, f in filters:
                relpath = "/".join(parts[s:i])
                if i < len(parts):
                    # Paths leading up to the final part are all directories,
                    # so need a trailing slash.
                    relpath += "/"
                matches += list(f.find_matching(relpath))
            ignore_filter = self._load_path(dirname)
            if ignore_filter is not None:
                filters.insert(0, (i, ignore_filter))
        return iter(matches)

    def is_ignored(self, path: str) -> Optional[bool]:
        """Check whether a path is explicitly included or excluded in ignores.

        Args:
          path: Path to check. For directories, the path should end with '/'.

        Returns:
          None if the file is not mentioned, True if it is included,
          False if it is explicitly excluded.
        """
        matches = list(self.find_matching(path))
        if not matches:
            return None

        # Standard behavior - last matching pattern wins
        result = matches[-1].is_exclude

        # Apply Git's parent directory exclusion rule for negations
        if not result:  # Only check if we would include due to negation
            result = _check_parent_exclusion(path, matches)

        # Apply special case for issue #1203: directory traversal with ** patterns
        if result and path.endswith("/"):
            result = self._apply_directory_traversal_rule(path, matches)

        return result

    def _apply_directory_traversal_rule(
        self, path: str, matches: list["Pattern"]
    ) -> bool:
        """Apply directory traversal rule for issue #1203.

        If a directory would be ignored by a ** pattern, but there are negation
        patterns for its subdirectories, then the directory itself should not
        be ignored (to allow traversal).
        """
        # Original logic for traversal check
        last_excluding_pattern = None
        for match in matches:
            if match.is_exclude:
                last_excluding_pattern = match

        if last_excluding_pattern and (
            last_excluding_pattern.pattern.endswith(b"**")
            or b"**" in last_excluding_pattern.pattern
        ):
            # Check if subdirectories would be unignored
            test_subdir = path + "test/"
            test_matches = list(self.find_matching(test_subdir))
            if test_matches:
                # Use standard logic for test case - last matching pattern wins
                test_result = test_matches[-1].is_exclude
                if test_result is False:
                    return False

        return True  # Keep original result

    @classmethod
    def from_repo(cls, repo: "Repo") -> "IgnoreFilterManager":
        """Create a IgnoreFilterManager from a repository.

        Args:
          repo: Repository object
        Returns:
          A `IgnoreFilterManager` object
        """
        global_filters = []
        for p in [
            os.path.join(repo.controldir(), "info", "exclude"),
            default_user_ignore_filter_path(repo.get_config_stack()),
        ]:
            with suppress(OSError):
                global_filters.append(IgnoreFilter.from_path(os.path.expanduser(p)))
        config = repo.get_config_stack()
        ignorecase = config.get_boolean((b"core"), (b"ignorecase"), False)
        return cls(repo.path, global_filters, ignorecase)
