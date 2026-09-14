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

__all__ = [
    "IgnoreFilter",
    "IgnoreFilterManager",
    "IgnoreFilterStack",
    "Pattern",
    "default_user_ignore_filter_path",
    "match_pattern",
    "read_ignore_patterns",
    "translate",
]

import logging
import os.path
import re
from collections.abc import Callable, Iterable, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:
    from .repo import Repo

from .config import Config, get_xdg_config_home_path
from .wildmatch import MalformedPattern
from .wildmatch import translate as translate_wildmatch

logger = logging.getLogger(__name__)


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
    """Translate a gitignore pattern to a regular expression following Git rules exactly.

    Raises:
      MalformedPattern: if wildmatch() would refuse the pattern outright;
        see :func:`dulwich.wildmatch.translate`.
    """
    res = b"(?ms)"

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

    # Everything left of the gitignore-specific decoration is plain wildmatch
    res += translate_wildmatch(pat)

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

        Raises:
            MalformedPattern: if wildmatch() would refuse the pattern
              outright. Loading a whole file of patterns should go through
              :meth:`IgnoreFilter.append_pattern` instead, which catches
              this and warns.
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

        # Lines like "!" or "/" carry no pattern once the negation prefix and
        # the directory suffix are stripped. Git keeps them in its pattern list
        # but they can never match a path, so neither do we.
        self.is_empty = not pattern.rstrip(b"/")

        flags = 0
        if self.ignorecase:
            flags = re.IGNORECASE
        self._re = re.compile(translate(pattern), flags)
        # Git drops the trailing slash of a directory-only pattern and requires
        # the entry to be a directory instead, so the pattern itself is matched
        # against a plain name.
        self._name_re = re.compile(translate(pattern.rstrip(b"/")), flags)
        # "dir/**" and "dir/*" name no entry of their own; they describe what
        # a directory holds.
        self._describes_contents = pattern.rstrip(b"/").endswith((b"/*", b"/**"))

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

    def matches(self, name: bytes, is_dir: bool) -> bool:
        """Check whether this pattern decides an entry.

        Args:
          name: Path of the entry, relative to the directory holding the
            pattern and without a trailing slash.
          is_dir: Whether the entry is a directory.

        A pattern written with a trailing slash only ever matches a directory;
        git drops that slash and applies this flag instead. A pattern covering
        what is inside a directory, such as ``dir/**``, names no entry of its
        own and so only matches below it.
        """
        if self.is_empty:
            return False
        if self.is_directory_only and not is_dir:
            return False
        return bool(self._name_re.match(name))

    def reaches_inside(self, name: bytes) -> bool:
        """Check whether this pattern decides entries below a directory.

        A pattern such as ``dir/**`` names no entry itself, so it never
        decides ``dir``; it does decide what ``dir`` holds, which is what a
        query about the directory is really asking.
        """
        if not self._describes_contents:
            return False
        prefix = name + b"/"
        return bool(self.match(prefix + b"x") or self.match(prefix + b"x/"))

    def match(self, path: bytes) -> bool:
        """Try to match a path against this ignore pattern.

        Args:
          path: Path to match (relative to ignore location)
        Returns: boolean
        """
        if self.is_empty:
            return False

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


#: A filter paired with the depth of the directory whose .gitignore holds it.
_DepthFilter = tuple[int, "IgnoreFilter"]
#: Returns the filters applying to a path, shallowest first.
_FiltersFor = Callable[[str], Sequence[_DepthFilter]]


def _relative_to(path: str, depth: int) -> str:
    """Return path as seen from the directory a filter lives in."""
    return "/".join(path.split("/")[depth:])


def _last_matching_pattern(
    filters: Sequence[_DepthFilter], name: str, is_dir: bool
) -> "Pattern | None":
    """Return the pattern deciding an entry, or None if none applies.

    Git consults the .gitignore files from the deepest directory upwards and
    stops at the first file that has something to say, so a rule file in a
    subdirectory overrides one closer to the root. Within a file the last
    matching pattern wins.
    """
    for depth, f in reversed(filters):
        relname = os.fsencode(_relative_to(name, depth))
        for pattern in reversed(f.patterns):
            if pattern.matches(relname, is_dir):
                return pattern
    return None


def _decide(filters_for: _FiltersFor, path: str, is_dir: bool) -> bool | None:
    """Decide whether an entry is ignored, given the filters applying to it.

    Git walks down from the top and stops at the first excluded directory, so
    a rule below one is never reached and cannot re-include anything under it.
    A negated match does not exclude, and so does not stop the walk.
    """
    name = path.rstrip("/")
    parts = name.split("/")
    for i in range(1, len(parts)):
        parent = "/".join(parts[:i])
        pattern = _last_matching_pattern(filters_for(parent + "/"), parent, True)
        if pattern is not None and pattern.is_exclude:
            return True

    filters = filters_for(path)
    pattern = _last_matching_pattern(filters, name, is_dir)
    if pattern is not None and pattern.is_exclude:
        return True

    # A path written with a trailing slash asks about what the directory holds
    # as well, so an exclusion reaching inside it applies even when nothing
    # names the directory itself.
    if path.endswith("/") and _excluded_inside(filters, name):
        return True

    return None if pattern is None else False


def _excluded_inside(filters: Sequence[_DepthFilter], name: str) -> bool:
    """Check whether the last pattern reaching inside a directory excludes."""
    for depth, f in reversed(filters):
        relname = os.fsencode(_relative_to(name, depth))
        for pattern in reversed(f.patterns):
            if pattern.reaches_inside(relname):
                return pattern.is_exclude
    return False


def _may_prune(filters_for: _FiltersFor, path: str) -> bool:
    """Check whether a directory can be skipped without missing anything.

    Git stops descending only when a pattern excludes the directory by name.
    A pattern that merely describes the contents, such as ``dir/*``, leaves
    the walk free to enter, so a later negation can still re-include
    something below. ``check-ignore`` reports such a directory as ignored --
    ``dir/*`` matches the text ``dir/`` because ``*`` also matches the empty
    string -- which is why walking callers must ask this instead of reading
    :meth:`is_ignored` as permission to prune.
    """
    name = path.rstrip("/")
    parts = name.split("/")
    for i in range(1, len(parts) + 1):
        prefix = "/".join(parts[:i])
        pattern = _last_matching_pattern(filters_for(prefix + "/"), prefix, True)
        if pattern is not None and pattern.is_exclude:
            return True
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
        path: str | None = None,
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
        """Add a pattern to the set.

        A malformed pattern is logged and skipped rather than raised, so one
        bad line in a gitignore file doesn't stop the rest from loading. To
        fail loudly instead, construct the ``Pattern`` directly.
        """
        try:
            compiled = Pattern(pattern, self._ignorecase)
        except MalformedPattern:
            logger.warning(
                "Ignoring malformed pattern %r in %s",
                pattern,
                self._path or "<patterns>",
            )
            return
        self._patterns.append(compiled)

    def find_matching(self, path: bytes | str) -> Iterable[Pattern]:
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

    @property
    def patterns(self) -> list[Pattern]:
        """Return the compiled patterns, in the order they were read."""
        return self._patterns

    def is_ignored(self, path: bytes | str) -> bool | None:
        """Check whether a path is ignored using Git-compliant logic.

        For directories, include a trailing slash.

        Returns: status is None if file is not mentioned, True if it is
            included, False if it is explicitly excluded.
        """
        if isinstance(path, bytes):
            path = path.decode()
        # A single file's patterns are all rooted at the filter itself. Without
        # a work tree to consult, a trailing slash is the only indication that
        # the path is a directory.
        return _decide(lambda _path: [(0, self)], path, path.endswith("/"))

    def may_prune_directory(self, path: bytes | str) -> bool:
        """Check whether a directory can be skipped when walking a work tree.

        See :func:`_may_prune`; a directory reported ignored by
        :meth:`is_ignored` may still need to be entered.
        """
        if isinstance(path, bytes):
            path = path.decode()
        return _may_prune(lambda _path: [(0, self)], path)

    @classmethod
    def from_path(
        cls, path: str | os.PathLike[str], ignorecase: bool = False
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

    def is_ignored(self, path: str) -> bool | None:
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
        self._path_filters: dict[str, IgnoreFilter | None] = {}
        self._top_path = top_path
        self._global_filters = global_filters
        self._ignorecase = ignorecase

    def __repr__(self) -> str:
        """Return string representation of IgnoreFilterManager."""
        return f"{type(self).__name__}({self._top_path}, {self._global_filters!r}, {self._ignorecase!r})"

    def _load_path(self, path: str) -> IgnoreFilter | None:
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
        path = self._normalize(path)
        matches: list[Pattern] = []
        for depth, f in self._filters_for(path):
            matches.extend(f.find_matching(_relative_to(path, depth)))
        return iter(matches)

    @staticmethod
    def _normalize(path: str) -> str:
        if os.path.isabs(path):
            raise ValueError(f"{path} is an absolute path")
        if os.path.sep != "/":
            path = path.replace(os.path.sep, "/")
        return path

    def _filters_for(self, path: str) -> list[_DepthFilter]:
        """Return the filters applying to path, shallowest first.

        Each entry pairs a filter with the depth of the directory holding it,
        so its patterns can be matched against a path relative to that
        directory.
        """
        filters = [(0, f) for f in self._global_filters]
        parts = path.split("/")
        for i in range(len(parts)):
            ignore_filter = self._load_path("/".join(parts[:i]))
            if ignore_filter is not None:
                filters.append((i, ignore_filter))
        return filters

    def is_ignored(self, path: str) -> bool | None:
        """Check whether a path is explicitly included or excluded in ignores.

        Args:
          path: Path to check. For directories, the path should end with '/'.

        Returns:
          None if the file is not mentioned, True if it is included,
          False if it is explicitly excluded.
        """
        path = self._normalize(path)
        return _decide(self._filters_for, path, self._is_dir(path))

    def may_prune_directory(self, path: str) -> bool:
        """Check whether a directory can be skipped when walking a work tree.

        See :func:`_may_prune`; a directory reported ignored by
        :meth:`is_ignored` may still need to be entered.
        """
        return _may_prune(self._filters_for, self._normalize(path))

    def _is_dir(self, path: str) -> bool:
        """Check whether path names a directory in the work tree.

        Git resolves this from the index or a stat of the entry; a trailing
        slash says so outright, and otherwise the work tree is consulted.
        """
        if path.endswith("/"):
            return True
        return os.path.isdir(os.path.join(self._top_path, path))

    @classmethod
    def from_repo(
        cls,
        repo: "Repo",
        config: "Config | None" = None,
    ) -> "IgnoreFilterManager":
        """Create a IgnoreFilterManager from a repository.

        Args:
          repo: Repository object
          config: Configuration to consult for ignorecase and the user-level
            ignore path. If None, falls back to ``repo.get_config_stack()``.

        Returns:
          A `IgnoreFilterManager` object
        """
        if config is None:
            config = repo.get_config_stack()
        global_filters = []
        for p in [
            os.path.join(repo.controldir(), "info", "exclude"),
            default_user_ignore_filter_path(config),
        ]:
            with suppress(OSError):
                global_filters.append(IgnoreFilter.from_path(os.path.expanduser(p)))
        ignorecase = config.get_boolean((b"core"), (b"ignorecase"), False)
        return cls(repo.path, global_filters, ignorecase)
