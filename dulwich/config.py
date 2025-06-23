# config.py - Reading and writing Git config files
# Copyright (C) 2011-2013 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

"""Reading and writing Git configuration files.

Todo:
 * preserve formatting when updating configuration files
"""

import logging
import os
import sys
from collections.abc import Iterable, Iterator, KeysView, MutableMapping
from contextlib import suppress
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Callable,
    Optional,
    Union,
    overload,
)

from .file import GitFile

logger = logging.getLogger(__name__)

# Type for file opener callback
FileOpener = Callable[[Union[str, os.PathLike]], BinaryIO]

# Security limits for include files
MAX_INCLUDE_FILE_SIZE = 1024 * 1024  # 1MB max for included config files
DEFAULT_MAX_INCLUDE_DEPTH = 10  # Maximum recursion depth for includes

SENTINEL = object()


def _match_gitdir_pattern(
    path: bytes, pattern: bytes, ignorecase: bool = False
) -> bool:
    """Simple gitdir pattern matching for includeIf conditions.

    This handles the basic gitdir patterns used in includeIf directives.
    """
    # Convert to strings for easier manipulation
    path_str = path.decode("utf-8", errors="replace")
    pattern_str = pattern.decode("utf-8", errors="replace")

    # Normalize paths to use forward slashes for consistent matching
    path_str = path_str.replace("\\", "/")
    pattern_str = pattern_str.replace("\\", "/")

    if ignorecase:
        path_str = path_str.lower()
        pattern_str = pattern_str.lower()

    # Handle the common cases for gitdir patterns
    if pattern_str.startswith("**/") and pattern_str.endswith("/**"):
        # Pattern like **/dirname/** should match any path containing dirname
        dirname = pattern_str[3:-3]  # Remove **/ and /**
        # Check if path contains the directory name as a path component
        return ("/" + dirname + "/") in path_str or path_str.endswith("/" + dirname)
    elif pattern_str.startswith("**/"):
        # Pattern like **/filename
        suffix = pattern_str[3:]  # Remove **/
        return suffix in path_str or path_str.endswith("/" + suffix)
    elif pattern_str.endswith("/**"):
        # Pattern like /path/to/dir/** should match /path/to/dir and any subdirectory
        base_pattern = pattern_str[:-3]  # Remove /**
        return path_str == base_pattern or path_str.startswith(base_pattern + "/")
    elif "**" in pattern_str:
        # Handle patterns with ** in the middle
        parts = pattern_str.split("**")
        if len(parts) == 2:
            prefix, suffix = parts
            # Path must start with prefix and end with suffix (if any)
            if prefix and not path_str.startswith(prefix):
                return False
            if suffix and not path_str.endswith(suffix):
                return False
            return True

    # Direct match or simple glob pattern
    if "*" in pattern_str or "?" in pattern_str or "[" in pattern_str:
        import fnmatch

        return fnmatch.fnmatch(path_str, pattern_str)
    else:
        return path_str == pattern_str


def lower_key(key):
    if isinstance(key, (bytes, str)):
        return key.lower()

    if isinstance(key, Iterable):
        # For config sections, only lowercase the section name (first element)
        # but preserve the case of subsection names (remaining elements)
        if len(key) > 0:
            return (key[0].lower(), *key[1:])
        return key

    return key


class CaseInsensitiveOrderedMultiDict(MutableMapping):
    def __init__(self) -> None:
        self._real: list[Any] = []
        self._keyed: dict[Any, Any] = {}

    @classmethod
    def make(cls, dict_in=None):
        if isinstance(dict_in, cls):
            return dict_in

        out = cls()

        if dict_in is None:
            return out

        if not isinstance(dict_in, MutableMapping):
            raise TypeError

        for key, value in dict_in.items():
            out[key] = value

        return out

    def __len__(self) -> int:
        return len(self._keyed)

    def keys(self) -> KeysView[tuple[bytes, ...]]:
        return self._keyed.keys()

    def items(self):
        return iter(self._real)

    def __iter__(self):
        return self._keyed.__iter__()

    def values(self):
        return self._keyed.values()

    def __setitem__(self, key, value) -> None:
        self._real.append((key, value))
        self._keyed[lower_key(key)] = value

    def set(self, key, value) -> None:
        # This method replaces all existing values for the key
        lower = lower_key(key)
        self._real = [(k, v) for k, v in self._real if lower_key(k) != lower]
        self._real.append((key, value))
        self._keyed[lower] = value

    def __delitem__(self, key) -> None:
        key = lower_key(key)
        del self._keyed[key]
        for i, (actual, unused_value) in reversed(list(enumerate(self._real))):
            if lower_key(actual) == key:
                del self._real[i]

    def __getitem__(self, item):
        return self._keyed[lower_key(item)]

    def get(self, key, default=SENTINEL):
        try:
            return self[key]
        except KeyError:
            pass

        if default is SENTINEL:
            return type(self)()

        return default

    def get_all(self, key):
        key = lower_key(key)
        for actual, value in self._real:
            if lower_key(actual) == key:
                yield value

    def setdefault(self, key, default=SENTINEL):
        try:
            return self[key]
        except KeyError:
            self[key] = self.get(key, default)

        return self[key]


Name = bytes
NameLike = Union[bytes, str]
Section = tuple[bytes, ...]
SectionLike = Union[bytes, str, tuple[Union[bytes, str], ...]]
Value = bytes
ValueLike = Union[bytes, str]


class Config:
    """A Git configuration."""

    def get(self, section: SectionLike, name: NameLike) -> Value:
        """Retrieve the contents of a configuration setting.

        Args:
          section: Tuple with section name and optional subsection name
          name: Variable name
        Returns:
          Contents of the setting
        Raises:
          KeyError: if the value is not set
        """
        raise NotImplementedError(self.get)

    def get_multivar(self, section: SectionLike, name: NameLike) -> Iterator[Value]:
        """Retrieve the contents of a multivar configuration setting.

        Args:
          section: Tuple with section name and optional subsection namee
          name: Variable name
        Returns:
          Contents of the setting as iterable
        Raises:
          KeyError: if the value is not set
        """
        raise NotImplementedError(self.get_multivar)

    @overload
    def get_boolean(
        self, section: SectionLike, name: NameLike, default: bool
    ) -> bool: ...

    @overload
    def get_boolean(self, section: SectionLike, name: NameLike) -> Optional[bool]: ...

    def get_boolean(
        self, section: SectionLike, name: NameLike, default: Optional[bool] = None
    ) -> Optional[bool]:
        """Retrieve a configuration setting as boolean.

        Args:
          section: Tuple with section name and optional subsection name
          name: Name of the setting, including section and possible
            subsection.

        Returns:
          Contents of the setting
        """
        try:
            value = self.get(section, name)
        except KeyError:
            return default
        if value.lower() == b"true":
            return True
        elif value.lower() == b"false":
            return False
        raise ValueError(f"not a valid boolean string: {value!r}")

    def set(
        self, section: SectionLike, name: NameLike, value: Union[ValueLike, bool]
    ) -> None:
        """Set a configuration value.

        Args:
          section: Tuple with section name and optional subsection namee
          name: Name of the configuration value, including section
            and optional subsection
          value: value of the setting
        """
        raise NotImplementedError(self.set)

    def items(self, section: SectionLike) -> Iterator[tuple[Name, Value]]:
        """Iterate over the configuration pairs for a specific section.

        Args:
          section: Tuple with section name and optional subsection namee
        Returns:
          Iterator over (name, value) pairs
        """
        raise NotImplementedError(self.items)

    def sections(self) -> Iterator[Section]:
        """Iterate over the sections.

        Returns: Iterator over section tuples
        """
        raise NotImplementedError(self.sections)

    def has_section(self, name: Section) -> bool:
        """Check if a specified section exists.

        Args:
          name: Name of section to check for
        Returns:
          boolean indicating whether the section exists
        """
        return name in self.sections()


class ConfigDict(Config, MutableMapping[Section, MutableMapping[Name, Value]]):
    """Git configuration stored in a dictionary."""

    def __init__(
        self,
        values: Union[
            MutableMapping[Section, MutableMapping[Name, Value]], None
        ] = None,
        encoding: Union[str, None] = None,
    ) -> None:
        """Create a new ConfigDict."""
        if encoding is None:
            encoding = sys.getdefaultencoding()
        self.encoding = encoding
        self._values = CaseInsensitiveOrderedMultiDict.make(values)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._values!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and other._values == self._values

    def __getitem__(self, key: Section) -> MutableMapping[Name, Value]:
        return self._values.__getitem__(key)

    def __setitem__(self, key: Section, value: MutableMapping[Name, Value]) -> None:
        return self._values.__setitem__(key, value)

    def __delitem__(self, key: Section) -> None:
        return self._values.__delitem__(key)

    def __iter__(self) -> Iterator[Section]:
        return self._values.__iter__()

    def __len__(self) -> int:
        return self._values.__len__()

    @classmethod
    def _parse_setting(cls, name: str):
        parts = name.split(".")
        if len(parts) == 3:
            return (parts[0], parts[1], parts[2])
        else:
            return (parts[0], None, parts[1])

    def _check_section_and_name(
        self, section: SectionLike, name: NameLike
    ) -> tuple[Section, Name]:
        if not isinstance(section, tuple):
            section = (section,)

        checked_section = tuple(
            [
                subsection.encode(self.encoding)
                if not isinstance(subsection, bytes)
                else subsection
                for subsection in section
            ]
        )

        if not isinstance(name, bytes):
            name = name.encode(self.encoding)

        return checked_section, name

    def get_multivar(self, section: SectionLike, name: NameLike) -> Iterator[Value]:
        section, name = self._check_section_and_name(section, name)

        if len(section) > 1:
            try:
                return self._values[section].get_all(name)
            except KeyError:
                pass

        return self._values[(section[0],)].get_all(name)

    def get(  # type: ignore[override]
        self,
        section: SectionLike,
        name: NameLike,
    ) -> Value:
        section, name = self._check_section_and_name(section, name)

        if len(section) > 1:
            try:
                return self._values[section][name]
            except KeyError:
                pass

        return self._values[(section[0],)][name]

    def set(
        self,
        section: SectionLike,
        name: NameLike,
        value: Union[ValueLike, bool],
    ) -> None:
        section, name = self._check_section_and_name(section, name)

        if isinstance(value, bool):
            value = b"true" if value else b"false"

        if not isinstance(value, bytes):
            value = value.encode(self.encoding)

        section_dict = self._values.setdefault(section)
        if hasattr(section_dict, "set"):
            section_dict.set(name, value)
        else:
            section_dict[name] = value

    def add(
        self,
        section: SectionLike,
        name: NameLike,
        value: Union[ValueLike, bool],
    ) -> None:
        """Add a value to a configuration setting, creating a multivar if needed."""
        section, name = self._check_section_and_name(section, name)

        if isinstance(value, bool):
            value = b"true" if value else b"false"

        if not isinstance(value, bytes):
            value = value.encode(self.encoding)

        self._values.setdefault(section)[name] = value

    def items(  # type: ignore[override]
        self, section: Section
    ) -> Iterator[tuple[Name, Value]]:
        return self._values.get(section).items()

    def sections(self) -> Iterator[Section]:
        return self._values.keys()


def _format_string(value: bytes) -> bytes:
    if (
        value.startswith((b" ", b"\t"))
        or value.endswith((b" ", b"\t"))
        or b"#" in value
    ):
        return b'"' + _escape_value(value) + b'"'
    else:
        return _escape_value(value)


_ESCAPE_TABLE = {
    ord(b"\\"): ord(b"\\"),
    ord(b'"'): ord(b'"'),
    ord(b"n"): ord(b"\n"),
    ord(b"t"): ord(b"\t"),
    ord(b"b"): ord(b"\b"),
}
_COMMENT_CHARS = [ord(b"#"), ord(b";")]
_WHITESPACE_CHARS = [ord(b"\t"), ord(b" ")]


def _parse_string(value: bytes) -> bytes:
    value = bytearray(value.strip())
    ret = bytearray()
    whitespace = bytearray()
    in_quotes = False
    i = 0
    while i < len(value):
        c = value[i]
        if c == ord(b"\\"):
            i += 1
            if i >= len(value):
                # Backslash at end of string - treat as literal backslash
                if whitespace:
                    ret.extend(whitespace)
                    whitespace = bytearray()
                ret.append(ord(b"\\"))
            else:
                try:
                    v = _ESCAPE_TABLE[value[i]]
                    if whitespace:
                        ret.extend(whitespace)
                        whitespace = bytearray()
                    ret.append(v)
                except KeyError:
                    # Unknown escape sequence - treat backslash as literal and process next char normally
                    if whitespace:
                        ret.extend(whitespace)
                        whitespace = bytearray()
                    ret.append(ord(b"\\"))
                    i -= 1  # Reprocess the character after the backslash
        elif c == ord(b'"'):
            in_quotes = not in_quotes
        elif c in _COMMENT_CHARS and not in_quotes:
            # the rest of the line is a comment
            break
        elif c in _WHITESPACE_CHARS:
            whitespace.append(c)
        else:
            if whitespace:
                ret.extend(whitespace)
                whitespace = bytearray()
            ret.append(c)
        i += 1

    if in_quotes:
        raise ValueError("missing end quote")

    return bytes(ret)


def _escape_value(value: bytes) -> bytes:
    """Escape a value."""
    value = value.replace(b"\\", b"\\\\")
    value = value.replace(b"\r", b"\\r")
    value = value.replace(b"\n", b"\\n")
    value = value.replace(b"\t", b"\\t")
    value = value.replace(b'"', b'\\"')
    return value


def _check_variable_name(name: bytes) -> bool:
    for i in range(len(name)):
        c = name[i : i + 1]
        if not c.isalnum() and c != b"-":
            return False
    return True


def _check_section_name(name: bytes) -> bool:
    for i in range(len(name)):
        c = name[i : i + 1]
        if not c.isalnum() and c not in (b"-", b"."):
            return False
    return True


def _strip_comments(line: bytes) -> bytes:
    comment_bytes = {ord(b"#"), ord(b";")}
    quote = ord(b'"')
    string_open = False
    # Normalize line to bytearray for simple 2/3 compatibility
    for i, character in enumerate(bytearray(line)):
        # Comment characters outside balanced quotes denote comment start
        if character == quote:
            string_open = not string_open
        elif not string_open and character in comment_bytes:
            return line[:i]
    return line


def _is_line_continuation(value: bytes) -> bool:
    """Check if a value ends with a line continuation backslash.

    A line continuation occurs when a line ends with a backslash that is:
    1. Not escaped (not preceded by another backslash)
    2. Not within quotes

    Args:
        value: The value to check

    Returns:
        True if the value ends with a line continuation backslash
    """
    if not value.endswith((b"\\\n", b"\\\r\n")):
        return False

    # Remove only the newline characters, keep the content including the backslash
    if value.endswith(b"\\\r\n"):
        content = value[:-2]  # Remove \r\n, keep the \
    else:
        content = value[:-1]  # Remove \n, keep the \

    if not content.endswith(b"\\"):
        return False

    # Count consecutive backslashes at the end
    backslash_count = 0
    for i in range(len(content) - 1, -1, -1):
        if content[i : i + 1] == b"\\":
            backslash_count += 1
        else:
            break

    # If we have an odd number of backslashes, the last one is a line continuation
    # If we have an even number, they are all escaped and there's no continuation
    return backslash_count % 2 == 1


def _parse_section_header_line(line: bytes) -> tuple[Section, bytes]:
    # Parse section header ("[bla]")
    line = _strip_comments(line).rstrip()
    in_quotes = False
    escaped = False
    for i, c in enumerate(line):
        if escaped:
            escaped = False
            continue
        if c == ord(b'"'):
            in_quotes = not in_quotes
        if c == ord(b"\\"):
            escaped = True
        if c == ord(b"]") and not in_quotes:
            last = i
            break
    else:
        raise ValueError("expected trailing ]")
    pts = line[1:last].split(b" ", 1)
    line = line[last + 1 :]
    section: Section
    if len(pts) == 2:
        # Handle subsections - Git allows more complex syntax for certain sections like includeIf
        if pts[1][:1] == b'"' and pts[1][-1:] == b'"':
            # Standard quoted subsection
            pts[1] = pts[1][1:-1]
        elif pts[0] == b"includeIf":
            # Special handling for includeIf sections which can have complex conditions
            # Git allows these without strict quote validation
            pts[1] = pts[1].strip()
            if pts[1][:1] == b'"' and pts[1][-1:] == b'"':
                pts[1] = pts[1][1:-1]
        else:
            # Other sections must have quoted subsections
            raise ValueError(f"Invalid subsection {pts[1]!r}")
        if not _check_section_name(pts[0]):
            raise ValueError(f"invalid section name {pts[0]!r}")
        section = (pts[0], pts[1])
    else:
        if not _check_section_name(pts[0]):
            raise ValueError(f"invalid section name {pts[0]!r}")
        pts = pts[0].split(b".", 1)
        if len(pts) == 2:
            section = (pts[0], pts[1])
        else:
            section = (pts[0],)
    return section, line


class ConfigFile(ConfigDict):
    """A Git configuration file, like .git/config or ~/.gitconfig."""

    def __init__(
        self,
        values: Union[
            MutableMapping[Section, MutableMapping[Name, Value]], None
        ] = None,
        encoding: Union[str, None] = None,
    ) -> None:
        super().__init__(values=values, encoding=encoding)
        self.path: Optional[str] = None
        self._included_paths: set[str] = set()  # Track included files to prevent cycles

    @classmethod
    def from_file(
        cls,
        f: BinaryIO,
        *,
        config_dir: Optional[str] = None,
        repo_dir: Optional[str] = None,
        included_paths: Optional[set[str]] = None,
        include_depth: int = 0,
        max_include_depth: int = DEFAULT_MAX_INCLUDE_DEPTH,
        file_opener: Optional[FileOpener] = None,
    ) -> "ConfigFile":
        """Read configuration from a file-like object.

        Args:
            f: File-like object to read from
            config_dir: Directory containing the config file (for relative includes)
            repo_dir: Repository directory (for gitdir conditions)
            included_paths: Set of already included paths (to prevent cycles)
            include_depth: Current include depth (to prevent infinite recursion)
            max_include_depth: Maximum allowed include depth
            file_opener: Optional callback to open included files
        """
        if include_depth > max_include_depth:
            # Prevent excessive recursion
            raise ValueError(f"Maximum include depth ({max_include_depth}) exceeded")

        ret = cls()
        if included_paths is not None:
            ret._included_paths = included_paths.copy()

        section: Optional[Section] = None
        setting = None
        continuation = None
        for lineno, line in enumerate(f.readlines()):
            if lineno == 0 and line.startswith(b"\xef\xbb\xbf"):
                line = line[3:]
            line = line.lstrip()
            if setting is None:
                if len(line) > 0 and line[:1] == b"[":
                    section, line = _parse_section_header_line(line)
                    ret._values.setdefault(section)
                if _strip_comments(line).strip() == b"":
                    continue
                if section is None:
                    raise ValueError(f"setting {line!r} without section")
                try:
                    setting, value = line.split(b"=", 1)
                except ValueError:
                    setting = line
                    value = b"true"
                setting = setting.strip()
                if not _check_variable_name(setting):
                    raise ValueError(f"invalid variable name {setting!r}")
                if _is_line_continuation(value):
                    if value.endswith(b"\\\r\n"):
                        continuation = value[:-3]
                    else:
                        continuation = value[:-2]
                else:
                    continuation = None
                    value = _parse_string(value)
                    ret._values[section][setting] = value

                    # Process include/includeIf directives
                    ret._handle_include_directive(
                        section,
                        setting,
                        value,
                        config_dir=config_dir,
                        repo_dir=repo_dir,
                        include_depth=include_depth,
                        max_include_depth=max_include_depth,
                        file_opener=file_opener,
                    )

                    setting = None
            else:  # continuation line
                assert continuation is not None
                if _is_line_continuation(line):
                    if line.endswith(b"\\\r\n"):
                        continuation += line[:-3]
                    else:
                        continuation += line[:-2]
                else:
                    continuation += line
                    value = _parse_string(continuation)
                    ret._values[section][setting] = value

                    # Process include/includeIf directives
                    ret._handle_include_directive(
                        section,
                        setting,
                        value,
                        config_dir=config_dir,
                        repo_dir=repo_dir,
                        include_depth=include_depth,
                        max_include_depth=max_include_depth,
                        file_opener=file_opener,
                    )

                    continuation = None
                    setting = None
        return ret

    def _handle_include_directive(
        self,
        section: Optional[Section],
        setting: bytes,
        value: bytes,
        *,
        config_dir: Optional[str],
        repo_dir: Optional[str],
        include_depth: int,
        max_include_depth: int,
        file_opener: Optional[FileOpener],
    ) -> None:
        """Handle include/includeIf directives during config parsing."""
        if (
            section is not None
            and setting == b"path"
            and (
                section[0].lower() == b"include"
                or (len(section) > 1 and section[0].lower() == b"includeif")
            )
        ):
            self._process_include(
                section,
                value,
                config_dir=config_dir,
                repo_dir=repo_dir,
                include_depth=include_depth,
                max_include_depth=max_include_depth,
                file_opener=file_opener,
            )

    def _process_include(
        self,
        section: Section,
        path_value: bytes,
        *,
        config_dir: Optional[str],
        repo_dir: Optional[str],
        include_depth: int,
        max_include_depth: int,
        file_opener: Optional[FileOpener],
    ) -> None:
        """Process an include or includeIf directive."""
        path_str = path_value.decode(self.encoding, errors="replace")

        # Handle includeIf conditions
        if len(section) > 1 and section[0].lower() == b"includeif":
            condition = section[1].decode(self.encoding, errors="replace")
            if not self._evaluate_includeif_condition(condition, repo_dir):
                return

        # Resolve the include path
        include_path = self._resolve_include_path(path_str, config_dir)
        if not include_path:
            return

        # Check for circular includes
        try:
            abs_path = str(Path(include_path).resolve())
        except (OSError, ValueError) as e:
            # Invalid path - log and skip
            logger.debug("Invalid include path %r: %s", include_path, e)
            return
        if abs_path in self._included_paths:
            return

        # Load and merge the included file
        try:
            # Use provided file opener or default to GitFile
            if file_opener is None:

                def opener(path):
                    return GitFile(path, "rb")
            else:
                opener = file_opener

            f = opener(include_path)
        except (OSError, ValueError) as e:
            # Git silently ignores missing or unreadable include files
            # Log for debugging purposes
            logger.debug("Invalid include path %r: %s", include_path, e)
        else:
            with f as included_file:
                # Track this path to prevent cycles
                self._included_paths.add(abs_path)

                # Parse the included file
                included_config = ConfigFile.from_file(
                    included_file,
                    config_dir=os.path.dirname(include_path),
                    repo_dir=repo_dir,
                    included_paths=self._included_paths,
                    include_depth=include_depth + 1,
                    max_include_depth=max_include_depth,
                    file_opener=file_opener,
                )

                # Merge the included configuration
                self._merge_config(included_config)

    def _merge_config(self, other: "ConfigFile") -> None:
        """Merge another config file into this one."""
        for section, values in other._values.items():
            if section not in self._values:
                self._values[section] = CaseInsensitiveOrderedMultiDict()
            for key, value in values.items():
                self._values[section][key] = value

    def _resolve_include_path(
        self, path: str, config_dir: Optional[str]
    ) -> Optional[str]:
        """Resolve an include path to an absolute path."""
        # Expand ~ to home directory
        path = os.path.expanduser(path)

        # If path is relative and we have a config directory, make it relative to that
        if not os.path.isabs(path) and config_dir:
            path = os.path.join(config_dir, path)

        return path

    def _evaluate_includeif_condition(
        self, condition: str, repo_dir: Optional[str]
    ) -> bool:
        """Evaluate an includeIf condition."""
        if condition.startswith("gitdir:"):
            return self._evaluate_gitdir_condition(condition[7:], repo_dir)
        elif condition.startswith("gitdir/i:"):
            return self._evaluate_gitdir_condition(
                condition[9:], repo_dir, case_sensitive=False
            )
        elif condition.startswith("hasconfig:"):
            # hasconfig conditions require special handling and access to the full config
            # For now, we'll skip these as they require more complex implementation
            return False
        else:
            # Unknown condition type - log and ignore (Git behavior)
            logger.debug("Unknown includeIf condition: %r", condition)
            return False

    def _evaluate_gitdir_condition(
        self, pattern: str, repo_dir: Optional[str], case_sensitive: bool = True
    ) -> bool:
        """Evaluate a gitdir condition using simplified pattern matching."""
        if not repo_dir:
            return False

        # Skip relative patterns for now (would need config file path)
        if pattern.startswith("./"):
            return False

        # Normalize repository path
        try:
            repo_path = str(Path(repo_dir).resolve())
        except (OSError, ValueError) as e:
            logger.debug("Invalid repository path %r: %s", repo_dir, e)
            return False

        # Expand ~ in pattern and normalize
        pattern = os.path.expanduser(pattern)
        pattern = self._normalize_gitdir_pattern(pattern)

        # Use simple pattern matching for gitdir conditions
        pattern_bytes = pattern.encode("utf-8", errors="replace")
        repo_path_bytes = repo_path.encode("utf-8", errors="replace")

        return _match_gitdir_pattern(
            repo_path_bytes, pattern_bytes, ignorecase=not case_sensitive
        )

    def _normalize_gitdir_pattern(self, pattern: str) -> str:
        """Normalize a gitdir pattern following Git's rules."""
        # Normalize path separators to forward slashes
        pattern = pattern.replace("\\", "/")

        # If pattern doesn't start with ~/, ./, /, drive letter (Windows), or **, prepend **/
        if not pattern.startswith(("~/", "./", "/", "**")):
            # Check for Windows absolute path (e.g., C:/, D:/)
            if len(pattern) >= 2 and pattern[1] == ":":
                pass  # Don't prepend **/ for Windows absolute paths
            else:
                pattern = "**/" + pattern

        # If pattern ends with /, append **
        if pattern.endswith("/"):
            pattern = pattern + "**"

        return pattern

    @classmethod
    def from_path(
        cls,
        path: Union[str, os.PathLike],
        *,
        repo_dir: Optional[str] = None,
        max_include_depth: int = DEFAULT_MAX_INCLUDE_DEPTH,
        file_opener: Optional[FileOpener] = None,
    ) -> "ConfigFile":
        """Read configuration from a file on disk.

        Args:
            path: Path to the configuration file
            repo_dir: Repository directory (for gitdir conditions in includeIf)
            max_include_depth: Maximum allowed include depth
            file_opener: Optional callback to open included files
        """
        abs_path = os.fspath(path)
        config_dir = os.path.dirname(abs_path)

        # Use provided file opener or default to GitFile
        if file_opener is None:

            def opener(p):
                return GitFile(p, "rb")
        else:
            opener = file_opener

        with opener(abs_path) as f:
            ret = cls.from_file(
                f,
                config_dir=config_dir,
                repo_dir=repo_dir,
                max_include_depth=max_include_depth,
                file_opener=file_opener,
            )
            ret.path = abs_path
            return ret

    def write_to_path(self, path: Optional[Union[str, os.PathLike]] = None) -> None:
        """Write configuration to a file on disk."""
        if path is None:
            if self.path is None:
                raise ValueError("No path specified and no default path available")
            path_to_use: Union[str, os.PathLike] = self.path
        else:
            path_to_use = path
        with GitFile(path_to_use, "wb") as f:
            self.write_to_file(f)

    def write_to_file(self, f: BinaryIO) -> None:
        """Write configuration to a file-like object."""
        for section, values in self._values.items():
            try:
                section_name, subsection_name = section
            except ValueError:
                (section_name,) = section
                subsection_name = None
            if subsection_name is None:
                f.write(b"[" + section_name + b"]\n")
            else:
                f.write(b"[" + section_name + b' "' + subsection_name + b'"]\n')
            for key, value in values.items():
                value = _format_string(value)
                f.write(b"\t" + key + b" = " + value + b"\n")


def get_xdg_config_home_path(*path_segments):
    xdg_config_home = os.environ.get(
        "XDG_CONFIG_HOME",
        os.path.expanduser("~/.config/"),
    )
    return os.path.join(xdg_config_home, *path_segments)


def _find_git_in_win_path():
    for exe in ("git.exe", "git.cmd"):
        for path in os.environ.get("PATH", "").split(";"):
            if os.path.exists(os.path.join(path, exe)):
                # in windows native shells (powershell/cmd) exe path is
                # .../Git/bin/git.exe or .../Git/cmd/git.exe
                #
                # in git-bash exe path is .../Git/mingw64/bin/git.exe
                git_dir, _bin_dir = os.path.split(path)
                yield git_dir
                parent_dir, basename = os.path.split(git_dir)
                if basename == "mingw32" or basename == "mingw64":
                    yield parent_dir
                break


def _find_git_in_win_reg():
    import platform
    import winreg

    if platform.machine() == "AMD64":
        subkey = (
            "SOFTWARE\\Wow6432Node\\Microsoft\\Windows\\"
            "CurrentVersion\\Uninstall\\Git_is1"
        )
    else:
        subkey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Git_is1"

    for key in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):  # type: ignore
        with suppress(OSError):
            with winreg.OpenKey(key, subkey) as k:  # type: ignore
                val, typ = winreg.QueryValueEx(k, "InstallLocation")  # type: ignore
                if typ == winreg.REG_SZ:  # type: ignore
                    yield val


# There is no set standard for system config dirs on windows. We try the
# following:
#   - %PROGRAMDATA%/Git/config - (deprecated) Windows config dir per CGit docs
#   - %PROGRAMFILES%/Git/etc/gitconfig - Git for Windows (msysgit) config dir
#     Used if CGit installation (Git/bin/git.exe) is found in PATH in the
#     system registry
def get_win_system_paths():
    if "PROGRAMDATA" in os.environ:
        yield os.path.join(os.environ["PROGRAMDATA"], "Git", "config")

    for git_dir in _find_git_in_win_path():
        yield os.path.join(git_dir, "etc", "gitconfig")
    for git_dir in _find_git_in_win_reg():
        yield os.path.join(git_dir, "etc", "gitconfig")


class StackedConfig(Config):
    """Configuration which reads from multiple config files.."""

    def __init__(
        self, backends: list[ConfigFile], writable: Optional[ConfigFile] = None
    ) -> None:
        self.backends = backends
        self.writable = writable

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} for {self.backends!r}>"

    @classmethod
    def default(cls) -> "StackedConfig":
        return cls(cls.default_backends())

    @classmethod
    def default_backends(cls) -> list[ConfigFile]:
        """Retrieve the default configuration.

        See git-config(1) for details on the files searched.
        """
        paths = []
        paths.append(os.path.expanduser("~/.gitconfig"))
        paths.append(get_xdg_config_home_path("git", "config"))

        if "GIT_CONFIG_NOSYSTEM" not in os.environ:
            paths.append("/etc/gitconfig")
            if sys.platform == "win32":
                paths.extend(get_win_system_paths())

        backends = []
        for path in paths:
            try:
                cf = ConfigFile.from_path(path)
            except FileNotFoundError:
                continue
            backends.append(cf)
        return backends

    def get(self, section: SectionLike, name: NameLike) -> Value:
        if not isinstance(section, tuple):
            section = (section,)
        for backend in self.backends:
            try:
                return backend.get(section, name)
            except KeyError:
                pass
        raise KeyError(name)

    def get_multivar(self, section: SectionLike, name: NameLike) -> Iterator[Value]:
        if not isinstance(section, tuple):
            section = (section,)
        for backend in self.backends:
            try:
                yield from backend.get_multivar(section, name)
            except KeyError:
                pass

    def set(
        self, section: SectionLike, name: NameLike, value: Union[ValueLike, bool]
    ) -> None:
        if self.writable is None:
            raise NotImplementedError(self.set)
        return self.writable.set(section, name, value)

    def sections(self) -> Iterator[Section]:
        seen = set()
        for backend in self.backends:
            for section in backend.sections():
                if section not in seen:
                    seen.add(section)
                    yield section


def read_submodules(
    path: Union[str, os.PathLike],
) -> Iterator[tuple[bytes, bytes, bytes]]:
    """Read a .gitmodules file."""
    cfg = ConfigFile.from_path(path)
    return parse_submodules(cfg)


def parse_submodules(config: ConfigFile) -> Iterator[tuple[bytes, bytes, bytes]]:
    """Parse a gitmodules GitConfig file, returning submodules.

    Args:
      config: A `ConfigFile`
    Returns:
      list of tuples (submodule path, url, name),
        where name is quoted part of the section's name.
    """
    for section in config.keys():
        section_kind, section_name = section
        if section_kind == b"submodule":
            try:
                sm_path = config.get(section, b"path")
                sm_url = config.get(section, b"url")
                yield (sm_path, sm_url, section_name)
            except KeyError:
                # If either path or url is missing, just ignore this
                # submodule entry and move on to the next one. This is
                # how git itself handles malformed .gitmodule entries.
                pass


def iter_instead_of(config: Config, push: bool = False) -> Iterable[tuple[str, str]]:
    """Iterate over insteadOf / pushInsteadOf values."""
    for section in config.sections():
        if section[0] != b"url":
            continue
        replacement = section[1]
        try:
            needles = list(config.get_multivar(section, "insteadOf"))
        except KeyError:
            needles = []
        if push:
            try:
                needles += list(config.get_multivar(section, "pushInsteadOf"))
            except KeyError:
                pass
        for needle in needles:
            assert isinstance(needle, bytes)
            yield needle.decode("utf-8"), replacement.decode("utf-8")


def apply_instead_of(config: Config, orig_url: str, push: bool = False) -> str:
    """Apply insteadOf / pushInsteadOf to a URL."""
    longest_needle = ""
    updated_url = orig_url
    for needle, replacement in iter_instead_of(config, push):
        if not orig_url.startswith(needle):
            continue
        if len(longest_needle) < len(needle):
            longest_needle = needle
            updated_url = replacement + orig_url[len(needle) :]
    return updated_url
