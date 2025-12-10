# config.py - Reading and writing Git config files
# Copyright (C) 2011-2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Reading and writing Git configuration files.

Todo:
 * preserve formatting when updating configuration files
"""

__all__ = [
    "DEFAULT_MAX_INCLUDE_DEPTH",
    "MAX_INCLUDE_FILE_SIZE",
    "CaseInsensitiveOrderedMultiDict",
    "ConditionMatcher",
    "Config",
    "ConfigDict",
    "ConfigFile",
    "ConfigKey",
    "ConfigValue",
    "FileOpener",
    "StackedConfig",
    "apply_instead_of",
    "get_win_legacy_system_paths",
    "get_win_system_paths",
    "get_xdg_config_home_path",
    "iter_instead_of",
    "lower_key",
    "match_glob_pattern",
    "parse_submodules",
    "read_submodules",
]

import logging
import os
import re
import sys
from collections.abc import (
    Callable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    MutableMapping,
    ValuesView,
)
from contextlib import suppress
from pathlib import Path
from typing import (
    IO,
    Generic,
    TypeVar,
    overload,
)

from .file import GitFile, _GitFile

ConfigKey = str | bytes | tuple[str | bytes, ...]
ConfigValue = str | bytes | bool | int

logger = logging.getLogger(__name__)

# Type for file opener callback
FileOpener = Callable[[str | os.PathLike[str]], IO[bytes]]

# Type for includeIf condition matcher
# Takes the condition value (e.g., "main" for onbranch:main) and returns bool
ConditionMatcher = Callable[[str], bool]

# Security limits for include files
MAX_INCLUDE_FILE_SIZE = 1024 * 1024  # 1MB max for included config files
DEFAULT_MAX_INCLUDE_DEPTH = 10  # Maximum recursion depth for includes


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


def match_glob_pattern(value: str, pattern: str) -> bool:
    r"""Match a value against a glob pattern.

    Supports simple glob patterns like ``*`` and ``**``.

    Raises:
        ValueError: If the pattern is invalid
    """
    # Convert glob pattern to regex
    pattern_escaped = re.escape(pattern)
    # Replace escaped \*\* with .* (match anything)
    pattern_escaped = pattern_escaped.replace(r"\*\*", ".*")
    # Replace escaped \* with [^/]* (match anything except /)
    pattern_escaped = pattern_escaped.replace(r"\*", "[^/]*")
    # Anchor the pattern
    pattern_regex = f"^{pattern_escaped}$"

    try:
        return bool(re.match(pattern_regex, value))
    except re.error as e:
        raise ValueError(f"Invalid glob pattern {pattern!r}: {e}")


def lower_key(key: ConfigKey) -> ConfigKey:
    """Convert a config key to lowercase, preserving subsection case.

    Args:
      key: Configuration key (str, bytes, or tuple)

    Returns:
      Key with section names lowercased, subsection names preserved

    Raises:
      TypeError: If key is not str, bytes, or tuple
    """
    if isinstance(key, (bytes, str)):
        return key.lower()

    if isinstance(key, tuple):
        # For config sections, only lowercase the section name (first element)
        # but preserve the case of subsection names (remaining elements)
        if len(key) > 0:
            first = key[0]
            assert isinstance(first, (bytes, str))
            return (first.lower(), *key[1:])
        return key

    raise TypeError(key)


K = TypeVar("K", bound=ConfigKey)  # Key type must be ConfigKey
V = TypeVar("V")  # Value type
_T = TypeVar("_T")  # For get() default parameter


class CaseInsensitiveOrderedMultiDict(MutableMapping[K, V], Generic[K, V]):
    """A case-insensitive ordered dictionary that can store multiple values per key.

    This class maintains the order of insertions and allows multiple values
    for the same key. Keys are compared case-insensitively.
    """

    def __init__(self, default_factory: Callable[[], V] | None = None) -> None:
        """Initialize a CaseInsensitiveOrderedMultiDict.

        Args:
          default_factory: Optional factory function for default values
        """
        self._real: list[tuple[K, V]] = []
        self._keyed: dict[ConfigKey, V] = {}
        self._default_factory = default_factory

    @classmethod
    def make(
        cls,
        dict_in: "MutableMapping[K, V] | CaseInsensitiveOrderedMultiDict[K, V] | None" = None,
        default_factory: Callable[[], V] | None = None,
    ) -> "CaseInsensitiveOrderedMultiDict[K, V]":
        """Create a CaseInsensitiveOrderedMultiDict from an existing mapping.

        Args:
          dict_in: Optional mapping to initialize from
          default_factory: Optional factory function for default values

        Returns:
          New CaseInsensitiveOrderedMultiDict instance

        Raises:
          TypeError: If dict_in is not a mapping or None
        """
        if isinstance(dict_in, cls):
            return dict_in

        out = cls(default_factory=default_factory)

        if dict_in is None:
            return out

        if not isinstance(dict_in, MutableMapping):
            raise TypeError

        for key, value in dict_in.items():
            out[key] = value

        return out

    def __len__(self) -> int:
        """Return the number of unique keys in the dictionary."""
        return len(self._keyed)

    def keys(self) -> KeysView[K]:
        """Return a view of the dictionary's keys."""
        # Return a view of the original keys (not lowercased)
        # We need to deduplicate since _real can have duplicates
        seen = set()
        unique_keys = []
        for k, _ in self._real:
            lower = lower_key(k)
            if lower not in seen:
                seen.add(lower)
                unique_keys.append(k)
        from collections.abc import KeysView as ABCKeysView

        class UniqueKeysView(ABCKeysView[K]):
            def __init__(self, keys: list[K]):
                self._keys = keys

            def __contains__(self, key: object) -> bool:
                return key in self._keys

            def __iter__(self) -> Iterator[K]:
                return iter(self._keys)

            def __len__(self) -> int:
                return len(self._keys)

        return UniqueKeysView(unique_keys)

    def items(self) -> ItemsView[K, V]:
        """Return a view of the dictionary's (key, value) pairs in insertion order."""

        # Return a view that iterates over the real list to preserve order
        class OrderedItemsView(ItemsView[K, V]):
            """Items view that preserves insertion order."""

            def __init__(self, mapping: CaseInsensitiveOrderedMultiDict[K, V]):
                self._mapping = mapping

            def __iter__(self) -> Iterator[tuple[K, V]]:
                return iter(self._mapping._real)

            def __len__(self) -> int:
                return len(self._mapping._real)

            def __contains__(self, item: object) -> bool:
                if not isinstance(item, tuple) or len(item) != 2:
                    return False
                key, value = item
                return any(k == key and v == value for k, v in self._mapping._real)

        return OrderedItemsView(self)

    def __iter__(self) -> Iterator[K]:
        """Iterate over the dictionary's keys."""
        # Return iterator over original keys (not lowercased), deduplicated
        seen = set()
        for k, _ in self._real:
            lower = lower_key(k)
            if lower not in seen:
                seen.add(lower)
                yield k

    def values(self) -> ValuesView[V]:
        """Return a view of the dictionary's values."""
        return self._keyed.values()

    def __setitem__(self, key: K, value: V) -> None:
        """Set a value for a key, appending to existing values."""
        self._real.append((key, value))
        self._keyed[lower_key(key)] = value

    def set(self, key: K, value: V) -> None:
        """Set a value for a key, replacing all existing values.

        Args:
          key: The key to set
          value: The value to set
        """
        # This method replaces all existing values for the key
        lower = lower_key(key)
        self._real = [(k, v) for k, v in self._real if lower_key(k) != lower]
        self._real.append((key, value))
        self._keyed[lower] = value

    def __delitem__(self, key: K) -> None:
        """Delete all values for a key.

        Raises:
          KeyError: If the key is not found
        """
        lower_k = lower_key(key)
        del self._keyed[lower_k]
        for i, (actual, unused_value) in reversed(list(enumerate(self._real))):
            if lower_key(actual) == lower_k:
                del self._real[i]

    def __getitem__(self, item: K) -> V:
        """Get the last value for a key.

        Raises:
          KeyError: If the key is not found
        """
        return self._keyed[lower_key(item)]

    def get(self, key: K, /, default: V | _T | None = None) -> V | _T | None:  # type: ignore[override]
        """Get the last value for a key, or a default if not found.

        Args:
          key: The key to look up
          default: Default value to return if key not found

        Returns:
          The value for the key, or default/default_factory result if not found
        """
        try:
            return self[key]
        except KeyError:
            if default is not None:
                return default
            elif self._default_factory is not None:
                return self._default_factory()
            else:
                return None

    def get_all(self, key: K) -> Iterator[V]:
        """Get all values for a key in insertion order.

        Args:
          key: The key to look up

        Returns:
          Iterator of all values for the key
        """
        lowered_key = lower_key(key)
        for actual, value in self._real:
            if lower_key(actual) == lowered_key:
                yield value

    def setdefault(self, key: K, default: V | None = None) -> V:
        """Get value for key, setting it to default if not present.

        Args:
          key: The key to look up
          default: Default value to set if key not found

        Returns:
          The existing value or the newly set default

        Raises:
          KeyError: If key not found and no default or default_factory
        """
        try:
            return self[key]
        except KeyError:
            if default is not None:
                self[key] = default
                return default
            elif self._default_factory is not None:
                value = self._default_factory()
                self[key] = value
                return value
            else:
                raise


Name = bytes
NameLike = bytes | str
Section = tuple[bytes, ...]
SectionLike = bytes | str | tuple[bytes | str, ...]
Value = bytes
ValueLike = bytes | str


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
    def get_boolean(self, section: SectionLike, name: NameLike) -> bool | None: ...

    def get_boolean(
        self, section: SectionLike, name: NameLike, default: bool | None = None
    ) -> bool | None:
        """Retrieve a configuration setting as boolean.

        Args:
          section: Tuple with section name and optional subsection name
          name: Name of the setting, including section and possible
            subsection.
          default: Default value if setting is not found

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
        self, section: SectionLike, name: NameLike, value: ValueLike | bool
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


class ConfigDict(Config):
    """Git configuration stored in a dictionary."""

    def __init__(
        self,
        values: MutableMapping[Section, CaseInsensitiveOrderedMultiDict[Name, Value]]
        | None = None,
        encoding: str | None = None,
    ) -> None:
        """Create a new ConfigDict."""
        if encoding is None:
            encoding = sys.getdefaultencoding()
        self.encoding = encoding
        self._values: CaseInsensitiveOrderedMultiDict[
            Section, CaseInsensitiveOrderedMultiDict[Name, Value]
        ] = CaseInsensitiveOrderedMultiDict.make(
            values, default_factory=CaseInsensitiveOrderedMultiDict
        )

    def __repr__(self) -> str:
        """Return string representation of ConfigDict."""
        return f"{self.__class__.__name__}({self._values!r})"

    def __eq__(self, other: object) -> bool:
        """Check equality with another ConfigDict."""
        return isinstance(other, self.__class__) and other._values == self._values

    def __getitem__(self, key: Section) -> CaseInsensitiveOrderedMultiDict[Name, Value]:
        """Get configuration values for a section.

        Raises:
          KeyError: If section not found
        """
        return self._values.__getitem__(key)

    def __setitem__(
        self, key: Section, value: CaseInsensitiveOrderedMultiDict[Name, Value]
    ) -> None:
        """Set configuration values for a section."""
        return self._values.__setitem__(key, value)

    def __delitem__(self, key: Section) -> None:
        """Delete a configuration section.

        Raises:
          KeyError: If section not found
        """
        return self._values.__delitem__(key)

    def __iter__(self) -> Iterator[Section]:
        """Iterate over configuration sections."""
        return self._values.__iter__()

    def __len__(self) -> int:
        """Return the number of sections."""
        return self._values.__len__()

    def keys(self) -> KeysView[Section]:
        """Return a view of section names."""
        return self._values.keys()

    @classmethod
    def _parse_setting(cls, name: str) -> tuple[str, str | None, str]:
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
        """Get multiple values for a configuration setting.

        Args:
            section: Section name
            name: Setting name

        Returns:
            Iterator of configuration values
        """
        section, name = self._check_section_and_name(section, name)

        if len(section) > 1:
            try:
                return self._values[section].get_all(name)
            except KeyError:
                pass

        return self._values[(section[0],)].get_all(name)

    def get(
        self,
        section: SectionLike,
        name: NameLike,
    ) -> Value:
        """Get a configuration value.

        Args:
            section: Section name
            name: Setting name

        Returns:
            Configuration value

        Raises:
            KeyError: if the value is not set
        """
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
        value: ValueLike | bool,
    ) -> None:
        """Set a configuration value.

        Args:
            section: Section name
            name: Setting name
            value: Configuration value
        """
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
        value: ValueLike | bool,
    ) -> None:
        """Add a value to a configuration setting, creating a multivar if needed."""
        section, name = self._check_section_and_name(section, name)

        if isinstance(value, bool):
            value = b"true" if value else b"false"

        if not isinstance(value, bytes):
            value = value.encode(self.encoding)

        self._values.setdefault(section)[name] = value

    def remove(self, section: SectionLike, name: NameLike) -> None:
        """Remove a configuration setting.

        Args:
            section: Section name
            name: Setting name

        Raises:
            KeyError: If the section or name doesn't exist
        """
        section, name = self._check_section_and_name(section, name)
        del self._values[section][name]

    def items(self, section: SectionLike) -> Iterator[tuple[Name, Value]]:
        """Get items in a section."""
        section_bytes, _ = self._check_section_and_name(section, b"")
        section_dict = self._values.get(section_bytes)
        if section_dict is not None:
            return iter(section_dict.items())
        return iter([])

    def sections(self) -> Iterator[Section]:
        """Get all sections."""
        return iter(self._values.keys())


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
    value_array = bytearray(value.strip())
    ret = bytearray()
    whitespace = bytearray()
    in_quotes = False
    i = 0
    while i < len(value_array):
        c = value_array[i]
        if c == ord(b"\\"):
            i += 1
            if i >= len(value_array):
                # Backslash at end of string - treat as literal backslash
                if whitespace:
                    ret.extend(whitespace)
                    whitespace = bytearray()
                ret.append(ord(b"\\"))
            else:
                try:
                    v = _ESCAPE_TABLE[value_array[i]]
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
        values: MutableMapping[Section, CaseInsensitiveOrderedMultiDict[Name, Value]]
        | None = None,
        encoding: str | None = None,
    ) -> None:
        """Initialize a ConfigFile.

        Args:
          values: Optional mapping of configuration values
          encoding: Optional encoding for the file (defaults to system encoding)
        """
        super().__init__(values=values, encoding=encoding)
        self.path: str | None = None
        self._included_paths: set[str] = set()  # Track included files to prevent cycles

    @classmethod
    def from_file(
        cls,
        f: IO[bytes],
        *,
        config_dir: str | None = None,
        included_paths: set[str] | None = None,
        include_depth: int = 0,
        max_include_depth: int = DEFAULT_MAX_INCLUDE_DEPTH,
        file_opener: FileOpener | None = None,
        condition_matchers: Mapping[str, ConditionMatcher] | None = None,
    ) -> "ConfigFile":
        """Read configuration from a file-like object.

        Args:
            f: File-like object to read from
            config_dir: Directory containing the config file (for relative includes)
            included_paths: Set of already included paths (to prevent cycles)
            include_depth: Current include depth (to prevent infinite recursion)
            max_include_depth: Maximum allowed include depth
            file_opener: Optional callback to open included files
            condition_matchers: Optional dict of condition matchers for includeIf
        """
        if include_depth > max_include_depth:
            # Prevent excessive recursion
            raise ValueError(f"Maximum include depth ({max_include_depth}) exceeded")

        ret = cls()
        if included_paths is not None:
            ret._included_paths = included_paths.copy()

        section: Section | None = None
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
                        include_depth=include_depth,
                        max_include_depth=max_include_depth,
                        file_opener=file_opener,
                        condition_matchers=condition_matchers,
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
                    assert section is not None  # Already checked above
                    ret._values[section][setting] = value

                    # Process include/includeIf directives
                    ret._handle_include_directive(
                        section,
                        setting,
                        value,
                        config_dir=config_dir,
                        include_depth=include_depth,
                        max_include_depth=max_include_depth,
                        file_opener=file_opener,
                        condition_matchers=condition_matchers,
                    )

                    continuation = None
                    setting = None
        return ret

    def _handle_include_directive(
        self,
        section: Section | None,
        setting: bytes,
        value: bytes,
        *,
        config_dir: str | None,
        include_depth: int,
        max_include_depth: int,
        file_opener: FileOpener | None,
        condition_matchers: Mapping[str, ConditionMatcher] | None,
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
                include_depth=include_depth,
                max_include_depth=max_include_depth,
                file_opener=file_opener,
                condition_matchers=condition_matchers,
            )

    def _process_include(
        self,
        section: Section,
        path_value: bytes,
        *,
        config_dir: str | None,
        include_depth: int,
        max_include_depth: int,
        file_opener: FileOpener | None,
        condition_matchers: Mapping[str, ConditionMatcher] | None,
    ) -> None:
        """Process an include or includeIf directive."""
        path_str = path_value.decode(self.encoding, errors="replace")

        # Handle includeIf conditions
        if len(section) > 1 and section[0].lower() == b"includeif":
            condition = section[1].decode(self.encoding, errors="replace")
            if not self._evaluate_includeif_condition(
                condition, config_dir, condition_matchers
            ):
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
            opener: FileOpener
            if file_opener is None:

                def opener(path: str | os.PathLike[str]) -> IO[bytes]:
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
                    included_paths=self._included_paths,
                    include_depth=include_depth + 1,
                    max_include_depth=max_include_depth,
                    file_opener=file_opener,
                    condition_matchers=condition_matchers,
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

    def _resolve_include_path(self, path: str, config_dir: str | None) -> str | None:
        """Resolve an include path to an absolute path."""
        # Expand ~ to home directory
        path = os.path.expanduser(path)

        # If path is relative and we have a config directory, make it relative to that
        if not os.path.isabs(path) and config_dir:
            path = os.path.join(config_dir, path)

        return path

    def _evaluate_includeif_condition(
        self,
        condition: str,
        config_dir: str | None = None,
        condition_matchers: Mapping[str, ConditionMatcher] | None = None,
    ) -> bool:
        """Evaluate an includeIf condition."""
        # Try custom matchers first if provided
        if condition_matchers:
            for prefix, matcher in condition_matchers.items():
                if condition.startswith(prefix):
                    return matcher(condition[len(prefix) :])

        # Fall back to built-in matchers
        if condition.startswith("hasconfig:"):
            return self._evaluate_hasconfig_condition(condition[10:])
        else:
            # Unknown condition type - log and ignore (Git behavior)
            logger.debug("Unknown includeIf condition: %r", condition)
            return False

    def _evaluate_hasconfig_condition(self, condition: str) -> bool:
        """Evaluate a hasconfig condition.

        Format: hasconfig:config.key:pattern
        Example: hasconfig:remote.*.url:ssh://org-*@github.com/**
        """
        # Split on the first colon to separate config key from pattern
        parts = condition.split(":", 1)
        if len(parts) != 2:
            logger.debug("Invalid hasconfig condition format: %r", condition)
            return False

        config_key, pattern = parts

        # Parse the config key to get section and name
        key_parts = config_key.split(".", 2)
        if len(key_parts) < 2:
            logger.debug("Invalid hasconfig config key: %r", config_key)
            return False

        # Handle wildcards in section names (e.g., remote.*)
        if len(key_parts) == 3 and key_parts[1] == "*":
            # Match any subsection
            section_prefix = key_parts[0].encode(self.encoding)
            name = key_parts[2].encode(self.encoding)

            # Check all sections that match the pattern
            for section in self.sections():
                if len(section) == 2 and section[0] == section_prefix:
                    try:
                        values = list(self.get_multivar(section, name))
                        for value in values:
                            if self._match_hasconfig_pattern(value, pattern):
                                return True
                    except KeyError:
                        continue
        else:
            # Direct section lookup
            if len(key_parts) == 2:
                section = (key_parts[0].encode(self.encoding),)
                name = key_parts[1].encode(self.encoding)
            else:
                section = (
                    key_parts[0].encode(self.encoding),
                    key_parts[1].encode(self.encoding),
                )
                name = key_parts[2].encode(self.encoding)

            try:
                values = list(self.get_multivar(section, name))
                for value in values:
                    if self._match_hasconfig_pattern(value, pattern):
                        return True
            except KeyError:
                pass

        return False

    def _match_hasconfig_pattern(self, value: bytes, pattern: str) -> bool:
        """Match a config value against a hasconfig pattern.

        Supports simple glob patterns like ``*`` and ``**``.
        """
        value_str = value.decode(self.encoding, errors="replace")
        return match_glob_pattern(value_str, pattern)

    @classmethod
    def from_path(
        cls,
        path: str | os.PathLike[str],
        *,
        max_include_depth: int = DEFAULT_MAX_INCLUDE_DEPTH,
        file_opener: FileOpener | None = None,
        condition_matchers: Mapping[str, ConditionMatcher] | None = None,
    ) -> "ConfigFile":
        """Read configuration from a file on disk.

        Args:
            path: Path to the configuration file
            max_include_depth: Maximum allowed include depth
            file_opener: Optional callback to open included files
            condition_matchers: Optional dict of condition matchers for includeIf
        """
        abs_path = os.fspath(path)
        config_dir = os.path.dirname(abs_path)

        # Use provided file opener or default to GitFile
        opener: FileOpener
        if file_opener is None:

            def opener(p: str | os.PathLike[str]) -> IO[bytes]:
                return GitFile(p, "rb")
        else:
            opener = file_opener

        with opener(abs_path) as f:
            ret = cls.from_file(
                f,
                config_dir=config_dir,
                max_include_depth=max_include_depth,
                file_opener=file_opener,
                condition_matchers=condition_matchers,
            )
            ret.path = abs_path
            return ret

    def write_to_path(self, path: str | os.PathLike[str] | None = None) -> None:
        """Write configuration to a file on disk."""
        if path is None:
            if self.path is None:
                raise ValueError("No path specified and no default path available")
            path_to_use: str | os.PathLike[str] = self.path
        else:
            path_to_use = path
        with GitFile(path_to_use, "wb") as f:
            self.write_to_file(f)

    def write_to_file(self, f: IO[bytes] | _GitFile) -> None:
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


def get_xdg_config_home_path(*path_segments: str) -> str:
    """Get a path in the XDG config home directory.

    Args:
      *path_segments: Path segments to join to the XDG config home

    Returns:
      Full path in XDG config home directory
    """
    xdg_config_home = os.environ.get(
        "XDG_CONFIG_HOME",
        os.path.expanduser("~/.config/"),
    )
    return os.path.join(xdg_config_home, *path_segments)


def _find_git_in_win_path() -> Iterator[str]:
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


def _find_git_in_win_reg() -> Iterator[str]:
    import platform
    import winreg

    if platform.machine() == "AMD64":
        subkey = (
            "SOFTWARE\\Wow6432Node\\Microsoft\\Windows\\"
            "CurrentVersion\\Uninstall\\Git_is1"
        )
    else:
        subkey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Git_is1"

    for key in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):  # type: ignore[attr-defined,unused-ignore]
        with suppress(OSError):
            with winreg.OpenKey(key, subkey) as k:  # type: ignore[attr-defined,unused-ignore]
                val, typ = winreg.QueryValueEx(k, "InstallLocation")  # type: ignore[attr-defined,unused-ignore]
                if typ == winreg.REG_SZ:  # type: ignore[attr-defined,unused-ignore]
                    yield val


# There is no set standard for system config dirs on windows. We try the
# following:
#   - %PROGRAMFILES%/Git/etc/gitconfig - Git for Windows (msysgit) config dir
#     Used if CGit installation (Git/bin/git.exe) is found in PATH in the
#     system registry
def get_win_system_paths() -> Iterator[str]:
    """Get current Windows system Git config paths.

    Only returns the current Git for Windows config location, not legacy paths.
    """
    # Try to find Git installation from PATH first
    for git_dir in _find_git_in_win_path():
        yield os.path.join(git_dir, "etc", "gitconfig")
        return  # Only use the first found path

    # Fall back to registry if not found in PATH
    for git_dir in _find_git_in_win_reg():
        yield os.path.join(git_dir, "etc", "gitconfig")
        return  # Only use the first found path


def get_win_legacy_system_paths() -> Iterator[str]:
    """Get legacy Windows system Git config paths.

    Returns all possible config paths including deprecated locations.
    This function can be used for diagnostics or migration purposes.
    """
    # Include deprecated PROGRAMDATA location
    if "PROGRAMDATA" in os.environ:
        yield os.path.join(os.environ["PROGRAMDATA"], "Git", "config")

    # Include all Git installations found
    for git_dir in _find_git_in_win_path():
        yield os.path.join(git_dir, "etc", "gitconfig")
    for git_dir in _find_git_in_win_reg():
        yield os.path.join(git_dir, "etc", "gitconfig")


class StackedConfig(Config):
    """Configuration which reads from multiple config files.."""

    def __init__(
        self, backends: list[ConfigFile], writable: ConfigFile | None = None
    ) -> None:
        """Initialize a StackedConfig.

        Args:
          backends: List of config files to read from (in order of precedence)
          writable: Optional config file to write changes to
        """
        self.backends = backends
        self.writable = writable

    def __repr__(self) -> str:
        """Return string representation of StackedConfig."""
        return f"<{self.__class__.__name__} for {self.backends!r}>"

    @classmethod
    def default(cls) -> "StackedConfig":
        """Create a StackedConfig with default system/user config files.

        Returns:
          StackedConfig with default configuration files loaded
        """
        return cls(cls.default_backends())

    @classmethod
    def default_backends(cls) -> list[ConfigFile]:
        """Retrieve the default configuration.

        See git-config(1) for details on the files searched.
        """
        paths = []

        # Handle GIT_CONFIG_GLOBAL - overrides user config paths
        try:
            paths.append(os.environ["GIT_CONFIG_GLOBAL"])
        except KeyError:
            paths.append(os.path.expanduser("~/.gitconfig"))
            paths.append(get_xdg_config_home_path("git", "config"))

        # Handle GIT_CONFIG_SYSTEM and GIT_CONFIG_NOSYSTEM
        try:
            paths.append(os.environ["GIT_CONFIG_SYSTEM"])
        except KeyError:
            if "GIT_CONFIG_NOSYSTEM" not in os.environ:
                paths.append("/etc/gitconfig")
                if sys.platform == "win32":
                    paths.extend(get_win_system_paths())

        logger.debug("Loading gitconfig from paths: %s", paths)

        backends = []
        for path in paths:
            try:
                cf = ConfigFile.from_path(path)
                logger.debug("Successfully loaded gitconfig from: %s", path)
            except FileNotFoundError:
                logger.debug("Gitconfig file not found: %s", path)
                continue
            backends.append(cf)
        return backends

    def get(self, section: SectionLike, name: NameLike) -> Value:
        """Get value from configuration."""
        if not isinstance(section, tuple):
            section = (section,)
        for backend in self.backends:
            try:
                return backend.get(section, name)
            except KeyError:
                pass
        raise KeyError(name)

    def get_multivar(self, section: SectionLike, name: NameLike) -> Iterator[Value]:
        """Get multiple values from configuration."""
        if not isinstance(section, tuple):
            section = (section,)
        for backend in self.backends:
            try:
                yield from backend.get_multivar(section, name)
            except KeyError:
                pass

    def set(
        self, section: SectionLike, name: NameLike, value: ValueLike | bool
    ) -> None:
        """Set value in configuration."""
        if self.writable is None:
            raise NotImplementedError(self.set)
        return self.writable.set(section, name, value)

    def sections(self) -> Iterator[Section]:
        """Get all sections."""
        seen = set()
        for backend in self.backends:
            for section in backend.sections():
                if section not in seen:
                    seen.add(section)
                    yield section


def read_submodules(
    path: str | os.PathLike[str],
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
    for section in config.sections():
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
