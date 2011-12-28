# config.py - Reading and writing Git config files
# Copyright (C) 2011 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

"""Reading and writing Git configuration files.

"""

import errno
import os
import re

from dulwich.file import GitFile


class Config(object):
    """A Git configuration."""

    def get(self, name):
        """Retrieve the contents of a configuration setting.
        
        :param name: Name of the setting, including section and 
            possible subsection.
        :return: Contents of the setting
        :raise KeyError: if the value is not set
        """
        raise NotImplementedError(self.get)

    def get_boolean(self, name):
        """Retrieve a configuration setting as boolean.

        :parma name: Name of the setting, including section and possible
            subsection.
        :return: Contents of the setting
        :raise KeyError: if the value is not set
        """
        return bool(self.get(name))

    def set(self, name, value):
        """Set a configuration value.
        
        :param name: Name of the configuration value, including section
            and optional subsection
        :param: Value of the setting
        """
        raise NotImplementedError(self.set)


class ConfigDict(Config):
    """Git configuration stored in a dictionary."""

    def __init__(self):
        """Create a new ConfigDict."""
        self._values = {}

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            other._values == self._values)

    @classmethod
    def _parse_setting(cls, name):
        parts = name.split(".")
        if len(parts) == 3:
            return (parts[0], parts[1], parts[2])
        else:
            return (parts[0], None, parts[1])

    def get(self, name):
        (section, subsection, variable) = self._parse_setting(name)
        if subsection is not None:
            try:
                return self._values[section][subsection][variable]
            except KeyError:
                pass
        return self._values[section][None][variable]

    def set(self, name, value):
        (section_name, subsection_name, variable) = self._parse_setting(name)
        section = self._values.setdefault(section_name, {})
        section.setdefault(subsection_name, {})[variable] = value


def _format_string(value):
    if (value.startswith(" ") or
        value.startswith("\t") or
        value.endswith(" ") or
        value.endswith("\t")):
        return '"%s"' % _escape_value(value)
    return _escape_value(value)


def _parse_string(value):
    value = value.strip()
    if value.startswith("\""):
        if not value.endswith("\""):
            raise ValueError("value starts with quote but lacks end quote")
        return _unescape_value(value[1:-1])
    return _unescape_value(value)


def _unescape_value(value):
    """Unescape a value."""
    def unescape(c):
        return {
            "\\\\": "\\",
            "\\\"": "\"",
            "\\n": "\n",
            "\\t": "\t",
            "\\b": "\b",
            }[c.group(0)]
    return re.sub(r"(\\.)", unescape, value)


def _escape_value(value):
    """Escape a value."""
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t").replace("\"", "\\\"")


class ConfigFile(ConfigDict):
    """A Git configuration file, like .git/config or ~/.gitconfig.
    """

    @classmethod
    def from_file(cls, f):
        """Read configuration from a file-like object."""
        ret = cls()
        section = None
        setting = None
        for lineno, line in enumerate(f.readlines()):
            line = line.lstrip()
            if setting is None:
                if line[0] == "[" and line.rstrip()[-1] == "]":
                    key = line.strip()
                    pts = key[1:-1].split(" ", 1)
                    if len(pts) == 2:
                        if pts[1][0] != "\"" or pts[1][-1] != "\"":
                            raise ValueError(pts[1])
                        section = (pts[0], pts[1][1:-1])
                    else:
                        section = (pts[0], None)
                    ret._values[section[0]] = {section[1]: {}}
                elif "=" in line:
                    setting, value = line.split("=", 1)
                    if section is None:
                        raise ValueError("setting %r without section" % line)
                    setting = setting.strip()
                    if value.endswith("\\\n"):
                        value = value[:-2]
                        continuation = True
                    else:
                        continuation = True
                    value = _parse_string(value)
                    ret._values[section[0]][section[1]][setting] = value
                    if not continuation:
                        setting = None
                else:
                    setting = line.strip()
                    if section is None:
                        raise ValueError("setting %r without section" % line)
                    ret._values[section[0]][section[1]][setting] = ""
                    setting = None
            else:
                if line.endswith("\\\n"):
                    line = line[:-2]
                    continuation = True
                else:
                    continuation = True
                value = _parse_string(line)
                ret._values[section[0]][section[1]][setting] += value
                if not continuation:
                    setting = None
        return ret

    @classmethod
    def from_path(cls, path):
        """Read configuration from a file on disk."""
        f = GitFile(path, 'rb')
        try:
            ret = cls.from_file(f)
            ret.path = path
            return ret
        finally:
            f.close()

    def write_to_path(self, path=None):
        """Write configuration to a file on disk."""
        if path is None:
            path = self.path
        f = GitFile(path, 'wb')
        try:
            self.write_to_file(f)
        finally:
            f.close()

    def write_to_file(self, f):
        """Write configuration to a file-like object."""
        for section_name, section in self._values.iteritems():
            for subsection_name, subsection in section.iteritems():
                if subsection_name is None:
                    f.write("[%s]\n" % section_name)
                else:
                    f.write("[%s \"%s\"]\n" % (section_name, subsection_name))
                for key, value in subsection.iteritems():
                    f.write("%s = %s\n" % (key, _escape_value(value)))


class StackedConfig(Config):
    """Configuration which reads from multiple config files.."""

    def __init__(self, backends):
        self._backends = backends

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self._backends)

    @classmethod
    def default_backends(cls):
        """Retrieve the default configuration.

        This will look in the repository configuration (if for_path is
        specified), the users' home directory and the system
        configuration.
        """
        paths = []
        paths.append(os.path.expanduser("~/.gitconfig"))
        paths.append("/etc/gitconfig")
        backends = []
        for path in paths:
            try:
                cf = ConfigFile.from_path(path)
            except (IOError, OSError), e:
                if e.errno != errno.ENOENT:
                    raise
                else:
                    continue
            backends.append(cf)
        return backends

    def get(self, name):
        for backend in self._backends:
            try:
                return backend.get(name)
            except KeyError:
                pass
        raise KeyError(name)

    def set(self, name, value):
        raise NotImplementedError(self.set)
