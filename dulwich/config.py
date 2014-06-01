# config.py - Reading and writing Git config files
# Copyright (C) 2011-2013 Jelmer Vernooij <jelmer@samba.org>
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

TODO:
 * preserve formatting when updating configuration files
 * treat subsection names as case-insensitive for [branch.foo] style
   subsections
"""

import errno
import os
import re

from collections import (
    OrderedDict,
    MutableMapping,
    )


from dulwich.file import GitFile


class Config(object):
    """A Git configuration."""

    def get(self, section, name):
        """Retrieve the contents of a configuration setting.

        :param section: Tuple with section name and optional subsection namee
        :param subsection: Subsection name
        :return: Contents of the setting
        :raise KeyError: if the value is not set
        """
        raise NotImplementedError(self.get)

    def get_boolean(self, section, name, default=None):
        """Retrieve a configuration setting as boolean.

        :param section: Tuple with section name and optional subsection namee
        :param name: Name of the setting, including section and possible
            subsection.
        :return: Contents of the setting
        :raise KeyError: if the value is not set
        """
        try:
            value = self.get(section, name)
        except KeyError:
            return default
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        raise ValueError("not a valid boolean string: %r" % value)

    def set(self, section, name, value):
        """Set a configuration value.

        :param section: Tuple with section name and optional subsection namee
        :param name: Name of the configuration value, including section
            and optional subsection
        :param: Value of the setting
        """
        raise NotImplementedError(self.set)

    def iteritems(self, section):
        """Iterate over the configuration pairs for a specific section.

        :param section: Tuple with section name and optional subsection namee
        :return: Iterator over (name, value) pairs
        """
        raise NotImplementedError(self.iteritems)

    def itersections(self):
        """Iterate over the sections.

        :return: Iterator over section tuples
        """
        raise NotImplementedError(self.itersections)


class ConfigDict(Config, MutableMapping):
    """Git configuration stored in a dictionary."""

    def __init__(self, values=None):
        """Create a new ConfigDict."""
        if values is None:
            values = OrderedDict()
        self._values = values

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._values)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            other._values == self._values)

    def __getitem__(self, key):
        return self._values.__getitem__(key)

    def __setitem__(self, key, value):
        return self._values.__setitem__(key, value)

    def __delitem__(self, key):
        return self._values.__delitem__(key)

    def __iter__(self):
        return self._values.__iter__()

    def __len__(self):
        return self._values.__len__()

    @classmethod
    def _parse_setting(cls, name):
        parts = name.split(".")
        if len(parts) == 3:
            return (parts[0], parts[1], parts[2])
        else:
            return (parts[0], None, parts[1])

    def get(self, section, name):
        if isinstance(section, basestring):
            section = (section, )
        if len(section) > 1:
            try:
                return self._values[section][name]
            except KeyError:
                pass
        return self._values[(section[0],)][name]

    def set(self, section, name, value):
        if isinstance(section, basestring):
            section = (section, )
        self._values.setdefault(section, OrderedDict())[name] = value

    def iteritems(self, section):
        return self._values.get(section, OrderedDict()).iteritems()

    def itersections(self):
        return self._values.keys()


def _format_string(value):
    if (value.startswith(" ") or
        value.startswith("\t") or
        value.endswith(" ") or
        value.endswith("\t")):
        return '"%s"' % _escape_value(value)
    return _escape_value(value)


def _parse_string(value):
    value = value.strip()
    ret = []
    block = []
    in_quotes = False
    for c in value:
        if c == "\"":
            in_quotes = (not in_quotes)
            ret.append(_unescape_value("".join(block)))
            block = []
        elif c in ("#", ";") and not in_quotes:
            # the rest of the line is a comment
            break
        else:
            block.append(c)

    if in_quotes:
        raise ValueError("value starts with quote but lacks end quote")

    ret.append(_unescape_value("".join(block)).rstrip())

    return "".join(ret)


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


def _check_variable_name(name):
    for c in name:
        if not c.isalnum() and c != '-':
            return False
    return True


def _check_section_name(name):
    for c in name:
        if not c.isalnum() and c not in ('-', '.'):
            return False
    return True


def _strip_comments(line):
    line = line.split("#")[0]
    line = line.split(";")[0]
    return line


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
                if len(line) > 0 and line[0] == "[":
                    line = _strip_comments(line).rstrip()
                    last = line.index("]")
                    if last == -1:
                        raise ValueError("expected trailing ]")
                    pts = line[1:last].split(" ", 1)
                    line = line[last+1:]
                    pts[0] = pts[0].lower()
                    if len(pts) == 2:
                        if pts[1][0] != "\"" or pts[1][-1] != "\"":
                            raise ValueError(
                                "Invalid subsection " + pts[1])
                        else:
                            pts[1] = pts[1][1:-1]
                        if not _check_section_name(pts[0]):
                            raise ValueError("invalid section name %s" %
                                             pts[0])
                        section = (pts[0], pts[1])
                    else:
                        if not _check_section_name(pts[0]):
                            raise ValueError("invalid section name %s" %
                                    pts[0])
                        pts = pts[0].split(".", 1)
                        if len(pts) == 2:
                            section = (pts[0], pts[1])
                        else:
                            section = (pts[0], )
                    ret._values[section] = OrderedDict()
                if _strip_comments(line).strip() == "":
                    continue
                if section is None:
                    raise ValueError("setting %r without section" % line)
                try:
                    setting, value = line.split("=", 1)
                except ValueError:
                    setting = line
                    value = "true"
                setting = setting.strip().lower()
                if not _check_variable_name(setting):
                    raise ValueError("invalid variable name %s" % setting)
                if value.endswith("\\\n"):
                    value = value[:-2]
                    continuation = True
                else:
                    continuation = False
                value = _parse_string(value)
                ret._values[section][setting] = value
                if not continuation:
                    setting = None
            else:  # continuation line
                if line.endswith("\\\n"):
                    line = line[:-2]
                    continuation = True
                else:
                    continuation = False
                value = _parse_string(line)
                ret._values[section][setting] += value
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
        for section, values in self._values.iteritems():
            try:
                section_name, subsection_name = section
            except ValueError:
                (section_name, ) = section
                subsection_name = None
            if subsection_name is None:
                f.write("[%s]\n" % section_name)
            else:
                f.write("[%s \"%s\"]\n" % (section_name, subsection_name))
            for key, value in values.iteritems():
                f.write("\t%s = %s\n" % (key, _escape_value(value)))


class StackedConfig(Config):
    """Configuration which reads from multiple config files.."""

    def __init__(self, backends, writable=None):
        self.backends = backends
        self.writable = writable

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self.backends)

    @classmethod
    def default_backends(cls):
        """Retrieve the default configuration.

        This will look in the users' home directory and the system
        configuration.
        """
        paths = []
        paths.append(os.path.expanduser("~/.gitconfig"))
        paths.append("/etc/gitconfig")
        backends = []
        for path in paths:
            try:
                cf = ConfigFile.from_path(path)
            except (IOError, OSError) as e:
                if e.errno != errno.ENOENT:
                    raise
                else:
                    continue
            backends.append(cf)
        return backends

    def get(self, section, name):
        for backend in self.backends:
            try:
                return backend.get(section, name)
            except KeyError:
                pass
        raise KeyError(name)

    def set(self, section, name, value):
        if self.writable is None:
            raise NotImplementedError(self.set)
        return self.writable.set(section, name, value)
