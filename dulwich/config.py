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

from dulwich.file import GitFile


class Config(object):
    """A Git configuration."""



class ConfigFile(Config):
    """A Git configuration file, like .git/config or ~/.gitconfig."""

    def __init__(self):
        """Create a new ConfigFile."""

    def __eq__(self, other):
        return isinstance(other, self.__class__)

    @classmethod
    def from_file(cls, f):
        """Read configuration from a file-like object."""
        ret = cls()
        # FIXME
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
        # FIXME


class StackedConfig(Config):
    """Configuration which reads from multiple config files.."""

    def __init__(self, backends):
        self._backends = backends

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self._backends)

    @classmethod
    def default(cls, for_path=None):
        """Retrieve the default configuration.

        This will look in the repository configuration (if for_path is
        specified), the users' home directory and the system
        configuration.
        """
        paths = []
        if for_path is not None:
            paths.append(for_path)
        paths.append(os.path.expanduser("~/.gitconfig"))
        paths.append("/etc/gitconfig")
        backends = []
        for path in paths:
            try:
                cf = ConfigFile.from_path(path)
            except IOError, e:
                if e.errno != errno.ENOENT:
                    raise
                else:
                    continue
            backends.append(cf)
        return cls(backends)
