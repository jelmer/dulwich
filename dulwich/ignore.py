# Copyright (C) 2017 Jelmer Vernooij <jelmer@jelmer.uk>
#
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

"""Parsing of gitignore files.

For details for the matching rules, see https://git-scm.com/docs/gitignore
"""

import os.path
import re
import sys


def _translate_segment(segment):
    if segment == b"*":
        return b'[^/]+'
    res = b""
    i, n = 0, len(segment)
    while i < n:
        c = segment[i:i+1]
        i = i+1
        if c == b'*':
            res += b'[^/]*'
        elif c == b'?':
            res += b'.'
        elif c == b'[':
            j = i
            if j < n and segment[j:j+1] == b'!':
                j = j+1
            if j < n and segment[j:j+1] == b']':
                j = j+1
            while j < n and segment[j:j+1] != b']':
                j = j+1
            if j >= n:
                res += b'\\['
            else:
                stuff = segment[i:j].replace(b'\\', b'\\\\')
                i = j+1
                if stuff.startswith(b'!'):
                    stuff = b'^' + stuff[1:]
                elif stuff.startswith(b'^'):
                    stuff = b'\\' + stuff
                res += b'[' + stuff + b']'
        else:
            res += re.escape(c)
    return res


def translate(pat):
    """Translate a shell PATTERN to a regular expression.

    There is no way to quote meta-characters.

    Originally copied from fnmatch in Python 2.7, but modified for Dulwich
    to cope with features in Git ignore patterns.
    """

    res = b'(?ms)'

    if b'/' not in pat[:-1]:
        # If there's no slash, this is a filename-based match
        res += b'(.*/)?'

    if pat.startswith(b'**/'):
        # Leading **/
        pat = pat[2:]
        res += b'(.*/)?'

    if pat.startswith(b'/'):
        pat = pat[1:]

    for i, segment in enumerate(pat.split(b'/')):
        if segment == b'**':
            res += b'(/.*)?'
            continue
        else:
            res += ((re.escape(b'/') if i > 0 else b'') +
                    _translate_segment(segment))

    if not pat.endswith(b'/'):
        res += b'/?'

    return res + b'\\Z'


def read_ignore_patterns(f):
    """Read a git ignore file.

    :param f: File-like object to read from
    :return: List of patterns
    """

    for line in f:
        line = line.rstrip(b"\r\n")

        # Ignore blank lines, they're used for readability.
        if not line:
            continue

        if line.startswith(b'#'):
            # Comment
            continue

        # Trailing spaces are ignored unless they are quoted with a backslash.
        while line.endswith(b' ') and not line.endswith(b'\\ '):
            line = line[:-1]
        line = line.replace(b'\\ ', b' ')

        yield line


def match_pattern(path, pattern, ignorecase=False):
    """Match a gitignore-style pattern against a path.

    :param path: Path to match
    :param pattern: Pattern to match
    :param ignorecase: Whether to do case-sensitive matching
    :return: bool indicating whether the pattern matched
    """
    return Pattern(pattern, ignorecase).match(path)


class Pattern(object):
    """A single ignore pattern."""

    def __init__(self, pattern, ignorecase=False):
        self.pattern = pattern
        self.ignorecase = ignorecase
        if pattern[0:1] == b'!':
            self.is_exclude = False
            pattern = pattern[1:]
        else:
            if pattern[0:1] == b'\\':
                pattern = pattern[1:]
            self.is_exclude = True
        flags = 0
        if self.ignorecase:
            flags = re.IGNORECASE
        self._re = re.compile(translate(pattern), flags)

    def __bytes__(self):
        return self.pattern

    def __str__(self):
        return self.pattern.decode(sys.getfilesystemencoding())

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.pattern == other.pattern and
                self.ignorecase == other.ignorecase)

    def __repr__(self):
        return "%s(%s, %r)" % (
            type(self).__name__, self.pattern, self.ignorecase)

    def match(self, path):
        """Try to match a path against this ignore pattern.

        :param path: Path to match (relative to ignore location)
        :return: boolean
        """
        return bool(self._re.match(path))


class IgnoreFilter(object):

    def __init__(self, patterns, ignorecase=False):
        self._patterns = []
        self._ignorecase = ignorecase
        for pattern in patterns:
            self.append_pattern(pattern)

    def append_pattern(self, pattern):
        """Add a pattern to the set."""
        self._patterns.append(Pattern(pattern, self._ignorecase))

    def find_matching(self, path):
        """Yield all matching patterns for path.

        :param path: Path to match
        :return: Iterator over  iterators
        """
        if not isinstance(path, bytes):
            path = path.encode(sys.getfilesystemencoding())
        for pattern in self._patterns:
            if pattern.match(path):
                yield pattern

    def is_ignored(self, path):
        """Check whether a path is ignored.

        For directories, include a trailing slash.

        :return: status is None if file is not mentioned, True if it is
            included, False if it is explicitly excluded.
        """
        status = None
        for pattern in self.find_matching(path):
            status = pattern.is_exclude
        return status

    @classmethod
    def from_path(cls, path, ignorecase=False):
        with open(path, 'rb') as f:
            ret = cls(read_ignore_patterns(f), ignorecase)
            ret._path = path
            return ret

    def __repr__(self):
        if getattr(self, '_path', None) is None:
            return "<%s>" % (type(self).__name__)
        else:
            return "%s.from_path(%r)" % (type(self).__name__, self._path)


class IgnoreFilterStack(object):
    """Check for ignore status in multiple filters."""

    def __init__(self, filters):
        self._filters = filters

    def is_ignored(self, path):
        """Check whether a path is explicitly included or excluded in ignores.

        :param path: Path to check
        :return: None if the file is not mentioned, True if it is included,
            False if it is explicitly excluded.
        """
        status = None
        for filter in self._filters:
            status = filter.is_ignored(path)
            if status is not None:
                return status
        return status


def default_user_ignore_filter_path(config):
    """Return default user ignore filter path.

    :param config: A Config object
    :return: Path to a global ignore file
    """
    try:
        return config.get((b'core', ), b'excludesFile')
    except KeyError:
        pass

    xdg_config_home = os.environ.get(
        "XDG_CONFIG_HOME", os.path.expanduser("~/.config/"),
    )
    return os.path.join(xdg_config_home, 'git', 'ignore')


class IgnoreFilterManager(object):
    """Ignore file manager."""

    def __init__(self, top_path, global_filters, ignorecase):
        self._path_filters = {}
        self._top_path = top_path
        self._global_filters = global_filters
        self._ignorecase = ignorecase

    def __repr__(self):
        return "%s(%s, %r, %r)" % (
            type(self).__name__, self._top_path,
            self._global_filters,
            self._ignorecase)

    def _load_path(self, path):
        try:
            return self._path_filters[path]
        except KeyError:
            pass

        p = os.path.join(self._top_path, path, '.gitignore')
        try:
            self._path_filters[path] = IgnoreFilter.from_path(
                p, self._ignorecase)
        except IOError:
            self._path_filters[path] = None
        return self._path_filters[path]

    def find_matching(self, path):
        """Find matching patterns for path.

        Stops after the first ignore file with matches.

        :param path: Path to check
        :return: Iterator over Pattern instances
        """
        if os.path.isabs(path):
            raise ValueError('%s is an absolute path' % path)
        filters = [(0, f) for f in self._global_filters]
        if os.path.sep != '/':
            path = path.replace(os.path.sep, '/')
        parts = path.split('/')
        for i in range(len(parts)+1):
            dirname = '/'.join(parts[:i])
            for s, f in filters:
                relpath = '/'.join(parts[s:i])
                if i < len(parts):
                    # Paths leading up to the final part are all directories,
                    # so need a trailing slash.
                    relpath += '/'
                matches = list(f.find_matching(relpath))
                if matches:
                    return iter(matches)
            ignore_filter = self._load_path(dirname)
            if ignore_filter is not None:
                filters.insert(0, (i, ignore_filter))
        return iter([])

    def is_ignored(self, path):
        """Check whether a path is explicitly included or excluded in ignores.

        :param path: Path to check
        :return: None if the file is not mentioned, True if it is included,
            False if it is explicitly excluded.
        """
        matches = list(self.find_matching(path))
        if matches:
            return matches[-1].is_exclude
        return None

    @classmethod
    def from_repo(cls, repo):
        """Create a IgnoreFilterManager from a repository.

        :param repo: Repository object
        :return: A `IgnoreFilterManager` object
        """
        global_filters = []
        for p in [
                os.path.join(repo.controldir(), 'info', 'exclude'),
                default_user_ignore_filter_path(repo.get_config_stack())]:
            try:
                global_filters.append(IgnoreFilter.from_path(p))
            except IOError:
                pass
        config = repo.get_config_stack()
        ignorecase = config.get_boolean((b'core'), (b'ignorecase'), False)
        return cls(repo.path, global_filters, ignorecase)
