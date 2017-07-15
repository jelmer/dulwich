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


def translate(pat):
    """Translate a shell PATTERN to a regular expression.

    There is no way to quote meta-characters.

    Originally copied from fnmatch in Python 2.7, but modified for Dulwich
    to cope with features in Git ignore patterns.
    """

    res = b'(?ms)'

    if b'/' not in pat[:-1]:
        # If there's no slash, this is a filename-based match
        res = res + b'(.*/)?'

    if pat.startswith(b'**/'):
        # Leading **/
        pat = pat[2:]
        res = res + b'(.*/)?'

    if pat.startswith(b'/'):
        pat = pat[1:]

    i, n = 0, len(pat)

    while i < n:
        if pat[i:i+3] == b'/**':
            res = res + b'(/.*)?'
            i = i+3
            continue
        c = pat[i:i+1]
        i = i+1
        if c == b'*':
            res = res + b'[^/]+'
        elif c == b'?':
            res = res + b'.'
        elif c == b'[':
            j = i
            if j < n and pat[j:j+1] == b'!':
                j = j+1
            if j < n and pat[j:j+1] == b']':
                j = j+1
            while j < n and pat[j:j+1] != b']':
                j = j+1
            if j >= n:
                res = res + b'\\['
            else:
                stuff = pat[i:j].replace(b'\\', b'\\\\')
                i = j+1
                if stuff.startswith(b'!'):
                    stuff = b'^' + stuff[1:]
                elif stuff.startswith(b'^'):
                    stuff = b'\\' + stuff
                res = res + b'[' + stuff + b']'
        else:
            res = res + re.escape(c)

    if not res.endswith(b'/'):
        res = res + b'/?'

    return res + b'\Z'


def read_ignore_patterns(f):
    """Read a git ignore file.

    :param f: File-like object to read from
    :return: List of patterns
    """

    for l in f:
        l = l.rstrip(b"\n")

        # Ignore blank lines, they're used for readability.
        if not l:
            continue

        if l.startswith(b'#'):
            # Comment
            continue

        # Trailing spaces are ignored unless they are quoted with a backslash.
        while l.endswith(b' ') and not l.endswith(b'\\ '):
            l = l[:-1]
        l = l.replace(b'\\ ', b' ')

        yield l


def match_pattern(path, pattern):
    """Match a gitignore-style pattern against a path.

    :param path: Path to match
    :param pattern: Pattern to match
    :return: bool indicating whether the pattern matched
    """
    re_pattern = translate(pattern)
    return re.match(re_pattern, path)


class IgnoreFilter(object):

    def __init__(self, patterns):
        self._patterns = []
        for pattern in patterns:
            self.append_pattern(pattern)

    def append_pattern(self, pattern):
        """Add a pattern to the set."""
        pattern_str = pattern
        if pattern[0:1] == b'!':
            is_exclude = False
            pattern = pattern[1:]
        else:
            if pattern[0:1] == b'\\':
                pattern = pattern[1:]
            is_exclude = True
        self._patterns.append(
            (is_exclude, re.compile(translate(pattern)), pattern_str))

    def is_ignored(self, path):
        """Check whether a path is ignored.

        For directories, include a trailing slash.

        :return: status is None if file is not mentioned, True if it is
            included, False if it is explicitly excluded.
        """
        if not isinstance(path, bytes):
            path = path.encode(sys.getfilesystemencoding())
        status = None
        matched = None
        for (is_exclude, compiled, pattern_str) in self._patterns:
            if compiled.match(path):
                status = is_exclude
        return status

    @classmethod
    def from_path(cls, path):
        with open(path, 'rb') as f:
            ret = cls(read_ignore_patterns(f))
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
        return config.get(('core', ), 'excludesFile')
    except KeyError:
        pass

    xdg_config_home = os.environ.get(
        "XDG_CONFIG_HOME", os.path.expanduser("~/.config/"),
    )
    return os.path.join(xdg_config_home, 'git', 'ignore')


class IgnoreFilterManager(object):
    """Ignore file manager."""

    def __init__(self, top_path, global_filters):
        self._path_filters = {}
        self._top_path = top_path
        self._global_filters = global_filters

    def __repr__(self):
        return "%s(%s, %r)" % (
            type(self).__name__, self._top_path,
            self._global_filters)

    def _load_path(self, path):
        try:
            return self._path_filters[path]
        except KeyError:
            pass

        p = os.path.join(self._top_path, path, '.gitignore')
        try:
            self._path_filters[path] = IgnoreFilter.from_path(p)
        except IOError:
            self._path_filters[path] = None
        return self._path_filters[path]

    def is_ignored(self, path):
        """Check whether a path is explicitly included or excluded in ignores.

        :param path: Path to check
        :return: None if the file is not mentioned, True if it is included,
            False if it is explicitly excluded.
        """
        if os.path.isabs(path):
            path = os.path.relpath(path, self._top_path)
        filters = [(0, f) for f in self._global_filters]
        parts = path.split('/')
        for i in range(len(parts)+1):
            dirname = '/'.join(parts[:i])
            for s, f in filters:
                relpath = '/'.join(parts[s:i])
                status = f.is_ignored(relpath)
                if status is not None:
                    return status
            ignore_filter = self._load_path(dirname)
            if ignore_filter is not None:
                filters.insert(0, (i, ignore_filter))
        return None

    @classmethod
    def from_repo(cls, repo):
        global_filters = []
        for p in [
                os.path.join(repo.controldir(), 'info', 'exclude'),
                default_user_ignore_filter_path(repo.get_config_stack())]:
            try:
                global_filters.append(IgnoreFilter.from_path(p))
            except IOError:
                pass
        return cls(repo.path, global_filters)
