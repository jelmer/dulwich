# test_ignore.py -- Tests for ignore files.
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

"""Tests for ignore files."""

from io import BytesIO
import re
import unittest

from dulwich.ignore import (
    IgnoreFilter,
    IgnoreFilterStack,
    match_pattern,
    read_ignore_patterns,
    translate,
    )


POSITIVE_MATCH_TESTS = [
    (b"foo.c", b"*.c"),
    (b"foo/foo.c", b"*.c"),
    (b"foo/foo.c", b"foo.c"),
    (b"foo.c", b"/*.c"),
    (b"foo.c", b"/foo.c"),
    (b"foo.c", b"foo.c"),
    (b"foo.c", b"foo.[ch]"),
    (b"foo/bar/bla.c", b"foo/**"),
    (b"foo/bar/bla/blie.c", b"foo/**/blie.c"),
    (b"foo/bar/bla.c", b"**/bla.c"),
    (b"bla.c", b"**/bla.c"),
    (b"foo/bar", b"foo/**/bar"),
    (b"foo/bla/bar", b"foo/**/bar"),
    (b"foo/bar/", b"bar/"),
    (b"foo/bar/", b"bar"),
]

NEGATIVE_MATCH_TESTS = [
    (b"foo.c", b"foo.[dh]"),
    (b"foo/foo.c", b"/foo.c"),
    (b"foo/foo.c", b"/*.c"),
    (b"foo/bar/", b"/bar/"),
]


TRANSLATE_TESTS = [
    (b"*.c", b'(?ms)(.*/)?[^/]+\\.c/?\\Z'),
    (b"foo.c", b'(?ms)(.*/)?foo\\.c/?\\Z'),
    (b"/*.c", b'(?ms)[^/]+\\.c/?\\Z'),
    (b"/foo.c", b'(?ms)foo\\.c/?\\Z'),
    (b"foo.c", b'(?ms)(.*/)?foo\\.c/?\\Z'),
    (b"foo.[ch]", b'(?ms)(.*/)?foo\\.[ch]/?\\Z'),
    (b"bar/", b'(?ms)(.*/)?bar\\/\\Z'),
    (b"foo/**", b'(?ms)foo(/.*)?/?\\Z'),
    (b"foo/**/blie.c", b'(?ms)foo(/.*)?\\/blie\\.c/?\\Z'),
    (b"**/bla.c", b'(?ms)(.*/)?bla\\.c/?\\Z'),
    (b"foo/**/bar", b'(?ms)foo(/.*)?\\/bar/?\\Z'),
]


class TranslateTests(unittest.TestCase):

    def test_translate(self):
        for (pattern, regex) in TRANSLATE_TESTS:
            if re.escape(b'/') == b'/':
                # Slash is no longer escaped in Python3.7, so undo the escaping
                # in the expected return value..
                regex = regex.replace(b'\\/', b'/')
            self.assertEqual(
                regex, translate(pattern),
                "orig pattern: %r, regex: %r, expected: %r" %
                (pattern, translate(pattern), regex))


class ReadIgnorePatterns(unittest.TestCase):

    def test_read_file(self):
        f = BytesIO(b"""
# a comment

# and an empty line:

\#not a comment
!negative
with trailing whitespace 
with escaped trailing whitespace\ 
""")
        self.assertEqual(list(read_ignore_patterns(f)), [
            b'\\#not a comment',
            b'!negative',
            b'with trailing whitespace',
            b'with escaped trailing whitespace '
        ])


class MatchPatternTests(unittest.TestCase):

    def test_matches(self):
        for (path, pattern) in POSITIVE_MATCH_TESTS:
            self.assertTrue(
                match_pattern(path, pattern),
                "path: %r, pattern: %r" % (path, pattern))

    def test_no_matches(self):
        for (path, pattern) in NEGATIVE_MATCH_TESTS:
            self.assertFalse(
                match_pattern(path, pattern),
                "path: %r, pattern: %r" % (path, pattern))


class IgnoreFilterTests(unittest.TestCase):

    def test_included(self):
        filter = IgnoreFilter([b'a.c', b'b.c'])
        self.assertTrue(filter.is_ignored(b'a.c'))
        self.assertIs(None, filter.is_ignored(b'c.c'))

    def test_excluded(self):
        filter = IgnoreFilter([b'a.c', b'b.c', b'!c.c'])
        self.assertFalse(filter.is_ignored(b'c.c'))
        self.assertIs(None, filter.is_ignored(b'd.c'))

    def test_include_exclude_include(self):
        filter = IgnoreFilter([b'a.c', b'!a.c', b'a.c'])
        self.assertTrue(filter.is_ignored(b'a.c'))


class IgnoreFilterStackTests(unittest.TestCase):

    def test_stack_first(self):
        filter1 = IgnoreFilter([b'[a].c', b'[b].c', b'![d].c'])
        filter2 = IgnoreFilter([b'[a].c', b'![b],c', b'[c].c', b'[d].c'])
        stack = IgnoreFilterStack([filter1, filter2])
        self.assertIs(True, stack.is_ignored(b'a.c'))
        self.assertIs(True, stack.is_ignored(b'b.c'))
        self.assertIs(True, stack.is_ignored(b'c.c'))
        self.assertIs(False, stack.is_ignored(b'd.c'))
        self.assertIs(None, stack.is_ignored(b'e.c'))
