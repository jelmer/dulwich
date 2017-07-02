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
import unittest

from dulwich.ignore import (
    IgnoreFilter,
    match_pattern,
    read_ignore_patterns,
    translate,
    )


POSITIVE_MATCH_TESTS = [
    ("foo.c", "*.c"),
    ("foo/foo.c", "*.c"),
    ("foo/foo.c", "foo.c"),
    ("foo.c", "/*.c"),
    ("foo.c", "/foo.c"),
    ("foo.c", "foo.c"),
    ("foo.c", "foo.[ch]"),
    ("foo/bar/bla.c", "foo/**"),
    ("foo/bar/bla/blie.c", "foo/**/blie.c"),
    ("foo/bar/bla.c", "**/bla.c"),
    ("bla.c", "**/bla.c"),
    ("foo/bar", "foo/**/bar"),
    ("foo/bla/bar", "foo/**/bar"),
]

NEGATIVE_MATCH_TESTS = [
    ("foo.c", "foo.[dh]"),
    ("foo/foo.c", "/foo.c"),
    ("foo/foo.c", "/*.c"),
]


TRANSLATE_TESTS = [
    ("*.c", '(.*\\/)?[^\\/]+\\.c\\Z(?ms)'),
    ("foo.c", '(.*\\/)?foo\\.c\\Z(?ms)'),
    ("/*.c", '[^\\/]+\\.c\\Z(?ms)'),
    ("/foo.c", 'foo\\.c\\Z(?ms)'),
    ("foo.c", '(.*\\/)?foo\\.c\\Z(?ms)'),
    ("foo.[ch]", '(.*\\/)?foo\\.[ch]\\Z(?ms)'),
    ("foo/**", 'foo(\\/.*)?\\Z(?ms)'),
    ("foo/**/blie.c", 'foo(\\/.*)?\\/blie\\.c\\Z(?ms)'),
    ("**/bla.c", '(.*\\/)?bla\\.c\\Z(?ms)'),
    ("foo/**/bar", 'foo(\\/.*)?\\/bar\\Z(?ms)'),
]


class TranslateTests(unittest.TestCase):

    def test_translate(self):
        for (pattern, regex) in TRANSLATE_TESTS:
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
            '\\#not a comment',
            '!negative',
            'with trailing whitespace',
            'with escaped trailing whitespace '
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
        filter = IgnoreFilter(['a.c', 'b.c'])
        self.assertTrue(filter.is_ignored('a.c'))
        self.assertIs(None, filter.is_ignored('c.c'))

    def test_excluded(self):
        filter = IgnoreFilter(['a.c', 'b.c', '!c.c'])
        self.assertFalse(filter.is_ignored('c.c'))
        self.assertIs(None, filter.is_ignored('d.c'))
