# test_wildmatch.py -- tests for Git's wildmatch() pattern language
# Copyright (C) 2026 Vincent Gao <gaobing1230@gmail.com>
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

"""Tests for wildmatch pattern translation."""

import re

from dulwich.wildmatch import (
    MalformedPattern,
    translate,
    translate_bracket_expression,
)

from . import TestCase


class BracketExpressionTests(TestCase):
    def assertBracketMatches(self, bracket: bytes, candidate: bytes) -> None:
        self.assertTrue(self._matches(bracket, candidate))

    def assertBracketNotMatches(self, bracket: bytes, candidate: bytes) -> None:
        self.assertFalse(self._matches(bracket, candidate))

    @staticmethod
    def _matches(bracket: bytes, candidate: bytes) -> bool:
        """Whether ``bracket`` (a full ``[...]`` expression) matches one byte."""
        _, frag = translate_bracket_expression(bracket + b"Z", 1)
        return re.compile(rb"^" + frag + rb"$").match(candidate) is not None

    def test_posix_classes(self) -> None:
        self.assertBracketMatches(b"[[:digit:]]", b"5")
        self.assertBracketNotMatches(b"[[:digit:]]", b"a")
        self.assertBracketMatches(b"[[:alpha:]]", b"a")
        self.assertBracketNotMatches(b"[[:alpha:]]", b"1")
        self.assertBracketMatches(b"[[:space:]]", b" ")
        self.assertBracketMatches(b"[[:upper:]]", b"A")
        self.assertBracketNotMatches(b"[[:upper:]]", b"a")

    def test_negation(self) -> None:
        self.assertBracketMatches(b"[^a-c]", b"d")
        self.assertBracketNotMatches(b"[^a-c]", b"b")
        self.assertBracketMatches(b"[!a-c]", b"d")
        self.assertBracketNotMatches(b"[!a-c]", b"b")

    def test_backslash_escapes_member(self) -> None:
        self.assertBracketMatches(b"[a\\-c]", b"-")
        self.assertBracketNotMatches(b"[a\\-c]", b"b")

    def test_class_never_matches_slash(self) -> None:
        self.assertBracketNotMatches(b"[^/]", b"/")

    def test_inverted_range_keeps_low_end(self) -> None:
        # Git takes "z" as a member before it sees the inverted "-a".
        self.assertBracketMatches(b"[z-a]", b"z")
        self.assertBracketNotMatches(b"[z-a]", b"a")

    def test_composed_class(self) -> None:
        self.assertBracketMatches(b"[[:digit:]a-f_]", b"_")
        self.assertBracketMatches(b"[[:digit:]a-f_]", b"c")
        self.assertBracketNotMatches(b"[[:digit:]a-f_]", b"g")

    def test_malformed(self) -> None:
        for pat in (b"[abc", b"[[:bogus:]]", b"[[:^alpha:]]"):
            self.assertRaises(
                MalformedPattern, translate_bracket_expression, pat + b"Z", 1
            )


class TranslateTests(TestCase):
    def assertMatches(self, pattern: bytes, path: bytes) -> None:
        self.assertTrue(self._matches(pattern, path))

    def assertNotMatches(self, pattern: bytes, path: bytes) -> None:
        self.assertFalse(self._matches(pattern, path))

    @staticmethod
    def _matches(pattern: bytes, path: bytes) -> bool:
        regex = re.compile(rb"^" + translate(pattern) + rb"$")
        return regex.match(path) is not None

    def test_star_does_not_cross_slash(self) -> None:
        self.assertMatches(b"a*c", b"abc")
        self.assertNotMatches(b"a*c", b"a/c")

    def test_question_mark(self) -> None:
        self.assertMatches(b"a?c", b"abc")
        self.assertNotMatches(b"a?c", b"a/c")

    def test_double_asterisk_crosses_slash(self) -> None:
        self.assertMatches(b"a/**/d", b"a/b/c/d")
        self.assertMatches(b"a/**/d", b"a/d")
        self.assertMatches(b"**/d", b"a/b/d")

    def test_trailing_double_asterisk_slash_requires_subdir(self) -> None:
        # 'a/**/' is directory-only: the trailing '/' must be preserved so
        # 'a/f' (a file directly under 'a') does not match.
        self.assertMatches(b"a/**/", b"a/b/")
        self.assertMatches(b"a/**/", b"a/b/c/")
        self.assertNotMatches(b"a/**/", b"a/")
        self.assertNotMatches(b"a/**/", b"a/f")

    def test_bracket(self) -> None:
        self.assertMatches(b"a/[[:digit:]]", b"a/5")
        self.assertNotMatches(b"a/[[:digit:]]", b"a/x")

    def test_bracket_spanning_slash(self) -> None:
        # Git never splits the pattern, so a class may span '/' but never match it.
        self.assertMatches(b"x[b/c]y", b"xby")
        self.assertNotMatches(b"x[b/c]y", b"x/y")

    def test_escaped_wildcard(self) -> None:
        self.assertMatches(b"a\\*c", b"a*c")
        self.assertNotMatches(b"a\\*c", b"abc")

    def test_empty_segment_is_a_literal_slash(self) -> None:
        # wildmatch() has no special case for "//": each '/' is matched
        # literally, so "a//b" matches only the text "a//b". Paths reaching
        # dulwich.ignore are normalized, so nothing there can match it.
        self.assertMatches(b"a//b", b"a//b")
        self.assertNotMatches(b"a//b", b"a/b")

    def test_malformed_raises(self) -> None:
        self.assertRaises(MalformedPattern, translate, b"a[bc")
        self.assertRaises(MalformedPattern, translate, b"[[:bogus:]]")
