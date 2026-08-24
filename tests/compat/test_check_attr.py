# test_check_attr.py -- Compatibility tests for git check-attr
# Copyright (C) 2026 Jelmer Vernooĳ <jelmer@jelmer.uk>
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

"""Compatibility tests for git check-attr functionality."""

import os
import shutil
import tempfile
from collections.abc import Sequence

from dulwich.attrs import AttributeValue, GitAttributes
from dulwich.repo import Repo

from .utils import CompatTestCase, run_git_or_fail


class CheckAttrCompatTestCase(CompatTestCase):
    """Test that dulwich matches git check-attr.

    Both read .gitattributes patterns with wildmatch() under WM_PATHNAME, the
    same matcher .gitignore uses, so the pattern grammar is shared with
    tests.compat.test_check_ignore.
    """

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def _write_gitattributes(self, content: str) -> None:
        path = os.path.join(self.test_dir, ".gitattributes")
        with open(path, "w") as f:
            f.write(content)

    def _create_file(self, path: str) -> None:
        full_path = os.path.join(self.test_dir, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w"):
            pass

    def _git_check_attr(
        self, attr: str, paths: Sequence[str]
    ) -> dict[str, AttributeValue]:
        """Return each path's value for attr, as git reports it."""
        output = run_git_or_fail(
            ["check-attr", "-z", attr, "--", *paths], cwd=self.test_dir
        )
        # -z emits a flat NUL-separated run of (path, attr, value) triples.
        fields = output.split(b"\0")
        result: dict[str, AttributeValue] = {}
        for i in range(0, len(fields) - 2, 3):
            path, _name, value = fields[i : i + 3]
            if value == b"unspecified":
                continue
            if value == b"set":
                result[path.decode()] = True
            elif value == b"unset":
                result[path.decode()] = False
            else:
                result[path.decode()] = value
        return result

    def _dulwich_check_attr(
        self, attr: str, paths: Sequence[str]
    ) -> dict[str, AttributeValue]:
        """Return each path's value for attr, as dulwich reports it."""
        attrs = GitAttributes.from_path(self.test_dir)
        name = attr.encode()
        result: dict[str, AttributeValue] = {}
        for path in paths:
            matched = attrs.match_path(path.encode())
            if name in matched and matched[name] is not None:
                result[path] = matched[name]
        return result

    def _assert_attr_match(self, attr: str, paths: Sequence[str]) -> None:
        self.assertEqual(
            self._git_check_attr(attr, paths),
            self._dulwich_check_attr(attr, paths),
            f"attr {attr!r} over {list(paths)}",
        )

    def test_basic_attributes(self) -> None:
        self._write_gitattributes("*.c text\n*.bin -text\n*.dat !text\n")
        paths = ["a.c", "a.bin", "a.dat", "a.txt", "sub/a.c"]
        for path in paths:
            self._create_file(path)
        self._assert_attr_match("text", paths)

    def test_attribute_values(self) -> None:
        self._write_gitattributes("*.c diff=cpp\n*.py diff=python\n*.o -diff\n")
        paths = ["a.c", "a.py", "a.o", "a.txt"]
        for path in paths:
            self._create_file(path)
        self._assert_attr_match("diff", paths)

    def test_later_pattern_wins(self) -> None:
        self._write_gitattributes("*.txt text\nbig.txt -text\n*.md text\n")
        paths = ["a.txt", "big.txt", "a.md"]
        for path in paths:
            self._create_file(path)
        self._assert_attr_match("text", paths)

    def test_anchored_and_directory_patterns(self) -> None:
        self._write_gitattributes("/root.c text\nsub/nested.c text\ndir/ text\n")
        paths = [
            "root.c",
            "deep/root.c",
            "sub/nested.c",
            "deep/sub/nested.c",
            "dir/file",
        ]
        for path in paths:
            self._create_file(path)
        self._assert_attr_match("text", paths)

    def test_double_asterisk_is_a_whole_component(self) -> None:
        # "a**b" is not slash-crossing: ** is only special as a full path
        # component, so it behaves like a single '*' here.
        self._write_gitattributes("a**b text\n**/deep text\nx/**/y text\n")
        paths = ["ab", "axb", "a/x/b", "deep", "n/deep", "x/y", "x/m/y", "x/m/n/y"]
        for path in paths:
            self._create_file(path)
        self._assert_attr_match("text", paths)

    def test_trailing_double_asterisk_slash(self) -> None:
        # ``foo/**/`` in .gitattributes goes through the same wildmatch
        # translation as .gitignore, so the trailing slash must be preserved:
        # only directories under foo get the attribute.
        self._write_gitattributes("foo/**/ text\n")
        paths = ["foo/f.txt", "foo/sub/", "foo/sub/g.txt"]
        for path in paths:
            if not path.endswith("/"):
                self._create_file(path)
        os.makedirs(os.path.join(self.test_dir, "foo", "sub"), exist_ok=True)
        self._assert_attr_match("text", paths)

    def test_trailing_double_asterisk_slash_at_root(self) -> None:
        # A bare ``**/`` gives the attribute to every directory below root.
        self._write_gitattributes("**/ text\n")
        self._create_file("keep.txt")
        self._create_file("foo/bla.c")
        os.makedirs(os.path.join(self.test_dir, "foo", "bar"), exist_ok=True)
        paths = ["keep.txt", "foo/", "foo/bla.c", "foo/bar/"]
        self._assert_attr_match("text", paths)

    def test_trailing_double_asterisk_slash_nested(self) -> None:
        # ``foo/**/`` against a deeper tree with its own subdirectories.
        self._write_gitattributes("foo/**/ text\n")
        self._create_file("foo/a.txt")
        self._create_file("foo/bar/b.txt")
        self._create_file("foo/bar/bla/c.txt")
        self._create_file("keep/foo/d.txt")
        paths = [
            "foo/a.txt",
            "foo/bar/",
            "foo/bar/b.txt",
            "foo/bar/bla/",
            "foo/bar/bla/c.txt",
            "keep/foo/",
            "keep/foo/d.txt",
        ]
        self._assert_attr_match("text", paths)

    def test_wildcards_do_not_cross_slash(self) -> None:
        self._write_gitattributes("a*c text\na?c text\n")
        paths = ["abc", "ac", "a/c", "abbc", "axc"]
        for path in paths:
            self._create_file(path)
        self._assert_attr_match("text", paths)

    def test_bracket_expression_grammar(self) -> None:
        """Test the bracket grammar against git, as test_check_ignore does.

        wildmatch() takes '^' as a negation character, understands [:name:]
        classes, lets a backslash escape a member and never matches '/'.
        """
        paths = [
            "a",
            "b",
            "c",
            "d",
            "z",
            "Q",
            "0",
            "9",
            "-",
            "^",
            "]",
            "[",
            "abc",
            "a-c",
            "sub/a",
            "sub/0",
        ]
        for path in paths:
            self._create_file(path)

        patterns = [
            "[abc]",
            "[a-c]",
            "[!a-c]",
            "[^a-c]",
            "[!0-9]",
            "[^0-9]",
            "[[:digit:]]",
            "[[:alpha:]]",
            "[[:punct:]]",
            "[[:xdigit:]]",
            "[![:digit:]]",
            "[^[:digit:]]",
            "[[:digit:]abc]",
            "[a[:digit:]]",
            "[]]",
            "[]a]",
            "[!]]",
            "[\\]]",
            "[\\[]",
            "a[a\\-c]c",
            "[a-]",
            "[-a]",
            "[z-a]",
            "[a/c]",
            "[!/]",
            "[^/]",
            "*[^a]",
            "?[^a]",
            "[[:digit:]]*",
            "sub/[^a]",
            "sub/[[:digit:]]",
            "**/[[:digit:]]",
        ]
        for pattern in patterns:
            self._write_gitattributes(pattern + " text\n")
            self.assertEqual(
                self._git_check_attr("text", paths),
                self._dulwich_check_attr("text", paths),
                f"pattern: {pattern}",
            )

    def test_malformed_pattern_is_skipped(self) -> None:
        """A malformed bracket expression matches nothing, and git keeps going.

        wildmatch() returns WM_ABORT_ALL for these rather than treating the
        file as broken, so the surrounding patterns must still apply.
        """
        self._write_gitattributes("*.[ch text\ngood.txt text\n")
        paths = ["a.c", "a.h", "a.txt", "good.txt"]
        for path in paths:
            self._create_file(path)

        # The bad line is dropped, so only good.txt has the attribute.
        self.assertEqual({"good.txt": True}, self._git_check_attr("text", paths))
        self._assert_attr_match("text", paths)

    def test_unmatched_bracket_does_not_abort_file(self) -> None:
        self._write_gitattributes("before.txt text\n[unclosed text\nafter.txt text\n")
        paths = ["before.txt", "after.txt"]
        for path in paths:
            self._create_file(path)

        self.assertEqual(
            {"before.txt": True, "after.txt": True},
            self._git_check_attr("text", paths),
        )
        self._assert_attr_match("text", paths)
