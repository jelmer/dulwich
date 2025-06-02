# test_config.py -- Tests for reading and writing configuration files
# Copyright (C) 2011 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

"""Tests for reading and writing configuration files."""

import os
import sys
from io import BytesIO
from unittest import skipIf
from unittest.mock import patch

from dulwich.config import (
    CaseInsensitiveOrderedMultiDict,
    ConfigDict,
    ConfigFile,
    StackedConfig,
    _check_section_name,
    _check_variable_name,
    _escape_value,
    _format_string,
    _parse_string,
    apply_instead_of,
    parse_submodules,
)

from . import TestCase


class ConfigFileTests(TestCase):
    def from_file(self, text):
        return ConfigFile.from_file(BytesIO(text))

    def test_empty(self) -> None:
        ConfigFile()

    def test_eq(self) -> None:
        self.assertEqual(ConfigFile(), ConfigFile())

    def test_default_config(self) -> None:
        cf = self.from_file(
            b"""[core]
\trepositoryformatversion = 0
\tfilemode = true
\tbare = false
\tlogallrefupdates = true
"""
        )
        self.assertEqual(
            ConfigFile(
                {
                    (b"core",): {
                        b"repositoryformatversion": b"0",
                        b"filemode": b"true",
                        b"bare": b"false",
                        b"logallrefupdates": b"true",
                    }
                }
            ),
            cf,
        )

    def test_from_file_empty(self) -> None:
        cf = self.from_file(b"")
        self.assertEqual(ConfigFile(), cf)

    def test_empty_line_before_section(self) -> None:
        cf = self.from_file(b"\n[section]\n")
        self.assertEqual(ConfigFile({(b"section",): {}}), cf)

    def test_comment_before_section(self) -> None:
        cf = self.from_file(b"# foo\n[section]\n")
        self.assertEqual(ConfigFile({(b"section",): {}}), cf)

    def test_comment_after_section(self) -> None:
        cf = self.from_file(b"[section] # foo\n")
        self.assertEqual(ConfigFile({(b"section",): {}}), cf)

    def test_comment_after_variable(self) -> None:
        cf = self.from_file(b"[section]\nbar= foo # a comment\n")
        self.assertEqual(ConfigFile({(b"section",): {b"bar": b"foo"}}), cf)

    def test_comment_character_within_value_string(self) -> None:
        cf = self.from_file(b'[section]\nbar= "foo#bar"\n')
        self.assertEqual(ConfigFile({(b"section",): {b"bar": b"foo#bar"}}), cf)

    def test_comment_character_within_section_string(self) -> None:
        cf = self.from_file(b'[branch "foo#bar"] # a comment\nbar= foo\n')
        self.assertEqual(ConfigFile({(b"branch", b"foo#bar"): {b"bar": b"foo"}}), cf)

    def test_closing_bracket_within_section_string(self) -> None:
        cf = self.from_file(b'[branch "foo]bar"] # a comment\nbar= foo\n')
        self.assertEqual(ConfigFile({(b"branch", b"foo]bar"): {b"bar": b"foo"}}), cf)

    def test_from_file_section(self) -> None:
        cf = self.from_file(b"[core]\nfoo = bar\n")
        self.assertEqual(b"bar", cf.get((b"core",), b"foo"))
        self.assertEqual(b"bar", cf.get((b"core", b"foo"), b"foo"))

    def test_from_file_multiple(self) -> None:
        cf = self.from_file(b"[core]\nfoo = bar\nfoo = blah\n")
        self.assertEqual([b"bar", b"blah"], list(cf.get_multivar((b"core",), b"foo")))
        self.assertEqual([], list(cf.get_multivar((b"core",), b"blah")))

    def test_from_file_utf8_bom(self) -> None:
        text = "[core]\nfoo = b\u00e4r\n".encode("utf-8-sig")
        cf = self.from_file(text)
        self.assertEqual(b"b\xc3\xa4r", cf.get((b"core",), b"foo"))

    def test_from_file_section_case_insensitive_lower(self) -> None:
        cf = self.from_file(b"[cOre]\nfOo = bar\n")
        self.assertEqual(b"bar", cf.get((b"core",), b"foo"))
        self.assertEqual(b"bar", cf.get((b"core", b"foo"), b"foo"))

    def test_from_file_section_case_insensitive_mixed(self) -> None:
        cf = self.from_file(b"[cOre]\nfOo = bar\n")
        self.assertEqual(b"bar", cf.get((b"core",), b"fOo"))
        self.assertEqual(b"bar", cf.get((b"cOre", b"fOo"), b"fOo"))

    def test_from_file_with_mixed_quoted(self) -> None:
        cf = self.from_file(b'[core]\nfoo = "bar"la\n')
        self.assertEqual(b"barla", cf.get((b"core",), b"foo"))

    def test_from_file_section_with_open_brackets(self) -> None:
        self.assertRaises(ValueError, self.from_file, b"[core\nfoo = bar\n")

    def test_from_file_value_with_open_quoted(self) -> None:
        self.assertRaises(ValueError, self.from_file, b'[core]\nfoo = "bar\n')

    def test_from_file_with_quotes(self) -> None:
        cf = self.from_file(b'[core]\nfoo = " bar"\n')
        self.assertEqual(b" bar", cf.get((b"core",), b"foo"))

    def test_from_file_with_interrupted_line(self) -> None:
        cf = self.from_file(b"[core]\nfoo = bar\\\n la\n")
        self.assertEqual(b"barla", cf.get((b"core",), b"foo"))

    def test_from_file_with_boolean_setting(self) -> None:
        cf = self.from_file(b"[core]\nfoo\n")
        self.assertEqual(b"true", cf.get((b"core",), b"foo"))

    def test_from_file_subsection(self) -> None:
        cf = self.from_file(b'[branch "foo"]\nfoo = bar\n')
        self.assertEqual(b"bar", cf.get((b"branch", b"foo"), b"foo"))

    def test_from_file_subsection_invalid(self) -> None:
        self.assertRaises(ValueError, self.from_file, b'[branch "foo]\nfoo = bar\n')

    def test_from_file_subsection_not_quoted(self) -> None:
        cf = self.from_file(b"[branch.foo]\nfoo = bar\n")
        self.assertEqual(b"bar", cf.get((b"branch", b"foo"), b"foo"))

    def test_write_preserve_multivar(self) -> None:
        cf = self.from_file(b"[core]\nfoo = bar\nfoo = blah\n")
        f = BytesIO()
        cf.write_to_file(f)
        self.assertEqual(b"[core]\n\tfoo = bar\n\tfoo = blah\n", f.getvalue())

    def test_write_to_file_empty(self) -> None:
        c = ConfigFile()
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b"", f.getvalue())

    def test_write_to_file_section(self) -> None:
        c = ConfigFile()
        c.set((b"core",), b"foo", b"bar")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b"[core]\n\tfoo = bar\n", f.getvalue())

    def test_write_to_file_section_multiple(self) -> None:
        c = ConfigFile()
        c.set((b"core",), b"foo", b"old")
        c.set((b"core",), b"foo", b"new")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b"[core]\n\tfoo = new\n", f.getvalue())

    def test_write_to_file_subsection(self) -> None:
        c = ConfigFile()
        c.set((b"branch", b"blie"), b"foo", b"bar")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b'[branch "blie"]\n\tfoo = bar\n', f.getvalue())

    def test_same_line(self) -> None:
        cf = self.from_file(b"[branch.foo] foo = bar\n")
        self.assertEqual(b"bar", cf.get((b"branch", b"foo"), b"foo"))

    def test_quoted_newlines_windows(self) -> None:
        cf = self.from_file(
            b"[alias]\r\n"
            b"c = '!f() { \\\r\n"
            b' printf \'[git commit -m \\"%s\\"]\\n\' \\"$*\\" && \\\r\n'
            b' git commit -m \\"$*\\"; \\\r\n'
            b" }; f'\r\n"
        )
        self.assertEqual(list(cf.sections()), [(b"alias",)])
        self.assertEqual(
            b'\'!f() { printf \'[git commit -m "%s"]\n\' "$*" && git commit -m "$*"',
            cf.get((b"alias",), b"c"),
        )

    def test_quoted(self) -> None:
        cf = self.from_file(
            b"""[gui]
\tfontdiff = -family \\\"Ubuntu Mono\\\" -size 11 -overstrike 0
"""
        )
        self.assertEqual(
            ConfigFile(
                {
                    (b"gui",): {
                        b"fontdiff": b'-family "Ubuntu Mono" -size 11 -overstrike 0',
                    }
                }
            ),
            cf,
        )

    def test_quoted_multiline(self) -> None:
        cf = self.from_file(
            b"""[alias]
who = \"!who() {\\
  git log --no-merges --pretty=format:'%an - %ae' $@ | uniq -c | sort -rn;\\
};\\
who\"
"""
        )
        self.assertEqual(
            ConfigFile(
                {
                    (b"alias",): {
                        b"who": (
                            b"!who() {git log --no-merges --pretty=format:'%an - "
                            b"%ae' $@ | uniq -c | sort -rn;};who"
                        )
                    }
                }
            ),
            cf,
        )

    def test_set_hash_gets_quoted(self) -> None:
        c = ConfigFile()
        c.set(b"xandikos", b"color", b"#665544")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b'[xandikos]\n\tcolor = "#665544"\n', f.getvalue())

    def test_windows_path_with_trailing_backslash_unquoted(self) -> None:
        """Test that Windows paths ending with escaped backslash are handled correctly."""
        # This reproduces the issue from https://github.com/jelmer/dulwich/issues/1088
        # A single backslash at the end should actually be a line continuation in strict Git config
        # But we want to be more tolerant like Git itself
        cf = self.from_file(
            b'[core]\n\trepositoryformatversion = 0\n[remote "origin"]\n\turl = C:/Users/test\\\\\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n'
        )
        self.assertEqual(b"C:/Users/test\\", cf.get((b"remote", b"origin"), b"url"))
        self.assertEqual(
            b"+refs/heads/*:refs/remotes/origin/*",
            cf.get((b"remote", b"origin"), b"fetch"),
        )

    def test_windows_path_with_trailing_backslash_quoted(self) -> None:
        """Test that quoted Windows paths with escaped backslashes work correctly."""
        cf = self.from_file(
            b'[core]\n\trepositoryformatversion = 0\n[remote "origin"]\n\turl = "C:\\\\Users\\\\test\\\\"\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n'
        )
        self.assertEqual(b"C:\\Users\\test\\", cf.get((b"remote", b"origin"), b"url"))
        self.assertEqual(
            b"+refs/heads/*:refs/remotes/origin/*",
            cf.get((b"remote", b"origin"), b"fetch"),
        )

    def test_single_backslash_at_line_end_shows_proper_escaping_needed(self) -> None:
        """Test that demonstrates proper escaping is needed for single backslashes."""
        # This test documents the current behavior: a single backslash at the end of a line
        # is treated as a line continuation per Git config spec. Users should escape backslashes.

        # This reproduces the original issue - single backslash causes line continuation
        cf = self.from_file(
            b'[remote "origin"]\n\turl = C:/Users/test\\\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n'
        )
        # The result shows that line continuation occurred
        self.assertEqual(
            b"C:/Users/testfetch = +refs/heads/*:refs/remotes/origin/*",
            cf.get((b"remote", b"origin"), b"url"),
        )

        # The proper way to include a literal backslash is to escape it
        cf2 = self.from_file(
            b'[remote "origin"]\n\turl = C:/Users/test\\\\\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n'
        )
        self.assertEqual(b"C:/Users/test\\", cf2.get((b"remote", b"origin"), b"url"))
        self.assertEqual(
            b"+refs/heads/*:refs/remotes/origin/*",
            cf2.get((b"remote", b"origin"), b"fetch"),
        )


class ConfigDictTests(TestCase):
    def test_get_set(self) -> None:
        cd = ConfigDict()
        self.assertRaises(KeyError, cd.get, b"foo", b"core")
        cd.set((b"core",), b"foo", b"bla")
        self.assertEqual(b"bla", cd.get((b"core",), b"foo"))
        cd.set((b"core",), b"foo", b"bloe")
        self.assertEqual(b"bloe", cd.get((b"core",), b"foo"))

    def test_get_boolean(self) -> None:
        cd = ConfigDict()
        cd.set((b"core",), b"foo", b"true")
        self.assertTrue(cd.get_boolean((b"core",), b"foo"))
        cd.set((b"core",), b"foo", b"false")
        self.assertFalse(cd.get_boolean((b"core",), b"foo"))
        cd.set((b"core",), b"foo", b"invalid")
        self.assertRaises(ValueError, cd.get_boolean, (b"core",), b"foo")

    def test_dict(self) -> None:
        cd = ConfigDict()
        cd.set((b"core",), b"foo", b"bla")
        cd.set((b"core2",), b"foo", b"bloe")

        self.assertEqual([(b"core",), (b"core2",)], list(cd.keys()))
        self.assertEqual(cd[(b"core",)], {b"foo": b"bla"})

        cd[b"a"] = b"b"
        self.assertEqual(cd[b"a"], b"b")

    def test_items(self) -> None:
        cd = ConfigDict()
        cd.set((b"core",), b"foo", b"bla")
        cd.set((b"core2",), b"foo", b"bloe")

        self.assertEqual([(b"foo", b"bla")], list(cd.items((b"core",))))

    def test_items_nonexistant(self) -> None:
        cd = ConfigDict()
        cd.set((b"core2",), b"foo", b"bloe")

        self.assertEqual([], list(cd.items((b"core",))))

    def test_sections(self) -> None:
        cd = ConfigDict()
        cd.set((b"core2",), b"foo", b"bloe")

        self.assertEqual([(b"core2",)], list(cd.sections()))

    def test_set_vs_add(self) -> None:
        cd = ConfigDict()
        # Test add() creates multivars
        cd.add((b"core",), b"foo", b"value1")
        cd.add((b"core",), b"foo", b"value2")
        self.assertEqual(
            [b"value1", b"value2"], list(cd.get_multivar((b"core",), b"foo"))
        )

        # Test set() replaces values
        cd.set((b"core",), b"foo", b"value3")
        self.assertEqual([b"value3"], list(cd.get_multivar((b"core",), b"foo")))
        self.assertEqual(b"value3", cd.get((b"core",), b"foo"))


class StackedConfigTests(TestCase):
    def test_default_backends(self) -> None:
        StackedConfig.default_backends()

    @skipIf(sys.platform != "win32", "Windows specific config location.")
    def test_windows_config_from_path(self) -> None:
        from dulwich.config import get_win_system_paths

        install_dir = os.path.join("C:", "foo", "Git")
        self.overrideEnv("PATH", os.path.join(install_dir, "cmd"))
        with patch("os.path.exists", return_value=True):
            paths = set(get_win_system_paths())
        self.assertEqual(
            {
                os.path.join(os.environ.get("PROGRAMDATA"), "Git", "config"),
                os.path.join(install_dir, "etc", "gitconfig"),
            },
            paths,
        )

    @skipIf(sys.platform != "win32", "Windows specific config location.")
    def test_windows_config_from_reg(self) -> None:
        import winreg

        from dulwich.config import get_win_system_paths

        self.overrideEnv("PATH", None)
        install_dir = os.path.join("C:", "foo", "Git")
        with patch("winreg.OpenKey"):
            with patch(
                "winreg.QueryValueEx",
                return_value=(install_dir, winreg.REG_SZ),
            ):
                paths = set(get_win_system_paths())
        self.assertEqual(
            {
                os.path.join(os.environ.get("PROGRAMDATA"), "Git", "config"),
                os.path.join(install_dir, "etc", "gitconfig"),
            },
            paths,
        )


class EscapeValueTests(TestCase):
    def test_nothing(self) -> None:
        self.assertEqual(b"foo", _escape_value(b"foo"))

    def test_backslash(self) -> None:
        self.assertEqual(b"foo\\\\", _escape_value(b"foo\\"))

    def test_newline(self) -> None:
        self.assertEqual(b"foo\\n", _escape_value(b"foo\n"))


class FormatStringTests(TestCase):
    def test_quoted(self) -> None:
        self.assertEqual(b'" foo"', _format_string(b" foo"))
        self.assertEqual(b'"\\tfoo"', _format_string(b"\tfoo"))

    def test_not_quoted(self) -> None:
        self.assertEqual(b"foo", _format_string(b"foo"))
        self.assertEqual(b"foo bar", _format_string(b"foo bar"))


class ParseStringTests(TestCase):
    def test_quoted(self) -> None:
        self.assertEqual(b" foo", _parse_string(b'" foo"'))
        self.assertEqual(b"\tfoo", _parse_string(b'"\\tfoo"'))

    def test_not_quoted(self) -> None:
        self.assertEqual(b"foo", _parse_string(b"foo"))
        self.assertEqual(b"foo bar", _parse_string(b"foo bar"))

    def test_nothing(self) -> None:
        self.assertEqual(b"", _parse_string(b""))

    def test_tab(self) -> None:
        self.assertEqual(b"\tbar\t", _parse_string(b"\\tbar\\t"))

    def test_newline(self) -> None:
        self.assertEqual(b"\nbar\t", _parse_string(b"\\nbar\\t\t"))

    def test_quote(self) -> None:
        self.assertEqual(b'"foo"', _parse_string(b'\\"foo\\"'))


class CheckVariableNameTests(TestCase):
    def test_invalid(self) -> None:
        self.assertFalse(_check_variable_name(b"foo "))
        self.assertFalse(_check_variable_name(b"bar,bar"))
        self.assertFalse(_check_variable_name(b"bar.bar"))

    def test_valid(self) -> None:
        self.assertTrue(_check_variable_name(b"FOO"))
        self.assertTrue(_check_variable_name(b"foo"))
        self.assertTrue(_check_variable_name(b"foo-bar"))


class CheckSectionNameTests(TestCase):
    def test_invalid(self) -> None:
        self.assertFalse(_check_section_name(b"foo "))
        self.assertFalse(_check_section_name(b"bar,bar"))

    def test_valid(self) -> None:
        self.assertTrue(_check_section_name(b"FOO"))
        self.assertTrue(_check_section_name(b"foo"))
        self.assertTrue(_check_section_name(b"foo-bar"))
        self.assertTrue(_check_section_name(b"bar.bar"))


class SubmodulesTests(TestCase):
    def testSubmodules(self) -> None:
        cf = ConfigFile.from_file(
            BytesIO(
                b"""\
[submodule "core/lib"]
\tpath = core/lib
\turl = https://github.com/phhusson/QuasselC.git
"""
            )
        )
        got = list(parse_submodules(cf))
        self.assertEqual(
            [
                (
                    b"core/lib",
                    b"https://github.com/phhusson/QuasselC.git",
                    b"core/lib",
                )
            ],
            got,
        )

    def testMalformedSubmodules(self) -> None:
        cf = ConfigFile.from_file(
            BytesIO(
                b"""\
[submodule "core/lib"]
\tpath = core/lib
\turl = https://github.com/phhusson/QuasselC.git

[submodule "dulwich"]
\turl = https://github.com/jelmer/dulwich
"""
            )
        )
        got = list(parse_submodules(cf))
        self.assertEqual(
            [
                (
                    b"core/lib",
                    b"https://github.com/phhusson/QuasselC.git",
                    b"core/lib",
                )
            ],
            got,
        )


class ApplyInsteadOfTests(TestCase):
    def test_none(self) -> None:
        config = ConfigDict()
        self.assertEqual(
            "https://example.com/", apply_instead_of(config, "https://example.com/")
        )

    def test_apply(self) -> None:
        config = ConfigDict()
        config.set(("url", "https://samba.org/"), "insteadOf", "https://example.com/")
        self.assertEqual(
            "https://samba.org/", apply_instead_of(config, "https://example.com/")
        )

    def test_apply_multiple(self) -> None:
        config = ConfigDict()
        config.add(("url", "https://samba.org/"), "insteadOf", "https://blah.com/")
        config.add(("url", "https://samba.org/"), "insteadOf", "https://example.com/")
        self.assertEqual(
            [b"https://blah.com/", b"https://example.com/"],
            list(config.get_multivar(("url", "https://samba.org/"), "insteadOf")),
        )
        self.assertEqual(
            "https://samba.org/", apply_instead_of(config, "https://example.com/")
        )

    def test_apply_preserves_case_in_subsection(self) -> None:
        """Test that mixed-case URLs (like those with access tokens) are preserved."""
        config = ConfigDict()
        # GitHub access tokens have mixed case that must be preserved
        url_with_token = "https://ghp_AbCdEfGhIjKlMnOpQrStUvWxYz1234567890@github.com/"
        config.set(("url", url_with_token), "insteadOf", "https://github.com/")

        # Apply the substitution
        result = apply_instead_of(config, "https://github.com/jelmer/dulwich.git")
        expected = "https://ghp_AbCdEfGhIjKlMnOpQrStUvWxYz1234567890@github.com/jelmer/dulwich.git"
        self.assertEqual(expected, result)

        # Verify the token case is preserved
        self.assertIn("ghp_AbCdEfGhIjKlMnOpQrStUvWxYz1234567890", result)


class CaseInsensitiveConfigTests(TestCase):
    def test_case_insensitive(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("core",)] = "value"
        self.assertEqual("value", config[("CORE",)])
        self.assertEqual("value", config[("CoRe",)])
        self.assertEqual([("core",)], list(config.keys()))

    def test_multiple_set(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("core",)] = "value1"
        config[("core",)] = "value2"
        # The second set overwrites the first one
        self.assertEqual("value2", config[("core",)])
        self.assertEqual("value2", config[("CORE",)])

    def test_get_all(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("core",)] = "value1"
        config[("CORE",)] = "value2"
        config[("CoRe",)] = "value3"
        self.assertEqual(
            ["value1", "value2", "value3"], list(config.get_all(("core",)))
        )
        self.assertEqual(
            ["value1", "value2", "value3"], list(config.get_all(("CORE",)))
        )

    def test_delitem(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("core",)] = "value1"
        config[("CORE",)] = "value2"
        config[("other",)] = "value3"
        del config[("core",)]
        self.assertNotIn(("core",), config)
        self.assertNotIn(("CORE",), config)
        self.assertEqual("value3", config[("other",)])
        self.assertEqual(1, len(config))

    def test_len(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        self.assertEqual(0, len(config))
        config[("core",)] = "value1"
        self.assertEqual(1, len(config))
        config[("CORE",)] = "value2"
        self.assertEqual(1, len(config))  # Same key, case insensitive

    def test_subsection_case_preserved(self) -> None:
        """Test that subsection names preserve their case."""
        config = CaseInsensitiveOrderedMultiDict()
        # Section names should be case-insensitive, but subsection names should preserve case
        config[("url", "https://Example.COM/Path")] = "value1"

        # Can retrieve with different case section name
        self.assertEqual("value1", config[("URL", "https://Example.COM/Path")])
        self.assertEqual("value1", config[("url", "https://Example.COM/Path")])

        # But not with different case subsection name
        with self.assertRaises(KeyError):
            config[("url", "https://example.com/path")]

        # Verify the stored key preserves subsection case
        stored_keys = list(config.keys())
        self.assertEqual(1, len(stored_keys))
        self.assertEqual(("url", "https://Example.COM/Path"), stored_keys[0])
        config[("other",)] = "value3"
        self.assertEqual(2, len(config))

    def test_make_from_dict(self) -> None:
        original = {("core",): "value1", ("other",): "value2"}
        config = CaseInsensitiveOrderedMultiDict.make(original)
        self.assertEqual("value1", config[("core",)])
        self.assertEqual("value1", config[("CORE",)])
        self.assertEqual("value2", config[("other",)])

    def test_make_from_self(self) -> None:
        config1 = CaseInsensitiveOrderedMultiDict()
        config1[("core",)] = "value"
        config2 = CaseInsensitiveOrderedMultiDict.make(config1)
        self.assertIs(config1, config2)

    def test_make_invalid_type(self) -> None:
        self.assertRaises(TypeError, CaseInsensitiveOrderedMultiDict.make, "invalid")

    def test_get_with_default(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("core",)] = "value"
        self.assertEqual("value", config.get(("core",)))
        self.assertEqual("value", config.get(("CORE",)))
        self.assertEqual("default", config.get(("missing",), "default"))
        # Test SENTINEL behavior
        result = config.get(("missing",))
        self.assertIsInstance(result, CaseInsensitiveOrderedMultiDict)
        self.assertEqual(0, len(result))

    def test_setdefault(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        # Set new value
        result1 = config.setdefault(("core",), "value1")
        self.assertEqual("value1", result1)
        self.assertEqual("value1", config[("core",)])
        # Try to set again with different case - should return existing
        result2 = config.setdefault(("CORE",), "value2")
        self.assertEqual("value1", result2)
        self.assertEqual("value1", config[("core",)])

    def test_values(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("core",)] = "value1"
        config[("other",)] = "value2"
        config[("CORE",)] = "value3"  # Overwrites previous core value
        self.assertEqual({"value3", "value2"}, set(config.values()))

    def test_items_iteration(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("core",)] = "value1"
        config[("other",)] = "value2"
        config[("CORE",)] = "value3"
        items = list(config.items())
        self.assertEqual(3, len(items))
        self.assertEqual((("core",), "value1"), items[0])
        self.assertEqual((("other",), "value2"), items[1])
        self.assertEqual((("CORE",), "value3"), items[2])

    def test_str_keys(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config["core"] = "value"
        self.assertEqual("value", config["CORE"])
        self.assertEqual("value", config["CoRe"])

    def test_nested_tuple_keys(self) -> None:
        config = CaseInsensitiveOrderedMultiDict()
        config[("branch", "master")] = "value"
        # Section names are case-insensitive
        self.assertEqual("value", config[("BRANCH", "master")])
        self.assertEqual("value", config[("Branch", "master")])
        # But subsection names are case-sensitive
        with self.assertRaises(KeyError):
            config[("branch", "MASTER")]


class ConfigFileSetTests(TestCase):
    def test_set_replaces_value(self) -> None:
        # Test that set() replaces the value instead of appending
        cf = ConfigFile()
        cf.set((b"core",), b"sshCommand", b"ssh -i ~/.ssh/id_rsa1")
        cf.set((b"core",), b"sshCommand", b"ssh -i ~/.ssh/id_rsa2")

        # Should only have one value
        self.assertEqual(b"ssh -i ~/.ssh/id_rsa2", cf.get((b"core",), b"sshCommand"))

        # When written to file, should only have one entry
        f = BytesIO()
        cf.write_to_file(f)
        content = f.getvalue()
        self.assertEqual(1, content.count(b"sshCommand"))
        self.assertIn(b"sshCommand = ssh -i ~/.ssh/id_rsa2", content)
        self.assertNotIn(b"id_rsa1", content)
