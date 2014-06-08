# test_config.py -- Tests for reading and writing configuration files
# Copyright (C) 2011 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
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

"""Tests for reading and writing configuration files."""

from io import BytesIO
from dulwich.config import (
    ConfigDict,
    ConfigFile,
    StackedConfig,
    _check_section_name,
    _check_variable_name,
    _format_string,
    _escape_value,
    _parse_string,
    _unescape_value,
    )
from dulwich.tests import TestCase


class ConfigFileTests(TestCase):

    def from_file(self, text):
        return ConfigFile.from_file(BytesIO(text))

    def test_empty(self):
        ConfigFile()

    def test_eq(self):
        self.assertEqual(ConfigFile(), ConfigFile())

    def test_default_config(self):
        cf = self.from_file("""[core]
	repositoryformatversion = 0
	filemode = true
	bare = false
	logallrefupdates = true
""")
        self.assertEqual(ConfigFile({("core", ): {
            "repositoryformatversion": "0",
            "filemode": "true",
            "bare": "false",
            "logallrefupdates": "true"}}), cf)

    def test_from_file_empty(self):
        cf = self.from_file("")
        self.assertEqual(ConfigFile(), cf)

    def test_empty_line_before_section(self):
        cf = self.from_file("\n[section]\n")
        self.assertEqual(ConfigFile({("section", ): {}}), cf)

    def test_comment_before_section(self):
        cf = self.from_file("# foo\n[section]\n")
        self.assertEqual(ConfigFile({("section", ): {}}), cf)

    def test_comment_after_section(self):
        cf = self.from_file("[section] # foo\n")
        self.assertEqual(ConfigFile({("section", ): {}}), cf)

    def test_comment_after_variable(self):
        cf = self.from_file("[section]\nbar= foo # a comment\n")
        self.assertEqual(ConfigFile({("section", ): {"bar": "foo"}}), cf)

    def test_from_file_section(self):
        cf = self.from_file("[core]\nfoo = bar\n")
        self.assertEqual("bar", cf.get(("core", ), "foo"))
        self.assertEqual("bar", cf.get(("core", "foo"), "foo"))

    def test_from_file_section_case_insensitive(self):
        cf = self.from_file("[cOre]\nfOo = bar\n")
        self.assertEqual("bar", cf.get(("core", ), "foo"))
        self.assertEqual("bar", cf.get(("core", "foo"), "foo"))

    def test_from_file_with_mixed_quoted(self):
        cf = self.from_file("[core]\nfoo = \"bar\"la\n")
        self.assertEqual("barla", cf.get(("core", ), "foo"))

    def test_from_file_with_open_quoted(self):
        self.assertRaises(ValueError,
            self.from_file, "[core]\nfoo = \"bar\n")

    def test_from_file_with_quotes(self):
        cf = self.from_file(
            "[core]\n"
            'foo = " bar"\n')
        self.assertEqual(" bar", cf.get(("core", ), "foo"))

    def test_from_file_with_interrupted_line(self):
        cf = self.from_file(
            "[core]\n"
            'foo = bar\\\n'
            ' la\n')
        self.assertEqual("barla", cf.get(("core", ), "foo"))

    def test_from_file_with_boolean_setting(self):
        cf = self.from_file(
            "[core]\n"
            'foo\n')
        self.assertEqual("true", cf.get(("core", ), "foo"))

    def test_from_file_subsection(self):
        cf = self.from_file("[branch \"foo\"]\nfoo = bar\n")
        self.assertEqual("bar", cf.get(("branch", "foo"), "foo"))

    def test_from_file_subsection_invalid(self):
        self.assertRaises(ValueError,
            self.from_file, "[branch \"foo]\nfoo = bar\n")

    def test_from_file_subsection_not_quoted(self):
        cf = self.from_file("[branch.foo]\nfoo = bar\n")
        self.assertEqual("bar", cf.get(("branch", "foo"), "foo"))

    def test_write_to_file_empty(self):
        c = ConfigFile()
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual("", f.getvalue())

    def test_write_to_file_section(self):
        c = ConfigFile()
        c.set(("core", ), "foo", "bar")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual("[core]\n\tfoo = bar\n", f.getvalue())

    def test_write_to_file_subsection(self):
        c = ConfigFile()
        c.set(("branch", "blie"), "foo", "bar")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual("[branch \"blie\"]\n\tfoo = bar\n", f.getvalue())

    def test_same_line(self):
        cf = self.from_file("[branch.foo] foo = bar\n")
        self.assertEqual("bar", cf.get(("branch", "foo"), "foo"))


class ConfigDictTests(TestCase):

    def test_get_set(self):
        cd = ConfigDict()
        self.assertRaises(KeyError, cd.get, "foo", "core")
        cd.set(("core", ), "foo", "bla")
        self.assertEqual("bla", cd.get(("core", ), "foo"))
        cd.set(("core", ), "foo", "bloe")
        self.assertEqual("bloe", cd.get(("core", ), "foo"))

    def test_get_boolean(self):
        cd = ConfigDict()
        cd.set(("core", ), "foo", "true")
        self.assertTrue(cd.get_boolean(("core", ), "foo"))
        cd.set(("core", ), "foo", "false")
        self.assertFalse(cd.get_boolean(("core", ), "foo"))
        cd.set(("core", ), "foo", "invalid")
        self.assertRaises(ValueError, cd.get_boolean, ("core", ), "foo")

    def test_dict(self):
        cd = ConfigDict()
        cd.set(("core", ), "foo", "bla")
        cd.set(("core2", ), "foo", "bloe")

        self.assertEqual([("core", ), ("core2", )], cd.keys())
        self.assertEqual(cd[("core", )], {'foo': 'bla'})

        cd['a'] = 'b'
        self.assertEqual(cd['a'], 'b')

    def test_iteritems(self):
        cd = ConfigDict()
        cd.set(("core", ), "foo", "bla")
        cd.set(("core2", ), "foo", "bloe")

        self.assertEqual(
            [('foo', 'bla')],
            list(cd.iteritems(("core", ))))

    def test_iteritems_nonexistant(self):
        cd = ConfigDict()
        cd.set(("core2", ), "foo", "bloe")

        self.assertEqual([],
            list(cd.iteritems(("core", ))))

    def test_itersections(self):
        cd = ConfigDict()
        cd.set(("core2", ), "foo", "bloe")

        self.assertEqual([("core2", )],
            list(cd.itersections()))



class StackedConfigTests(TestCase):

    def test_default_backends(self):
        StackedConfig.default_backends()


class UnescapeTests(TestCase):

    def test_nothing(self):
        self.assertEqual("", _unescape_value(""))

    def test_tab(self):
        self.assertEqual("\tbar\t", _unescape_value("\\tbar\\t"))

    def test_newline(self):
        self.assertEqual("\nbar\t", _unescape_value("\\nbar\\t"))

    def test_quote(self):
        self.assertEqual("\"foo\"", _unescape_value("\\\"foo\\\""))


class EscapeValueTests(TestCase):

    def test_nothing(self):
        self.assertEqual("foo", _escape_value("foo"))

    def test_backslash(self):
        self.assertEqual("foo\\\\", _escape_value("foo\\"))

    def test_newline(self):
        self.assertEqual("foo\\n", _escape_value("foo\n"))


class FormatStringTests(TestCase):

    def test_quoted(self):
        self.assertEqual('" foo"', _format_string(" foo"))
        self.assertEqual('"\\tfoo"', _format_string("\tfoo"))

    def test_not_quoted(self):
        self.assertEqual('foo', _format_string("foo"))
        self.assertEqual('foo bar', _format_string("foo bar"))


class ParseStringTests(TestCase):

    def test_quoted(self):
        self.assertEqual(' foo', _parse_string('" foo"'))
        self.assertEqual('\tfoo', _parse_string('"\\tfoo"'))

    def test_not_quoted(self):
        self.assertEqual('foo', _parse_string("foo"))
        self.assertEqual('foo bar', _parse_string("foo bar"))


class CheckVariableNameTests(TestCase):

    def test_invalid(self):
        self.assertFalse(_check_variable_name("foo "))
        self.assertFalse(_check_variable_name("bar,bar"))
        self.assertFalse(_check_variable_name("bar.bar"))

    def test_valid(self):
        self.assertTrue(_check_variable_name("FOO"))
        self.assertTrue(_check_variable_name("foo"))
        self.assertTrue(_check_variable_name("foo-bar"))


class CheckSectionNameTests(TestCase):

    def test_invalid(self):
        self.assertFalse(_check_section_name("foo "))
        self.assertFalse(_check_section_name("bar,bar"))

    def test_valid(self):
        self.assertTrue(_check_section_name("FOO"))
        self.assertTrue(_check_section_name("foo"))
        self.assertTrue(_check_section_name("foo-bar"))
        self.assertTrue(_check_section_name("bar.bar"))
