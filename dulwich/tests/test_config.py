# test_config.py -- Tests for reading and writing configuration files
# Copyright (C) 2011 Jelmer Vernooij <jelmer@samba.org>
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

"""Tests for reading and writing configuration files."""

from io import BytesIO
import os
from dulwich.config import (
    ConfigDict,
    ConfigFile,
    StackedConfig,
    _check_section_name,
    _check_variable_name,
    _format_string,
    _escape_value,
    _parse_string,
    parse_submodules,
    )
from dulwich.tests import (
    TestCase,
    )


class ConfigFileTests(TestCase):

    def from_file(self, text):
        return ConfigFile.from_file(BytesIO(text))

    def test_empty(self):
        ConfigFile()

    def test_eq(self):
        self.assertEqual(ConfigFile(), ConfigFile())

    def test_default_config(self):
        cf = self.from_file(b"""[core]
	repositoryformatversion = 0
	filemode = true
	bare = false
	logallrefupdates = true
""")
        self.assertEqual(ConfigFile({(b"core", ): {
            b"repositoryformatversion": b"0",
            b"filemode": b"true",
            b"bare": b"false",
            b"logallrefupdates": b"true"}}), cf)

    def test_from_file_empty(self):
        cf = self.from_file(b"")
        self.assertEqual(ConfigFile(), cf)

    def test_empty_line_before_section(self):
        cf = self.from_file(b"\n[section]\n")
        self.assertEqual(ConfigFile({(b"section", ): {}}), cf)

    def test_comment_before_section(self):
        cf = self.from_file(b"# foo\n[section]\n")
        self.assertEqual(ConfigFile({(b"section", ): {}}), cf)

    def test_comment_after_section(self):
        cf = self.from_file(b"[section] # foo\n")
        self.assertEqual(ConfigFile({(b"section", ): {}}), cf)

    def test_comment_after_variable(self):
        cf = self.from_file(b"[section]\nbar= foo # a comment\n")
        self.assertEqual(ConfigFile({(b"section", ): {b"bar": b"foo"}}), cf)

    def test_from_file_section(self):
        cf = self.from_file(b"[core]\nfoo = bar\n")
        self.assertEqual(b"bar", cf.get((b"core", ), b"foo"))
        self.assertEqual(b"bar", cf.get((b"core", b"foo"), b"foo"))

    def test_from_file_section_case_insensitive(self):
        cf = self.from_file(b"[cOre]\nfOo = bar\n")
        self.assertEqual(b"bar", cf.get((b"core", ), b"foo"))
        self.assertEqual(b"bar", cf.get((b"core", b"foo"), b"foo"))

    def test_from_file_with_mixed_quoted(self):
        cf = self.from_file(b"[core]\nfoo = \"bar\"la\n")
        self.assertEqual(b"barla", cf.get((b"core", ), b"foo"))

    def test_from_file_with_open_quoted(self):
        self.assertRaises(ValueError,
            self.from_file, b"[core]\nfoo = \"bar\n")

    def test_from_file_with_quotes(self):
        cf = self.from_file(
            b"[core]\n"
            b'foo = " bar"\n')
        self.assertEqual(b" bar", cf.get((b"core", ), b"foo"))

    def test_from_file_with_interrupted_line(self):
        cf = self.from_file(
            b"[core]\n"
            b'foo = bar\\\n'
            b' la\n')
        self.assertEqual(b"barla", cf.get((b"core", ), b"foo"))

    def test_from_file_with_boolean_setting(self):
        cf = self.from_file(
            b"[core]\n"
            b'foo\n')
        self.assertEqual(b"true", cf.get((b"core", ), b"foo"))

    def test_from_file_subsection(self):
        cf = self.from_file(b"[branch \"foo\"]\nfoo = bar\n")
        self.assertEqual(b"bar", cf.get((b"branch", b"foo"), b"foo"))

    def test_from_file_subsection_invalid(self):
        self.assertRaises(ValueError,
            self.from_file, b"[branch \"foo]\nfoo = bar\n")

    def test_from_file_subsection_not_quoted(self):
        cf = self.from_file(b"[branch.foo]\nfoo = bar\n")
        self.assertEqual(b"bar", cf.get((b"branch", b"foo"), b"foo"))

    def test_write_to_file_empty(self):
        c = ConfigFile()
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b"", f.getvalue())

    def test_write_to_file_section(self):
        c = ConfigFile()
        c.set((b"core", ), b"foo", b"bar")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b"[core]\n\tfoo = bar\n", f.getvalue())

    def test_write_to_file_subsection(self):
        c = ConfigFile()
        c.set((b"branch", b"blie"), b"foo", b"bar")
        f = BytesIO()
        c.write_to_file(f)
        self.assertEqual(b"[branch \"blie\"]\n\tfoo = bar\n", f.getvalue())

    def test_same_line(self):
        cf = self.from_file(b"[branch.foo] foo = bar\n")
        self.assertEqual(b"bar", cf.get((b"branch", b"foo"), b"foo"))

    #@expectedFailure
    def test_quoted(self):
        cf = self.from_file(b"""[gui]
	fontdiff = -family \\\"Ubuntu Mono\\\" -size 11 -weight normal -slant roman -underline 0 -overstrike 0
""")
        self.assertEqual(ConfigFile({(b'gui', ): {
            b'fontdiff': b'-family "Ubuntu Mono" -size 11 -weight normal -slant roman -underline 0 -overstrike 0',
        }}), cf)


class ConfigDictTests(TestCase):

    def test_get_set(self):
        cd = ConfigDict()
        self.assertRaises(KeyError, cd.get, b"foo", b"core")
        cd.set((b"core", ), b"foo", b"bla")
        self.assertEqual(b"bla", cd.get((b"core", ), b"foo"))
        cd.set((b"core", ), b"foo", b"bloe")
        self.assertEqual(b"bloe", cd.get((b"core", ), b"foo"))

    def test_get_boolean(self):
        cd = ConfigDict()
        cd.set((b"core", ), b"foo", b"true")
        self.assertTrue(cd.get_boolean((b"core", ), b"foo"))
        cd.set((b"core", ), b"foo", b"false")
        self.assertFalse(cd.get_boolean((b"core", ), b"foo"))
        cd.set((b"core", ), b"foo", b"invalid")
        self.assertRaises(ValueError, cd.get_boolean, (b"core", ), b"foo")

    def test_dict(self):
        cd = ConfigDict()
        cd.set((b"core", ), b"foo", b"bla")
        cd.set((b"core2", ), b"foo", b"bloe")

        self.assertEqual([(b"core", ), (b"core2", )], list(cd.keys()))
        self.assertEqual(cd[(b"core", )], {b'foo': b'bla'})

        cd[b'a'] = b'b'
        self.assertEqual(cd[b'a'], b'b')

    def test_iteritems(self):
        cd = ConfigDict()
        cd.set((b"core", ), b"foo", b"bla")
        cd.set((b"core2", ), b"foo", b"bloe")

        self.assertEqual(
            [(b'foo', b'bla')],
            list(cd.iteritems((b"core", ))))

    def test_iteritems_nonexistant(self):
        cd = ConfigDict()
        cd.set((b"core2", ), b"foo", b"bloe")

        self.assertEqual([],
            list(cd.iteritems((b"core", ))))

    def test_itersections(self):
        cd = ConfigDict()
        cd.set((b"core2", ), b"foo", b"bloe")

        self.assertEqual([(b"core2", )],
            list(cd.itersections()))


class StackedConfigTests(TestCase):

    def test_default_backends(self):
        StackedConfig.default_backends()


class EscapeValueTests(TestCase):

    def test_nothing(self):
        self.assertEqual(b"foo", _escape_value(b"foo"))

    def test_backslash(self):
        self.assertEqual(b"foo\\\\", _escape_value(b"foo\\"))

    def test_newline(self):
        self.assertEqual(b"foo\\n", _escape_value(b"foo\n"))


class FormatStringTests(TestCase):

    def test_quoted(self):
        self.assertEqual(b'" foo"', _format_string(b" foo"))
        self.assertEqual(b'"\\tfoo"', _format_string(b"\tfoo"))

    def test_not_quoted(self):
        self.assertEqual(b'foo', _format_string(b"foo"))
        self.assertEqual(b'foo bar', _format_string(b"foo bar"))


class ParseStringTests(TestCase):

    def test_quoted(self):
        self.assertEqual(b' foo', _parse_string(b'" foo"'))
        self.assertEqual(b'\tfoo', _parse_string(b'"\\tfoo"'))

    def test_not_quoted(self):
        self.assertEqual(b'foo', _parse_string(b"foo"))
        self.assertEqual(b'foo bar', _parse_string(b"foo bar"))

    def test_nothing(self):
        self.assertEqual(b"", _parse_string(b''))

    def test_tab(self):
        self.assertEqual(b"\tbar\t", _parse_string(b"\\tbar\\t"))

    def test_newline(self):
        self.assertEqual(b"\nbar\t", _parse_string(b"\\nbar\\t\t"))

    def test_quote(self):
        self.assertEqual(b"\"foo\"", _parse_string(b"\\\"foo\\\""))


class CheckVariableNameTests(TestCase):

    def test_invalid(self):
        self.assertFalse(_check_variable_name(b"foo "))
        self.assertFalse(_check_variable_name(b"bar,bar"))
        self.assertFalse(_check_variable_name(b"bar.bar"))

    def test_valid(self):
        self.assertTrue(_check_variable_name(b"FOO"))
        self.assertTrue(_check_variable_name(b"foo"))
        self.assertTrue(_check_variable_name(b"foo-bar"))


class CheckSectionNameTests(TestCase):

    def test_invalid(self):
        self.assertFalse(_check_section_name(b"foo "))
        self.assertFalse(_check_section_name(b"bar,bar"))

    def test_valid(self):
        self.assertTrue(_check_section_name(b"FOO"))
        self.assertTrue(_check_section_name(b"foo"))
        self.assertTrue(_check_section_name(b"foo-bar"))
        self.assertTrue(_check_section_name(b"bar.bar"))


class SubmodulesTests(TestCase):

    def testSubmodules(self):
        cf = ConfigFile.from_file(BytesIO(b"""\
[submodule "core/lib"]
	path = core/lib
	url = https://github.com/phhusson/QuasselC.git
"""))
        got = list(parse_submodules(cf))
        self.assertEqual([
            (b'core/lib', b'https://github.com/phhusson/QuasselC.git', b'core/lib')], got)
