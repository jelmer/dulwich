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

"""Tests for reading and writing configuraiton files."""

from cStringIO import StringIO
from dulwich.config import (
    ConfigDict,
    ConfigFile,
    StackedConfig,
    _unescape_value,
    )
from dulwich.tests import TestCase


class ConfigFileTests(TestCase):

    def from_file(self, text):
        return ConfigFile.from_file(StringIO(text))

    def test_empty(self):
        ConfigFile()

    def test_eq(self):
        self.assertEquals(ConfigFile(), ConfigFile())

    def test_from_file_empty(self):
        cf = self.from_file("")
        self.assertEquals(ConfigFile(), cf)


class ConfigDictTests(TestCase):

    def test_get_set(self):
        cd = ConfigDict()
        self.assertRaises(KeyError, cd.get, "core.foo")
        cd.set("core.foo", "bla")
        self.assertEquals("bla", cd.get("core.foo"))
        cd.set("core.foo", "bloe")
        self.assertEquals("bloe", cd.get("core.foo"))


class StackedConfigTests(TestCase):

    def test_default_backends(self):
        StackedConfig.default_backends()


class UnescapeTests(TestCase):

    def test_nothing(self):
        self.assertEquals("", _unescape_value(""))

    def test_tab(self):
        self.assertEquals("\tbar\t", _unescape_value("\\tbar\\t"))

    def test_newline(self):
        self.assertEquals("\nbar\t", _unescape_value("\\nbar\\t"))

    def test_quote(self):
        self.assertEquals("\"foo\"", _unescape_value("\\\"foo\\\""))

