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
    ConfigFile,
    StackedConfig,
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


class StackedConfigTests(TestCase):

    def test_default(self):
        StackedConfig.default()
