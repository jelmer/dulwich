# test_mailmap.py -- Tests for dulwich.mailmap
# Copyright (C) 2018 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.mailmap."""

from io import BytesIO

from unittest import TestCase

from dulwich.mailmap import Mailmap, read_mailmap


class ReadMailmapTests(TestCase):

    def test_read(self):
        b = BytesIO("""\
Jane Doe         <jane@desktop.(none)>
Joe R. Developer <joe@example.com>
# A comment
<cto@company.xx>                       <cto@coompany.xx> # Comment
Some Dude <some@dude.xx>         nick1 <bugs@company.xx>
Other Author <other@author.xx>   nick2 <bugs@company.xx>
Other Author <other@author.xx>         <nick2@company.xx>
Santa Claus <santa.claus@northpole.xx> <me@company.xx>
""")
        self.assertEqual([
            (('Jane Doe', 'jane@desktop.(none)'), None),
            (('Joe R. Developer', 'joe@example.com'), None),
            ((None, 'cto@company.xx'), (None, 'cto@coompany.xx')),
            (('Some Dude', 'some@dude.xx'), ('nick1', 'bugs@company.xx')),
            (('Other Author', 'other@author.xx'),
                ('nick2', 'bugs@company.xx')),
            (('Other Author', 'other@author.xx'),
                (None, 'nick2@company.xx')),
            (('Santa Claus', 'santa.claus@northpole.xx'),
                (None, 'me@company.xx'))],
            list(read_mailmap(b)))


class MailmapTests(TestCase):

    def test_lookup(self):
        m = Mailmap()
        m.add_entry(('Jane Doe', 'jane@desktop.(none)'), (None, None))
        m.add_entry(('Joe R. Developer', 'joe@example.com'), None)
        m.add_entry((None, 'cto@company.xx'), (None, 'cto@coompany.xx'))
        m.add_entry(
                ('Some Dude', 'some@dude.xx'),
                ('nick1', 'bugs@company.xx'))
        m.add_entry(
                ('Other Author', 'other@author.xx'),
                ('nick2', 'bugs@company.xx'))
        m.add_entry(
                ('Other Author', 'other@author.xx'),
                (None, 'nick2@company.xx'))
        m.add_entry(
                ('Santa Claus', 'santa.claus@northpole.xx'),
                (None, 'me@company.xx'))
        self.assertEqual(
            'Jane Doe <jane@desktop.(none)>',
            m.lookup('Jane Doe <jane@desktop.(none)>'))
        self.assertEqual(
            'Jane Doe <jane@desktop.(none)>',
            m.lookup('Jane Doe <jane@example.com>'))
        self.assertEqual(
            'Jane Doe <jane@desktop.(none)>',
            m.lookup('Jane D. <jane@desktop.(none)>'))
        self.assertEqual(
            'Some Dude <some@dude.xx>',
            m.lookup('nick1 <bugs@company.xx>'))
        self.assertEqual(
            'CTO <cto@company.xx>',
            m.lookup('CTO <cto@coompany.xx>'))
