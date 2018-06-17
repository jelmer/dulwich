# test_reflog.py -- tests for reflog.py
# encoding: utf-8
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.reflog."""


from dulwich.reflog import (
    format_reflog_line,
    parse_reflog_line,
    )

from dulwich.tests import (
    TestCase,
    )


class ReflogLineTests(TestCase):

    def test_format(self):
        self.assertEqual(
            b'0000000000000000000000000000000000000000 '
            b'49030649db3dfec5a9bc03e5dde4255a14499f16 Jelmer Vernooij '
            b'<jelmer@jelmer.uk> 1446552482 +0000	'
            b'clone: from git://jelmer.uk/samba',
            format_reflog_line(
                b'0000000000000000000000000000000000000000',
                b'49030649db3dfec5a9bc03e5dde4255a14499f16',
                b'Jelmer Vernooij <jelmer@jelmer.uk>',
                1446552482, 0, b'clone: from git://jelmer.uk/samba'))

        self.assertEqual(
            b'0000000000000000000000000000000000000000 '
            b'49030649db3dfec5a9bc03e5dde4255a14499f16 Jelmer Vernooij '
            b'<jelmer@jelmer.uk> 1446552482 +0000	'
            b'clone: from git://jelmer.uk/samba',
            format_reflog_line(
                None,
                b'49030649db3dfec5a9bc03e5dde4255a14499f16',
                b'Jelmer Vernooij <jelmer@jelmer.uk>',
                1446552482, 0, b'clone: from git://jelmer.uk/samba'))

    def test_parse(self):
        reflog_line = (
                 b'0000000000000000000000000000000000000000 '
                 b'49030649db3dfec5a9bc03e5dde4255a14499f16 Jelmer Vernooij '
                 b'<jelmer@jelmer.uk> 1446552482 +0000	'
                 b'clone: from git://jelmer.uk/samba'
                 )
        self.assertEqual(
                (b'0000000000000000000000000000000000000000',
                 b'49030649db3dfec5a9bc03e5dde4255a14499f16',
                 b'Jelmer Vernooij <jelmer@jelmer.uk>',
                 1446552482, 0, b'clone: from git://jelmer.uk/samba'),
                parse_reflog_line(reflog_line))
