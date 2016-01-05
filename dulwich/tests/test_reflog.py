# test_reflog.py -- tests for reflog.py
# encoding: utf-8
# Copyright (C) 2015 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
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
        self.assertEqual(
                (b'0000000000000000000000000000000000000000',
                 b'49030649db3dfec5a9bc03e5dde4255a14499f16',
                 b'Jelmer Vernooij <jelmer@jelmer.uk>',
                 1446552482, 0, b'clone: from git://jelmer.uk/samba'),
                 parse_reflog_line(
                     b'0000000000000000000000000000000000000000 '
                     b'49030649db3dfec5a9bc03e5dde4255a14499f16 Jelmer Vernooij '
                     b'<jelmer@jelmer.uk> 1446552482 +0000	'
                     b'clone: from git://jelmer.uk/samba'))
