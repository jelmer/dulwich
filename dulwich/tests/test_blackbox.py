# test_blackbox.py -- blackbox tests
# Copyright (C) 2010 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version.
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

"""Blackbox tests for Dulwich commands."""

import tempfile

from dulwich.repo import (
    Repo,
    )
from dulwich.tests import (
    BlackboxTestCase,
    )


class GitReceivePackTests(BlackboxTestCase):
    """Blackbox tests for dul-receive-pack."""

    def setUp(self):
        super(GitReceivePackTests, self).setUp()
        self.path = tempfile.mkdtemp()
        self.repo = Repo.init(self.path)

    def test_basic(self):
        process = self.run_command("dul-receive-pack", [self.path])
        (stdout, stderr) = process.communicate("0000")
        self.assertEqual('', stderr)
        self.assertEqual('0000', stdout[-4:])
        self.assertEqual(0, process.returncode)

    def test_missing_arg(self):
        process = self.run_command("dul-receive-pack", [])
        (stdout, stderr) = process.communicate()
        self.assertEqual('usage: dul-receive-pack <git-dir>\n', stderr)
        self.assertEqual('', stdout)
        self.assertEqual(1, process.returncode)


class GitUploadPackTests(BlackboxTestCase):
    """Blackbox tests for dul-upload-pack."""

    def setUp(self):
        super(GitUploadPackTests, self).setUp()
        self.path = tempfile.mkdtemp()
        self.repo = Repo.init(self.path)

    def test_missing_arg(self):
        process = self.run_command("dul-upload-pack", [])
        (stdout, stderr) = process.communicate()
        self.assertEqual('usage: dul-upload-pack <git-dir>\n', stderr)
        self.assertEqual('', stdout)
        self.assertEqual(1, process.returncode)
