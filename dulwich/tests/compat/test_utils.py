# test_utils.py -- Tests for git compatibility utilities
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License

# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA  02110-1301, USA.

"""Tests for git compatibility utilities."""

from unittest import SkipTest

from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.compat import utils


class GitVersionTests(TestCase):

    def setUp(self):
        super(GitVersionTests, self).setUp()
        self._orig_run_git = utils.run_git
        self._version_str = None  # tests can override to set stub version

        def run_git(args, **unused_kwargs):
            self.assertEqual(['--version'], args)
            return 0, self._version_str
        utils.run_git = run_git

    def tearDown(self):
        super(GitVersionTests, self).tearDown()
        utils.run_git = self._orig_run_git

    def test_git_version_none(self):
        self._version_str = 'not a git version'
        self.assertEqual(None, utils.git_version())

    def test_git_version_3(self):
        self._version_str = 'git version 1.6.6'
        self.assertEqual((1, 6, 6, 0), utils.git_version())

    def test_git_version_4(self):
        self._version_str = 'git version 1.7.0.2'
        self.assertEqual((1, 7, 0, 2), utils.git_version())

    def test_git_version_extra(self):
        self._version_str = 'git version 1.7.0.3.295.gd8fa2'
        self.assertEqual((1, 7, 0, 3), utils.git_version())

    def assertRequireSucceeds(self, required_version):
        try:
            utils.require_git_version(required_version)
        except SkipTest:
            self.fail()

    def assertRequireFails(self, required_version):
        self.assertRaises(SkipTest, utils.require_git_version,
                          required_version)

    def test_require_git_version(self):
        try:
            self._version_str = 'git version 1.6.6'
            self.assertRequireSucceeds((1, 6, 6))
            self.assertRequireSucceeds((1, 6, 6, 0))
            self.assertRequireSucceeds((1, 6, 5))
            self.assertRequireSucceeds((1, 6, 5, 99))
            self.assertRequireFails((1, 7, 0))
            self.assertRequireFails((1, 7, 0, 2))
            self.assertRaises(ValueError, utils.require_git_version,
                              (1, 6, 6, 0, 0))

            self._version_str = 'git version 1.7.0.2'
            self.assertRequireSucceeds((1, 6, 6))
            self.assertRequireSucceeds((1, 6, 6, 0))
            self.assertRequireSucceeds((1, 7, 0))
            self.assertRequireSucceeds((1, 7, 0, 2))
            self.assertRequireFails((1, 7, 0, 3))
            self.assertRequireFails((1, 7, 1))
        except SkipTest as e:
            # This test is designed to catch all SkipTest exceptions.
            self.fail('Test unexpectedly skipped: %s' % e)
