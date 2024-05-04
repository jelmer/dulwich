# test_utils.py -- Tests for git compatibility utilities
# Copyright (C) 2010 Google, Inc.
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

"""Tests for git compatibility utilities."""

from .. import SkipTest, TestCase
from . import utils


class GitVersionTests(TestCase):
    def setUp(self):
        super().setUp()
        self._orig_run_git = utils.run_git
        self._version_str = None  # tests can override to set stub version

        def run_git(args, **unused_kwargs):
            self.assertEqual(["--version"], args)
            return 0, self._version_str, ""

        utils.run_git = run_git

    def tearDown(self):
        super().tearDown()
        utils.run_git = self._orig_run_git

    def test_git_version_none(self):
        self._version_str = b"not a git version"
        self.assertEqual(None, utils.git_version())

    def test_git_version_3(self):
        self._version_str = b"git version 1.6.6"
        self.assertEqual((1, 6, 6, 0), utils.git_version())

    def test_git_version_4(self):
        self._version_str = b"git version 1.7.0.2"
        self.assertEqual((1, 7, 0, 2), utils.git_version())

    def test_git_version_extra(self):
        self._version_str = b"git version 1.7.0.3.295.gd8fa2"
        self.assertEqual((1, 7, 0, 3), utils.git_version())

    def assertRequireSucceeds(self, required_version):
        try:
            utils.require_git_version(required_version)
        except SkipTest:
            self.fail()

    def assertRequireFails(self, required_version):
        self.assertRaises(SkipTest, utils.require_git_version, required_version)

    def test_require_git_version(self):
        try:
            self._version_str = b"git version 1.6.6"
            self.assertRequireSucceeds((1, 6, 6))
            self.assertRequireSucceeds((1, 6, 6, 0))
            self.assertRequireSucceeds((1, 6, 5))
            self.assertRequireSucceeds((1, 6, 5, 99))
            self.assertRequireFails((1, 7, 0))
            self.assertRequireFails((1, 7, 0, 2))
            self.assertRaises(ValueError, utils.require_git_version, (1, 6, 6, 0, 0))

            self._version_str = b"git version 1.7.0.2"
            self.assertRequireSucceeds((1, 6, 6))
            self.assertRequireSucceeds((1, 6, 6, 0))
            self.assertRequireSucceeds((1, 7, 0))
            self.assertRequireSucceeds((1, 7, 0, 2))
            self.assertRequireFails((1, 7, 0, 3))
            self.assertRequireFails((1, 7, 1))
        except SkipTest as e:
            # This test is designed to catch all SkipTest exceptions.
            self.fail(f"Test unexpectedly skipped: {e}")
