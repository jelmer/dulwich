# test_lfs.py -- tests for LFS
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for LFS support."""

import shutil
import tempfile

from dulwich.lfs import LFSStore

from . import TestCase


class LFSTests(TestCase):
    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.lfs = LFSStore.create(self.test_dir)

    def test_create(self):
        sha = self.lfs.write_object([b"a", b"b"])
        with self.lfs.open_object(sha) as f:
            self.assertEqual(b"ab", f.read())

    def test_missing(self):
        self.assertRaises(KeyError, self.lfs.open_object, "abcdeabcdeabcdeabcde")
