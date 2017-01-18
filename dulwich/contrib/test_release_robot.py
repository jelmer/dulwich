# release_robot.py
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

"""Tests for release_robot."""

import json
import os
import re
import unittest

import dulwich
from dulwich.contrib import release_robot

BASEDIR = os.path.dirname(__file__)  # this directory
# path to dulwich tag test data, also in this folder
DULWICH_TAG_TEST_DATA = os.path.join(BASEDIR, 'dulwich_tag_test.dat')
with open(DULWICH_TAG_TEST_DATA, 'r') as f:
    DULWICH_TAG_TEST_DATA = json.load(f)  # dictionary of tags
# Git repo for dulwich project
PROJDIR = os.path.abspath(os.path.dirname(os.path.dirname(dulwich.__file__)))


class TagPatternTests(unittest.TestCase):

    def test_tag_pattern(self):
        test_cases = {
            '0.3': '0.3', 'v0.3': '0.3', 'release0.3': '0.3',
            'Release-0.3': '0.3', 'v0.3rc1': '0.3rc1', 'v0.3-rc1': '0.3-rc1',
            'v0.3-rc.1': '0.3-rc.1', 'version 0.3': '0.3',
            'version_0.3_rc_1': '0.3_rc_1', 'v1': '1', '0.3rc1': '0.3rc1'
        }
        for tc, version in test_cases.items():
            m = re.match(release_robot.PATTERN, tc)
            self.assertEqual(m.group(1), version)

    def test_dulwich_tags(self):
        tags = release_robot.get_recent_tags(PROJDIR)  # get test tags
        self.assertEqual(len(DULWICH_TAG_TEST_DATA), 95)  # number of test tags
        for tag, metadata in tags[-95:]:
            test_data = DULWICH_TAG_TEST_DATA[tag]
            # test commit meta data
            self.assertEqual(metadata[0].isoformat(), test_data[0])  # date
            self.assertEqual(metadata[1], test_data[1])  # id
            # test author, encode unicode as utf-8 for comparison
            self.assertEqual(metadata[2], test_data[2].encode('utf-8'))
            # test tag meta
            if None in test_data[3]:
                # skip since no tag meta data
                continue
            # tag date
            self.assertEqual(metadata[3][0].isoformat(), test_data[3][0])
            self.assertEqual(metadata[3][1], test_data[3][1])  # tag id
            self.assertEqual(metadata[3][2], test_data[3][2])  # tag name
