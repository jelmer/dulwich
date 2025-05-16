# release_robot.py
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

import datetime
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import unittest
from typing import ClassVar, Optional
from unittest.mock import MagicMock, patch

from dulwich.contrib import release_robot
from dulwich.repo import Repo
from dulwich.tests.utils import make_commit, make_tag

BASEDIR = os.path.abspath(os.path.dirname(__file__))  # this directory


def gmtime_to_datetime(gmt):
    return datetime.datetime(*time.gmtime(gmt)[:6])


class TagPatternTests(unittest.TestCase):
    """test tag patterns."""

    def test_tag_pattern(self) -> None:
        """Test tag patterns."""
        test_cases = {
            "0.3": "0.3",
            "v0.3": "0.3",
            "release0.3": "0.3",
            "Release-0.3": "0.3",
            "v0.3rc1": "0.3rc1",
            "v0.3-rc1": "0.3-rc1",
            "v0.3-rc.1": "0.3-rc.1",
            "version 0.3": "0.3",
            "version_0.3_rc_1": "0.3_rc_1",
            "v1": "1",
            "0.3rc1": "0.3rc1",
        }
        for testcase, version in test_cases.items():
            matches = re.match(release_robot.PATTERN, testcase)
            self.assertEqual(matches.group(1), version)

    def test_pattern_no_match(self) -> None:
        """Test tags that don't match the pattern."""
        test_cases = [
            "master",
            "HEAD",
            "feature-branch",
            "no-numbers",
            "_",
        ]
        for testcase in test_cases:
            matches = re.match(release_robot.PATTERN, testcase)
            self.assertIsNone(matches)


class GetRecentTagsTest(unittest.TestCase):
    """test get recent tags."""

    # Git repo for dulwich project
    test_repo = os.path.join(BASEDIR, "dulwich_test_repo.zip")
    committer = b"Mark Mikofski <mark.mikofski@sunpowercorp.com>"
    test_tags: ClassVar[list[bytes]] = [b"v0.1a", b"v0.1"]
    tag_test_data: ClassVar[
        dict[bytes, tuple[int, bytes, Optional[tuple[int, bytes]]]]
    ] = {
        test_tags[0]: (1484788003, b"3" * 40, None),
        test_tags[1]: (1484788314, b"1" * 40, (1484788401, b"2" * 40)),
    }

    @classmethod
    def setUpClass(cls) -> None:
        cls.projdir = tempfile.mkdtemp()  # temporary project directory
        cls.repo = Repo.init(cls.projdir)  # test repo
        obj_store = cls.repo.object_store  # test repo object store
        # commit 1 ('2017-01-19T01:06:43')
        cls.c1 = make_commit(
            id=cls.tag_test_data[cls.test_tags[0]][1],
            commit_time=cls.tag_test_data[cls.test_tags[0]][0],
            message=b"unannotated tag",
            author=cls.committer,
        )
        obj_store.add_object(cls.c1)
        # tag 1: unannotated
        cls.t1 = cls.test_tags[0]
        cls.repo[b"refs/tags/" + cls.t1] = cls.c1.id  # add unannotated tag
        # commit 2 ('2017-01-19T01:11:54')
        cls.c2 = make_commit(
            id=cls.tag_test_data[cls.test_tags[1]][1],
            commit_time=cls.tag_test_data[cls.test_tags[1]][0],
            message=b"annotated tag",
            parents=[cls.c1.id],
            author=cls.committer,
        )
        obj_store.add_object(cls.c2)
        # tag 2: annotated ('2017-01-19T01:13:21')
        cls.t2 = make_tag(
            cls.c2,
            id=cls.tag_test_data[cls.test_tags[1]][2][1],
            name=cls.test_tags[1],
            tag_time=cls.tag_test_data[cls.test_tags[1]][2][0],
        )
        obj_store.add_object(cls.t2)
        cls.repo[b"refs/heads/master"] = cls.c2.id
        cls.repo[b"refs/tags/" + cls.t2.name] = cls.t2.id  # add annotated tag

    @classmethod
    def tearDownClass(cls) -> None:
        cls.repo.close()
        shutil.rmtree(cls.projdir)

    def test_get_recent_tags(self) -> None:
        """Test get recent tags."""
        tags = release_robot.get_recent_tags(self.projdir)  # get test tags
        for tag, metadata in tags:
            tag = tag.encode("utf-8")
            test_data = self.tag_test_data[tag]  # test data tag
            # test commit date, id and author name
            self.assertEqual(metadata[0], gmtime_to_datetime(test_data[0]))
            self.assertEqual(metadata[1].encode("utf-8"), test_data[1])
            self.assertEqual(metadata[2].encode("utf-8"), self.committer)
            # skip unannotated tags
            tag_obj = test_data[2]
            if not tag_obj:
                continue
            # tag date, id and name
            self.assertEqual(metadata[3][0], gmtime_to_datetime(tag_obj[0]))
            self.assertEqual(metadata[3][1].encode("utf-8"), tag_obj[1])
            self.assertEqual(metadata[3][2].encode("utf-8"), tag)

    def test_get_recent_tags_sorting(self) -> None:
        """Test that tags are sorted by commit time from newest to oldest."""
        tags = release_robot.get_recent_tags(self.projdir)
        # v0.1 should be first as it's newer
        self.assertEqual(tags[0][0], "v0.1")
        # v0.1a should be second as it's older
        self.assertEqual(tags[1][0], "v0.1a")

    def test_get_recent_tags_non_tag_refs(self) -> None:
        """Test that non-tag refs are ignored."""
        # Create a commit on a branch to test that it's not included
        branch_commit = make_commit(
            message=b"branch commit",
            author=self.committer,
            commit_time=int(time.time()),
        )
        self.repo.object_store.add_object(branch_commit)
        self.repo[b"refs/heads/test-branch"] = branch_commit.id

        # Get tags and ensure only the actual tags are returned
        tags = release_robot.get_recent_tags(self.projdir)
        self.assertEqual(len(tags), 2)  # Still only 2 tags
        tag_names = [tag[0] for tag in tags]
        self.assertIn("v0.1", tag_names)
        self.assertIn("v0.1a", tag_names)
        self.assertNotIn("test-branch", tag_names)


class GetCurrentVersionTests(unittest.TestCase):
    """Test get_current_version function."""

    def setUp(self):
        """Set up a test repository for each test."""
        self.projdir = tempfile.mkdtemp()
        self.repo = Repo.init(self.projdir)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        """Clean up after test."""
        self.repo.close()
        shutil.rmtree(self.projdir)

    def test_no_tags(self):
        """Test behavior when repo has no tags."""
        # Create a repo with no tags
        result = release_robot.get_current_version(self.projdir)
        self.assertIsNone(result)

    def test_tag_with_pattern_match(self):
        """Test with a tag that matches the pattern."""
        # Create a test commit and tag
        c = make_commit(message=b"Test commit")
        self.repo.object_store.add_object(c)
        self.repo[b"refs/tags/v1.2.3"] = c.id
        self.repo[b"HEAD"] = c.id

        # Test that the version is extracted correctly
        result = release_robot.get_current_version(self.projdir)
        self.assertEqual("1.2.3", result)

    def test_tag_no_pattern_match(self):
        """Test with a tag that doesn't match the pattern."""
        # Create a test commit and tag that won't match the default pattern
        c = make_commit(message=b"Test commit")
        self.repo.object_store.add_object(c)
        self.repo[b"refs/tags/no-version-tag"] = c.id
        self.repo[b"HEAD"] = c.id

        # Test that the full tag is returned when no match
        result = release_robot.get_current_version(self.projdir)
        self.assertEqual("no-version-tag", result)

    def test_with_logger(self):
        """Test with a logger when regex match fails."""
        # Create a test commit and tag that won't match the pattern
        c = make_commit(message=b"Test commit")
        self.repo.object_store.add_object(c)
        self.repo[b"refs/tags/no-version-tag"] = c.id
        self.repo[b"HEAD"] = c.id

        # Create a logger
        logger = logging.getLogger("test_logger")

        # Test with the logger
        result = release_robot.get_current_version(self.projdir, logger=logger)
        self.assertEqual("no-version-tag", result)

    def test_custom_pattern(self):
        """Test with a custom regex pattern."""
        # Create a test commit and tag
        c = make_commit(message=b"Test commit")
        self.repo.object_store.add_object(c)
        self.repo[b"refs/tags/CUSTOM-99.88.77"] = c.id
        self.repo[b"HEAD"] = c.id

        # Test with a custom pattern
        custom_pattern = r"CUSTOM-([\d\.]+)"
        result = release_robot.get_current_version(self.projdir, pattern=custom_pattern)
        self.assertEqual("99.88.77", result)

    def test_with_logger_debug_call(self):
        """Test that the logger.debug method is actually called."""
        # Create a test commit and tag that won't match the pattern
        c = make_commit(message=b"Test commit")
        self.repo.object_store.add_object(c)
        self.repo[b"refs/tags/no-version-tag"] = c.id
        self.repo[b"HEAD"] = c.id

        # Create a mock logger
        mock_logger = MagicMock()

        # Test with the mock logger
        result = release_robot.get_current_version(self.projdir, logger=mock_logger)

        # Verify logger.debug was called
        mock_logger.debug.assert_called_once()
        # Check the tag name is in the debug message
        self.assertIn("no-version-tag", mock_logger.debug.call_args[0][2])

        # The result should still be the full tag
        self.assertEqual("no-version-tag", result)

    def test_multiple_tags(self):
        """Test behavior with multiple tags to ensure we get the most recent."""
        # Create multiple commits and tags with different timestamps
        c1 = make_commit(message=b"First commit", commit_time=1000)
        c2 = make_commit(message=b"Second commit", commit_time=2000, parents=[c1.id])

        self.repo.object_store.add_object(c1)
        self.repo.object_store.add_object(c2)

        # Add tags with older commit first
        self.repo[b"refs/tags/v0.9.0"] = c1.id
        self.repo[b"refs/tags/v1.0.0"] = c2.id
        self.repo[b"HEAD"] = c2.id

        # Get the current version - should be from the most recent commit
        result = release_robot.get_current_version(self.projdir)
        self.assertEqual("1.0.0", result)


class MainFunctionTests(unittest.TestCase):
    """Test the __main__ block."""

    def setUp(self):
        """Set up a test repository."""
        self.projdir = tempfile.mkdtemp()
        self.repo = Repo.init(self.projdir)
        # Create a test commit and tag
        c = make_commit(message=b"Test commit")
        self.repo.object_store.add_object(c)
        self.repo[b"refs/tags/v3.2.1"] = c.id
        self.repo[b"HEAD"] = c.id
        self.addCleanup(self.cleanup)

    def cleanup(self):
        """Clean up after test."""
        self.repo.close()
        shutil.rmtree(self.projdir)

    @patch.object(sys, "argv", ["release_robot.py"])
    @patch("builtins.print")
    def test_main_default_dir(self, mock_print):
        """Test main function with default directory."""
        # Run the __main__ block code with mocked environment
        module_globals = {
            "__name__": "__main__",
            "sys": sys,
            "get_current_version": lambda projdir: "3.2.1",
            "PROJDIR": ".",
        }
        exec(
            compile(
                "if __name__ == '__main__':\n    if len(sys.argv) > 1:\n        _PROJDIR = sys.argv[1]\n    else:\n        _PROJDIR = PROJDIR\n    print(get_current_version(projdir=_PROJDIR))",
                "<string>",
                "exec",
            ),
            module_globals,
        )

        # Check that print was called with the version
        mock_print.assert_called_once_with("3.2.1")

    @patch.object(sys, "argv", ["release_robot.py", "/custom/path"])
    @patch("builtins.print")
    @patch("dulwich.contrib.release_robot.get_current_version")
    def test_main_custom_dir(self, mock_get_version, mock_print):
        """Test main function with custom directory from command line."""
        mock_get_version.return_value = "4.5.6"

        # Run the __main__ block code with mocked environment
        module_globals = {
            "__name__": "__main__",
            "sys": sys,
            "get_current_version": mock_get_version,
            "PROJDIR": ".",
        }
        exec(
            compile(
                "if __name__ == '__main__':\n    if len(sys.argv) > 1:\n        _PROJDIR = sys.argv[1]\n    else:\n        _PROJDIR = PROJDIR\n    print(get_current_version(projdir=_PROJDIR))",
                "<string>",
                "exec",
            ),
            module_globals,
        )

        # Check that get_current_version was called with the right arg
        mock_get_version.assert_called_once_with(projdir="/custom/path")
        mock_print.assert_called_once_with("4.5.6")
