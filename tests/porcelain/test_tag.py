# test_tag.py -- tests for porcelain tag functions
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
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

"""Tests for porcelain tag functions."""

import os
import shutil
import tempfile

from dulwich import porcelain
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo

from .. import TestCase


class TagDeleteTests(TestCase):
    """Tests for tag_delete function."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)

        # Create a simple commit to tag
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"Test commit"
        self.repo.object_store.add_object(commit)

        self.repo.refs[b"refs/heads/main"] = commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super().tearDown()

    def test_tag_delete_with_bytes(self) -> None:
        """Test tag_delete with bytes tag name."""
        # Create a tag
        porcelain.tag_create(self.repo, b"test-tag", annotated=False)
        self.assertIn(b"refs/tags/test-tag", self.repo.refs)

        # Delete the tag with bytes
        porcelain.tag_delete(self.repo, b"test-tag")
        self.assertNotIn(b"refs/tags/test-tag", self.repo.refs)

    def test_tag_delete_invalid_type(self) -> None:
        """Test tag_delete with invalid input type raises Error."""
        from dulwich.porcelain import Error

        # Create a tag
        porcelain.tag_create(self.repo, b"test-tag", annotated=False)

        # Try to delete with invalid type (should raise Error)
        with self.assertRaises(Error) as cm:
            porcelain.tag_delete(self.repo, 123)  # type: ignore

        self.assertEqual("Unexpected tag name type 123", str(cm.exception))


class TagCreateWithEncodingTests(TestCase):
    """Tests for tag_create with encoding parameters."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)

        # Create a simple commit
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"Test commit"
        self.repo.object_store.add_object(commit)

        self.repo.refs[b"refs/heads/main"] = commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super().tearDown()

    def test_tag_create_with_string_tag_name(self) -> None:
        """Test tag_create with string tag name gets encoded."""
        porcelain.tag_create(self.repo, "my-tag", annotated=True, message="Tag message")

        # Verify tag was created
        self.assertIn(b"refs/tags/my-tag", self.repo.refs)

    def test_tag_create_with_string_author(self) -> None:
        """Test tag_create with string author gets encoded."""
        porcelain.tag_create(
            self.repo,
            b"test-tag",
            author="Test Author <author@example.com>",
            annotated=True,
            message=b"Test message",
        )

        # Verify tag was created
        tag_ref = self.repo.refs[b"refs/tags/test-tag"]
        tag_obj = self.repo[tag_ref]
        self.assertEqual(b"Test Author <author@example.com>", tag_obj.tagger)

    def test_tag_create_with_bytes_author(self) -> None:
        """Test tag_create with bytes author."""
        porcelain.tag_create(
            self.repo,
            b"test-tag",
            author=b"Test Author <author@example.com>",
            annotated=True,
            message=b"Test message",
        )

        # Verify tag was created
        tag_ref = self.repo.refs[b"refs/tags/test-tag"]
        tag_obj = self.repo[tag_ref]
        self.assertEqual(b"Test Author <author@example.com>", tag_obj.tagger)

    def test_tag_create_with_string_message(self) -> None:
        """Test tag_create with string message gets encoded."""
        porcelain.tag_create(
            self.repo,
            b"test-tag",
            author=b"Test <test@example.com>",
            annotated=True,
            message="This is a test message",
        )

        # Verify tag was created with message
        tag_ref = self.repo.refs[b"refs/tags/test-tag"]
        tag_obj = self.repo[tag_ref]
        self.assertEqual(b"This is a test message\n", tag_obj.message)

    def test_tag_create_with_bytes_message(self) -> None:
        """Test tag_create with bytes message."""
        porcelain.tag_create(
            self.repo,
            b"test-tag",
            author=b"Test <test@example.com>",
            annotated=True,
            message=b"This is a test message",
        )

        # Verify tag was created with message
        tag_ref = self.repo.refs[b"refs/tags/test-tag"]
        tag_obj = self.repo[tag_ref]
        self.assertEqual(b"This is a test message\n", tag_obj.message)

    def test_tag_create_with_none_message(self) -> None:
        """Test tag_create with None message defaults to empty."""
        porcelain.tag_create(
            self.repo,
            b"test-tag",
            author=b"Test <test@example.com>",
            annotated=True,
            message=None,
        )

        # Verify tag was created with empty message
        tag_ref = self.repo.refs[b"refs/tags/test-tag"]
        tag_obj = self.repo[tag_ref]
        self.assertEqual(b"\n", tag_obj.message)

    def test_tag_create_with_explicit_tag_time(self) -> None:
        """Test tag_create with explicit tag_time."""
        tag_time = 1700000000
        porcelain.tag_create(
            self.repo,
            b"test-tag",
            author=b"Test <test@example.com>",
            annotated=True,
            message=b"Test",
            tag_time=tag_time,
        )

        # Verify tag time is set correctly
        tag_ref = self.repo.refs[b"refs/tags/test-tag"]
        tag_obj = self.repo[tag_ref]
        self.assertEqual(tag_time, tag_obj.tag_time)

    def test_tag_create_with_string_timezone(self) -> None:
        """Test tag_create with string timezone."""
        porcelain.tag_create(
            self.repo,
            b"test-tag",
            author=b"Test <test@example.com>",
            annotated=True,
            message=b"Test",
            tag_timezone="+0200",
        )

        # Verify tag was created with timezone
        tag_ref = self.repo.refs[b"refs/tags/test-tag"]
        tag_obj = self.repo[tag_ref]
        # +0200 is 2 * 60 * 60 = 7200 seconds
        self.assertEqual(7200, tag_obj.tag_timezone)

    def test_tag_create_with_custom_encoding(self) -> None:
        """Test tag_create with custom encoding."""
        porcelain.tag_create(
            self.repo,
            "test-tag",
            author="Test <test@example.com>",
            annotated=True,
            message="Test message",
            encoding="utf-8",
        )

        # Verify tag was created
        self.assertIn(b"refs/tags/test-tag", self.repo.refs)
