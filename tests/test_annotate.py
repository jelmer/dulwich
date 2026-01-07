# test_annotate.py -- tests for annotate
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for annotate support."""

import tempfile
from typing import Any
from unittest import TestCase

from dulwich.annotate import annotate_lines, update_lines
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo


class UpdateLinesTestCase(TestCase):
    """Tests for update_lines function."""

    def test_update_lines_equal(self) -> None:
        """Test update_lines when all lines are equal."""
        old_lines: list[tuple[tuple[Any, Any], bytes]] = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line2"),
        ]
        new_blob = b"line1\nline2"
        new_history_data = ("commit3", "entry3")

        result = update_lines(old_lines, new_history_data, new_blob)  # type: ignore[arg-type]
        self.assertEqual(old_lines, result)

    def test_update_lines_insert(self) -> None:
        """Test update_lines when new lines are inserted."""
        old_lines: list[tuple[tuple[Any, Any], bytes]] = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line3"),
        ]
        new_blob = b"line1\nline2\nline3"
        new_history_data = ("commit3", "entry3")

        result = update_lines(old_lines, new_history_data, new_blob)  # type: ignore[arg-type]
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit3", "entry3"), b"line2"),
            (("commit2", "entry2"), b"line3"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_delete(self) -> None:
        """Test update_lines when lines are deleted."""
        old_lines: list[tuple[tuple[Any, Any], bytes]] = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line2"),
            (("commit3", "entry3"), b"line3"),
        ]
        new_blob = b"line1\nline3"
        new_history_data = ("commit4", "entry4")

        result = update_lines(old_lines, new_history_data, new_blob)  # type: ignore[arg-type]
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit3", "entry3"), b"line3"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_replace(self) -> None:
        """Test update_lines when lines are replaced."""
        old_lines: list[tuple[tuple[Any, Any], bytes]] = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line2"),
        ]
        new_blob = b"line1\nline2_modified"
        new_history_data = ("commit3", "entry3")

        result = update_lines(old_lines, new_history_data, new_blob)  # type: ignore[arg-type]
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit3", "entry3"), b"line2_modified"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_empty_old(self) -> None:
        """Test update_lines with empty old lines."""
        old_lines: list[tuple[tuple[Any, Any], bytes]] = []
        new_blob = b"line1\nline2"
        new_history_data = ("commit1", "entry1")

        result = update_lines(old_lines, new_history_data, new_blob)  # type: ignore[arg-type]
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit1", "entry1"), b"line2"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_empty_new(self) -> None:
        """Test update_lines with empty new blob."""
        old_lines: list[tuple[tuple[Any, Any], bytes]] = [
            (("commit1", "entry1"), b"line1")
        ]
        new_blob = b""
        new_history_data = ("commit2", "entry2")

        result = update_lines(old_lines, new_history_data, new_blob)  # type: ignore[arg-type]
        self.assertEqual([], result)


class AnnotateLinesTestCase(TestCase):
    """Tests for annotate_lines function."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.temp_dir)

    def tearDown(self) -> None:
        self.repo.close()
        import shutil

        shutil.rmtree(self.temp_dir)

    def _make_commit(
        self, blob_content: bytes, message: str, parent: bytes | None = None
    ) -> bytes:
        """Helper to create a commit with a single file."""
        # Create blob
        blob = Blob()
        blob.data = blob_content
        self.repo.object_store.add_object(blob)

        # Create tree
        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        # Create commit
        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test Author <test@example.com>"
        commit.author_time = commit.commit_time = 1000000000
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = message.encode("utf-8")

        if parent:
            commit.parents = [parent]
        else:
            commit.parents = []

        self.repo.object_store.add_object(commit)
        return commit.id

    def test_annotate_lines_single_commit(self) -> None:
        """Test annotating a file with a single commit."""
        commit_id = self._make_commit(b"line1\nline2\nline3\n", "Initial commit")

        result = annotate_lines(self.repo.object_store, commit_id, b"test.txt")

        self.assertEqual(3, len(result))
        for (commit, entry), line in result:
            self.assertEqual(commit_id, commit.id)
            self.assertIn(line, [b"line1\n", b"line2\n", b"line3\n"])

    def test_annotate_lines_multiple_commits(self) -> None:
        """Test annotating a file with multiple commits."""
        # First commit
        commit1_id = self._make_commit(b"line1\nline2\n", "Initial commit")

        # Second commit - add a line
        commit2_id = self._make_commit(
            b"line1\nline1.5\nline2\n", "Add line between", parent=commit1_id
        )

        # Third commit - modify a line
        commit3_id = self._make_commit(
            b"line1_modified\nline1.5\nline2\n", "Modify first line", parent=commit2_id
        )

        result = annotate_lines(self.repo.object_store, commit3_id, b"test.txt")

        self.assertEqual(3, len(result))
        # First line should be from commit3
        self.assertEqual(commit3_id, result[0][0][0].id)
        self.assertEqual(b"line1_modified\n", result[0][1])

        # Second line should be from commit2
        self.assertEqual(commit2_id, result[1][0][0].id)
        self.assertEqual(b"line1.5\n", result[1][1])

        # Third line should be from commit1
        self.assertEqual(commit1_id, result[2][0][0].id)
        self.assertEqual(b"line2\n", result[2][1])

    def test_annotate_lines_nonexistent_path(self) -> None:
        """Test annotating a nonexistent file."""
        commit_id = self._make_commit(b"content\n", "Initial commit")

        result = annotate_lines(self.repo.object_store, commit_id, b"nonexistent.txt")
        self.assertEqual([], result)
