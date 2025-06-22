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

import os
import tempfile
import unittest
from unittest import TestCase

from dulwich.annotate import annotate_lines, update_lines
from dulwich.objects import Blob, Commit, Tree
from dulwich.porcelain import annotate, blame
from dulwich.repo import Repo


class UpdateLinesTestCase(TestCase):
    """Tests for update_lines function."""

    def test_update_lines_equal(self):
        """Test update_lines when all lines are equal."""
        old_lines = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line2"),
        ]
        new_blob = b"line1\nline2"
        new_history_data = ("commit3", "entry3")

        result = update_lines(old_lines, new_history_data, new_blob)
        self.assertEqual(old_lines, result)

    def test_update_lines_insert(self):
        """Test update_lines when new lines are inserted."""
        old_lines = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line3"),
        ]
        new_blob = b"line1\nline2\nline3"
        new_history_data = ("commit3", "entry3")

        result = update_lines(old_lines, new_history_data, new_blob)
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit3", "entry3"), b"line2"),
            (("commit2", "entry2"), b"line3"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_delete(self):
        """Test update_lines when lines are deleted."""
        old_lines = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line2"),
            (("commit3", "entry3"), b"line3"),
        ]
        new_blob = b"line1\nline3"
        new_history_data = ("commit4", "entry4")

        result = update_lines(old_lines, new_history_data, new_blob)
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit3", "entry3"), b"line3"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_replace(self):
        """Test update_lines when lines are replaced."""
        old_lines = [
            (("commit1", "entry1"), b"line1"),
            (("commit2", "entry2"), b"line2"),
        ]
        new_blob = b"line1\nline2_modified"
        new_history_data = ("commit3", "entry3")

        result = update_lines(old_lines, new_history_data, new_blob)
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit3", "entry3"), b"line2_modified"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_empty_old(self):
        """Test update_lines with empty old lines."""
        old_lines = []
        new_blob = b"line1\nline2"
        new_history_data = ("commit1", "entry1")

        result = update_lines(old_lines, new_history_data, new_blob)
        expected = [
            (("commit1", "entry1"), b"line1"),
            (("commit1", "entry1"), b"line2"),
        ]
        self.assertEqual(expected, result)

    def test_update_lines_empty_new(self):
        """Test update_lines with empty new blob."""
        old_lines = [(("commit1", "entry1"), b"line1")]
        new_blob = b""
        new_history_data = ("commit2", "entry2")

        result = update_lines(old_lines, new_history_data, new_blob)
        self.assertEqual([], result)


class AnnotateLinesTestCase(TestCase):
    """Tests for annotate_lines function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.temp_dir)

    def tearDown(self):
        self.repo.close()
        import shutil

        shutil.rmtree(self.temp_dir)

    def _make_commit(self, blob_content, message, parent=None):
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

    def test_annotate_lines_single_commit(self):
        """Test annotating a file with a single commit."""
        commit_id = self._make_commit(b"line1\nline2\nline3\n", "Initial commit")

        result = annotate_lines(self.repo.object_store, commit_id, b"test.txt")

        self.assertEqual(3, len(result))
        for (commit, entry), line in result:
            self.assertEqual(commit_id, commit.id)
            self.assertIn(line, [b"line1\n", b"line2\n", b"line3\n"])

    def test_annotate_lines_multiple_commits(self):
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

    def test_annotate_lines_nonexistent_path(self):
        """Test annotating a nonexistent file."""
        commit_id = self._make_commit(b"content\n", "Initial commit")

        result = annotate_lines(self.repo.object_store, commit_id, b"nonexistent.txt")
        self.assertEqual([], result)


class PorcelainAnnotateTestCase(TestCase):
    """Tests for the porcelain annotate function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.temp_dir)

    def tearDown(self):
        self.repo.close()
        import shutil

        shutil.rmtree(self.temp_dir)

    def _make_commit_with_file(self, filename, content, message, parent=None):
        """Helper to create a commit with a file."""
        # Create blob
        blob = Blob()
        blob.data = content
        self.repo.object_store.add_object(blob)

        # Create tree
        tree = Tree()
        tree.add(filename.encode(), 0o100644, blob.id)
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

        # Update HEAD
        self.repo.refs[b"HEAD"] = commit.id

        return commit.id

    def test_porcelain_annotate(self):
        """Test the porcelain annotate function."""
        # Create commits
        commit1_id = self._make_commit_with_file(
            "file.txt", b"line1\nline2\n", "Initial commit"
        )
        self._make_commit_with_file(
            "file.txt", b"line1\nline2\nline3\n", "Add third line", parent=commit1_id
        )

        # Test annotate
        result = list(annotate(self.temp_dir, "file.txt"))

        self.assertEqual(3, len(result))
        # Check that each result has the right structure
        for (commit, entry), line in result:
            self.assertIsNotNone(commit)
            self.assertIsNotNone(entry)
            self.assertIn(line, [b"line1\n", b"line2\n", b"line3\n"])

    def test_porcelain_annotate_with_committish(self):
        """Test porcelain annotate with specific commit."""
        # Create commits
        commit1_id = self._make_commit_with_file(
            "file.txt", b"original\n", "Initial commit"
        )
        self._make_commit_with_file(
            "file.txt", b"modified\n", "Modify file", parent=commit1_id
        )

        # Annotate at first commit
        result = list(
            annotate(self.temp_dir, "file.txt", committish=commit1_id.decode())
        )
        self.assertEqual(1, len(result))
        self.assertEqual(b"original\n", result[0][1])

        # Annotate at HEAD (second commit)
        result = list(annotate(self.temp_dir, "file.txt"))
        self.assertEqual(1, len(result))
        self.assertEqual(b"modified\n", result[0][1])

    def test_blame_alias(self):
        """Test that blame is an alias for annotate."""
        self.assertIs(blame, annotate)


class IntegrationTestCase(TestCase):
    """Integration tests with more complex scenarios."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.temp_dir)

    def tearDown(self):
        self.repo.close()
        import shutil

        shutil.rmtree(self.temp_dir)

    def _create_file_commit(self, filename, content, message, parent=None):
        """Helper to create a commit with file content."""
        # Write file to working directory
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, "wb") as f:
            f.write(content)

        # Stage file
        self.repo.stage([filename.encode()])

        # Create commit
        commit_id = self.repo.do_commit(
            message.encode(),
            committer=b"Test Committer <test@example.com>",
            author=b"Test Author <test@example.com>",
            commit_timestamp=1000000000,
            commit_timezone=0,
            author_timestamp=1000000000,
            author_timezone=0,
        )

        return commit_id

    def test_complex_file_history(self):
        """Test annotating a file with complex history."""
        # Initial commit with 3 lines
        self._create_file_commit(
            "complex.txt", b"First line\nSecond line\nThird line\n", "Initial commit"
        )

        # Add lines at the beginning and end
        self._create_file_commit(
            "complex.txt",
            b"New first line\nFirst line\nSecond line\nThird line\nNew last line\n",
            "Add lines at beginning and end",
        )

        # Modify middle line
        self._create_file_commit(
            "complex.txt",
            b"New first line\nFirst line\n"
            b"Modified second line\nThird line\n"
            b"New last line\n",
            "Modify middle line",
        )

        # Delete a line
        self._create_file_commit(
            "complex.txt",
            b"New first line\nFirst line\nModified second line\nNew last line\n",
            "Delete third line",
        )

        # Run annotate
        result = list(annotate(self.temp_dir, "complex.txt"))

        # Should have 4 lines
        self.assertEqual(4, len(result))

        # Verify each line comes from the correct commit
        lines = [line for (commit, entry), line in result]
        self.assertEqual(
            [
                b"New first line\n",
                b"First line\n",
                b"Modified second line\n",
                b"New last line\n",
            ],
            lines,
        )


if __name__ == "__main__":
    unittest.main()
