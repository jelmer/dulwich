# test_annotate.py -- tests for porcelain annotate
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for porcelain annotate and blame functions."""

import os
import tempfile
from unittest import TestCase

from dulwich.objects import Blob, Commit, Tree
from dulwich.porcelain import annotate, blame
from dulwich.repo import Repo


class PorcelainAnnotateTestCase(TestCase):
    """Tests for the porcelain annotate function."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.temp_dir)

    def tearDown(self) -> None:
        self.repo.close()
        import shutil

        shutil.rmtree(self.temp_dir)

    def _make_commit_with_file(
        self,
        filename: str,
        content: bytes,
        message: str,
        parent: bytes | None = None,
    ) -> bytes:
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

    def test_porcelain_annotate(self) -> None:
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

    def test_porcelain_annotate_with_committish(self) -> None:
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

    def test_blame_alias(self) -> None:
        """Test that blame is an alias for annotate."""
        self.assertIs(blame, annotate)


class IntegrationTestCase(TestCase):
    """Integration tests with more complex scenarios."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.temp_dir)

    def tearDown(self) -> None:
        self.repo.close()
        import shutil

        shutil.rmtree(self.temp_dir)

    def _create_file_commit(
        self,
        filename: str,
        content: bytes,
        message: str,
        parent: bytes | None = None,
    ) -> bytes:
        """Helper to create a commit with file content."""
        # Write file to working directory
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, "wb") as f:
            f.write(content)

        # Stage file
        self.repo.get_worktree().stage([filename.encode()])

        # Create commit
        commit_id = self.repo.get_worktree().commit(
            message=message.encode(),
            committer=b"Test Committer <test@example.com>",
            author=b"Test Author <test@example.com>",
            commit_timestamp=1000000000,
            commit_timezone=0,
            author_timestamp=1000000000,
            author_timezone=0,
        )

        return commit_id

    def test_complex_file_history(self) -> None:
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
