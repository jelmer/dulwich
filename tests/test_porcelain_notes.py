# test_porcelain_notes.py -- Tests for porcelain notes functions
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for porcelain notes functions."""

import os
import tempfile
from unittest import TestCase

from dulwich import porcelain
from dulwich.repo import Repo


class TestPorcelainNotes(TestCase):
    """Test porcelain notes functions."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.cleanup)

        # Create a test commit to annotate using porcelain
        with open(os.path.join(self.test_dir, "test.txt"), "wb") as f:
            f.write(b"Test content")

        porcelain.add(self.test_dir, ["test.txt"])
        self.test_commit_id = porcelain.commit(
            self.test_dir,
            message=b"Test commit",
            author=b"Test User <test@example.com>",
            committer=b"Test User <test@example.com>",
        )

    def cleanup(self):
        import shutil

        shutil.rmtree(self.test_dir)

    def test_notes_add_and_show(self):
        """Test adding and showing a note."""
        # Add a note
        note_commit = porcelain.notes_add(
            self.test_dir, self.test_commit_id, "This is a test note"
        )
        self.assertIsNotNone(note_commit)

        # Show the note
        note = porcelain.notes_show(self.test_dir, self.test_commit_id)
        self.assertEqual(b"This is a test note", note)

    def test_notes_add_bytes(self):
        """Test adding a note with bytes."""
        # Add a note with bytes
        note_commit = porcelain.notes_add(
            self.test_dir, self.test_commit_id, b"This is a byte note"
        )
        self.assertIsNotNone(note_commit)

        # Show the note
        note = porcelain.notes_show(self.test_dir, self.test_commit_id)
        self.assertEqual(b"This is a byte note", note)

    def test_notes_show_nonexistent(self):
        """Test showing a note that doesn't exist."""
        note = porcelain.notes_show(self.test_dir, self.test_commit_id)
        self.assertIsNone(note)

    def test_notes_remove(self):
        """Test removing a note."""
        # First add a note
        porcelain.notes_add(self.test_dir, self.test_commit_id, "Test note to remove")

        # Then remove it
        result = porcelain.notes_remove(self.test_dir, self.test_commit_id)
        self.assertIsNotNone(result)

        # Verify it's gone
        note = porcelain.notes_show(self.test_dir, self.test_commit_id)
        self.assertIsNone(note)

    def test_notes_remove_nonexistent(self):
        """Test removing a note that doesn't exist."""
        result = porcelain.notes_remove(self.test_dir, self.test_commit_id)
        self.assertIsNone(result)

    def test_notes_list_empty(self):
        """Test listing notes when there are none."""
        notes = porcelain.notes_list(self.test_dir)
        self.assertEqual([], notes)

    def test_notes_list(self):
        """Test listing notes."""
        # Create another commit to test multiple notes
        with open(os.path.join(self.test_dir, "test2.txt"), "wb") as f:
            f.write(b"Test content 2")

        porcelain.add(self.test_dir, ["test2.txt"])
        commit2_id = porcelain.commit(
            self.test_dir,
            message=b"Test commit 2",
            author=b"Test User <test@example.com>",
            committer=b"Test User <test@example.com>",
        )

        porcelain.notes_add(self.test_dir, self.test_commit_id, "Note 1")
        porcelain.notes_add(self.test_dir, commit2_id, "Note 2")

        # List notes
        notes = porcelain.notes_list(self.test_dir)
        self.assertEqual(2, len(notes))

        # Check content
        notes_dict = dict(notes)
        self.assertEqual(b"Note 1", notes_dict[self.test_commit_id])
        self.assertEqual(b"Note 2", notes_dict[commit2_id])

    def test_notes_custom_ref(self):
        """Test using a custom notes ref."""
        # Add note to custom ref
        porcelain.notes_add(
            self.test_dir, self.test_commit_id, "Custom ref note", ref="custom"
        )

        # Show from default ref (should not exist)
        note = porcelain.notes_show(self.test_dir, self.test_commit_id)
        self.assertIsNone(note)

        # Show from custom ref
        note = porcelain.notes_show(self.test_dir, self.test_commit_id, ref="custom")
        self.assertEqual(b"Custom ref note", note)

    def test_notes_update(self):
        """Test updating an existing note."""
        # Add initial note
        porcelain.notes_add(self.test_dir, self.test_commit_id, "Initial note")

        # Update the note
        porcelain.notes_add(self.test_dir, self.test_commit_id, "Updated note")

        # Verify update
        note = porcelain.notes_show(self.test_dir, self.test_commit_id)
        self.assertEqual(b"Updated note", note)

    def test_notes_custom_author_committer(self):
        """Test adding note with custom author and committer."""
        note_commit_id = porcelain.notes_add(
            self.test_dir,
            self.test_commit_id,
            "Test note",
            author=b"Custom Author <author@example.com>",
            committer=b"Custom Committer <committer@example.com>",
        )

        # Check the commit
        commit = self.repo[note_commit_id]
        self.assertEqual(b"Custom Author <author@example.com>", commit.author)
        self.assertEqual(b"Custom Committer <committer@example.com>", commit.committer)
