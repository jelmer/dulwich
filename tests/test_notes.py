# test_notes.py -- Tests for Git notes functionality
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

"""Tests for Git notes."""

import stat
from unittest import TestCase

from dulwich.notes import (
    DEFAULT_NOTES_REF,
    Notes,
    NotesTree,
    create_notes_tree,
    get_note_path,
    split_path_for_fanout,
)
from dulwich.object_store import MemoryObjectStore
from dulwich.objects import Blob, Commit, Tree
from dulwich.refs import DictRefsContainer


class TestNotesHelpers(TestCase):
    """Test helper functions for notes."""

    def test_split_path_for_fanout_no_fanout(self):
        """Test splitting path with no fanout."""
        hexsha = b"1234567890abcdef1234567890abcdef12345678"
        result = split_path_for_fanout(hexsha, 0)
        self.assertEqual((hexsha,), result)

    def test_split_path_for_fanout_level_1(self):
        """Test splitting path with fanout level 1."""
        hexsha = b"1234567890abcdef1234567890abcdef12345678"
        result = split_path_for_fanout(hexsha, 1)
        self.assertEqual((b"12", b"34567890abcdef1234567890abcdef12345678"), result)

    def test_split_path_for_fanout_level_2(self):
        """Test splitting path with fanout level 2."""
        hexsha = b"1234567890abcdef1234567890abcdef12345678"
        result = split_path_for_fanout(hexsha, 2)
        self.assertEqual(
            (b"12", b"34", b"567890abcdef1234567890abcdef12345678"), result
        )

    def test_get_note_path_no_fanout(self):
        """Test getting note path with no fanout."""
        sha = b"1234567890abcdef1234567890abcdef12345678"
        path = get_note_path(sha, 0)
        self.assertEqual(b"1234567890abcdef1234567890abcdef12345678", path)

    def test_get_note_path_with_fanout(self):
        """Test getting note path with fanout."""
        sha = b"1234567890abcdef1234567890abcdef12345678"
        path = get_note_path(sha, 2)
        self.assertEqual(b"12/34/567890abcdef1234567890abcdef12345678", path)


class TestNotesTree(TestCase):
    """Test NotesTree class."""

    def setUp(self):
        self.store = MemoryObjectStore()
        self.tree = Tree()
        self.store.add_object(self.tree)

    def test_create_notes_tree(self):
        """Test creating an empty notes tree."""
        tree = create_notes_tree(self.store)
        self.assertIsInstance(tree, Tree)
        self.assertEqual(0, len(tree))
        self.assertIn(tree.id, self.store)

    def test_get_note_not_found(self):
        """Test getting a note that doesn't exist."""
        notes_tree = NotesTree(self.tree, self.store)
        sha = b"1234567890abcdef1234567890abcdef12345678"
        self.assertIsNone(notes_tree.get_note(sha))

    def test_set_and_get_note(self):
        """Test setting and getting a note."""
        notes_tree = NotesTree(self.tree, self.store)
        sha = b"1234567890abcdef1234567890abcdef12345678"
        note_content = b"This is a test note"

        new_tree = notes_tree.set_note(sha, note_content)
        self.assertIsInstance(new_tree, Tree)
        self.assertIn(new_tree.id, self.store)

        # Create new NotesTree with updated tree
        notes_tree = NotesTree(new_tree, self.store)
        retrieved_note = notes_tree.get_note(sha)
        self.assertEqual(note_content, retrieved_note)

    def test_remove_note(self):
        """Test removing a note."""
        notes_tree = NotesTree(self.tree, self.store)
        sha = b"1234567890abcdef1234567890abcdef12345678"
        note_content = b"This is a test note"

        # First add a note
        new_tree = notes_tree.set_note(sha, note_content)
        notes_tree = NotesTree(new_tree, self.store)

        # Then remove it
        new_tree = notes_tree.remove_note(sha)
        self.assertIsNotNone(new_tree)

        # Verify it's gone
        notes_tree = NotesTree(new_tree, self.store)
        self.assertIsNone(notes_tree.get_note(sha))

    def test_remove_nonexistent_note(self):
        """Test removing a note that doesn't exist."""
        notes_tree = NotesTree(self.tree, self.store)
        sha = b"1234567890abcdef1234567890abcdef12345678"

        result = notes_tree.remove_note(sha)
        self.assertIsNone(result)

    def test_list_notes_empty(self):
        """Test listing notes from empty tree."""
        notes_tree = NotesTree(self.tree, self.store)
        notes = list(notes_tree.list_notes())
        self.assertEqual([], notes)

    def test_list_notes(self):
        """Test listing notes."""
        notes_tree = NotesTree(self.tree, self.store)

        # Add multiple notes
        sha1 = b"1234567890abcdef1234567890abcdef12345678"
        sha2 = b"abcdef1234567890abcdef1234567890abcdef12"

        new_tree = notes_tree.set_note(sha1, b"Note 1")
        notes_tree = NotesTree(new_tree, self.store)
        new_tree = notes_tree.set_note(sha2, b"Note 2")
        notes_tree = NotesTree(new_tree, self.store)

        # List notes
        notes = list(notes_tree.list_notes())
        self.assertEqual(2, len(notes))

        # Sort by SHA for consistent comparison
        notes.sort(key=lambda x: x[0])
        self.assertEqual(sha1, notes[0][0])
        self.assertEqual(sha2, notes[1][0])

    def test_detect_fanout_level(self):
        """Test fanout level detection."""
        # Test no fanout (files at root)
        tree = Tree()
        blob = Blob.from_string(b"test note")
        self.store.add_object(blob)
        tree.add(
            b"1234567890abcdef1234567890abcdef12345678", stat.S_IFREG | 0o644, blob.id
        )
        self.store.add_object(tree)

        notes_tree = NotesTree(tree, self.store)
        self.assertEqual(0, notes_tree._fanout_level)

        # Test level 1 fanout (2-char dirs with files)
        tree = Tree()
        subtree = Tree()
        self.store.add_object(subtree)
        subtree.add(
            b"34567890abcdef1234567890abcdef12345678", stat.S_IFREG | 0o644, blob.id
        )
        tree.add(b"12", stat.S_IFDIR, subtree.id)
        tree.add(b"ab", stat.S_IFDIR, subtree.id)
        self.store.add_object(tree)

        notes_tree = NotesTree(tree, self.store)
        self.assertEqual(1, notes_tree._fanout_level)

        # Test level 2 fanout (2-char dirs containing 2-char dirs)
        tree = Tree()
        subtree1 = Tree()
        subtree2 = Tree()
        self.store.add_object(subtree2)
        subtree2.add(
            b"567890abcdef1234567890abcdef12345678", stat.S_IFREG | 0o644, blob.id
        )
        subtree1.add(b"34", stat.S_IFDIR, subtree2.id)
        self.store.add_object(subtree1)
        tree.add(b"12", stat.S_IFDIR, subtree1.id)
        self.store.add_object(tree)

        notes_tree = NotesTree(tree, self.store)
        self.assertEqual(2, notes_tree._fanout_level)

    def test_automatic_fanout_reorganization(self):
        """Test that tree automatically reorganizes when crossing fanout thresholds."""
        notes_tree = NotesTree(self.tree, self.store)

        # Add notes until we cross the fanout threshold
        # We need to add enough notes to trigger fanout (256+)
        for i in range(260):
            # Generate unique SHA for each note
            sha = f"{i:040x}".encode("ascii")
            note_content = f"Note {i}".encode("ascii")
            new_tree = notes_tree.set_note(sha, note_content)
            notes_tree = NotesTree(new_tree, self.store)

        # Should now have fanout level 1
        self.assertEqual(1, notes_tree._fanout_level)

        # Verify all notes are still accessible
        for i in range(260):
            sha = f"{i:040x}".encode("ascii")
            note = notes_tree.get_note(sha)
            self.assertEqual(f"Note {i}".encode("ascii"), note)


class TestNotes(TestCase):
    """Test Notes high-level interface."""

    def setUp(self):
        self.store = MemoryObjectStore()
        self.refs = DictRefsContainer({})

    def test_get_notes_ref_default(self):
        """Test getting default notes ref."""
        notes = Notes(self.store, self.refs)
        ref = notes.get_notes_ref()
        self.assertEqual(DEFAULT_NOTES_REF, ref)

    def test_get_notes_ref_custom(self):
        """Test getting custom notes ref."""
        notes = Notes(self.store, self.refs)
        ref = notes.get_notes_ref(b"refs/notes/custom")
        self.assertEqual(b"refs/notes/custom", ref)

    def test_get_note_no_ref(self):
        """Test getting note when ref doesn't exist."""
        notes = Notes(self.store, self.refs)
        sha = b"1234567890abcdef1234567890abcdef12345678"
        self.assertIsNone(notes.get_note(sha))

    def test_set_and_get_note(self):
        """Test setting and getting a note through Notes interface."""
        notes = Notes(self.store, self.refs)
        sha = b"1234567890abcdef1234567890abcdef12345678"
        note_content = b"Test note content"

        # Set note
        commit_sha = notes.set_note(sha, note_content)
        self.assertIsInstance(commit_sha, bytes)
        self.assertIn(commit_sha, self.store)

        # Verify commit
        commit = self.store[commit_sha]
        self.assertIsInstance(commit, Commit)
        self.assertEqual(b"Notes added by 'git notes add'", commit.message)

        # Get note
        retrieved_note = notes.get_note(sha)
        self.assertEqual(note_content, retrieved_note)

    def test_remove_note(self):
        """Test removing a note through Notes interface."""
        notes = Notes(self.store, self.refs)
        sha = b"1234567890abcdef1234567890abcdef12345678"
        note_content = b"Test note content"

        # First set a note
        notes.set_note(sha, note_content)

        # Then remove it
        commit_sha = notes.remove_note(sha)
        self.assertIsNotNone(commit_sha)

        # Verify it's gone
        self.assertIsNone(notes.get_note(sha))

    def test_list_notes(self):
        """Test listing notes through Notes interface."""
        notes = Notes(self.store, self.refs)

        # Add multiple notes
        sha1 = b"1234567890abcdef1234567890abcdef12345678"
        sha2 = b"abcdef1234567890abcdef1234567890abcdef12"

        notes.set_note(sha1, b"Note 1")
        notes.set_note(sha2, b"Note 2")

        # List notes
        notes_list = notes.list_notes()
        self.assertEqual(2, len(notes_list))

        # Sort for consistent comparison
        notes_list.sort(key=lambda x: x[0])
        self.assertEqual(sha1, notes_list[0][0])
        self.assertEqual(b"Note 1", notes_list[0][1])
        self.assertEqual(sha2, notes_list[1][0])
        self.assertEqual(b"Note 2", notes_list[1][1])

    def test_custom_commit_info(self):
        """Test setting note with custom commit info."""
        notes = Notes(self.store, self.refs)
        sha = b"1234567890abcdef1234567890abcdef12345678"

        commit_sha = notes.set_note(
            sha,
            b"Test note",
            author=b"Test Author <test@example.com>",
            committer=b"Test Committer <committer@example.com>",
            message=b"Custom commit message",
        )

        commit = self.store[commit_sha]
        self.assertEqual(b"Test Author <test@example.com>", commit.author)
        self.assertEqual(b"Test Committer <committer@example.com>", commit.committer)
        self.assertEqual(b"Custom commit message", commit.message)
