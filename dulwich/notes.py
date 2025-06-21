# notes.py -- Git notes handling
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

"""Git notes handling."""

import stat
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from .objects import Blob, Tree

if TYPE_CHECKING:
    from .config import StackedConfig

NOTES_REF_PREFIX = b"refs/notes/"
DEFAULT_NOTES_REF = NOTES_REF_PREFIX + b"commits"


def get_note_fanout_level(tree: Tree, object_store) -> int:
    """Determine the fanout level for a note tree.

    Git uses a fanout directory structure for performance with large numbers
    of notes. The fanout level determines how many levels of subdirectories
    are used.

    Args:
        tree: The notes tree to analyze
        object_store: Object store to retrieve subtrees

    Returns:
        Fanout level (0 for no fanout, 1 or 2 for fanout)
    """

    # Count the total number of notes in the tree recursively
    def count_notes(tree: Tree, level: int = 0) -> int:
        count = 0
        for name, mode, sha in tree.items():
            if stat.S_ISREG(mode):
                count += 1
            elif stat.S_ISDIR(mode) and level < 2:  # Only recurse 2 levels deep
                try:
                    subtree = object_store[sha]
                    count += count_notes(subtree, level + 1)
                except KeyError:
                    pass
        return count

    note_count = count_notes(tree)

    # Use fanout based on number of notes
    # Git typically starts using fanout around 256 notes
    if note_count < 256:
        return 0
    elif note_count < 65536:  # 256^2
        return 1
    else:
        return 2


def split_path_for_fanout(hexsha: bytes, fanout_level: int) -> tuple[bytes, ...]:
    """Split a hex SHA into path components based on fanout level.

    Args:
        hexsha: Hex SHA of the object
        fanout_level: Number of directory levels for fanout

    Returns:
        Tuple of path components
    """
    if fanout_level == 0:
        return (hexsha,)

    components = []
    for i in range(fanout_level):
        components.append(hexsha[i * 2 : (i + 1) * 2])
    components.append(hexsha[fanout_level * 2 :])
    return tuple(components)


def get_note_path(object_sha: bytes, fanout_level: int = 0) -> bytes:
    """Get the path within the notes tree for a given object.

    Args:
        object_sha: Hex SHA of the object to get notes for
        fanout_level: Fanout level to use

    Returns:
        Path within the notes tree
    """
    components = split_path_for_fanout(object_sha, fanout_level)
    return b"/".join(components)


class NotesTree:
    """Represents a Git notes tree."""

    def __init__(self, tree: Tree, object_store):
        """Initialize a notes tree.

        Args:
            tree: The tree object containing notes
            object_store: Object store to retrieve note contents from
        """
        self._tree = tree
        self._object_store = object_store
        self._fanout_level = self._detect_fanout_level()

    def _detect_fanout_level(self) -> int:
        """Detect the fanout level used in this notes tree.

        Returns:
            Detected fanout level
        """
        if not self._tree.items():
            return 0

        # Check for presence of both files and directories
        has_files = False
        has_dirs = False
        dir_names = []

        for name, mode, sha in self._tree.items():
            if stat.S_ISDIR(mode):
                has_dirs = True
                dir_names.append(name)
            elif stat.S_ISREG(mode):
                has_files = True

        # If we have files at the root level, check if they're full SHA names
        if has_files and not has_dirs:
            # Check if any file names are full 40-char hex strings
            for name, mode, sha in self._tree.items():
                if stat.S_ISREG(mode) and len(name) == 40:
                    try:
                        int(name, 16)  # Verify it's a valid hex string
                        return 0  # No fanout
                    except ValueError:
                        pass

        # Check if all directories are 2-character hex names
        if has_dirs and dir_names:
            all_two_char_hex = all(
                len(name) == 2 and all(c in b"0123456789abcdef" for c in name)
                for name in dir_names
            )

            if all_two_char_hex:
                # Check a sample directory to determine if it's level 1 or 2
                sample_dir_name = dir_names[0]
                try:
                    sample_mode, sample_sha = self._tree[sample_dir_name]
                    sample_tree = self._object_store[sample_sha]

                    # Check if this subtree also has 2-char hex directories
                    sub_has_dirs = False
                    for sub_name, sub_mode, sub_sha in sample_tree.items():
                        if stat.S_ISDIR(sub_mode) and len(sub_name) == 2:
                            try:
                                int(sub_name, 16)
                                sub_has_dirs = True
                                break
                            except ValueError:
                                pass

                    return 2 if sub_has_dirs else 1
                except KeyError:
                    return 1  # Assume level 1 if we can't inspect

        return 0

    def _reorganize_tree(self, new_fanout_level: int) -> None:
        """Reorganize the notes tree to use a different fanout level.

        Args:
            new_fanout_level: The desired fanout level
        """
        if new_fanout_level == self._fanout_level:
            return

        # Collect all existing notes
        notes = []
        for object_sha, note_sha in self.list_notes():
            note_obj = self._object_store[note_sha]
            if isinstance(note_obj, Blob):
                notes.append((object_sha, note_obj.data))

        # Create new empty tree
        new_tree = Tree()
        self._object_store.add_object(new_tree)
        self._tree = new_tree
        self._fanout_level = new_fanout_level

        # Re-add all notes with new fanout structure using set_note
        # Temporarily set fanout back to avoid recursion
        for object_sha, note_content in notes:
            # Use the internal tree update logic without checking fanout again
            note_blob = Blob.from_string(note_content)
            self._object_store.add_object(note_blob)

            path = get_note_path(object_sha, new_fanout_level)
            components = path.split(b"/")

            # Build new tree structure
            def update_tree(tree: Tree, components: list, blob_sha: bytes) -> Tree:
                if len(components) == 1:
                    # Leaf level - add the note blob
                    new_tree = Tree()
                    for name, mode, sha in tree.items():
                        if name != components[0]:
                            new_tree.add(name, mode, sha)
                    new_tree.add(components[0], stat.S_IFREG | 0o644, blob_sha)
                    return new_tree
                else:
                    # Directory level
                    new_tree = Tree()
                    found = False
                    for name, mode, sha in tree.items():
                        if name == components[0]:
                            # Update this subtree
                            if stat.S_ISDIR(mode):
                                subtree = self._object_store[sha]
                            else:
                                # If not a directory, we need to replace it
                                subtree = Tree()
                            new_subtree = update_tree(subtree, components[1:], blob_sha)
                            self._object_store.add_object(new_subtree)
                            new_tree.add(name, stat.S_IFDIR, new_subtree.id)
                            found = True
                        else:
                            new_tree.add(name, mode, sha)

                    if not found:
                        # Create new subtree path
                        subtree = Tree()
                        new_subtree = update_tree(subtree, components[1:], blob_sha)
                        self._object_store.add_object(new_subtree)
                        new_tree.add(components[0], stat.S_IFDIR, new_subtree.id)

                    return new_tree

            self._tree = update_tree(self._tree, components, note_blob.id)
            self._object_store.add_object(self._tree)

    def _update_tree_entry(
        self, tree: Tree, name: bytes, mode: int, sha: bytes
    ) -> Tree:
        """Update a tree entry and return the updated tree.

        Args:
            tree: The tree to update
            name: Name of the entry
            mode: File mode
            sha: SHA of the object

        Returns:
            The updated tree
        """
        new_tree = Tree()
        for existing_name, existing_mode, existing_sha in tree.items():
            if existing_name != name:
                new_tree.add(existing_name, existing_mode, existing_sha)
        new_tree.add(name, mode, sha)
        self._object_store.add_object(new_tree)

        # Update the tree reference
        if tree is self._tree:
            self._tree = new_tree

        return new_tree

    def _get_note_sha(self, object_sha: bytes) -> Optional[bytes]:
        """Get the SHA of the note blob for an object.

        Args:
            object_sha: SHA of the object to get notes for

        Returns:
            SHA of the note blob, or None if no note exists
        """
        path = get_note_path(object_sha, self._fanout_level)
        components = path.split(b"/")

        current_tree = self._tree
        for component in components[:-1]:
            try:
                mode, sha = current_tree[component]
                if not stat.S_ISDIR(mode):  # Not a directory
                    return None
                current_tree = self._object_store[sha]
            except KeyError:
                return None

        try:
            mode, sha = current_tree[components[-1]]
            if not stat.S_ISREG(mode):  # Not a regular file
                return None
            return sha
        except KeyError:
            return None

    def get_note(self, object_sha: bytes) -> Optional[bytes]:
        """Get the note content for an object.

        Args:
            object_sha: SHA of the object to get notes for

        Returns:
            Note content as bytes, or None if no note exists
        """
        note_sha = self._get_note_sha(object_sha)
        if note_sha is None:
            return None

        try:
            note_obj = self._object_store[note_sha]
            if not isinstance(note_obj, Blob):
                return None
            return note_obj.data
        except KeyError:
            return None

    def set_note(self, object_sha: bytes, note_content: bytes) -> Tree:
        """Set or update a note for an object.

        Args:
            object_sha: SHA of the object to annotate
            note_content: Content of the note

        Returns:
            New tree object with the note added/updated
        """
        # Create note blob
        note_blob = Blob.from_string(note_content)
        self._object_store.add_object(note_blob)

        # Check if we need to reorganize the tree for better fanout
        desired_fanout = get_note_fanout_level(self._tree, self._object_store)
        if desired_fanout != self._fanout_level:
            self._reorganize_tree(desired_fanout)

        # Get path components
        path = get_note_path(object_sha, self._fanout_level)
        components = path.split(b"/")

        # Build new tree structure
        def update_tree(tree: Tree, components: list, blob_sha: bytes) -> Tree:
            if len(components) == 1:
                # Leaf level - add the note blob
                new_tree = Tree()
                for name, mode, sha in tree.items():
                    if name != components[0]:
                        new_tree.add(name, mode, sha)
                new_tree.add(components[0], stat.S_IFREG | 0o644, blob_sha)
                return new_tree
            else:
                # Directory level
                new_tree = Tree()
                found = False
                for name, mode, sha in tree.items():
                    if name == components[0]:
                        # Update this subtree
                        if stat.S_ISDIR(mode):
                            subtree = self._object_store[sha]
                        else:
                            # If not a directory, we need to replace it
                            subtree = Tree()
                        new_subtree = update_tree(subtree, components[1:], blob_sha)
                        self._object_store.add_object(new_subtree)
                        new_tree.add(name, stat.S_IFDIR, new_subtree.id)
                        found = True
                    else:
                        new_tree.add(name, mode, sha)

                if not found:
                    # Create new subtree path
                    subtree = Tree()
                    new_subtree = update_tree(subtree, components[1:], blob_sha)
                    self._object_store.add_object(new_subtree)
                    new_tree.add(components[0], stat.S_IFDIR, new_subtree.id)

                return new_tree

        new_tree = update_tree(self._tree, components, note_blob.id)
        self._object_store.add_object(new_tree)
        self._tree = new_tree
        self._fanout_level = self._detect_fanout_level()
        return new_tree

    def remove_note(self, object_sha: bytes) -> Optional[Tree]:
        """Remove a note for an object.

        Args:
            object_sha: SHA of the object to remove notes from

        Returns:
            New tree object with the note removed, or None if no note existed
        """
        if self._get_note_sha(object_sha) is None:
            return None

        # Get path components
        path = get_note_path(object_sha, self._fanout_level)
        components = path.split(b"/")

        # Build new tree structure without the note
        def remove_from_tree(tree: Tree, components: list) -> Optional[Tree]:
            if len(components) == 1:
                # Leaf level - remove the note
                new_tree = Tree()
                found = False
                for name, mode, sha in tree.items():
                    if name != components[0]:
                        new_tree.add(name, mode, sha)
                    else:
                        found = True

                if not found:
                    return None

                # Return None if tree is now empty
                return new_tree if len(new_tree) > 0 else None
            else:
                # Directory level
                new_tree = Tree()
                modified = False
                for name, mode, sha in tree.items():
                    if name == components[0] and stat.S_ISDIR(mode):
                        # Update this subtree
                        subtree = self._object_store[sha]
                        new_subtree = remove_from_tree(subtree, components[1:])
                        if new_subtree is not None:
                            self._object_store.add_object(new_subtree)
                            new_tree.add(name, stat.S_IFDIR, new_subtree.id)
                        modified = True
                    else:
                        new_tree.add(name, mode, sha)

                if not modified:
                    return None

                # Return None if tree is now empty
                return new_tree if len(new_tree) > 0 else None

        new_tree = remove_from_tree(self._tree, components)
        if new_tree is None:
            new_tree = Tree()  # Empty tree

        self._object_store.add_object(new_tree)
        self._tree = new_tree
        self._fanout_level = self._detect_fanout_level()
        return new_tree

    def list_notes(self) -> Iterator[tuple[bytes, bytes]]:
        """List all notes in this tree.

        Yields:
            Tuples of (object_sha, note_sha)
        """

        def walk_tree(tree: Tree, prefix: bytes = b"") -> Iterator[tuple[bytes, bytes]]:
            for name, mode, sha in tree.items():
                if stat.S_ISDIR(mode):  # Directory
                    subtree = self._object_store[sha]
                    yield from walk_tree(subtree, prefix + name)
                elif stat.S_ISREG(mode):  # File
                    # Reconstruct the full hex SHA from the path
                    full_hex = prefix + name
                    yield (full_hex, sha)

        yield from walk_tree(self._tree)


def create_notes_tree(object_store) -> Tree:
    """Create an empty notes tree.

    Args:
        object_store: Object store to add the tree to

    Returns:
        Empty tree object
    """
    tree = Tree()
    object_store.add_object(tree)
    return tree


class Notes:
    """High-level interface for Git notes operations."""

    def __init__(self, object_store, refs_container):
        """Initialize Notes.

        Args:
            object_store: Object store to read/write objects
            refs_container: Refs container to read/write refs
        """
        self._object_store = object_store
        self._refs = refs_container

    def get_notes_ref(
        self,
        notes_ref: Optional[bytes] = None,
        config: Optional["StackedConfig"] = None,
    ) -> bytes:
        """Get the notes reference to use.

        Args:
            notes_ref: The notes ref to use, or None to use the default
            config: Config to read notes.displayRef from

        Returns:
            The notes reference name
        """
        if notes_ref is None:
            if config is not None:
                notes_ref = config.get((b"notes",), b"displayRef")
            if notes_ref is None:
                notes_ref = DEFAULT_NOTES_REF
        return notes_ref

    def get_note(
        self,
        object_sha: bytes,
        notes_ref: Optional[bytes] = None,
        config: Optional["StackedConfig"] = None,
    ) -> Optional[bytes]:
        """Get the note for an object.

        Args:
            object_sha: SHA of the object to get notes for
            notes_ref: The notes ref to use, or None to use the default
            config: Config to read notes.displayRef from

        Returns:
            The note content as bytes, or None if no note exists
        """
        notes_ref = self.get_notes_ref(notes_ref, config)
        try:
            notes_commit_sha = self._refs[notes_ref]
        except KeyError:
            return None

        # Get the commit object
        notes_obj = self._object_store[notes_commit_sha]

        # If it's a commit, get the tree from it
        from .objects import Commit

        if isinstance(notes_obj, Commit):
            notes_tree = self._object_store[notes_obj.tree]
        else:
            # If it's directly a tree (shouldn't happen in normal usage)
            notes_tree = notes_obj

        if not isinstance(notes_tree, Tree):
            return None

        notes_tree_obj = NotesTree(notes_tree, self._object_store)
        return notes_tree_obj.get_note(object_sha)

    def set_note(
        self,
        object_sha: bytes,
        note_content: bytes,
        notes_ref: Optional[bytes] = None,
        author: Optional[bytes] = None,
        committer: Optional[bytes] = None,
        message: Optional[bytes] = None,
        config: Optional["StackedConfig"] = None,
    ) -> bytes:
        """Set or update a note for an object.

        Args:
            object_sha: SHA of the object to annotate
            note_content: Content of the note
            notes_ref: The notes ref to use, or None to use the default
            author: Author identity (defaults to committer)
            committer: Committer identity (defaults to config)
            message: Commit message for the notes update
            config: Config to read user identity and notes.displayRef from

        Returns:
            SHA of the new notes commit
        """
        import time

        from .objects import Commit
        from .repo import get_user_identity

        notes_ref = self.get_notes_ref(notes_ref, config)

        # Get current notes tree
        try:
            notes_commit_sha = self._refs[notes_ref]
            notes_obj = self._object_store[notes_commit_sha]

            # If it's a commit, get the tree from it
            if isinstance(notes_obj, Commit):
                notes_tree = self._object_store[notes_obj.tree]
            else:
                # If it's directly a tree (shouldn't happen in normal usage)
                notes_tree = notes_obj

            if not isinstance(notes_tree, Tree):
                notes_tree = create_notes_tree(self._object_store)
        except KeyError:
            notes_tree = create_notes_tree(self._object_store)

        # Update notes tree
        notes_tree_obj = NotesTree(notes_tree, self._object_store)
        new_tree = notes_tree_obj.set_note(object_sha, note_content)

        # Create commit
        if committer is None and config is not None:
            committer = get_user_identity(config, kind="COMMITTER")
        if committer is None:
            committer = b"Git User <user@example.com>"
        if author is None:
            author = committer
        if message is None:
            message = b"Notes added by 'git notes add'"

        commit = Commit()
        commit.tree = new_tree.id
        commit.author = author
        commit.committer = committer
        commit.commit_time = commit.author_time = int(time.time())
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = message

        # Set parent to previous notes commit if exists
        try:
            parent_sha = self._refs[notes_ref]
            parent = self._object_store[parent_sha]
            if isinstance(parent, Commit):
                commit.parents = [parent_sha]
        except KeyError:
            commit.parents = []

        self._object_store.add_object(commit)
        self._refs[notes_ref] = commit.id

        return commit.id

    def remove_note(
        self,
        object_sha: bytes,
        notes_ref: Optional[bytes] = None,
        author: Optional[bytes] = None,
        committer: Optional[bytes] = None,
        message: Optional[bytes] = None,
        config: Optional["StackedConfig"] = None,
    ) -> Optional[bytes]:
        """Remove a note for an object.

        Args:
            object_sha: SHA of the object to remove notes from
            notes_ref: The notes ref to use, or None to use the default
            author: Author identity (defaults to committer)
            committer: Committer identity (defaults to config)
            message: Commit message for the notes removal
            config: Config to read user identity and notes.displayRef from

        Returns:
            SHA of the new notes commit, or None if no note existed
        """
        import time

        from .objects import Commit
        from .repo import get_user_identity

        notes_ref = self.get_notes_ref(notes_ref, config)

        # Get current notes tree
        try:
            notes_commit_sha = self._refs[notes_ref]
            notes_obj = self._object_store[notes_commit_sha]

            # If it's a commit, get the tree from it
            if isinstance(notes_obj, Commit):
                notes_tree = self._object_store[notes_obj.tree]
            else:
                # If it's directly a tree (shouldn't happen in normal usage)
                notes_tree = notes_obj

            if not isinstance(notes_tree, Tree):
                return None
        except KeyError:
            return None

        # Remove from notes tree
        notes_tree_obj = NotesTree(notes_tree, self._object_store)
        new_tree = notes_tree_obj.remove_note(object_sha)
        if new_tree is None:
            return None

        # Create commit
        if committer is None and config is not None:
            committer = get_user_identity(config, kind="COMMITTER")
        if committer is None:
            committer = b"Git User <user@example.com>"
        if author is None:
            author = committer
        if message is None:
            message = b"Notes removed by 'git notes remove'"

        commit = Commit()
        commit.tree = new_tree.id
        commit.author = author
        commit.committer = committer
        commit.commit_time = commit.author_time = int(time.time())
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = message

        # Set parent to previous notes commit
        parent_sha = self._refs[notes_ref]
        parent = self._object_store[parent_sha]
        if isinstance(parent, Commit):
            commit.parents = [parent_sha]

        self._object_store.add_object(commit)
        self._refs[notes_ref] = commit.id

        return commit.id

    def list_notes(
        self,
        notes_ref: Optional[bytes] = None,
        config: Optional["StackedConfig"] = None,
    ) -> list[tuple[bytes, bytes]]:
        """List all notes in a notes ref.

        Args:
            notes_ref: The notes ref to use, or None to use the default
            config: Config to read notes.displayRef from

        Returns:
            List of tuples of (object_sha, note_content)
        """
        notes_ref = self.get_notes_ref(notes_ref, config)
        try:
            notes_commit_sha = self._refs[notes_ref]
        except KeyError:
            return []

        # Get the commit object
        from .objects import Commit

        notes_obj = self._object_store[notes_commit_sha]

        # If it's a commit, get the tree from it
        if isinstance(notes_obj, Commit):
            notes_tree = self._object_store[notes_obj.tree]
        else:
            # If it's directly a tree (shouldn't happen in normal usage)
            notes_tree = notes_obj

        if not isinstance(notes_tree, Tree):
            return []

        notes_tree_obj = NotesTree(notes_tree, self._object_store)
        result = []
        for object_sha, note_sha in notes_tree_obj.list_notes():
            note_obj = self._object_store[note_sha]
            if isinstance(note_obj, Blob):
                result.append((object_sha, note_obj.data))
        return result
