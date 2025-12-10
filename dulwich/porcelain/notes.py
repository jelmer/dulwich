# notes.py -- Porcelain-like interface for Git notes
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Porcelain-like interface for Git notes."""

from typing import TYPE_CHECKING

from dulwich.objects import ObjectID
from dulwich.objectspec import parse_object

from ..refs import LOCAL_NOTES_PREFIX

if TYPE_CHECKING:
    from . import RepoPath


def _make_notes_ref(name: bytes) -> bytes:
    """Make a notes ref name."""
    if name.startswith(b"refs/notes/"):
        return name
    return LOCAL_NOTES_PREFIX + name


def notes_add(
    repo: "RepoPath",
    object_sha: bytes,
    note: bytes,
    ref: bytes = b"commits",
    author: bytes | None = None,
    committer: bytes | None = None,
    message: bytes | None = None,
) -> bytes:
    """Add or update a note for an object.

    Args:
      repo: Path to repository
      object_sha: SHA of the object to annotate
      note: Note content
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)
      author: Author identity (defaults to committer)
      committer: Committer identity (defaults to config)
      message: Commit message for the notes update

    Returns:
      SHA of the new notes commit
    """
    from . import DEFAULT_ENCODING, open_repo_closing

    with open_repo_closing(repo) as r:
        # Parse the object to get its SHA
        obj = parse_object(r, object_sha)
        object_sha = obj.id

        if isinstance(note, str):
            note = note.encode(DEFAULT_ENCODING)
        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.set_note(
            object_sha,
            note,
            notes_ref,
            author=author,
            committer=committer,
            message=message,
            config=config,
        )


def notes_remove(
    repo: "RepoPath",
    object_sha: bytes,
    ref: bytes = b"commits",
    author: bytes | None = None,
    committer: bytes | None = None,
    message: bytes | None = None,
) -> bytes | None:
    """Remove a note for an object.

    Args:
      repo: Path to repository
      object_sha: SHA of the object to remove notes from
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)
      author: Author identity (defaults to committer)
      committer: Committer identity (defaults to config)
      message: Commit message for the notes removal

    Returns:
      SHA of the new notes commit, or None if no note existed
    """
    from . import DEFAULT_ENCODING, open_repo_closing

    with open_repo_closing(repo) as r:
        # Parse the object to get its SHA
        obj = parse_object(r, object_sha)
        object_sha = obj.id

        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.remove_note(
            object_sha,
            notes_ref,
            author=author,
            committer=committer,
            message=message,
            config=config,
        )


def notes_show(
    repo: "RepoPath", object_sha: bytes, ref: bytes = b"commits"
) -> bytes | None:
    """Show the note for an object.

    Args:
      repo: Path to repository
      object_sha: SHA of the object
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)

    Returns:
      Note content as bytes, or None if no note exists
    """
    from . import DEFAULT_ENCODING, open_repo_closing

    with open_repo_closing(repo) as r:
        # Parse the object to get its SHA
        obj = parse_object(r, object_sha)
        object_sha = obj.id

        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.get_note(object_sha, notes_ref, config=config)


def notes_list(
    repo: "RepoPath", ref: bytes = b"commits"
) -> list[tuple[ObjectID, bytes]]:
    """List all notes in a notes ref.

    Args:
      repo: Path to repository
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)

    Returns:
      List of tuples of (object_sha, note_content)
    """
    from . import DEFAULT_ENCODING, open_repo_closing

    with open_repo_closing(repo) as r:
        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.list_notes(notes_ref, config=config)
