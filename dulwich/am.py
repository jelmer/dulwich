# am.py -- Applying mailbox-style patches (git am)
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

"""Applying mailbox-style patches to a repository, similar to git am.

Supports state persistence in .git/rebase-apply/ for --continue, --skip,
--abort, and --quit operations when a patch fails to apply.
"""

__all__ = [
    "AmConflict",
    "AmError",
    "DiskAmStateManager",
    "am",
    "am_abort",
    "am_continue",
    "am_quit",
    "am_skip",
]

import calendar
import email.message
import email.parser
import email.utils
import os
import shutil
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .repo import Repo

from .objects import Commit, ObjectID
from .patch import (
    PatchApplicationFailure,
    apply_patches,
    mailinfo,
    parse_unified_diff,
)
from .refs import HEADREF


class AmError(Exception):
    """Base class for am errors."""


class AmConflict(AmError):
    """Raised when a patch fails to apply during am."""

    def __init__(self, patch_number: int, total: int, message: str = "") -> None:
        """Initialize AmConflict.

        Args:
            patch_number: 1-based index of the failing patch
            total: Total number of patches in the series
            message: Optional detail about the failure
        """
        self.patch_number = patch_number
        self.total = total
        super().__init__(
            f"Patch {patch_number}/{total} failed to apply"
            + (f": {message}" if message else "")
        )


class DiskAmStateManager:
    """Manages git am state on disk in .git/rebase-apply/.

    Follows the same file layout as C git.
    """

    def __init__(self, path: str) -> None:
        """Initialize with the path to the rebase-apply directory."""
        self.path = path

    def exists(self) -> bool:
        """Check if am state directory exists."""
        return os.path.isdir(self.path)

    def _write_file(self, name: str, content: bytes) -> None:
        with open(os.path.join(self.path, name), "wb") as f:
            f.write(content)

    def _read_file(self, name: str, strip: bool = True) -> bytes | None:
        try:
            with open(os.path.join(self.path, name), "rb") as f:
                data = f.read()
                return data.strip() if strip else data
        except FileNotFoundError:
            return None

    def _has_file(self, name: str) -> bool:
        return os.path.exists(os.path.join(self.path, name))

    def save_initial(
        self,
        orig_head: bytes,
        msg_bytes_list: list[bytes],
        *,
        three_way: bool = False,
        keep_subject: bool = False,
        keep_non_patch: bool = False,
        scissors: bool = False,
        message_id: bool = False,
        strip: int = 1,
    ) -> None:
        """Initialize state directory with patches and options."""
        os.makedirs(self.path, exist_ok=True)

        self._write_file("orig-head", orig_head)
        self._write_file("next", b"1")
        self._write_file("last", str(len(msg_bytes_list)).encode())
        self._write_file("utf8", b"t")
        self._write_file("strip", str(strip).encode())

        # Write option flags
        if three_way:
            self._write_file("threeway", b"t")
        if keep_subject:
            self._write_file("keep", b"t")
        if keep_non_patch:
            self._write_file("keep_non_patch", b"t")
        if scissors:
            self._write_file("scissors", b"t")
        if message_id:
            self._write_file("messageid", b"t")

        # Write individual patch files as 0001, 0002, etc.
        for i, msg_bytes in enumerate(msg_bytes_list, start=1):
            self._write_file(f"{i:04d}", msg_bytes)

    def get_next(self) -> int:
        """Return the 1-based index of the current patch."""
        data = self._read_file("next")
        if data is None:
            raise AmError("Corrupt am state: missing 'next' file")
        return int(data)

    def get_last(self) -> int:
        """Return the total number of patches."""
        data = self._read_file("last")
        if data is None:
            raise AmError("Corrupt am state: missing 'last' file")
        return int(data)

    def get_orig_head(self) -> ObjectID:
        """Return the original HEAD sha from before am started."""
        data = self._read_file("orig-head")
        if data is None:
            raise AmError("Corrupt am state: missing 'orig-head' file")
        return ObjectID(data)

    def get_strip(self) -> int:
        """Return the number of path components to strip."""
        data = self._read_file("strip")
        return int(data) if data is not None else 1

    def get_three_way(self) -> bool:
        """Return whether three-way merge fallback is enabled."""
        return self._has_file("threeway")

    def get_keep_subject(self) -> bool:
        """Return whether subject munging is disabled."""
        return self._has_file("keep")

    def get_keep_non_patch(self) -> bool:
        """Return whether only [PATCH] is stripped from brackets."""
        return self._has_file("keep_non_patch")

    def get_scissors(self) -> bool:
        """Return whether scissors line handling is enabled."""
        return self._has_file("scissors")

    def get_message_id(self) -> bool:
        """Return whether Message-ID inclusion is enabled."""
        return self._has_file("messageid")

    def get_patch(self, number: int) -> bytes:
        """Return the raw email bytes for the given patch number."""
        data = self._read_file(f"{number:04d}", strip=False)
        if data is None:
            raise AmError(f"Corrupt am state: missing patch file '{number:04d}'")
        return data

    def set_next(self, value: int) -> None:
        """Set the current patch index."""
        self._write_file("next", str(value).encode())

    def save_author_info(self, name: str, email_addr: str, date: str | None) -> None:
        """Save author metadata for the current patch."""
        content = (
            f"GIT_AUTHOR_NAME={name}\n"
            f"GIT_AUTHOR_EMAIL={email_addr}\n"
            f"GIT_AUTHOR_DATE={date or ''}\n"
        )
        self._write_file("author-script", content.encode())

    def load_author_info(self) -> tuple[str, str, str | None]:
        """Load saved author metadata for the current patch."""
        data = self._read_file("author-script")
        if data is None:
            raise AmError("Corrupt am state: missing 'author-script' file")
        name = ""
        email_addr = ""
        date: str | None = None
        for line in data.decode().splitlines():
            if line.startswith("GIT_AUTHOR_NAME="):
                name = line[len("GIT_AUTHOR_NAME=") :]
            elif line.startswith("GIT_AUTHOR_EMAIL="):
                email_addr = line[len("GIT_AUTHOR_EMAIL=") :]
            elif line.startswith("GIT_AUTHOR_DATE="):
                val = line[len("GIT_AUTHOR_DATE=") :]
                date = val if val else None
        return name, email_addr, date

    def save_message(self, message: bytes) -> None:
        """Save the commit message for the current patch."""
        self._write_file("final-commit", message)

    def load_message(self) -> bytes:
        """Load the saved commit message for the current patch."""
        data = self._read_file("final-commit")
        if data is None:
            raise AmError("Corrupt am state: missing 'final-commit' file")
        return data

    def clean(self) -> None:
        """Remove the state directory."""
        try:
            shutil.rmtree(self.path)
        except FileNotFoundError:
            pass


def _get_state_manager(r: "Repo") -> DiskAmStateManager:
    return DiskAmStateManager(os.path.join(r.controldir(), "rebase-apply"))


def _parse_author_date(
    date_str: str | None,
) -> tuple[float | None, int | None]:
    """Parse an RFC 2822 date string into (timestamp, timezone_offset)."""
    if not date_str:
        return None, None
    parsed = email.utils.parsedate_tz(date_str)
    if parsed is None:
        return None, None
    timestamp = float(calendar.timegm(parsed[:9]))
    tz_offset = parsed[9]
    return timestamp, tz_offset


def _reset_worktree_to_tree(r: "Repo", target_tree_id: ObjectID) -> None:
    """Hard-reset working tree and index to match a given tree."""
    from .diff_tree import tree_changes
    from .index import update_working_tree

    index = r.open_index()
    if len(index) > 0:
        index_tree_id = index.commit(r.object_store)
    else:
        index_tree_id = None

    changes = tree_changes(
        r.object_store, index_tree_id, target_tree_id, want_unchanged=True
    )
    update_working_tree(
        r,
        index_tree_id,
        target_tree_id,
        change_iterator=changes,
        force_remove_untracked=True,
        allow_overwrite_modified=True,
    )


def _apply_single_patch(
    r: "Repo",
    state: DiskAmStateManager,
    patch_number: int,
    total: int,
    *,
    committer: bytes | None = None,
    commit_timestamp: float | None = None,
    commit_timezone: int | None = None,
) -> ObjectID:
    """Apply a single patch from state and create a commit.

    Returns the commit SHA.

    Raises:
        AmConflict: If the patch cannot be applied.
    """
    patch_data = state.get_patch(patch_number)

    # Parse the email message
    parser = email.parser.BytesParser()
    msg = parser.parsebytes(patch_data)

    # Extract info
    info = mailinfo(
        msg,
        keep_subject=state.get_keep_subject(),
        keep_non_patch=state.get_keep_non_patch(),
        scissors=state.get_scissors(),
        message_id=state.get_message_id(),
    )

    # Save author info and message for --continue to use if this fails
    state.save_author_info(info.author_name, info.author_email, info.author_date)
    commit_message = info.subject
    if info.message:
        commit_message += "\n\n" + info.message
    commit_message_bytes = commit_message.encode()
    state.save_message(commit_message_bytes)

    # Parse and apply the patch
    patch_bytes = info.patch.encode()
    patches = parse_unified_diff(patch_bytes)
    if not patches:
        raise AmConflict(patch_number, total, "no patch content found in message")

    strip = state.get_strip()
    three_way = state.get_three_way()

    try:
        apply_patches(r, patches, strip=strip, three_way=three_way)
    except PatchApplicationFailure as e:
        raise AmConflict(patch_number, total, str(e)) from e

    # Build author identity and date
    author = f"{info.author_name} <{info.author_email}>".encode()
    author_timestamp, author_timezone = _parse_author_date(info.author_date)

    # Create commit
    sha = r.get_worktree().commit(
        message=commit_message_bytes,
        author=author,
        author_timestamp=author_timestamp,
        author_timezone=author_timezone,
        committer=committer,
        commit_timestamp=commit_timestamp,
        commit_timezone=commit_timezone,
    )
    return sha


def _apply_remaining(
    r: "Repo",
    state: DiskAmStateManager,
    start: int,
    total: int,
    *,
    committer: bytes | None = None,
    commit_timestamp: float | None = None,
    commit_timezone: int | None = None,
) -> list[ObjectID]:
    """Apply patches from start to total, cleaning up state on success.

    Raises:
        AmConflict: If a patch fails to apply (state preserved for recovery).
    """
    commit_shas: list[ObjectID] = []
    for i in range(start, total + 1):
        state.set_next(i)
        sha = _apply_single_patch(
            r,
            state,
            i,
            total,
            committer=committer,
            commit_timestamp=commit_timestamp,
            commit_timezone=commit_timezone,
        )
        commit_shas.append(sha)

    state.clean()
    return commit_shas


def am(
    r: "Repo",
    msgs: Iterable[email.message.Message],
    *,
    three_way: bool = False,
    keep_subject: bool = False,
    keep_non_patch: bool = False,
    scissors: bool = False,
    message_id: bool = False,
    strip: int = 1,
    committer: bytes | None = None,
    commit_timestamp: float | None = None,
    commit_timezone: int | None = None,
) -> list[ObjectID]:
    """Apply patches from email messages to a repository, creating commits.

    Saves state to .git/rebase-apply/ so that if a patch fails, the user
    can resolve and use am_continue(), am_skip(), am_abort(), or am_quit().

    Args:
        r: Repository object
        msgs: Iterable of email.message.Message objects containing patches
        three_way: Fall back to 3-way merge if patch does not apply cleanly
        keep_subject: If True, keep subject intact without munging
        keep_non_patch: If True, only strip [PATCH] from brackets
        scissors: If True, remove everything before scissors line
        message_id: If True, include Message-ID in commit message
        strip: Number of leading path components to strip (default: 1)
        committer: Optional committer identity (bytes)
        commit_timestamp: Optional committer timestamp
        commit_timezone: Optional committer timezone offset

    Returns:
        List of commit SHAs (bytes) created

    Raises:
        AmConflict: If a patch fails to apply (state is saved for recovery)
        AmError: If am state already exists (previous am in progress)
    """
    state = _get_state_manager(r)
    if state.exists():
        raise AmError(
            "previous am is still in progress; "
            "use am_continue, am_skip, am_abort, or am_quit first"
        )

    # Serialize all messages to bytes for state persistence
    msg_bytes_list = [msg.as_bytes() for msg in msgs]
    if not msg_bytes_list:
        return []

    # Record original HEAD
    try:
        orig_head = r.head()
    except KeyError:
        raise AmError("Cannot run am on a repository with no commits") from None

    # Save initial state
    state.save_initial(
        orig_head,
        msg_bytes_list,
        three_way=three_way,
        keep_subject=keep_subject,
        keep_non_patch=keep_non_patch,
        scissors=scissors,
        message_id=message_id,
        strip=strip,
    )

    return _apply_remaining(
        r,
        state,
        start=1,
        total=len(msg_bytes_list),
        committer=committer,
        commit_timestamp=commit_timestamp,
        commit_timezone=commit_timezone,
    )


def am_continue(
    r: "Repo",
    *,
    committer: bytes | None = None,
    commit_timestamp: float | None = None,
    commit_timezone: int | None = None,
) -> list[ObjectID]:
    """Continue applying patches after resolving a conflict.

    The user should have resolved conflicts in the working tree and staged
    the result before calling this.

    Args:
        r: Repository object
        committer: Optional committer identity
        commit_timestamp: Optional committer timestamp
        commit_timezone: Optional committer timezone offset

    Returns:
        List of commit SHAs created (for current + remaining patches)

    Raises:
        AmError: If no am is in progress
        AmConflict: If a subsequent patch fails to apply
    """
    state = _get_state_manager(r)
    if not state.exists():
        raise AmError("No am in progress")

    current = state.get_next()
    total = state.get_last()

    # Create commit for the current (conflicted) patch using saved metadata
    name, email_addr, date = state.load_author_info()
    message = state.load_message()
    author = f"{name} <{email_addr}>".encode()
    author_timestamp, author_timezone = _parse_author_date(date)

    sha = r.get_worktree().commit(
        message=message,
        author=author,
        author_timestamp=author_timestamp,
        author_timezone=author_timezone,
        committer=committer,
        commit_timestamp=commit_timestamp,
        commit_timezone=commit_timezone,
    )
    commit_shas = [sha]

    # Continue with remaining patches
    if current < total:
        commit_shas.extend(
            _apply_remaining(
                r,
                state,
                start=current + 1,
                total=total,
                committer=committer,
                commit_timestamp=commit_timestamp,
                commit_timezone=commit_timezone,
            )
        )
    else:
        state.clean()

    return commit_shas


def am_skip(
    r: "Repo",
    *,
    committer: bytes | None = None,
    commit_timestamp: float | None = None,
    commit_timezone: int | None = None,
) -> list[ObjectID]:
    """Skip the current patch and continue with remaining patches.

    Resets the working tree and index to HEAD before continuing.

    Args:
        r: Repository object
        committer: Optional committer identity
        commit_timestamp: Optional committer timestamp
        commit_timezone: Optional committer timezone offset

    Returns:
        List of commit SHAs created (for remaining patches)

    Raises:
        AmError: If no am is in progress
        AmConflict: If a subsequent patch fails to apply
    """
    state = _get_state_manager(r)
    if not state.exists():
        raise AmError("No am in progress")

    # Reset worktree to HEAD to undo the failed patch application
    head_commit = r[r.head()]
    assert isinstance(head_commit, Commit)
    _reset_worktree_to_tree(r, head_commit.tree)

    current = state.get_next()
    total = state.get_last()

    if current < total:
        return _apply_remaining(
            r,
            state,
            start=current + 1,
            total=total,
            committer=committer,
            commit_timestamp=commit_timestamp,
            commit_timezone=commit_timezone,
        )
    else:
        state.clean()
        return []


def am_abort(r: "Repo") -> None:
    """Abort the current am operation and restore the original state.

    Resets HEAD to the original commit from before am started, and
    resets the working tree and index to match.

    Args:
        r: Repository object

    Raises:
        AmError: If no am is in progress
    """
    state = _get_state_manager(r)
    if not state.exists():
        raise AmError("No am in progress")

    orig_head = state.get_orig_head()
    orig_commit = r[orig_head]
    assert isinstance(orig_commit, Commit)

    # Reset working tree and index to orig-head's tree
    _reset_worktree_to_tree(r, orig_commit.tree)

    # Reset HEAD to original
    try:
        old_head: ObjectID | None = r.refs[HEADREF]
    except KeyError:
        old_head = None
    r.refs.set_if_equals(HEADREF, old_head, orig_head)

    state.clean()


def am_quit(r: "Repo") -> None:
    """Quit the current am operation without reverting changes.

    Removes the am state directory but keeps HEAD, index, and working
    tree as they are.

    Args:
        r: Repository object

    Raises:
        AmError: If no am is in progress
    """
    state = _get_state_manager(r)
    if not state.exists():
        raise AmError("No am in progress")

    state.clean()
