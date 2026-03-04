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

"""Applying mailbox-style patches to a repository, similar to git am."""

__all__ = [
    "am",
]

import calendar
import email.message
import email.utils
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .repo import Repo

from .patch import apply_patches, mailinfo, parse_unified_diff


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
) -> list[bytes]:
    """Apply patches from email messages to a repository, creating commits.

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
        ValueError: If a message has no patch content or cannot be applied
    """
    commit_shas: list[bytes] = []

    for msg in msgs:
        info = mailinfo(
            msg,
            keep_subject=keep_subject,
            keep_non_patch=keep_non_patch,
            scissors=scissors,
            message_id=message_id,
        )

        # Parse the patch content
        patch_bytes = info.patch.encode("utf-8")
        patches = parse_unified_diff(patch_bytes)
        if not patches:
            continue

        # Apply patches to worktree and index
        apply_patches(r, patches, strip=strip, three_way=three_way)

        # Build author identity
        author = f"{info.author_name} <{info.author_email}>".encode()

        # Parse author date
        author_timestamp: float | None = None
        author_timezone: int | None = None
        if info.author_date:
            parsed_date = email.utils.parsedate_tz(info.author_date)
            if parsed_date is not None:
                author_timestamp = float(calendar.timegm(parsed_date[:9]))
                tz_offset = parsed_date[9]
                if tz_offset is not None:
                    # Convert seconds offset to dulwich format (seconds east of UTC)
                    author_timezone = tz_offset

        # Build commit message from subject and body
        commit_message = info.subject
        if info.message:
            commit_message += "\n\n" + info.message
        commit_message_bytes = commit_message.encode("utf-8")

        # Create the commit
        sha = r.get_worktree().commit(
            message=commit_message_bytes,
            author=author,
            author_timestamp=author_timestamp,
            author_timezone=author_timezone,
            committer=committer,
            commit_timestamp=commit_timestamp,
            commit_timezone=commit_timezone,
        )
        commit_shas.append(sha)

    return commit_shas
