# reflog.py -- Parsing and writing reflog files
# Copyright (C) 2015 Jelmer Vernooij and others.
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

"""Utilities for reading and generating reflogs."""

import collections
from collections.abc import Callable, Generator
from typing import IO, BinaryIO, Optional, Union

from .file import _GitFile
from .objects import ZERO_SHA, format_timezone, parse_timezone

Entry = collections.namedtuple(
    "Entry",
    ["old_sha", "new_sha", "committer", "timestamp", "timezone", "message"],
)


def parse_reflog_spec(refspec: Union[str, bytes]) -> tuple[bytes, int]:
    """Parse a reflog specification like 'HEAD@{1}' or 'refs/heads/master@{2}'.

    Args:
        refspec: Reflog specification (e.g., 'HEAD@{1}', 'master@{0}')

    Returns:
        Tuple of (ref_name, index) where index is in Git reflog order (0 = newest)

    Raises:
        ValueError: If the refspec is not a valid reflog specification
    """
    if isinstance(refspec, str):
        refspec = refspec.encode("utf-8")

    if b"@{" not in refspec:
        raise ValueError(
            f"Invalid reflog spec: {refspec!r}. Expected format: ref@{{n}}"
        )

    ref, rest = refspec.split(b"@{", 1)
    if not rest.endswith(b"}"):
        raise ValueError(
            f"Invalid reflog spec: {refspec!r}. Expected format: ref@{{n}}"
        )

    index_str = rest[:-1]
    if not index_str.isdigit():
        raise ValueError(
            f"Invalid reflog index: {index_str!r}. Expected integer in ref@{{n}}"
        )

    # Use HEAD if no ref specified (e.g., "@{1}")
    if not ref:
        ref = b"HEAD"

    return ref, int(index_str)


def format_reflog_line(
    old_sha: Optional[bytes],
    new_sha: bytes,
    committer: bytes,
    timestamp: Union[int, float],
    timezone: int,
    message: bytes,
) -> bytes:
    """Generate a single reflog line.

    Args:
      old_sha: Old Commit SHA
      new_sha: New Commit SHA
      committer: Committer name and e-mail
      timestamp: Timestamp
      timezone: Timezone
      message: Message
    """
    if old_sha is None:
        old_sha = ZERO_SHA
    return (
        old_sha
        + b" "
        + new_sha
        + b" "
        + committer
        + b" "
        + str(int(timestamp)).encode("ascii")
        + b" "
        + format_timezone(timezone)
        + b"\t"
        + message
    )


def parse_reflog_line(line: bytes) -> Entry:
    """Parse a reflog line.

    Args:
      line: Line to parse
    Returns: Tuple of (old_sha, new_sha, committer, timestamp, timezone,
        message)
    """
    (begin, message) = line.split(b"\t", 1)
    (old_sha, new_sha, rest) = begin.split(b" ", 2)
    (committer, timestamp_str, timezone_str) = rest.rsplit(b" ", 2)
    return Entry(
        old_sha,
        new_sha,
        committer,
        int(timestamp_str),
        parse_timezone(timezone_str)[0],
        message,
    )


def read_reflog(
    f: Union[BinaryIO, IO[bytes], _GitFile],
) -> Generator[Entry, None, None]:
    """Read reflog.

    Args:
      f: File-like object
    Returns: Iterator over Entry objects
    """
    for line in f:
        yield parse_reflog_line(line.rstrip(b"\n"))


def drop_reflog_entry(f: BinaryIO, index: int, rewrite: bool = False) -> None:
    """Drop the specified reflog entry.

    Args:
        f: File-like object
        index: Reflog entry index (in Git reflog reverse 0-indexed order)
        rewrite: If a reflog entry's predecessor is removed, set its
            old SHA to the new SHA of the entry that now precedes it
    """
    if index < 0:
        raise ValueError(f"Invalid reflog index {index}")

    log = []
    offset = f.tell()
    for line in f:
        log.append((offset, parse_reflog_line(line)))
        offset = f.tell()

    inverse_index = len(log) - index - 1
    write_offset = log[inverse_index][0]
    f.seek(write_offset)

    if index == 0:
        f.truncate()
        return

    del log[inverse_index]
    if rewrite and index > 0 and log:
        if inverse_index == 0:
            previous_new = ZERO_SHA
        else:
            previous_new = log[inverse_index - 1][1].new_sha
        offset, entry = log[inverse_index]
        log[inverse_index] = (
            offset,
            Entry(
                previous_new,
                entry.new_sha,
                entry.committer,
                entry.timestamp,
                entry.timezone,
                entry.message,
            ),
        )

    for _, entry in log[inverse_index:]:
        f.write(
            format_reflog_line(
                entry.old_sha,
                entry.new_sha,
                entry.committer,
                entry.timestamp,
                entry.timezone,
                entry.message,
            )
        )
    f.truncate()


def expire_reflog(
    f: BinaryIO,
    expire_time: Optional[int] = None,
    expire_unreachable_time: Optional[int] = None,
    # String annotation to work around typing module bug in Python 3.9.0/3.9.1
    # See: https://github.com/jelmer/dulwich/issues/1948
    reachable_checker: "Optional[Callable[[bytes], bool]]" = None,
) -> int:
    """Expire reflog entries based on age and reachability.

    Args:
        f: File-like object for the reflog
        expire_time: Expire entries older than this timestamp (seconds since epoch).
            If None, entries are not expired based on age alone.
        expire_unreachable_time: Expire unreachable entries older than this
            timestamp. If None, unreachable entries are not expired.
        reachable_checker: Optional callable that takes a SHA and returns True
            if the commit is reachable. If None, all entries are considered
            reachable.

    Returns:
        Number of entries expired
    """
    if expire_time is None and expire_unreachable_time is None:
        return 0

    entries = []
    offset = f.tell()
    for line in f:
        entries.append((offset, parse_reflog_line(line)))
        offset = f.tell()

    # Filter entries that should be kept
    kept_entries = []
    expired_count = 0

    for offset, entry in entries:
        should_expire = False

        # Check if entry is reachable
        is_reachable = True
        if reachable_checker is not None:
            is_reachable = reachable_checker(entry.new_sha)

        # Apply expiration rules
        # Check the appropriate expiration time based on reachability
        if is_reachable:
            if expire_time is not None and entry.timestamp < expire_time:
                should_expire = True
        else:
            if (
                expire_unreachable_time is not None
                and entry.timestamp < expire_unreachable_time
            ):
                should_expire = True

        if should_expire:
            expired_count += 1
        else:
            kept_entries.append((offset, entry))

    # Write back the kept entries
    if expired_count > 0:
        f.seek(0)
        for _, entry in kept_entries:
            f.write(
                format_reflog_line(
                    entry.old_sha,
                    entry.new_sha,
                    entry.committer,
                    entry.timestamp,
                    entry.timezone,
                    entry.message,
                )
            )
        f.truncate()

    return expired_count


def iter_reflogs(logs_dir: str) -> Generator[bytes, None, None]:
    """Iterate over all reflogs in a repository.

    Args:
        logs_dir: Path to the logs directory (e.g., .git/logs)

    Yields:
        Reference names (as bytes) that have reflogs
    """
    import os
    from pathlib import Path

    if not os.path.exists(logs_dir):
        return

    logs_path = Path(logs_dir)
    for log_file in logs_path.rglob("*"):
        if log_file.is_file():
            # Get the ref name by removing the logs_dir prefix
            ref_name = str(log_file.relative_to(logs_path))
            # Convert path separators to / for refs
            ref_name = ref_name.replace(os.sep, "/")
            yield ref_name.encode("utf-8")
