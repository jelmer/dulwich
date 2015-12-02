# reflog.py -- Parsing and writing reflog files
# Copyright (C) 2015 Jelmer Vernooij and others.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version of the License.
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

"""Utilities for reading and generating reflogs.
"""

from dulwich.objects import (
    format_timezone,
    ZERO_SHA,
    )


def format_reflog_line(old_sha, new_sha, committer, timestamp, timezone, message):
    """Generate a single reflog line.

    :param old_sha: Old Commit SHA
    :param new_sha: New Commit SHA
    :param committer: Committer name and e-mail
    :param timestamp: Timestamp
    :param timezone: Timezone
    :param message: Message
    """
    if old_sha is None:
        old_sha = ZERO_SHA
    return (old_sha + b' ' + new_sha + b' ' + committer + b' ' +
            str(timestamp).encode('ascii') + b' ' +
            format_timezone(timezone).encode('ascii') + b'\t' + message)
