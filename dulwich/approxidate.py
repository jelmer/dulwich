# approxidate.py -- Parsing of Git's "approxidate" time specifications
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

"""Parsing of Git's "approxidate" time specifications.

Git uses a flexible date parser called "approxidate" that accepts various
formats for specifying dates and times, including:
- Relative times: "yesterday", "2 days ago", "2.weeks.ago"
- Absolute dates: "2005-04-07", "2005-04-07 22:13:13"
- Unix timestamps: "1234567890"
- Special keywords: "now", "today", "yesterday"
"""

__all__ = ["parse_approxidate", "parse_relative_time"]

import time
from datetime import datetime


def parse_approxidate(time_spec: str | bytes) -> int:
    """Parse a Git approxidate specification and return a Unix timestamp.

    Args:
        time_spec: Time specification string. Can be:
            - A Unix timestamp (integer as string)
            - A relative time like "2 weeks ago" or "2.weeks.ago"
            - Special keywords: "now", "today", "yesterday"
            - Absolute date: "2005-04-07" or "2005-04-07 22:13:13"

    Returns:
        Unix timestamp (seconds since epoch)

    Raises:
        ValueError: If the time specification cannot be parsed
    """
    if isinstance(time_spec, bytes):
        time_spec = time_spec.decode("utf-8")

    time_spec = time_spec.strip()

    # Get current time
    now = time.time()

    # Handle special keywords
    if time_spec == "yesterday":
        return int(now - 86400)
    elif time_spec == "today":
        # Start of today (midnight)
        dt = datetime.fromtimestamp(now)
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(dt.timestamp())
    elif time_spec == "now":
        return int(now)

    # Try parsing as Unix timestamp
    try:
        return int(time_spec)
    except ValueError:
        pass

    # Handle relative time specifications
    # Supports both "2 weeks ago" and "2.weeks.ago" formats
    if " ago" in time_spec or ".ago" in time_spec:
        seconds_ago = parse_relative_time(time_spec)
        return int(now - seconds_ago)

    # Try parsing as absolute timestamp formats
    # Git supports various formats like:
    # - "2005-04-07" (ISO date)
    # - "2005-04-07 22:13:13" (ISO datetime)
    # - "2005-04-07T22:13:13" (ISO 8601)
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(time_spec, fmt)
            return int(dt.timestamp())
        except ValueError:
            continue

    raise ValueError(f"Unable to parse time specification: {time_spec!r}")


def parse_relative_time(time_str: str) -> int:
    """Parse a relative time string like '2 weeks ago' into seconds.

    Args:
        time_str: String like '2 weeks ago', '2.weeks.ago', or 'now'

    Returns:
        Number of seconds (relative to current time)

    Raises:
        ValueError: If the time string cannot be parsed
    """
    if time_str == "now":
        return 0

    # Normalize dot-separated format to space-separated
    # "2.weeks.ago" -> "2 weeks ago"
    normalized = time_str.replace(".ago", " ago").replace(".", " ")

    if not normalized.endswith(" ago"):
        raise ValueError(f"Invalid relative time format: {time_str}")

    parts = normalized[:-4].split()
    if len(parts) != 2:
        raise ValueError(f"Invalid relative time format: {time_str}")

    try:
        num = int(parts[0])
        unit = parts[1]

        multipliers = {
            "second": 1,
            "seconds": 1,
            "minute": 60,
            "minutes": 60,
            "hour": 3600,
            "hours": 3600,
            "day": 86400,
            "days": 86400,
            "week": 604800,
            "weeks": 604800,
            "month": 2592000,  # 30 days
            "months": 2592000,
            "year": 31536000,  # 365 days
            "years": 31536000,
        }

        if unit in multipliers:
            return num * multipliers[unit]
        else:
            raise ValueError(f"Unknown time unit: {unit}")
    except ValueError as e:
        if "invalid literal" in str(e):
            raise ValueError(f"Invalid number in relative time: {parts[0]}")
        raise
