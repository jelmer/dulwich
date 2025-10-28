# test_approxidate.py -- tests for approxidate.py
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

"""Tests for approxidate parsing."""

import time

from dulwich.approxidate import parse_approxidate, parse_relative_time

from . import TestCase


class ParseRelativeTimeTests(TestCase):
    """Test parse_relative_time."""

    def test_now(self) -> None:
        self.assertEqual(0, parse_relative_time("now"))

    def test_seconds_ago(self) -> None:
        self.assertEqual(5, parse_relative_time("5 seconds ago"))
        self.assertEqual(1, parse_relative_time("1 second ago"))

    def test_minutes_ago(self) -> None:
        self.assertEqual(5 * 60, parse_relative_time("5 minutes ago"))
        self.assertEqual(1 * 60, parse_relative_time("1 minute ago"))

    def test_hours_ago(self) -> None:
        self.assertEqual(5 * 3600, parse_relative_time("5 hours ago"))
        self.assertEqual(1 * 3600, parse_relative_time("1 hour ago"))

    def test_days_ago(self) -> None:
        self.assertEqual(5 * 86400, parse_relative_time("5 days ago"))
        self.assertEqual(1 * 86400, parse_relative_time("1 day ago"))

    def test_weeks_ago(self) -> None:
        self.assertEqual(2 * 604800, parse_relative_time("2 weeks ago"))
        self.assertEqual(1 * 604800, parse_relative_time("1 week ago"))

    def test_months_ago(self) -> None:
        self.assertEqual(2 * 2592000, parse_relative_time("2 months ago"))
        self.assertEqual(1 * 2592000, parse_relative_time("1 month ago"))

    def test_years_ago(self) -> None:
        self.assertEqual(2 * 31536000, parse_relative_time("2 years ago"))
        self.assertEqual(1 * 31536000, parse_relative_time("1 year ago"))

    def test_dot_separated_format(self) -> None:
        # Git supports both "2 weeks ago" and "2.weeks.ago"
        self.assertEqual(2 * 604800, parse_relative_time("2.weeks.ago"))
        self.assertEqual(5 * 86400, parse_relative_time("5.days.ago"))

    def test_invalid_format(self) -> None:
        self.assertRaises(ValueError, parse_relative_time, "not a time")
        self.assertRaises(ValueError, parse_relative_time, "5 weeks")  # Missing "ago"

    def test_invalid_unit(self) -> None:
        self.assertRaises(ValueError, parse_relative_time, "5 fortnights ago")

    def test_invalid_number(self) -> None:
        self.assertRaises(ValueError, parse_relative_time, "abc weeks ago")


class ParseApproxidateTests(TestCase):
    """Test parse_approxidate."""

    def test_now(self) -> None:
        result = parse_approxidate("now")
        # Should be close to current time
        self.assertAlmostEqual(result, time.time(), delta=2)

    def test_yesterday(self) -> None:
        result = parse_approxidate("yesterday")
        expected = time.time() - 86400
        self.assertAlmostEqual(result, expected, delta=2)

    def test_today(self) -> None:
        result = parse_approxidate("today")
        # Should be midnight of current day
        from datetime import datetime

        now = datetime.fromtimestamp(time.time())
        expected_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        expected = int(expected_dt.timestamp())
        self.assertEqual(result, expected)

    def test_unix_timestamp(self) -> None:
        self.assertEqual(1234567890, parse_approxidate("1234567890"))
        self.assertEqual(0, parse_approxidate("0"))

    def test_relative_times(self) -> None:
        # Test relative time parsing
        result = parse_approxidate("2 weeks ago")
        expected = time.time() - (2 * 604800)
        self.assertAlmostEqual(result, expected, delta=2)

        result = parse_approxidate("5.days.ago")
        expected = time.time() - (5 * 86400)
        self.assertAlmostEqual(result, expected, delta=2)

    def test_absolute_date_iso(self) -> None:
        # Test ISO format date
        result = parse_approxidate("2009-02-13")
        # 2009-02-13 00:00:00 UTC
        from datetime import datetime

        expected = int(datetime(2009, 2, 13, 0, 0, 0).timestamp())
        self.assertEqual(result, expected)

    def test_absolute_datetime_iso(self) -> None:
        # Test ISO format datetime
        result = parse_approxidate("2009-02-13 23:31:30")
        from datetime import datetime

        expected = int(datetime(2009, 2, 13, 23, 31, 30).timestamp())
        self.assertEqual(result, expected)

    def test_absolute_datetime_iso8601(self) -> None:
        # Test ISO 8601 format
        result = parse_approxidate("2009-02-13T23:31:30")
        from datetime import datetime

        expected = int(datetime(2009, 2, 13, 23, 31, 30).timestamp())
        self.assertEqual(result, expected)

    def test_bytes_input(self) -> None:
        # Test that bytes input works
        result = parse_approxidate(b"1234567890")
        self.assertEqual(1234567890, result)

        result = parse_approxidate(b"yesterday")
        expected = time.time() - 86400
        self.assertAlmostEqual(result, expected, delta=2)

    def test_whitespace_handling(self) -> None:
        # Test that leading/trailing whitespace is handled
        self.assertEqual(1234567890, parse_approxidate("  1234567890  "))
        result = parse_approxidate("  yesterday  ")
        expected = time.time() - 86400
        self.assertAlmostEqual(result, expected, delta=2)

    def test_invalid_spec(self) -> None:
        self.assertRaises(ValueError, parse_approxidate, "not a valid time")
        self.assertRaises(ValueError, parse_approxidate, "abc123")
