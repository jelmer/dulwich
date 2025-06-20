"""Tests for dulwich.cli utilities."""

from unittest import TestCase

from dulwich.cli import format_bytes, parse_relative_time


class FormatBytesTestCase(TestCase):
    """Tests for format_bytes function."""

    def test_bytes(self):
        """Test formatting bytes."""
        self.assertEqual("0.0 B", format_bytes(0))
        self.assertEqual("1.0 B", format_bytes(1))
        self.assertEqual("512.0 B", format_bytes(512))
        self.assertEqual("1023.0 B", format_bytes(1023))

    def test_kilobytes(self):
        """Test formatting kilobytes."""
        self.assertEqual("1.0 KB", format_bytes(1024))
        self.assertEqual("1.5 KB", format_bytes(1536))
        self.assertEqual("2.0 KB", format_bytes(2048))
        self.assertEqual("1023.0 KB", format_bytes(1024 * 1023))

    def test_megabytes(self):
        """Test formatting megabytes."""
        self.assertEqual("1.0 MB", format_bytes(1024 * 1024))
        self.assertEqual("1.5 MB", format_bytes(1024 * 1024 * 1.5))
        self.assertEqual("10.0 MB", format_bytes(1024 * 1024 * 10))
        self.assertEqual("1023.0 MB", format_bytes(1024 * 1024 * 1023))

    def test_gigabytes(self):
        """Test formatting gigabytes."""
        self.assertEqual("1.0 GB", format_bytes(1024 * 1024 * 1024))
        self.assertEqual("2.5 GB", format_bytes(1024 * 1024 * 1024 * 2.5))
        self.assertEqual("1023.0 GB", format_bytes(1024 * 1024 * 1024 * 1023))

    def test_terabytes(self):
        """Test formatting terabytes."""
        self.assertEqual("1.0 TB", format_bytes(1024 * 1024 * 1024 * 1024))
        self.assertEqual("5.0 TB", format_bytes(1024 * 1024 * 1024 * 1024 * 5))
        self.assertEqual("1000.0 TB", format_bytes(1024 * 1024 * 1024 * 1024 * 1000))


class ParseRelativeTimeTestCase(TestCase):
    """Tests for parse_relative_time function."""

    def test_now(self):
        """Test parsing 'now'."""
        self.assertEqual(0, parse_relative_time("now"))

    def test_seconds(self):
        """Test parsing seconds."""
        self.assertEqual(1, parse_relative_time("1 second ago"))
        self.assertEqual(5, parse_relative_time("5 seconds ago"))
        self.assertEqual(30, parse_relative_time("30 seconds ago"))

    def test_minutes(self):
        """Test parsing minutes."""
        self.assertEqual(60, parse_relative_time("1 minute ago"))
        self.assertEqual(300, parse_relative_time("5 minutes ago"))
        self.assertEqual(1800, parse_relative_time("30 minutes ago"))

    def test_hours(self):
        """Test parsing hours."""
        self.assertEqual(3600, parse_relative_time("1 hour ago"))
        self.assertEqual(7200, parse_relative_time("2 hours ago"))
        self.assertEqual(86400, parse_relative_time("24 hours ago"))

    def test_days(self):
        """Test parsing days."""
        self.assertEqual(86400, parse_relative_time("1 day ago"))
        self.assertEqual(604800, parse_relative_time("7 days ago"))
        self.assertEqual(2592000, parse_relative_time("30 days ago"))

    def test_weeks(self):
        """Test parsing weeks."""
        self.assertEqual(604800, parse_relative_time("1 week ago"))
        self.assertEqual(1209600, parse_relative_time("2 weeks ago"))
        self.assertEqual(
            36288000, parse_relative_time("60 weeks ago")
        )  # 60 * 7 * 24 * 60 * 60

    def test_invalid_format(self):
        """Test invalid time formats."""
        with self.assertRaises(ValueError) as cm:
            parse_relative_time("invalid")
        self.assertIn("Invalid relative time format", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            parse_relative_time("2 weeks")
        self.assertIn("Invalid relative time format", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            parse_relative_time("ago")
        self.assertIn("Invalid relative time format", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            parse_relative_time("two weeks ago")
        self.assertIn("Invalid number in relative time", str(cm.exception))

    def test_invalid_unit(self):
        """Test invalid time units."""
        with self.assertRaises(ValueError) as cm:
            parse_relative_time("5 months ago")
        self.assertIn("Unknown time unit: months", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            parse_relative_time("2 years ago")
        self.assertIn("Unknown time unit: years", str(cm.exception))

    def test_singular_plural(self):
        """Test that both singular and plural forms work."""
        self.assertEqual(
            parse_relative_time("1 second ago"), parse_relative_time("1 seconds ago")
        )
        self.assertEqual(
            parse_relative_time("1 minute ago"), parse_relative_time("1 minutes ago")
        )
        self.assertEqual(
            parse_relative_time("1 hour ago"), parse_relative_time("1 hours ago")
        )
        self.assertEqual(
            parse_relative_time("1 day ago"), parse_relative_time("1 days ago")
        )
        self.assertEqual(
            parse_relative_time("1 week ago"), parse_relative_time("1 weeks ago")
        )
