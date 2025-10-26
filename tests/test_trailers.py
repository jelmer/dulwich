# test_trailers.py -- tests for git trailers
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

"""Tests for dulwich.trailers."""

import unittest

from dulwich.trailers import (
    Trailer,
    add_trailer_to_message,
    format_trailers,
    parse_trailers,
)


class TestTrailer(unittest.TestCase):
    """Tests for the Trailer class."""

    def test_init(self) -> None:
        """Test Trailer initialization."""
        trailer = Trailer("Signed-off-by", "Alice <alice@example.com>")
        self.assertEqual(trailer.key, "Signed-off-by")
        self.assertEqual(trailer.value, "Alice <alice@example.com>")
        self.assertEqual(trailer.separator, ":")

    def test_str(self) -> None:
        """Test Trailer string representation."""
        trailer = Trailer("Signed-off-by", "Alice <alice@example.com>")
        self.assertEqual(str(trailer), "Signed-off-by: Alice <alice@example.com>")

    def test_equality(self) -> None:
        """Test Trailer equality."""
        t1 = Trailer("Signed-off-by", "Alice")
        t2 = Trailer("Signed-off-by", "Alice")
        t3 = Trailer("Signed-off-by", "Bob")
        self.assertEqual(t1, t2)
        self.assertNotEqual(t1, t3)


class TestParseTrailers(unittest.TestCase):
    """Tests for parse_trailers function."""

    def test_no_trailers(self) -> None:
        """Test parsing a message with no trailers."""
        message = b"Subject\n\nBody text\n"
        body, trailers = parse_trailers(message)
        self.assertEqual(body, b"Subject\n\nBody text\n")
        self.assertEqual(trailers, [])

    def test_simple_trailer(self) -> None:
        """Test parsing a message with a single trailer."""
        message = b"Subject\n\nBody text\n\nSigned-off-by: Alice <alice@example.com>\n"
        body, trailers = parse_trailers(message)
        self.assertEqual(body, b"Subject\n\nBody text\n")
        self.assertEqual(len(trailers), 1)
        self.assertEqual(trailers[0].key, "Signed-off-by")
        self.assertEqual(trailers[0].value, "Alice <alice@example.com>")

    def test_multiple_trailers(self) -> None:
        """Test parsing a message with multiple trailers."""
        message = b"Subject\n\nBody text\n\nSigned-off-by: Alice <alice@example.com>\nReviewed-by: Bob <bob@example.com>\n"
        body, trailers = parse_trailers(message)
        self.assertEqual(body, b"Subject\n\nBody text\n")
        self.assertEqual(len(trailers), 2)
        self.assertEqual(trailers[0].key, "Signed-off-by")
        self.assertEqual(trailers[0].value, "Alice <alice@example.com>")
        self.assertEqual(trailers[1].key, "Reviewed-by")
        self.assertEqual(trailers[1].value, "Bob <bob@example.com>")

    def test_trailer_with_multiline_value(self) -> None:
        """Test parsing a trailer with multiline value."""
        message = b"Subject\n\nBody\n\nTrailer: line1\n line2\n line3\n"
        _body, trailers = parse_trailers(message)
        self.assertEqual(len(trailers), 1)
        self.assertEqual(trailers[0].key, "Trailer")
        self.assertEqual(trailers[0].value, "line1 line2 line3")

    def test_no_blank_line_before_trailer(self) -> None:
        """Test that trailers without preceding blank line are not parsed."""
        message = b"Subject\nBody\nSigned-off-by: Alice\n"
        body, trailers = parse_trailers(message)
        self.assertEqual(body, message)
        self.assertEqual(trailers, [])

    def test_trailer_at_end_only(self) -> None:
        """Test that trailers must be at the end of the message."""
        message = b"Subject\n\nSigned-off-by: Alice\n\nMore body text\n"
        body, trailers = parse_trailers(message)
        # The "Signed-off-by" is not at the end, so it shouldn't be parsed as a trailer
        self.assertEqual(body, message)
        self.assertEqual(trailers, [])

    def test_different_separators(self) -> None:
        """Test parsing trailers with different separators."""
        message = b"Subject\n\nBody\n\nKey= value\n"
        _body, trailers = parse_trailers(message, separators="=")
        self.assertEqual(len(trailers), 1)
        self.assertEqual(trailers[0].key, "Key")
        self.assertEqual(trailers[0].value, "value")
        self.assertEqual(trailers[0].separator, "=")

    def test_empty_message(self) -> None:
        """Test parsing an empty message."""
        body, trailers = parse_trailers(b"")
        self.assertEqual(body, b"")
        self.assertEqual(trailers, [])


class TestFormatTrailers(unittest.TestCase):
    """Tests for format_trailers function."""

    def test_empty_list(self) -> None:
        """Test formatting an empty list of trailers."""
        result = format_trailers([])
        self.assertEqual(result, b"")

    def test_single_trailer(self) -> None:
        """Test formatting a single trailer."""
        trailers = [Trailer("Signed-off-by", "Alice <alice@example.com>")]
        result = format_trailers(trailers)
        self.assertEqual(result, b"Signed-off-by: Alice <alice@example.com>\n")

    def test_multiple_trailers(self) -> None:
        """Test formatting multiple trailers."""
        trailers = [
            Trailer("Signed-off-by", "Alice <alice@example.com>"),
            Trailer("Reviewed-by", "Bob <bob@example.com>"),
        ]
        result = format_trailers(trailers)
        expected = b"Signed-off-by: Alice <alice@example.com>\nReviewed-by: Bob <bob@example.com>\n"
        self.assertEqual(result, expected)


class TestAddTrailerToMessage(unittest.TestCase):
    """Tests for add_trailer_to_message function."""

    def test_add_to_empty_message(self) -> None:
        """Test adding a trailer to an empty message."""
        message = b""
        result = add_trailer_to_message(message, "Signed-off-by", "Alice")
        # Empty messages should get a trailer added
        self.assertIn(b"Signed-off-by: Alice", result)

    def test_add_to_message_without_trailers(self) -> None:
        """Test adding a trailer to a message without existing trailers."""
        message = b"Subject\n\nBody text\n"
        result = add_trailer_to_message(message, "Signed-off-by", "Alice")
        expected = b"Subject\n\nBody text\n\nSigned-off-by: Alice\n"
        self.assertEqual(result, expected)

    def test_add_to_message_with_existing_trailers(self) -> None:
        """Test adding a trailer to a message with existing trailers."""
        message = b"Subject\n\nBody\n\nSigned-off-by: Alice\n"
        result = add_trailer_to_message(message, "Reviewed-by", "Bob")
        self.assertIn(b"Signed-off-by: Alice", result)
        self.assertIn(b"Reviewed-by: Bob", result)

    def test_add_duplicate_trailer_default(self) -> None:
        """Test adding a duplicate trailer with default if_exists."""
        message = b"Subject\n\nBody\n\nSigned-off-by: Alice\n"
        result = add_trailer_to_message(
            message, "Signed-off-by", "Alice", if_exists="addIfDifferentNeighbor"
        )
        # Should not add duplicate
        self.assertEqual(result, message)

    def test_add_duplicate_trailer_add(self) -> None:
        """Test adding a duplicate trailer with if_exists=add."""
        message = b"Subject\n\nBody\n\nSigned-off-by: Alice\n"
        result = add_trailer_to_message(
            message, "Signed-off-by", "Alice", if_exists="add"
        )
        # Should add duplicate
        self.assertEqual(result.count(b"Signed-off-by: Alice"), 2)

    def test_add_different_value(self) -> None:
        """Test adding a trailer with same key but different value."""
        message = b"Subject\n\nBody\n\nSigned-off-by: Alice\n"
        result = add_trailer_to_message(message, "Signed-off-by", "Bob")
        self.assertIn(b"Signed-off-by: Alice", result)
        self.assertIn(b"Signed-off-by: Bob", result)

    def test_replace_existing(self) -> None:
        """Test replacing existing trailers with if_exists=replace."""
        message = b"Subject\n\nBody\n\nSigned-off-by: Alice\nSigned-off-by: Bob\n"
        result = add_trailer_to_message(
            message, "Signed-off-by", "Charlie", if_exists="replace"
        )
        self.assertNotIn(b"Alice", result)
        self.assertNotIn(b"Bob", result)
        self.assertIn(b"Signed-off-by: Charlie", result)

    def test_do_nothing_if_exists(self) -> None:
        """Test if_exists=doNothing."""
        message = b"Subject\n\nBody\n\nSigned-off-by: Alice\n"
        result = add_trailer_to_message(
            message, "Signed-off-by", "Bob", if_exists="doNothing"
        )
        # Should not modify the message
        self.assertEqual(result, message)

    def test_if_missing_do_nothing(self) -> None:
        """Test if_missing=doNothing."""
        message = b"Subject\n\nBody\n"
        result = add_trailer_to_message(
            message, "Signed-off-by", "Alice", if_missing="doNothing"
        )
        # Should not add the trailer
        self.assertNotIn(b"Signed-off-by", result)

    def test_where_start(self) -> None:
        """Test adding trailer at start."""
        message = b"Subject\n\nBody\n\nReviewed-by: Bob\n"
        result = add_trailer_to_message(
            message, "Signed-off-by", "Alice", where="start"
        )
        # Parse to check order
        _, trailers = parse_trailers(result)
        self.assertEqual(len(trailers), 2)
        self.assertEqual(trailers[0].key, "Signed-off-by")
        self.assertEqual(trailers[1].key, "Reviewed-by")

    def test_custom_separator(self) -> None:
        """Test adding trailer with custom separator."""
        message = b"Subject\n\nBody\n"
        result = add_trailer_to_message(message, "Key", "value", separator="=")
        self.assertIn(b"Key= value", result)


class TestIntegration(unittest.TestCase):
    """Integration tests for trailers."""

    def test_parse_and_format_roundtrip(self) -> None:
        """Test that parse and format are inverse operations."""
        original = b"Subject\n\nBody\n\nSigned-off-by: Alice\nReviewed-by: Bob\n"
        body, trailers = parse_trailers(original)
        formatted = body
        if body and not body.endswith(b"\n"):
            formatted += b"\n"
        if trailers:
            formatted += b"\n"
            formatted += format_trailers(trailers)
        self.assertEqual(formatted, original)

    def test_add_multiple_trailers(self) -> None:
        """Test adding multiple trailers in sequence."""
        message = b"Subject\n\nBody\n"
        message = add_trailer_to_message(message, "Signed-off-by", "Alice")
        message = add_trailer_to_message(message, "Reviewed-by", "Bob")
        message = add_trailer_to_message(message, "Tested-by", "Charlie")

        _, trailers = parse_trailers(message)
        self.assertEqual(len(trailers), 3)
        self.assertEqual(trailers[0].key, "Signed-off-by")
        self.assertEqual(trailers[1].key, "Reviewed-by")
        self.assertEqual(trailers[2].key, "Tested-by")
