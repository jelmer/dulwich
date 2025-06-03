"""Tests for variable-width integer encoding/decoding."""

import unittest
from io import BytesIO

from dulwich.varint import (
    decode_varint,
    decode_varint_from_stream,
    encode_varint,
)


class TestVarint(unittest.TestCase):
    """Test variable-width integer encoding and decoding."""

    def test_encode_decode_basic(self):
        """Test basic varint encoding/decoding."""
        test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 65535, 65536]

        for value in test_values:
            encoded = encode_varint(value)
            decoded, new_offset = decode_varint(encoded, 0)
            self.assertEqual(value, decoded, f"Failed for value {value}")
            self.assertEqual(
                len(encoded), new_offset, f"Offset mismatch for value {value}"
            )

    def test_encode_decode_stream(self):
        """Test varint encoding/decoding with streams."""
        test_values = [0, 1, 127, 128, 255, 256, 16383, 16384, 65535, 65536]

        for value in test_values:
            encoded = encode_varint(value)
            stream = BytesIO(encoded)
            decoded = decode_varint_from_stream(stream)
            self.assertEqual(
                value, decoded, f"Failed for stream decode of value {value}"
            )

    def test_multiple_varints(self):
        """Test encoding/decoding multiple varints in sequence."""
        values = [42, 127, 128, 1000, 0]

        # Encode all values
        encoded_data = b""
        for value in values:
            encoded_data += encode_varint(value)

        # Decode all values using byte offset
        offset = 0
        decoded_values = []
        for _ in values:
            decoded, offset = decode_varint(encoded_data, offset)
            decoded_values.append(decoded)

        self.assertEqual(values, decoded_values)

        # Decode all values using stream
        stream = BytesIO(encoded_data)
        decoded_values_stream = []
        for _ in values:
            decoded = decode_varint_from_stream(stream)
            self.assertIsNotNone(decoded)
            decoded_values_stream.append(decoded)

        self.assertEqual(values, decoded_values_stream)

    def test_stream_end_of_data(self):
        """Test that stream decode returns None at end of data."""
        stream = BytesIO(b"")
        result = decode_varint_from_stream(stream)
        self.assertIsNone(result)

        # Test with some data followed by end
        stream = BytesIO(encode_varint(42))
        result1 = decode_varint_from_stream(stream)
        self.assertEqual(42, result1)

        result2 = decode_varint_from_stream(stream)
        self.assertIsNone(result2)

    def test_specific_encoding_values(self):
        """Test specific encoding patterns."""
        # Single byte values (0-127)
        for i in range(128):
            encoded = encode_varint(i)
            self.assertEqual(1, len(encoded))
            self.assertEqual(i, encoded[0])

        # Two byte values (128-16383)
        encoded_128 = encode_varint(128)
        self.assertEqual(b"\x80\x01", encoded_128)

        encoded_255 = encode_varint(255)
        self.assertEqual(b"\xff\x01", encoded_255)

        encoded_256 = encode_varint(256)
        self.assertEqual(b"\x80\x02", encoded_256)

    def test_large_values(self):
        """Test encoding/decoding of large values."""
        large_values = [
            (1 << 7) - 1,  # 127
            (1 << 7),  # 128
            (1 << 14) - 1,  # 16383
            (1 << 14),  # 16384
            (1 << 21) - 1,  # 2097151
            (1 << 21),  # 2097152
            (1 << 28) - 1,  # 268435455
            (1 << 28),  # 268435456
        ]

        for value in large_values:
            encoded = encode_varint(value)
            decoded, _ = decode_varint(encoded, 0)
            self.assertEqual(value, decoded, f"Failed for large value {value}")


if __name__ == "__main__":
    unittest.main()
