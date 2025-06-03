# varint.py -- Variable-width integer encoding/decoding
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

"""Variable-width integer encoding/decoding.

This format is used in multiple places in Git:
- Git index file format version 4 for path compression
- Git pack files for OFS_DELTA entries
- Git reftable format for various fields
"""

from typing import BinaryIO, Optional


def encode_varint(value: int) -> bytes:
    """Encode an integer using variable-width encoding.

    Same format as used for OFS_DELTA pack entries and index v4 path compression.
    Uses 7 bits per byte, with the high bit indicating continuation.

    Args:
      value: Integer to encode
    Returns:
      Encoded bytes
    """
    if value == 0:
        return b"\x00"

    result = []
    while value > 0:
        byte = value & 0x7F  # Take lower 7 bits
        value >>= 7
        if value > 0:
            byte |= 0x80  # Set continuation bit
        result.append(byte)

    return bytes(result)


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Decode a variable-width encoded integer from bytes.

    Args:
      data: Bytes to decode from
      offset: Starting offset in data
    Returns:
      tuple of (decoded_value, new_offset)
    """
    value = 0
    shift = 0
    pos = offset

    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):  # No continuation bit
            break

    return value, pos


def decode_varint_from_stream(stream: BinaryIO) -> Optional[int]:
    """Decode a variable-width encoded integer from a stream.

    Args:
      stream: Stream to read from
    Returns:
      Decoded integer, or None if end of stream
    """
    value = 0
    shift = 0

    while True:
        byte_data = stream.read(1)
        if not byte_data:
            return None  # End of stream

        byte = byte_data[0]
        value |= (byte & 0x7F) << shift
        shift += 7

        if not (byte & 0x80):  # No continuation bit
            break

    return value
