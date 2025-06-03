"""Implementation of the reftable refs storage format.

The reftable format is a binary format for storing Git refs that provides
better performance and atomic operations compared to the traditional
loose refs format.

See: https://git-scm.com/docs/reftable
"""

import os
import random
import shutil
import struct
import time
import zlib
from dataclasses import dataclass
from io import BytesIO
from typing import BinaryIO, Optional, Union

from dulwich.objects import ObjectID
from dulwich.refs import (
    SYMREF,
    RefsContainer,
)


def decode_varint_from_stream(stream: BinaryIO) -> Optional[int]:
    """Decode a variable-length integer from a stream."""
    result = 0
    shift = 0
    while True:
        byte = stream.read(1)
        if not byte:
            return None
        byte_val = byte[0]
        result |= (byte_val & 0x7F) << shift
        if byte_val < 0x80:
            break
        shift += 7
    return result


def encode_varint(value: int) -> bytes:
    """Encode an integer as a variable-width integer."""
    result = []
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _encode_reftable_suffix_and_type(value: int) -> bytes:
    """Encode suffix_and_type using Git-compatible format.

    Git uses an additive format instead of proper LEB128:
    - Values < 128: Single byte (standard)
    - Values >= 128: Two bytes where byte1 + byte2 = value
    """
    if value < 128:
        return bytes([value])
    # Git's broken format: split into two bytes that add up to the value
    return bytes([0x80, value - 0x80])


def _decode_reftable_suffix_and_type(stream: BinaryIO) -> Optional[int]:
    """Decode suffix_and_type handling both Git's broken and standard formats."""
    pos = stream.tell()
    first_byte_data = stream.read(1)
    if not first_byte_data:
        return None

    first_byte = first_byte_data[0]

    # Single byte case - most common path
    if not (first_byte & 0x80):
        return first_byte

    # Two byte case - handle missing second byte
    second_byte_data = stream.read(1)
    if not second_byte_data:
        stream.seek(pos)
        return first_byte & 0x7F

    second_byte = second_byte_data[0]

    # Multi-byte varint case - delegate to proper decoder
    if second_byte & 0x80:
        stream.seek(pos)
        return decode_varint_from_stream(stream)

    # Two-byte case: choose between Git's format and standard LEB128
    git_value = first_byte + second_byte
    git_suffix_len = git_value >> 3
    git_value_type = git_value & 7

    # Use Git's format if it produces reasonable values
    if git_suffix_len < MAX_REASONABLE_SUFFIX_LEN and git_value_type <= 3:
        return git_value

    # Fall back to standard LEB128
    return (first_byte & 0x7F) | ((second_byte & 0x7F) << 7)


# Reftable magic bytes
REFTABLE_MAGIC = b"REFT"

# Reftable version
REFTABLE_VERSION = 1

# Constants - extracted magic numbers
EMBEDDED_FOOTER_MARKER = 0x1C  # 28 - version 2 header size
HEADER_SIZE_V1 = 24
DEFAULT_BLOCK_SIZE = 4096
MAX_SYMREF_DEPTH = 5
CRC_DATA_SIZE = 64
FINAL_PADDING_SIZE = 36

# SHA1 and size constants
SHA1_BINARY_SIZE = 20  # SHA1 is 20 bytes
SHA1_HEX_SIZE = 40  # SHA1 as hex string is 40 characters
SHA1_PEELED_HEX_SIZE = 80  # Peeled ref is 80 hex characters (2 SHA1s)

# Validation limits
MAX_REASONABLE_SUFFIX_LEN = 100
MAX_REASONABLE_BLOCK_SIZE = 1000000
MIN_RECORD_SIZE = 3

# Block types
BLOCK_TYPE_REF = b"r"
BLOCK_TYPE_OBJ = b"o"
BLOCK_TYPE_LOG = b"g"
BLOCK_TYPE_INDEX = b"i"

# Value types for ref records (matching Git's format)
REF_VALUE_DELETE = 0x0  # deletion
REF_VALUE_REF = 0x1  # one object name
REF_VALUE_PEELED = 0x2  # two object names (ref + peeled)
REF_VALUE_SYMREF = 0x3  # symbolic reference


def _encode_ref_value(value_type: int, value: bytes) -> bytes:
    """Encode a ref value based on its type."""
    if value_type == REF_VALUE_DELETE:
        return b""
    elif value_type == REF_VALUE_REF:
        # Convert hex string to binary if needed
        if len(value) == SHA1_HEX_SIZE:
            return bytes.fromhex(value.decode())
        else:
            return value
    elif value_type == REF_VALUE_PEELED:
        # Convert hex strings to binary if needed
        if len(value) == SHA1_PEELED_HEX_SIZE:
            return bytes.fromhex(value.decode())
        else:
            return value
    elif value_type == REF_VALUE_SYMREF:
        # varint(target_len) + target
        return encode_varint(len(value)) + value
    else:
        raise ValueError(f"Unknown ref value type: {value_type}")


def _decode_ref_value(stream: BinaryIO, value_type: int) -> bytes:
    """Decode a ref value from stream based on its type."""
    if value_type == REF_VALUE_DELETE:
        return b""
    elif value_type == REF_VALUE_REF:
        value = stream.read(SHA1_BINARY_SIZE)
        if len(value) != SHA1_BINARY_SIZE:
            raise ValueError("Unexpected end of stream while reading ref value")
        # Convert to hex string for interface compatibility
        return value.hex().encode()
    elif value_type == REF_VALUE_PEELED:
        value = stream.read(SHA1_BINARY_SIZE * 2)
        if len(value) != SHA1_BINARY_SIZE * 2:
            raise ValueError("Unexpected end of stream while reading peeled value")
        # Convert to hex string for interface compatibility
        return value.hex().encode()
    elif value_type == REF_VALUE_SYMREF:
        target_len = decode_varint_from_stream(stream)
        if target_len is None:
            raise ValueError("Unexpected end of stream while reading target length")
        value = stream.read(target_len)
        if len(value) != target_len:
            raise ValueError("Unexpected end of stream while reading symref target")
        return value
    else:
        raise ValueError(f"Unknown ref value type: {value_type}")


@dataclass
class RefValue:
    """A reference value with its type."""

    value_type: int
    value: bytes

    @property
    def is_symbolic(self) -> bool:
        """Check if this is a symbolic reference."""
        return self.value_type == REF_VALUE_SYMREF

    @property
    def is_deletion(self) -> bool:
        """Check if this is a deletion."""
        return self.value_type == REF_VALUE_DELETE

    @property
    def is_peeled(self) -> bool:
        """Check if this is a peeled reference."""
        return self.value_type == REF_VALUE_PEELED

    def get_sha(self) -> Optional[bytes]:
        """Get the SHA1 value (for regular or peeled refs)."""
        if self.value_type == REF_VALUE_REF:
            return self.value
        elif self.value_type == REF_VALUE_PEELED:
            return self.value[:SHA1_HEX_SIZE]
        return None


class RefUpdate:
    """A reference update operation."""

    def __init__(self, name: bytes, value_type: int, value: bytes):
        self.name = name
        self.value_type = value_type
        self.value = value


class RefRecord:
    """A reference record in a reftable."""

    def __init__(
        self, refname: bytes, value_type: int, value: bytes, update_index: int = 0
    ):
        self.refname = refname
        self.value_type = value_type
        self.value = value
        self.update_index = update_index

    def encode(self, prefix: bytes = b"", min_update_index: int = 0) -> bytes:
        """Encode the ref record with prefix compression."""
        common_prefix_len = 0
        for i in range(min(len(prefix), len(self.refname))):
            if prefix[i] != self.refname[i]:
                break
            common_prefix_len += 1

        suffix = self.refname[common_prefix_len:]

        # Encode according to Git format:
        # varint(prefix_length)
        # varint((suffix_length << 3) | value_type)
        # suffix
        # varint(update_index_delta)
        # value?
        result = encode_varint(common_prefix_len)
        result += _encode_reftable_suffix_and_type((len(suffix) << 3) | self.value_type)
        result += suffix

        # Calculate update_index_delta
        update_index_delta = self.update_index - min_update_index
        result += encode_varint(update_index_delta)

        # Encode value based on type
        result += _encode_ref_value(self.value_type, self.value)

        return result

    @classmethod
    def decode(
        cls, stream: BinaryIO, prefix: bytes = b"", min_update_index: int = 0
    ) -> tuple["RefRecord", bytes]:
        """Decode a ref record from a stream. Returns (record, full_refname)."""
        # Read prefix length
        prefix_len = decode_varint_from_stream(stream)
        if prefix_len is None:
            raise ValueError("Unexpected end of stream while reading prefix length")

        # Read combined suffix_length and value_type
        suffix_and_type = _decode_reftable_suffix_and_type(stream)
        if suffix_and_type is None:
            raise ValueError("Unexpected end of stream while reading suffix_and_type")

        suffix_len = suffix_and_type >> 3
        value_type = suffix_and_type & 0x7

        # Read suffix
        suffix = stream.read(suffix_len)
        if len(suffix) != suffix_len:
            raise ValueError("Unexpected end of stream while reading suffix")

        refname = prefix[:prefix_len] + suffix

        # Read update_index_delta
        update_index_delta = decode_varint_from_stream(stream)
        if update_index_delta is None:
            raise ValueError(
                "Unexpected end of stream while reading update_index_delta"
            )

        update_index = min_update_index + update_index_delta

        # Read value based on type
        value = _decode_ref_value(stream, value_type)

        return cls(refname, value_type, value, update_index), refname


class LogBlock:
    """A block containing reflog records - simplified for empty blocks only."""

    def encode(self) -> bytes:
        """Encode empty log block for Git compatibility."""
        # Git expects compressed empty data for new repos
        return zlib.compress(b"")


class RefBlock:
    """A block containing reference records."""

    def __init__(self):
        self.refs = []

    def add_ref(
        self, refname: bytes, value_type: int, value: bytes, update_index: int = 1
    ):
        """Add a reference to the block."""
        self.refs.append(RefRecord(refname, value_type, value, update_index))

    def _create_embedded_footer(
        self, ref_offsets: list[int], header_data: bytes
    ) -> bytes:
        """Create the embedded footer pattern Git uses."""
        result = BytesIO()
        result.write(b"\x00\x00")  # padding

        if len(self.refs) == 1:
            # Single ref pattern
            result.write(bytes([EMBEDDED_FOOTER_MARKER]))
            result.write(b"\x00\x01")
        else:
            # Multiple refs pattern
            result.write(bytes([EMBEDDED_FOOTER_MARKER]))
            result.write(b"\x00\x00")  # first restart offset (always 0)

            # Special offset calculation: second_ref_offset + EMBEDDED_FOOTER_MARKER
            if len(ref_offsets) > 0:
                special_offset = ref_offsets[0] + EMBEDDED_FOOTER_MARKER
                result.write(struct.pack("B", special_offset))
            else:
                result.write(b"\x00")

            result.write(b"\x00")  # padding byte
            result.write(struct.pack("B", 2))  # restart count (Git always uses 2)

        # Add header copy
        if header_data and len(header_data) >= HEADER_SIZE_V1:
            result.write(header_data[:HEADER_SIZE_V1])
        else:
            # Create a minimal header copy
            result.write(REFTABLE_MAGIC)
            result.write(b"\x01\x00\x10\x00")  # version 1, block size 4096
            result.write(b"\x00" * 16)  # min/max update indices

        # Final padding
        result.write(b"\x00\x00\x00\x00")

        return result.getvalue()

    def encode(
        self,
        min_update_index: int = 0,
        embed_footer: bool = False,
        header_data: bytes = b"",
    ) -> bytes:
        """Encode the ref block with restart points for Git compatibility."""
        # Sort refs by name
        self.refs.sort(key=lambda r: r.refname)

        # Encode refs with prefix compression
        ref_data = BytesIO()
        prefix = b""
        ref_offsets = []

        for i, ref in enumerate(self.refs):
            # Record offset for restart points
            if i > 0:  # Git records offsets starting from second ref
                ref_offsets.append(ref_data.tell())

            encoded = ref.encode(prefix, min_update_index)
            ref_data.write(encoded)
            prefix = ref.refname

        record_data = ref_data.getvalue()

        # Git uses embedded footer format for ref blocks
        result = BytesIO()
        result.write(record_data)
        result.write(self._create_embedded_footer(ref_offsets, header_data))

        return result.getvalue()

    @classmethod
    def _find_ref_data_end(cls, data: bytes) -> int:
        """Find where ref records end in the data.

        Git embeds a footer at the end of ref blocks that contains offset
        information. This footer is marked by a special pattern (0x00 0x00 0x1c)
        where 0x1c (28) represents the version 2 header size.

        Args:
            data: The raw block data to search

        Returns:
            The position where ref records end and footer begins
        """
        # Look for embedded footer marker pattern
        marker_pattern = b"\x00\x00" + bytes([EMBEDDED_FOOTER_MARKER])
        marker_pos = data.find(marker_pattern)
        if marker_pos > 0:
            return marker_pos

        # Fallback: use most of the data, leaving room for footer
        return max(0, len(data) - 50)

    @classmethod
    def decode(cls, data: bytes, min_update_index: int = 0) -> "RefBlock":
        """Decode a ref block from bytes - simplified."""
        block = cls()
        if not data:
            return block

        # Find where ref data ends (before embedded footer)
        ref_data_end = cls._find_ref_data_end(data)
        record_data = data[:ref_data_end]

        stream = BytesIO(record_data)
        prefix = b""

        while stream.tell() < len(record_data):
            # Save position to check for progress
            pos_before = stream.tell()

            # Check if we have enough data for a minimal record
            remaining = len(record_data) - stream.tell()
            if (
                remaining < MIN_RECORD_SIZE
            ):  # Need at least prefix_len + suffix_and_type + some data
                break

            try:
                ref, prefix = RefRecord.decode(stream, prefix, min_update_index)
                # Skip metadata records (empty refnames are Git's internal index records)
                if ref.refname:  # Only add non-empty refnames
                    block.refs.append(ref)
            except ValueError:
                # If we can't decode a record, we might have hit padding or invalid data
                # Stop parsing rather than raising an error
                break

            # Ensure we made progress to avoid infinite loops
            if stream.tell() == pos_before:
                break

        return block


class ReftableWriter:
    """Writer for reftable files."""

    def __init__(
        self,
        f: BinaryIO,
        auto_create_head: bool = True,
        is_batch_operation: bool = False,
    ):
        self.f = f
        self.refs: dict[bytes, tuple[int, bytes]] = {}
        self.refs_order: list[bytes] = []  # Track insertion order for update indices
        self._written_data: list[bytes] = []  # Track written data for CRC calculation
        self.min_update_index = 1
        self.max_update_index = 1
        self.auto_create_head = auto_create_head
        self.is_batch_operation = (
            is_batch_operation  # Track if this is a batch operation
        )

    def add_ref(self, refname: bytes, sha: bytes):
        """Add a direct reference."""
        self.refs[refname] = (REF_VALUE_REF, sha)
        if refname not in self.refs_order:
            self.refs_order.append(refname)

        self._maybe_auto_create_head()

    def add_symbolic_ref(self, refname: bytes, target: bytes):
        """Add a symbolic reference."""
        self.refs[refname] = (REF_VALUE_SYMREF, target)
        if refname not in self.refs_order:
            # HEAD should always be first (like git does)
            if refname == b"HEAD":
                self.refs_order.insert(0, refname)
            else:
                self.refs_order.append(refname)
        else:
            # Update existing ref (e.g., if HEAD was auto-created and now explicitly set)
            pass

    def delete_ref(self, refname: bytes):
        """Mark a reference as deleted."""
        self.refs[refname] = (REF_VALUE_DELETE, b"")
        if refname not in self.refs_order:
            self.refs_order.append(refname)

    def _maybe_auto_create_head(self):
        """Auto-create HEAD -> refs/heads/master if needed (Git compatibility)."""
        if self.auto_create_head and b"HEAD" not in self.refs:
            # Git always creates HEAD -> refs/heads/master by default
            self.refs[b"HEAD"] = (REF_VALUE_SYMREF, b"refs/heads/master")
            self.refs_order.insert(0, b"HEAD")

    def write(self):
        """Write the reftable to the file."""
        # Skip recalculation if max_update_index was already set higher than default
        # This preserves Git's behavior for symbolic-ref operations
        # Also skip for batch operations where min == max by design
        if (
            not self.is_batch_operation
            and self.max_update_index <= self.min_update_index
            and self.refs_order
        ):
            # Calculate max_update_index based on actual number of refs
            self.max_update_index = self.min_update_index + len(self.refs_order) - 1

        # Write header
        self._write_header()

        # Write ref blocks (includes embedded footer)
        self._write_ref_blocks()

        # Git embeds the footer within the ref block for small files,
        # so we only need to add final padding and CRC
        self._write_final_padding()

    def _write_header(self):
        """Write the reftable header."""
        # Magic bytes
        header_data = REFTABLE_MAGIC

        # Version + block size (4 bytes total, big-endian network order)
        # Format: uint8(version) + uint24(block_size)
        version_and_blocksize = (REFTABLE_VERSION << 24) | (
            DEFAULT_BLOCK_SIZE & 0xFFFFFF
        )
        header_data += struct.pack(">I", version_and_blocksize)

        # Min/max update index (timestamps) (big-endian network order)
        # Git uses increasing sequence numbers for update indices
        header_data += struct.pack(">Q", self.min_update_index)  # min_update_index
        header_data += struct.pack(">Q", self.max_update_index)  # max_update_index

        # Store header for footer
        self.header_data = header_data
        self.f.write(header_data)
        self._written_data.append(header_data)

    def _get_ref_update_indices(self):
        """Get update indices for all refs based on operation type.

        In batch operations, all refs get the same update index (timestamp).
        In non-batch operations, refs are assigned sequential indices starting
        from min_update_index.

        Returns:
            dict[bytes, int]: Mapping of ref names to their update indices
        """
        if self.is_batch_operation:
            # All refs get same index in batch
            return {name: self.min_update_index for name in self.refs_order}
        elif hasattr(self, "_ref_update_indices"):
            # Use provided indices
            return self._ref_update_indices
        elif len(self.refs_order) == 1 and self.refs_order[0] == b"HEAD":
            # Special case for single HEAD symbolic ref
            value_type, _ = self.refs[b"HEAD"]
            if value_type == REF_VALUE_SYMREF:
                return {b"HEAD": self.max_update_index}
            else:
                return {b"HEAD": self.min_update_index}
        else:
            # Sequential indices
            indices = {}
            for i, name in enumerate(self.refs_order):
                indices[name] = self.min_update_index + i
            return indices

    def _write_ref_blocks(self):
        """Write reference blocks."""
        # Only write block if we have refs
        if not self.refs:
            return

        # Write refs in insertion order to preserve update indices like Git
        block = RefBlock()

        # Get update indices for all refs
        update_indices = self._get_ref_update_indices()

        # Add refs to block with their update indices
        for refname in self.refs_order:
            value_type, value = self.refs[refname]
            update_index = update_indices[refname]
            block.add_ref(refname, value_type, value, update_index)

        # Generate block data (may use embedded footer for small blocks)
        block_data = block.encode(
            min_update_index=self.min_update_index, header_data=self.header_data
        )

        # Write block type
        block_header = BLOCK_TYPE_REF
        self.f.write(block_header)
        self._written_data.append(block_header)

        # Write block length (3 bytes, big-endian network order)
        length_bytes = len(block_data).to_bytes(3, "big")
        self.f.write(length_bytes)
        self._written_data.append(length_bytes)

        # Write block data
        self.f.write(block_data)
        self._written_data.append(block_data)

    def _write_final_padding(self):
        """Write final padding and CRC for Git compatibility."""
        # Git writes exactly 40 bytes after the ref block (which includes embedded footer)
        # This is 36 bytes of zeros followed by 4-byte CRC
        padding = b"\x00" * FINAL_PADDING_SIZE
        self.f.write(padding)
        self._written_data.append(padding)

        # Calculate CRC over the last 64 bytes before CRC position
        # Collect all data written so far
        all_data = b"".join(self._written_data)

        # CRC is calculated over the 64 bytes before the CRC itself
        if len(all_data) >= CRC_DATA_SIZE:
            crc_data = all_data[-CRC_DATA_SIZE:]
        else:
            # Pad with zeros if file is too small
            crc_data = all_data + b"\x00" * (CRC_DATA_SIZE - len(all_data))

        crc = zlib.crc32(crc_data) & 0xFFFFFFFF
        self.f.write(struct.pack(">I", crc))


class ReftableReader:
    """Reader for reftable files."""

    def __init__(self, f: BinaryIO):
        self.f = f
        self._read_header()
        self.refs: dict[bytes, tuple[int, bytes]] = {}
        self._read_blocks()

    def _read_header(self):
        """Read and validate the reftable header."""
        # Read magic bytes
        magic = self.f.read(4)
        if magic != REFTABLE_MAGIC:
            raise ValueError(f"Invalid reftable magic: {magic}")

        # Read version + block size (4 bytes total, big-endian network order)
        # Format: uint8(version) + uint24(block_size)
        version_and_blocksize = struct.unpack(">I", self.f.read(4))[0]
        version = (version_and_blocksize >> 24) & 0xFF  # First byte
        block_size = version_and_blocksize & 0xFFFFFF  # Last 3 bytes

        if version != REFTABLE_VERSION:
            raise ValueError(f"Unsupported reftable version: {version}")

        self.block_size = block_size

        # Read min/max update index (big-endian network order)
        self.min_update_index = struct.unpack(">Q", self.f.read(8))[0]
        self.max_update_index = struct.unpack(">Q", self.f.read(8))[0]

    def _read_blocks(self):
        """Read all blocks from the reftable."""
        while True:
            # Read block type
            block_type = self.f.read(1)
            if not block_type:
                break

            # Read block length (3 bytes, big-endian network order)
            length_bytes = self.f.read(3)
            if len(length_bytes) < 3:
                break
            # Convert 3-byte big-endian to int
            block_length = int.from_bytes(length_bytes, "big")

            # Read block data
            block_data = self.f.read(block_length)

            if block_type == BLOCK_TYPE_REF:
                self._process_ref_block(block_data)
            # TODO: Handle other block types

            # Stop if we encounter footer header copy
            if (
                block_type == b"R" and block_length > MAX_REASONABLE_BLOCK_SIZE
            ):  # Likely parsing footer as block
                break

    def _process_ref_block(self, data: bytes):
        """Process a reference block."""
        block = RefBlock.decode(data, min_update_index=self.min_update_index)
        for ref in block.refs:
            # Store all refs including deletion records - deletion handling is done at container level
            self.refs[ref.refname] = (ref.value_type, ref.value)

    def get_ref(self, refname: bytes) -> Optional[tuple[int, bytes]]:
        """Get a reference by name."""
        return self.refs.get(refname)

    def all_refs(self) -> dict[bytes, tuple[int, bytes]]:
        """Get all references."""
        return self.refs.copy()


class _ReftableBatchContext:
    """Context manager for batching reftable updates."""

    def __init__(self, refs_container):
        self.refs_container = refs_container

    def __enter__(self):
        self.refs_container._batch_mode = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.refs_container._batch_mode = False
        if exc_type is None:  # Only flush if no exception occurred
            self.refs_container._flush_pending_updates()


class ReftableRefsContainer(RefsContainer):
    """A refs container backed by the reftable format."""

    def __init__(self, path: Union[str, bytes], logger=None):
        """Initialize a reftable refs container.

        Args:
            path: Path to the reftable directory
            logger: Optional logger for reflog
        """
        super().__init__(logger=logger)
        # Normalize path to string
        if isinstance(path, bytes):
            self.path = os.fsdecode(path)
        else:
            self.path = path
        self.reftable_dir = os.path.join(self.path, "reftable")
        if not os.path.exists(self.reftable_dir):
            os.makedirs(self.reftable_dir)
        self._pending_updates: list[RefUpdate] = []  # Buffer for batching ref updates
        self._ref_update_indices: dict[
            bytes, int
        ] = {}  # Track chronological update index for each ref

        # Create refs/heads marker file for Git compatibility
        self._ensure_refs_heads_marker()

    def _ensure_refs_heads_marker(self):
        """Ensure refs/heads marker file exists for Git compatibility.

        Git expects a refs/heads file (not directory) to exist when using
        reftables. This file contains a marker message indicating that the
        repository uses the reftable format instead of loose refs.
        """
        refs_dir = os.path.join(self.path, "refs")
        if not os.path.exists(refs_dir):
            os.makedirs(refs_dir)

        refs_heads_path = os.path.join(refs_dir, "heads")

        # If refs/heads is a directory, remove it
        if os.path.isdir(refs_heads_path):
            shutil.rmtree(refs_heads_path)

        # Create marker file if it doesn't exist
        if not os.path.exists(refs_heads_path):
            with open(refs_heads_path, "wb") as f:
                f.write(b"this repository uses the reftable format\n")

    def _read_table_file(self, table_file: str):
        """Context manager helper to open and read a reftable file."""
        return open(table_file, "rb")

    def _load_ref_update_indices(self):
        """Load the update indices for all refs from existing reftable files."""
        for table_file in self._get_table_files():
            with self._read_table_file(table_file) as f:
                # Read header to get min_update_index
                f.seek(0)
                reader = ReftableReader(f)

                # Read the ref block to get individual update indices
                f.seek(HEADER_SIZE_V1)  # Skip header
                block_type = f.read(1)
                if block_type == BLOCK_TYPE_REF:
                    length_bytes = f.read(3)
                    block_length = (
                        (length_bytes[0] << 16)
                        | (length_bytes[1] << 8)
                        | length_bytes[2]
                    )
                    block_data = f.read(block_length)

                    # Parse the block to get refs with their update indices
                    block = RefBlock.decode(
                        block_data, min_update_index=reader.min_update_index
                    )
                    for ref in block.refs:
                        # Store the update index for each ref
                        # This preserves the chronological order
                        self._ref_update_indices[ref.refname] = ref.update_index

    def _get_next_update_index(self) -> int:
        """Get the next available update index across all tables."""
        # Find the highest update index used so far by reading all tables
        max_index = 0
        table_files = self._get_table_files()

        if not table_files:
            # No existing tables, but Git starts with HEAD at index 1
            # Our first operations start at index 2
            return 2

        for table_file in table_files:
            with self._read_table_file(table_file) as f:
                reader = ReftableReader(f)
                max_index = max(max_index, reader.max_update_index)

        return max_index + 1

    def _get_table_files(self) -> list[str]:
        """Get sorted list of reftable files from tables.list."""
        if not os.path.exists(self.reftable_dir):
            return []

        tables_list_path = os.path.join(self.reftable_dir, "tables.list")
        if not os.path.exists(tables_list_path):
            return []

        files = []
        with open(tables_list_path, "rb") as f:
            for line in f:
                table_name = line.decode().strip()
                if table_name:
                    files.append(os.path.join(self.reftable_dir, table_name))
        return files

    def _read_all_tables(self) -> dict[bytes, tuple[int, bytes]]:
        """Read all reftable files and merge results."""
        # First, read all tables and sort them by min_update_index
        table_data = []
        for table_file in self._get_table_files():
            with self._read_table_file(table_file) as f:
                reader = ReftableReader(f)
                table_data.append(
                    (reader.min_update_index, table_file, reader.all_refs())
                )

        # Sort by min_update_index to ensure chronological order
        table_data.sort(key=lambda x: x[0])

        # Merge results in chronological order
        all_refs: dict[bytes, tuple[int, bytes]] = {}
        for min_update_index, table_file, refs in table_data:
            # Apply updates from this table
            for refname, (value_type, value) in refs.items():
                if value_type == REF_VALUE_DELETE:
                    # Remove ref if it exists
                    all_refs.pop(refname, None)
                else:
                    # Add/update ref
                    all_refs[refname] = (value_type, value)
        return all_refs

    def allkeys(self):
        """Return set of all ref names."""
        refs = self._read_all_tables()
        result = set(refs.keys())

        # For symbolic refs, also include their targets as implicit refs
        for refname, (value_type, value) in refs.items():
            if value_type == REF_VALUE_SYMREF:
                # Add the target ref as an implicit ref
                target = value
                result.add(target)

        return result

    def follow(self, name: bytes) -> tuple[list[bytes], bytes]:
        """Follow a reference name.

        Returns: a tuple of (refnames, sha), where refnames are the names of
            references in the chain
        """
        refnames = []
        current = name
        refs = self._read_all_tables()

        for _ in range(MAX_SYMREF_DEPTH):
            refnames.append(current)
            ref_data = refs.get(current)
            if ref_data is None:
                raise KeyError(current)

            value_type, value = ref_data
            if value_type == REF_VALUE_REF:
                return refnames, value
            if value_type == REF_VALUE_PEELED:
                return refnames, value[:SHA1_HEX_SIZE]  # First SHA1 hex chars
            if value_type == REF_VALUE_SYMREF:
                current = value
                continue

            # Unknown value type
            raise ValueError(f"Unknown ref value type: {value_type}")

        # Too many levels of indirection
        raise ValueError(f"Too many levels of symbolic ref indirection for {name!r}")

    def __getitem__(self, name: bytes) -> ObjectID:
        """Get the SHA1 for a reference name.

        This method follows all symbolic references.
        """
        _, sha = self.follow(name)
        if sha is None:
            raise KeyError(name)
        return sha

    def read_loose_ref(self, name: bytes) -> bytes:
        """Read a reference value without following symbolic refs.

        Args:
            name: the refname to read
        Returns: The contents of the ref file.
                 For symbolic refs, returns b"ref: <target>"
        Raises:
            KeyError: if the ref does not exist
        """
        refs = self._read_all_tables()
        ref_data = refs.get(name)

        if ref_data is None:
            raise KeyError(name)

        value_type, value = ref_data
        if value_type == REF_VALUE_REF:
            return value
        elif value_type == REF_VALUE_SYMREF:
            # Return in Git format: "ref: <target>"
            return SYMREF + value
        elif value_type == REF_VALUE_PEELED:
            # Return the first SHA (not the peeled one)
            return value[:SHA1_HEX_SIZE]  # First SHA1 hex chars

        raise ValueError(f"Unknown ref value type: {value_type}")

    def get_packed_refs(self) -> dict[bytes, bytes]:
        """Get packed refs. Reftable doesn't distinguish packed/loose."""
        refs = self._read_all_tables()
        result = {}
        for name, (value_type, value) in refs.items():
            if value_type == REF_VALUE_REF:
                result[name] = value
            elif value_type == REF_VALUE_PEELED:
                result[name] = value[:SHA1_HEX_SIZE]  # First SHA1 hex chars
        return result

    def get_peeled(self, name: bytes) -> Optional[bytes]:
        """Return the cached peeled value of a ref, if available.

        Args:
            name: Name of the ref to peel
        Returns: The peeled value of the ref. If the ref is known not point to
            a tag, this will be the SHA the ref refers to. If the ref may point
            to a tag, but no cached information is available, None is returned.
        """
        refs = self._read_all_tables()
        ref_data = refs.get(name)

        if ref_data is None:
            return None

        value_type, value = ref_data
        if value_type == REF_VALUE_PEELED:
            # Return the peeled SHA (second 40 hex chars)
            return value[40:80]
        elif value_type == REF_VALUE_REF:
            # Known not to be peeled
            return value
        else:
            # Symbolic ref or other - no peeled info
            return None

    def _generate_table_path(
        self,
        min_update_index: Optional[int] = None,
        max_update_index: Optional[int] = None,
    ) -> str:
        """Generate a new reftable file path."""
        if min_update_index is None or max_update_index is None:
            timestamp = int(time.time() * 1000000)
            min_idx = max_idx = timestamp
        else:
            min_idx = min_update_index
            max_idx = max_update_index

        hash_part = random.randint(0, 0xFFFFFFFF)
        table_name = f"0x{min_idx:016x}-0x{max_idx:016x}-{hash_part:08x}.ref"
        return os.path.join(self.reftable_dir, table_name)

    def add_packed_refs(self, new_refs: dict[bytes, Optional[bytes]]):
        """Add packed refs. Creates a new reftable file with all refs consolidated."""
        if not new_refs:
            return

        self._write_batch_updates(new_refs)

    def _write_batch_updates(self, updates: dict[bytes, Optional[bytes]]):
        """Write multiple ref updates to a single reftable file."""
        if not updates:
            return

        table_path = self._generate_table_path()

        with open(table_path, "wb") as f:
            writer = ReftableWriter(f)
            for refname, sha in updates.items():
                if sha is not None:
                    writer.add_ref(refname, sha)
                else:
                    writer.delete_ref(refname)
            writer.write()

        self._update_tables_list()

    def set_if_equals(
        self,
        name,
        old_ref,
        new_ref,
        committer=None,
        timestamp=None,
        timezone=None,
        message=None,
    ) -> bool:
        """Atomically set a ref if it currently equals old_ref."""
        # For now, implement a simple non-atomic version
        # TODO: Implement proper atomic compare-and-swap
        try:
            current = self.read_loose_ref(name)
        except KeyError:
            current = None

        if current != old_ref:
            return False

        if new_ref is None:
            # Delete ref
            self._write_ref_update(name, REF_VALUE_DELETE, b"")
        else:
            # Update ref
            self._write_ref_update(name, REF_VALUE_REF, new_ref)

        return True

    def add_if_new(
        self, name, ref, committer=None, timestamp=None, timezone=None, message=None
    ) -> bool:
        """Add a ref only if it doesn't exist."""
        try:
            self.read_loose_ref(name)
            return False  # Ref exists
        except KeyError:
            pass  # Ref doesn't exist, continue
        self._write_ref_update(name, REF_VALUE_REF, ref)
        return True

    def remove_if_equals(
        self, name, old_ref, committer=None, timestamp=None, timezone=None, message=None
    ) -> bool:
        """Remove a ref if it equals old_ref."""
        return self.set_if_equals(
            name,
            old_ref,
            None,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )

    def set_symbolic_ref(
        self, name, other, committer=None, timestamp=None, timezone=None, message=None
    ):
        """Set a symbolic reference."""
        self._write_ref_update(name, REF_VALUE_SYMREF, other)

    def _write_ref_update(self, name: bytes, value_type: int, value: bytes):
        """Write a single ref update immediately to its own reftable file."""
        # Check if we're in batch mode - if so, buffer for later
        if getattr(self, "_batch_mode", False):
            # Buffer the update for later batching using RefUpdate objects
            self._pending_updates.append(RefUpdate(name, value_type, value))
            return

        # Write immediately like Git does - one file per update
        self._write_single_ref_update(name, value_type, value)

    def _write_single_ref_update(self, name: bytes, value_type: int, value: bytes):
        """Write a single ref update to its own reftable file like Git does."""
        table_path = self._generate_table_path()
        next_update_index = self._get_next_update_index()

        with open(table_path, "wb") as f:
            writer = ReftableWriter(f, auto_create_head=False)
            writer.min_update_index = next_update_index
            # Git uses max_update_index = min + 1 for single ref updates
            writer.max_update_index = next_update_index + 1

            # Don't auto-create HEAD - let it be set explicitly

            # Add the requested ref
            if value_type == REF_VALUE_REF:
                writer.add_ref(name, value)
            elif value_type == REF_VALUE_SYMREF:
                writer.add_symbolic_ref(name, value)
            elif value_type == REF_VALUE_DELETE:
                writer.delete_ref(name)

            writer.write()

        self._update_tables_list()

    def _flush_pending_updates(self):
        """Flush pending ref updates like Git does - consolidate all refs."""
        if not self._pending_updates:
            return

        # First, load the current update indices for all refs
        if not self._ref_update_indices:
            self._load_ref_update_indices()

        # Read all existing refs to create complete consolidated view
        all_refs = self._read_all_tables()

        # Process pending updates
        head_update, other_updates = self._process_pending_updates()

        # Get next update index - all refs in batch get the SAME index
        batch_update_index = self._get_next_update_index()

        # Apply updates to get final state
        self._apply_batch_updates(
            all_refs, other_updates, head_update, batch_update_index
        )

        # Write consolidated batch file
        created_files = (
            self._write_batch_file(all_refs, batch_update_index) if all_refs else []
        )

        # Update tables list with new files (don't compact, keep separate)
        if created_files:
            # Remove old files and update tables.list
            tables_list_path = os.path.join(self.reftable_dir, "tables.list")

            # Remove old .ref files
            for name in os.listdir(self.reftable_dir):
                if name.endswith(".ref") and name not in created_files:
                    os.remove(os.path.join(self.reftable_dir, name))

            # Write new tables.list with separate files
            with open(tables_list_path, "wb") as f:
                for filename in sorted(created_files):  # Sort for deterministic order
                    f.write((filename + "\n").encode())

        self._pending_updates.clear()

    def _process_pending_updates(self):
        """Process pending updates and return (head_update, other_updates)."""
        head_update = None
        other_updates = []

        # Process all RefUpdate objects
        for update in self._pending_updates:
            if update.name == b"HEAD":
                if update.value_type == REF_VALUE_SYMREF:
                    # Resolve symref chain like Git does (only for HEAD)
                    resolved_target = self._resolve_symref_chain(update.value)
                    head_update = (update.name, REF_VALUE_SYMREF, resolved_target)
                else:
                    head_update = (update.name, update.value_type, update.value)
            else:
                # Regular ref, symref, or deletion
                other_updates.append((update.name, update.value_type, update.value))

        return head_update, other_updates

    def _resolve_symref_chain(self, target: bytes) -> bytes:
        """Resolve a symbolic reference chain to its final target.

        Git limits symref chains to 5 levels to prevent infinite loops.
        This method follows the chain until it finds a non-symbolic ref
        or hits the depth limit.

        Args:
            target: The initial target of a symbolic reference

        Returns:
            The final target after resolving all symbolic references
        """
        visited = set()
        current = target

        # Follow the chain up to 5 levels deep (Git's limit)
        for _ in range(MAX_SYMREF_DEPTH):
            if current in visited:
                # Circular reference, return current
                break
            visited.add(current)

            # Check if current target is also a symref in pending updates
            found_symref = False
            for update in self._pending_updates:
                if update.name == current and update.value_type == REF_VALUE_SYMREF:
                    current = update.value
                    found_symref = True
                    break

            if not found_symref:
                # Not a symref, this is the final target
                break

        return current

    def _apply_batch_updates(
        self, all_refs, other_updates, head_update, batch_update_index
    ):
        """Apply batch updates to the refs dict and update indices."""
        # Process all updates and assign the SAME update index to all refs in batch
        for name, value_type, value in other_updates:
            if value_type == REF_VALUE_DELETE:
                all_refs.pop(name, None)
                # Deletion still gets recorded with the batch index
                self._ref_update_indices[name] = batch_update_index
            else:
                all_refs[name] = (value_type, value)
                # All refs in batch get the same update index
                self._ref_update_indices[name] = batch_update_index

        # Process HEAD update with same batch index
        if head_update:
            name, value_type, value = head_update
            if value_type == REF_VALUE_DELETE:
                all_refs.pop(name, None)
                self._ref_update_indices[name] = batch_update_index
            else:
                all_refs[name] = (value_type, value)
                self._ref_update_indices[name] = batch_update_index

    def _write_batch_file(self, all_refs, batch_update_index):
        """Write all refs to a single batch file and return created filenames."""
        # All refs in batch have same update index
        table_path = self._generate_table_path(batch_update_index, batch_update_index)
        with open(table_path, "wb") as f:
            writer = ReftableWriter(f, auto_create_head=False, is_batch_operation=True)
            writer.min_update_index = batch_update_index
            writer.max_update_index = batch_update_index

            # Add all refs in sorted order
            writer.refs_order = sorted(all_refs.keys())
            # Git typically puts HEAD first if present
            if b"HEAD" in writer.refs_order:
                writer.refs_order.remove(b"HEAD")
                writer.refs_order.insert(0, b"HEAD")

            for refname in writer.refs_order:
                value_type, value = all_refs[refname]
                writer.refs[refname] = (value_type, value)

            # Pass the update indices to the writer
            writer._ref_update_indices = {
                name: batch_update_index for name in all_refs.keys()
            }
            writer.write()

        return [os.path.basename(table_path)]

    def batch_update(self):
        """Context manager for batching multiple ref updates into a single reftable."""
        return _ReftableBatchContext(self)

    def remove_packed_ref(self, name: bytes):
        """Remove a packed ref. Creates a deletion record."""
        self._write_ref_update(name, REF_VALUE_DELETE, b"")

    def _compact_tables_list(self, new_table_name: str):
        """Compact tables list to single file like Git does."""
        tables_list_path = os.path.join(self.reftable_dir, "tables.list")

        # Remove old .ref files (Git's compaction behavior)
        for name in os.listdir(self.reftable_dir):
            if name.endswith(".ref") and name != new_table_name:
                os.remove(os.path.join(self.reftable_dir, name))

        # Write new tables.list with just the consolidated file
        with open(tables_list_path, "wb") as f:
            f.write((new_table_name + "\n").encode())

    def _update_tables_list(self):
        """Update the tables.list file with current table files."""
        tables_list_path = os.path.join(self.reftable_dir, "tables.list")

        # Get all .ref files in the directory
        ref_files = []
        for name in os.listdir(self.reftable_dir):
            if name.endswith(".ref"):
                ref_files.append(name)

        # Sort by name (which includes timestamp)
        ref_files.sort()

        # Write to tables.list
        with open(tables_list_path, "wb") as f:
            for name in ref_files:
                f.write((name + "\n").encode())
