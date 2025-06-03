"""Implementation of the reftable refs storage format.

The reftable format is a binary format for storing Git refs that provides
better performance and atomic operations compared to the traditional
loose refs format.

See: https://git-scm.com/docs/reftable
"""

import os
import struct
from io import BytesIO
from typing import BinaryIO, Optional

from dulwich.refs import (
    RefsContainer,
)

# Reftable magic bytes
REFTABLE_MAGIC = b"REFT"

# Reftable version
REFTABLE_VERSION = 1

# Block types
BLOCK_TYPE_REF = b"r"
BLOCK_TYPE_OBJ = b"o"
BLOCK_TYPE_LOG = b"g"
BLOCK_TYPE_INDEX = b"i"

# Value types for ref records
REF_VALUE_DELETE = 0
REF_VALUE_REF = 1
REF_VALUE_SYMREF = 2
REF_VALUE_PEELED = 3


def encode_varint(value: int) -> bytes:
    """Encode an integer as a variable-length integer."""
    result = []
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def decode_varint(stream: BinaryIO) -> int:
    """Decode a variable-length integer from a stream."""
    result = 0
    shift = 0
    while True:
        byte = stream.read(1)
        if not byte:
            raise ValueError("Unexpected end of stream while reading varint")
        byte_val = byte[0]
        result |= (byte_val & 0x7F) << shift
        if byte_val < 0x80:
            break
        shift += 7
    return result


def encode_string(s: bytes) -> bytes:
    """Encode a string with its length as a varint prefix."""
    return encode_varint(len(s)) + s


def decode_string(stream: BinaryIO) -> bytes:
    """Decode a string with a varint length prefix from a stream."""
    length = decode_varint(stream)
    return stream.read(length)


class ReftableBlock:
    """Base class for reftable blocks."""

    def __init__(self, block_type: bytes):
        self.block_type = block_type
        self.entries: list = []

    def add_entry(self, entry):
        """Add an entry to the block."""
        self.entries.append(entry)

    def encode(self) -> bytes:
        """Encode the block to bytes."""
        raise NotImplementedError

    @classmethod
    def decode(cls, data: bytes):
        """Decode a block from bytes."""
        raise NotImplementedError


class RefRecord:
    """A reference record in a reftable."""

    def __init__(self, refname: bytes, value_type: int, value: bytes):
        self.refname = refname
        self.value_type = value_type
        self.value = value

    def encode(self, prefix: bytes = b"") -> bytes:
        """Encode the ref record with prefix compression."""
        common_prefix_len = 0
        for i in range(min(len(prefix), len(self.refname))):
            if prefix[i] != self.refname[i]:
                break
            common_prefix_len += 1

        suffix = self.refname[common_prefix_len:]

        # Encode: prefix_len (varint) + suffix_len (varint) + suffix + value_type + value
        result = encode_varint(common_prefix_len)
        result += encode_varint(len(suffix))
        result += suffix
        result += bytes([self.value_type])

        if self.value_type == REF_VALUE_REF:
            # Direct SHA-1 reference (20 bytes)
            # Convert hex string to binary if needed
            if len(self.value) == 40:
                result += bytes.fromhex(self.value.decode())
            else:
                result += self.value
        elif self.value_type == REF_VALUE_SYMREF:
            # Symbolic reference
            result += encode_string(self.value)
        elif self.value_type == REF_VALUE_PEELED:
            # SHA-1 + peeled SHA-1 (40 bytes)
            # Convert hex strings to binary if needed
            if len(self.value) == 80:
                result += bytes.fromhex(self.value.decode())
            else:
                result += self.value
        elif self.value_type == REF_VALUE_DELETE:
            # Deleted ref, no value
            pass
        else:
            raise ValueError(f"Unknown ref value type: {self.value_type}")

        return result

    @classmethod
    def decode(cls, stream: BinaryIO, prefix: bytes = b"") -> tuple["RefRecord", bytes]:
        """Decode a ref record from a stream. Returns (record, full_refname)."""
        prefix_len = decode_varint(stream)
        suffix_len = decode_varint(stream)
        suffix = stream.read(suffix_len)

        if len(suffix) != suffix_len:
            raise ValueError("Unexpected end of stream while reading suffix")

        refname = prefix[:prefix_len] + suffix

        value_type_bytes = stream.read(1)
        if not value_type_bytes:
            raise ValueError("Unexpected end of stream while reading value type")
        value_type = value_type_bytes[0]

        if value_type == REF_VALUE_REF:
            value = stream.read(20)
            if len(value) != 20:
                raise ValueError("Unexpected end of stream while reading ref value")
            # Convert to hex string for interface compatibility
            value = value.hex().encode()
        elif value_type == REF_VALUE_SYMREF:
            value = decode_string(stream)
        elif value_type == REF_VALUE_PEELED:
            value = stream.read(40)
            if len(value) != 40:
                raise ValueError("Unexpected end of stream while reading peeled value")
            # Convert to hex string for interface compatibility
            value = value.hex().encode()
        elif value_type == REF_VALUE_DELETE:
            value = b""
        else:
            raise ValueError(f"Unknown ref value type: {value_type}")

        return cls(refname, value_type, value), refname


class RefBlock(ReftableBlock):
    """A block containing reference records."""

    def __init__(self):
        super().__init__(BLOCK_TYPE_REF)
        self.refs = []

    def add_ref(self, refname: bytes, value_type: int, value: bytes):
        """Add a reference to the block."""
        self.refs.append(RefRecord(refname, value_type, value))

    def encode(self) -> bytes:
        """Encode the ref block."""
        # Sort refs by name
        self.refs.sort(key=lambda r: r.refname)

        # Encode with prefix compression
        result = BytesIO()
        prefix = b""
        for ref in self.refs:
            encoded = ref.encode(prefix)
            result.write(encoded)
            prefix = ref.refname

        return result.getvalue()

    @classmethod
    def decode(cls, data: bytes) -> "RefBlock":
        """Decode a ref block from bytes."""
        block = cls()
        if not data:
            return block

        stream = BytesIO(data)
        prefix = b""

        while stream.tell() < len(data):
            try:
                ref, prefix = RefRecord.decode(stream, prefix)
                block.refs.append(ref)
            except ValueError:
                # End of valid data
                break

        return block


class ReftableWriter:
    """Writer for reftable files."""

    def __init__(self, f: BinaryIO):
        self.f = f
        self.refs: dict[bytes, tuple[int, bytes]] = {}

    def add_ref(self, refname: bytes, sha: bytes):
        """Add a direct reference."""
        self.refs[refname] = (REF_VALUE_REF, sha)

    def add_symbolic_ref(self, refname: bytes, target: bytes):
        """Add a symbolic reference."""
        self.refs[refname] = (REF_VALUE_SYMREF, target)

    def delete_ref(self, refname: bytes):
        """Mark a reference as deleted."""
        self.refs[refname] = (REF_VALUE_DELETE, b"")

    def write(self):
        """Write the reftable to the file."""
        # Write header
        self._write_header()

        # Write ref blocks
        self._write_ref_blocks()

        # Write footer
        self._write_footer()

    def _write_header(self):
        """Write the reftable header."""
        # Magic bytes
        self.f.write(REFTABLE_MAGIC)

        # Version
        self.f.write(struct.pack(">I", REFTABLE_VERSION))

        # Min/max update index (timestamps)
        self.f.write(struct.pack(">Q", 0))  # min_update_index
        self.f.write(struct.pack(">Q", 0))  # max_update_index

    def _write_ref_blocks(self):
        """Write reference blocks."""
        # Only write block if we have refs
        if not self.refs:
            return

        # For now, write all refs in a single block
        # TODO: Split into multiple blocks based on size
        block = RefBlock()
        for refname, (value_type, value) in sorted(self.refs.items()):
            block.add_ref(refname, value_type, value)

        block_data = block.encode()

        # Write block type
        self.f.write(BLOCK_TYPE_REF)

        # Write block length
        self.f.write(struct.pack(">I", len(block_data)))

        # Write block data
        self.f.write(block_data)

    def _write_footer(self):
        """Write the reftable footer."""
        # For now, write a simple footer
        # TODO: Write proper index blocks and CRC


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

        # Read version
        version = struct.unpack(">I", self.f.read(4))[0]
        if version != REFTABLE_VERSION:
            raise ValueError(f"Unsupported reftable version: {version}")

        # Read min/max update index
        self.min_update_index = struct.unpack(">Q", self.f.read(8))[0]
        self.max_update_index = struct.unpack(">Q", self.f.read(8))[0]

    def _read_blocks(self):
        """Read all blocks from the reftable."""
        while True:
            # Read block type
            block_type = self.f.read(1)
            if not block_type:
                break

            # Read block length
            block_length = struct.unpack(">I", self.f.read(4))[0]

            # Read block data
            block_data = self.f.read(block_length)

            if block_type == BLOCK_TYPE_REF:
                self._process_ref_block(block_data)
            # TODO: Handle other block types

    def _process_ref_block(self, data: bytes):
        """Process a reference block."""
        block = RefBlock.decode(data)
        for ref in block.refs:
            # Store all refs including deletion records - deletion handling is done at container level
            self.refs[ref.refname] = (ref.value_type, ref.value)

    def get_ref(self, refname: bytes) -> Optional[tuple[int, bytes]]:
        """Get a reference by name."""
        return self.refs.get(refname)

    def all_refs(self) -> dict[bytes, tuple[int, bytes]]:
        """Get all references."""
        return self.refs.copy()


class ReftableRefsContainer(RefsContainer):
    """A refs container backed by the reftable format."""

    def __init__(self, path: str):
        """Initialize a reftable refs container.

        Args:
            path: Path to the reftable directory
        """
        self.path = path
        self.reftable_dir = os.path.join(path, "reftable")
        if not os.path.exists(self.reftable_dir):
            os.makedirs(self.reftable_dir)

    def _get_table_files(self) -> list[str]:
        """Get sorted list of reftable files."""
        if not os.path.exists(self.reftable_dir):
            return []
        files = []
        for name in os.listdir(self.reftable_dir):
            if name.endswith(".ref"):
                files.append(os.path.join(self.reftable_dir, name))
        return sorted(files)

    def _read_all_tables(self) -> dict[bytes, tuple[int, bytes]]:
        """Read all reftable files and merge results."""
        all_refs: dict[bytes, tuple[int, bytes]] = {}
        for table_file in self._get_table_files():
            with open(table_file, "rb") as f:
                reader = ReftableReader(f)
                # Apply updates from this table
                for refname, (value_type, value) in reader.all_refs().items():
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
        return set(refs.keys())

    def read_loose_ref(self, name: bytes) -> bytes:
        """Read a reference value."""
        refs = self._read_all_tables()
        ref_data = refs.get(name)
        if ref_data is None:
            raise KeyError(name)

        value_type, value = ref_data
        if value_type == REF_VALUE_REF:
            return value
        elif value_type == REF_VALUE_SYMREF:
            # For symbolic refs, resolve them
            return self._follow_symref(value)
        elif value_type == REF_VALUE_PEELED:
            # Return the first SHA (not the peeled one)
            return value[:40]  # First 40 hex chars
        else:
            raise KeyError(name)

    def _follow_symref(self, target: bytes) -> bytes:
        """Follow a symbolic reference to its target."""
        # Prevent infinite loops
        max_depth = 5
        for _ in range(max_depth):
            try:
                ref_value = self.read_loose_ref(target)
            except KeyError:
                raise KeyError(target)
            if not ref_value.startswith(b"ref: "):
                return ref_value
            target = ref_value[5:]
        raise ValueError("Symbolic reference loop detected")

    def get_packed_refs(self) -> dict[bytes, bytes]:
        """Get packed refs. Reftable doesn't distinguish packed/loose."""
        refs = self._read_all_tables()
        result = {}
        for name, (value_type, value) in refs.items():
            if value_type == REF_VALUE_REF:
                result[name] = value
            elif value_type == REF_VALUE_PEELED:
                result[name] = value[:40]  # First 40 hex chars
        return result

    def add_packed_refs(self, new_refs: dict[bytes, Optional[bytes]]):
        """Add packed refs. Creates a new reftable file."""
        # Generate a new table filename
        import time

        table_name = f"{int(time.time() * 1000000)}.ref"
        table_path = os.path.join(self.reftable_dir, table_name)

        with open(table_path, "wb") as f:
            writer = ReftableWriter(f)
            for refname, sha in new_refs.items():
                if sha is not None:
                    writer.add_ref(refname, sha)
                else:
                    writer.delete_ref(refname)
            writer.write()

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
        """Write a single ref update to a new reftable file."""
        import time

        table_name = f"{int(time.time() * 1000000)}.ref"
        table_path = os.path.join(self.reftable_dir, table_name)

        # For simplified implementation, write a table with just this update
        # In a real implementation, we'd use compaction and proper table management
        with open(table_path, "wb") as f:
            writer = ReftableWriter(f)
            if value_type == REF_VALUE_REF:
                writer.add_ref(name, value)
            elif value_type == REF_VALUE_SYMREF:
                writer.add_symbolic_ref(name, value)
            elif value_type == REF_VALUE_DELETE:
                writer.delete_ref(name)
            writer.write()

    def remove_packed_ref(self, name: bytes):
        """Remove a packed ref. Creates a deletion record."""
        self._write_ref_update(name, REF_VALUE_DELETE, b"")
