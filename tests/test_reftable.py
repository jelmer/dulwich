"""Tests for the reftable refs storage format."""

import tempfile
import unittest
from io import BytesIO

from dulwich.reftable import (
    REF_VALUE_REF,
    REF_VALUE_SYMREF,
    RefBlock,
    RefRecord,
    ReftableReader,
    ReftableRefsContainer,
    ReftableWriter,
    decode_varint_from_stream,
    encode_varint,
)


class TestVarintEncoding(unittest.TestCase):
    """Test varint encoding/decoding."""

    def test_encode_decode_small(self):
        """Test encoding/decoding small integers."""
        for i in range(128):
            encoded = encode_varint(i)
            stream = BytesIO(encoded)
            decoded = decode_varint_from_stream(stream)
            self.assertEqual(i, decoded)

    def test_encode_decode_large(self):
        """Test encoding/decoding larger integers."""
        test_values = [127, 128, 255, 256, 16383, 16384, 65535, 65536, 1048576]
        for i in test_values:
            encoded = encode_varint(i)
            stream = BytesIO(encoded)
            decoded = decode_varint_from_stream(stream)
            self.assertEqual(i, decoded)


class TestRefRecord(unittest.TestCase):
    """Test ref record encoding/decoding."""

    def test_encode_decode_ref(self):
        """Test encoding/decoding a direct ref."""
        sha = b"a" * 40
        ref = RefRecord(b"refs/heads/master", REF_VALUE_REF, sha)
        encoded = ref.encode()

        stream = BytesIO(encoded)
        decoded_ref, refname = RefRecord.decode(stream)

        self.assertEqual(ref.refname, decoded_ref.refname)
        self.assertEqual(ref.value_type, decoded_ref.value_type)
        self.assertEqual(ref.value, decoded_ref.value)
        self.assertEqual(b"refs/heads/master", refname)

    def test_encode_decode_symref(self):
        """Test encoding/decoding a symbolic ref."""
        target = b"refs/heads/main"
        ref = RefRecord(b"HEAD", REF_VALUE_SYMREF, target)
        encoded = ref.encode()

        stream = BytesIO(encoded)
        decoded_ref, refname = RefRecord.decode(stream)

        self.assertEqual(ref.refname, decoded_ref.refname)
        self.assertEqual(ref.value_type, decoded_ref.value_type)
        self.assertEqual(ref.value, decoded_ref.value)
        self.assertEqual(b"HEAD", refname)

    def test_prefix_compression(self):
        """Test prefix compression in ref encoding."""
        ref2 = RefRecord(b"refs/heads/main", REF_VALUE_REF, b"b" * 40)

        # Encode ref2 with ref1 as prefix
        encoded = ref2.encode(b"refs/heads/master")

        # Should be shorter due to common prefix
        encoded_no_prefix = ref2.encode()
        self.assertLess(len(encoded), len(encoded_no_prefix))

        # Decode should still work
        stream = BytesIO(encoded)
        decoded_ref, refname = RefRecord.decode(stream, b"refs/heads/master")
        self.assertEqual(b"refs/heads/main", refname)


class TestRefBlock(unittest.TestCase):
    """Test ref block encoding/decoding."""

    def test_encode_decode_block(self):
        """Test encoding/decoding a ref block."""
        block = RefBlock()
        block.add_ref(b"refs/heads/master", REF_VALUE_REF, b"a" * 40)
        block.add_ref(b"refs/heads/main", REF_VALUE_REF, b"b" * 40)
        block.add_ref(b"HEAD", REF_VALUE_SYMREF, b"refs/heads/master")

        encoded = block.encode()
        decoded_block = RefBlock.decode(encoded)

        self.assertEqual(len(block.refs), len(decoded_block.refs))

        # Check that refs are sorted
        ref_names = [ref.refname for ref in decoded_block.refs]
        self.assertEqual(ref_names, sorted(ref_names))


class TestReftableIO(unittest.TestCase):
    """Test reftable file I/O."""

    def test_write_read_reftable(self):
        """Test writing and reading a reftable file."""
        with tempfile.NamedTemporaryFile() as f:
            # Write reftable
            writer = ReftableWriter(f)
            writer.add_ref(b"refs/heads/master", b"a" * 40)
            writer.add_ref(b"refs/heads/main", b"b" * 40)
            writer.add_symbolic_ref(b"HEAD", b"refs/heads/master")
            writer.write()

            # Read reftable
            f.seek(0)
            reader = ReftableReader(f)

            # Check refs
            master_ref = reader.get_ref(b"refs/heads/master")
            self.assertIsNotNone(master_ref)
            self.assertEqual(master_ref[0], REF_VALUE_REF)
            self.assertEqual(master_ref[1], b"a" * 40)

            main_ref = reader.get_ref(b"refs/heads/main")
            self.assertIsNotNone(main_ref)
            self.assertEqual(main_ref[0], REF_VALUE_REF)
            self.assertEqual(main_ref[1], b"b" * 40)

            head_ref = reader.get_ref(b"HEAD")
            self.assertIsNotNone(head_ref)
            self.assertEqual(head_ref[0], REF_VALUE_SYMREF)
            self.assertEqual(head_ref[1], b"refs/heads/master")


class TestReftableRefsContainer(unittest.TestCase):
    """Test ReftableRefsContainer functionality."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.container = ReftableRefsContainer(self.test_dir)

    def tearDown(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.test_dir)

    def test_empty_container(self):
        """Test operations on empty container."""
        self.assertEqual(set(), self.container.allkeys())
        with self.assertRaises(KeyError):
            self.container.read_loose_ref(b"refs/heads/master")

    def test_add_ref(self):
        """Test adding a reference."""
        sha = b"a" * 40
        success = self.container.add_if_new(b"refs/heads/master", sha)
        self.assertTrue(success)

        # Check ref exists
        self.assertIn(b"refs/heads/master", self.container.allkeys())
        self.assertEqual(sha, self.container.read_loose_ref(b"refs/heads/master"))

        # Adding again should fail
        success = self.container.add_if_new(b"refs/heads/master", b"b" * 40)
        self.assertFalse(success)

    def test_set_ref(self):
        """Test setting a reference."""
        sha1 = b"a" * 40
        sha2 = b"b" * 40

        # Set initial ref
        success = self.container.set_if_equals(b"refs/heads/master", None, sha1)
        self.assertTrue(success)

        # Update ref
        success = self.container.set_if_equals(b"refs/heads/master", sha1, sha2)
        self.assertTrue(success)
        self.assertEqual(sha2, self.container.read_loose_ref(b"refs/heads/master"))

        # Try to update with wrong old value
        success = self.container.set_if_equals(b"refs/heads/master", sha1, b"c" * 40)
        self.assertFalse(success)
        self.assertEqual(sha2, self.container.read_loose_ref(b"refs/heads/master"))

    def test_remove_ref(self):
        """Test removing a reference."""
        sha = b"a" * 40

        # Add ref
        self.container.add_if_new(b"refs/heads/master", sha)
        self.assertIn(b"refs/heads/master", self.container.allkeys())

        # Remove ref
        success = self.container.remove_if_equals(b"refs/heads/master", sha)
        self.assertTrue(success)
        self.assertNotIn(b"refs/heads/master", self.container.allkeys())

        # Try to remove again
        success = self.container.remove_if_equals(b"refs/heads/master", sha)
        self.assertFalse(success)

    def test_symbolic_ref(self):
        """Test symbolic references."""
        # Add target ref
        sha = b"a" * 40
        self.container.add_if_new(b"refs/heads/master", sha)

        # Add symbolic ref
        self.container.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        # Check symbolic ref is stored correctly
        self.assertEqual(
            b"ref: refs/heads/master", self.container.read_loose_ref(b"HEAD")
        )
        self.assertIn(b"HEAD", self.container.allkeys())

        # Check that following the symref works
        self.assertEqual(sha, self.container[b"HEAD"])

    def test_packed_refs(self):
        """Test packed refs functionality."""
        refs = {
            b"refs/heads/master": b"a" * 40,
            b"refs/heads/main": b"b" * 40,
            b"refs/tags/v1.0": b"c" * 40,
        }

        # Add packed refs
        self.container.add_packed_refs(refs)

        # Check refs exist
        for refname, sha in refs.items():
            self.assertIn(refname, self.container.allkeys())
            self.assertEqual(sha, self.container.read_loose_ref(refname))

        # Get packed refs
        packed = self.container.get_packed_refs()
        for refname, sha in refs.items():
            self.assertEqual(sha, packed[refname])

    def test_multiple_table_files(self):
        """Test with multiple reftable files."""
        # Add some refs
        self.container.add_if_new(b"refs/heads/master", b"a" * 40)
        self.container.add_if_new(b"refs/heads/main", b"b" * 40)

        # Update a ref (creates new table file)
        self.container.set_if_equals(b"refs/heads/master", b"a" * 40, b"c" * 40)

        # Check updated ref
        self.assertEqual(b"c" * 40, self.container.read_loose_ref(b"refs/heads/master"))
        self.assertEqual(b"b" * 40, self.container.read_loose_ref(b"refs/heads/main"))

        # Should have multiple table files
        table_files = self.container._get_table_files()
        self.assertGreaterEqual(len(table_files), 2)


if __name__ == "__main__":
    unittest.main()
