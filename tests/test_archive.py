# test_archive.py -- tests for archive
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for archive support."""

import struct
import tarfile
from io import BytesIO
from unittest.mock import patch

from dulwich.archive import (
    ChunkedBytesIO,
    UnsafeArchivePathError,
    _is_unsafe_archive_path,
    tar_stream,
)
from dulwich.object_store import MemoryObjectStore
from dulwich.objects import Blob, Tree
from dulwich.tests.utils import build_commit_graph

from . import TestCase


class ArchiveTests(TestCase):
    def test_empty(self) -> None:
        store = MemoryObjectStore()
        _c1, _c2, c3 = build_commit_graph(store, [[1], [2, 1], [3, 1, 2]])
        tree = store[c3.tree]
        stream = b"".join(tar_stream(store, tree, 10))
        out = BytesIO(stream)
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEqual([], tf.getnames())

    def _get_example_tar_stream(
        self, mtime: int, prefix: bytes = b"", format: str = ""
    ) -> BytesIO:
        store = MemoryObjectStore()
        b1 = Blob.from_string(b"somedata")
        store.add_object(b1)
        t1 = Tree()
        t1.add(b"somename", 0o100644, b1.id)
        store.add_object(t1)
        stream = b"".join(tar_stream(store, t1, mtime, prefix, format))
        return BytesIO(stream)

    def test_simple(self) -> None:
        stream = self._get_example_tar_stream(mtime=0)
        tf = tarfile.TarFile(fileobj=stream)
        self.addCleanup(tf.close)
        self.assertEqual(["somename"], tf.getnames())

    def test_unicode(self) -> None:
        store = MemoryObjectStore()
        b1 = Blob.from_string(b"somedata")
        store.add_object(b1)
        t1 = Tree()
        t1.add("ő".encode(), 0o100644, b1.id)
        store.add_object(t1)
        stream = b"".join(tar_stream(store, t1, mtime=0))
        tf = tarfile.TarFile(fileobj=BytesIO(stream))
        self.addCleanup(tf.close)
        self.assertEqual(["ő"], tf.getnames())

    def test_prefix(self) -> None:
        stream = self._get_example_tar_stream(mtime=0, prefix=b"blah")
        tf = tarfile.TarFile(fileobj=stream)
        self.addCleanup(tf.close)
        self.assertEqual(["blah/somename"], tf.getnames())

    def test_gzip_mtime(self) -> None:
        stream = self._get_example_tar_stream(mtime=1234, format="gz")
        expected_mtime = struct.pack("<L", 1234)
        self.assertEqual(stream.getvalue()[4:8], expected_mtime)

    def test_same_file(self) -> None:
        contents: list[bytes | None] = [None, None]
        for format in ["", "gz", "bz2"]:
            for i in [0, 1]:
                with patch("time.time", return_value=i):
                    stream = self._get_example_tar_stream(mtime=0, format=format)
                    contents[i] = stream.getvalue()
            self.assertEqual(
                contents[0],
                contents[1],
                f"Different file contents for format {format!r}",
            )

    def test_tar_stream_with_directory(self) -> None:
        """Test tar_stream with a tree containing directories."""
        store = MemoryObjectStore()

        # Create a blob for a file
        b1 = Blob.from_string(b"file in subdir")
        store.add_object(b1)

        # Create a subtree
        subtree = Tree()
        subtree.add(b"file.txt", 0o100644, b1.id)
        store.add_object(subtree)

        # Create root tree with a directory
        root_tree = Tree()
        root_tree.add(b"subdir", 0o040000, subtree.id)
        store.add_object(root_tree)

        # Generate tar stream
        stream = b"".join(tar_stream(store, root_tree, 0))
        tf = tarfile.TarFile(fileobj=BytesIO(stream))
        self.addCleanup(tf.close)

        # Should contain the file in the subdirectory
        self.assertEqual(["subdir/file.txt"], tf.getnames())

    def test_unsafe_paths_rejected(self) -> None:
        """Tree entry names that git would reject as invalid are refused.

        Mirrors git's ``error: invalid path`` from ``git archive`` for
        absolute paths, parent-traversal, ``.git`` aliases, embedded
        backslashes, and NTFS alternate-data-stream separators.
        """
        # NUL is excluded here because Tree serialization uses it as a name
        # terminator, so a NUL-containing name cannot be round-tripped via the
        # public Tree API. _is_unsafe_archive_path still rejects it on its own.
        unsafe_names = [
            b"../evil.txt",
            b"/absolute.txt",
            b".git/hooks/pre-commit",
            b"..\\evil.txt",
            b".git\\hooks\\pre-commit",
            b"visible.txt:hidden",
            b"..",
            b".git",
            b".GIT",
            b".Git.",
        ]
        for name in unsafe_names:
            store = MemoryObjectStore()
            b1 = Blob.from_string(b"x")
            store.add_object(b1)
            t = Tree()
            t.add(name, 0o100644, b1.id)
            store.add_object(t)
            with self.assertRaises(UnsafeArchivePathError, msg=repr(name)) as cm:
                b"".join(tar_stream(store, t, mtime=0))
            self.assertEqual(name, cm.exception.path)

    def test_is_unsafe_archive_path_nul(self) -> None:
        """NUL bytes in a path are rejected even though they can't be stored in a Tree."""
        self.assertTrue(_is_unsafe_archive_path(b"foo\x00bar"))
        self.assertTrue(_is_unsafe_archive_path(b""))
        self.assertFalse(_is_unsafe_archive_path(b"good.txt"))
        self.assertFalse(_is_unsafe_archive_path(b"sub/dir/good.txt"))

    def test_unsafe_path_nested_in_subtree(self) -> None:
        """A ``.git`` directory hidden under a benign parent is still rejected."""
        store = MemoryObjectStore()
        b1 = Blob.from_string(b"payload")
        store.add_object(b1)
        dotgit = Tree()
        dotgit.add(b"config", 0o100644, b1.id)
        store.add_object(dotgit)
        root = Tree()
        root.add(b"subdir", 0o040000, dotgit.id)
        store.add_object(root)
        # benign so far - now add a malicious sibling
        outer = Tree()
        outer.add(b".git", 0o040000, dotgit.id)
        store.add_object(outer)
        with self.assertRaises(UnsafeArchivePathError):
            b"".join(tar_stream(store, outer, mtime=0))

    def test_mode_canonicalized(self) -> None:
        """setuid/setgid/sticky and other non-canonical mode bits are dropped.

        A crafted tree can hold a regular-file mode such as 0o104755. git's
        archive writer canonicalizes these before emitting the tar permission
        field, so an extracted file never ends up setuid.
        """
        store = MemoryObjectStore()
        b1 = Blob.from_string(b"payload")
        store.add_object(b1)
        t = Tree()
        t.add(b"suid_exec", 0o104755, b1.id)
        t.add(b"sgid_sticky", 0o106644, b1.id)
        t.add(b"plain", 0o100644, b1.id)
        t.add(b"exec", 0o100755, b1.id)
        store.add_object(t)
        stream = b"".join(tar_stream(store, t, mtime=0))
        tf = tarfile.TarFile(fileobj=BytesIO(stream))
        self.addCleanup(tf.close)
        modes = {m.name: m.mode for m in tf.getmembers()}
        self.assertEqual(0o755, modes["suid_exec"])
        self.assertEqual(0o644, modes["sgid_sticky"])
        self.assertEqual(0o644, modes["plain"])
        self.assertEqual(0o755, modes["exec"])
        for mode in modes.values():
            self.assertEqual(0, mode & 0o7000)

    def _archive_names(self, store, tree, **kwargs) -> list[str]:
        stream = b"".join(tar_stream(store, tree, mtime=0, **kwargs))
        tf = tarfile.TarFile(fileobj=BytesIO(stream))
        self.addCleanup(tf.close)
        return sorted(tf.getnames())

    def test_export_ignore(self) -> None:
        """Paths marked ``export-ignore`` in the tree are left out.

        git archive takes the attribute from the tree it is archiving, skips
        matching files, and does not descend into a matching directory.
        """
        store = MemoryObjectStore()
        attrs = Blob.from_string(
            b"ignored.txt export-ignore\n"
            b"docs export-ignore\n"
            b"build/ export-ignore\n"
            b"sub/dropped.txt export-ignore\n"
        )
        payload = Blob.from_string(b"payload")
        for blob in (attrs, payload):
            store.add_object(blob)
        docs = Tree()
        docs.add(b"guide.md", 0o100644, payload.id)
        build = Tree()
        build.add(b"out.o", 0o100644, payload.id)
        sub = Tree()
        sub.add(b"dropped.txt", 0o100644, payload.id)
        sub.add(b"kept.txt", 0o100644, payload.id)
        for tree in (docs, build, sub):
            store.add_object(tree)
        root = Tree()
        root.add(b".gitattributes", 0o100644, attrs.id)
        root.add(b"ignored.txt", 0o100644, payload.id)
        root.add(b"kept.txt", 0o100644, payload.id)
        root.add(b"docs", 0o040000, docs.id)
        root.add(b"build", 0o040000, build.id)
        root.add(b"sub", 0o040000, sub.id)
        store.add_object(root)

        self.assertEqual(
            [
                "blah/.gitattributes",
                "blah/kept.txt",
                "blah/sub/kept.txt",
            ],
            self._archive_names(store, root, prefix=b"blah"),
        )

    def test_export_ignore_negated(self) -> None:
        """``-export-ignore`` on a path overrides a broader pattern."""
        store = MemoryObjectStore()
        attrs = Blob.from_string(
            b"*.secret export-ignore\nkeep.secret -export-ignore\n"
        )
        payload = Blob.from_string(b"payload")
        for blob in (attrs, payload):
            store.add_object(blob)
        root = Tree()
        root.add(b".gitattributes", 0o100644, attrs.id)
        root.add(b"drop.secret", 0o100644, payload.id)
        root.add(b"keep.secret", 0o100644, payload.id)
        store.add_object(root)

        self.assertEqual(
            [".gitattributes", "keep.secret"], self._archive_names(store, root)
        )

    def test_export_ignore_subdirectory(self) -> None:
        """A subdirectory's ``.gitattributes`` applies to its own contents.

        Its patterns are relative to that directory, so ``dropped.txt`` only
        drops ``sub/dropped.txt``, and ``deep`` prunes ``sub/deep``.
        """
        store = MemoryObjectStore()
        sub_attrs = Blob.from_string(b"dropped.txt export-ignore\ndeep export-ignore\n")
        payload = Blob.from_string(b"payload")
        for blob in (sub_attrs, payload):
            store.add_object(blob)
        deep = Tree()
        deep.add(b"deeper.txt", 0o100644, payload.id)
        store.add_object(deep)
        sub = Tree()
        sub.add(b".gitattributes", 0o100644, sub_attrs.id)
        sub.add(b"dropped.txt", 0o100644, payload.id)
        sub.add(b"kept.txt", 0o100644, payload.id)
        sub.add(b"deep", 0o040000, deep.id)
        store.add_object(sub)
        root = Tree()
        # Same name as the one dropped below, to show the pattern is scoped
        # to the directory holding the .gitattributes.
        root.add(b"dropped.txt", 0o100644, payload.id)
        root.add(b"sub", 0o040000, sub.id)
        store.add_object(root)

        self.assertEqual(
            ["dropped.txt", "sub/.gitattributes", "sub/kept.txt"],
            self._archive_names(store, root),
        )

    def test_export_ignore_subdirectory_overrides_root(self) -> None:
        """A deeper ``.gitattributes`` overrides a shallower one."""
        store = MemoryObjectStore()
        root_attrs = Blob.from_string(b"*.secret export-ignore\n")
        sub_attrs = Blob.from_string(b"keep.secret -export-ignore\n")
        payload = Blob.from_string(b"payload")
        for blob in (root_attrs, sub_attrs, payload):
            store.add_object(blob)
        sub = Tree()
        sub.add(b".gitattributes", 0o100644, sub_attrs.id)
        sub.add(b"drop.secret", 0o100644, payload.id)
        sub.add(b"keep.secret", 0o100644, payload.id)
        store.add_object(sub)
        root = Tree()
        root.add(b".gitattributes", 0o100644, root_attrs.id)
        root.add(b"top.secret", 0o100644, payload.id)
        root.add(b"sub", 0o040000, sub.id)
        store.add_object(root)

        self.assertEqual(
            [".gitattributes", "sub/.gitattributes", "sub/keep.secret"],
            self._archive_names(store, root),
        )

    def test_export_ignore_non_blob_gitattributes(self) -> None:
        """A ``.gitattributes`` that is not a regular file is ignored."""
        store = MemoryObjectStore()
        payload = Blob.from_string(b"payload")
        store.add_object(payload)
        attrs_dir = Tree()
        attrs_dir.add(b"nested", 0o100644, payload.id)
        store.add_object(attrs_dir)
        root = Tree()
        root.add(b".gitattributes", 0o040000, attrs_dir.id)
        root.add(b"kept.txt", 0o100644, payload.id)
        store.add_object(root)

        self.assertEqual(
            [".gitattributes/nested", "kept.txt"], self._archive_names(store, root)
        )

    def test_tar_stream_with_submodule(self) -> None:
        """Test tar_stream handles missing objects (submodules) gracefully."""
        store = MemoryObjectStore()

        # Create a tree with an entry that doesn't exist in the store
        # (simulating a submodule reference)
        root_tree = Tree()
        # Use a valid hex SHA (40 hex chars = 20 bytes)
        nonexistent_sha = b"a" * 40
        root_tree.add(b"submodule", 0o160000, nonexistent_sha)
        store.add_object(root_tree)

        # Should not raise, just skip the missing entry
        stream = b"".join(tar_stream(store, root_tree, 0))
        tf = tarfile.TarFile(fileobj=BytesIO(stream))
        self.addCleanup(tf.close)

        # Submodule should be skipped
        self.assertEqual([], tf.getnames())


class ChunkedBytesIOTests(TestCase):
    """Tests for ChunkedBytesIO class."""

    def test_read_all(self) -> None:
        """Test reading all bytes from ChunkedBytesIO."""
        chunks = [b"hello", b" ", b"world"]
        chunked = ChunkedBytesIO(chunks)

        result = chunked.read()
        self.assertEqual(b"hello world", result)

    def test_read_with_limit(self) -> None:
        """Test reading limited bytes from ChunkedBytesIO."""
        chunks = [b"hello", b" ", b"world"]
        chunked = ChunkedBytesIO(chunks)

        # Read first 5 bytes
        result = chunked.read(5)
        self.assertEqual(b"hello", result)

        # Read next 3 bytes
        result = chunked.read(3)
        self.assertEqual(b" wo", result)

        # Read remaining
        result = chunked.read()
        self.assertEqual(b"rld", result)

    def test_read_negative_maxbytes(self) -> None:
        """Test reading with negative maxbytes reads all."""
        chunks = [b"hello", b" ", b"world"]
        chunked = ChunkedBytesIO(chunks)

        result = chunked.read(-1)
        self.assertEqual(b"hello world", result)

    def test_read_across_chunks(self) -> None:
        """Test reading across multiple chunks."""
        chunks = [b"abc", b"def", b"ghi"]
        chunked = ChunkedBytesIO(chunks)

        # Read 7 bytes (spans three chunks)
        result = chunked.read(7)
        self.assertEqual(b"abcdefg", result)

        # Read remaining
        result = chunked.read()
        self.assertEqual(b"hi", result)

    def test_read_empty_chunks(self) -> None:
        """Test reading from empty chunks list."""
        chunked = ChunkedBytesIO([])

        result = chunked.read()
        self.assertEqual(b"", result)

    def test_read_with_empty_chunks_mixed(self) -> None:
        """Test reading with some empty chunks in the list."""
        chunks = [b"hello", b"", b"world", b""]
        chunked = ChunkedBytesIO(chunks)

        result = chunked.read()
        self.assertEqual(b"helloworld", result)

    def test_read_exact_chunk_boundary(self) -> None:
        """Test reading exactly to a chunk boundary."""
        chunks = [b"abc", b"def", b"ghi"]
        chunked = ChunkedBytesIO(chunks)

        # Read exactly first chunk
        result = chunked.read(3)
        self.assertEqual(b"abc", result)

        # Read exactly second chunk
        result = chunked.read(3)
        self.assertEqual(b"def", result)

        # Read exactly third chunk
        result = chunked.read(3)
        self.assertEqual(b"ghi", result)

        # Should be at end
        result = chunked.read()
        self.assertEqual(b"", result)
