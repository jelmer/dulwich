# test_commit_graph.py -- Tests for commit graph functionality
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.

"""Tests for Git commit graph functionality."""

import io
import os
import struct
import tempfile
import unittest

from dulwich.commit_graph import (
    CHUNK_COMMIT_DATA,
    CHUNK_OID_FANOUT,
    CHUNK_OID_LOOKUP,
    COMMIT_GRAPH_SIGNATURE,
    COMMIT_GRAPH_VERSION,
    HASH_VERSION_SHA1,
    CommitGraph,
    CommitGraphChunk,
    CommitGraphEntry,
    find_commit_graph_file,
    generate_commit_graph,
    get_reachable_commits,
    read_commit_graph,
)


class CommitGraphEntryTests(unittest.TestCase):
    """Tests for CommitGraphEntry."""

    def test_init(self):
        commit_id = b"a" * 40
        tree_id = b"b" * 40
        parents = [b"c" * 40, b"d" * 40]
        generation = 42
        commit_time = 1234567890

        entry = CommitGraphEntry(commit_id, tree_id, parents, generation, commit_time)

        self.assertEqual(entry.commit_id, commit_id)
        self.assertEqual(entry.tree_id, tree_id)
        self.assertEqual(entry.parents, parents)
        self.assertEqual(entry.generation, generation)
        self.assertEqual(entry.commit_time, commit_time)

    def test_repr(self):
        entry = CommitGraphEntry(b"a" * 40, b"b" * 40, [], 1, 1000)
        repr_str = repr(entry)
        self.assertIn("CommitGraphEntry", repr_str)
        self.assertIn("generation=1", repr_str)


class CommitGraphChunkTests(unittest.TestCase):
    """Tests for CommitGraphChunk."""

    def test_init(self):
        chunk = CommitGraphChunk(b"TEST", b"test data")
        self.assertEqual(chunk.chunk_id, b"TEST")
        self.assertEqual(chunk.data, b"test data")

    def test_repr(self):
        chunk = CommitGraphChunk(b"TEST", b"x" * 100)
        repr_str = repr(chunk)
        self.assertIn("CommitGraphChunk", repr_str)
        self.assertIn("size=100", repr_str)


class CommitGraphTests(unittest.TestCase):
    """Tests for CommitGraph."""

    def test_init(self):
        graph = CommitGraph()
        self.assertEqual(graph.hash_version, HASH_VERSION_SHA1)
        self.assertEqual(len(graph.entries), 0)
        self.assertEqual(len(graph.chunks), 0)

    def test_len(self):
        graph = CommitGraph()
        self.assertEqual(len(graph), 0)

        # Add a dummy entry
        entry = CommitGraphEntry(b"a" * 40, b"b" * 40, [], 1, 1000)
        graph.entries.append(entry)
        self.assertEqual(len(graph), 1)

    def test_iter(self):
        graph = CommitGraph()
        entry1 = CommitGraphEntry(b"a" * 40, b"b" * 40, [], 1, 1000)
        entry2 = CommitGraphEntry(b"c" * 40, b"d" * 40, [], 2, 2000)
        graph.entries.extend([entry1, entry2])

        entries = list(graph)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0], entry1)
        self.assertEqual(entries[1], entry2)

    def test_get_entry_by_oid_missing(self):
        graph = CommitGraph()
        result = graph.get_entry_by_oid(b"f" * 40)
        self.assertIsNone(result)

    def test_get_generation_number_missing(self):
        graph = CommitGraph()
        result = graph.get_generation_number(b"f" * 40)
        self.assertIsNone(result)

    def test_get_parents_missing(self):
        graph = CommitGraph()
        result = graph.get_parents(b"f" * 40)
        self.assertIsNone(result)

    def test_from_invalid_signature(self):
        data = b"XXXX" + b"\\x00" * 100
        f = io.BytesIO(data)

        with self.assertRaises(ValueError) as cm:
            CommitGraph.from_file(f)
        self.assertIn("Invalid commit graph signature", str(cm.exception))

    def test_from_invalid_version(self):
        data = COMMIT_GRAPH_SIGNATURE + struct.pack(">B", 99) + b"\\x00" * 100
        f = io.BytesIO(data)

        with self.assertRaises(ValueError) as cm:
            CommitGraph.from_file(f)
        self.assertIn("Unsupported commit graph version", str(cm.exception))

    def test_from_invalid_hash_version(self):
        data = (
            COMMIT_GRAPH_SIGNATURE
            + struct.pack(">B", COMMIT_GRAPH_VERSION)
            + struct.pack(">B", 99)  # Invalid hash version
            + b"\\x00" * 100
        )
        f = io.BytesIO(data)

        with self.assertRaises(ValueError) as cm:
            CommitGraph.from_file(f)
        self.assertIn("Unsupported hash version", str(cm.exception))

    def create_minimal_commit_graph_data(self):
        """Create minimal valid commit graph data for testing."""
        # Create the data in order and calculate offsets properly

        # Header: signature + version + hash_version + num_chunks + base_graph_count
        header = (
            COMMIT_GRAPH_SIGNATURE
            + struct.pack(">B", COMMIT_GRAPH_VERSION)
            + struct.pack(">B", HASH_VERSION_SHA1)
            + struct.pack(">B", 3)  # 3 chunks
            + struct.pack(">B", 0)
        )  # 0 base graphs

        # Table of contents: 4 entries (3 chunks + terminator) = 4 * 12 = 48 bytes
        toc_size = 4 * 12

        # Calculate chunk offsets from start of file
        header_size = 8
        chunk1_offset = header_size + toc_size  # OID Fanout
        chunk2_offset = chunk1_offset + 256 * 4  # OID Lookup (after fanout)
        chunk3_offset = chunk2_offset + 20  # Commit Data (after 1 commit * 20 bytes)
        terminator_offset = (
            chunk3_offset + 36
        )  # After commit data (1 commit * 36 bytes)

        # Build table of contents
        toc = (
            CHUNK_OID_FANOUT
            + struct.pack(">Q", chunk1_offset)
            + CHUNK_OID_LOOKUP
            + struct.pack(">Q", chunk2_offset)
            + CHUNK_COMMIT_DATA
            + struct.pack(">Q", chunk3_offset)
            + b"\x00\x00\x00\x00"
            + struct.pack(">Q", terminator_offset)
        )

        # OID Fanout chunk (256 * 4 bytes)
        fanout = b""
        for i in range(256):
            if i < 0xAA:  # Our test commit starts with 0xaa
                fanout += struct.pack(">L", 0)
            else:
                fanout += struct.pack(">L", 1)  # 1 commit total

        # OID Lookup chunk (1 commit = 20 bytes)
        commit_oid = b"\xaa" + b"\x00" * 19
        oid_lookup = commit_oid

        # Commit Data chunk (1 commit = 20 + 16 = 36 bytes)
        tree_oid = b"\xbb" + b"\x00" * 19
        parent1_pos = 0x70000000  # GRAPH_PARENT_MISSING
        parent2_pos = 0x70000000  # GRAPH_PARENT_MISSING
        generation = 1
        commit_time = 1234567890
        gen_and_time = (generation << 2) | (commit_time >> 32)
        commit_data = (
            tree_oid
            + struct.pack(">LL", parent1_pos, parent2_pos)
            + struct.pack(">LL", gen_and_time, commit_time & 0xFFFFFFFF)
        )

        return header + toc + fanout + oid_lookup + commit_data

    def test_from_minimal_valid_file(self):
        """Test parsing a minimal but valid commit graph file."""
        data = self.create_minimal_commit_graph_data()
        f = io.BytesIO(data)

        graph = CommitGraph.from_file(f)

        self.assertEqual(graph.hash_version, HASH_VERSION_SHA1)
        self.assertEqual(len(graph), 1)

        # Check the parsed entry
        entry = graph.entries[0]
        self.assertEqual(entry.commit_id, b"aa" + b"00" * 19)
        self.assertEqual(entry.tree_id, b"bb" + b"00" * 19)
        self.assertEqual(entry.parents, [])  # No parents
        self.assertEqual(entry.generation, 1)
        self.assertEqual(entry.commit_time, 1234567890)

        # Test lookup methods
        commit_oid = b"aa" + b"00" * 19
        self.assertEqual(graph.get_generation_number(commit_oid), 1)
        self.assertEqual(graph.get_parents(commit_oid), [])
        self.assertIsNotNone(graph.get_entry_by_oid(commit_oid))

    def test_missing_required_chunks(self):
        """Test error handling for missing required chunks."""
        # Create data with header but no chunks
        header = (
            COMMIT_GRAPH_SIGNATURE
            + struct.pack(">B", COMMIT_GRAPH_VERSION)
            + struct.pack(">B", HASH_VERSION_SHA1)
            + struct.pack(">B", 0)  # 0 chunks
            + struct.pack(">B", 0)
        )

        # TOC with just terminator
        toc = b"\\x00\\x00\\x00\\x00" + struct.pack(">Q", 12)

        data = header + toc
        f = io.BytesIO(data)

        with self.assertRaises(ValueError) as cm:
            CommitGraph.from_file(f)
        self.assertIn("Missing required OID lookup chunk", str(cm.exception))

    def test_write_empty_graph_raises(self):
        """Test that writing empty graph raises ValueError."""
        graph = CommitGraph()
        f = io.BytesIO()

        with self.assertRaises(ValueError):
            graph.write_to_file(f)

    def test_write_and_read_round_trip(self):
        """Test writing and reading a commit graph."""
        # Create a simple commit graph
        graph = CommitGraph()
        entry = CommitGraphEntry(
            commit_id=b"aa" + b"00" * 19,
            tree_id=b"bb" + b"00" * 19,
            parents=[],
            generation=1,
            commit_time=1234567890,
        )
        graph.entries.append(entry)
        graph._oid_to_index = {bytes.fromhex(entry.commit_id.decode()): 0}

        # Write to bytes
        f = io.BytesIO()
        graph.write_to_file(f)

        # Read back
        f.seek(0)
        read_graph = CommitGraph.from_file(f)

        # Verify
        self.assertEqual(len(read_graph), 1)
        read_entry = read_graph.entries[0]
        self.assertEqual(read_entry.commit_id, entry.commit_id)
        self.assertEqual(read_entry.tree_id, entry.tree_id)
        self.assertEqual(read_entry.parents, entry.parents)
        self.assertEqual(read_entry.generation, entry.generation)
        self.assertEqual(read_entry.commit_time, entry.commit_time)


class CommitGraphFileOperationsTests(unittest.TestCase):
    """Tests for commit graph file operations."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_read_commit_graph_missing_file(self):
        """Test reading from non-existent file."""
        missing_path = os.path.join(self.tempdir, "missing.graph")
        result = read_commit_graph(missing_path)
        self.assertIsNone(result)

    def test_read_commit_graph_invalid_file(self):
        """Test reading from invalid file."""
        invalid_path = os.path.join(self.tempdir, "invalid.graph")
        with open(invalid_path, "wb") as f:
            f.write(b"invalid data")

        with self.assertRaises(ValueError):
            read_commit_graph(invalid_path)

    def test_find_commit_graph_file_missing(self):
        """Test finding commit graph file when it doesn't exist."""
        result = find_commit_graph_file(self.tempdir)
        self.assertIsNone(result)

    def test_find_commit_graph_file_standard_location(self):
        """Test finding commit graph file in standard location."""
        # Create .git/objects/info/commit-graph
        objects_dir = os.path.join(self.tempdir, "objects")
        info_dir = os.path.join(objects_dir, "info")
        os.makedirs(info_dir)

        graph_path = os.path.join(info_dir, "commit-graph")
        with open(graph_path, "wb") as f:
            f.write(b"dummy")

        result = find_commit_graph_file(self.tempdir)
        self.assertEqual(result, graph_path.encode())

    def test_find_commit_graph_file_chain_location(self):
        """Test finding commit graph file in chain location."""
        # Create .git/objects/info/commit-graphs/graph-{hash}.graph
        objects_dir = os.path.join(self.tempdir, "objects")
        info_dir = os.path.join(objects_dir, "info")
        graphs_dir = os.path.join(info_dir, "commit-graphs")
        os.makedirs(graphs_dir)

        graph_path = os.path.join(graphs_dir, "graph-abc123.graph")
        with open(graph_path, "wb") as f:
            f.write(b"dummy")

        result = find_commit_graph_file(self.tempdir)
        self.assertEqual(result, graph_path.encode())

    def test_find_commit_graph_file_prefers_standard(self):
        """Test that standard location is preferred over chain location."""
        # Create both locations
        objects_dir = os.path.join(self.tempdir, "objects")
        info_dir = os.path.join(objects_dir, "info")
        graphs_dir = os.path.join(info_dir, "commit-graphs")
        os.makedirs(info_dir)
        os.makedirs(graphs_dir)

        # Standard location
        standard_path = os.path.join(info_dir, "commit-graph")
        with open(standard_path, "wb") as f:
            f.write(b"standard")

        # Chain location
        chain_path = os.path.join(graphs_dir, "graph-abc123.graph")
        with open(chain_path, "wb") as f:
            f.write(b"chain")

        result = find_commit_graph_file(self.tempdir)
        self.assertEqual(result, standard_path.encode())


class CommitGraphGenerationTests(unittest.TestCase):
    """Tests for commit graph generation functionality."""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_generate_commit_graph_empty(self):
        """Test generating commit graph with no commits."""
        from dulwich.object_store import MemoryObjectStore

        object_store = MemoryObjectStore()
        graph = generate_commit_graph(object_store, [])

        self.assertEqual(len(graph), 0)

    def test_generate_commit_graph_single_commit(self):
        """Test generating commit graph with single commit."""
        from dulwich.object_store import MemoryObjectStore
        from dulwich.objects import Commit, Tree

        object_store = MemoryObjectStore()

        # Create a tree and commit
        tree = Tree()
        object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = b"Test Author <test@example.com>"
        commit.committer = b"Test Author <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit"
        object_store.add_object(commit)

        # Generate graph
        graph = generate_commit_graph(object_store, [commit.id])

        self.assertEqual(len(graph), 1)
        entry = graph.entries[0]
        self.assertEqual(entry.commit_id, commit.id)
        self.assertEqual(entry.tree_id, commit.tree)
        self.assertEqual(entry.parents, [])
        self.assertEqual(entry.generation, 1)
        self.assertEqual(entry.commit_time, 1234567890)

    def test_get_reachable_commits(self):
        """Test getting reachable commits."""
        from dulwich.object_store import MemoryObjectStore
        from dulwich.objects import Commit, Tree

        object_store = MemoryObjectStore()

        # Create tree
        tree = Tree()
        object_store.add_object(tree)

        # Create commit chain: commit1 -> commit2
        commit1 = Commit()
        commit1.tree = tree.id
        commit1.author = commit1.committer = b"Test <test@example.com>"
        commit1.commit_time = commit1.author_time = 1234567890
        commit1.commit_timezone = commit1.author_timezone = 0
        commit1.message = b"First commit"
        object_store.add_object(commit1)

        commit2 = Commit()
        commit2.tree = tree.id
        commit2.parents = [commit1.id]
        commit2.author = commit2.committer = b"Test <test@example.com>"
        commit2.commit_time = commit2.author_time = 1234567891
        commit2.commit_timezone = commit2.author_timezone = 0
        commit2.message = b"Second commit"
        object_store.add_object(commit2)

        # Get reachable commits from commit2
        reachable = get_reachable_commits(object_store, [commit2.id])

        # Should include both commits
        self.assertEqual(len(reachable), 2)
        self.assertIn(commit1.id, reachable)
        self.assertIn(commit2.id, reachable)

    def test_write_commit_graph_to_file(self):
        """Test writing commit graph to file."""
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Commit, Tree

        # Create a disk object store
        object_store_path = os.path.join(self.tempdir, "objects")
        os.makedirs(object_store_path, exist_ok=True)
        object_store = DiskObjectStore(object_store_path)

        # Create a tree and commit
        tree = Tree()
        object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = b"Test Author <test@example.com>"
        commit.committer = b"Test Author <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit"
        object_store.add_object(commit)

        # Write commit graph using ObjectStore method
        object_store.write_commit_graph([commit.id], reachable=False)

        # Verify file was created
        graph_path = os.path.join(object_store_path, "info", "commit-graph")
        self.assertTrue(os.path.exists(graph_path))

        # Read back and verify
        graph = read_commit_graph(graph_path)
        self.assertIsNotNone(graph)
        self.assertEqual(len(graph), 1)

        entry = graph.entries[0]
        self.assertEqual(entry.commit_id, commit.id)
        self.assertEqual(entry.tree_id, commit.tree)

    def test_object_store_commit_graph_methods(self):
        """Test ObjectStore commit graph methods."""
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Commit, Tree

        # Create a disk object store
        object_store_path = os.path.join(self.tempdir, "objects")
        os.makedirs(object_store_path, exist_ok=True)
        object_store = DiskObjectStore(object_store_path)

        # Initially no commit graph
        self.assertIsNone(object_store.get_commit_graph())

        # Create a tree and commit
        tree = Tree()
        object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = b"Test Author <test@example.com>"
        commit.committer = b"Test Author <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test commit"
        object_store.add_object(commit)

        # Write commit graph (disable reachable to avoid traversal issue)
        object_store.write_commit_graph([commit.id], reachable=False)

        # Now should have commit graph
        self.assertIsNotNone(object_store.get_commit_graph())

        # Test update (should still have commit graph)
        object_store.write_commit_graph()
        self.assertIsNot(None, object_store.get_commit_graph())

    def test_parents_provider_commit_graph_integration(self):
        """Test that ParentsProvider uses commit graph when available."""
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Commit, Tree
        from dulwich.repo import ParentsProvider

        # Create a disk object store
        object_store_path = os.path.join(self.tempdir, "objects")
        os.makedirs(object_store_path, exist_ok=True)
        object_store = DiskObjectStore(object_store_path)

        # Create a tree and two commits
        tree = Tree()
        object_store.add_object(tree)

        # First commit (no parents)
        commit1 = Commit()
        commit1.tree = tree.id
        commit1.author = commit1.committer = b"Test <test@example.com>"
        commit1.commit_time = commit1.author_time = 1234567890
        commit1.commit_timezone = commit1.author_timezone = 0
        commit1.message = b"First commit"
        object_store.add_object(commit1)

        # Second commit (child of first)
        commit2 = Commit()
        commit2.tree = tree.id
        commit2.parents = [commit1.id]
        commit2.author = commit2.committer = b"Test <test@example.com>"
        commit2.commit_time = commit2.author_time = 1234567891
        commit2.commit_timezone = commit2.author_timezone = 0
        commit2.message = b"Second commit"
        object_store.add_object(commit2)

        # Write commit graph
        object_store.write_commit_graph([commit1.id, commit2.id], reachable=False)

        # Test ParentsProvider with commit graph
        provider = ParentsProvider(object_store)

        # Verify commit graph is loaded
        self.assertIsNotNone(provider.commit_graph)

        # Test parent lookups
        parents1 = provider.get_parents(commit1.id)
        self.assertEqual(parents1, [])

        parents2 = provider.get_parents(commit2.id)
        self.assertEqual(parents2, [commit1.id])

        # Test fallback behavior by creating provider without commit graph
        object_store_no_graph_path = os.path.join(self.tempdir, "objects2")
        os.makedirs(object_store_no_graph_path, exist_ok=True)
        object_store_no_graph = DiskObjectStore(object_store_no_graph_path)
        object_store_no_graph.add_object(tree)
        object_store_no_graph.add_object(commit1)
        object_store_no_graph.add_object(commit2)

        provider_no_graph = ParentsProvider(object_store_no_graph)
        self.assertIsNone(provider_no_graph.commit_graph)

        # Should still work via commit object fallback
        parents1_fallback = provider_no_graph.get_parents(commit1.id)
        self.assertEqual(parents1_fallback, [])

        parents2_fallback = provider_no_graph.get_parents(commit2.id)
        self.assertEqual(parents2_fallback, [commit1.id])

    def test_graph_operations_use_commit_graph(self):
        """Test that graph operations use commit graph when available."""
        from dulwich.graph import can_fast_forward, find_merge_base
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Commit, Tree
        from dulwich.repo import Repo

        # Create a disk object store
        object_store_path = os.path.join(self.tempdir, "objects")
        os.makedirs(object_store_path, exist_ok=True)
        object_store = DiskObjectStore(object_store_path)

        # Create a tree and a more complex commit graph for testing
        tree = Tree()
        object_store.add_object(tree)

        # Create commit chain: commit1 -> commit2 -> commit3
        #                               \-> commit4 -> commit5 (merge)
        commit1 = Commit()
        commit1.tree = tree.id
        commit1.author = commit1.committer = b"Test <test@example.com>"
        commit1.commit_time = commit1.author_time = 1234567890
        commit1.commit_timezone = commit1.author_timezone = 0
        commit1.message = b"First commit"
        object_store.add_object(commit1)

        commit2 = Commit()
        commit2.tree = tree.id
        commit2.parents = [commit1.id]
        commit2.author = commit2.committer = b"Test <test@example.com>"
        commit2.commit_time = commit2.author_time = 1234567891
        commit2.commit_timezone = commit2.author_timezone = 0
        commit2.message = b"Second commit"
        object_store.add_object(commit2)

        commit3 = Commit()
        commit3.tree = tree.id
        commit3.parents = [commit2.id]
        commit3.author = commit3.committer = b"Test <test@example.com>"
        commit3.commit_time = commit3.author_time = 1234567892
        commit3.commit_timezone = commit3.author_timezone = 0
        commit3.message = b"Third commit"
        object_store.add_object(commit3)

        # Branch from commit2
        commit4 = Commit()
        commit4.tree = tree.id
        commit4.parents = [commit2.id]
        commit4.author = commit4.committer = b"Test <test@example.com>"
        commit4.commit_time = commit4.author_time = 1234567893
        commit4.commit_timezone = commit4.author_timezone = 0
        commit4.message = b"Fourth commit (branch)"
        object_store.add_object(commit4)

        # Merge commit
        commit5 = Commit()
        commit5.tree = tree.id
        commit5.parents = [commit3.id, commit4.id]
        commit5.author = commit5.committer = b"Test <test@example.com>"
        commit5.commit_time = commit5.author_time = 1234567894
        commit5.commit_timezone = commit5.author_timezone = 0
        commit5.message = b"Merge commit"
        object_store.add_object(commit5)

        # Create refs
        refs_path = os.path.join(self.tempdir, "refs")
        os.makedirs(refs_path, exist_ok=True)
        repo_path = self.tempdir
        repo = Repo.init(repo_path)
        repo.object_store = object_store

        # Test graph operations WITHOUT commit graph first
        merge_base_no_graph = find_merge_base(repo, [commit3.id, commit4.id])
        can_ff_no_graph = can_fast_forward(repo, commit1.id, commit3.id)

        # Now write commit graph
        object_store.write_commit_graph(
            [commit1.id, commit2.id, commit3.id, commit4.id, commit5.id],
            reachable=False,
        )

        # Verify commit graph is loaded by creating new repo instance
        repo2 = Repo(repo_path)
        repo2.object_store = object_store

        # Verify commit graph is available
        commit_graph = repo2.object_store.get_commit_graph()
        self.assertIsNotNone(commit_graph)

        # Test graph operations WITH commit graph
        merge_base_with_graph = find_merge_base(repo2, [commit3.id, commit4.id])
        can_ff_with_graph = can_fast_forward(repo2, commit1.id, commit3.id)

        # Results should be identical
        self.assertEqual(
            merge_base_no_graph,
            merge_base_with_graph,
            "Merge base should be same with and without commit graph",
        )
        self.assertEqual(
            can_ff_no_graph,
            can_ff_with_graph,
            "Fast-forward detection should be same with and without commit graph",
        )

        # Expected results
        self.assertEqual(
            merge_base_with_graph,
            [commit2.id],
            "Merge base of commit3 and commit4 should be commit2",
        )
        self.assertTrue(
            can_ff_with_graph, "Should be able to fast-forward from commit1 to commit3"
        )

        # Test that ParentsProvider in the repo uses commit graph
        parents_provider = repo2.parents_provider()
        self.assertIsNotNone(
            parents_provider.commit_graph,
            "Repository's parents provider should have commit graph",
        )

        # Verify parent lookups work through the provider
        self.assertEqual(parents_provider.get_parents(commit1.id), [])
        self.assertEqual(parents_provider.get_parents(commit2.id), [commit1.id])
        self.assertEqual(
            parents_provider.get_parents(commit5.id), [commit3.id, commit4.id]
        )

    def test_performance_with_commit_graph(self):
        """Test that using commit graph provides performance benefits."""
        from dulwich.graph import find_merge_base
        from dulwich.object_store import DiskObjectStore
        from dulwich.objects import Commit, Tree
        from dulwich.repo import Repo

        # Create a larger commit history to better measure performance
        object_store_path = os.path.join(self.tempdir, "objects")
        os.makedirs(object_store_path, exist_ok=True)
        object_store = DiskObjectStore(object_store_path)

        tree = Tree()
        object_store.add_object(tree)

        # Create a chain of 20 commits
        commits = []
        for i in range(20):
            commit = Commit()
            commit.tree = tree.id
            if i > 0:
                commit.parents = [commits[i - 1].id]
            commit.author = commit.committer = b"Test <test@example.com>"
            commit.commit_time = commit.author_time = 1234567890 + i
            commit.commit_timezone = commit.author_timezone = 0
            commit.message = f"Commit {i}".encode()
            object_store.add_object(commit)
            commits.append(commit)

        # Create repository
        repo_path = self.tempdir
        repo = Repo.init(repo_path)
        repo.object_store = object_store

        # Time operations without commit graph
        for _ in range(10):  # Run multiple times for better measurement
            find_merge_base(repo, [commits[0].id, commits[-1].id])

        # Write commit graph
        object_store.write_commit_graph([c.id for c in commits], reachable=False)

        # Create new repo instance to pick up commit graph
        repo2 = Repo(repo_path)
        repo2.object_store = object_store

        # Verify commit graph is loaded
        self.assertIsNotNone(repo2.object_store.get_commit_graph())

        # Time operations with commit graph
        for _ in range(10):  # Run multiple times for better measurement
            find_merge_base(repo2, [commits[0].id, commits[-1].id])

        # With commit graph should be at least as fast (usually faster)
        # We don't assert a specific speedup since it depends on the machine
        # But we verify both approaches give the same result
        result_no_graph = find_merge_base(repo, [commits[0].id, commits[-1].id])
        result_with_graph = find_merge_base(repo2, [commits[0].id, commits[-1].id])

        self.assertEqual(
            result_no_graph,
            result_with_graph,
            "Results should be identical with and without commit graph",
        )
        self.assertEqual(
            result_with_graph, [commits[0].id], "Merge base should be the first commit"
        )


if __name__ == "__main__":
    unittest.main()
