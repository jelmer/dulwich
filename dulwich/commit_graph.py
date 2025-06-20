# commit_graph.py -- Git commit graph file format support
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.

"""Git commit graph file format support.

Git's commit graph files store commit metadata and generation numbers
for faster graph traversal operations like merge-base computation.

The commit graph format is documented at:
https://git-scm.com/docs/gitformat-commit-graph
"""

import os
import struct
from collections.abc import Iterator
from typing import TYPE_CHECKING, BinaryIO, Optional, Union

if TYPE_CHECKING:
    from .object_store import BaseObjectStore

from .objects import Commit, ObjectID, hex_to_sha, sha_to_hex

# File format constants
COMMIT_GRAPH_SIGNATURE = b"CGPH"
COMMIT_GRAPH_VERSION = 1
HASH_VERSION_SHA1 = 1
HASH_VERSION_SHA256 = 2

# Chunk IDs
CHUNK_OID_FANOUT = b"OIDF"
CHUNK_OID_LOOKUP = b"OIDL"
CHUNK_COMMIT_DATA = b"CDAT"
CHUNK_GENERATION_DATA = b"GDA2"
CHUNK_GENERATION_DATA_OVERFLOW = b"GDO2"
CHUNK_EXTRA_EDGE_LIST = b"EDGE"
CHUNK_BLOOM_FILTER_INDEX = b"BIDX"
CHUNK_BLOOM_FILTER_DATA = b"BDAT"
CHUNK_BASE_GRAPHS_LIST = b"BASE"

# Generation number constants
GENERATION_NUMBER_INFINITY = 0xFFFFFFFF
GENERATION_NUMBER_ZERO = 0
GENERATION_NUMBER_V1_MAX = 0x3FFFFFFF

# Parent encoding constants
GRAPH_PARENT_MISSING = 0x70000000
GRAPH_PARENT_NONE = 0x70000000
GRAPH_EXTRA_EDGES_NEEDED = 0x80000000
GRAPH_LAST_EDGE = 0x80000000


class CommitGraphEntry:
    """Represents a single commit entry in the commit graph."""

    def __init__(
        self,
        commit_id: ObjectID,
        tree_id: ObjectID,
        parents: list[ObjectID],
        generation: int,
        commit_time: int,
    ) -> None:
        self.commit_id = commit_id
        self.tree_id = tree_id
        self.parents = parents
        self.generation = generation
        self.commit_time = commit_time

    def __repr__(self) -> str:
        return (
            f"CommitGraphEntry(commit_id={self.commit_id!r}, "
            f"tree_id={self.tree_id!r}, parents={self.parents!r}, "
            f"generation={self.generation}, commit_time={self.commit_time})"
        )


class CommitGraphChunk:
    """Represents a chunk in the commit graph file."""

    def __init__(self, chunk_id: bytes, data: bytes) -> None:
        self.chunk_id = chunk_id
        self.data = data

    def __repr__(self) -> str:
        return f"CommitGraphChunk(chunk_id={self.chunk_id!r}, size={len(self.data)})"


class CommitGraph:
    """Git commit graph file reader/writer."""

    def __init__(self, hash_version: int = HASH_VERSION_SHA1) -> None:
        self.hash_version = hash_version
        self.chunks: dict[bytes, CommitGraphChunk] = {}
        self.entries: list[CommitGraphEntry] = []
        self._oid_to_index: dict[ObjectID, int] = {}
        self._hash_size = 20 if hash_version == HASH_VERSION_SHA1 else 32

    @classmethod
    def from_file(cls, f: BinaryIO) -> "CommitGraph":
        """Read commit graph from file."""
        graph = cls()
        graph._read_from_file(f)
        return graph

    def _read_from_file(self, f: BinaryIO) -> None:
        """Read commit graph data from file."""
        # Read header
        signature = f.read(4)
        if signature != COMMIT_GRAPH_SIGNATURE:
            raise ValueError(f"Invalid commit graph signature: {signature!r}")

        version = struct.unpack(">B", f.read(1))[0]
        if version != COMMIT_GRAPH_VERSION:
            raise ValueError(f"Unsupported commit graph version: {version}")

        self.hash_version = struct.unpack(">B", f.read(1))[0]
        if self.hash_version not in (HASH_VERSION_SHA1, HASH_VERSION_SHA256):
            raise ValueError(f"Unsupported hash version: {self.hash_version}")

        self._hash_size = 20 if self.hash_version == HASH_VERSION_SHA1 else 32

        num_chunks = struct.unpack(">B", f.read(1))[0]
        struct.unpack(">B", f.read(1))[0]

        # Read table of contents
        toc_entries = []
        for _ in range(num_chunks + 1):  # +1 for terminating entry
            chunk_id = f.read(4)
            offset = struct.unpack(">Q", f.read(8))[0]
            toc_entries.append((chunk_id, offset))

        # Read chunks
        # Offsets in TOC are absolute from start of file
        for i in range(num_chunks):
            chunk_id, offset = toc_entries[i]
            next_offset = toc_entries[i + 1][1]
            chunk_size = next_offset - offset

            f.seek(offset)
            chunk_data = f.read(chunk_size)
            self.chunks[chunk_id] = CommitGraphChunk(chunk_id, chunk_data)

        # Parse chunks
        self._parse_chunks()

    def _parse_chunks(self) -> None:
        """Parse chunk data into entries."""
        if CHUNK_OID_LOOKUP not in self.chunks:
            raise ValueError("Missing required OID lookup chunk")
        if CHUNK_COMMIT_DATA not in self.chunks:
            raise ValueError("Missing required commit data chunk")

        # Parse OID lookup chunk
        oid_lookup_data = self.chunks[CHUNK_OID_LOOKUP].data
        num_commits = len(oid_lookup_data) // self._hash_size

        oids = []
        for i in range(num_commits):
            start = i * self._hash_size
            end = start + self._hash_size
            oid = oid_lookup_data[start:end]
            oids.append(oid)
            self._oid_to_index[oid] = i

        # Parse commit data chunk
        commit_data = self.chunks[CHUNK_COMMIT_DATA].data
        expected_size = num_commits * (self._hash_size + 16)
        if len(commit_data) != expected_size:
            raise ValueError(
                f"Invalid commit data chunk size: {len(commit_data)}, expected {expected_size}"
            )

        self.entries = []
        for i in range(num_commits):
            offset = i * (self._hash_size + 16)

            # Tree OID
            tree_id = commit_data[offset : offset + self._hash_size]
            offset += self._hash_size

            # Parent positions (2 x 4 bytes)
            parent1_pos, parent2_pos = struct.unpack(
                ">LL", commit_data[offset : offset + 8]
            )
            offset += 8

            # Generation number and commit time (2 x 4 bytes)
            gen_and_time = struct.unpack(">LL", commit_data[offset : offset + 8])
            generation = gen_and_time[0] >> 2  # Upper 30 bits
            commit_time = gen_and_time[1] | (
                (gen_and_time[0] & 0x3) << 32
            )  # 34 bits total

            # Parse parents
            parents = []
            if parent1_pos < GRAPH_PARENT_MISSING:
                if parent1_pos >= len(oids):
                    raise ValueError(f"Invalid parent1 position: {parent1_pos}")
                parents.append(oids[parent1_pos])

            if parent2_pos < GRAPH_PARENT_MISSING:
                if parent2_pos >= len(oids):
                    raise ValueError(f"Invalid parent2 position: {parent2_pos}")
                parents.append(oids[parent2_pos])
            elif parent2_pos >= GRAPH_EXTRA_EDGES_NEEDED:
                # Handle extra edges (3+ parents)
                edge_offset = parent2_pos & ~GRAPH_EXTRA_EDGES_NEEDED
                parents.extend(self._parse_extra_edges(edge_offset, oids))

            entry = CommitGraphEntry(
                commit_id=sha_to_hex(oids[i]),
                tree_id=sha_to_hex(tree_id),
                parents=[sha_to_hex(p) for p in parents],
                generation=generation,
                commit_time=commit_time,
            )
            self.entries.append(entry)

    def _parse_extra_edges(self, offset: int, oids: list[bytes]) -> list[bytes]:
        """Parse extra parent edges for commits with 3+ parents."""
        if CHUNK_EXTRA_EDGE_LIST not in self.chunks:
            return []

        edge_data = self.chunks[CHUNK_EXTRA_EDGE_LIST].data
        parents = []

        while offset < len(edge_data):
            parent_pos = struct.unpack(">L", edge_data[offset : offset + 4])[0]
            offset += 4

            if parent_pos & GRAPH_LAST_EDGE:
                parent_pos &= ~GRAPH_LAST_EDGE
                if parent_pos < len(oids):
                    parents.append(oids[parent_pos])
                break
            else:
                if parent_pos < len(oids):
                    parents.append(oids[parent_pos])

        return parents

    def get_entry_by_oid(self, oid: ObjectID) -> Optional[CommitGraphEntry]:
        """Get commit graph entry by commit OID."""
        # Convert hex ObjectID to binary if needed for lookup
        if isinstance(oid, bytes) and len(oid) == 40:
            # Input is hex ObjectID, convert to binary for internal lookup
            lookup_oid = hex_to_sha(oid)
        else:
            # Input is already binary
            lookup_oid = oid
        index = self._oid_to_index.get(lookup_oid)
        if index is not None:
            return self.entries[index]
        return None

    def get_generation_number(self, oid: ObjectID) -> Optional[int]:
        """Get generation number for a commit."""
        entry = self.get_entry_by_oid(oid)
        return entry.generation if entry else None

    def get_parents(self, oid: ObjectID) -> Optional[list[ObjectID]]:
        """Get parent commit IDs for a commit."""
        entry = self.get_entry_by_oid(oid)
        return entry.parents if entry else None

    def write_to_file(self, f: BinaryIO) -> None:
        """Write commit graph to file."""
        if not self.entries:
            raise ValueError("Cannot write empty commit graph")

        # Sort entries by commit ID for consistent output
        sorted_entries = sorted(self.entries, key=lambda e: e.commit_id)

        # Build OID lookup chunk
        oid_lookup_data = b""
        for entry in sorted_entries:
            oid_lookup_data += hex_to_sha(entry.commit_id)

        # Build commit data chunk
        commit_data = b""
        # Create OID to index mapping for parent lookups
        oid_to_index = {entry.commit_id: i for i, entry in enumerate(sorted_entries)}

        for entry in sorted_entries:
            # Tree OID (20 bytes)
            commit_data += hex_to_sha(entry.tree_id)

            # Parent positions (2 x 4 bytes)
            if len(entry.parents) == 0:
                parent1_pos = GRAPH_PARENT_MISSING
                parent2_pos = GRAPH_PARENT_MISSING
            elif len(entry.parents) == 1:
                parent1_pos = oid_to_index.get(entry.parents[0], GRAPH_PARENT_MISSING)
                parent2_pos = GRAPH_PARENT_MISSING
            elif len(entry.parents) == 2:
                parent1_pos = oid_to_index.get(entry.parents[0], GRAPH_PARENT_MISSING)
                parent2_pos = oid_to_index.get(entry.parents[1], GRAPH_PARENT_MISSING)
            else:
                # More than 2 parents - would need extra edge list chunk
                # For now, just store first two parents
                parent1_pos = oid_to_index.get(entry.parents[0], GRAPH_PARENT_MISSING)
                parent2_pos = oid_to_index.get(entry.parents[1], GRAPH_PARENT_MISSING)

            commit_data += struct.pack(">LL", parent1_pos, parent2_pos)

            # Generation and commit time (2 x 4 bytes)
            gen_and_time = (entry.generation << 2) | (entry.commit_time >> 32)
            commit_time_lower = entry.commit_time & 0xFFFFFFFF
            commit_data += struct.pack(">LL", gen_and_time, commit_time_lower)

        # Build fanout table
        fanout_data = b""
        fanout_counts = [0] * 256
        for i, entry in enumerate(sorted_entries):
            commit_oid_bytes = hex_to_sha(entry.commit_id)
            fanout_counts[commit_oid_bytes[0]] = i + 1

        # Fill in gaps - each fanout entry should be cumulative
        for i in range(1, 256):
            if fanout_counts[i] == 0:
                fanout_counts[i] = fanout_counts[i - 1]

        for count in fanout_counts:
            fanout_data += struct.pack(">L", count)

        # Calculate chunk offsets
        header_size = (
            8  # signature + version + hash_version + num_chunks + base_graph_count
        )
        toc_size = 4 * 12  # 4 entries (3 chunks + terminator) * 12 bytes each

        chunk1_offset = header_size + toc_size  # OID Fanout
        chunk2_offset = chunk1_offset + len(fanout_data)  # OID Lookup
        chunk3_offset = chunk2_offset + len(oid_lookup_data)  # Commit Data
        terminator_offset = chunk3_offset + len(commit_data)

        # Write header
        f.write(COMMIT_GRAPH_SIGNATURE)
        f.write(struct.pack(">B", COMMIT_GRAPH_VERSION))
        f.write(struct.pack(">B", self.hash_version))
        f.write(struct.pack(">B", 3))  # 3 chunks
        f.write(struct.pack(">B", 0))  # 0 base graphs

        # Write table of contents
        f.write(CHUNK_OID_FANOUT + struct.pack(">Q", chunk1_offset))
        f.write(CHUNK_OID_LOOKUP + struct.pack(">Q", chunk2_offset))
        f.write(CHUNK_COMMIT_DATA + struct.pack(">Q", chunk3_offset))
        f.write(b"\x00\x00\x00\x00" + struct.pack(">Q", terminator_offset))

        # Write chunks
        f.write(fanout_data)
        f.write(oid_lookup_data)
        f.write(commit_data)

    def __len__(self) -> int:
        """Return number of commits in the graph."""
        return len(self.entries)

    def __iter__(self) -> Iterator["CommitGraphEntry"]:
        """Iterate over commit graph entries."""
        return iter(self.entries)


def read_commit_graph(path: Union[str, bytes]) -> Optional[CommitGraph]:
    """Read commit graph from file path."""
    if isinstance(path, str):
        path = path.encode()

    if not os.path.exists(path):
        return None

    with open(path, "rb") as f:
        return CommitGraph.from_file(f)


def find_commit_graph_file(git_dir: Union[str, bytes]) -> Optional[bytes]:
    """Find commit graph file in a Git repository."""
    if isinstance(git_dir, str):
        git_dir = git_dir.encode()

    # Standard location: .git/objects/info/commit-graph
    commit_graph_path = os.path.join(git_dir, b"objects", b"info", b"commit-graph")
    if os.path.exists(commit_graph_path):
        return commit_graph_path

    # Chain files in .git/objects/info/commit-graphs/
    commit_graphs_dir = os.path.join(git_dir, b"objects", b"info", b"commit-graphs")
    if os.path.exists(commit_graphs_dir):
        # Look for graph-{hash}.graph files
        for filename in os.listdir(commit_graphs_dir):
            if filename.startswith(b"graph-") and filename.endswith(b".graph"):
                return os.path.join(commit_graphs_dir, filename)

    return None


def generate_commit_graph(
    object_store: "BaseObjectStore", commit_ids: list[ObjectID]
) -> CommitGraph:
    """Generate a commit graph from a set of commits.

    Args:
        object_store: Object store to retrieve commits from
        commit_ids: List of commit IDs to include in the graph

    Returns:
        CommitGraph object containing the specified commits
    """
    graph = CommitGraph()

    if not commit_ids:
        return graph

    # Ensure all commit_ids are in the correct format for object store access
    # DiskObjectStore expects hex ObjectIDs (40-byte hex strings)
    normalized_commit_ids = []
    for commit_id in commit_ids:
        if isinstance(commit_id, bytes) and len(commit_id) == 40:
            # Already hex ObjectID
            normalized_commit_ids.append(commit_id)
        elif isinstance(commit_id, bytes) and len(commit_id) == 20:
            # Binary SHA, convert to hex ObjectID
            normalized_commit_ids.append(sha_to_hex(commit_id))
        else:
            # Assume it's already correct format
            normalized_commit_ids.append(commit_id)

    # Build a map of all commits and their metadata
    commit_map: dict[bytes, Commit] = {}
    for commit_id in normalized_commit_ids:
        try:
            commit_obj = object_store[commit_id]
            if commit_obj.type_name != b"commit":
                continue
            assert isinstance(commit_obj, Commit)
            commit_map[commit_id] = commit_obj
        except KeyError:
            # Commit not found, skip
            continue

    # Calculate generation numbers using topological sort
    generation_map: dict[bytes, int] = {}

    def calculate_generation(commit_id: ObjectID) -> int:
        if commit_id in generation_map:
            return generation_map[commit_id]

        if commit_id not in commit_map:
            # Unknown commit, assume generation 0
            generation_map[commit_id] = 0
            return 0

        commit_obj = commit_map[commit_id]
        if not commit_obj.parents:
            # Root commit
            generation_map[commit_id] = 1
            return 1

        # Calculate based on parents
        max_parent_gen = 0
        for parent_id in commit_obj.parents:
            parent_gen = calculate_generation(parent_id)
            max_parent_gen = max(max_parent_gen, parent_gen)

        generation = max_parent_gen + 1
        generation_map[commit_id] = generation
        return generation

    # Calculate generation numbers for all commits
    for commit_id in commit_map:
        calculate_generation(commit_id)

    # Build commit graph entries
    for commit_id, commit_obj in commit_map.items():
        # commit_id is already hex ObjectID from normalized_commit_ids
        commit_hex = commit_id

        # Handle tree ID - might already be hex ObjectID
        if isinstance(commit_obj.tree, bytes) and len(commit_obj.tree) == 40:
            tree_hex = commit_obj.tree  # Already hex ObjectID
        else:
            tree_hex = sha_to_hex(commit_obj.tree)  # Binary, convert to hex

        # Handle parent IDs - might already be hex ObjectIDs
        parents_hex = []
        for parent_id in commit_obj.parents:
            if isinstance(parent_id, bytes) and len(parent_id) == 40:
                parents_hex.append(parent_id)  # Already hex ObjectID
            else:
                parents_hex.append(sha_to_hex(parent_id))  # Binary, convert to hex

        entry = CommitGraphEntry(
            commit_id=commit_hex,
            tree_id=tree_hex,
            parents=parents_hex,
            generation=generation_map[commit_id],
            commit_time=commit_obj.commit_time,
        )
        graph.entries.append(entry)

    # Build the OID to index mapping for lookups
    graph._oid_to_index = {}
    for i, entry in enumerate(graph.entries):
        binary_oid = hex_to_sha(entry.commit_id.decode())
        graph._oid_to_index[binary_oid] = i

    return graph


def write_commit_graph(
    git_dir: Union[str, bytes],
    object_store: "BaseObjectStore",
    commit_ids: list[ObjectID],
) -> None:
    """Write a commit graph file for the given commits.

    Args:
        git_dir: Git directory path
        object_store: Object store to retrieve commits from
        commit_ids: List of commit IDs to include in the graph
    """
    if isinstance(git_dir, str):
        git_dir = git_dir.encode()

    # Generate the commit graph
    graph = generate_commit_graph(object_store, commit_ids)

    if not graph.entries:
        return  # Nothing to write

    # Ensure the objects/info directory exists
    info_dir = os.path.join(git_dir, b"objects", b"info")
    os.makedirs(info_dir, exist_ok=True)

    # Write using GitFile for atomic operation
    from .file import GitFile

    graph_path = os.path.join(info_dir, b"commit-graph")
    with GitFile(graph_path, "wb") as f:
        from typing import BinaryIO, cast

        graph.write_to_file(cast(BinaryIO, f))


def get_reachable_commits(
    object_store: "BaseObjectStore", start_commits: list[ObjectID]
) -> list[ObjectID]:
    """Get all commits reachable from the given starting commits.

    Args:
        object_store: Object store to retrieve commits from
        start_commits: List of starting commit IDs

    Returns:
        List of all reachable commit IDs (including the starting commits)
    """
    visited = set()
    reachable = []
    stack = []

    # Normalize commit IDs for object store access and tracking
    for commit_id in start_commits:
        if isinstance(commit_id, bytes) and len(commit_id) == 40:
            # Hex ObjectID - use directly for object store access
            if commit_id not in visited:
                stack.append(commit_id)
        elif isinstance(commit_id, bytes) and len(commit_id) == 20:
            # Binary SHA, convert to hex ObjectID for object store access
            hex_id = sha_to_hex(commit_id)
            if hex_id not in visited:
                stack.append(hex_id)
        else:
            # Assume it's already correct format
            if commit_id not in visited:
                stack.append(commit_id)

    while stack:
        commit_id = stack.pop()
        if commit_id in visited:
            continue

        visited.add(commit_id)

        try:
            commit_obj = object_store[commit_id]
            if not isinstance(commit_obj, Commit):
                continue

            # Add to reachable list (commit_id is already hex ObjectID)
            reachable.append(commit_id)

            # Add parents to stack
            for parent_id in commit_obj.parents:
                if parent_id not in visited:
                    stack.append(parent_id)
        except KeyError:
            # Commit not found, skip
            continue

    return reachable
