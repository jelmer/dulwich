# object_store.py -- Object store for git objects
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@jelmer.uk>
#                         and others
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


"""Git object store interfaces and implementation."""

__all__ = [
    "DEFAULT_TEMPFILE_GRACE_PERIOD",
    "INFODIR",
    "PACKDIR",
    "PACK_MODE",
    "BaseObjectStore",
    "BitmapReachability",
    "BucketBasedObjectStore",
    "DiskObjectStore",
    "GraphTraversalReachability",
    "GraphWalker",
    "MemoryObjectStore",
    "MissingObjectFinder",
    "ObjectIterator",
    "ObjectReachabilityProvider",
    "ObjectStoreGraphWalker",
    "OverlayObjectStore",
    "PackBasedObjectStore",
    "PackCapableObjectStore",
    "PackContainer",
    "commit_tree_changes",
    "find_shallow",
    "get_depth",
    "iter_commit_contents",
    "iter_tree_contents",
    "peel_sha",
    "read_packs_file",
    "tree_lookup_path",
]

import binascii
import os
import stat
import sys
import time
import warnings
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence, Set
from contextlib import suppress
from io import BytesIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Protocol,
    cast,
)

if TYPE_CHECKING:
    from .object_format import ObjectFormat

from .errors import NotTreeError
from .file import GitFile, _GitFile
from .midx import MultiPackIndex, load_midx
from .objects import (
    S_ISGITLINK,
    Blob,
    Commit,
    ObjectID,
    RawObjectID,
    ShaFile,
    Tag,
    Tree,
    TreeEntry,
    hex_to_filename,
    hex_to_sha,
    object_class,
    sha_to_hex,
    valid_hexsha,
)
from .pack import (
    PACK_SPOOL_FILE_MAX_SIZE,
    ObjectContainer,
    Pack,
    PackData,
    PackedObjectContainer,
    PackFileDisappeared,
    PackHint,
    PackIndexer,
    PackInflater,
    PackStreamCopier,
    UnpackedObject,
    extend_pack,
    full_unpacked_object,
    generate_unpacked_objects,
    iter_sha1,
    load_pack_index_file,
    pack_objects_to_data,
    write_pack_data,
    write_pack_index,
)
from .protocol import DEPTH_INFINITE, PEELED_TAG_SUFFIX
from .refs import Ref

if TYPE_CHECKING:
    from .bitmap import EWAHBitmap
    from .commit_graph import CommitGraph
    from .config import Config
    from .diff_tree import RenameDetector
    from .pack import Pack


class GraphWalker(Protocol):
    """Protocol for graph walker objects."""

    def __next__(self) -> ObjectID | None:
        """Return the next object SHA to visit."""
        ...

    def ack(self, sha: ObjectID) -> None:
        """Acknowledge that an object has been received."""
        ...

    def nak(self) -> None:
        """Nothing in common was found."""
        ...


class ObjectReachabilityProvider(Protocol):
    """Protocol for computing object reachability queries.

    This abstraction allows reachability computations to be backed by either
    naive graph traversal or optimized bitmap indexes, with a consistent interface.
    """

    def get_reachable_commits(
        self,
        heads: Iterable[ObjectID],
        exclude: Iterable[ObjectID] | None = None,
        shallow: Set[ObjectID] | None = None,
    ) -> set[ObjectID]:
        """Get all commits reachable from heads, excluding those in exclude.

        Args:
          heads: Starting commit SHAs
          exclude: Commit SHAs to exclude (and their ancestors)
          shallow: Set of shallow commit boundaries (traversal stops here)

        Returns:
          Set of commit SHAs reachable from heads but not from exclude
        """
        ...

    def get_reachable_objects(
        self,
        commits: Iterable[ObjectID],
        exclude_commits: Iterable[ObjectID] | None = None,
    ) -> set[ObjectID]:
        """Get all objects (commits + trees + blobs) reachable from commits.

        Args:
          commits: Starting commit SHAs
          exclude_commits: Commits whose objects should be excluded

        Returns:
          Set of all object SHAs (commits, trees, blobs, tags)
        """
        ...

    def get_tree_objects(
        self,
        tree_shas: Iterable[ObjectID],
    ) -> set[ObjectID]:
        """Get all trees and blobs reachable from the given trees.

        Args:
          tree_shas: Starting tree SHAs

        Returns:
          Set of tree and blob SHAs
        """
        ...


INFODIR = "info"
PACKDIR = "pack"

# use permissions consistent with Git; just readable by everyone
# TODO: should packs also be non-writable on Windows? if so, that
# would requite some rather significant adjustments to the test suite
PACK_MODE = 0o444 if sys.platform != "win32" else 0o644

# Grace period for cleaning up temporary pack files (in seconds)
# Matches git's default of 2 weeks
DEFAULT_TEMPFILE_GRACE_PERIOD = 14 * 24 * 60 * 60  # 2 weeks


def find_shallow(
    store: ObjectContainer, heads: Iterable[ObjectID], depth: int
) -> tuple[set[ObjectID], set[ObjectID]]:
    """Find shallow commits according to a given depth.

    Args:
      store: An ObjectStore for looking up objects.
      heads: Iterable of head SHAs to start walking from.
      depth: The depth of ancestors to include. A depth of one includes
        only the heads themselves.
    Returns: A tuple of (shallow, not_shallow), sets of SHAs that should be
        considered shallow and unshallow according to the arguments. Note that
        these sets may overlap if a commit is reachable along multiple paths.
    """
    parents: dict[ObjectID, list[ObjectID]] = {}
    commit_graph = store.get_commit_graph()

    def get_parents(sha: ObjectID) -> list[ObjectID]:
        result = parents.get(sha, None)
        if not result:
            # Try to use commit graph first if available
            if commit_graph:
                graph_parents = commit_graph.get_parents(sha)
                if graph_parents is not None:
                    result = graph_parents
                    parents[sha] = result
                    return result
            # Fall back to loading the object
            commit = store[sha]
            assert isinstance(commit, Commit)
            result = commit.parents
            parents[sha] = result
        return result

    todo = []  # stack of (sha, depth)
    for head_sha in heads:
        obj = store[head_sha]
        # Peel tags if necessary
        while isinstance(obj, Tag):
            _, sha = obj.object
            obj = store[sha]
        if isinstance(obj, Commit):
            todo.append((obj.id, 1))

    not_shallow = set()
    shallow = set()
    while todo:
        sha, cur_depth = todo.pop()
        if cur_depth < depth:
            not_shallow.add(sha)
            new_depth = cur_depth + 1
            todo.extend((p, new_depth) for p in get_parents(sha))
        else:
            shallow.add(sha)

    return shallow, not_shallow


def get_depth(
    store: ObjectContainer,
    head: ObjectID,
    get_parents: Callable[..., list[ObjectID]] = lambda commit: commit.parents,
    max_depth: int | None = None,
) -> int:
    """Return the current available depth for the given head.

    For commits with multiple parents, the largest possible depth will be
    returned.

    Args:
        store: Object store to search in
        head: commit to start from
        get_parents: optional function for getting the parents of a commit
        max_depth: maximum depth to search
    """
    if head not in store:
        return 0
    current_depth = 1
    queue = [(head, current_depth)]
    commit_graph = store.get_commit_graph()

    while queue and (max_depth is None or current_depth < max_depth):
        e, depth = queue.pop(0)
        current_depth = max(current_depth, depth)

        # Try to use commit graph for parent lookup if available
        parents = None
        if commit_graph:
            parents = commit_graph.get_parents(e)

        if parents is None:
            # Fall back to loading the object
            cmt = store[e]
            if isinstance(cmt, Tag):
                _cls, sha = cmt.object
                cmt = store[sha]
            parents = get_parents(cmt)

        queue.extend((parent, depth + 1) for parent in parents if parent in store)
    return current_depth


class PackContainer(Protocol):
    """Protocol for containers that can accept pack files."""

    def add_pack(self) -> tuple[BytesIO, Callable[[], None], Callable[[], None]]:
        """Add a new pack."""


class BaseObjectStore:
    """Object store interface."""

    def __init__(self, *, object_format: "ObjectFormat | None" = None) -> None:
        """Initialize object store.

        Args:
            object_format: Object format to use (defaults to DEFAULT_OBJECT_FORMAT)
        """
        from .object_format import DEFAULT_OBJECT_FORMAT

        self.object_format = object_format if object_format else DEFAULT_OBJECT_FORMAT

    def determine_wants_all(
        self, refs: Mapping[Ref, ObjectID], depth: int | None = None
    ) -> list[ObjectID]:
        """Determine which objects are wanted based on refs."""

        def _want_deepen(sha: ObjectID) -> bool:
            if not depth:
                return False
            if depth == DEPTH_INFINITE:
                return True
            return depth > self._get_depth(sha)

        return [
            sha
            for (ref, sha) in refs.items()
            if (sha not in self or _want_deepen(sha))
            and not ref.endswith(PEELED_TAG_SUFFIX)
        ]

    def contains_loose(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1 and is loose."""
        raise NotImplementedError(self.contains_loose)

    def contains_packed(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1 and is packed."""
        return False  # Default implementation for stores that don't support packing

    def __contains__(self, sha1: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1.

        This method makes no distinction between loose and packed objects.
        """
        return self.contains_loose(sha1)

    @property
    def packs(self) -> list[Pack]:
        """Iterable of pack objects."""
        raise NotImplementedError

    def get_raw(self, name: RawObjectID | ObjectID) -> tuple[int, bytes]:
        """Obtain the raw text for an object.

        Args:
          name: sha for the object.
        Returns: tuple with numeric type and object contents.
        """
        raise NotImplementedError(self.get_raw)

    def __getitem__(self, sha1: ObjectID | RawObjectID) -> ShaFile:
        """Obtain an object by SHA1."""
        type_num, uncomp = self.get_raw(sha1)
        return ShaFile.from_raw_string(
            type_num, uncomp, sha=sha1, object_format=self.object_format
        )

    def __iter__(self) -> Iterator[ObjectID]:
        """Iterate over the SHAs that are present in this store."""
        raise NotImplementedError(self.__iter__)

    def add_object(self, obj: ShaFile) -> None:
        """Add a single object to this object store."""
        raise NotImplementedError(self.add_object)

    def add_objects(
        self,
        objects: Sequence[tuple[ShaFile, str | None]],
        progress: Callable[..., None] | None = None,
    ) -> "Pack | None":
        """Add a set of objects to this object store.

        Args:
          objects: Iterable over a list of (object, path) tuples
          progress: Optional progress callback
        """
        raise NotImplementedError(self.add_objects)

    def get_reachability_provider(
        self, prefer_bitmap: bool = True
    ) -> ObjectReachabilityProvider:
        """Get a reachability provider for this object store.

        Returns an ObjectReachabilityProvider that can efficiently compute
        object reachability queries. Subclasses can override this to provide
        optimized implementations (e.g., using bitmap indexes).

        Args:
            prefer_bitmap: Whether to prefer bitmap-based reachability if
                available.

        Returns:
          ObjectReachabilityProvider instance
        """
        return GraphTraversalReachability(self)

    def tree_changes(
        self,
        source: ObjectID | None,
        target: ObjectID | None,
        want_unchanged: bool = False,
        include_trees: bool = False,
        change_type_same: bool = False,
        rename_detector: "RenameDetector | None" = None,
        paths: Sequence[bytes] | None = None,
    ) -> Iterator[
        tuple[
            tuple[bytes | None, bytes | None],
            tuple[int | None, int | None],
            tuple[ObjectID | None, ObjectID | None],
        ]
    ]:
        """Find the differences between the contents of two trees.

        Args:
          source: SHA1 of the source tree
          target: SHA1 of the target tree
          want_unchanged: Whether unchanged files should be reported
          include_trees: Whether to include trees
          change_type_same: Whether to report files changing
            type in the same entry.
          rename_detector: RenameDetector object for detecting renames.
          paths: Optional list of paths to filter to (as bytes).
        Returns: Iterator over tuples with
            (oldpath, newpath), (oldmode, newmode), (oldsha, newsha)
        """
        from .diff_tree import tree_changes

        for change in tree_changes(
            self,
            source,
            target,
            want_unchanged=want_unchanged,
            include_trees=include_trees,
            change_type_same=change_type_same,
            rename_detector=rename_detector,
            paths=paths,
        ):
            old_path = change.old.path if change.old is not None else None
            new_path = change.new.path if change.new is not None else None
            old_mode = change.old.mode if change.old is not None else None
            new_mode = change.new.mode if change.new is not None else None
            old_sha = change.old.sha if change.old is not None else None
            new_sha = change.new.sha if change.new is not None else None
            yield (
                (old_path, new_path),
                (old_mode, new_mode),
                (old_sha, new_sha),
            )

    def iter_tree_contents(
        self, tree_id: ObjectID, include_trees: bool = False
    ) -> Iterator[TreeEntry]:
        """Iterate the contents of a tree and all subtrees.

        Iteration is depth-first pre-order, as in e.g. os.walk.

        Args:
          tree_id: SHA1 of the tree.
          include_trees: If True, include tree objects in the iteration.
        Returns: Iterator over TreeEntry namedtuples for all the objects in a
            tree.
        """
        warnings.warn(
            "Please use dulwich.object_store.iter_tree_contents",
            DeprecationWarning,
            stacklevel=2,
        )
        return iter_tree_contents(self, tree_id, include_trees=include_trees)

    def iterobjects_subset(
        self, shas: Iterable[ObjectID], *, allow_missing: bool = False
    ) -> Iterator[ShaFile]:
        """Iterate over a subset of objects in the store.

        Args:
          shas: Iterable of object SHAs to retrieve
          allow_missing: If True, skip missing objects; if False, raise KeyError

        Returns:
          Iterator of ShaFile objects

        Raises:
          KeyError: If an object is missing and allow_missing is False
        """
        for sha in shas:
            try:
                yield self[sha]
            except KeyError:
                if not allow_missing:
                    raise

    def iter_unpacked_subset(
        self,
        shas: Iterable[ObjectID | RawObjectID],
        include_comp: bool = False,
        allow_missing: bool = False,
        convert_ofs_delta: bool = True,
    ) -> "Iterator[UnpackedObject]":
        """Iterate over unpacked objects for a subset of SHAs.

        Default implementation that converts ShaFile objects to UnpackedObject.
        Subclasses may override for more efficient unpacked access.

        Args:
          shas: Iterable of object SHAs to retrieve
          include_comp: Whether to include compressed data (ignored in base
            implementation)
          allow_missing: If True, skip missing objects; if False, raise
            KeyError
          convert_ofs_delta: Whether to convert OFS_DELTA objects (ignored in
            base implementation)

        Returns:
          Iterator of UnpackedObject instances

        Raises:
          KeyError: If an object is missing and allow_missing is False
        """
        from .pack import UnpackedObject

        for sha in shas:
            try:
                obj = self[sha]
                # Convert ShaFile to UnpackedObject
                unpacked = UnpackedObject(
                    obj.type_num, decomp_chunks=obj.as_raw_chunks(), sha=obj.id
                )
                yield unpacked
            except KeyError:
                if not allow_missing:
                    raise

    def find_missing_objects(
        self,
        haves: Iterable[ObjectID],
        wants: Iterable[ObjectID],
        shallow: Set[ObjectID] | None = None,
        progress: Callable[..., None] | None = None,
        get_tagged: Callable[[], dict[ObjectID, ObjectID]] | None = None,
        get_parents: Callable[..., list[ObjectID]] = lambda commit: commit.parents,
    ) -> Iterator[tuple[ObjectID, PackHint | None]]:
        """Find the missing objects required for a set of revisions.

        Args:
          haves: Iterable over SHAs already in common.
          wants: Iterable over SHAs of objects to fetch.
          shallow: Set of shallow commit SHA1s to skip
          progress: Simple progress function that will be called with
            updated progress strings.
          get_tagged: Function that returns a dict of pointed-to sha ->
            tag sha for including tags.
          get_parents: Optional function for getting the parents of a
            commit.
        Returns: Iterator over (sha, path) pairs.
        """
        warnings.warn("Please use MissingObjectFinder(store)", DeprecationWarning)
        finder = MissingObjectFinder(
            self,
            haves=haves,
            wants=wants,
            shallow=shallow,
            progress=progress,
            get_tagged=get_tagged,
            get_parents=get_parents,
        )
        return iter(finder)

    def find_common_revisions(self, graphwalker: GraphWalker) -> list[ObjectID]:
        """Find which revisions this store has in common using graphwalker.

        Args:
          graphwalker: A graphwalker object.
        Returns: List of SHAs that are in common
        """
        haves = []
        sha = next(graphwalker)
        while sha:
            if sha in self:
                haves.append(sha)
                graphwalker.ack(sha)
            sha = next(graphwalker)
        return haves

    def generate_pack_data(
        self,
        have: Iterable[ObjectID],
        want: Iterable[ObjectID],
        *,
        shallow: Set[ObjectID] | None = None,
        progress: Callable[..., None] | None = None,
        ofs_delta: bool = True,
    ) -> tuple[int, Iterator[UnpackedObject]]:
        """Generate pack data objects for a set of wants/haves.

        Args:
          have: List of SHA1s of objects that should not be sent
          want: List of SHA1s of objects that should be sent
          shallow: Set of shallow commit SHA1s to skip
          ofs_delta: Whether OFS deltas can be included
          progress: Optional progress reporting method
        """
        # Note that the pack-specific implementation below is more efficient,
        # as it reuses deltas
        missing_objects = MissingObjectFinder(
            self, haves=have, wants=want, shallow=shallow, progress=progress
        )
        object_ids = list(missing_objects)
        return pack_objects_to_data(
            [(self[oid], path) for oid, path in object_ids],
            ofs_delta=ofs_delta,
            progress=progress,
        )

    def peel_sha(self, sha: ObjectID | RawObjectID) -> ObjectID:
        """Peel all tags from a SHA.

        Args:
          sha: The object SHA to peel.
        Returns: The fully-peeled SHA1 of a tag object, after peeling all
            intermediate tags; if the original ref does not point to a tag,
            this will equal the original SHA1.
        """
        warnings.warn(
            "Please use dulwich.object_store.peel_sha()",
            DeprecationWarning,
            stacklevel=2,
        )
        return peel_sha(self, sha)[1].id

    def _get_depth(
        self,
        head: ObjectID,
        get_parents: Callable[..., list[ObjectID]] = lambda commit: commit.parents,
        max_depth: int | None = None,
    ) -> int:
        """Return the current available depth for the given head.

        For commits with multiple parents, the largest possible depth will be
        returned.

        Args:
            head: commit to start from
            get_parents: optional function for getting the parents of a commit
            max_depth: maximum depth to search
        """
        return get_depth(self, head, get_parents=get_parents, max_depth=max_depth)

    def close(self) -> None:
        """Close any files opened by this object store."""
        # Default implementation is a NO-OP

    def prune(self, grace_period: int | None = None) -> None:
        """Prune/clean up this object store.

        This includes removing orphaned temporary files and other
        housekeeping tasks. Default implementation is a NO-OP.

        Args:
          grace_period: Grace period in seconds for removing temporary files.
                       If None, uses the default grace period.
        """
        # Default implementation is a NO-OP

    def iter_prefix(self, prefix: bytes) -> Iterator[ObjectID]:
        """Iterate over all SHA1s that start with a given prefix.

        The default implementation is a naive iteration over all objects.
        However, subclasses may override this method with more efficient
        implementations.
        """
        for sha in self:
            if sha.startswith(prefix):
                yield sha

    def get_commit_graph(self) -> "CommitGraph | None":
        """Get the commit graph for this object store.

        Returns:
          CommitGraph object if available, None otherwise
        """
        return None

    def write_commit_graph(
        self, refs: Iterable[ObjectID] | None = None, reachable: bool = True
    ) -> None:
        """Write a commit graph file for this object store.

        Args:
            refs: List of refs to include. If None, includes all refs from object store.
            reachable: If True, includes all commits reachable from refs.
                      If False, only includes the direct ref targets.

        Note:
            Default implementation does nothing. Subclasses should override
            this method to provide commit graph writing functionality.
        """
        raise NotImplementedError(self.write_commit_graph)

    def get_object_mtime(self, sha: ObjectID) -> float:
        """Get the modification time of an object.

        Args:
          sha: SHA1 of the object

        Returns:
          Modification time as seconds since epoch

        Raises:
          KeyError: if the object is not found
        """
        # Default implementation raises KeyError
        # Subclasses should override to provide actual mtime
        raise KeyError(sha)


class PackCapableObjectStore(BaseObjectStore, PackedObjectContainer):
    """Object store that supports pack operations.

    This is a base class for object stores that can handle pack files,
    including both disk-based and memory-based stores.
    """

    def add_pack(self) -> tuple[BinaryIO, Callable[[], None], Callable[[], None]]:
        """Add a new pack to this object store.

        Returns: Tuple of (file, commit_func, abort_func)
        """
        raise NotImplementedError(self.add_pack)

    def add_pack_data(
        self,
        count: int,
        unpacked_objects: Iterator["UnpackedObject"],
        progress: Callable[..., None] | None = None,
    ) -> "Pack | None":
        """Add pack data to this object store.

        Args:
          count: Number of objects
          unpacked_objects: Iterator over unpacked objects
          progress: Optional progress callback
        """
        raise NotImplementedError(self.add_pack_data)

    def get_unpacked_object(
        self, sha1: ObjectID | RawObjectID, *, include_comp: bool = False
    ) -> "UnpackedObject":
        """Get a raw unresolved object.

        Args:
            sha1: SHA-1 hash of the object
            include_comp: Whether to include compressed data

        Returns:
            UnpackedObject instance
        """
        from .pack import UnpackedObject

        obj = self[sha1]
        return UnpackedObject(obj.type_num, sha=sha1, decomp_chunks=obj.as_raw_chunks())

    def iterobjects_subset(
        self, shas: Iterable[ObjectID], *, allow_missing: bool = False
    ) -> Iterator[ShaFile]:
        """Iterate over a subset of objects.

        Args:
            shas: Iterable of object SHAs to retrieve
            allow_missing: If True, skip missing objects

        Returns:
            Iterator of ShaFile objects
        """
        for sha in shas:
            try:
                yield self[sha]
            except KeyError:
                if not allow_missing:
                    raise


class PackBasedObjectStore(PackCapableObjectStore, PackedObjectContainer):
    """Object store that uses pack files for storage.

    This class provides a base implementation for object stores that use
    Git pack files as their primary storage mechanism. It handles caching
    of open pack files and provides configuration for pack file operations.
    """

    def __init__(
        self,
        pack_compression_level: int = -1,
        pack_index_version: int | None = None,
        pack_delta_window_size: int | None = None,
        pack_window_memory: int | None = None,
        pack_delta_cache_size: int | None = None,
        pack_depth: int | None = None,
        pack_threads: int | None = None,
        pack_big_file_threshold: int | None = None,
        *,
        object_format: "ObjectFormat | None" = None,
    ) -> None:
        """Initialize a PackBasedObjectStore.

        Args:
          pack_compression_level: Compression level for pack files (-1 to 9)
          pack_index_version: Pack index version to use
          pack_delta_window_size: Window size for delta compression
          pack_window_memory: Maximum memory to use for delta window
          pack_delta_cache_size: Cache size for delta operations
          pack_depth: Maximum depth for pack deltas
          pack_threads: Number of threads to use for packing
          pack_big_file_threshold: Threshold for treating files as "big"
          object_format: Hash algorithm to use
        """
        super().__init__(object_format=object_format)
        self._pack_cache: dict[str, Pack] = {}
        self.pack_compression_level = pack_compression_level
        self.pack_index_version = pack_index_version
        self.pack_delta_window_size = pack_delta_window_size
        self.pack_window_memory = pack_window_memory
        self.pack_delta_cache_size = pack_delta_cache_size
        self.pack_depth = pack_depth
        self.pack_threads = pack_threads
        self.pack_big_file_threshold = pack_big_file_threshold

    def get_reachability_provider(
        self,
        prefer_bitmaps: bool = True,
    ) -> ObjectReachabilityProvider:
        """Get the best reachability provider for the object store.

        Args:
          prefer_bitmaps: Whether to use bitmaps if available

        Returns:
          ObjectReachabilityProvider implementation (either bitmap-accelerated
          or graph traversal)
        """
        if prefer_bitmaps:
            # Check if any packs have bitmaps
            has_bitmap = False
            for pack in self.packs:
                try:
                    # Try to access bitmap property
                    if pack.bitmap is not None:
                        has_bitmap = True
                        break
                except FileNotFoundError:
                    # Bitmap file doesn't exist for this pack
                    continue

            if has_bitmap:
                return BitmapReachability(self)

        # Fall back to graph traversal
        return GraphTraversalReachability(self)

    def add_pack(self) -> tuple[BinaryIO, Callable[[], None], Callable[[], None]]:
        """Add a new pack to this object store."""
        raise NotImplementedError(self.add_pack)

    def add_pack_data(
        self,
        count: int,
        unpacked_objects: Iterator[UnpackedObject],
        progress: Callable[..., None] | None = None,
    ) -> "Pack | None":
        """Add pack data to this object store.

        Args:
          count: Number of items to add
          unpacked_objects: Iterator of UnpackedObject instances
          progress: Optional progress callback
        """
        if count == 0:
            # Don't bother writing an empty pack file
            return None
        f, commit, abort = self.add_pack()
        try:
            write_pack_data(
                f.write,
                unpacked_objects,
                num_records=count,
                progress=progress,
                compression_level=self.pack_compression_level,
                object_format=self.object_format,
            )
        except BaseException:
            abort()
            raise
        else:
            return commit()

    @property
    def alternates(self) -> list["BaseObjectStore"]:
        """Return list of alternate object stores."""
        return []

    def contains_packed(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1 and is packed.

        This does not check alternates.
        """
        for pack in self.packs:
            try:
                if sha in pack:
                    return True
            except PackFileDisappeared:
                pass
        return False

    def __contains__(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1.

        This method makes no distinction between loose and packed objects.
        """
        if self.contains_packed(sha) or self.contains_loose(sha):
            return True
        for alternate in self.alternates:
            if sha in alternate:
                return True
        return False

    def _add_cached_pack(self, base_name: str, pack: Pack) -> None:
        """Add a newly appeared pack to the cache by path."""
        prev_pack = self._pack_cache.get(base_name)
        if prev_pack is not pack:
            self._pack_cache[base_name] = pack
            if prev_pack:
                prev_pack.close()

    def generate_pack_data(
        self,
        have: Iterable[ObjectID],
        want: Iterable[ObjectID],
        *,
        shallow: Set[ObjectID] | None = None,
        progress: Callable[..., None] | None = None,
        ofs_delta: bool = True,
    ) -> tuple[int, Iterator[UnpackedObject]]:
        """Generate pack data objects for a set of wants/haves.

        Args:
          have: List of SHA1s of objects that should not be sent
          want: List of SHA1s of objects that should be sent
          shallow: Set of shallow commit SHA1s to skip
          ofs_delta: Whether OFS deltas can be included
          progress: Optional progress reporting method
        """
        missing_objects = MissingObjectFinder(
            self, haves=have, wants=want, shallow=shallow, progress=progress
        )
        remote_has = missing_objects.get_remote_has()
        object_ids = list(missing_objects)
        return len(object_ids), generate_unpacked_objects(
            self,
            object_ids,
            progress=progress,
            ofs_delta=ofs_delta,
            other_haves=remote_has,
        )

    def _clear_cached_packs(self) -> None:
        pack_cache = self._pack_cache
        self._pack_cache = {}
        while pack_cache:
            (_name, pack) = pack_cache.popitem()
            pack.close()

    def _iter_cached_packs(self) -> Iterator[Pack]:
        return iter(self._pack_cache.values())

    def _update_pack_cache(self) -> list[Pack]:
        raise NotImplementedError(self._update_pack_cache)

    def close(self) -> None:
        """Close the object store and release resources.

        This method closes all cached pack files and frees associated resources.
        """
        self._clear_cached_packs()

    @property
    def packs(self) -> list[Pack]:
        """List with pack objects."""
        return list(self._iter_cached_packs()) + list(self._update_pack_cache())

    def count_pack_files(self) -> int:
        """Count the number of pack files.

        Returns:
            Number of pack files (excluding those with .keep files)
        """
        count = 0
        for pack in self.packs:
            # Check if there's a .keep file for this pack
            keep_path = pack._basename + ".keep"
            if not os.path.exists(keep_path):
                count += 1
        return count

    def _iter_alternate_objects(self) -> Iterator[ObjectID]:
        """Iterate over the SHAs of all the objects in alternate stores."""
        for alternate in self.alternates:
            yield from alternate

    def _iter_loose_objects(self) -> Iterator[ObjectID]:
        """Iterate over the SHAs of all loose objects."""
        raise NotImplementedError(self._iter_loose_objects)

    def _get_loose_object(self, sha: ObjectID | RawObjectID) -> ShaFile | None:
        raise NotImplementedError(self._get_loose_object)

    def delete_loose_object(self, sha: ObjectID) -> None:
        """Delete a loose object.

        This method only handles loose objects. For packed objects,
        use repack(exclude=...) to exclude them during repacking.
        """
        raise NotImplementedError(self.delete_loose_object)

    def _remove_pack(self, pack: "Pack") -> None:
        raise NotImplementedError(self._remove_pack)

    def pack_loose_objects(self, progress: Callable[[str], None] | None = None) -> int:
        """Pack loose objects.

        Args:
          progress: Optional progress reporting callback

        Returns: Number of objects packed
        """
        objects: list[tuple[ShaFile, None]] = []
        for sha in self._iter_loose_objects():
            obj = self._get_loose_object(sha)
            if obj is not None:
                objects.append((obj, None))
        self.add_objects(objects, progress=progress)
        for obj, path in objects:
            self.delete_loose_object(obj.id)
        return len(objects)

    def repack(
        self,
        exclude: Set[bytes] | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> int:
        """Repack the packs in this repository.

        Note that this implementation is fairly naive and currently keeps all
        objects in memory while it repacks.

        Args:
          exclude: Optional set of object SHAs to exclude from repacking
          progress: Optional progress reporting callback
        """
        if exclude is None:
            exclude = set()

        loose_objects = set()
        excluded_loose_objects = set()
        for sha in self._iter_loose_objects():
            if sha not in exclude:
                obj = self._get_loose_object(sha)
                if obj is not None:
                    loose_objects.add(obj)
            else:
                excluded_loose_objects.add(sha)

        objects: set[tuple[ShaFile, None]] = {(obj, None) for obj in loose_objects}
        old_packs = {p.name(): p for p in self.packs}
        for name, pack in old_packs.items():
            objects.update(
                (obj, None) for obj in pack.iterobjects() if obj.id not in exclude
            )

        # Only create a new pack if there are objects to pack
        if objects:
            # The name of the consolidated pack might match the name of a
            # pre-existing pack. Take care not to remove the newly created
            # consolidated pack.
            consolidated = self.add_objects(list(objects), progress=progress)
            if consolidated is not None:
                old_packs.pop(consolidated.name(), None)

        # Delete loose objects that were packed
        for obj in loose_objects:
            if obj is not None:
                self.delete_loose_object(obj.id)
        # Delete excluded loose objects
        for sha in excluded_loose_objects:
            self.delete_loose_object(sha)
        for name, pack in old_packs.items():
            self._remove_pack(pack)
        self._update_pack_cache()
        return len(objects)

    def generate_pack_bitmaps(
        self,
        refs: dict[Ref, ObjectID],
        *,
        commit_interval: int | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> int:
        """Generate bitmap indexes for all packs that don't have them.

        This generates .bitmap files for packfiles, enabling fast reachability
        queries. Equivalent to the bitmap generation part of 'git repack -b'.

        Args:
          refs: Dictionary of ref names to commit SHAs
          commit_interval: Include every Nth commit in bitmap index (None for default)
          progress: Optional progress reporting callback

        Returns:
          Number of bitmaps generated
        """
        count = 0
        for pack in self.packs:
            pack.ensure_bitmap(
                self, refs, commit_interval=commit_interval, progress=progress
            )
            count += 1

        # Update cache to pick up new bitmaps
        self._update_pack_cache()

        return count

    def __iter__(self) -> Iterator[ObjectID]:
        """Iterate over the SHAs that are present in this store."""
        self._update_pack_cache()
        for pack in self._iter_cached_packs():
            try:
                yield from pack
            except PackFileDisappeared:
                pass
        yield from self._iter_loose_objects()
        yield from self._iter_alternate_objects()

    def contains_loose(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1 and is loose.

        This does not check alternates.
        """
        return self._get_loose_object(sha) is not None

    def get_raw(self, name: RawObjectID | ObjectID) -> tuple[int, bytes]:
        """Obtain the raw fulltext for an object.

        Args:
          name: sha for the object.
        Returns: tuple with numeric type and object contents.
        """
        sha: RawObjectID
        if len(name) == self.object_format.hex_length:
            sha = hex_to_sha(ObjectID(name))
            hexsha = name
        elif len(name) == self.object_format.oid_length:
            sha = RawObjectID(name)
            hexsha = None
        else:
            raise AssertionError(f"Invalid object name {name!r}")
        for pack in self._iter_cached_packs():
            try:
                return pack.get_raw(sha)
            except (KeyError, PackFileDisappeared):
                pass
        if hexsha is None:
            hexsha = sha_to_hex(sha)
        ret = self._get_loose_object(hexsha)
        if ret is not None:
            return ret.type_num, ret.as_raw_string()
        # Maybe something else has added a pack with the object
        # in the mean time?
        for pack in self._update_pack_cache():
            try:
                return pack.get_raw(sha)
            except KeyError:
                pass
        for alternate in self.alternates:
            try:
                return alternate.get_raw(hexsha)
            except KeyError:
                pass
        raise KeyError(hexsha)

    def iter_unpacked_subset(
        self,
        shas: Iterable[ObjectID | RawObjectID],
        include_comp: bool = False,
        allow_missing: bool = False,
        convert_ofs_delta: bool = True,
    ) -> Iterator[UnpackedObject]:
        """Iterate over a subset of objects, yielding UnpackedObject instances.

        Args:
          shas: Set of object SHAs to retrieve
          include_comp: Whether to include compressed data
          allow_missing: If True, skip missing objects; if False, raise KeyError
          convert_ofs_delta: Whether to convert OFS_DELTA objects

        Returns:
          Iterator of UnpackedObject instances

        Raises:
          KeyError: If an object is missing and allow_missing is False
        """
        todo: set[ObjectID | RawObjectID] = set(shas)
        for p in self._iter_cached_packs():
            for unpacked in p.iter_unpacked_subset(
                todo,
                include_comp=include_comp,
                allow_missing=True,
                convert_ofs_delta=convert_ofs_delta,
            ):
                yield unpacked
                hexsha = sha_to_hex(unpacked.sha())
                todo.remove(hexsha)
        # Maybe something else has added a pack with the object
        # in the mean time?
        for p in self._update_pack_cache():
            for unpacked in p.iter_unpacked_subset(
                todo,
                include_comp=include_comp,
                allow_missing=True,
                convert_ofs_delta=convert_ofs_delta,
            ):
                yield unpacked
                hexsha = sha_to_hex(unpacked.sha())
                todo.remove(hexsha)
        for alternate in self.alternates:
            assert isinstance(alternate, PackBasedObjectStore)
            for unpacked in alternate.iter_unpacked_subset(
                todo,
                include_comp=include_comp,
                allow_missing=True,
                convert_ofs_delta=convert_ofs_delta,
            ):
                yield unpacked
                hexsha = sha_to_hex(unpacked.sha())
                todo.remove(hexsha)

    def iterobjects_subset(
        self, shas: Iterable[ObjectID], *, allow_missing: bool = False
    ) -> Iterator[ShaFile]:
        """Iterate over a subset of objects in the store.

        This method searches for objects in pack files, alternates, and loose storage.

        Args:
          shas: Iterable of object SHAs to retrieve
          allow_missing: If True, skip missing objects; if False, raise KeyError

        Returns:
          Iterator of ShaFile objects

        Raises:
          KeyError: If an object is missing and allow_missing is False
        """
        todo: set[ObjectID] = set(shas)
        for p in self._iter_cached_packs():
            for o in p.iterobjects_subset(todo, allow_missing=True):
                yield o
                todo.remove(o.id)
        # Maybe something else has added a pack with the object
        # in the mean time?
        for p in self._update_pack_cache():
            for o in p.iterobjects_subset(todo, allow_missing=True):
                yield o
                todo.remove(o.id)
        for alternate in self.alternates:
            for o in alternate.iterobjects_subset(todo, allow_missing=True):
                yield o
                todo.remove(o.id)
        for oid in todo:
            loose_obj: ShaFile | None = self._get_loose_object(oid)
            if loose_obj is not None:
                yield loose_obj
            elif not allow_missing:
                raise KeyError(oid)

    def get_unpacked_object(
        self, sha1: bytes, *, include_comp: bool = False
    ) -> UnpackedObject:
        """Obtain the unpacked object.

        Args:
          sha1: sha for the object.
          include_comp: Whether to include compression metadata.
        """
        if len(sha1) == self.object_format.hex_length:
            sha = hex_to_sha(cast(ObjectID, sha1))
            hexsha = cast(ObjectID, sha1)
        elif len(sha1) == self.object_format.oid_length:
            sha = cast(RawObjectID, sha1)
            hexsha = None
        else:
            raise AssertionError(f"Invalid object sha1 {sha1!r}")
        for pack in self._iter_cached_packs():
            try:
                return pack.get_unpacked_object(sha, include_comp=include_comp)
            except (KeyError, PackFileDisappeared):
                pass
        if hexsha is None:
            hexsha = sha_to_hex(sha)
        # Maybe something else has added a pack with the object
        # in the mean time?
        for pack in self._update_pack_cache():
            try:
                return pack.get_unpacked_object(sha, include_comp=include_comp)
            except KeyError:
                pass
        for alternate in self.alternates:
            assert isinstance(alternate, PackBasedObjectStore)
            try:
                return alternate.get_unpacked_object(hexsha, include_comp=include_comp)
            except KeyError:
                pass
        raise KeyError(hexsha)

    def add_objects(
        self,
        objects: Sequence[tuple[ShaFile, str | None]],
        progress: Callable[[str], None] | None = None,
    ) -> "Pack | None":
        """Add a set of objects to this object store.

        Args:
          objects: Iterable over (object, path) tuples, should support
            __len__.
          progress: Optional progress reporting function.
        Returns: Pack object of the objects written.
        """
        count = len(objects)
        record_iter = (full_unpacked_object(o) for (o, p) in objects)
        return self.add_pack_data(count, record_iter, progress=progress)


class DiskObjectStore(PackBasedObjectStore):
    """Git-style object store that exists on disk."""

    path: str | os.PathLike[str]
    pack_dir: str | os.PathLike[str]
    _alternates: "list[BaseObjectStore] | None"
    _commit_graph: "CommitGraph | None"

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        loose_compression_level: int = -1,
        pack_compression_level: int = -1,
        pack_index_version: int | None = None,
        pack_delta_window_size: int | None = None,
        pack_window_memory: int | None = None,
        pack_delta_cache_size: int | None = None,
        pack_depth: int | None = None,
        pack_threads: int | None = None,
        pack_big_file_threshold: int | None = None,
        fsync_object_files: bool = False,
        pack_write_bitmaps: bool = False,
        pack_write_bitmap_hash_cache: bool = True,
        pack_write_bitmap_lookup_table: bool = True,
        file_mode: int | None = None,
        dir_mode: int | None = None,
        object_format: "ObjectFormat | None" = None,
    ) -> None:
        """Open an object store.

        Args:
          path: Path of the object store.
          loose_compression_level: zlib compression level for loose objects
          pack_compression_level: zlib compression level for pack objects
          pack_index_version: pack index version to use (1, 2, or 3)
          pack_delta_window_size: sliding window size for delta compression
          pack_window_memory: memory limit for delta window operations
          pack_delta_cache_size: size of cache for delta operations
          pack_depth: maximum delta chain depth
          pack_threads: number of threads for pack operations
          pack_big_file_threshold: threshold for treating files as big
          fsync_object_files: whether to fsync object files for durability
          pack_write_bitmaps: whether to write bitmap indexes for packs
          pack_write_bitmap_hash_cache: whether to include name-hash cache in bitmaps
          pack_write_bitmap_lookup_table: whether to include lookup table in bitmaps
          file_mode: File permission mask for shared repository
          dir_mode: Directory permission mask for shared repository
          object_format: Hash algorithm to use (SHA1 or SHA256)
        """
        # Import here to avoid circular dependency
        from .object_format import DEFAULT_OBJECT_FORMAT

        super().__init__(
            pack_compression_level=pack_compression_level,
            pack_index_version=pack_index_version,
            pack_delta_window_size=pack_delta_window_size,
            pack_window_memory=pack_window_memory,
            pack_delta_cache_size=pack_delta_cache_size,
            pack_depth=pack_depth,
            pack_threads=pack_threads,
            pack_big_file_threshold=pack_big_file_threshold,
            object_format=object_format if object_format else DEFAULT_OBJECT_FORMAT,
        )
        self.path = path
        self.pack_dir = os.path.join(self.path, PACKDIR)
        self._alternates = None
        self.loose_compression_level = loose_compression_level
        self.pack_compression_level = pack_compression_level
        self.pack_index_version = pack_index_version
        self.fsync_object_files = fsync_object_files
        self.pack_write_bitmaps = pack_write_bitmaps
        self.pack_write_bitmap_hash_cache = pack_write_bitmap_hash_cache
        self.pack_write_bitmap_lookup_table = pack_write_bitmap_lookup_table
        self.file_mode = file_mode
        self.dir_mode = dir_mode

        # Commit graph support - lazy loaded
        self._commit_graph = None
        self._use_commit_graph = True  # Default to true

        # Multi-pack-index support - lazy loaded
        self._midx: MultiPackIndex | None = None
        self._use_midx = True  # Default to true

    def __repr__(self) -> str:
        """Return string representation of DiskObjectStore.

        Returns:
          String representation including the store path
        """
        return f"<{self.__class__.__name__}({self.path!r})>"

    @classmethod
    def from_config(
        cls,
        path: str | os.PathLike[str],
        config: "Config",
        *,
        file_mode: int | None = None,
        dir_mode: int | None = None,
    ) -> "DiskObjectStore":
        """Create a DiskObjectStore from a configuration object.

        Args:
          path: Path to the object store directory
          config: Configuration object to read settings from
          file_mode: Optional file permission mask for shared repository
          dir_mode: Optional directory permission mask for shared repository

        Returns:
          New DiskObjectStore instance configured according to config
        """
        try:
            default_compression_level = int(
                config.get((b"core",), b"compression").decode()
            )
        except KeyError:
            default_compression_level = -1
        try:
            loose_compression_level = int(
                config.get((b"core",), b"looseCompression").decode()
            )
        except KeyError:
            loose_compression_level = default_compression_level
        try:
            pack_compression_level = int(
                config.get((b"core",), "packCompression").decode()
            )
        except KeyError:
            pack_compression_level = default_compression_level
        try:
            pack_index_version = int(config.get((b"pack",), b"indexVersion").decode())
        except KeyError:
            pack_index_version = None

        # Read pack configuration options
        try:
            pack_delta_window_size = int(
                config.get((b"pack",), b"deltaWindowSize").decode()
            )
        except KeyError:
            pack_delta_window_size = None
        try:
            pack_window_memory = int(config.get((b"pack",), b"windowMemory").decode())
        except KeyError:
            pack_window_memory = None
        try:
            pack_delta_cache_size = int(
                config.get((b"pack",), b"deltaCacheSize").decode()
            )
        except KeyError:
            pack_delta_cache_size = None
        try:
            pack_depth = int(config.get((b"pack",), b"depth").decode())
        except KeyError:
            pack_depth = None
        try:
            pack_threads = int(config.get((b"pack",), b"threads").decode())
        except KeyError:
            pack_threads = None
        try:
            pack_big_file_threshold = int(
                config.get((b"pack",), b"bigFileThreshold").decode()
            )
        except KeyError:
            pack_big_file_threshold = None

        # Read core.commitGraph setting
        use_commit_graph = config.get_boolean((b"core",), b"commitGraph", True)

        # Read core.multiPackIndex setting
        use_midx = config.get_boolean((b"core",), b"multiPackIndex", True)

        # Read core.fsyncObjectFiles setting
        fsync_object_files = config.get_boolean((b"core",), b"fsyncObjectFiles", False)

        # Read bitmap settings
        pack_write_bitmaps = config.get_boolean((b"pack",), b"writeBitmaps", False)
        pack_write_bitmap_hash_cache = config.get_boolean(
            (b"pack",), b"writeBitmapHashCache", True
        )
        pack_write_bitmap_lookup_table = config.get_boolean(
            (b"pack",), b"writeBitmapLookupTable", True
        )
        # Also check repack.writeBitmaps for backwards compatibility
        if not pack_write_bitmaps:
            pack_write_bitmaps = config.get_boolean(
                (b"repack",), b"writeBitmaps", False
            )

        # Get hash algorithm from config
        from .object_format import get_object_format

        object_format = None
        try:
            try:
                version = int(config.get((b"core",), b"repositoryformatversion"))
            except KeyError:
                version = 0
            if version == 1:
                try:
                    object_format_name = config.get((b"extensions",), b"objectformat")
                except KeyError:
                    object_format_name = b"sha1"
                object_format = get_object_format(object_format_name.decode("ascii"))
        except (KeyError, ValueError):
            pass

        instance = cls(
            path,
            loose_compression_level=loose_compression_level,
            pack_compression_level=pack_compression_level,
            pack_index_version=pack_index_version,
            pack_delta_window_size=pack_delta_window_size,
            pack_window_memory=pack_window_memory,
            pack_delta_cache_size=pack_delta_cache_size,
            pack_depth=pack_depth,
            pack_threads=pack_threads,
            pack_big_file_threshold=pack_big_file_threshold,
            fsync_object_files=fsync_object_files,
            pack_write_bitmaps=pack_write_bitmaps,
            pack_write_bitmap_hash_cache=pack_write_bitmap_hash_cache,
            pack_write_bitmap_lookup_table=pack_write_bitmap_lookup_table,
            file_mode=file_mode,
            dir_mode=dir_mode,
            object_format=object_format,
        )
        instance._use_commit_graph = use_commit_graph
        instance._use_midx = use_midx
        return instance

    @property
    def alternates(self) -> list["BaseObjectStore"]:
        """Get the list of alternate object stores.

        Reads from .git/objects/info/alternates if not already cached.

        Returns:
          List of DiskObjectStore instances for alternate object directories
        """
        if self._alternates is not None:
            return self._alternates
        self._alternates = []
        for path in self._read_alternate_paths():
            self._alternates.append(DiskObjectStore(path))
        return self._alternates

    def _read_alternate_paths(self) -> Iterator[str]:
        try:
            f = GitFile(os.path.join(self.path, INFODIR, "alternates"), "rb")
        except FileNotFoundError:
            return
        with f:
            for line in f.readlines():
                line = line.rstrip(b"\n")
                if line.startswith(b"#"):
                    continue
                if os.path.isabs(line):
                    yield os.fsdecode(line)
                else:
                    yield os.fsdecode(os.path.join(os.fsencode(self.path), line))

    def add_alternate_path(self, path: str | os.PathLike[str]) -> None:
        """Add an alternate path to this object store."""
        info_dir = os.path.join(self.path, INFODIR)
        try:
            os.mkdir(info_dir)
            if self.dir_mode is not None:
                os.chmod(info_dir, self.dir_mode)
        except FileExistsError:
            pass
        alternates_path = os.path.join(self.path, INFODIR, "alternates")
        mask = self.file_mode if self.file_mode is not None else 0o644
        with GitFile(alternates_path, "wb", mask=mask) as f:
            try:
                orig_f = open(alternates_path, "rb")
            except FileNotFoundError:
                pass
            else:
                with orig_f:
                    f.write(orig_f.read())
            f.write(os.fsencode(path) + b"\n")

        if not os.path.isabs(path):
            path = os.path.join(self.path, path)
        self.alternates.append(DiskObjectStore(path))

    def _update_pack_cache(self) -> list[Pack]:
        """Read and iterate over new pack files and cache them."""
        try:
            pack_dir_contents = os.listdir(self.pack_dir)
        except FileNotFoundError:
            self.close()
            return []
        pack_files = set()
        for name in pack_dir_contents:
            if name.startswith("pack-") and name.endswith(".pack"):
                # verify that idx exists first (otherwise the pack was not yet
                # fully written)
                idx_name = os.path.splitext(name)[0] + ".idx"
                if idx_name in pack_dir_contents:
                    pack_name = name[: -len(".pack")]
                    pack_files.add(pack_name)

        # Open newly appeared pack files
        new_packs = []
        for f in pack_files:
            if f not in self._pack_cache:
                pack = Pack(
                    os.path.join(self.pack_dir, f),
                    object_format=self.object_format,
                    delta_window_size=self.pack_delta_window_size,
                    window_memory=self.pack_window_memory,
                    delta_cache_size=self.pack_delta_cache_size,
                    depth=self.pack_depth,
                    threads=self.pack_threads,
                    big_file_threshold=self.pack_big_file_threshold,
                )
                new_packs.append(pack)
                self._pack_cache[f] = pack
        # Remove disappeared pack files
        for f in set(self._pack_cache) - pack_files:
            self._pack_cache.pop(f).close()
        return new_packs

    def _get_shafile_path(self, sha: ObjectID | RawObjectID) -> str:
        # Check from object dir
        return hex_to_filename(os.fspath(self.path), sha)

    def _iter_loose_objects(self) -> Iterator[ObjectID]:
        for base in os.listdir(self.path):
            if len(base) != 2:
                continue
            for rest in os.listdir(os.path.join(self.path, base)):
                sha = os.fsencode(base + rest)
                if not valid_hexsha(sha):
                    continue
                yield ObjectID(sha)

    def count_loose_objects(self) -> int:
        """Count the number of loose objects in the object store.

        Returns:
            Number of loose objects
        """
        # Calculate expected filename length for loose
        # objects (excluding directory)
        fn_length = self.object_format.hex_length - 2
        count = 0
        if not os.path.exists(self.path):
            return 0

        for i in range(256):
            subdir = os.path.join(self.path, f"{i:02x}")
            try:
                count += len(
                    [name for name in os.listdir(subdir) if len(name) == fn_length]
                )
            except FileNotFoundError:
                # Directory may have been removed or is inaccessible
                continue

        return count

    def _get_loose_object(self, sha: ObjectID | RawObjectID) -> ShaFile | None:
        path = self._get_shafile_path(sha)
        try:
            # Load the object from path with SHA and hash algorithm from object store
            # Convert to hex ObjectID if needed
            if len(sha) == self.object_format.oid_length:
                hex_sha: ObjectID = sha_to_hex(RawObjectID(sha))
            else:
                hex_sha = ObjectID(sha)
            return ShaFile.from_path(path, hex_sha, object_format=self.object_format)
        except FileNotFoundError:
            return None

    def delete_loose_object(self, sha: ObjectID) -> None:
        """Delete a loose object from disk.

        Args:
          sha: SHA1 of the object to delete

        Raises:
          FileNotFoundError: If the object file doesn't exist
        """
        os.remove(self._get_shafile_path(sha))

    def get_object_mtime(self, sha: ObjectID) -> float:
        """Get the modification time of an object.

        Args:
          sha: SHA1 of the object

        Returns:
          Modification time as seconds since epoch

        Raises:
          KeyError: if the object is not found
        """
        # First check if it's a loose object
        if self.contains_loose(sha):
            path = self._get_shafile_path(sha)
            try:
                return os.path.getmtime(path)
            except FileNotFoundError:
                pass

        # Check if it's in a pack file
        for pack in self.packs:
            try:
                if sha in pack:
                    # Use the pack file's mtime for packed objects
                    pack_path = pack._data_path
                    try:
                        return os.path.getmtime(pack_path)
                    except (FileNotFoundError, AttributeError):
                        pass
            except PackFileDisappeared:
                pass

        raise KeyError(sha)

    def _remove_pack(self, pack: Pack) -> None:
        try:
            del self._pack_cache[os.path.basename(pack._basename)]
        except KeyError:
            pass
        pack.close()
        os.remove(pack.data.path)
        if hasattr(pack.index, "path"):
            os.remove(pack.index.path)

    def _get_pack_basepath(
        self, entries: Iterable[tuple[bytes, int, int | None]]
    ) -> str:
        suffix_bytes = iter_sha1(entry[0] for entry in entries)
        # TODO: Handle self.pack_dir being bytes
        suffix = suffix_bytes.decode("ascii")
        return os.path.join(self.pack_dir, "pack-" + suffix)

    def _complete_pack(
        self,
        f: BinaryIO,
        path: str,
        num_objects: int,
        indexer: PackIndexer,
        progress: Callable[..., None] | None = None,
        refs: dict[Ref, ObjectID] | None = None,
    ) -> Pack:
        """Move a specific file containing a pack into the pack directory.

        Note: The file should be on the same file system as the
            packs directory.

        Args:
          f: Open file object for the pack.
          path: Path to the pack file.
          num_objects: Number of objects in the pack.
          indexer: A PackIndexer for indexing the pack.
          progress: Optional progress reporting function.
          refs: Optional dictionary of refs for bitmap generation.
        """
        entries = []
        for i, entry in enumerate(indexer):
            if progress is not None:
                progress(f"generating index: {i}/{num_objects}\r".encode("ascii"))
            entries.append(entry)

        pack_sha, extra_entries = extend_pack(
            f,
            set(indexer.ext_refs()),
            get_raw=self.get_raw,
            compression_level=self.pack_compression_level,
            progress=progress,
            object_format=self.object_format,
        )
        f.flush()
        if self.fsync_object_files:
            try:
                fileno = f.fileno()
            except AttributeError as e:
                raise OSError("fsync requested but file has no fileno()") from e
            else:
                os.fsync(fileno)
        f.close()

        entries.extend(extra_entries)

        # Move the pack in.
        entries.sort()
        pack_base_name = self._get_pack_basepath(entries)

        for pack in self.packs:
            if pack._basename == pack_base_name:
                return pack

        target_pack_path = pack_base_name + ".pack"
        target_index_path = pack_base_name + ".idx"
        if sys.platform == "win32":
            # Windows might have the target pack file lingering. Attempt
            # removal, silently passing if the target does not exist.
            with suppress(FileNotFoundError):
                os.remove(target_pack_path)
        os.rename(path, target_pack_path)

        # Write the index.
        mask = self.file_mode if self.file_mode is not None else PACK_MODE
        with GitFile(
            target_index_path,
            "wb",
            mask=mask,
            fsync=self.fsync_object_files,
        ) as index_file:
            write_pack_index(
                index_file, entries, pack_sha, version=self.pack_index_version
            )

        # Generate bitmap if configured and refs are available
        if self.pack_write_bitmaps and refs:
            from .bitmap import generate_bitmap, write_bitmap
            from .pack import load_pack_index_file

            if progress:
                progress("Generating bitmap index\r".encode("ascii"))

            # Load the index we just wrote
            with open(target_index_path, "rb") as idx_file:
                pack_index = load_pack_index_file(
                    os.path.basename(target_index_path),
                    idx_file,
                    self.object_format,
                )

            # Generate the bitmap
            bitmap = generate_bitmap(
                pack_index=pack_index,
                object_store=self,
                refs=refs,
                pack_checksum=pack_sha,
                include_hash_cache=self.pack_write_bitmap_hash_cache,
                include_lookup_table=self.pack_write_bitmap_lookup_table,
                progress=lambda msg: progress(msg.encode("ascii"))
                if progress and isinstance(msg, str)
                else None,
            )

            # Write the bitmap
            target_bitmap_path = pack_base_name + ".bitmap"
            write_bitmap(target_bitmap_path, bitmap)

            if progress:
                progress("Bitmap index written\r".encode("ascii"))

        # Add the pack to the store and return it.
        final_pack = Pack(
            pack_base_name,
            object_format=self.object_format,
            delta_window_size=self.pack_delta_window_size,
            window_memory=self.pack_window_memory,
            delta_cache_size=self.pack_delta_cache_size,
            depth=self.pack_depth,
            threads=self.pack_threads,
            big_file_threshold=self.pack_big_file_threshold,
        )
        final_pack.check_length_and_checksum()
        self._add_cached_pack(pack_base_name, final_pack)
        return final_pack

    def add_thin_pack(
        self,
        read_all: Callable[[int], bytes],
        read_some: Callable[[int], bytes] | None,
        progress: Callable[..., None] | None = None,
    ) -> "Pack":
        """Add a new thin pack to this object store.

        Thin packs are packs that contain deltas with parents that exist
        outside the pack. They should never be placed in the object store
        directly, and always indexed and completed as they are copied.

        Args:
          read_all: Read function that blocks until the number of
            requested bytes are read.
          read_some: Read function that returns at least one byte, but may
            not return the number of bytes requested.
          progress: Optional progress reporting function.
        Returns: A Pack object pointing at the now-completed thin pack in the
            objects/pack directory.
        """
        import tempfile

        fd, path = tempfile.mkstemp(dir=self.path, prefix="tmp_pack_")
        with os.fdopen(fd, "w+b") as f:
            os.chmod(path, PACK_MODE)
            indexer = PackIndexer(
                f,
                self.object_format.hash_func,
                resolve_ext_ref=self.get_raw,  # type: ignore[arg-type]
            )
            copier = PackStreamCopier(
                self.object_format.hash_func,
                read_all,
                read_some,
                f,
                delta_iter=indexer,  # type: ignore[arg-type]
            )
            copier.verify(progress=progress)
            return self._complete_pack(f, path, len(copier), indexer, progress=progress)

    def add_pack(
        self,
    ) -> tuple[BinaryIO, Callable[[], None], Callable[[], None]]:
        """Add a new pack to this object store.

        Returns: Fileobject to write to, a commit function to
            call when the pack is finished and an abort
            function.
        """
        import tempfile

        fd, path = tempfile.mkstemp(dir=self.pack_dir, suffix=".pack")
        f = os.fdopen(fd, "w+b")
        mask = self.file_mode if self.file_mode is not None else PACK_MODE
        os.chmod(path, mask)

        def commit() -> "Pack | None":
            if f.tell() > 0:
                f.seek(0)

                with PackData(path, file=f, object_format=self.object_format) as pd:
                    indexer = PackIndexer.for_pack_data(
                        pd,
                        resolve_ext_ref=self.get_raw,  # type: ignore[arg-type]
                    )
                    return self._complete_pack(f, path, len(pd), indexer)  # type: ignore[arg-type]
            else:
                f.close()
                os.remove(path)
                return None

        def abort() -> None:
            f.close()
            os.remove(path)

        return f, commit, abort  # type: ignore[return-value]

    def add_object(self, obj: ShaFile) -> None:
        """Add a single object to this object store.

        Args:
          obj: Object to add
        """
        # Use the correct hash algorithm for the object ID
        obj_id = ObjectID(obj.get_id(self.object_format))
        path = self._get_shafile_path(obj_id)
        dir = os.path.dirname(path)
        try:
            os.mkdir(dir)
            if self.dir_mode is not None:
                os.chmod(dir, self.dir_mode)
        except FileExistsError:
            pass
        if os.path.exists(path):
            return  # Already there, no need to write again
        mask = self.file_mode if self.file_mode is not None else PACK_MODE
        with GitFile(path, "wb", mask=mask, fsync=self.fsync_object_files) as f:
            f.write(
                obj.as_legacy_object(compression_level=self.loose_compression_level)
            )

    @classmethod
    def init(
        cls,
        path: str | os.PathLike[str],
        *,
        file_mode: int | None = None,
        dir_mode: int | None = None,
        object_format: "ObjectFormat | None" = None,
    ) -> "DiskObjectStore":
        """Initialize a new disk object store.

        Creates the necessary directory structure for a Git object store.

        Args:
          path: Path where the object store should be created
          file_mode: Optional file permission mask for shared repository
          dir_mode: Optional directory permission mask for shared repository
          object_format: Hash algorithm to use (SHA1 or SHA256)

        Returns:
          New DiskObjectStore instance
        """
        try:
            os.mkdir(path)
            if dir_mode is not None:
                os.chmod(path, dir_mode)
        except FileExistsError:
            pass
        info_path = os.path.join(path, "info")
        pack_path = os.path.join(path, PACKDIR)
        os.mkdir(info_path)
        os.mkdir(pack_path)
        if dir_mode is not None:
            os.chmod(info_path, dir_mode)
            os.chmod(pack_path, dir_mode)
        return cls(
            path, file_mode=file_mode, dir_mode=dir_mode, object_format=object_format
        )

    def iter_prefix(self, prefix: bytes) -> Iterator[ObjectID]:
        """Iterate over all object SHAs with the given prefix.

        Args:
          prefix: Hex prefix to search for (as bytes)

        Returns:
          Iterator of object SHAs (as ObjectID) matching the prefix
        """
        if len(prefix) < 2:
            yield from super().iter_prefix(prefix)
            return
        seen = set()
        dir = prefix[:2].decode()
        rest = prefix[2:].decode()
        try:
            for name in os.listdir(os.path.join(self.path, dir)):
                if name.startswith(rest):
                    sha = ObjectID(os.fsencode(dir + name))
                    if sha not in seen:
                        seen.add(sha)
                        yield sha
        except FileNotFoundError:
            pass

        for p in self.packs:
            bin_prefix = (
                binascii.unhexlify(prefix)
                if len(prefix) % 2 == 0
                else binascii.unhexlify(prefix[:-1])
            )
            for bin_sha in p.index.iter_prefix(bin_prefix):
                sha = sha_to_hex(bin_sha)
                if sha.startswith(prefix) and sha not in seen:
                    seen.add(sha)
                    yield sha
        for alternate in self.alternates:
            for sha in alternate.iter_prefix(prefix):
                if sha not in seen:
                    seen.add(sha)
                    yield sha

    def get_commit_graph(self) -> "CommitGraph | None":
        """Get the commit graph for this object store.

        Returns:
          CommitGraph object if available, None otherwise
        """
        if not self._use_commit_graph:
            return None

        if self._commit_graph is None:
            from .commit_graph import read_commit_graph

            # Look for commit graph in our objects directory
            graph_file = os.path.join(self.path, "info", "commit-graph")
            if os.path.exists(graph_file):
                self._commit_graph = read_commit_graph(graph_file)
        return self._commit_graph

    def get_midx(self) -> MultiPackIndex | None:
        """Get the multi-pack-index for this object store.

        Returns:
          MultiPackIndex object if available, None otherwise

        Raises:
          ValueError: If MIDX file is corrupt
          OSError: If MIDX file cannot be read
        """
        if not self._use_midx:
            return None

        if self._midx is None:
            # Look for MIDX in pack directory
            midx_file = os.path.join(self.pack_dir, "multi-pack-index")
            if os.path.exists(midx_file):
                self._midx = load_midx(midx_file)
        return self._midx

    def _get_pack_by_name(self, pack_name: str) -> Pack:
        """Get a pack by its base name.

        Args:
            pack_name: Base name of the pack (e.g., 'pack-abc123.pack' or 'pack-abc123.idx')

        Returns:
            Pack object

        Raises:
            KeyError: If pack doesn't exist
        """
        # Remove .pack or .idx extension if present
        if pack_name.endswith(".pack"):
            base_name = pack_name[:-5]
        elif pack_name.endswith(".idx"):
            base_name = pack_name[:-4]
        else:
            base_name = pack_name

        # Check if already in cache
        if base_name in self._pack_cache:
            return self._pack_cache[base_name]

        # Load the pack
        pack_path = os.path.join(self.pack_dir, base_name)
        if not os.path.exists(pack_path + ".pack"):
            raise KeyError(f"Pack {pack_name} not found")

        pack = Pack(
            pack_path,
            object_format=self.object_format,
            delta_window_size=self.pack_delta_window_size,
            window_memory=self.pack_window_memory,
            delta_cache_size=self.pack_delta_cache_size,
            depth=self.pack_depth,
            threads=self.pack_threads,
            big_file_threshold=self.pack_big_file_threshold,
        )
        self._pack_cache[base_name] = pack
        return pack

    def contains_packed(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1 and is packed.

        This checks the MIDX first if available, then falls back to checking
        individual pack indexes.

        Args:
            sha: Binary SHA of the object

        Returns:
            True if the object is in a pack file
        """
        # Check MIDX first for faster lookup
        midx = self.get_midx()
        if midx is not None and sha in midx:
            return True

        # Fall back to checking individual packs
        return super().contains_packed(sha)

    def get_raw(self, name: RawObjectID | ObjectID) -> tuple[int, bytes]:
        """Obtain the raw fulltext for an object.

        This uses the MIDX if available for faster lookups.

        Args:
            name: SHA for the object (20 bytes binary or 40 bytes hex)

        Returns:
            Tuple with numeric type and object contents

        Raises:
            KeyError: If object not found
        """
        sha: RawObjectID
        if len(name) in (40, 64):
            # name is ObjectID (hex), convert to RawObjectID
            # Support both SHA1 (40) and SHA256 (64)
            sha = hex_to_sha(cast(ObjectID, name))
        elif len(name) in (20, 32):
            # name is already RawObjectID (binary)
            # Support both SHA1 (20) and SHA256 (32)
            sha = RawObjectID(name)
        else:
            raise AssertionError(f"Invalid object name {name!r}")

        # Try MIDX first for faster lookup
        midx = self.get_midx()
        if midx is not None:
            result = midx.object_offset(sha)
            if result is not None:
                pack_name, _offset = result
                try:
                    pack = self._get_pack_by_name(pack_name)
                    return pack.get_raw(sha)
                except (KeyError, PackFileDisappeared):
                    # Pack disappeared or object not found, fall through to standard lookup
                    pass

        # Fall back to the standard implementation
        return super().get_raw(name)

    def write_midx(self) -> bytes:
        """Write a multi-pack-index file for this object store.

        Creates a MIDX file that indexes all pack files in the pack directory.

        Returns:
            SHA-1 checksum of the written MIDX file

        Raises:
            OSError: If the pack directory doesn't exist or MIDX can't be written
        """
        from .midx import write_midx_file

        # Get all pack files
        packs = self.packs
        if not packs:
            # No packs to index
            return b"\x00" * 20

        # Collect entries from all packs
        pack_entries: list[tuple[str, list[tuple[RawObjectID, int, int | None]]]] = []

        for pack in packs:
            # Git stores .idx extension in MIDX, not .pack
            pack_name = os.path.basename(pack._basename) + ".idx"
            entries = list(pack.index.iterentries())
            pack_entries.append((pack_name, entries))

        # Write MIDX file
        midx_path = os.path.join(self.pack_dir, "multi-pack-index")
        return write_midx_file(midx_path, pack_entries)

    def write_commit_graph(
        self, refs: Iterable[ObjectID] | None = None, reachable: bool = True
    ) -> None:
        """Write a commit graph file for this object store.

        Args:
            refs: List of refs to include. If None, includes all refs from object store.
            reachable: If True, includes all commits reachable from refs.
                      If False, only includes the direct ref targets.
        """
        from .commit_graph import get_reachable_commits

        if refs is None:
            # Get all commit objects from the object store
            all_refs = []
            # Iterate through all objects to find commits
            for sha in self:
                try:
                    obj = self[sha]
                    if obj.type_name == b"commit":
                        all_refs.append(sha)
                except KeyError:
                    continue
        else:
            # Use provided refs
            all_refs = list(refs)

        if not all_refs:
            return  # No commits to include

        if reachable:
            # Get all reachable commits
            commit_ids = get_reachable_commits(self, all_refs)
        else:
            # Just use the direct ref targets - ensure they're hex ObjectIDs
            commit_ids = []
            for ref in all_refs:
                if isinstance(ref, bytes) and len(ref) == self.object_format.hex_length:
                    # Already hex ObjectID
                    commit_ids.append(ref)
                elif (
                    isinstance(ref, bytes) and len(ref) == self.object_format.oid_length
                ):
                    # Binary SHA, convert to hex ObjectID
                    from .objects import sha_to_hex

                    commit_ids.append(sha_to_hex(RawObjectID(ref)))
                else:
                    # Assume it's already correct format
                    commit_ids.append(ref)

        if commit_ids:
            # Write commit graph directly to our object store path
            # Generate the commit graph
            from .commit_graph import generate_commit_graph

            graph = generate_commit_graph(self, commit_ids)

            if graph.entries:
                # Ensure the info directory exists
                info_dir = os.path.join(self.path, "info")
                os.makedirs(info_dir, exist_ok=True)
                if self.dir_mode is not None:
                    os.chmod(info_dir, self.dir_mode)

                # Write using GitFile for atomic operation
                graph_path = os.path.join(info_dir, "commit-graph")
                mask = self.file_mode if self.file_mode is not None else 0o644
                with GitFile(graph_path, "wb", mask=mask) as f:
                    assert isinstance(
                        f, _GitFile
                    )  # GitFile in write mode always returns _GitFile
                    graph.write_to_file(f)

            # Clear cached commit graph so it gets reloaded
            self._commit_graph = None

    def prune(self, grace_period: int | None = None) -> None:
        """Prune/clean up this object store.

        This removes temporary files that were left behind by interrupted
        pack operations. These are files that start with ``tmp_pack_`` in the
        repository directory or files with .pack extension but no corresponding
        .idx file in the pack directory.

        Args:
          grace_period: Grace period in seconds for removing temporary files.
                       If None, uses DEFAULT_TEMPFILE_GRACE_PERIOD.
        """
        import glob

        if grace_period is None:
            grace_period = DEFAULT_TEMPFILE_GRACE_PERIOD

        # Clean up tmp_pack_* files in the repository directory
        for tmp_file in glob.glob(os.path.join(self.path, "tmp_pack_*")):
            # Check if file is old enough (more than grace period)
            mtime = os.path.getmtime(tmp_file)
            if time.time() - mtime > grace_period:
                os.remove(tmp_file)

        # Clean up orphaned .pack files without corresponding .idx files
        try:
            pack_dir_contents = os.listdir(self.pack_dir)
        except FileNotFoundError:
            return

        pack_files = {}
        idx_files = set()

        for name in pack_dir_contents:
            if name.endswith(".pack"):
                base_name = name[:-5]  # Remove .pack extension
                pack_files[base_name] = name
            elif name.endswith(".idx"):
                base_name = name[:-4]  # Remove .idx extension
                idx_files.add(base_name)

        # Remove .pack files without corresponding .idx files
        for base_name, pack_name in pack_files.items():
            if base_name not in idx_files:
                pack_path = os.path.join(self.pack_dir, pack_name)
                # Check if file is old enough (more than grace period)
                mtime = os.path.getmtime(pack_path)
                if time.time() - mtime > grace_period:
                    os.remove(pack_path)

    def close(self) -> None:
        """Close the object store and release resources.

        This method closes all cached pack files, MIDX, and frees associated resources.
        """
        # Close MIDX if it's loaded
        if self._midx is not None:
            self._midx.close()
            self._midx = None

        # Close alternates
        if self._alternates is not None:
            for alt in self._alternates:
                alt.close()
            self._alternates = None

        # Call parent class close to handle pack files
        super().close()


class MemoryObjectStore(PackCapableObjectStore):
    """Object store that keeps all objects in memory."""

    def __init__(self, *, object_format: "ObjectFormat | None" = None) -> None:
        """Initialize a MemoryObjectStore.

        Creates an empty in-memory object store.

        Args:
            object_format: Hash algorithm to use (defaults to SHA1)
        """
        super().__init__(object_format=object_format)
        self._data: dict[ObjectID, ShaFile] = {}
        self.pack_compression_level = -1

    def _to_hexsha(self, sha: ObjectID | RawObjectID) -> ObjectID:
        if len(sha) == self.object_format.hex_length:
            return cast(ObjectID, sha)
        elif len(sha) == self.object_format.oid_length:
            return sha_to_hex(cast(RawObjectID, sha))
        else:
            raise ValueError(f"Invalid sha {sha!r}")

    def contains_loose(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1 and is loose."""
        return self._to_hexsha(sha) in self._data

    def contains_packed(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if a particular object is present by SHA1 and is packed."""
        return False

    def __iter__(self) -> Iterator[ObjectID]:
        """Iterate over the SHAs that are present in this store."""
        return iter(self._data.keys())

    @property
    def packs(self) -> list[Pack]:
        """List with pack objects."""
        return []

    def get_raw(self, name: RawObjectID | ObjectID) -> tuple[int, bytes]:
        """Obtain the raw text for an object.

        Args:
          name: sha for the object.
        Returns: tuple with numeric type and object contents.
        """
        obj = self[self._to_hexsha(name)]
        return obj.type_num, obj.as_raw_string()

    def __getitem__(self, name: ObjectID | RawObjectID) -> ShaFile:
        """Retrieve an object by SHA.

        Args:
          name: SHA of the object (as hex string or bytes)

        Returns:
          Copy of the ShaFile object

        Raises:
          KeyError: If the object is not found
        """
        return self._data[self._to_hexsha(name)].copy()

    def __delitem__(self, name: ObjectID) -> None:
        """Delete an object from this store, for testing only."""
        del self._data[self._to_hexsha(name)]

    def add_object(self, obj: ShaFile) -> None:
        """Add a single object to this object store."""
        self._data[obj.id] = obj.copy()

    def add_objects(
        self,
        objects: Iterable[tuple[ShaFile, str | None]],
        progress: Callable[[str], None] | None = None,
    ) -> None:
        """Add a set of objects to this object store.

        Args:
          objects: Iterable over a list of (object, path) tuples
          progress: Optional progress reporting function.
        """
        for obj, path in objects:
            self.add_object(obj)

    def add_pack(self) -> tuple[BinaryIO, Callable[[], None], Callable[[], None]]:
        """Add a new pack to this object store.

        Because this object store doesn't support packs, we extract and add the
        individual objects.

        Returns: Fileobject to write to and a commit function to
            call when the pack is finished.
        """
        from tempfile import SpooledTemporaryFile

        f = SpooledTemporaryFile(max_size=PACK_SPOOL_FILE_MAX_SIZE, prefix="incoming-")

        def commit() -> None:
            size = f.tell()
            if size > 0:
                f.seek(0)

                p = PackData.from_file(f, self.object_format, size)
                for obj in PackInflater.for_pack_data(p, self.get_raw):  # type: ignore[arg-type]
                    self.add_object(obj)
                p.close()
                f.close()
            else:
                f.close()

        def abort() -> None:
            f.close()

        return f, commit, abort  # type: ignore[return-value]

    def add_pack_data(
        self,
        count: int,
        unpacked_objects: Iterator[UnpackedObject],
        progress: Callable[[str], None] | None = None,
    ) -> None:
        """Add pack data to this object store.

        Args:
          count: Number of items to add
          unpacked_objects: Iterator of UnpackedObject instances
          progress: Optional progress reporting function.
        """
        if count == 0:
            return

        # Since MemoryObjectStore doesn't support pack files, we need to
        # extract individual objects. To handle deltas properly, we write
        # to a temporary pack and then use PackInflater to resolve them.
        f, commit, abort = self.add_pack()
        try:
            write_pack_data(
                f.write,
                unpacked_objects,
                num_records=count,
                progress=progress,
                object_format=self.object_format,
            )
        except BaseException:
            abort()
            raise
        else:
            commit()

    def add_thin_pack(
        self,
        read_all: Callable[[int], bytes],
        read_some: Callable[[int], bytes] | None,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        """Add a new thin pack to this object store.

        Thin packs are packs that contain deltas with parents that exist
        outside the pack. Because this object store doesn't support packs, we
        extract and add the individual objects.

        Args:
          read_all: Read function that blocks until the number of
            requested bytes are read.
          read_some: Read function that returns at least one byte, but may
            not return the number of bytes requested.
          progress: Optional progress reporting function.
        """
        f, commit, abort = self.add_pack()
        try:
            copier = PackStreamCopier(
                self.object_format.hash_func,
                read_all,
                read_some,
                f,
            )
            copier.verify()
        except BaseException:
            abort()
            raise
        else:
            commit()


class ObjectIterator(Protocol):
    """Interface for iterating over objects."""

    def iterobjects(self) -> Iterator[ShaFile]:
        """Iterate over all objects.

        Returns:
          Iterator of ShaFile objects
        """
        raise NotImplementedError(self.iterobjects)


def tree_lookup_path(
    lookup_obj: Callable[[ObjectID | RawObjectID], ShaFile],
    root_sha: ObjectID | RawObjectID,
    path: bytes,
) -> tuple[int, ObjectID]:
    """Look up an object in a Git tree.

    Args:
      lookup_obj: Callback for retrieving object by SHA1
      root_sha: SHA1 of the root tree
      path: Path to lookup
    Returns: A tuple of (mode, SHA) of the resulting path.
    """
    tree = lookup_obj(root_sha)
    if not isinstance(tree, Tree):
        raise NotTreeError(root_sha)
    return tree.lookup_path(lookup_obj, path)


def _collect_filetree_revs(
    obj_store: ObjectContainer, tree_sha: ObjectID, kset: set[ObjectID]
) -> None:
    """Collect SHA1s of files and directories for specified tree.

    Args:
      obj_store: Object store to get objects by SHA from
      tree_sha: tree reference to walk
      kset: set to fill with references to files and directories
    """
    filetree = obj_store[tree_sha]
    assert isinstance(filetree, Tree)
    for name, mode, sha in filetree.iteritems():
        assert mode is not None
        assert sha is not None
        if not S_ISGITLINK(mode) and sha not in kset:
            kset.add(sha)
            if stat.S_ISDIR(mode):
                _collect_filetree_revs(obj_store, sha, kset)


def _split_commits_and_tags(
    obj_store: ObjectContainer,
    lst: Iterable[ObjectID],
    *,
    unknown: str = "error",
) -> tuple[set[ObjectID], set[ObjectID], set[ObjectID]]:
    """Split object id list into three lists with commit, tag, and other SHAs.

    Commits referenced by tags are included into commits
    list as well. Only SHA1s known in this repository will get
    through, controlled by the unknown parameter.

    Args:
      obj_store: Object store to get objects by SHA1 from
      lst: Collection of commit and tag SHAs
      unknown: How to handle unknown objects: "error", "warn", or "ignore"
    Returns: A tuple of (commits, tags, others) SHA1s
    """
    import logging

    if unknown not in ("error", "warn", "ignore"):
        raise ValueError(
            f"unknown must be 'error', 'warn', or 'ignore', got {unknown!r}"
        )

    commits: set[ObjectID] = set()
    tags: set[ObjectID] = set()
    others: set[ObjectID] = set()
    for e in lst:
        try:
            o = obj_store[e]
        except KeyError:
            if unknown == "error":
                raise
            elif unknown == "warn":
                logging.warning(
                    "Object %s not found in object store", e.decode("ascii")
                )
            # else: ignore
        else:
            if isinstance(o, Commit):
                commits.add(e)
            elif isinstance(o, Tag):
                tags.add(e)
                tagged = o.object[1]
                c, t, os = _split_commits_and_tags(obj_store, [tagged], unknown=unknown)
                commits |= c
                tags |= t
                others |= os
            else:
                others.add(e)
    return (commits, tags, others)


class MissingObjectFinder:
    """Find the objects missing from another object store.

    Args:
      object_store: Object store containing at least all objects to be
        sent
      haves: SHA1s of commits not to send (already present in target)
      wants: SHA1s of commits to send
      progress: Optional function to report progress to.
      get_tagged: Function that returns a dict of pointed-to sha -> tag
        sha for including tags.
      get_parents: Optional function for getting the parents of a commit.
    """

    def __init__(
        self,
        object_store: BaseObjectStore,
        haves: Iterable[ObjectID],
        wants: Iterable[ObjectID],
        *,
        shallow: Set[ObjectID] | None = None,
        progress: Callable[[bytes], None] | None = None,
        get_tagged: Callable[[], dict[ObjectID, ObjectID]] | None = None,
        get_parents: Callable[[Commit], list[ObjectID]] = lambda commit: commit.parents,
    ) -> None:
        """Initialize a MissingObjectFinder.

        Args:
          object_store: Object store containing objects
          haves: SHA1s of objects already present in target
          wants: SHA1s of objects to send
          shallow: Set of shallow commit SHA1s
          progress: Optional progress reporting callback
          get_tagged: Function returning dict of pointed-to sha -> tag sha
          get_parents: Function for getting commit parents
        """
        self.object_store = object_store
        if shallow is None:
            shallow = set()
        self._get_parents = get_parents
        reachability = object_store.get_reachability_provider()
        # process Commits and Tags differently
        # haves may list commits/tags not available locally (silently ignore them).
        # wants should only contain valid SHAs (fail fast if not).
        have_commits, have_tags, have_others = _split_commits_and_tags(
            object_store, haves, unknown="ignore"
        )
        want_commits, want_tags, want_others = _split_commits_and_tags(
            object_store, wants, unknown="error"
        )
        # all_ancestors is a set of commits that shall not be sent
        # (complete repository up to 'haves')
        all_ancestors = reachability.get_reachable_commits(
            have_commits, exclude=None, shallow=shallow
        )
        # all_missing - complete set of commits between haves and wants
        # common_commits - boundary commits directly encountered when traversing wants
        # We use _collect_ancestors here because we need the exact boundary behavior:
        # commits that are in all_ancestors and directly reachable from wants,
        # but we don't traverse past them. This is hard to express with the
        # reachability abstraction alone.
        missing_commits, common_commits = _collect_ancestors(
            object_store,
            want_commits,
            frozenset(all_ancestors),
            shallow=frozenset(shallow),
            get_parents=self._get_parents,
        )

        self.remote_has: set[ObjectID] = set()
        # Now, fill sha_done with commits and revisions of
        # files and directories known to be both locally
        # and on target. Thus these commits and files
        # won't get selected for fetch
        for h in common_commits:
            self.remote_has.add(h)
            cmt = object_store[h]
            assert isinstance(cmt, Commit)
            # Get tree objects for this commit
            tree_objects = reachability.get_tree_objects([cmt.tree])
            self.remote_has.update(tree_objects)

        # record tags we have as visited, too
        for t in have_tags:
            self.remote_has.add(t)
        self.sha_done = set(self.remote_has)

        # in fact, what we 'want' is commits, tags, and others
        # we've found missing
        self.objects_to_send: set[tuple[ObjectID, bytes | None, int | None, bool]] = {
            (w, None, Commit.type_num, False) for w in missing_commits
        }
        missing_tags = want_tags.difference(have_tags)
        self.objects_to_send.update(
            {(w, None, Tag.type_num, False) for w in missing_tags}
        )
        missing_others = want_others.difference(have_others)
        self.objects_to_send.update({(w, None, None, False) for w in missing_others})

        if progress is None:
            self.progress: Callable[[bytes], None] = lambda x: None
        else:
            self.progress = progress
        self._tagged = (get_tagged and get_tagged()) or {}

    def get_remote_has(self) -> set[ObjectID]:
        """Get the set of SHAs the remote has.

        Returns:
          Set of SHA1s that the remote side already has
        """
        return self.remote_has

    def add_todo(
        self, entries: Iterable[tuple[ObjectID, bytes | None, int | None, bool]]
    ) -> None:
        """Add objects to the todo list.

        Args:
          entries: Iterable of tuples (sha, name, type_num, is_leaf)
        """
        self.objects_to_send.update([e for e in entries if e[0] not in self.sha_done])

    def __next__(self) -> tuple[ObjectID, PackHint | None]:
        """Get the next object to send.

        Returns:
          Tuple of (sha, pack_hint)

        Raises:
          StopIteration: When no more objects to send
        """
        while True:
            if not self.objects_to_send:
                self.progress(
                    f"counting objects: {len(self.sha_done)}, done.\n".encode("ascii")
                )
                raise StopIteration
            (sha, name, type_num, leaf) = self.objects_to_send.pop()
            if sha not in self.sha_done:
                break
        if not leaf:
            o = self.object_store[sha]
            if isinstance(o, Commit):
                self.add_todo([(o.tree, b"", Tree.type_num, False)])
            elif isinstance(o, Tree):
                todos = []
                for n, m, s in o.iteritems():
                    assert m is not None
                    assert n is not None
                    assert s is not None
                    if not S_ISGITLINK(m):
                        todos.append(
                            (
                                s,
                                n,
                                (Blob.type_num if stat.S_ISREG(m) else Tree.type_num),
                                not stat.S_ISDIR(m),
                            )
                        )
                self.add_todo(todos)
            elif isinstance(o, Tag):
                self.add_todo([(o.object[1], None, o.object[0].type_num, False)])
        if sha in self._tagged:
            self.add_todo([(self._tagged[sha], None, None, True)])
        self.sha_done.add(sha)
        if len(self.sha_done) % 1000 == 0:
            self.progress(f"counting objects: {len(self.sha_done)}\r".encode("ascii"))
        if type_num is None:
            pack_hint = None
        else:
            pack_hint = (type_num, name)
        return (sha, pack_hint)

    def __iter__(self) -> Iterator[tuple[ObjectID, PackHint | None]]:
        """Return iterator over objects to send.

        Returns:
          Self (this class implements the iterator protocol)
        """
        return self


class ObjectStoreGraphWalker:
    """Graph walker that finds what commits are missing from an object store."""

    heads: set[ObjectID]
    """Revisions without descendants in the local repo."""

    get_parents: Callable[[ObjectID], list[ObjectID]]
    """Function to retrieve parents in the local repo."""

    shallow: set[ObjectID]

    def __init__(
        self,
        local_heads: Iterable[ObjectID],
        get_parents: Callable[[ObjectID], list[ObjectID]],
        shallow: set[ObjectID] | None = None,
        update_shallow: Callable[[set[ObjectID] | None, set[ObjectID] | None], None]
        | None = None,
    ) -> None:
        """Create a new instance.

        Args:
          local_heads: Heads to start search with
          get_parents: Function for finding the parents of a SHA1.
          shallow: Set of shallow commits.
          update_shallow: Function to update shallow commits.
        """
        self.heads = set(local_heads)
        self.get_parents = get_parents
        self.parents: dict[ObjectID, list[ObjectID] | None] = {}
        if shallow is None:
            shallow = set()
        self.shallow = shallow
        self.update_shallow = update_shallow

    def nak(self) -> None:
        """Nothing in common was found."""

    def ack(self, sha: ObjectID) -> None:
        """Ack that a revision and its ancestors are present in the source."""
        if len(sha) != 40:
            # TODO: support SHA256
            raise ValueError(f"unexpected sha {sha!r} received")
        ancestors = {sha}

        # stop if we run out of heads to remove
        while self.heads:
            for a in ancestors:
                if a in self.heads:
                    self.heads.remove(a)

            # collect all ancestors
            new_ancestors = set()
            for a in ancestors:
                ps = self.parents.get(a)
                if ps is not None:
                    new_ancestors.update(ps)
                self.parents[a] = None

            # no more ancestors; stop
            if not new_ancestors:
                break

            ancestors = new_ancestors

    def next(self) -> ObjectID | None:
        """Iterate over ancestors of heads in the target."""
        if self.heads:
            ret = self.heads.pop()
            try:
                ps = self.get_parents(ret)
            except KeyError:
                return None
            self.parents[ret] = ps
            self.heads.update([p for p in ps if p not in self.parents])
            return ret
        return None

    __next__ = next


def commit_tree_changes(
    object_store: BaseObjectStore,
    tree: ObjectID | Tree,
    changes: Sequence[tuple[bytes, int | None, ObjectID | None]],
) -> ObjectID:
    """Commit a specified set of changes to a tree structure.

    This will apply a set of changes on top of an existing tree, storing new
    objects in object_store.

    changes are a list of tuples with (path, mode, object_sha).
    Paths can be both blobs and trees. See the mode and
    object sha to None deletes the path.

    This method works especially well if there are only a small
    number of changes to a big tree. For a large number of changes
    to a large tree, use e.g. commit_tree.

    Args:
      object_store: Object store to store new objects in
        and retrieve old ones from.
      tree: Original tree root (SHA or Tree object)
      changes: changes to apply
    Returns: New tree root object
    """
    # TODO(jelmer): Save up the objects and add them using .add_objects
    # rather than with individual calls to .add_object.
    # Handle both Tree object and SHA
    if isinstance(tree, Tree):
        tree_obj: Tree = tree
    else:
        sha_obj = object_store[tree]
        assert isinstance(sha_obj, Tree)
        tree_obj = sha_obj
    nested_changes: dict[bytes, list[tuple[bytes, int | None, ObjectID | None]]] = {}
    for path, new_mode, new_sha in changes:
        try:
            (dirname, subpath) = path.split(b"/", 1)
        except ValueError:
            if new_sha is None:
                del tree_obj[path]
            else:
                assert new_mode is not None
                tree_obj[path] = (new_mode, new_sha)
        else:
            nested_changes.setdefault(dirname, []).append((subpath, new_mode, new_sha))
    for name, subchanges in nested_changes.items():
        try:
            orig_subtree_id: ObjectID | Tree = tree_obj[name][1]
        except KeyError:
            # For new directories, pass an empty Tree object
            orig_subtree_id = Tree()
        subtree_id = commit_tree_changes(object_store, orig_subtree_id, subchanges)
        subtree = object_store[subtree_id]
        assert isinstance(subtree, Tree)
        if len(subtree) == 0:
            del tree_obj[name]
        else:
            tree_obj[name] = (stat.S_IFDIR, subtree.id)
    object_store.add_object(tree_obj)
    return tree_obj.id


class OverlayObjectStore(BaseObjectStore):
    """Object store that can overlay multiple object stores."""

    def __init__(
        self,
        bases: list[BaseObjectStore],
        add_store: BaseObjectStore | None = None,
    ) -> None:
        """Initialize an OverlayObjectStore.

        Args:
          bases: List of base object stores to overlay
          add_store: Optional store to write new objects to

        Raises:
          ValueError: If stores have different hash algorithms
        """
        from .object_format import verify_same_object_format

        # Verify all stores use the same hash algorithm
        store_algorithms = [store.object_format for store in bases]
        if add_store:
            store_algorithms.append(add_store.object_format)

        object_format = verify_same_object_format(*store_algorithms)

        super().__init__(object_format=object_format)
        self.bases = bases
        self.add_store = add_store

    def add_object(self, object: ShaFile) -> None:
        """Add a single object to the store.

        Args:
          object: Object to add

        Raises:
          NotImplementedError: If no add_store was provided
        """
        if self.add_store is None:
            raise NotImplementedError(self.add_object)
        return self.add_store.add_object(object)

    def add_objects(
        self,
        objects: Sequence[tuple[ShaFile, str | None]],
        progress: Callable[[str], None] | None = None,
    ) -> Pack | None:
        """Add multiple objects to the store.

        Args:
          objects: Iterator of objects to add
          progress: Optional progress reporting callback

        Raises:
          NotImplementedError: If no add_store was provided
        """
        if self.add_store is None:
            raise NotImplementedError(self.add_object)
        return self.add_store.add_objects(objects, progress)

    @property
    def packs(self) -> list[Pack]:
        """Get the list of packs from all overlaid stores.

        Returns:
          Combined list of packs from all base stores
        """
        ret = []
        for b in self.bases:
            ret.extend(b.packs)
        return ret

    def __iter__(self) -> Iterator[ObjectID]:
        """Iterate over all object SHAs in the overlaid stores.

        Returns:
          Iterator of object SHAs (deduped across stores)
        """
        done = set()
        for b in self.bases:
            for o_id in b:
                if o_id not in done:
                    yield o_id
                    done.add(o_id)

    def iterobjects_subset(
        self, shas: Iterable[ObjectID], *, allow_missing: bool = False
    ) -> Iterator[ShaFile]:
        """Iterate over a subset of objects from the overlaid stores.

        Args:
          shas: Iterable of object SHAs to retrieve
          allow_missing: If True, skip missing objects; if False, raise KeyError

        Returns:
          Iterator of ShaFile objects

        Raises:
          KeyError: If an object is missing and allow_missing is False
        """
        todo = set(shas)
        found: set[ObjectID] = set()

        for b in self.bases:
            # Create a copy of todo for each base to avoid modifying
            # the set while iterating through it
            current_todo = todo - found
            for o in b.iterobjects_subset(current_todo, allow_missing=True):
                yield o
                found.add(o.id)

        # Check for any remaining objects not found
        missing = todo - found
        if missing and not allow_missing:
            raise KeyError(next(iter(missing)))

    def iter_unpacked_subset(
        self,
        shas: Iterable[ObjectID | RawObjectID],
        include_comp: bool = False,
        allow_missing: bool = False,
        convert_ofs_delta: bool = True,
    ) -> Iterator[UnpackedObject]:
        """Iterate over unpacked objects from the overlaid stores.

        Args:
          shas: Iterable of object SHAs to retrieve
          include_comp: Whether to include compressed data
          allow_missing: If True, skip missing objects; if False, raise KeyError
          convert_ofs_delta: Whether to convert OFS_DELTA objects

        Returns:
          Iterator of unpacked objects

        Raises:
          KeyError: If an object is missing and allow_missing is False
        """
        todo: set[ObjectID | RawObjectID] = set(shas)
        for b in self.bases:
            for o in b.iter_unpacked_subset(
                todo,
                include_comp=include_comp,
                allow_missing=True,
                convert_ofs_delta=convert_ofs_delta,
            ):
                yield o
                todo.remove(o.sha())
        if todo and not allow_missing:
            raise KeyError(next(iter(todo)))

    def get_raw(self, sha_id: ObjectID | RawObjectID) -> tuple[int, bytes]:
        """Get the raw object data from the overlaid stores.

        Args:
          sha_id: SHA of the object

        Returns:
          Tuple of (type_num, raw_data)

        Raises:
          KeyError: If object not found in any base store
        """
        for b in self.bases:
            try:
                return b.get_raw(sha_id)
            except KeyError:
                pass
        raise KeyError(sha_id)

    def contains_packed(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if an object is packed in any base store.

        Args:
          sha: SHA of the object

        Returns:
          True if object is packed in any base store
        """
        for b in self.bases:
            if b.contains_packed(sha):
                return True
        return False

    def contains_loose(self, sha: ObjectID | RawObjectID) -> bool:
        """Check if an object is loose in any base store.

        Args:
          sha: SHA of the object

        Returns:
          True if object is loose in any base store
        """
        for b in self.bases:
            if b.contains_loose(sha):
                return True
        return False


def read_packs_file(f: BinaryIO) -> Iterator[str]:
    """Yield the packs listed in a packs file."""
    for line in f.read().splitlines():
        if not line:
            continue
        (kind, name) = line.split(b" ", 1)
        if kind != b"P":
            continue
        yield os.fsdecode(name)


class BucketBasedObjectStore(PackBasedObjectStore):
    """Object store implementation that uses a bucket store like S3 as backend."""

    def _iter_loose_objects(self) -> Iterator[ObjectID]:
        """Iterate over the SHAs of all loose objects."""
        return iter([])

    def _get_loose_object(self, sha: ObjectID | RawObjectID) -> None:
        return None

    def delete_loose_object(self, sha: ObjectID) -> None:
        """Delete a loose object (no-op for bucket stores).

        Bucket-based stores don't have loose objects, so this is a no-op.

        Args:
          sha: SHA of the object to delete
        """
        # Doesn't exist..

    def pack_loose_objects(self, progress: Callable[[str], None] | None = None) -> int:
        """Pack loose objects. Returns number of objects packed.

        BucketBasedObjectStore doesn't support loose objects, so this is a no-op.

        Args:
          progress: Optional progress reporting callback (ignored)
        """
        return 0

    def _remove_pack_by_name(self, name: str) -> None:
        """Remove a pack by name. Subclasses should implement this."""
        raise NotImplementedError(self._remove_pack_by_name)

    def _iter_pack_names(self) -> Iterator[str]:
        raise NotImplementedError(self._iter_pack_names)

    def _get_pack(self, name: str) -> Pack:
        raise NotImplementedError(self._get_pack)

    def _update_pack_cache(self) -> list[Pack]:
        pack_files = set(self._iter_pack_names())

        # Open newly appeared pack files
        new_packs = []
        for f in pack_files:
            if f not in self._pack_cache:
                pack = self._get_pack(f)
                new_packs.append(pack)
                self._pack_cache[f] = pack
        # Remove disappeared pack files
        for f in set(self._pack_cache) - pack_files:
            self._pack_cache.pop(f).close()
        return new_packs

    def _upload_pack(
        self, basename: str, pack_file: BinaryIO, index_file: BinaryIO
    ) -> None:
        raise NotImplementedError

    def add_pack(self) -> tuple[BinaryIO, Callable[[], None], Callable[[], None]]:
        """Add a new pack to this object store.

        Returns: Fileobject to write to, a commit function to
            call when the pack is finished and an abort
            function.
        """
        import tempfile

        pf = tempfile.SpooledTemporaryFile(
            max_size=PACK_SPOOL_FILE_MAX_SIZE, prefix="incoming-"
        )

        def commit() -> Pack | None:
            if pf.tell() == 0:
                pf.close()
                return None

            pf.seek(0)

            p = PackData(pf.name, file=pf, object_format=self.object_format)
            entries = p.sorted_entries()
            basename = iter_sha1(entry[0] for entry in entries).decode("ascii")
            idxf = tempfile.SpooledTemporaryFile(
                max_size=PACK_SPOOL_FILE_MAX_SIZE, prefix="incoming-"
            )
            checksum = p.get_stored_checksum()
            write_pack_index(idxf, entries, checksum, version=self.pack_index_version)
            idxf.seek(0)
            idx = load_pack_index_file(basename + ".idx", idxf, self.object_format)
            for pack in self.packs:
                if pack.get_stored_checksum() == p.get_stored_checksum():
                    p.close()
                    idx.close()
                    pf.close()
                    idxf.close()
                    return pack
            pf.seek(0)
            idxf.seek(0)
            self._upload_pack(basename, pf, idxf)  # type: ignore[arg-type]
            final_pack = Pack.from_objects(p, idx)
            self._add_cached_pack(basename, final_pack)
            pf.close()
            idxf.close()
            return final_pack

        return pf, commit, pf.close  # type: ignore[return-value]


def _collect_ancestors(
    store: ObjectContainer,
    heads: Iterable[ObjectID],
    common: frozenset[ObjectID] = frozenset(),
    shallow: frozenset[ObjectID] = frozenset(),
    get_parents: Callable[[Commit], list[ObjectID]] = lambda commit: commit.parents,
) -> tuple[set[ObjectID], set[ObjectID]]:
    """Collect all ancestors of heads up to (excluding) those in common.

    Args:
      store: Object store to get commits from
      heads: commits to start from
      common: commits to end at, or empty set to walk repository
        completely
      shallow: Set of shallow commits
      get_parents: Optional function for getting the parents of a
        commit.
    Returns: a tuple (A, B) where A - all commits reachable
        from heads but not present in common, B - common (shared) elements
        that are directly reachable from heads
    """
    bases = set()
    commits = set()
    queue: list[ObjectID] = []
    queue.extend(heads)

    # Try to use commit graph if available
    commit_graph = store.get_commit_graph()

    while queue:
        e = queue.pop(0)
        if e in common:
            bases.add(e)
        elif e not in commits:
            commits.add(e)
            if e in shallow:
                continue

            # Try to use commit graph for parent lookup
            parents = None
            if commit_graph:
                parents = commit_graph.get_parents(e)

            if parents is None:
                # Fall back to loading the object
                cmt = store[e]
                assert isinstance(cmt, Commit)
                parents = get_parents(cmt)

            queue.extend(parents)
    return (commits, bases)


def iter_tree_contents(
    store: ObjectContainer, tree_id: ObjectID | None, *, include_trees: bool = False
) -> Iterator[TreeEntry]:
    """Iterate the contents of a tree and all subtrees.

    Iteration is depth-first pre-order, as in e.g. os.walk.

    Args:
      store: Object store to get trees from
      tree_id: SHA1 of the tree.
      include_trees: If True, include tree objects in the iteration.

    Yields: TreeEntry namedtuples for all the objects in a tree.
    """
    if tree_id is None:
        return
    # This could be fairly easily generalized to >2 trees if we find a use
    # case.
    todo = [TreeEntry(b"", stat.S_IFDIR, tree_id)]
    while todo:
        entry = todo.pop()
        assert entry.mode is not None
        if stat.S_ISDIR(entry.mode):
            extra = []
            assert entry.sha is not None
            tree = store[entry.sha]
            assert isinstance(tree, Tree)
            for subentry in tree.iteritems(name_order=True):
                assert entry.path is not None
                extra.append(subentry.in_path(entry.path))
            todo.extend(reversed(extra))
        if not stat.S_ISDIR(entry.mode) or include_trees:
            yield entry


def iter_commit_contents(
    store: ObjectContainer,
    commit: Commit | ObjectID | RawObjectID,
    *,
    include: Sequence[str | bytes | Path] | None = None,
) -> Iterator[TreeEntry]:
    """Iterate the contents of the repository at the specified commit.

    This is a wrapper around iter_tree_contents() and
    tree_lookup_path() to simplify the common task of getting the
    contest of a repo at a particular commit. See also
    dulwich.index.build_file_from_blob() for writing individual files
    to disk.

    Args:
      store: Object store to get trees from
      commit: Commit object, or SHA1 of a commit
      include: if provided, only the entries whose paths are in the
        list, or whose parent tree is in the list, will be
        included. Note that duplicate or overlapping paths
        (e.g. ["foo", "foo/bar"]) may result in duplicate entries

    Yields: TreeEntry namedtuples for all matching files in a commit.
    """
    sha = commit.id if isinstance(commit, Commit) else commit
    if not isinstance(obj := store[sha], Commit):
        raise TypeError(
            f"{sha.decode('ascii')} should be ID of a Commit, but is {type(obj)}"
        )
    commit = obj
    encoding = commit.encoding or "utf-8"
    include_bytes: list[bytes] = (
        [
            path if isinstance(path, bytes) else str(path).encode(encoding)
            for path in include
        ]
        if include is not None
        else [b""]
    )

    for path in include_bytes:
        mode, obj_id = tree_lookup_path(store.__getitem__, commit.tree, path)
        # Iterate all contained files if path points to a dir, otherwise just get that
        # single file
        if isinstance(store[obj_id], Tree):
            for entry in iter_tree_contents(store, obj_id):
                yield entry.in_path(path)
        else:
            yield TreeEntry(path, mode, obj_id)


def peel_sha(
    store: ObjectContainer, sha: ObjectID | RawObjectID
) -> tuple[ShaFile, ShaFile]:
    """Peel all tags from a SHA.

    Args:
      store: Object store to get objects from
      sha: The object SHA to peel.
    Returns: The fully-peeled SHA1 of a tag object, after peeling all
        intermediate tags; if the original ref does not point to a tag,
        this will equal the original SHA1.
    """
    unpeeled = obj = store[sha]
    obj_class = object_class(obj.type_name)
    while obj_class is Tag:
        assert isinstance(obj, Tag)
        obj_class, sha = obj.object
        obj = store[sha]
    return unpeeled, obj


class GraphTraversalReachability:
    """Naive graph traversal implementation of ObjectReachabilityProvider.

    This implementation wraps existing graph traversal functions
    (_collect_ancestors, _collect_filetree_revs) to provide the standard
    reachability interface without any performance optimizations.
    """

    def __init__(self, object_store: BaseObjectStore) -> None:
        """Initialize the graph traversal provider.

        Args:
          object_store: Object store to query
        """
        self.store = object_store

    def get_reachable_commits(
        self,
        heads: Iterable[ObjectID],
        exclude: Iterable[ObjectID] | None = None,
        shallow: Set[ObjectID] | None = None,
    ) -> set[ObjectID]:
        """Get all commits reachable from heads, excluding those in exclude.

        Uses _collect_ancestors for commit traversal.

        Args:
          heads: Starting commit SHAs
          exclude: Commit SHAs to exclude (and their ancestors)
          shallow: Set of shallow commit boundaries

        Returns:
          Set of commit SHAs reachable from heads but not from exclude
        """
        exclude_set = frozenset(exclude) if exclude else frozenset()
        shallow_set = frozenset(shallow) if shallow else frozenset()
        commits, _bases = _collect_ancestors(
            self.store, heads, exclude_set, shallow_set
        )
        return commits

    def get_tree_objects(
        self,
        tree_shas: Iterable[ObjectID],
    ) -> set[ObjectID]:
        """Get all trees and blobs reachable from the given trees.

        Uses _collect_filetree_revs for tree traversal.

        Args:
          tree_shas: Starting tree SHAs

        Returns:
          Set of tree and blob SHAs
        """
        result: set[ObjectID] = set()
        for tree_sha in tree_shas:
            _collect_filetree_revs(self.store, tree_sha, result)
        return result

    def get_reachable_objects(
        self,
        commits: Iterable[ObjectID],
        exclude_commits: Iterable[ObjectID] | None = None,
    ) -> set[ObjectID]:
        """Get all objects (commits + trees + blobs) reachable from commits.

        Args:
          commits: Starting commit SHAs
          exclude_commits: Commits whose objects should be excluded

        Returns:
          Set of all object SHAs (commits, trees, blobs)
        """
        commits_set = set(commits)
        result = set(commits_set)

        # Get trees for all commits
        tree_shas = []
        for commit_sha in commits_set:
            try:
                commit = self.store[commit_sha]
                if isinstance(commit, Commit):
                    tree_shas.append(commit.tree)
            except KeyError:
                # Commit not in store, skip
                continue

        # Collect all tree/blob objects
        result.update(self.get_tree_objects(tree_shas))

        # Exclude objects from exclude_commits if needed
        if exclude_commits:
            exclude_objects = self.get_reachable_objects(exclude_commits, None)
            result -= exclude_objects

        return result


class BitmapReachability:
    """Bitmap-accelerated implementation of ObjectReachabilityProvider.

    This implementation uses packfile bitmap indexes where available to
    accelerate reachability queries. Falls back to graph traversal when
    bitmaps don't cover the requested commits.
    """

    def __init__(self, object_store: "PackBasedObjectStore") -> None:
        """Initialize the bitmap provider.

        Args:
          object_store: Pack-based object store with bitmap support
        """
        self.store = object_store
        # Fallback to graph traversal for operations not yet optimized
        self._fallback = GraphTraversalReachability(object_store)

    def _combine_commit_bitmaps(
        self,
        commit_shas: set[ObjectID],
        exclude_shas: set[ObjectID] | None = None,
    ) -> tuple["EWAHBitmap", "Pack"] | None:
        """Combine bitmaps for multiple commits using OR, with optional exclusion.

        Args:
          commit_shas: Set of commit SHAs to combine
          exclude_shas: Optional set of commit SHAs to exclude

        Returns:
          Tuple of (combined_bitmap, pack) or None if bitmaps unavailable
        """
        from .bitmap import find_commit_bitmaps

        # Find bitmaps for the commits
        commit_bitmaps = find_commit_bitmaps(commit_shas, self.store.packs)

        # If we can't find bitmaps for all commits, return None
        if len(commit_bitmaps) < len(commit_shas):
            return None

        # Combine bitmaps using OR
        combined_bitmap = None
        result_pack = None

        for commit_sha in commit_shas:
            pack, pack_bitmap, _sha_to_pos = commit_bitmaps[commit_sha]
            commit_bitmap = pack_bitmap.get_bitmap(commit_sha)

            if commit_bitmap is None:
                return None

            if combined_bitmap is None:
                combined_bitmap = commit_bitmap
                result_pack = pack
            elif pack == result_pack:
                # Same pack, can OR directly
                combined_bitmap = combined_bitmap | commit_bitmap
            else:
                # Different packs, can't combine
                return None

        # Handle exclusions if provided
        if exclude_shas and result_pack and combined_bitmap:
            exclude_bitmaps = find_commit_bitmaps(exclude_shas, [result_pack])

            if len(exclude_bitmaps) == len(exclude_shas):
                # All excludes have bitmaps, compute exclusion
                exclude_combined = None

                for commit_sha in exclude_shas:
                    _pack, pack_bitmap, _sha_to_pos = exclude_bitmaps[commit_sha]
                    exclude_bitmap = pack_bitmap.get_bitmap(commit_sha)

                    if exclude_bitmap is None:
                        break

                    if exclude_combined is None:
                        exclude_combined = exclude_bitmap
                    else:
                        exclude_combined = exclude_combined | exclude_bitmap

                # Subtract excludes using set difference
                if exclude_combined:
                    combined_bitmap = combined_bitmap - exclude_combined

        if combined_bitmap and result_pack:
            return (combined_bitmap, result_pack)
        return None

    def get_reachable_commits(
        self,
        heads: Iterable[ObjectID],
        exclude: Iterable[ObjectID] | None = None,
        shallow: Set[ObjectID] | None = None,
    ) -> set[ObjectID]:
        """Get all commits reachable from heads using bitmaps where possible.

        Args:
          heads: Starting commit SHAs
          exclude: Commit SHAs to exclude (and their ancestors)
          shallow: Set of shallow commit boundaries

        Returns:
          Set of commit SHAs reachable from heads but not from exclude
        """
        from .bitmap import bitmap_to_object_shas

        # If shallow is specified, fall back to graph traversal
        # (bitmaps don't support shallow boundaries well)
        if shallow:
            return self._fallback.get_reachable_commits(heads, exclude, shallow)

        heads_set = set(heads)
        exclude_set = set(exclude) if exclude else None

        # Try to combine bitmaps
        result = self._combine_commit_bitmaps(heads_set, exclude_set)
        if result is None:
            return self._fallback.get_reachable_commits(heads, exclude, shallow)

        combined_bitmap, result_pack = result

        # Convert bitmap to commit SHAs, filtering for commits only
        pack_bitmap = result_pack.bitmap
        if pack_bitmap is None:
            return self._fallback.get_reachable_commits(heads, exclude, shallow)
        commit_type_filter = pack_bitmap.commit_bitmap
        return bitmap_to_object_shas(
            combined_bitmap, result_pack.index, commit_type_filter
        )

    def get_tree_objects(
        self,
        tree_shas: Iterable[ObjectID],
    ) -> set[ObjectID]:
        """Get all trees and blobs reachable from the given trees.

        Args:
          tree_shas: Starting tree SHAs

        Returns:
          Set of tree and blob SHAs
        """
        # Tree traversal doesn't benefit much from bitmaps, use fallback
        return self._fallback.get_tree_objects(tree_shas)

    def get_reachable_objects(
        self,
        commits: Iterable[ObjectID],
        exclude_commits: Iterable[ObjectID] | None = None,
    ) -> set[ObjectID]:
        """Get all objects reachable from commits using bitmaps.

        Args:
          commits: Starting commit SHAs
          exclude_commits: Commits whose objects should be excluded

        Returns:
          Set of all object SHAs (commits, trees, blobs)
        """
        from .bitmap import bitmap_to_object_shas

        commits_set = set(commits)
        exclude_set = set(exclude_commits) if exclude_commits else None

        # Try to combine bitmaps
        result = self._combine_commit_bitmaps(commits_set, exclude_set)
        if result is None:
            return self._fallback.get_reachable_objects(commits, exclude_commits)

        combined_bitmap, result_pack = result

        # Convert bitmap to all object SHAs (no type filter)
        return bitmap_to_object_shas(combined_bitmap, result_pack.index, None)
