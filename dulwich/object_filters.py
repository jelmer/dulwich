# object_filters.py -- Object filtering for partial clone and similar operations
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Object filtering for Git partial clone and pack generation.

This module implements Git's object filter specifications for partial clone,
as documented in:
https://git-scm.com/docs/rev-list-options#Documentation/rev-list-options.txt---filterltfilter-specgt

Filter specifications control which objects are included when generating packs,
enabling partial clone (downloading only needed objects) and similar operations.

Supported filter specs:
- blob:none - Exclude all blobs
- blob:limit=<n>[kmg] - Exclude blobs larger than n bytes/KB/MB/GB
- tree:<depth> - Exclude trees beyond specified depth
- sparse:oid=<oid> - Use sparse specification from object
- combine:<filter>+<filter>+... - Combine multiple filters
"""

__all__ = [
    "BlobLimitFilter",
    "BlobNoneFilter",
    "CombineFilter",
    "FilterSpec",
    "SparseOidFilter",
    "TreeDepthFilter",
    "filter_pack_objects",
    "filter_pack_objects_with_paths",
    "parse_filter_spec",
]

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from .objects import S_ISGITLINK, Blob, Commit, ObjectID, Tag, Tree, valid_hexsha

if TYPE_CHECKING:
    from collections.abc import Callable

    from .object_store import BaseObjectStore
    from .objects import ObjectID


class FilterSpec(ABC):
    """Base class for all filter specifications."""

    @abstractmethod
    def should_include_blob(self, blob_size: int) -> bool:
        """Determine if a blob of given size should be included.

        Args:
            blob_size: Size of the blob in bytes

        Returns:
            True if the blob should be included, False otherwise
        """
        ...

    @abstractmethod
    def should_include_tree(self, depth: int) -> bool:
        """Determine if a tree at given depth should be included.

        Args:
            depth: Depth of the tree (0 = root)

        Returns:
            True if the tree should be included, False otherwise
        """
        ...

    @abstractmethod
    def to_spec_string(self) -> str:
        """Convert filter spec back to string format.

        Returns:
            Filter specification string (e.g., ``blob:none``, ``blob:limit=1m``)
        """
        ...


class BlobNoneFilter(FilterSpec):
    """Filter that excludes all blobs."""

    def should_include_blob(self, blob_size: int) -> bool:
        """Exclude all blobs."""
        return False

    def should_include_tree(self, depth: int) -> bool:
        """Include all trees."""
        return True

    def to_spec_string(self) -> str:
        """Return 'blob:none'."""
        return "blob:none"

    def __repr__(self) -> str:
        """Return string representation of the filter."""
        return "BlobNoneFilter()"


class BlobLimitFilter(FilterSpec):
    """Filter that excludes blobs larger than a specified size."""

    def __init__(self, limit: int) -> None:
        """Initialize blob limit filter.

        Args:
            limit: Maximum blob size in bytes
        """
        self.limit = limit

    def should_include_blob(self, blob_size: int) -> bool:
        """Include only blobs smaller than or equal to the limit."""
        return blob_size <= self.limit

    def should_include_tree(self, depth: int) -> bool:
        """Include all trees."""
        return True

    def to_spec_string(self) -> str:
        """Return 'blob:limit=<size>' with appropriate unit."""
        size = self.limit
        if size >= 1024 * 1024 * 1024 and size % (1024 * 1024 * 1024) == 0:
            return f"blob:limit={size // (1024 * 1024 * 1024)}g"
        elif size >= 1024 * 1024 and size % (1024 * 1024) == 0:
            return f"blob:limit={size // (1024 * 1024)}m"
        elif size >= 1024 and size % 1024 == 0:
            return f"blob:limit={size // 1024}k"
        else:
            return f"blob:limit={size}"

    def __repr__(self) -> str:
        """Return string representation of the filter."""
        return f"BlobLimitFilter(limit={self.limit})"


class TreeDepthFilter(FilterSpec):
    """Filter that excludes trees beyond a specified depth."""

    def __init__(self, max_depth: int) -> None:
        """Initialize tree depth filter.

        Args:
            max_depth: Maximum tree depth (0 = only root tree)
        """
        self.max_depth = max_depth

    def should_include_blob(self, blob_size: int) -> bool:
        """Include all blobs."""
        return True

    def should_include_tree(self, depth: int) -> bool:
        """Include only trees up to max_depth."""
        return depth <= self.max_depth

    def to_spec_string(self) -> str:
        """Return 'tree:<depth>'."""
        return f"tree:{self.max_depth}"

    def __repr__(self) -> str:
        """Return string representation of the filter."""
        return f"TreeDepthFilter(max_depth={self.max_depth})"


class SparseOidFilter(FilterSpec):
    """Filter that uses a sparse specification from an object.

    This filter reads sparse-checkout patterns from a blob object and uses them
    to determine which paths should be included in the partial clone.
    """

    def __init__(
        self, oid: "ObjectID", object_store: "BaseObjectStore | None" = None
    ) -> None:
        """Initialize sparse OID filter.

        Args:
            oid: Object ID of the sparse specification blob
            object_store: Optional object store to load the sparse patterns from
        """
        self.oid = oid
        self._patterns: list[tuple[str, bool, bool, bool]] | None = None
        self._object_store = object_store

    def _load_patterns(self) -> None:
        """Load and parse sparse patterns from the blob."""
        if self._patterns is not None:
            return

        if self._object_store is None:
            raise ValueError("Cannot load sparse patterns without an object store")

        from .sparse_patterns import parse_sparse_patterns

        try:
            obj = self._object_store[self.oid]
        except KeyError:
            raise ValueError(
                f"Sparse specification blob {self.oid.hex() if isinstance(self.oid, bytes) else self.oid} not found"
            )

        if not isinstance(obj, Blob):
            raise ValueError(
                f"Sparse specification {self.oid.hex() if isinstance(self.oid, bytes) else self.oid} is not a blob"
            )

        # Parse the blob content as sparse patterns
        lines = obj.data.decode("utf-8").splitlines()
        self._patterns = parse_sparse_patterns(lines)

    def should_include_path(self, path: str) -> bool:
        """Determine if a path should be included based on sparse patterns.

        Args:
            path: Path to check (e.g., 'src/file.py')

        Returns:
            True if the path matches the sparse patterns, False otherwise
        """
        self._load_patterns()
        from .sparse_patterns import match_sparse_patterns

        # Determine if path is a directory based on whether it ends with '/'
        path_is_dir = path.endswith("/")
        path_str = path.rstrip("/")

        assert self._patterns is not None  # _load_patterns ensures this
        return match_sparse_patterns(path_str, self._patterns, path_is_dir=path_is_dir)

    def should_include_blob(self, blob_size: int) -> bool:
        """Include all blobs (sparse filtering is path-based, not size-based)."""
        return True

    def should_include_tree(self, depth: int) -> bool:
        """Include all trees (sparse filtering is path-based)."""
        return True

    def to_spec_string(self) -> str:
        """Return 'sparse:oid=<oid>'."""
        return f"sparse:oid={self.oid.decode('ascii') if isinstance(self.oid, bytes) else self.oid}"

    def __repr__(self) -> str:
        """Return string representation of the filter."""
        oid_str = self.oid.decode("ascii") if isinstance(self.oid, bytes) else self.oid
        return f"SparseOidFilter(oid={oid_str!r})"


class CombineFilter(FilterSpec):
    """Filter that combines multiple filters with AND logic."""

    def __init__(self, filters: list[FilterSpec]) -> None:
        """Initialize combine filter.

        Args:
            filters: List of filters to combine
        """
        self.filters = filters

    def should_include_blob(self, blob_size: int) -> bool:
        """Include blob only if all filters agree."""
        return all(f.should_include_blob(blob_size) for f in self.filters)

    def should_include_tree(self, depth: int) -> bool:
        """Include tree only if all filters agree."""
        return all(f.should_include_tree(depth) for f in self.filters)

    def to_spec_string(self) -> str:
        """Return 'combine:<filter1>+<filter2>+...'."""
        return "combine:" + "+".join(f.to_spec_string() for f in self.filters)

    def __repr__(self) -> str:
        """Return string representation of the filter."""
        return f"CombineFilter(filters={self.filters!r})"


def _parse_size(size_str: str) -> int:
    """Parse a size specification like '100', '10k', '5m', '1g'.

    Args:
        size_str: Size string with optional unit suffix

    Returns:
        Size in bytes

    Raises:
        ValueError: If size_str is not a valid size specification
    """
    size_str = size_str.lower()
    multipliers = {"k": 1024, "m": 1024 * 1024, "g": 1024 * 1024 * 1024}

    if size_str[-1] in multipliers:
        try:
            value = int(size_str[:-1])
            return value * multipliers[size_str[-1]]
        except ValueError:
            raise ValueError(f"Invalid size specification: {size_str}")
    else:
        try:
            return int(size_str)
        except ValueError:
            raise ValueError(f"Invalid size specification: {size_str}")


def parse_filter_spec(
    spec: str | bytes, object_store: "BaseObjectStore | None" = None
) -> FilterSpec:
    """Parse a filter specification string.

    Args:
        spec: Filter specification (e.g., 'blob:none', 'blob:limit=1m')
        object_store: Optional object store for loading sparse specifications

    Returns:
        Parsed FilterSpec object

    Raises:
        ValueError: If spec is not a valid filter specification

    Examples:
        >>> parse_filter_spec("blob:none")
        BlobNoneFilter()
        >>> parse_filter_spec("blob:limit=1m")
        BlobLimitFilter(limit=1048576)
        >>> parse_filter_spec("tree:0")
        TreeDepthFilter(max_depth=0)
    """
    if isinstance(spec, bytes):
        try:
            spec = spec.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ValueError(f"Filter specification must be valid UTF-8: {e}")

    spec = spec.strip()

    if not spec:
        raise ValueError("Filter specification cannot be empty")

    if spec == "blob:none":
        return BlobNoneFilter()
    elif spec.startswith("blob:limit="):
        limit_str = spec[11:]  # len('blob:limit=') == 11
        if not limit_str:
            raise ValueError("blob:limit requires a size value (e.g., blob:limit=1m)")
        try:
            limit = _parse_size(limit_str)
            if limit < 0:
                raise ValueError(
                    f"blob:limit size must be non-negative, got {limit_str}"
                )
            return BlobLimitFilter(limit)
        except ValueError as e:
            raise ValueError(f"Invalid blob:limit specification: {e}")
    elif spec.startswith("tree:"):
        depth_str = spec[5:]  # len('tree:') == 5
        if not depth_str:
            raise ValueError("tree filter requires a depth value (e.g., tree:0)")
        try:
            depth = int(depth_str)
            if depth < 0:
                raise ValueError(f"tree depth must be non-negative, got {depth}")
            return TreeDepthFilter(depth)
        except ValueError as e:
            raise ValueError(f"Invalid tree filter: {e}")
    elif spec.startswith("sparse:oid="):
        oid_str = spec[11:]  # len('sparse:oid=') == 11
        if not oid_str:
            raise ValueError(
                "sparse:oid requires an object ID (e.g., sparse:oid=abc123...)"
            )
        # Validate OID format (should be 40 hex chars for SHA-1 or 64 for SHA-256)
        if not valid_hexsha(oid_str):
            raise ValueError(
                f"sparse:oid requires a valid object ID (40 or 64 hex chars), got {len(oid_str)} chars"
            )

        oid: ObjectID = ObjectID(oid_str.encode("ascii"))
        return SparseOidFilter(oid, object_store=object_store)
    elif spec.startswith("combine:"):
        filter_str = spec[8:]  # len('combine:') == 8
        if not filter_str:
            raise ValueError(
                "combine filter requires at least one filter (e.g., combine:blob:none+tree:0)"
            )
        filter_specs = filter_str.split("+")
        if len(filter_specs) < 2:
            raise ValueError(
                "combine filter requires at least two filters separated by '+'"
            )
        try:
            filters = [
                parse_filter_spec(f, object_store=object_store) for f in filter_specs
            ]
        except ValueError as e:
            raise ValueError(f"Invalid filter in combine specification: {e}")
        return CombineFilter(filters)
    else:
        # Provide helpful error message with supported formats
        raise ValueError(
            f"Unknown filter specification: '{spec}'. "
            f"Supported formats: blob:none, blob:limit=<n>[kmg], tree:<depth>, "
            f"sparse:oid=<oid>, combine:<filter>+<filter>+..."
        )


def filter_pack_objects(
    object_store: "BaseObjectStore",
    object_ids: list["ObjectID"],
    filter_spec: FilterSpec,
) -> list["ObjectID"]:
    """Filter a list of object IDs based on a filter specification.

    This function examines each object and excludes those that don't match
    the filter criteria (e.g., blobs that are too large, trees beyond max depth).

    Args:
        object_store: Object store to retrieve objects from
        object_ids: List of object IDs to filter
        filter_spec: Filter specification to apply

    Returns:
        Filtered list of object IDs that should be included in the pack

    Note:
        This function currently supports blob size filtering. Tree depth filtering
        requires additional path/depth tracking which is not yet implemented.
    """
    filtered_ids = []

    for oid in object_ids:
        try:
            obj = object_store[oid]
        except KeyError:
            # Object not found, skip it
            continue

        # Determine object type and apply appropriate filter
        if isinstance(obj, Blob):
            # Check if blob should be included based on size
            blob_size = len(obj.data)
            if filter_spec.should_include_blob(blob_size):
                filtered_ids.append(oid)
            # else: blob is filtered out
        elif isinstance(obj, (Tree, Commit, Tag)):
            # For now, include all trees, commits, and tags
            # Tree depth filtering would require tracking depth during traversal
            # which needs to be implemented at the object collection stage
            if filter_spec.should_include_tree(0):  # depth=0 for now
                filtered_ids.append(oid)
        else:
            # Unknown object type, include it to be safe
            filtered_ids.append(oid)

    return filtered_ids


def filter_pack_objects_with_paths(
    object_store: "BaseObjectStore",
    wants: list["ObjectID"],
    filter_spec: FilterSpec,
    *,
    progress: "Callable[[bytes], None] | None" = None,
) -> list["ObjectID"]:
    """Filter objects for a pack with full path and depth tracking.

    This function performs a complete tree traversal starting from the wanted
    commits, tracking paths and depths to enable proper filtering for sparse:oid
    and tree:<depth> filters.

    Args:
        object_store: Object store to retrieve objects from
        wants: List of commit/tree/blob IDs that are wanted
        filter_spec: Filter specification to apply
        progress: Optional progress callback

    Returns:
        Filtered list of object IDs that should be included in the pack
    """
    import stat

    included_objects: set[ObjectID] = set()
    # Track (oid, path, depth) tuples to process
    to_process: list[tuple[ObjectID, str, int]] = []

    # Start with the wanted commits
    for want in wants:
        try:
            obj = object_store[want]
        except KeyError:
            continue

        if isinstance(obj, Commit):
            # Always include commits
            included_objects.add(want)
            # Add the root tree to process with depth 0
            to_process.append((obj.tree, "", 0))
        elif isinstance(obj, Tree):
            # Direct tree wants start at depth 0
            to_process.append((want, "", 0))
        elif isinstance(obj, Tag):
            # Always include tags
            included_objects.add(want)
            # Process the tagged object
            tagged_oid = obj.object[1]
            to_process.append((tagged_oid, "", 0))
        elif isinstance(obj, Blob):
            # Direct blob wants - check size filter
            blob_size = len(obj.data)
            if filter_spec.should_include_blob(blob_size):
                included_objects.add(want)

    # Process trees and their contents
    processed_trees: set[ObjectID] = set()

    while to_process:
        oid, current_path, depth = to_process.pop()

        # Skip if already processed
        if oid in processed_trees:
            continue

        try:
            obj = object_store[oid]
        except KeyError:
            continue

        if isinstance(obj, Tree):
            # Check if this tree should be included based on depth
            if not filter_spec.should_include_tree(depth):
                continue

            # Include this tree
            included_objects.add(oid)
            processed_trees.add(oid)

            # Process tree entries
            for name, mode, entry_oid in obj.iteritems():
                assert name is not None
                assert mode is not None
                assert entry_oid is not None

                # Skip gitlinks
                if S_ISGITLINK(mode):
                    continue

                # Build full path
                if current_path:
                    full_path = f"{current_path}/{name.decode('utf-8')}"
                else:
                    full_path = name.decode("utf-8")

                if stat.S_ISDIR(mode):
                    # It's a subdirectory - add to process list with increased depth
                    to_process.append((entry_oid, full_path, depth + 1))
                elif stat.S_ISREG(mode):
                    # It's a blob - check filters
                    try:
                        blob = object_store[entry_oid]
                    except KeyError:
                        continue

                    if not isinstance(blob, Blob):
                        continue

                    # Check filters
                    blob_size = len(blob.data)

                    # For non-path-based filters (size, blob:none), check directly
                    if not filter_spec.should_include_blob(blob_size):
                        continue

                    # Check path filter for sparse:oid
                    path_allowed = True
                    if isinstance(filter_spec, SparseOidFilter):
                        path_allowed = filter_spec.should_include_path(full_path)
                    elif isinstance(filter_spec, CombineFilter):
                        # Check path filters in combination
                        for f in filter_spec.filters:
                            if isinstance(f, SparseOidFilter):
                                if not f.should_include_path(full_path):
                                    path_allowed = False
                                    break

                    if not path_allowed:
                        continue

                    # Include this blob
                    included_objects.add(entry_oid)

        elif isinstance(obj, Blob):
            # Standalone blob (shouldn't normally happen in tree traversal)
            blob_size = len(obj.data)
            if filter_spec.should_include_blob(blob_size):
                included_objects.add(oid)

    return list(included_objects)
