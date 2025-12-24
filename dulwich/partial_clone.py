# partial_clone.py -- Partial clone filter specification handling
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

"""Partial clone filter specification parsing and handling.

This module implements Git's partial clone filter specifications as documented in:
https://git-scm.com/docs/rev-list-options#Documentation/rev-list-options.txt---filterltfilter-specgt

Supported filter specs:
- blob:none - Exclude all blobs
- blob:limit=<n>[kmg] - Exclude blobs larger than n bytes/KB/MB/GB
- tree:<depth> - Exclude trees beyond specified depth
- sparse:oid=<oid> - Use sparse specification from object
- combine:<filter>+<filter>+... - Combine multiple filters
"""

__all__ = [
    "FilterSpec",
    "BlobNoneFilter",
    "BlobLimitFilter",
    "TreeDepthFilter",
    "SparseOidFilter",
    "CombineFilter",
    "parse_filter_spec",
    "filter_pack_objects",
]

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .objects import ObjectID, ShaFile


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
            Filter specification string (e.g., 'blob:none', 'blob:limit=1m')
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
        return f"BlobNoneFilter()"


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
        return f"TreeDepthFilter(max_depth={self.max_depth})"


class SparseOidFilter(FilterSpec):
    """Filter that uses a sparse specification from an object."""

    def __init__(self, oid: "ObjectID") -> None:
        """Initialize sparse OID filter.

        Args:
            oid: Object ID of the sparse specification
        """
        self.oid = oid

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


def parse_filter_spec(spec: str | bytes) -> FilterSpec:
    """Parse a filter specification string.

    Args:
        spec: Filter specification (e.g., 'blob:none', 'blob:limit=1m')

    Returns:
        Parsed FilterSpec object

    Raises:
        ValueError: If spec is not a valid filter specification
    """
    if isinstance(spec, bytes):
        spec = spec.decode("utf-8")

    spec = spec.strip()

    if spec == "blob:none":
        return BlobNoneFilter()
    elif spec.startswith("blob:limit="):
        limit_str = spec[11:]  # len('blob:limit=') == 11
        limit = _parse_size(limit_str)
        return BlobLimitFilter(limit)
    elif spec.startswith("tree:"):
        depth_str = spec[5:]  # len('tree:') == 5
        try:
            depth = int(depth_str)
            return TreeDepthFilter(depth)
        except ValueError:
            raise ValueError(f"Invalid tree depth: {depth_str}")
    elif spec.startswith("sparse:oid="):
        oid_str = spec[11:]  # len('sparse:oid=') == 11
        # Convert to bytes for OID
        oid = oid_str.encode("ascii")
        return SparseOidFilter(oid)
    elif spec.startswith("combine:"):
        filter_specs = spec[8:].split("+")  # len('combine:') == 8
        filters = [parse_filter_spec(f) for f in filter_specs]
        return CombineFilter(filters)
    else:
        raise ValueError(f"Unknown filter specification: {spec}")


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
    from .objects import Blob, Tree, Commit, Tag

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
