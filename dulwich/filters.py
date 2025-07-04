# filters.py -- Git filter drivers (clean/smudge) implementation
# Copyright (C) 2024 Jelmer Vernooij
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

"""Implementation of Git filter drivers (clean/smudge filters)."""

import subprocess
from collections.abc import Mapping
from typing import TYPE_CHECKING, Callable, Optional, Protocol

from .attrs import AttributeValue, Pattern, match_path
from .objects import Blob

if TYPE_CHECKING:
    from .config import StackedConfig


class FilterDriver(Protocol):
    """Protocol for filter drivers."""

    def clean(self, data: bytes) -> bytes:
        """Apply clean filter (working tree → repository)."""
        ...

    def smudge(self, data: bytes) -> bytes:
        """Apply smudge filter (repository → working tree)."""
        ...


class ProcessFilterDriver:
    """Filter driver that executes external processes."""

    def __init__(
        self, clean_cmd: Optional[str] = None, smudge_cmd: Optional[str] = None
    ) -> None:
        self.clean_cmd = clean_cmd
        self.smudge_cmd = smudge_cmd

    def clean(self, data: bytes) -> bytes:
        """Apply clean filter using external process."""
        if not self.clean_cmd:
            return data

        result = subprocess.run(
            self.clean_cmd,
            shell=True,
            input=data,
            capture_output=True,
            check=True,
        )
        return result.stdout

    def smudge(self, data: bytes) -> bytes:
        """Apply smudge filter using external process."""
        if not self.smudge_cmd:
            return data

        result = subprocess.run(
            self.smudge_cmd,
            shell=True,
            input=data,
            capture_output=True,
            check=True,
        )
        return result.stdout


class FilterRegistry:
    """Registry for filter drivers."""

    def __init__(self, config: Optional["StackedConfig"] = None, repo=None) -> None:
        self.config = config
        self.repo = repo
        self._drivers: dict[str, FilterDriver] = {}
        self._factories: dict[str, Callable[[FilterRegistry], FilterDriver]] = {}

        # Register built-in filter factories
        self.register_factory("lfs", self._create_lfs_filter)

    def register_factory(
        self, name: str, factory: Callable[["FilterRegistry"], FilterDriver]
    ) -> None:
        """Register a filter driver factory."""
        self._factories[name] = factory

    def register_driver(self, name: str, driver: FilterDriver) -> None:
        """Register a filter driver instance."""
        self._drivers[name] = driver

    def get_driver(self, name: str) -> Optional[FilterDriver]:
        """Get a filter driver by name."""
        # Check if we already have an instance
        if name in self._drivers:
            return self._drivers[name]

        # Try to create from factory
        if name in self._factories:
            driver = self._factories[name](self)
            self._drivers[name] = driver
            return driver

        # Try to create from config
        if self.config is not None:
            config_driver = self._create_from_config(name)
            if config_driver is not None:
                self._drivers[name] = config_driver
                return config_driver

        return None

    def _create_from_config(self, name: str) -> Optional[FilterDriver]:
        """Create a filter driver from config."""
        if self.config is None:
            return None

        clean_cmd: Optional[str] = None
        smudge_cmd: Optional[str] = None

        # Get clean command
        try:
            clean_value = self.config.get(("filter", name), "clean")
            if isinstance(clean_value, bytes):
                clean_cmd = clean_value.decode("utf-8")
            else:
                clean_cmd = clean_value
        except KeyError:
            pass

        # Get smudge command
        try:
            smudge_value = self.config.get(("filter", name), "smudge")
            if isinstance(smudge_value, bytes):
                smudge_cmd = smudge_value.decode("utf-8")
            else:
                smudge_cmd = smudge_value
        except KeyError:
            pass

        if clean_cmd or smudge_cmd:
            return ProcessFilterDriver(clean_cmd, smudge_cmd)

        return None

    def _create_lfs_filter(self, registry: "FilterRegistry") -> FilterDriver:
        """Create LFS filter driver."""
        from .lfs import LFSFilterDriver, LFSStore

        # If we have a repo, use its LFS store
        if registry.repo is not None:
            lfs_store = LFSStore.from_repo(registry.repo, create=True)
        else:
            # Fall back to creating a temporary LFS store
            import tempfile

            lfs_dir = tempfile.mkdtemp(prefix="dulwich-lfs-")
            lfs_store = LFSStore.create(lfs_dir)

        return LFSFilterDriver(lfs_store)


def get_filter_for_path(
    path: bytes,
    gitattributes: dict[bytes, dict[bytes, AttributeValue]],
    filter_registry: FilterRegistry,
) -> Optional[FilterDriver]:
    """Get the appropriate filter driver for a given path.

    Args:
        path: Path to check
        gitattributes: Parsed gitattributes (pattern -> attributes mapping)
        filter_registry: Registry of filter drivers

    Returns:
        FilterDriver instance or None
    """
    # Convert gitattributes dict to list of (Pattern, attrs) tuples
    patterns: list[tuple[Pattern, Mapping[bytes, AttributeValue]]] = []
    for pattern_bytes, attrs in gitattributes.items():
        pattern = Pattern(pattern_bytes)
        patterns.append((pattern, attrs))

    # Get all attributes for this path
    attributes = match_path(patterns, path)

    # Check if there's a filter attribute
    filter_name = attributes.get(b"filter")
    if filter_name is not None:
        if isinstance(filter_name, bool):
            return None
        if isinstance(filter_name, bytes):
            filter_name_str = filter_name.decode("utf-8")
            return filter_registry.get_driver(filter_name_str)
        return None

    return None


class FilterBlobNormalizer:
    """Blob normalizer that applies clean/smudge filters based on gitattributes.

    This can be used in addition to or instead of line ending normalization.
    """

    def __init__(
        self,
        config_stack: Optional["StackedConfig"],
        gitattributes: dict[bytes, dict[bytes, AttributeValue]],
        filter_registry: Optional[FilterRegistry] = None,
        repo=None,
    ) -> None:
        self.config_stack = config_stack
        self.gitattributes = gitattributes
        self.filter_registry = filter_registry or FilterRegistry(config_stack, repo)

    def checkin_normalize(self, blob: Blob, path: bytes) -> Blob:
        """Apply clean filter during checkin (working tree -> repository)."""
        # Get filter for this path
        filter_driver = get_filter_for_path(
            path, self.gitattributes, self.filter_registry
        )
        if filter_driver is None:
            return blob

        # Apply clean filter
        filtered_data = filter_driver.clean(blob.data)
        if filtered_data == blob.data:
            return blob

        # Create new blob with filtered data
        new_blob = Blob()
        new_blob.data = filtered_data
        return new_blob

    def checkout_normalize(self, blob: Blob, path: bytes) -> Blob:
        """Apply smudge filter during checkout (repository -> working tree)."""
        # Get filter for this path
        filter_driver = get_filter_for_path(
            path, self.gitattributes, self.filter_registry
        )
        if filter_driver is None:
            return blob

        # Apply smudge filter
        filtered_data = filter_driver.smudge(blob.data)
        if filtered_data == blob.data:
            return blob

        # Create new blob with filtered data
        new_blob = Blob()
        new_blob.data = filtered_data
        return new_blob
