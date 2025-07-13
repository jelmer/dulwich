# filters.py -- Git filter drivers (clean/smudge) implementation
# Copyright (C) 2024 Jelmer Vernooij
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

"""Implementation of Git filter drivers (clean/smudge filters)."""

import logging
import subprocess
from typing import TYPE_CHECKING, Callable, Optional, Protocol

from .attrs import GitAttributes
from .objects import Blob

if TYPE_CHECKING:
    from .config import StackedConfig


class FilterError(Exception):
    """Exception raised when filter operations fail."""


class FilterDriver(Protocol):
    """Protocol for filter drivers."""

    def clean(self, data: bytes) -> bytes:
        """Apply clean filter (working tree → repository)."""
        ...

    def smudge(self, data: bytes, path: bytes = b"") -> bytes:
        """Apply smudge filter (repository → working tree)."""
        ...


class ProcessFilterDriver:
    """Filter driver that executes external processes."""

    def __init__(
        self,
        clean_cmd: Optional[str] = None,
        smudge_cmd: Optional[str] = None,
        required: bool = False,
        cwd: Optional[str] = None,
    ) -> None:
        self.clean_cmd = clean_cmd
        self.smudge_cmd = smudge_cmd
        self.required = required
        self.cwd = cwd

    def clean(self, data: bytes) -> bytes:
        """Apply clean filter using external process."""
        if not self.clean_cmd:
            if self.required:
                raise FilterError("Clean command is required but not configured")
            return data

        try:
            result = subprocess.run(
                self.clean_cmd,
                shell=True,
                input=data,
                capture_output=True,
                check=True,
                cwd=self.cwd,
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            if self.required:
                raise FilterError(f"Required clean filter failed: {e}")
            # If not required, log warning and return original data on failure
            logging.warning(f"Optional clean filter failed: {e}")
            return data

    def smudge(self, data: bytes, path: bytes = b"") -> bytes:
        """Apply smudge filter using external process."""
        if not self.smudge_cmd:
            if self.required:
                raise FilterError("Smudge command is required but not configured")
            return data

        # Substitute %f placeholder with file path
        cmd = self.smudge_cmd.replace("%f", path.decode("utf-8", errors="replace"))

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                input=data,
                capture_output=True,
                check=True,
                cwd=self.cwd,
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            if self.required:
                raise FilterError(f"Required smudge filter failed: {e}")
            # If not required, log warning and return original data on failure
            logging.warning(f"Optional smudge filter failed: {e}")
            return data


class FilterRegistry:
    """Registry for filter drivers."""

    def __init__(self, config: Optional["StackedConfig"] = None, repo=None) -> None:
        self.config = config
        self.repo = repo
        self._drivers: dict[str, FilterDriver] = {}
        self._factories: dict[str, Callable[[FilterRegistry], FilterDriver]] = {}

        # Register built-in filter factories
        self.register_factory("lfs", self._create_lfs_filter)
        self.register_factory("text", self._create_text_filter)

        # Auto-register line ending filter if autocrlf is enabled
        self._setup_line_ending_filter()

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

        # Try to create from config first (respect user configuration)
        if self.config is not None:
            config_driver = self._create_from_config(name)
            if config_driver is not None:
                self._drivers[name] = config_driver
                return config_driver

        # Try to create from factory as fallback
        if name in self._factories:
            factory_driver = self._factories[name](self)
            self._drivers[name] = factory_driver
            return factory_driver

        return None

    def _create_from_config(self, name: str) -> Optional[FilterDriver]:
        """Create a filter driver from config."""
        if self.config is None:
            return None

        clean_cmd: Optional[str] = None
        smudge_cmd: Optional[str] = None

        # Get clean command
        try:
            clean_cmd_raw = self.config.get(("filter", name), "clean")
            if isinstance(clean_cmd_raw, bytes):
                clean_cmd = clean_cmd_raw.decode("utf-8")
            else:
                clean_cmd = clean_cmd_raw
        except KeyError:
            pass

        # Get smudge command
        try:
            smudge_cmd_raw = self.config.get(("filter", name), "smudge")
            if isinstance(smudge_cmd_raw, bytes):
                smudge_cmd = smudge_cmd_raw.decode("utf-8")
            else:
                smudge_cmd = smudge_cmd_raw
        except KeyError:
            pass

        # Get required flag (defaults to False)
        required = self.config.get_boolean(("filter", name), "required", False)

        if clean_cmd or smudge_cmd:
            # Get repository working directory
            repo_path = self.repo.path if self.repo else None
            return ProcessFilterDriver(clean_cmd, smudge_cmd, required, repo_path)

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

        config = registry.repo.get_config_stack() if registry.repo else None
        return LFSFilterDriver(lfs_store, config=config)

    def _create_text_filter(self, registry: "FilterRegistry") -> FilterDriver:
        """Create text filter driver for line ending conversion.

        This filter is used when files have the 'text' attribute set explicitly.
        It always normalizes line endings on checkin (CRLF -> LF).
        """
        from .line_ending import (
            LineEndingFilter,
            convert_crlf_to_lf,
            get_smudge_filter,
        )

        if self.config is None:
            # Default text filter: always normalize on checkin
            return LineEndingFilter(
                clean_conversion=convert_crlf_to_lf,
                smudge_conversion=None,
                binary_detection=True,
            )

        # Get core.eol and core.autocrlf settings for smudge behavior
        try:
            core_eol_raw = self.config.get("core", "eol")
            core_eol: str = (
                core_eol_raw.decode("ascii")
                if isinstance(core_eol_raw, bytes)
                else core_eol_raw
            )
        except KeyError:
            core_eol = "native"

        # Parse autocrlf as bytes (can be b"true", b"input", or b"false")
        try:
            autocrlf_raw = self.config.get("core", "autocrlf")
            autocrlf: bytes = (
                autocrlf_raw.lower()
                if isinstance(autocrlf_raw, bytes)
                else str(autocrlf_raw).lower().encode("ascii")
            )
        except KeyError:
            autocrlf = b"false"

        # For explicit text attribute:
        # - Always normalize to LF on checkin (clean)
        # - Smudge behavior depends on core.eol and core.autocrlf
        smudge_filter = get_smudge_filter(core_eol, autocrlf)
        clean_filter = convert_crlf_to_lf

        return LineEndingFilter(
            clean_conversion=clean_filter,
            smudge_conversion=smudge_filter,
            binary_detection=True,
        )

    def _setup_line_ending_filter(self) -> None:
        """Automatically register line ending filter if configured."""
        if self.config is None:
            return

        # Parse autocrlf as bytes
        try:
            autocrlf_raw = self.config.get("core", "autocrlf")
            autocrlf: bytes = (
                autocrlf_raw.lower()
                if isinstance(autocrlf_raw, bytes)
                else str(autocrlf_raw).lower().encode("ascii")
            )
        except KeyError:
            return

        # If autocrlf is enabled, register the text filter
        if autocrlf in (b"true", b"input"):
            # Pre-create the text filter so it's available
            self.get_driver("text")


def get_filter_for_path(
    path: bytes,
    gitattributes: "GitAttributes",
    filter_registry: FilterRegistry,
) -> Optional[FilterDriver]:
    """Get the appropriate filter driver for a given path.

    Args:
        path: Path to check
        gitattributes: GitAttributes object with parsed patterns
        filter_registry: Registry of filter drivers

    Returns:
        FilterDriver instance or None
    """
    # Get all attributes for this path
    attributes = gitattributes.match_path(path)

    # Check if there's a filter attribute
    filter_name = attributes.get(b"filter")
    if filter_name is not None:
        if isinstance(filter_name, bool):
            return None
        if isinstance(filter_name, bytes):
            filter_name_str = filter_name.decode("utf-8")
            driver = filter_registry.get_driver(filter_name_str)

            # Check if filter is required but missing
            if driver is None and filter_registry.config is not None:
                required = filter_registry.config.get_boolean(
                    ("filter", filter_name_str), "required", False
                )
                if required:
                    raise FilterError(
                        f"Required filter '{filter_name_str}' is not available"
                    )

            return driver
        return None

    # Check for text attribute
    text_attr = attributes.get(b"text")
    if text_attr is True:
        # Use the text filter for line ending conversion
        return filter_registry.get_driver("text")
    elif text_attr is False:
        # -text means binary, no conversion
        return None

    # If no explicit text attribute, check if autocrlf is enabled
    # When autocrlf is true/input, files are treated as text by default
    if filter_registry.config is not None:
        try:
            autocrlf_raw = filter_registry.config.get("core", "autocrlf")
            autocrlf: bytes = (
                autocrlf_raw.lower()
                if isinstance(autocrlf_raw, bytes)
                else str(autocrlf_raw).lower().encode("ascii")
            )
            if autocrlf in (b"true", b"input"):
                # Use text filter for files without explicit attributes
                return filter_registry.get_driver("text")
        except KeyError:
            pass

    return None


class FilterBlobNormalizer:
    """Blob normalizer that applies clean/smudge filters based on gitattributes.

    This can be used in addition to or instead of line ending normalization.
    """

    def __init__(
        self,
        config_stack: Optional["StackedConfig"],
        gitattributes: GitAttributes,
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
        filtered_data = filter_driver.smudge(blob.data, path)
        if filtered_data == blob.data:
            return blob

        # Create new blob with filtered data
        new_blob = Blob()
        new_blob.data = filtered_data
        return new_blob
