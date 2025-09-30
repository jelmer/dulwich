# merge_drivers.py -- Merge driver support for dulwich
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
#
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

"""Merge driver support for dulwich."""

import os
import subprocess
import tempfile
from typing import Callable, Optional, Protocol

from .config import Config


class MergeDriver(Protocol):
    """Protocol for merge drivers."""

    def merge(
        self,
        ancestor: bytes,
        ours: bytes,
        theirs: bytes,
        path: Optional[str] = None,
        marker_size: int = 7,
    ) -> tuple[bytes, bool]:
        """Perform a three-way merge.

        Args:
            ancestor: Content of the common ancestor version
            ours: Content of our version
            theirs: Content of their version
            path: Optional path of the file being merged
            marker_size: Size of conflict markers (default 7)

        Returns:
            Tuple of (merged content, success flag)
            If success is False, the content may contain conflict markers
        """
        ...


class ProcessMergeDriver:
    """Merge driver that runs an external process."""

    def __init__(self, command: str, name: str = "custom"):
        """Initialize process merge driver.

        Args:
            command: Command to run for merging
            name: Name of the merge driver
        """
        self.command = command
        self.name = name

    def merge(
        self,
        ancestor: bytes,
        ours: bytes,
        theirs: bytes,
        path: Optional[str] = None,
        marker_size: int = 7,
    ) -> tuple[bytes, bool]:
        """Perform merge using external process.

        The command is executed with the following placeholders:
        - %O: path to ancestor version (base)
        - %A: path to our version
        - %B: path to their version
        - %L: conflict marker size
        - %P: original path of the file

        The command should write the merge result to the file at %A.
        Exit code 0 means successful merge, non-zero means conflicts.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write temporary files
            ancestor_path = os.path.join(tmpdir, "ancestor")
            ours_path = os.path.join(tmpdir, "ours")
            theirs_path = os.path.join(tmpdir, "theirs")

            with open(ancestor_path, "wb") as f:
                f.write(ancestor)
            with open(ours_path, "wb") as f:
                f.write(ours)
            with open(theirs_path, "wb") as f:
                f.write(theirs)

            # Prepare command with placeholders
            cmd = self.command
            cmd = cmd.replace("%O", ancestor_path)
            cmd = cmd.replace("%A", ours_path)
            cmd = cmd.replace("%B", theirs_path)
            cmd = cmd.replace("%L", str(marker_size))
            if path:
                cmd = cmd.replace("%P", path)

            # Execute merge command
            try:
                result = subprocess.run(
                    cmd,
                    shell=True,
                    capture_output=True,
                    text=False,
                )

                # Read merged content from ours file
                with open(ours_path, "rb") as f:
                    merged_content = f.read()

                # Exit code 0 means clean merge, non-zero means conflicts
                success = result.returncode == 0

                return merged_content, success

            except subprocess.SubprocessError:
                # If the command fails completely, return original with conflicts
                return ours, False


class MergeDriverRegistry:
    """Registry for merge drivers."""

    def __init__(self, config: Optional[Config] = None):
        """Initialize merge driver registry.

        Args:
            config: Git configuration object
        """
        self._drivers: dict[str, MergeDriver] = {}
        self._factories: dict[str, Callable[[], MergeDriver]] = {}
        self._config = config

        # Register built-in drivers
        self._register_builtin_drivers()

    def _register_builtin_drivers(self) -> None:
        """Register built-in merge drivers."""
        # The "text" driver is the default three-way merge
        # We don't register it here as it's handled by the default merge code

    def register_driver(self, name: str, driver: MergeDriver) -> None:
        """Register a merge driver instance.

        Args:
            name: Name of the merge driver
            driver: Driver instance
        """
        self._drivers[name] = driver

    def register_factory(self, name: str, factory: Callable[[], MergeDriver]) -> None:
        """Register a factory function for creating merge drivers.

        Args:
            name: Name of the merge driver
            factory: Factory function that returns a MergeDriver
        """
        self._factories[name] = factory

    def get_driver(self, name: str) -> Optional[MergeDriver]:
        """Get a merge driver by name.

        Args:
            name: Name of the merge driver

        Returns:
            MergeDriver instance or None if not found
        """
        # First check registered drivers
        if name in self._drivers:
            return self._drivers[name]

        # Then check factories
        if name in self._factories:
            driver = self._factories[name]()
            self._drivers[name] = driver
            return driver

        # Finally check configuration
        if self._config:
            config_driver = self._create_from_config(name)
            if config_driver is not None:
                self._drivers[name] = config_driver
                return config_driver

        return None

    def _create_from_config(self, name: str) -> Optional[MergeDriver]:
        """Create a merge driver from git configuration.

        Args:
            name: Name of the merge driver

        Returns:
            MergeDriver instance or None if not configured
        """
        if not self._config:
            return None

        # Look for merge.<name>.driver configuration
        try:
            command = self._config.get(("merge", name), "driver")
            if command:
                return ProcessMergeDriver(command.decode(), name)
        except KeyError:
            pass

        return None


# Global registry instance
_merge_driver_registry: Optional[MergeDriverRegistry] = None


def get_merge_driver_registry(config: Optional[Config] = None) -> MergeDriverRegistry:
    """Get the global merge driver registry.

    Args:
        config: Git configuration object

    Returns:
        MergeDriverRegistry instance
    """
    global _merge_driver_registry
    if _merge_driver_registry is None:
        _merge_driver_registry = MergeDriverRegistry(config)
    elif config is not None:
        # Update config if provided
        _merge_driver_registry._config = config
    return _merge_driver_registry
