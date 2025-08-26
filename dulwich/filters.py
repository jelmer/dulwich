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
import threading
from typing import TYPE_CHECKING, Callable, Optional
from typing import Protocol as TypingProtocol

from .attrs import GitAttributes
from .objects import Blob

if TYPE_CHECKING:
    from .config import StackedConfig
    from .protocol import Protocol
    from .repo import BaseRepo


class FilterError(Exception):
    """Exception raised when filter operations fail."""


class FilterDriver(TypingProtocol):
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
        process_cmd: Optional[str] = None,
    ) -> None:
        """Initialize ProcessFilterDriver.

        Args:
          clean_cmd: Command to run for clean filter
          smudge_cmd: Command to run for smudge filter
          required: Whether the filter is required
          cwd: Working directory for filter execution
          process_cmd: Command to run for process filter (preferred for performance)
        """
        self.clean_cmd = clean_cmd
        self.smudge_cmd = smudge_cmd
        self.required = required
        self.cwd = cwd
        self.process_cmd = process_cmd
        self._process: Optional[subprocess.Popen] = None
        self._protocol: Optional[Protocol] = None
        self._capabilities: set[bytes] = set()
        self._process_lock = threading.Lock()

    def _get_or_start_process(self):
        """Get or start the long-running process filter."""
        if self._process is None and self.process_cmd:
            from .errors import HangupException
            from .protocol import Protocol

            try:
                self._process = subprocess.Popen(
                    self.process_cmd,
                    shell=True,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.cwd,
                    text=False,  # Use bytes
                )

                # Check if process started successfully
                if self._process.poll() is not None:
                    # Process already terminated
                    raise OSError(
                        f"Process terminated immediately with code {self._process.returncode}"
                    )

                # Create protocol wrapper
                def write_func(data):
                    n = self._process.stdin.write(data)
                    self._process.stdin.flush()
                    return n

                def read_func(size):
                    return self._process.stdout.read(size)

                self._protocol = Protocol(read_func, write_func)

                # Send handshake using pkt-line format
                self._protocol.write_pkt_line(b"git-filter-client")
                self._protocol.write_pkt_line(b"version=2")
                self._protocol.write_pkt_line(None)  # flush packet

                # Read handshake response
                welcome = self._protocol.read_pkt_line()
                version = self._protocol.read_pkt_line()
                flush = self._protocol.read_pkt_line()

                # Verify handshake (be liberal - accept with or without newlines)
                if welcome and welcome.rstrip(b"\n\r") != b"git-filter-server":
                    raise FilterError(f"Invalid welcome message: {welcome}")
                if version and version.rstrip(b"\n\r") != b"version=2":
                    raise FilterError(f"Invalid version: {version}")
                if flush is not None:
                    raise FilterError("Expected flush packet after handshake")

                # Send capabilities
                self._protocol.write_pkt_line(b"capability=clean")
                self._protocol.write_pkt_line(b"capability=smudge")
                self._protocol.write_pkt_line(None)  # flush packet

                # Read capability response
                capabilities = []
                while True:
                    pkt = self._protocol.read_pkt_line()
                    if pkt is None:  # flush packet
                        break
                    capabilities.append(pkt)

                # Store supported capabilities
                self._capabilities = set()
                for cap in capabilities:
                    cap = cap.rstrip(b"\n\r")  # Be liberal - strip any line endings
                    if cap.startswith(b"capability="):
                        self._capabilities.add(cap[11:])  # Remove "capability=" prefix

            except (OSError, subprocess.SubprocessError, HangupException) as e:
                self._cleanup_process()
                raise FilterError(f"Failed to start process filter: {e}")
        return self._process

    def _use_process_filter(self, data: bytes, operation: str, path: str = "") -> bytes:
        """Use the long-running process filter for the operation."""
        with self._process_lock:
            try:
                proc = self._get_or_start_process()
                if proc is None:
                    return data

                operation_bytes = operation.encode()
                if operation_bytes not in self._capabilities:
                    raise FilterError(f"Operation {operation} not supported by filter")

                if not self._protocol:
                    raise FilterError("Protocol not initialized")

                # Send request using pkt-line format
                self._protocol.write_pkt_line(f"command={operation}".encode())
                self._protocol.write_pkt_line(f"pathname={path}".encode())
                self._protocol.write_pkt_line(None)  # flush packet

                # Send data
                # Split data into chunks if needed (max pkt-line payload is 65516 bytes)
                chunk_size = 65516
                for i in range(0, len(data), chunk_size):
                    chunk = data[i : i + chunk_size]
                    self._protocol.write_pkt_line(chunk)
                self._protocol.write_pkt_line(None)  # flush packet to end data

                # Read response
                response_headers = {}
                while True:
                    pkt = self._protocol.read_pkt_line()
                    if pkt is None:  # flush packet ends headers
                        break
                    key, _, value = pkt.decode().rstrip("\n\r").partition("=")
                    response_headers[key] = value

                # Check status
                status = response_headers.get("status", "error")
                if status != "success":
                    raise FilterError(f"Process filter {operation} failed: {status}")

                # Read result data
                result_chunks = []
                while True:
                    pkt = self._protocol.read_pkt_line()
                    if pkt is None:  # flush packet ends data
                        break
                    result_chunks.append(pkt)

                return b"".join(result_chunks)

            except (OSError, subprocess.SubprocessError, ValueError) as e:
                # Clean up broken process
                self._cleanup_process()
                raise FilterError(f"Process filter failed: {e}")

    def clean(self, data: bytes) -> bytes:
        """Apply clean filter using external process."""
        # Try process filter first (much faster)
        if self.process_cmd:
            try:
                return self._use_process_filter(data, "clean")
            except FilterError as e:
                if self.required:
                    raise
                logging.warning(f"Process filter failed, falling back: {e}")

        # Fall back to clean command
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
        path_str = path.decode("utf-8", errors="replace")

        # Try process filter first (much faster)
        if self.process_cmd:
            try:
                return self._use_process_filter(data, "smudge", path_str)
            except FilterError as e:
                if self.required:
                    raise
                logging.warning(f"Process filter failed, falling back: {e}")

        # Fall back to smudge command
        if not self.smudge_cmd:
            if self.required:
                raise FilterError("Smudge command is required but not configured")
            return data

        # Substitute %f placeholder with file path
        cmd = self.smudge_cmd.replace("%f", path_str)

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

    def _cleanup_process(self):
        """Clean up the process filter."""
        if self._process:
            # Close stdin first to signal the process to quit cleanly
            if self._process.stdin and not self._process.stdin.closed:
                try:
                    self._process.stdin.close()
                except BrokenPipeError:
                    pass

            # Try to terminate gracefully first
            if self._process.poll() is None:  # Still running
                try:
                    self._process.terminate()
                    self._process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    # Force kill if terminate didn't work
                    try:
                        self._process.kill()
                        self._process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        # On Windows, sometimes we need to be more aggressive
                        import os

                        if os.name == "nt":
                            try:
                                subprocess.run(
                                    [
                                        "taskkill",
                                        "/F",
                                        "/T",
                                        "/PID",
                                        str(self._process.pid),
                                    ],
                                    capture_output=True,
                                    timeout=5,
                                )
                                self._process.wait(timeout=1)
                            except (
                                subprocess.CalledProcessError,
                                subprocess.TimeoutExpired,
                            ):
                                pass
                        else:
                            try:
                                import signal

                                os.kill(self._process.pid, signal.SIGKILL)
                                self._process.wait(timeout=1)
                            except (ProcessLookupError, subprocess.TimeoutExpired):
                                pass
                except ProcessLookupError:
                    # Process already dead
                    pass
        self._process = None
        self._protocol = None

    def __del__(self):
        """Clean up the process filter on destruction."""
        self._cleanup_process()


class FilterRegistry:
    """Registry for filter drivers."""

    def __init__(
        self,
        config: Optional["StackedConfig"] = None,
        repo: Optional["BaseRepo"] = None,
    ) -> None:
        """Initialize FilterRegistry.

        Args:
          config: Git configuration stack
          repo: Repository instance
        """
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

    def close(self) -> None:
        """Close all filter drivers, ensuring process cleanup."""
        for driver in self._drivers.values():
            if isinstance(driver, ProcessFilterDriver):
                driver._cleanup_process()
        self._drivers.clear()

    def __del__(self) -> None:
        """Clean up filter drivers on destruction."""
        try:
            self.close()
        except Exception:
            # Don't raise exceptions in __del__
            pass

    def _create_from_config(self, name: str) -> Optional[FilterDriver]:
        """Create a filter driver from config."""
        if self.config is None:
            return None

        clean_cmd: Optional[str] = None
        smudge_cmd: Optional[str] = None
        process_cmd: Optional[str] = None

        # Get process command (preferred over clean/smudge for performance)
        try:
            process_cmd_raw = self.config.get(("filter", name), "process")
            if isinstance(process_cmd_raw, bytes):
                process_cmd = process_cmd_raw.decode("utf-8")
            else:
                process_cmd = process_cmd_raw
        except KeyError:
            pass

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

        if process_cmd or clean_cmd or smudge_cmd:
            # Get repository working directory (only for Repo, not BaseRepo)
            from .repo import Repo

            repo_path = (
                self.repo.path if self.repo and isinstance(self.repo, Repo) else None
            )
            return ProcessFilterDriver(
                clean_cmd, smudge_cmd, required, repo_path, process_cmd
            )

        return None

    def _create_lfs_filter(self, registry: "FilterRegistry") -> FilterDriver:
        """Create LFS filter driver."""
        from .lfs import LFSFilterDriver, LFSStore

        # If we have a Repo (not just BaseRepo), use its LFS store
        from .repo import Repo

        if registry.repo is not None and isinstance(registry.repo, Repo):
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
        repo: Optional["BaseRepo"] = None,
    ) -> None:
        """Initialize FilterBlobNormalizer.

        Args:
          config_stack: Git configuration stack
          gitattributes: GitAttributes instance
          filter_registry: Optional filter registry to use
          repo: Optional repository instance
        """
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

    def close(self) -> None:
        """Close all filter drivers, ensuring process cleanup."""
        self.filter_registry.close()

    def __del__(self) -> None:
        """Clean up filter drivers on destruction."""
        try:
            self.close()
        except Exception:
            # Don't raise exceptions in __del__
            pass
