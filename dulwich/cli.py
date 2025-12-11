#
# dulwich - Simple command-line interface to Dulwich
# Copyright (C) 2008-2011 Jelmer Vernooij <jelmer@jelmer.uk>
# vim: expandtab
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

"""Simple command-line interface to Dulwich>.

This is a very simple command-line wrapper for Dulwich. It is by
no means intended to be a full-blown Git command-line interface but just
a way to test Dulwich.
"""

__all__ = [
    "AutoFlushBinaryIOWrapper",
    "AutoFlushTextIOWrapper",
    "Command",
    "CommitMessageError",
    "Pager",
    "PagerBuffer",
    "SuperCommand",
    "detect_terminal_width",
    "disable_pager",
    "enable_pager",
    "format_bytes",
    "format_columns",
    "get_pager",
    "launch_editor",
    "main",
    "parse_time_to_timestamp",
    "signal_int",
    "signal_quit",
    "to_display_str",
    "write_columns",
]

# TODO: Add support for GIT_NAMESPACE environment variable by wrapping
# repository refs with NamespacedRefsContainer when the environment
# variable is set. See issue #1809 and dulwich.refs.NamespacedRefsContainer.

import argparse
import io
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import types
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import (
    BinaryIO,
    ClassVar,
    TextIO,
)

from dulwich import porcelain
from dulwich._typing import Buffer
from dulwich.refs import HEADREF, Ref

from .bundle import Bundle, create_bundle_from_repo, read_bundle, write_bundle
from .client import get_transport_and_path
from .config import Config
from .errors import (
    ApplyDeltaError,
    FileFormatException,
    GitProtocolError,
    NotGitRepository,
)
from .index import Index
from .log_utils import _configure_logging_from_trace
from .objects import Commit, ObjectID, RawObjectID, sha_to_hex, valid_hexsha
from .objectspec import parse_commit_range
from .pack import Pack
from .patch import DiffAlgorithmNotAvailable
from .repo import Repo

logger = logging.getLogger(__name__)


def to_display_str(value: bytes | str) -> str:
    """Convert a bytes or string value to a display string.

    Args:
        value: The value to convert (bytes or str)

    Returns:
        A string suitable for display
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _should_auto_flush(
    stream: TextIO | BinaryIO, env: Mapping[str, str] | None = None
) -> bool:
    """Determine if output should be auto-flushed based on GIT_FLUSH environment variable.

    Args:
        stream: The output stream to check
        env: Environment variables dict (defaults to os.environ)

    Returns:
        True if output should be flushed after each write, False otherwise
    """
    if env is None:
        env = os.environ
    git_flush = env.get("GIT_FLUSH", "").strip()
    if git_flush == "1":
        return True
    elif git_flush == "0":
        return False
    else:
        # Auto-detect: don't flush if redirected to a file
        return hasattr(stream, "isatty") and not stream.isatty()


class AutoFlushTextIOWrapper:
    """Wrapper that automatically flushes a TextIO stream based on configuration.

    This wrapper can be configured to flush after each write operation,
    which is useful for real-time output monitoring in CI/CD systems.
    """

    def __init__(self, stream: TextIO) -> None:
        """Initialize the wrapper.

        Args:
            stream: The stream to wrap
        """
        self._stream = stream

    @classmethod
    def env(
        cls, stream: TextIO, env: Mapping[str, str] | None = None
    ) -> "AutoFlushTextIOWrapper | TextIO":
        """Create wrapper respecting the GIT_FLUSH environment variable.

        Respects the GIT_FLUSH environment variable:
        - GIT_FLUSH=1: Always flush after each write
        - GIT_FLUSH=0: Never auto-flush (use buffered I/O)
        - Not set: Auto-detect based on whether output is redirected

        Args:
            stream: The stream to wrap
            env: Environment variables dict (defaults to os.environ)

        Returns:
            AutoFlushTextIOWrapper instance configured based on GIT_FLUSH
        """
        if _should_auto_flush(stream, env):
            return cls(stream)
        else:
            return stream

    def write(self, data: str) -> int:
        """Write data to the stream and optionally flush.

        Args:
            data: Data to write

        Returns:
            Number of characters written
        """
        result = self._stream.write(data)
        self._stream.flush()
        return result

    def writelines(self, lines: Iterable[str]) -> None:
        """Write multiple lines to the stream and optionally flush.

        Args:
            lines: Lines to write
        """
        self._stream.writelines(lines)
        self._stream.flush()

    def flush(self) -> None:
        """Flush the underlying stream."""
        self._stream.flush()

    def __getattr__(self, name: str) -> object:
        """Delegate all other attributes to the underlying stream."""
        return getattr(self._stream, name)

    def __enter__(self) -> "AutoFlushTextIOWrapper":
        """Support context manager protocol."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Support context manager protocol."""
        if hasattr(self._stream, "__exit__"):
            self._stream.__exit__(exc_type, exc_val, exc_tb)


class AutoFlushBinaryIOWrapper:
    """Wrapper that automatically flushes a BinaryIO stream based on configuration.

    This wrapper can be configured to flush after each write operation,
    which is useful for real-time output monitoring in CI/CD systems.
    """

    def __init__(self, stream: BinaryIO) -> None:
        """Initialize the wrapper.

        Args:
            stream: The stream to wrap
        """
        self._stream = stream

    @classmethod
    def env(
        cls, stream: BinaryIO, env: Mapping[str, str] | None = None
    ) -> "AutoFlushBinaryIOWrapper | BinaryIO":
        """Create wrapper respecting the GIT_FLUSH environment variable.

        Respects the GIT_FLUSH environment variable:
        - GIT_FLUSH=1: Always flush after each write
        - GIT_FLUSH=0: Never auto-flush (use buffered I/O)
        - Not set: Auto-detect based on whether output is redirected

        Args:
            stream: The stream to wrap
            env: Environment variables dict (defaults to os.environ)

        Returns:
            AutoFlushBinaryIOWrapper instance configured based on GIT_FLUSH
        """
        if _should_auto_flush(stream, env):
            return cls(stream)
        else:
            return stream

    def write(self, data: Buffer) -> int:
        """Write data to the stream and optionally flush.

        Args:
            data: Data to write

        Returns:
            Number of bytes written
        """
        result = self._stream.write(data)
        self._stream.flush()
        return result

    def writelines(self, lines: Iterable[Buffer]) -> None:
        """Write multiple lines to the stream and optionally flush.

        Args:
            lines: Lines to write
        """
        self._stream.writelines(lines)
        self._stream.flush()

    def flush(self) -> None:
        """Flush the underlying stream."""
        self._stream.flush()

    def __getattr__(self, name: str) -> object:
        """Delegate all other attributes to the underlying stream."""
        return getattr(self._stream, name)

    def __enter__(self) -> "AutoFlushBinaryIOWrapper":
        """Support context manager protocol."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Support context manager protocol."""
        if hasattr(self._stream, "__exit__"):
            self._stream.__exit__(exc_type, exc_val, exc_tb)


class CommitMessageError(Exception):
    """Raised when there's an issue with the commit message."""


def signal_int(signal: int, frame: types.FrameType | None) -> None:
    """Handle interrupt signal by exiting.

    Args:
        signal: Signal number
        frame: Current stack frame
    """
    sys.exit(1)


def signal_quit(signal: int, frame: types.FrameType | None) -> None:
    """Handle quit signal by entering debugger.

    Args:
        signal: Signal number
        frame: Current stack frame
    """
    import pdb

    pdb.set_trace()


def parse_time_to_timestamp(time_spec: str) -> int:
    """Parse a time specification and return a Unix timestamp.

    Args:
        time_spec: Time specification. Can be:
            - A Unix timestamp (integer as string)
            - A relative time like "2 weeks ago"
            - "now" for current time
            - "all" to expire all entries (returns future time)
            - "never" to never expire (returns 0 - epoch start)

    Returns:
        Unix timestamp

    Raises:
        ValueError: If the time specification cannot be parsed
    """
    import time

    from .approxidate import parse_approxidate

    # Handle special cases specific to CLI
    if time_spec == "all":
        # Expire all entries - set to future time so everything is "older"
        return int(time.time()) + (100 * 365 * 24 * 60 * 60)  # 100 years in future
    if time_spec == "never":
        # Never expire - set to epoch start so nothing is older
        return 0

    # Use approxidate parser for everything else
    return parse_approxidate(time_spec)


def format_bytes(bytes: float) -> str:
    """Format bytes as human-readable string.

    Args:
        bytes: Number of bytes

    Returns:
        Human-readable string like "1.5 MB"
    """
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes < 1024.0:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.1f} TB"


def launch_editor(template_content: bytes = b"") -> bytes:
    """Launch an editor for the user to enter text.

    Args:
        template_content: Initial content for the editor

    Returns:
        The edited content as bytes
    """
    # Determine which editor to use
    editor = os.environ.get("GIT_EDITOR") or os.environ.get("EDITOR") or "vi"

    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f:
        temp_file = f.name
        f.write(template_content)

    try:
        # Launch the editor
        subprocess.run([editor, temp_file], check=True)

        # Read the edited content
        with open(temp_file, "rb") as f:
            content = f.read()

        return content
    finally:
        # Clean up the temporary file
        os.unlink(temp_file)


def detect_terminal_width() -> int:
    """Detect the width of the terminal.

    Returns:
        Width of the terminal in characters, or 80 if it cannot be determined
    """
    try:
        return os.get_terminal_size().columns
    except OSError:
        return 80


def write_columns(
    items: Iterator[bytes] | Sequence[bytes],
    out: TextIO,
    width: int | None = None,
) -> None:
    """Display items in formatted columns based on terminal width.

    Args:
        items: List or iterator of bytes objects to display in columns
        out: Output stream to write to
        width: Optional width of the terminal (if None, auto-detect)

    The function calculates the optimal number of columns to fit the terminal
    width and displays the items in a formatted column layout with proper
    padding and alignment.
    """
    if width is None:
        ter_width = detect_terminal_width()
    else:
        ter_width = width

    item_names = [item.decode() for item in items]

    def columns(
        names: Sequence[str], width: int, num_cols: int
    ) -> tuple[bool, list[int]]:
        if num_cols <= 0:
            return False, []

        num_rows = (len(names) + num_cols - 1) // num_cols
        col_widths = []

        for col in range(num_cols):
            max_width = 0
            for row in range(num_rows):
                idx = row + col * num_rows
                if idx < len(names):
                    max_width = max(max_width, len(names[idx]))
            col_widths.append(max_width + 2)  # add padding

        total_width = sum(col_widths)
        if total_width <= width:
            return True, col_widths
        return False, []

    best_cols = 1
    best_widths = []

    for num_cols in range(min(8, len(item_names)), 0, -1):
        fits, widths = columns(item_names, ter_width, num_cols)
        if fits:
            best_cols = num_cols
            best_widths = widths
            break

    if not best_widths:
        best_cols = 1
        best_widths = [max(len(name) for name in item_names) + 2]

    num_rows = (len(item_names) + best_cols - 1) // best_cols

    for row in range(num_rows):
        lines = []
        for col in range(best_cols):
            idx = row + col * num_rows
            if idx < len(item_names):
                branch_name = item_names[idx]
                if col < len(best_widths):
                    lines.append(branch_name.ljust(best_widths[col]))
                else:
                    lines.append(branch_name)

        if lines:
            out.write("".join(lines).rstrip() + "\n")


def format_columns(
    items: list[str],
    width: int | None = None,
    mode: str = "column",
    padding: int = 1,
    indent: str = "",
    nl: str = "\n",
) -> str:
    r"""Format items into columns with various layout modes.

    Args:
        items: List of strings to format
        width: Terminal width (auto-detected if None)
        mode: Layout mode - "column" (fill columns first), "row" (fill rows first),
              "plain" (one column), or add ",dense" for unequal column widths
        padding: Number of spaces between columns
        indent: String to prepend to each line
        nl: String to append to each line (including newline)

    Returns:
        Formatted string with items in columns

    Examples:
        >>> format_columns(["a", "b", "c"], width=20, mode="column")
        "a  b\\nc\\n"
        >>> format_columns(["a", "b", "c"], width=20, mode="row")
        "a  b  c\\n"
    """
    if not items:
        return ""

    if width is None:
        width = detect_terminal_width()

    # Parse mode
    mode_parts = mode.split(",")
    layout_mode = "column"
    dense = False

    for part in mode_parts:
        part = part.strip()
        if part in ("column", "row", "plain"):
            layout_mode = part
        elif part == "dense":
            dense = True
        elif part == "nodense":
            dense = False

    # Plain mode - one item per line
    if layout_mode == "plain":
        return "".join(indent + item + nl for item in items)

    # Calculate available width for content (excluding indent)
    available_width = width - len(indent)
    if available_width <= 0:
        available_width = width

    # Find optimal number of columns
    max_item_len = max(len(item) for item in items)

    # Start with maximum possible columns and work down
    best_num_cols = 1
    best_col_widths: list[int] = []

    for num_cols in range(min(len(items), 20), 0, -1):
        if layout_mode == "column":
            # Column mode: fill columns first (items go down, then across)
            num_rows = (len(items) + num_cols - 1) // num_cols
        else:  # row mode
            # Row mode: fill rows first (items go across, then down)
            num_rows = (len(items) + num_cols - 1) // num_cols

        col_widths: list[int] = []

        if dense:
            # Calculate width for each column based on its contents
            for col in range(num_cols):
                max_width = 0
                for row in range(num_rows):
                    if layout_mode == "column":
                        idx = row + col * num_rows
                    else:  # row mode
                        idx = row * num_cols + col

                    if idx < len(items):
                        max_width = max(max_width, len(items[idx]))

                if max_width > 0:
                    col_widths.append(max_width)
        else:
            # All columns same width (nodense)
            max_width = 0
            for col in range(num_cols):
                for row in range(num_rows):
                    if layout_mode == "column":
                        idx = row + col * num_rows
                    else:  # row mode
                        idx = row * num_cols + col

                    if idx < len(items):
                        max_width = max(max_width, len(items[idx]))

            col_widths = [max_width] * num_cols

        # Calculate total width including padding (but not after last column)
        total_width = sum(col_widths) + padding * (len(col_widths) - 1)

        if total_width <= available_width:
            best_num_cols = num_cols
            best_col_widths = col_widths
            break

    # If no fit found, use single column
    if not best_col_widths:
        best_num_cols = 1
        best_col_widths = [max_item_len]

    # Format output
    num_rows = (len(items) + best_num_cols - 1) // best_num_cols
    lines = []

    for row in range(num_rows):
        line_parts = []
        for col in range(best_num_cols):
            if layout_mode == "column":
                idx = row + col * num_rows
            else:  # row mode
                idx = row * best_num_cols + col

            if idx < len(items):
                item = items[idx]
                # Pad item to column width, except for last column in row
                if col < best_num_cols - 1 and col < len(best_col_widths) - 1:
                    item = item.ljust(best_col_widths[col] + padding)
                line_parts.append(item)

        if line_parts:
            lines.append(indent + "".join(line_parts).rstrip() + nl)

    return "".join(lines)


class PagerBuffer(BinaryIO):
    """Binary buffer wrapper for Pager to mimic sys.stdout.buffer."""

    def __init__(self, pager: "Pager") -> None:
        """Initialize PagerBuffer.

        Args:
            pager: Pager instance to wrap
        """
        self.pager = pager

    def write(self, data: bytes | bytearray | memoryview) -> int:  # type: ignore[override]
        """Write bytes to pager."""
        # Convert to bytes and decode to string for the pager
        text = bytes(data).decode("utf-8", errors="replace")
        return self.pager.write(text)

    def flush(self) -> None:
        """Flush the pager."""
        return self.pager.flush()

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None:  # type: ignore[override]
        """Write multiple lines to pager."""
        for line in lines:
            self.write(line)

    def readable(self) -> bool:
        """Return whether the buffer is readable (it's not)."""
        return False

    def writable(self) -> bool:
        """Return whether the buffer is writable."""
        return not self.pager._closed

    def seekable(self) -> bool:
        """Return whether the buffer is seekable (it's not)."""
        return False

    def close(self) -> None:
        """Close the pager."""
        return self.pager.close()

    @property
    def closed(self) -> bool:
        """Return whether the buffer is closed."""
        return self.pager.closed

    @property
    def mode(self) -> str:
        """Return the mode."""
        return "wb"

    @property
    def name(self) -> str:
        """Return the name."""
        return "<pager.buffer>"

    def fileno(self) -> int:
        """Return the file descriptor (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support fileno()")

    def isatty(self) -> bool:
        """Return whether the buffer is a TTY."""
        return False

    def read(self, size: int = -1) -> bytes:
        """Read from the buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support reading")

    def read1(self, size: int = -1) -> bytes:
        """Read from the buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support reading")

    def readinto(self, b: bytearray) -> int:
        """Read into buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support reading")

    def readinto1(self, b: bytearray) -> int:
        """Read into buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support reading")

    def readline(self, size: int = -1) -> bytes:
        """Read a line from the buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support reading")

    def readlines(self, hint: int = -1) -> list[bytes]:
        """Read lines from the buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support reading")

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek in the buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support seeking")

    def tell(self) -> int:
        """Return the current position (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support tell()")

    def truncate(self, size: int | None = None) -> int:
        """Truncate the buffer (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support truncation")

    def __iter__(self) -> "PagerBuffer":
        """Return iterator (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support iteration")

    def __next__(self) -> bytes:
        """Return next line (not supported)."""
        raise io.UnsupportedOperation("PagerBuffer does not support iteration")

    def __enter__(self) -> "PagerBuffer":
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager."""
        self.close()


class Pager(TextIO):
    """File-like object that pages output through external pager programs."""

    def __init__(self, pager_cmd: str = "cat") -> None:
        """Initialize Pager.

        Args:
            pager_cmd: Command to use for paging (default: "cat")
        """
        self.pager_process: subprocess.Popen[str] | None = None
        self._buffer = PagerBuffer(self)
        self._closed = False
        self.pager_cmd = pager_cmd
        self._pager_died = False

    def _get_pager_command(self) -> str:
        """Get the pager command to use."""
        return self.pager_cmd

    def _ensure_pager_started(self) -> None:
        """Start the pager process if not already started."""
        if self.pager_process is None and not self._closed:
            try:
                pager_cmd = self._get_pager_command()
                self.pager_process = subprocess.Popen(
                    pager_cmd,
                    shell=True,
                    stdin=subprocess.PIPE,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                    text=True,
                )
            except (OSError, subprocess.SubprocessError):
                # Pager failed to start, fall back to direct output
                self.pager_process = None

    def write(self, text: str) -> int:
        """Write text to the pager."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        # If pager died (user quit), stop writing output
        if self._pager_died:
            return len(text)

        self._ensure_pager_started()

        if self.pager_process and self.pager_process.stdin:
            try:
                result = self.pager_process.stdin.write(text)
                assert isinstance(result, int)
                return result
            except (OSError, subprocess.SubprocessError, BrokenPipeError):
                # Pager died (user quit), stop writing output
                self._pager_died = True
                return len(text)
        else:
            # No pager available, write directly to stdout
            return sys.stdout.write(text)

    def flush(self) -> None:
        """Flush the pager."""
        if self._closed or self._pager_died:
            return

        if self.pager_process and self.pager_process.stdin:
            try:
                self.pager_process.stdin.flush()
            except (OSError, subprocess.SubprocessError, BrokenPipeError):
                self._pager_died = True
        else:
            sys.stdout.flush()

    def close(self) -> None:
        """Close the pager."""
        if self._closed:
            return

        self._closed = True
        if self.pager_process:
            try:
                if self.pager_process.stdin:
                    self.pager_process.stdin.close()
                self.pager_process.wait()
            except (OSError, subprocess.SubprocessError):
                pass
            self.pager_process = None

    def __enter__(self) -> "Pager":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Context manager exit."""
        self.close()

    # Additional file-like methods for compatibility
    def writelines(self, lines: Iterable[str]) -> None:
        """Write a list of lines to the pager."""
        if self._pager_died:
            return
        for line in lines:
            self.write(line)

    @property
    def closed(self) -> bool:
        """Return whether the pager is closed."""
        return self._closed

    def readable(self) -> bool:
        """Return whether the pager is readable (it's not)."""
        return False

    def writable(self) -> bool:
        """Return whether the pager is writable."""
        return not self._closed

    def seekable(self) -> bool:
        """Return whether the pager is seekable (it's not)."""
        return False

    @property
    def buffer(self) -> BinaryIO:
        """Return the underlying binary buffer."""
        return self._buffer

    @property
    def encoding(self) -> str:
        """Return the encoding used."""
        return "utf-8"

    @property
    def errors(self) -> str | None:
        """Return the error handling scheme."""
        return "replace"

    def fileno(self) -> int:
        """Return the file descriptor (not supported)."""
        raise io.UnsupportedOperation("Pager does not support fileno()")

    def isatty(self) -> bool:
        """Return whether the pager is a TTY."""
        return False

    @property
    def line_buffering(self) -> bool:
        """Return whether line buffering is enabled."""
        return True

    @property
    def mode(self) -> str:
        """Return the mode."""
        return "w"

    @property
    def name(self) -> str:
        """Return the name."""
        return "<pager>"

    @property
    def newlines(self) -> str | tuple[str, ...] | None:
        """Return the newlines mode."""
        return None

    def read(self, size: int = -1) -> str:
        """Read from the pager (not supported)."""
        raise io.UnsupportedOperation("Pager does not support reading")

    def readline(self, size: int = -1) -> str:
        """Read a line from the pager (not supported)."""
        raise io.UnsupportedOperation("Pager does not support reading")

    def readlines(self, hint: int = -1) -> list[str]:
        """Read lines from the pager (not supported)."""
        raise io.UnsupportedOperation("Pager does not support reading")

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek in the pager (not supported)."""
        raise io.UnsupportedOperation("Pager does not support seeking")

    def tell(self) -> int:
        """Return the current position (not supported)."""
        raise io.UnsupportedOperation("Pager does not support tell()")

    def truncate(self, size: int | None = None) -> int:
        """Truncate the pager (not supported)."""
        raise io.UnsupportedOperation("Pager does not support truncation")

    def __iter__(self) -> "Pager":
        """Return iterator (not supported)."""
        raise io.UnsupportedOperation("Pager does not support iteration")

    def __next__(self) -> str:
        """Return next line (not supported)."""
        raise io.UnsupportedOperation("Pager does not support iteration")


class _StreamContextAdapter:
    """Adapter to make streams work with context manager protocol."""

    def __init__(self, stream: TextIO | BinaryIO) -> None:
        self.stream = stream
        # Expose buffer if it exists
        if hasattr(stream, "buffer"):
            self.buffer = stream.buffer
        else:
            self.buffer = stream

    def __enter__(self) -> TextIO:
        # We only use this with sys.stdout which is TextIO
        return self.stream  # type: ignore[return-value]

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # For stdout/stderr, we don't close them
        pass

    def __getattr__(self, name: str) -> object:
        return getattr(self.stream, name)


def get_pager(
    config: Config | None = None, cmd_name: str | None = None
) -> "_StreamContextAdapter | Pager":
    """Get a pager instance if paging should be used, otherwise return sys.stdout.

    Args:
        config: Optional config instance (e.g., StackedConfig) to read settings from
        cmd_name: Optional command name for per-command pager settings

    Returns:
        Either a wrapped sys.stdout or a Pager instance (both context managers)
    """
    # Check global pager disable flag
    if getattr(get_pager, "_disabled", False):
        return _StreamContextAdapter(sys.stdout)

    # Don't page if stdout is not a terminal
    if not sys.stdout.isatty():
        return _StreamContextAdapter(sys.stdout)

    # Priority order for pager command (following git's behavior):
    # 1. Check pager.<cmd> config (if cmd_name provided)
    # 2. Check environment variables: DULWICH_PAGER, GIT_PAGER, PAGER
    # 3. Check core.pager config
    # 4. Fallback to common pagers

    pager_cmd = None

    # 1. Check per-command pager config (pager.<cmd>)
    if config and cmd_name:
        try:
            pager_value = config.get(
                ("pager",), cmd_name.encode() if isinstance(cmd_name, str) else cmd_name
            )
        except KeyError:
            pass
        else:
            if pager_value == b"false":
                return _StreamContextAdapter(sys.stdout)
            elif pager_value != b"true":
                # It's a custom pager command
                pager_cmd = (
                    pager_value.decode()
                    if isinstance(pager_value, bytes)
                    else pager_value
                )

    # 2. Check environment variables
    if not pager_cmd:
        for env_var in ["DULWICH_PAGER", "GIT_PAGER", "PAGER"]:
            pager = os.environ.get(env_var)
            if pager:
                if pager == "false":
                    return _StreamContextAdapter(sys.stdout)
                pager_cmd = pager
                break

    # 3. Check core.pager config
    if not pager_cmd and config:
        try:
            core_pager = config.get(("core",), b"pager")
        except KeyError:
            pass
        else:
            if core_pager == b"false" or core_pager == b"":
                return _StreamContextAdapter(sys.stdout)
            pager_cmd = (
                core_pager.decode() if isinstance(core_pager, bytes) else core_pager
            )

    # 4. Fallback to common pagers
    if not pager_cmd:
        for pager in ["less", "more", "cat"]:
            if shutil.which(pager):
                if pager == "less":
                    pager_cmd = "less -FRX"  # -F: quit if one screen, -R: raw control chars, -X: no init/deinit
                else:
                    pager_cmd = pager
                break
        else:
            pager_cmd = "cat"  # Ultimate fallback

    return Pager(pager_cmd)


def disable_pager() -> None:
    """Disable pager for this session."""
    get_pager._disabled = True  # type: ignore[attr-defined]


def enable_pager() -> None:
    """Enable pager for this session."""
    get_pager._disabled = False  # type: ignore[attr-defined]


class Command:
    """A Dulwich subcommand."""

    def run(self, args: Sequence[str]) -> int | None:
        """Run the command."""
        raise NotImplementedError(self.run)


class cmd_archive(Command):
    """Create an archive of files from a named tree."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the archive command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--remote",
            type=str,
            help="Retrieve archive from specified remote repo",
        )
        parser.add_argument("committish", type=str, nargs="?")
        parsed_args = parser.parse_args(args)
        if parsed_args.remote:
            client, path = get_transport_and_path(parsed_args.remote)

            def stdout_write(data: bytes) -> None:
                sys.stdout.buffer.write(data)

            def stderr_write(data: bytes) -> None:
                sys.stderr.buffer.write(data)

            client.archive(
                path.encode("utf-8") if isinstance(path, str) else path,
                parsed_args.committish.encode("utf-8")
                if isinstance(parsed_args.committish, str)
                else parsed_args.committish,
                stdout_write,
                write_error=stderr_write,
            )
        else:
            # Use binary buffer for archive output
            outstream: BinaryIO = sys.stdout.buffer
            errstream: BinaryIO = sys.stderr.buffer
            porcelain.archive(
                ".",
                parsed_args.committish,
                outstream=outstream,
                errstream=errstream,
            )


class cmd_add(Command):
    """Add file contents to the index."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the add command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("path", nargs="+")
        args = parser.parse_args(argv)

        # Convert '.' to None to add all files
        paths = args.path
        if len(paths) == 1 and paths[0] == ".":
            paths = None

        porcelain.add(".", paths=paths)


class cmd_annotate(Command):
    """Annotate each line in a file with commit information."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the annotate command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("path", help="Path to file to annotate")
        parser.add_argument("committish", nargs="?", help="Commit to start from")
        args = parser.parse_args(argv)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="annotate") as outstream:
                results = porcelain.annotate(repo, args.path, args.committish)
                for (commit, entry), line in results:
                    # Show shortened commit hash and line content
                    commit_hash = commit.id[:8]
                    outstream.write(f"{commit_hash.decode()} {line.decode()}\n")


class cmd_blame(Command):
    """Show what revision and author last modified each line of a file."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the blame command.

        Args:
            argv: Command line arguments
        """
        # blame is an alias for annotate
        cmd_annotate().run(argv)


class cmd_rm(Command):
    """Remove files from the working tree and from the index."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the rm command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--cached", action="store_true", help="Remove from index only"
        )
        parser.add_argument("path", type=Path, nargs="+")
        args = parser.parse_args(argv)

        porcelain.remove(".", paths=args.path, cached=args.cached)


class cmd_mv(Command):
    """Move or rename a file, a directory, or a symlink."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the mv command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force move even if destination exists",
        )
        parser.add_argument("source", type=Path)
        parser.add_argument("destination", type=Path)
        args = parser.parse_args(argv)

        porcelain.mv(".", args.source, args.destination, force=args.force)


class cmd_fetch_pack(Command):
    """Receive missing objects from another repository."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the fetch-pack command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--all", action="store_true")
        parser.add_argument("location", nargs="?", type=str)
        parser.add_argument("refs", nargs="*", type=str)
        args = parser.parse_args(argv)
        client, path = get_transport_and_path(args.location)
        r = Repo(".")
        if args.all:
            determine_wants = r.object_store.determine_wants_all
        else:

            def determine_wants(
                refs: Mapping[Ref, ObjectID], depth: int | None = None
            ) -> list[ObjectID]:
                return [
                    ObjectID(y.encode("utf-8"))
                    for y in args.refs
                    if y not in r.object_store
                ]

        client.fetch(path.encode("utf-8"), r, determine_wants)


class cmd_fetch(Command):
    """Download objects and refs from another repository."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the fetch command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()

        # Mutually exclusive group for location vs --all
        location_group = parser.add_mutually_exclusive_group(required=True)
        location_group.add_argument(
            "location", nargs="?", default=None, help="Remote location to fetch from"
        )
        location_group.add_argument(
            "--all", action="store_true", help="Fetch all remotes"
        )

        # Mutually exclusive group for tag handling
        tag_group = parser.add_mutually_exclusive_group()
        tag_group.add_argument(
            "--tags", action="store_true", help="Fetch all tags from remote"
        )
        tag_group.add_argument(
            "--no-tags", action="store_true", help="Don't fetch any tags from remote"
        )

        parser.add_argument(
            "--depth",
            type=int,
            help="Create a shallow clone with a history truncated to the specified number of commits",
        )
        parser.add_argument(
            "--shallow-since",
            type=str,
            help="Deepen or shorten the history of a shallow repository to include all reachable commits after <date>",
        )
        parser.add_argument(
            "--shallow-exclude",
            type=str,
            action="append",
            help="Deepen or shorten the history of a shallow repository to exclude commits reachable from a specified remote branch or tag",
        )
        parsed_args = parser.parse_args(args)

        r = Repo(".")

        def progress(msg: bytes) -> None:
            sys.stdout.buffer.write(msg)

        # Determine include_tags setting
        include_tags = False
        if parsed_args.tags:
            include_tags = True
        elif not parsed_args.no_tags:
            # Default behavior - don't force tag inclusion
            include_tags = False

        if parsed_args.all:
            # Fetch from all remotes
            config = r.get_config()
            remotes = set()
            for section in config.sections():
                if len(section) == 2 and section[0] == b"remote":
                    remotes.add(section[1].decode())

            if not remotes:
                logger.warning("No remotes configured")
                return

            for remote_name in sorted(remotes):
                logger.info("Fetching %s", remote_name)
                porcelain.fetch(
                    r,
                    remote_location=remote_name,
                    depth=parsed_args.depth,
                    include_tags=include_tags,
                    shallow_since=parsed_args.shallow_since,
                    shallow_exclude=parsed_args.shallow_exclude,
                )
        else:
            # Fetch from specific location
            porcelain.fetch(
                r,
                remote_location=parsed_args.location,
                depth=parsed_args.depth,
                include_tags=include_tags,
                shallow_since=parsed_args.shallow_since,
                shallow_exclude=parsed_args.shallow_exclude,
            )


class cmd_for_each_ref(Command):
    """Output information on each ref."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the for-each-ref command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("pattern", type=str, nargs="?")
        parsed_args = parser.parse_args(args)
        for sha, object_type, ref in porcelain.for_each_ref(".", parsed_args.pattern):
            logger.info("%s %s\t%s", sha.decode(), object_type.decode(), ref.decode())


class cmd_fsck(Command):
    """Verify the connectivity and validity of objects in the database."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the fsck command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        for obj, msg in porcelain.fsck("."):
            logger.info("%s: %s", obj.decode() if isinstance(obj, bytes) else obj, msg)


class cmd_log(Command):
    """Show commit logs."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the log command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--reverse",
            action="store_true",
            help="Reverse order in which entries are printed",
        )
        parser.add_argument(
            "--name-status",
            action="store_true",
            help="Print name/status for each changed file",
        )
        parser.add_argument("paths", nargs="*", help="Paths to show log for")
        parsed_args = parser.parse_args(args)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="log") as outstream:
                porcelain.log(
                    repo,
                    paths=parsed_args.paths,
                    reverse=parsed_args.reverse,
                    name_status=parsed_args.name_status,
                    outstream=outstream,
                )


class cmd_diff(Command):
    """Show changes between commits, commit and working tree, etc."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the diff command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "committish", nargs="*", default=[], help="Commits or refs to compare"
        )
        parser.add_argument("--staged", action="store_true", help="Show staged changes")
        parser.add_argument(
            "--cached",
            action="store_true",
            help="Show staged changes (same as --staged)",
        )
        parser.add_argument(
            "--color",
            choices=["always", "never", "auto"],
            default="auto",
            help="Use colored output (requires rich)",
        )
        parser.add_argument(
            "--patience",
            action="store_true",
            help="Use patience diff algorithm",
        )
        parser.add_argument(
            "--diff-algorithm",
            choices=["myers", "patience"],
            default="myers",
            help="Choose a diff algorithm",
        )
        parser.add_argument(
            "--stat",
            action="store_true",
            help="Show diffstat instead of full diff",
        )
        parser.add_argument(
            "--", dest="separator", action="store_true", help=argparse.SUPPRESS
        )
        parser.add_argument("paths", nargs="*", default=[], help="Paths to limit diff")

        # Handle the -- separator for paths
        if "--" in args:
            sep_index = args.index("--")
            parsed_args = parser.parse_args(args[:sep_index])
            parsed_args.paths = args[sep_index + 1 :]
        else:
            parsed_args = parser.parse_args(args)

        # Determine diff algorithm
        diff_algorithm = parsed_args.diff_algorithm
        if parsed_args.patience:
            diff_algorithm = "patience"

        # Determine if we should use color
        def _should_use_color() -> bool:
            if parsed_args.color == "always":
                return True
            elif parsed_args.color == "never":
                return False
            else:  # auto
                return sys.stdout.isatty()

        def _create_output_stream(outstream: TextIO) -> BinaryIO:
            """Create output stream, optionally with colorization."""
            if not _should_use_color():
                return outstream.buffer

            from .diff import ColorizedDiffStream

            if not ColorizedDiffStream.is_available():
                if parsed_args.color == "always":
                    raise ImportError(
                        "Rich is required for colored output. Install with: pip install 'dulwich[colordiff]'"
                    )
                else:
                    logging.warning(
                        "Rich not available, disabling colored output. Install with: pip install 'dulwich[colordiff]'"
                    )
                    return outstream.buffer

            return ColorizedDiffStream(outstream.buffer)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="diff") as outstream:
                # For --stat mode, capture the diff in a BytesIO buffer
                if parsed_args.stat:
                    import io

                    from .diffstat import diffstat

                    diff_buffer: BinaryIO = io.BytesIO()
                    output_stream: BinaryIO = diff_buffer
                else:
                    output_stream = _create_output_stream(outstream)

                try:
                    if len(parsed_args.committish) == 0:
                        # Show diff for working tree or staged changes
                        porcelain.diff(
                            repo,
                            staged=(parsed_args.staged or parsed_args.cached),
                            paths=parsed_args.paths or None,
                            outstream=output_stream,
                            diff_algorithm=diff_algorithm,
                        )
                    elif len(parsed_args.committish) == 1:
                        # Show diff between working tree and specified commit
                        if parsed_args.staged or parsed_args.cached:
                            parser.error(
                                "--staged/--cached cannot be used with commits"
                            )
                        porcelain.diff(
                            repo,
                            commit=parsed_args.committish[0],
                            staged=False,
                            paths=parsed_args.paths or None,
                            outstream=output_stream,
                            diff_algorithm=diff_algorithm,
                        )
                    elif len(parsed_args.committish) == 2:
                        # Show diff between two commits
                        porcelain.diff(
                            repo,
                            commit=parsed_args.committish[0],
                            commit2=parsed_args.committish[1],
                            paths=parsed_args.paths or None,
                            outstream=output_stream,
                            diff_algorithm=diff_algorithm,
                        )
                    else:
                        parser.error("Too many arguments - specify at most two commits")
                except DiffAlgorithmNotAvailable as e:
                    sys.stderr.write(f"fatal: {e}\n")
                    sys.exit(1)

                if parsed_args.stat:
                    # Generate and output diffstat from captured diff
                    assert isinstance(diff_buffer, io.BytesIO)
                    diff_data = diff_buffer.getvalue()
                    lines = diff_data.split(b"\n")
                    stat_output = diffstat(lines)
                    outstream.buffer.write(stat_output + b"\n")
                else:
                    # Flush any remaining output
                    if hasattr(output_stream, "flush"):
                        output_stream.flush()


class cmd_dump_pack(Command):
    """Dump the contents of a pack file for debugging."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the dump-pack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("filename", help="Pack file to dump")
        parser.add_argument(
            "--object-format",
            choices=["sha1", "sha256"],
            default="sha1",
            help="Object format (hash algorithm) used in the pack file",
        )
        parsed_args = parser.parse_args(args)

        from .object_format import OBJECT_FORMATS

        object_format = OBJECT_FORMATS[parsed_args.object_format]
        basename, _ = os.path.splitext(parsed_args.filename)
        x = Pack(basename, object_format=object_format)
        logger.info("Object names checksum: %s", x.name().decode("ascii", "replace"))
        logger.info("Checksum: %r", sha_to_hex(RawObjectID(x.get_stored_checksum())))
        x.check()
        logger.info("Length: %d", len(x))
        for name in x:
            try:
                logger.info("\t%s", x[name])
            except KeyError as k:
                logger.error(
                    "\t%s: Unable to resolve base %r",
                    name.decode("ascii", "replace"),
                    k,
                )
            except ApplyDeltaError as e:
                logger.error(
                    "\t%s: Unable to apply delta: %r",
                    name.decode("ascii", "replace"),
                    e,
                )


class cmd_dump_index(Command):
    """Show information about a pack index file."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the dump-index command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("filename", help="Index file to dump")
        parsed_args = parser.parse_args(args)

        idx = Index(parsed_args.filename)

        for o in idx:
            logger.info("%s %s", o, idx[o])


class cmd_interpret_trailers(Command):
    """Add or parse structured information in commit messages."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the interpret-trailers command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "file",
            nargs="?",
            help="File to read message from. If not specified, reads from stdin.",
        )
        parser.add_argument(
            "--trailer",
            action="append",
            dest="trailers",
            metavar="<token>[(=|:)<value>]",
            help="Trailer to add. Can be specified multiple times.",
        )
        parser.add_argument(
            "--trim-empty",
            action="store_true",
            help="Remove trailers with empty values",
        )
        parser.add_argument(
            "--only-trailers",
            action="store_true",
            help="Output only the trailers, not the message body",
        )
        parser.add_argument(
            "--only-input",
            action="store_true",
            help="Don't add new trailers, only parse existing ones",
        )
        parser.add_argument(
            "--unfold", action="store_true", help="Join multiline values into one line"
        )
        parser.add_argument(
            "--parse",
            action="store_true",
            help="Shorthand for --only-trailers --only-input --unfold",
        )
        parser.add_argument(
            "--where",
            choices=["end", "start", "after", "before"],
            default="end",
            help="Where to place new trailers",
        )
        parser.add_argument(
            "--if-exists",
            choices=[
                "add",
                "replace",
                "addIfDifferent",
                "addIfDifferentNeighbor",
                "doNothing",
            ],
            default="addIfDifferentNeighbor",
            help="Action if trailer already exists",
        )
        parser.add_argument(
            "--if-missing",
            choices=["add", "doNothing"],
            default="add",
            help="Action if trailer is missing",
        )
        parsed_args = parser.parse_args(args)

        # Read message from file or stdin
        if parsed_args.file:
            with open(parsed_args.file, "rb") as f:
                message = f.read()
        else:
            message = sys.stdin.buffer.read()

        # Parse trailer arguments
        trailer_list = []
        if parsed_args.trailers:
            for trailer_spec in parsed_args.trailers:
                # Parse "key:value" or "key=value" or just "key"
                if ":" in trailer_spec:
                    key, value = trailer_spec.split(":", 1)
                elif "=" in trailer_spec:
                    key, value = trailer_spec.split("=", 1)
                else:
                    key = trailer_spec
                    value = ""
                trailer_list.append((key.strip(), value.strip()))

        # Call interpret_trailers
        result = porcelain.interpret_trailers(
            message,
            trailers=trailer_list if trailer_list else None,
            trim_empty=parsed_args.trim_empty,
            only_trailers=parsed_args.only_trailers,
            only_input=parsed_args.only_input,
            unfold=parsed_args.unfold,
            parse=parsed_args.parse,
            where=parsed_args.where,
            if_exists=parsed_args.if_exists,
            if_missing=parsed_args.if_missing,
        )

        # Output result
        sys.stdout.buffer.write(result)


class cmd_stripspace(Command):
    """Remove unnecessary whitespace from text."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the stripspace command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "file",
            nargs="?",
            help="File to read text from. If not specified, reads from stdin.",
        )
        parser.add_argument(
            "-s",
            "--strip-comments",
            action="store_true",
            help="Strip lines that begin with comment character",
        )
        parser.add_argument(
            "-c",
            "--comment-lines",
            action="store_true",
            help="Prepend comment character to each line",
        )
        parser.add_argument(
            "--comment-char",
            default="#",
            help="Comment character to use (default: #)",
        )
        parsed_args = parser.parse_args(args)

        # Read text from file or stdin
        if parsed_args.file:
            with open(parsed_args.file, "rb") as f:
                text = f.read()
        else:
            text = sys.stdin.buffer.read()

        # Call stripspace
        result = porcelain.stripspace(
            text,
            strip_comments=parsed_args.strip_comments,
            comment_char=parsed_args.comment_char,
            comment_lines=parsed_args.comment_lines,
        )

        # Output result
        sys.stdout.buffer.write(result)


class cmd_column(Command):
    """Display data in columns."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the column command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Format input data into columns for better readability"
        )
        parser.add_argument(
            "--mode",
            default="column",
            help=(
                "Layout mode: 'column' (fill columns first), 'row' (fill rows first), "
                "'plain' (one column). Add ',dense' for unequal column widths, "
                "',nodense' for equal widths (default: column)"
            ),
        )
        parser.add_argument(
            "--width",
            type=int,
            help="Terminal width (default: auto-detect)",
        )
        parser.add_argument(
            "--indent",
            default="",
            help="String to prepend to each line (default: empty)",
        )
        parser.add_argument(
            "--nl",
            default="\n",
            help="String to append to each line, including newline (default: \\n)",
        )
        parser.add_argument(
            "--padding",
            type=int,
            default=1,
            help="Number of spaces between columns (default: 1)",
        )
        parsed_args = parser.parse_args(args)

        # Read lines from stdin
        lines = []
        for line in sys.stdin:
            # Strip the newline but keep the content
            lines.append(line.rstrip("\n\r"))

        # Format and output
        result = format_columns(
            lines,
            width=parsed_args.width,
            mode=parsed_args.mode,
            padding=parsed_args.padding,
            indent=parsed_args.indent,
            nl=parsed_args.nl,
        )

        sys.stdout.write(result)


class cmd_init(Command):
    """Create an empty Git repository or reinitialize an existing one."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the init command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--bare", action="store_true", help="Create a bare repository"
        )
        parser.add_argument(
            "--objectformat",
            type=str,
            choices=["sha1", "sha256"],
            help="Object format to use (sha1 or sha256)",
        )
        parser.add_argument(
            "path", nargs="?", default=os.getcwd(), help="Repository path"
        )
        parsed_args = parser.parse_args(args)

        porcelain.init(
            parsed_args.path,
            bare=parsed_args.bare,
            object_format=parsed_args.objectformat,
        )


class cmd_clone(Command):
    """Clone a repository into a new directory."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the clone command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--bare",
            help="Whether to create a bare repository.",
            action="store_true",
        )
        parser.add_argument("--depth", type=int, help="Depth at which to fetch")
        parser.add_argument(
            "-b",
            "--branch",
            type=str,
            help="Check out branch instead of branch pointed to by remote HEAD",
        )
        parser.add_argument(
            "--refspec",
            type=str,
            help="References to fetch",
            action="append",
        )
        parser.add_argument(
            "--filter",
            dest="filter_spec",
            type=str,
            help="git-rev-list-style object filter",
        )
        parser.add_argument(
            "--protocol",
            type=int,
            help="Git protocol version to use",
        )
        parser.add_argument(
            "--recurse-submodules",
            action="store_true",
            help="Initialize and clone submodules",
        )
        parser.add_argument("source", help="Repository to clone from")
        parser.add_argument("target", nargs="?", help="Directory to clone into")
        parsed_args = parser.parse_args(args)

        try:
            porcelain.clone(
                parsed_args.source,
                parsed_args.target,
                bare=parsed_args.bare,
                depth=parsed_args.depth,
                branch=parsed_args.branch,
                refspec=parsed_args.refspec,
                filter_spec=parsed_args.filter_spec,
                protocol_version=parsed_args.protocol,
                recurse_submodules=parsed_args.recurse_submodules,
            )
        except GitProtocolError as e:
            logging.exception(e)


def _get_commit_message_with_template(
    initial_message: bytes | None,
    repo: Repo | None = None,
    commit: Commit | None = None,
) -> bytes:
    """Get commit message with an initial message template."""
    # Start with the initial message
    template = initial_message or b""
    if template and not template.endswith(b"\n"):
        template += b"\n"

    template += b"\n"
    template += b"# Please enter the commit message for your changes. Lines starting\n"
    template += b"# with '#' will be ignored, and an empty message aborts the commit.\n"
    template += b"#\n"

    # Add branch info if repo is provided
    if repo:
        try:
            ref_names, _ref_sha = repo.refs.follow(HEADREF)
            ref_path = ref_names[-1]  # Get the final reference
            if ref_path.startswith(b"refs/heads/"):
                branch = ref_path[11:]  # Remove 'refs/heads/' prefix
            else:
                branch = ref_path
            template += b"# On branch %s\n" % branch
        except (KeyError, IndexError):
            template += b"# On branch (unknown)\n"
        template += b"#\n"

    template += b"# Changes to be committed:\n"

    # Launch editor
    content = launch_editor(template)

    # Remove comment lines and strip
    lines = content.split(b"\n")
    message_lines = [line for line in lines if not line.strip().startswith(b"#")]
    message = b"\n".join(message_lines).strip()

    if not message:
        raise CommitMessageError("Aborting commit due to empty commit message")

    return message


class cmd_config(Command):
    """Get and set repository or global options."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the config command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--global",
            dest="global_config",
            action="store_true",
            help="Use global config file",
        )
        parser.add_argument(
            "--local",
            action="store_true",
            help="Use repository config file (default)",
        )
        parser.add_argument(
            "-l",
            "--list",
            action="store_true",
            help="List all variables",
        )
        parser.add_argument(
            "--unset",
            action="store_true",
            help="Remove a variable",
        )
        parser.add_argument(
            "--unset-all",
            action="store_true",
            help="Remove all matches for a variable",
        )
        parser.add_argument(
            "--get-all",
            action="store_true",
            help="Get all values for a multivar",
        )
        parser.add_argument(
            "key",
            nargs="?",
            help="Config key (e.g., user.name)",
        )
        parser.add_argument(
            "value",
            nargs="?",
            help="Config value to set",
        )
        parsed_args = parser.parse_args(args)

        # Determine which config file to use
        if parsed_args.global_config:
            # Use global config file
            config_path = os.path.expanduser("~/.gitconfig")
            try:
                from .config import ConfigFile

                config = ConfigFile.from_path(config_path)
            except FileNotFoundError:
                from .config import ConfigFile

                config = ConfigFile()
                config.path = config_path
        else:
            # Use local repository config (default)
            try:
                repo = Repo(".")
                config = repo.get_config()
            except NotGitRepository:
                logger.error("error: not a git repository")
                return 1

        # Handle --list
        if parsed_args.list:
            for section in config.sections():
                for key, value in config.items(section):
                    section_str = ".".join(
                        s.decode("utf-8") if isinstance(s, bytes) else s
                        for s in section
                    )
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                    value_str = (
                        value.decode("utf-8") if isinstance(value, bytes) else value
                    )
                    print(f"{section_str}.{key_str}={value_str}")
            return 0

        # Handle --unset or --unset-all
        if parsed_args.unset or parsed_args.unset_all:
            if not parsed_args.key:
                logger.error("error: key is required for --unset")
                return 1

            # Parse the key (e.g., "user.name" or "remote.origin.url")
            parts = parsed_args.key.split(".")
            if len(parts) < 2:
                logger.error("error: invalid key format")
                return 1

            if len(parts) == 2:
                section = (parts[0],)
                name = parts[1]
            else:
                # For keys like "remote.origin.url", section is ("remote", "origin")
                section = tuple(parts[:-1])
                name = parts[-1]

            try:
                # Check if the key exists first
                try:
                    config.get(section, name)
                except KeyError:
                    logger.error(f"error: key '{parsed_args.key}' not found")
                    return 1

                # Delete the configuration key using ConfigDict's delete method
                section_bytes = tuple(
                    s.encode("utf-8") if isinstance(s, str) else s for s in section
                )
                name_bytes = name.encode("utf-8") if isinstance(name, str) else name

                section_dict = config._values.get(section_bytes)
                if section_dict:
                    del section_dict[name_bytes]
                    config.write_to_path()
                else:
                    logger.error(f"error: key '{parsed_args.key}' not found")
                    return 1
            except Exception as e:
                logger.error(f"error: {e}")
                return 1

            return 0

        # Handle --get-all
        if parsed_args.get_all:
            if not parsed_args.key:
                logger.error("error: key is required for --get-all")
                return 1

            parts = parsed_args.key.split(".")
            if len(parts) < 2:
                logger.error("error: invalid key format")
                return 1

            if len(parts) == 2:
                section = (parts[0],)
                name = parts[1]
            else:
                section = tuple(parts[:-1])
                name = parts[-1]

            try:
                for value in config.get_multivar(section, name):
                    value_str = (
                        value.decode("utf-8") if isinstance(value, bytes) else value
                    )
                    print(value_str)
                return 0
            except KeyError:
                return 1

        # Handle get (no value provided)
        if parsed_args.key and not parsed_args.value:
            parts = parsed_args.key.split(".")
            if len(parts) < 2:
                logger.error("error: invalid key format")
                return 1

            if len(parts) == 2:
                section = (parts[0],)
                name = parts[1]
            else:
                # For keys like "remote.origin.url", section is ("remote", "origin")
                section = tuple(parts[:-1])
                name = parts[-1]

            try:
                value = config.get(section, name)
                value_str = value.decode("utf-8") if isinstance(value, bytes) else value
                print(value_str)
                return 0
            except KeyError:
                return 1

        # Handle set (key and value provided)
        if parsed_args.key and parsed_args.value:
            parts = parsed_args.key.split(".")
            if len(parts) < 2:
                logger.error("error: invalid key format")
                return 1

            if len(parts) == 2:
                section = (parts[0],)
                name = parts[1]
            else:
                # For keys like "remote.origin.url", section is ("remote", "origin")
                section = tuple(parts[:-1])
                name = parts[-1]

            config.set(section, name, parsed_args.value)
            if parsed_args.global_config:
                config.write_to_path()
            else:
                config.write_to_path()
            return 0

        # No action specified
        parser.print_help()
        return 1


class cmd_commit(Command):
    """Record changes to the repository."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the commit command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--message", "-m", help="Commit message")
        parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            help="Automatically stage all tracked files that have been modified",
        )
        parser.add_argument(
            "--amend",
            action="store_true",
            help="Replace the tip of the current branch by creating a new commit",
        )
        parsed_args = parser.parse_args(args)

        message: bytes | str | Callable[[Repo | None, Commit | None], bytes]

        if parsed_args.message:
            message = parsed_args.message
        elif parsed_args.amend:
            # For amend, create a callable that opens editor with original message pre-populated
            def get_amend_message(repo: Repo | None, commit: Commit | None) -> bytes:
                # Get the original commit message from current HEAD
                assert repo is not None
                try:
                    head_commit = repo[repo.head()]
                    assert isinstance(head_commit, Commit)
                    original_message = head_commit.message
                except KeyError:
                    original_message = b""

                # Open editor with original message
                return _get_commit_message_with_template(original_message, repo, commit)

            message = get_amend_message
        else:
            # For regular commits, use empty template
            def get_regular_message(repo: Repo | None, commit: Commit | None) -> bytes:
                return _get_commit_message_with_template(b"", repo, commit)

            message = get_regular_message

        try:
            porcelain.commit(
                ".", message=message, all=parsed_args.all, amend=parsed_args.amend
            )
        except CommitMessageError as e:
            logging.exception(e)
            return 1
        return None


class cmd_commit_tree(Command):
    """Create a new commit object from a tree."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the commit-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--message", "-m", required=True, help="Commit message")
        parser.add_argument("tree", help="Tree SHA to commit")
        parsed_args = parser.parse_args(args)
        porcelain.commit_tree(".", tree=parsed_args.tree, message=parsed_args.message)


class cmd_update_server_info(Command):
    """Update auxiliary info file to help dumb servers."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the update-server-info command.

        Args:
            args: Command line arguments
        """
        porcelain.update_server_info(".")


class cmd_symbolic_ref(Command):
    """Read, modify and delete symbolic refs."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the symbolic-ref command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("name", help="Symbolic reference name")
        parser.add_argument("ref", nargs="?", help="Target reference")
        parser.add_argument("--force", action="store_true", help="Force update")
        parsed_args = parser.parse_args(args)

        # If ref is provided, we're setting; otherwise we're reading
        if parsed_args.ref:
            # Set symbolic reference
            from .repo import Repo

            with Repo(".") as repo:
                repo.refs.set_symbolic_ref(
                    parsed_args.name.encode(), parsed_args.ref.encode()
                )
            return 0
        else:
            # Read symbolic reference
            from .repo import Repo

            with Repo(".") as repo:
                try:
                    target = repo.refs.read_ref(parsed_args.name.encode())
                    if target is None:
                        logger.error(
                            "fatal: ref '%s' is not a symbolic ref", parsed_args.name
                        )
                        return 1
                    elif target.startswith(b"ref: "):
                        logger.info(target[5:].decode())
                    else:
                        logger.info(target.decode())
                    return 0
                except KeyError:
                    logger.error(
                        "fatal: ref '%s' is not a symbolic ref", parsed_args.name
                    )
                    return 1


class cmd_pack_refs(Command):
    """Pack heads and tags for efficient repository access."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the pack-refs command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--all", action="store_true")
        # ignored, we never prune
        parser.add_argument("--no-prune", action="store_true")

        args = parser.parse_args(argv)

        porcelain.pack_refs(".", all=args.all)


class cmd_var(Command):
    """Display Git logical variables."""

    def run(self, argv: Sequence[str]) -> int | None:
        """Execute the var command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "variable",
            nargs="?",
            help="Variable to query (e.g., GIT_AUTHOR_IDENT)",
        )
        parser.add_argument(
            "-l",
            "--list",
            action="store_true",
            help="List all variables",
        )
        args = parser.parse_args(argv)

        if args.list:
            # List all variables
            variables = porcelain.var_list(".")
            for key, value in sorted(variables.items()):
                print(f"{key}={value}")
            return 0
        elif args.variable:
            # Query specific variable
            try:
                value = porcelain.var(".", variable=args.variable)
                print(value)
                return 0
            except KeyError:
                logger.error("error: variable '%s' has no value", args.variable)
                return 1
        else:
            # No arguments - print error
            logger.error("error: variable or -l is required")
            parser.print_help()
            return 1


class cmd_show(Command):
    """Show various types of objects."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the show command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("objectish", type=str, nargs="*")
        parser.add_argument(
            "--color",
            choices=["always", "never", "auto"],
            default="auto",
            help="Use colored output (requires rich)",
        )
        args = parser.parse_args(argv)

        # Determine if we should use color
        def _should_use_color() -> bool:
            if args.color == "always":
                return True
            elif args.color == "never":
                return False
            else:  # auto
                return sys.stdout.isatty()

        def _create_output_stream(outstream: TextIO) -> TextIO:
            """Create output stream, optionally with colorization."""
            if not _should_use_color():
                return outstream

            from .diff import ColorizedDiffStream

            if not ColorizedDiffStream.is_available():
                if args.color == "always":
                    raise ImportError(
                        "Rich is required for colored output. Install with: pip install 'dulwich[colordiff]'"
                    )
                else:
                    logging.warning(
                        "Rich not available, disabling colored output. Install with: pip install 'dulwich[colordiff]'"
                    )
                    return outstream

            # Wrap the ColorizedDiffStream (BinaryIO) back to TextIO
            import io

            colorized = ColorizedDiffStream(outstream.buffer)
            return io.TextIOWrapper(colorized, encoding="utf-8", line_buffering=True)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="show") as outstream:
                output_stream = _create_output_stream(outstream)
                porcelain.show(repo, args.objectish or None, outstream=output_stream)


class cmd_show_ref(Command):
    """List references in a local repository."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the show-ref command.

        Args:
            args: Command line arguments
        Returns:
            Exit code (0 for success, 1 for error/no matches, 2 for missing ref with --exists)
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--head",
            action="store_true",
            help="Show the HEAD reference",
        )
        parser.add_argument(
            "--branches",
            action="store_true",
            help="Limit to local branches",
        )
        parser.add_argument(
            "--tags",
            action="store_true",
            help="Limit to local tags",
        )
        parser.add_argument(
            "-d",
            "--dereference",
            action="store_true",
            help="Dereference tags into object IDs",
        )
        parser.add_argument(
            "-s",
            "--hash",
            nargs="?",
            const=40,  # TODO: Support SHA256
            type=int,
            metavar="n",
            help="Only show the OID, not the reference name",
        )
        parser.add_argument(
            "--abbrev",
            nargs="?",
            const=7,
            type=int,
            metavar="n",
            help="Abbreviate the object name",
        )
        parser.add_argument(
            "--verify",
            action="store_true",
            help="Enable stricter reference checking (exact path match)",
        )
        parser.add_argument(
            "--exists",
            action="store_true",
            help="Check whether the given reference exists",
        )
        parser.add_argument(
            "-q",
            "--quiet",
            action="store_true",
            help="Do not print any results to stdout",
        )
        parser.add_argument(
            "patterns",
            nargs="*",
            help="Show references matching patterns",
        )
        parsed_args = parser.parse_args(args)

        # Handle --exists mode
        if parsed_args.exists:
            if not parsed_args.patterns or len(parsed_args.patterns) != 1:
                logger.error("--exists requires exactly one reference argument")
                return 1

            try:
                with Repo(".") as repo:
                    repo_refs = repo.get_refs()
                    pattern_bytes = os.fsencode(parsed_args.patterns[0])
                    if pattern_bytes in repo_refs:
                        return 0  # Reference exists
                    else:
                        return 2  # Reference missing
            except (NotGitRepository, OSError, FileFormatException) as e:
                logger.error(f"Error looking up reference: {e}")
                return 1  # Error looking up reference

        # Regular show-ref mode
        try:
            matched_refs = porcelain.show_ref(
                ".",
                patterns=parsed_args.patterns if parsed_args.patterns else None,
                head=parsed_args.head,
                branches=parsed_args.branches,
                tags=parsed_args.tags,
                dereference=parsed_args.dereference,
                verify=parsed_args.verify,
            )
        except (NotGitRepository, OSError, FileFormatException) as e:
            logger.error(f"Error: {e}")
            return 1

        # Return error if no matches found (unless quiet)
        if not matched_refs:
            if parsed_args.verify and not parsed_args.quiet:
                logger.error("error: no matching refs found")
            return 1

        # Output results
        if not parsed_args.quiet:
            # TODO: Add support for SHA256
            abbrev_len = parsed_args.abbrev if parsed_args.abbrev else 40
            hash_only = parsed_args.hash is not None
            if hash_only and parsed_args.hash:
                abbrev_len = parsed_args.hash

            for sha, ref in matched_refs:
                sha_str = sha.decode()
                if abbrev_len < 40:
                    sha_str = sha_str[:abbrev_len]

                if hash_only:
                    logger.info(sha_str)
                else:
                    logger.info(f"{sha_str} {ref.decode()}")

        return 0


class cmd_show_branch(Command):
    """Show branches and their commits."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the show-branch command.

        Args:
            args: Command line arguments
        Returns:
            Exit code (0 for success, 1 for error)
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-r",
            "--remotes",
            action="store_true",
            help="Show remote-tracking branches",
        )
        parser.add_argument(
            "-a",
            "--all",
            dest="all_branches",
            action="store_true",
            help="Show both remote-tracking and local branches",
        )
        parser.add_argument(
            "--current",
            action="store_true",
            help="Include current branch if not given on command line",
        )
        parser.add_argument(
            "--topo-order",
            dest="topo_order",
            action="store_true",
            help="Show commits in topological order",
        )
        parser.add_argument(
            "--date-order",
            action="store_true",
            help="Show commits in date order (default)",
        )
        parser.add_argument(
            "--more",
            type=int,
            metavar="n",
            help="Show n more commits beyond common ancestor",
        )
        parser.add_argument(
            "--list",
            dest="list_branches",
            action="store_true",
            help="Show only branch names and their tip commits",
        )
        parser.add_argument(
            "--independent",
            dest="independent_branches",
            action="store_true",
            help="Show only branches not reachable from any other",
        )
        parser.add_argument(
            "--merge-base",
            dest="merge_base",
            action="store_true",
            help="Show merge base of specified branches",
        )
        parser.add_argument(
            "branches",
            nargs="*",
            help="Branches to show (default: all local branches)",
        )
        parsed_args = parser.parse_args(args)

        try:
            output_lines = porcelain.show_branch(
                ".",
                branches=parsed_args.branches if parsed_args.branches else None,
                all_branches=parsed_args.all_branches,
                remotes=parsed_args.remotes,
                current=parsed_args.current,
                topo_order=parsed_args.topo_order,
                more=parsed_args.more,
                list_branches=parsed_args.list_branches,
                independent_branches=parsed_args.independent_branches,
                merge_base=parsed_args.merge_base,
            )
        except (NotGitRepository, OSError, FileFormatException) as e:
            logger.error(f"Error: {e}")
            return 1

        # Output results
        for line in output_lines:
            logger.info(line)

        return 0


class cmd_diff_tree(Command):
    """Compare the content and mode of trees."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the diff-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("old_tree", help="Old tree SHA")
        parser.add_argument("new_tree", help="New tree SHA")
        parsed_args = parser.parse_args(args)
        porcelain.diff_tree(".", parsed_args.old_tree, parsed_args.new_tree)


class cmd_rev_list(Command):
    """List commit objects in reverse chronological order."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the rev-list command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("commits", nargs="+", help="Commit IDs to list")
        parsed_args = parser.parse_args(args)
        porcelain.rev_list(".", parsed_args.commits)


class cmd_tag(Command):
    """Create, list, delete or verify a tag object."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the tag command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-a",
            "--annotated",
            help="Create an annotated tag.",
            action="store_true",
        )
        parser.add_argument(
            "-s", "--sign", help="Sign the annotated tag.", action="store_true"
        )
        parser.add_argument("tag_name", help="Name of the tag to create")
        parsed_args = parser.parse_args(args)
        porcelain.tag_create(
            ".",
            parsed_args.tag_name,
            annotated=parsed_args.annotated,
            sign=parsed_args.sign,
        )


class cmd_verify_commit(Command):
    """Check the GPG signature of commits."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the verify-commit command.

        Args:
            args: Command line arguments

        Returns:
            Exit code (1 on error, None on success)
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-v",
            "--verbose",
            help="Print the contents of the commit object before validating it.",
            action="store_true",
        )
        parser.add_argument(
            "--raw",
            help="Print the raw gpg status output to standard error.",
            action="store_true",
        )
        parser.add_argument(
            "commits",
            nargs="*",
            default=["HEAD"],
            help="Commits to verify (defaults to HEAD)",
        )
        parsed_args = parser.parse_args(args)

        exit_code = None
        for commit in parsed_args.commits:
            try:
                if parsed_args.verbose:
                    # Show commit contents before verification
                    porcelain.show(
                        ".",
                        objects=[commit],
                        outstream=sys.stdout,
                    )
                porcelain.verify_commit(".", commit)
                if not parsed_args.raw:
                    print(f"gpg: Good signature from commit '{commit}'")
            except Exception as e:
                if not parsed_args.raw:
                    print(f"error: {commit}: {e}", file=sys.stderr)
                else:
                    # In raw mode, let the exception propagate
                    raise
                exit_code = 1

        return exit_code


class cmd_verify_tag(Command):
    """Check the GPG signature of tags."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the verify-tag command.

        Args:
            args: Command line arguments

        Returns:
            Exit code (1 on error, None on success)
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-v",
            "--verbose",
            help="Print the contents of the tag object before validating it.",
            action="store_true",
        )
        parser.add_argument(
            "--raw",
            help="Print the raw gpg status output to standard error.",
            action="store_true",
        )
        parser.add_argument("tags", nargs="+", help="Tags to verify")
        parsed_args = parser.parse_args(args)

        exit_code = None
        for tag in parsed_args.tags:
            try:
                if parsed_args.verbose:
                    # Show tag contents before verification
                    porcelain.show(
                        ".",
                        objects=[tag],
                        outstream=sys.stdout,
                    )
                porcelain.verify_tag(".", tag)
                if not parsed_args.raw:
                    print(f"gpg: Good signature from tag '{tag}'")
            except Exception as e:
                if not parsed_args.raw:
                    print(f"error: {tag}: {e}", file=sys.stderr)
                else:
                    # In raw mode, let the exception propagate
                    raise
                exit_code = 1

        return exit_code


class cmd_repack(Command):
    """Pack unpacked objects in a repository."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the repack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--write-bitmap-index",
            action="store_true",
            help="write a bitmap index for packs",
        )
        parsed_args = parser.parse_args(args)
        porcelain.repack(".", write_bitmaps=parsed_args.write_bitmap_index)


class cmd_reflog(Command):
    """Manage reflog information."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the reflog command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(prog="dulwich reflog")
        subparsers = parser.add_subparsers(dest="subcommand", help="Subcommand")

        # Show subcommand (default when no subcommand is specified)
        show_parser = subparsers.add_parser(
            "show", help="Show reflog entries (default)", add_help=False
        )
        show_parser.add_argument(
            "ref", nargs="?", default="HEAD", help="Reference to show reflog for"
        )
        show_parser.add_argument(
            "--all", action="store_true", help="Show reflogs for all refs"
        )

        # Expire subcommand
        expire_parser = subparsers.add_parser("expire", help="Expire reflog entries")
        expire_parser.add_argument(
            "ref", nargs="?", help="Reference to expire reflog for"
        )
        expire_parser.add_argument(
            "--all", action="store_true", help="Expire reflogs for all refs"
        )
        expire_parser.add_argument(
            "--expire",
            type=str,
            help="Expire entries older than time (e.g., '90 days ago', 'all', 'never')",
        )
        expire_parser.add_argument(
            "--expire-unreachable",
            type=str,
            help="Expire unreachable entries older than time",
        )
        expire_parser.add_argument(
            "--dry-run", "-n", action="store_true", help="Show what would be expired"
        )

        # Delete subcommand
        delete_parser = subparsers.add_parser(
            "delete", help="Delete specific reflog entry"
        )
        delete_parser.add_argument(
            "refspec", help="Reference specification (e.g., HEAD@{1})"
        )
        delete_parser.add_argument(
            "--rewrite",
            action="store_true",
            help="Rewrite subsequent entries to maintain consistency",
        )

        # If no arguments or first arg is not a subcommand, treat as show
        if not args or (args[0] not in ["show", "expire", "delete"]):
            # Parse as show command
            parsed_args = parser.parse_args(["show", *list(args)])
        else:
            parsed_args = parser.parse_args(args)

        if parsed_args.subcommand == "expire":
            self._run_expire(parsed_args)
        elif parsed_args.subcommand == "delete":
            self._run_delete(parsed_args)
        else:  # show or default
            self._run_show(parsed_args)

    def _run_show(self, parsed_args: argparse.Namespace) -> None:
        """Show reflog entries."""
        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="reflog") as outstream:
                if parsed_args.all:
                    # Show reflogs for all refs
                    for ref_bytes, entry in porcelain.reflog(repo, all=True):
                        ref_str = ref_bytes.decode("utf-8", "replace")
                        short_new = entry.new_sha[:8].decode("ascii")
                        outstream.write(
                            f"{short_new} {ref_str}: {entry.message.decode('utf-8', 'replace')}\n"
                        )
                else:
                    ref = (
                        parsed_args.ref.encode("utf-8")
                        if isinstance(parsed_args.ref, str)
                        else parsed_args.ref
                    )

                    for i, entry in enumerate(porcelain.reflog(repo, ref)):
                        # Format similar to git reflog
                        from dulwich.reflog import Entry

                        assert isinstance(entry, Entry)
                        short_new = entry.new_sha[:8].decode("ascii")
                        message = (
                            entry.message.decode("utf-8", "replace")
                            if entry.message
                            else ""
                        )
                        outstream.write(
                            f"{short_new} {ref.decode('utf-8', 'replace')}@{{{i}}}: {message}\n"
                        )

    def _run_expire(self, parsed_args: argparse.Namespace) -> None:
        """Expire reflog entries."""
        # Parse time specifications
        expire_time = None
        expire_unreachable_time = None

        if parsed_args.expire:
            expire_time = parse_time_to_timestamp(parsed_args.expire)
        if parsed_args.expire_unreachable:
            expire_unreachable_time = parse_time_to_timestamp(
                parsed_args.expire_unreachable
            )

        # Execute expire
        result = porcelain.reflog_expire(
            repo=".",
            ref=parsed_args.ref,
            all=parsed_args.all,
            expire_time=expire_time,
            expire_unreachable_time=expire_unreachable_time,
            dry_run=parsed_args.dry_run,
        )

        # Print results
        for ref_name, count in result.items():
            ref_str = ref_name.decode("utf-8", "replace")
            if parsed_args.dry_run:
                print(f"Would expire {count} entries from {ref_str}")
            else:
                print(f"Expired {count} entries from {ref_str}")

    def _run_delete(self, parsed_args: argparse.Namespace) -> None:
        """Delete a specific reflog entry."""
        from dulwich.reflog import parse_reflog_spec

        # Parse refspec (e.g., "HEAD@{1}" or "refs/heads/master@{2}")
        ref, index = parse_reflog_spec(parsed_args.refspec)

        # Execute delete
        porcelain.reflog_delete(
            repo=".",
            ref=ref,
            index=index,
            rewrite=parsed_args.rewrite,
        )
        print(f"Deleted entry {ref.decode('utf-8', 'replace')}@{{{index}}}")


class cmd_reset(Command):
    """Reset current HEAD to the specified state."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the reset command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        mode_group = parser.add_mutually_exclusive_group()
        mode_group.add_argument(
            "--hard", action="store_true", help="Reset working tree and index"
        )
        mode_group.add_argument("--soft", action="store_true", help="Reset only HEAD")
        mode_group.add_argument(
            "--mixed", action="store_true", help="Reset HEAD and index"
        )
        parser.add_argument("treeish", nargs="?", help="Commit/tree to reset to")
        parsed_args = parser.parse_args(args)

        if parsed_args.hard:
            mode = "hard"
        elif parsed_args.soft:
            mode = "soft"
        elif parsed_args.mixed:
            mode = "mixed"
        else:
            # Default to mixed behavior
            mode = "mixed"

        # Use the porcelain.reset function for all modes
        porcelain.reset(".", mode=mode, treeish=parsed_args.treeish)


class cmd_revert(Command):
    """Revert some existing commits."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the revert command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--no-commit",
            "-n",
            action="store_true",
            help="Apply changes but don't create a commit",
        )
        parser.add_argument("-m", "--message", help="Custom commit message")
        parser.add_argument("commits", nargs="+", help="Commits to revert")
        parsed_args = parser.parse_args(args)

        result = porcelain.revert(
            ".",
            commits=parsed_args.commits,
            no_commit=parsed_args.no_commit,
            message=parsed_args.message,
        )

        if result and not parsed_args.no_commit:
            logger.info("[%s] Revert completed", result.decode("ascii")[:7])


class cmd_daemon(Command):
    """Run a simple Git protocol server."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the daemon command.

        Args:
            args: Command line arguments
        """
        from dulwich import log_utils

        from .protocol import TCP_GIT_PORT

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-l",
            "--listen_address",
            default="localhost",
            help="Binding IP address.",
        )
        parser.add_argument(
            "-p",
            "--port",
            type=int,
            default=TCP_GIT_PORT,
            help="Binding TCP port.",
        )
        parser.add_argument(
            "gitdir", nargs="?", default=".", help="Git directory to serve"
        )
        parsed_args = parser.parse_args(args)

        log_utils.default_logging_config()
        porcelain.daemon(
            parsed_args.gitdir,
            address=parsed_args.listen_address,
            port=parsed_args.port,
        )


class cmd_web_daemon(Command):
    """Run a simple HTTP server for Git repositories."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the web-daemon command.

        Args:
            args: Command line arguments
        """
        from dulwich import log_utils

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-l",
            "--listen_address",
            default="",
            help="Binding IP address.",
        )
        parser.add_argument(
            "-p",
            "--port",
            type=int,
            default=8000,
            help="Binding TCP port.",
        )
        parser.add_argument(
            "gitdir", nargs="?", default=".", help="Git directory to serve"
        )
        parsed_args = parser.parse_args(args)

        log_utils.default_logging_config()
        porcelain.web_daemon(
            parsed_args.gitdir,
            address=parsed_args.listen_address,
            port=parsed_args.port,
        )


class cmd_write_tree(Command):
    """Create a tree object from the current index."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the write-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        sys.stdout.write("{}\n".format(porcelain.write_tree(".").decode()))


class cmd_receive_pack(Command):
    """Receive what is pushed into the repository."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the receive-pack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        parsed_args = parser.parse_args(args)
        porcelain.receive_pack(parsed_args.gitdir)


class cmd_upload_pack(Command):
    """Send objects packed back to git-fetch-pack."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the upload-pack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        parsed_args = parser.parse_args(args)
        porcelain.upload_pack(parsed_args.gitdir)


class cmd_shortlog(Command):
    """Show a shortlog of commits by author."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the shortlog command with the given CLI arguments.

        Args:
            args: List of command line arguments.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        parser.add_argument("--summary", action="store_true", help="Show summary only")
        parser.add_argument(
            "--sort", action="store_true", help="Sort authors by commit count"
        )
        parsed_args = parser.parse_args(args)

        shortlog_items: list[dict[str, str]] = porcelain.shortlog(
            repo=parsed_args.gitdir,
            summary_only=parsed_args.summary,
            sort_by_commits=parsed_args.sort,
        )

        for item in shortlog_items:
            author: str = item["author"]
            messages: str = item["messages"]
            if parsed_args.summary:
                count = len(messages.splitlines())
                sys.stdout.write(f"{count}\t{author}\n")
            else:
                sys.stdout.write(f"{author} ({len(messages.splitlines())}):\n")
                for msg in messages.splitlines():
                    sys.stdout.write(f"    {msg}\n")
                sys.stdout.write("\n")


class cmd_status(Command):
    """Show the working tree status."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the status command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        parser.add_argument(
            "--column",
            action="store_true",
            help="Display untracked files in columns",
        )
        parsed_args = parser.parse_args(args)
        status = porcelain.status(parsed_args.gitdir)
        if any(names for (kind, names) in status.staged.items()):
            sys.stdout.write("Changes to be committed:\n\n")
            for kind, names in status.staged.items():
                for name in names:
                    sys.stdout.write(
                        f"\t{kind}: {name.decode(sys.getfilesystemencoding())}\n"
                    )
            sys.stdout.write("\n")
        if status.unstaged:
            sys.stdout.write("Changes not staged for commit:\n\n")
            for name in status.unstaged:
                sys.stdout.write(f"\t{name.decode(sys.getfilesystemencoding())}\n")
            sys.stdout.write("\n")
        if status.untracked:
            sys.stdout.write("Untracked files:\n\n")
            if parsed_args.column:
                # Format untracked files in columns
                untracked_names = [name for name in status.untracked]
                output = format_columns(untracked_names, mode="column", indent="\t")
                sys.stdout.write(output)
            else:
                for name in status.untracked:
                    sys.stdout.write(f"\t{name}\n")
            sys.stdout.write("\n")


class cmd_ls_remote(Command):
    """List references in a remote repository."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the ls-remote command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--symref", action="store_true", help="Show symbolic references"
        )
        parser.add_argument("url", help="Remote URL to list references from")
        parsed_args = parser.parse_args(args)
        result = porcelain.ls_remote(parsed_args.url)

        if parsed_args.symref:
            # Show symrefs first, like git does
            for ref, target in sorted(result.symrefs.items()):
                if target:
                    sys.stdout.write(f"ref: {target.decode()}\t{ref.decode()}\n")

        # Show regular refs
        for ref in sorted(result.refs):
            sha = result.refs[ref]
            if sha is not None:
                sys.stdout.write(f"{sha.decode()}\t{ref.decode()}\n")


class cmd_ls_tree(Command):
    """List the contents of a tree object."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the ls-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-r",
            "--recursive",
            action="store_true",
            help="Recursively list tree contents.",
        )
        parser.add_argument(
            "--name-only", action="store_true", help="Only display name."
        )
        parser.add_argument("treeish", nargs="?", help="Tree-ish to list")
        parsed_args = parser.parse_args(args)
        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="ls-tree") as outstream:
                porcelain.ls_tree(
                    repo,
                    parsed_args.treeish,
                    outstream=outstream,
                    recursive=parsed_args.recursive,
                    name_only=parsed_args.name_only,
                )


class cmd_pack_objects(Command):
    """Create a packed archive of objects."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the pack-objects command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--stdout", action="store_true", help="Write pack to stdout"
        )
        parser.add_argument("--deltify", action="store_true", help="Create deltas")
        parser.add_argument(
            "--no-reuse-deltas", action="store_true", help="Don't reuse existing deltas"
        )
        parser.add_argument("basename", nargs="?", help="Base name for pack files")
        parsed_args = parser.parse_args(args)

        if not parsed_args.stdout and not parsed_args.basename:
            parser.error("basename required when not using --stdout")

        object_ids = [ObjectID(line.strip().encode()) for line in sys.stdin.readlines()]
        deltify = parsed_args.deltify
        reuse_deltas = not parsed_args.no_reuse_deltas

        if parsed_args.stdout:
            packf = getattr(sys.stdout, "buffer", sys.stdout)
            assert isinstance(packf, BinaryIO)
            idxf = None
            close = []
        else:
            packf = open(parsed_args.basename + ".pack", "wb")
            idxf = open(parsed_args.basename + ".idx", "wb")
            close = [packf, idxf]

        porcelain.pack_objects(
            ".", object_ids, packf, idxf, deltify=deltify, reuse_deltas=reuse_deltas
        )
        for f in close:
            f.close()


class cmd_unpack_objects(Command):
    """Unpack objects from a packed archive."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the unpack-objects command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("pack_file", help="Pack file to unpack")
        parsed_args = parser.parse_args(args)

        count = porcelain.unpack_objects(parsed_args.pack_file)
        logger.info("Unpacked %d objects", count)


class cmd_prune(Command):
    """Prune all unreachable objects from the object database."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the prune command.

        Args:
            args: Command line arguments
        """
        import datetime
        import time

        from dulwich.object_store import DEFAULT_TEMPFILE_GRACE_PERIOD

        parser = argparse.ArgumentParser(
            description="Remove temporary pack files left behind by interrupted operations"
        )
        parser.add_argument(
            "--expire",
            nargs="?",
            const="2.weeks.ago",
            help="Only prune files older than the specified date (default: 2.weeks.ago)",
        )
        parser.add_argument(
            "--dry-run",
            "-n",
            action="store_true",
            help="Only report what would be removed",
        )
        parser.add_argument(
            "--verbose",
            "-v",
            action="store_true",
            help="Report all actions",
        )
        parsed_args = parser.parse_args(args)

        # Parse expire grace period
        grace_period = DEFAULT_TEMPFILE_GRACE_PERIOD
        if parsed_args.expire:
            from .approxidate import parse_relative_time

            try:
                grace_period = parse_relative_time(parsed_args.expire)
            except ValueError:
                # Try to parse as absolute date
                try:
                    date = datetime.datetime.strptime(parsed_args.expire, "%Y-%m-%d")
                    grace_period = int(time.time() - date.timestamp())
                except ValueError:
                    logger.error("Invalid expire date: %s", parsed_args.expire)
                    return 1

        # Progress callback
        def progress(msg: str) -> None:
            if parsed_args.verbose:
                logger.info("%s", msg)

        try:
            porcelain.prune(
                ".",
                grace_period=grace_period,
                dry_run=parsed_args.dry_run,
                progress=progress if parsed_args.verbose else None,
            )
            return None
        except porcelain.Error as e:
            logger.error("%s", e)
            return 1


class cmd_pull(Command):
    """Fetch from and integrate with another repository or a local branch."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the pull command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("from_location", type=str)
        parser.add_argument("refspec", type=str, nargs="*")
        parser.add_argument("--filter", type=str, nargs=1)
        parser.add_argument("--protocol", type=int)
        parsed_args = parser.parse_args(args)
        porcelain.pull(
            ".",
            remote_location=parsed_args.from_location or None,
            refspecs=parsed_args.refspec or None,
            filter_spec=parsed_args.filter,
            protocol_version=parsed_args.protocol or None,
        )


class cmd_push(Command):
    """Update remote refs along with associated objects."""

    def run(self, argv: Sequence[str]) -> int | None:
        """Execute the push command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--force", action="store_true", help="Force")
        parser.add_argument("to_location", type=str)
        parser.add_argument("refspec", type=str, nargs="*")
        args = parser.parse_args(argv)
        try:
            porcelain.push(
                ".", args.to_location, args.refspec or None, force=args.force
            )
        except porcelain.DivergedBranches:
            sys.stderr.write("Diverged branches; specify --force to override")
            return 1

        return None


class cmd_remote_add(Command):
    """Add a remote repository."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the remote-add command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("name", help="Name of the remote")
        parser.add_argument("url", help="URL of the remote")
        parsed_args = parser.parse_args(args)
        porcelain.remote_add(".", parsed_args.name, parsed_args.url)


class SuperCommand(Command):
    """Base class for commands that have subcommands."""

    subcommands: ClassVar[dict[str, type[Command]]] = {}
    default_command: ClassVar[type[Command] | None] = None

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the subcommand command.

        Args:
            args: Command line arguments
        """
        if not args:
            if self.default_command:
                return self.default_command().run(args)
            else:
                logger.info(
                    "Supported subcommands: %s", ", ".join(self.subcommands.keys())
                )
                return False
        cmd = args[0]
        try:
            cmd_kls = self.subcommands[cmd]
        except KeyError:
            logger.error("No such subcommand: %s", args[0])
            sys.exit(1)
        return cmd_kls().run(args[1:])


class cmd_remote(SuperCommand):
    """Manage set of tracked repositories."""

    subcommands: ClassVar[dict[str, type[Command]]] = {
        "add": cmd_remote_add,
    }


class cmd_submodule_list(Command):
    """List submodules."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the submodule-list command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(argv)
        for path, sha in porcelain.submodule_list("."):
            sys.stdout.write(f" {sha} {path}\n")


class cmd_submodule_init(Command):
    """Initialize submodules."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the submodule-init command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(argv)
        porcelain.submodule_init(".")


class cmd_submodule_add(Command):
    """Add a submodule."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the submodule-add command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("url", help="URL of repository to add as submodule")
        parser.add_argument("path", nargs="?", help="Path where submodule should live")
        parser.add_argument("--name", help="Name for the submodule")
        args = parser.parse_args(argv)
        porcelain.submodule_add(".", args.url, args.path, args.name)


class cmd_submodule_update(Command):
    """Update submodules."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the submodule-update command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--init", action="store_true", help="Initialize submodules first"
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force update even if local changes exist",
        )
        parser.add_argument(
            "--recursive",
            action="store_true",
            help="Recursively update nested submodules",
        )
        parser.add_argument(
            "paths", nargs="*", help="Specific submodule paths to update"
        )
        args = parser.parse_args(argv)
        paths = args.paths if args.paths else None
        porcelain.submodule_update(
            ".", paths=paths, init=args.init, force=args.force, recursive=args.recursive
        )


class cmd_submodule(SuperCommand):
    """Initialize, update or inspect submodules."""

    subcommands: ClassVar[dict[str, type[Command]]] = {
        "add": cmd_submodule_add,
        "init": cmd_submodule_init,
        "list": cmd_submodule_list,
        "update": cmd_submodule_update,
    }

    default_command = cmd_submodule_list


class cmd_check_ignore(Command):
    """Check whether files are excluded by gitignore."""

    def run(self, args: Sequence[str]) -> int:
        """Execute the check-ignore command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("paths", nargs="+", help="Paths to check")
        parsed_args = parser.parse_args(args)
        ret = 1
        for path in porcelain.check_ignore(".", parsed_args.paths):
            logger.info(path)
            ret = 0
        return ret


class cmd_check_mailmap(Command):
    """Show canonical names and email addresses of contacts."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the check-mailmap command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("identities", nargs="+", help="Identities to check")
        parsed_args = parser.parse_args(args)
        for identity in parsed_args.identities:
            canonical_identity = porcelain.check_mailmap(".", identity)
            logger.info(canonical_identity)


class cmd_branch(Command):
    """List, create, or delete branches."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the branch command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "branch",
            type=str,
            nargs="?",
            help="Name of the branch",
        )
        parser.add_argument(
            "-d",
            "--delete",
            action="store_true",
            help="Delete branch",
        )
        parser.add_argument("--all", action="store_true", help="List all branches")
        parser.add_argument(
            "--merged", action="store_true", help="List merged into current branch"
        )
        parser.add_argument(
            "--no-merged",
            action="store_true",
            help="List branches not merged into current branch",
        )
        parser.add_argument(
            "--remotes", action="store_true", help="List remotes branches"
        )
        parser.add_argument(
            "--contains",
            nargs="?",
            const="HEAD",
            help="List branches that contain a specific commit",
        )
        parser.add_argument(
            "--column", action="store_true", help="Display branch list in columns"
        )
        parser.add_argument(
            "--list",
            nargs="?",
            const=None,
            help="List branches matching a pattern",
        )
        parsed_args = parser.parse_args(args)

        def print_branches(
            branches: Iterator[bytes] | Sequence[bytes], use_columns: bool = False
        ) -> None:
            if use_columns:
                branch_names = [branch.decode() for branch in branches]
                output = format_columns(branch_names, mode="column")
                sys.stdout.write(output)
            else:
                for branch in branches:
                    sys.stdout.write(f"{branch.decode()}\n")

        branches: Iterator[bytes] | list[bytes] | None = None

        try:
            if parsed_args.all:
                branches = porcelain.branch_list(".") + porcelain.branch_remotes_list(
                    "."
                )
            elif parsed_args.remotes:
                branches = porcelain.branch_remotes_list(".")
            elif parsed_args.merged:
                branches = porcelain.merged_branches(".")
            elif parsed_args.no_merged:
                branches = porcelain.no_merged_branches(".")
            elif parsed_args.contains:
                try:
                    branches = list(
                        porcelain.branches_containing(".", commit=parsed_args.contains)
                    )

                except KeyError as e:
                    sys.stderr.write(
                        f"error: object name {e.args[0].decode()} not found\n"
                    )
                    return 1

        except porcelain.Error as e:
            sys.stderr.write(f"{e}")
            return 1

        pattern = parsed_args.list
        if pattern is not None and branches:
            branches = porcelain.filter_branches_by_pattern(branches, pattern)

        if branches is not None:
            print_branches(branches, parsed_args.column)
            return 0

        if not parsed_args.branch:
            logger.error("Usage: dulwich branch [-d] BRANCH_NAME")
            return 1

        if parsed_args.delete:
            porcelain.branch_delete(".", name=parsed_args.branch)
        else:
            try:
                porcelain.branch_create(".", name=parsed_args.branch)
            except porcelain.Error as e:
                sys.stderr.write(f"{e}")
                return 1
        return 0


class cmd_checkout(Command):
    """Switch branches or restore working tree files."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the checkout command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "target",
            type=str,
            help="Name of the branch, tag, or commit to checkout",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force checkout",
        )
        parser.add_argument(
            "-b",
            "--new-branch",
            type=str,
            help="Create a new branch at the target and switch to it",
        )
        parsed_args = parser.parse_args(args)
        if not parsed_args.target:
            logger.error("Usage: dulwich checkout TARGET [--force] [-b NEW_BRANCH]")
            return 1

        try:
            porcelain.checkout(
                ".",
                target=parsed_args.target,
                force=parsed_args.force,
                new_branch=parsed_args.new_branch,
            )
        except porcelain.CheckoutError as e:
            sys.stderr.write(f"{e}\n")
            return 1
        return 0


class cmd_restore(Command):
    """Restore working tree files."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the restore command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "paths",
            nargs="+",
            type=str,
            help="Paths to restore",
        )
        parser.add_argument(
            "-s",
            "--source",
            type=str,
            help="Restore from a specific commit (default: HEAD for --staged, index for worktree)",
        )
        parser.add_argument(
            "--staged",
            action="store_true",
            help="Restore files in the index",
        )
        parser.add_argument(
            "--worktree",
            action="store_true",
            help="Restore files in the working tree",
        )
        parsed_args = parser.parse_args(args)

        # If neither --staged nor --worktree is specified, default to --worktree
        if not parsed_args.staged and not parsed_args.worktree:
            worktree = True
            staged = False
        else:
            worktree = parsed_args.worktree
            staged = parsed_args.staged

        try:
            porcelain.restore(
                ".",
                paths=parsed_args.paths,
                source=parsed_args.source,
                staged=staged,
                worktree=worktree,
            )
        except porcelain.CheckoutError as e:
            sys.stderr.write(f"{e}\n")
            return 1
        return 0


class cmd_switch(Command):
    """Switch branches."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the switch command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "target",
            type=str,
            help="Branch or commit to switch to",
        )
        parser.add_argument(
            "-c",
            "--create",
            type=str,
            help="Create a new branch at the target and switch to it",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force switch even if there are local changes",
        )
        parser.add_argument(
            "-d",
            "--detach",
            action="store_true",
            help="Switch to a commit in detached HEAD state",
        )
        parsed_args = parser.parse_args(args)

        if not parsed_args.target:
            logger.error(
                "Usage: dulwich switch TARGET [-c NEW_BRANCH] [--force] [--detach]"
            )
            return 1

        try:
            porcelain.switch(
                ".",
                target=parsed_args.target,
                create=parsed_args.create,
                force=parsed_args.force,
                detach=parsed_args.detach,
            )
        except porcelain.CheckoutError as e:
            sys.stderr.write(f"{e}\n")
            return 1
        return 0


class cmd_stash_list(Command):
    """List stash entries."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the stash-list command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        from .repo import Repo
        from .stash import Stash

        with Repo(".") as r:
            stash = Stash.from_repo(r)
            for i, entry in enumerate(stash.stashes()):
                logger.info(
                    "stash@{%d}: %s",
                    i,
                    entry.message.decode("utf-8", "replace").rstrip("\n"),
                )


class cmd_stash_push(Command):
    """Save your local modifications to a new stash."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the stash-push command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        porcelain.stash_push(".")
        logger.info("Saved working directory and index state")


class cmd_stash_pop(Command):
    """Apply a stash and remove it from the stash list."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the stash-pop command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        porcelain.stash_pop(".")
        logger.info("Restored working directory and index state")


class cmd_bisect(SuperCommand):
    """Use binary search to find the commit that introduced a bug."""

    subcommands: ClassVar[dict[str, type[Command]]] = {}

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the bisect command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(prog="dulwich bisect")
        subparsers = parser.add_subparsers(dest="subcommand", help="bisect subcommands")

        # bisect start
        start_parser = subparsers.add_parser("start", help="Start a new bisect session")
        start_parser.add_argument("bad", nargs="?", help="Bad commit")
        start_parser.add_argument("good", nargs="*", help="Good commit(s)")
        start_parser.add_argument(
            "--no-checkout",
            action="store_true",
            help="Don't checkout commits during bisect",
        )
        start_parser.add_argument(
            "--term-bad", default="bad", help="Term to use for bad commits"
        )
        start_parser.add_argument(
            "--term-good", default="good", help="Term to use for good commits"
        )
        start_parser.add_argument(
            "--", dest="paths", nargs="*", help="Paths to limit bisect to"
        )

        # bisect bad
        bad_parser = subparsers.add_parser("bad", help="Mark a commit as bad")
        bad_parser.add_argument("rev", nargs="?", help="Commit to mark as bad")

        # bisect good
        good_parser = subparsers.add_parser("good", help="Mark a commit as good")
        good_parser.add_argument("rev", nargs="?", help="Commit to mark as good")

        # bisect skip
        skip_parser = subparsers.add_parser("skip", help="Skip commits")
        skip_parser.add_argument("revs", nargs="*", help="Commits to skip")

        # bisect reset
        reset_parser = subparsers.add_parser("reset", help="Reset bisect state")
        reset_parser.add_argument("commit", nargs="?", help="Commit to reset to")

        # bisect log
        subparsers.add_parser("log", help="Show bisect log")

        # bisect replay
        replay_parser = subparsers.add_parser("replay", help="Replay bisect log")
        replay_parser.add_argument("logfile", help="Log file to replay")

        # bisect help
        subparsers.add_parser("help", help="Show help")

        parsed_args = parser.parse_args(args)

        if not parsed_args.subcommand:
            parser.print_help()
            return 1

        try:
            if parsed_args.subcommand == "start":
                next_sha = porcelain.bisect_start(
                    bad=parsed_args.bad,
                    good=parsed_args.good if parsed_args.good else None,
                    paths=parsed_args.paths,
                    no_checkout=parsed_args.no_checkout,
                    term_bad=parsed_args.term_bad,
                    term_good=parsed_args.term_good,
                )
                if next_sha:
                    logger.info(
                        "Bisecting: checking out '%s'", next_sha.decode("ascii")
                    )

            elif parsed_args.subcommand == "bad":
                next_sha = porcelain.bisect_bad(rev=parsed_args.rev)
                if next_sha:
                    logger.info(
                        "Bisecting: checking out '%s'", next_sha.decode("ascii")
                    )
                else:
                    # Bisect complete - find the first bad commit
                    with porcelain.open_repo_closing(".") as r:
                        bad_ref = os.path.join(r.controldir(), "refs", "bisect", "bad")
                        with open(bad_ref, "rb") as f:
                            bad_sha = ObjectID(f.read().strip())
                        commit = r.object_store[bad_sha]
                        assert isinstance(commit, Commit)
                        message = commit.message.decode(
                            "utf-8", errors="replace"
                        ).split("\n")[0]
                        logger.info(
                            "%s is the first bad commit", bad_sha.decode("ascii")
                        )
                        logger.info("commit %s", bad_sha.decode("ascii"))
                        logger.info("    %s", message)

            elif parsed_args.subcommand == "good":
                next_sha = porcelain.bisect_good(rev=parsed_args.rev)
                if next_sha:
                    logger.info(
                        "Bisecting: checking out '%s'", next_sha.decode("ascii")
                    )

            elif parsed_args.subcommand == "skip":
                next_sha = porcelain.bisect_skip(
                    revs=parsed_args.revs if parsed_args.revs else None
                )
                if next_sha:
                    logger.info(
                        "Bisecting: checking out '%s'", next_sha.decode("ascii")
                    )

            elif parsed_args.subcommand == "reset":
                porcelain.bisect_reset(commit=parsed_args.commit)
                logger.info("Bisect reset")

            elif parsed_args.subcommand == "log":
                log = porcelain.bisect_log()
                logger.info(log.rstrip())

            elif parsed_args.subcommand == "replay":
                porcelain.bisect_replay(".", log_file=parsed_args.logfile)
                logger.info("Replayed bisect log from %s", parsed_args.logfile)

            elif parsed_args.subcommand == "help":
                parser.print_help()

        except porcelain.Error as e:
            logger.error("%s", e)
            return 1
        except ValueError as e:
            logger.error("%s", e)
            return 1

        return 0


class cmd_stash(SuperCommand):
    """Stash the changes in a dirty working directory away."""

    subcommands: ClassVar[dict[str, type[Command]]] = {
        "list": cmd_stash_list,
        "pop": cmd_stash_pop,
        "push": cmd_stash_push,
    }


class cmd_ls_files(Command):
    """Show information about files in the index and working tree."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the ls-files command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        for name in porcelain.ls_files("."):
            logger.info(name)


class cmd_describe(Command):
    """Give an object a human readable name based on an available ref."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the describe command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        logger.info(porcelain.describe("."))


class cmd_diagnose(Command):
    """Display diagnostic information about the Python environment."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the diagnose command.

        Args:
            args: Command line arguments
        """
        # TODO: Support creating zip files with diagnostic information
        parser = argparse.ArgumentParser()
        parser.parse_args(args)

        # Python version and executable
        logger.info("Python version: %s", sys.version)
        logger.info("Python executable: %s", sys.executable)

        # PYTHONPATH
        pythonpath = os.environ.get("PYTHONPATH", "")
        if pythonpath:
            logger.info("PYTHONPATH: %s", pythonpath)
        else:
            logger.info("PYTHONPATH: (not set)")

        # sys.path
        logger.info("sys.path:")
        for path_entry in sys.path:
            logger.info("  %s", path_entry)

        # Dulwich version
        try:
            import dulwich

            logger.info("Dulwich version: %s", dulwich.__version__)
        except AttributeError:
            logger.info("Dulwich version: (unknown)")

        # List installed dependencies and their versions
        logger.info("Installed dependencies:")

        # Core dependencies
        dependencies = [
            ("urllib3", "core"),
            ("typing_extensions", "core (Python < 3.12)"),
        ]

        # Optional dependencies
        optional_dependencies = [
            ("fastimport", "fastimport"),
            ("gpg", "pgp"),
            ("paramiko", "paramiko"),
            ("rich", "colordiff"),
            ("merge3", "merge"),
            ("patiencediff", "patiencediff"),
            ("atheris", "fuzzing"),
        ]

        for dep, dep_type in dependencies + optional_dependencies:
            try:
                module = __import__(dep)
                version = getattr(module, "__version__", "(unknown)")
                logger.info("  %s: %s [%s]", dep, version, dep_type)
            except ImportError:
                logger.info("  %s: (not installed) [%s]", dep, dep_type)


class cmd_merge(Command):
    """Join two or more development histories together."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the merge command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("commit", type=str, nargs="+", help="Commit(s) to merge")
        parser.add_argument(
            "--no-commit", action="store_true", help="Do not create a merge commit"
        )
        parser.add_argument(
            "--no-ff", action="store_true", help="Force create a merge commit"
        )
        parser.add_argument("-m", "--message", type=str, help="Merge commit message")
        parsed_args = parser.parse_args(args)

        try:
            # If multiple commits are provided, pass them as a list
            # If only one commit is provided, pass it as a string
            if len(parsed_args.commit) == 1:
                committish = parsed_args.commit[0]
            else:
                committish = parsed_args.commit

            merge_commit_id, conflicts = porcelain.merge(
                ".",
                committish,
                no_commit=parsed_args.no_commit,
                no_ff=parsed_args.no_ff,
                message=parsed_args.message,
            )

            if conflicts:
                logger.warning("Merge conflicts in %d file(s):", len(conflicts))
                for conflict_path in conflicts:
                    logger.warning("  %s", conflict_path.decode())
                if len(parsed_args.commit) > 1:
                    logger.error(
                        "Octopus merge failed; refusing to merge with conflicts."
                    )
                else:
                    logger.error(
                        "Automatic merge failed; fix conflicts and then commit the result."
                    )
                return 1
            elif merge_commit_id is None and not parsed_args.no_commit:
                logger.info("Already up to date.")
            elif parsed_args.no_commit:
                logger.info("Automatic merge successful; not committing as requested.")
            else:
                assert merge_commit_id is not None
                if len(parsed_args.commit) > 1:
                    logger.info(
                        "Octopus merge successful. Created merge commit %s",
                        merge_commit_id.decode(),
                    )
                else:
                    logger.info(
                        "Merge successful. Created merge commit %s",
                        merge_commit_id.decode(),
                    )
            return 0
        except porcelain.Error as e:
            logger.error("%s", e)
            return 1


class cmd_merge_base(Command):
    """Find the best common ancestor between commits."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the merge-base command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Find the best common ancestor between commits",
            prog="dulwich merge-base",
        )
        parser.add_argument("commits", nargs="+", help="Commits to find merge base for")
        parser.add_argument("--all", action="store_true", help="Output all merge bases")
        parser.add_argument(
            "--octopus",
            action="store_true",
            help="Compute common ancestor of all commits",
        )
        parser.add_argument(
            "--is-ancestor",
            action="store_true",
            help="Check if first commit is ancestor of second",
        )
        parser.add_argument(
            "--independent",
            action="store_true",
            help="List commits not reachable from others",
        )
        parsed_args = parser.parse_args(args)

        try:
            if parsed_args.is_ancestor:
                if len(parsed_args.commits) != 2:
                    logger.error("--is-ancestor requires exactly two commits")
                    return 1
                is_anc = porcelain.is_ancestor(
                    ".",
                    ancestor=parsed_args.commits[0],
                    descendant=parsed_args.commits[1],
                )
                return 0 if is_anc else 1
            elif parsed_args.independent:
                commits = porcelain.independent_commits(".", parsed_args.commits)
                for commit_id in commits:
                    print(commit_id.decode())
                return 0
            else:
                if len(parsed_args.commits) < 2:
                    logger.error("At least two commits are required")
                    return 1
                merge_bases = porcelain.merge_base(
                    ".",
                    parsed_args.commits,
                    all=parsed_args.all,
                    octopus=parsed_args.octopus,
                )
                for commit_id in merge_bases:
                    print(commit_id.decode())
                return 0
        except (ValueError, KeyError) as e:
            logger.error("%s", e)
            return 1


class cmd_notes_add(Command):
    """Add notes to a commit."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the notes-add command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("object", help="Object to annotate")
        parser.add_argument("-m", "--message", help="Note message", required=True)
        parser.add_argument(
            "--ref", default="commits", help="Notes ref (default: commits)"
        )
        parsed_args = parser.parse_args(args)

        porcelain.notes_add(
            ".", parsed_args.object, parsed_args.message, ref=parsed_args.ref
        )


class cmd_notes_show(Command):
    """Show notes for a commit."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the notes-show command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("object", help="Object to show notes for")
        parser.add_argument(
            "--ref", default="commits", help="Notes ref (default: commits)"
        )
        parsed_args = parser.parse_args(args)

        note = porcelain.notes_show(".", parsed_args.object, ref=parsed_args.ref)
        if note:
            sys.stdout.buffer.write(note)
        else:
            logger.info("No notes found for object %s", parsed_args.object)


class cmd_notes_remove(Command):
    """Remove notes for a commit."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the notes-remove command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("object", help="Object to remove notes from")
        parser.add_argument(
            "--ref", default="commits", help="Notes ref (default: commits)"
        )
        parsed_args = parser.parse_args(args)

        result = porcelain.notes_remove(".", parsed_args.object, ref=parsed_args.ref)
        if result:
            logger.info("Removed notes for object %s", parsed_args.object)
        else:
            logger.info("No notes found for object %s", parsed_args.object)


class cmd_notes_list(Command):
    """List all note objects."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the notes-list command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--ref", default="commits", help="Notes ref (default: commits)"
        )
        parsed_args = parser.parse_args(args)

        notes = porcelain.notes_list(".", ref=parsed_args.ref)
        for object_sha, note_content in notes:
            logger.info(object_sha.hex())


class cmd_notes(SuperCommand):
    """Add or inspect object notes."""

    subcommands: ClassVar[dict[str, type[Command]]] = {
        "add": cmd_notes_add,
        "show": cmd_notes_show,
        "remove": cmd_notes_remove,
        "list": cmd_notes_list,
    }

    default_command = cmd_notes_list


class cmd_replace_list(Command):
    """List all replacement refs."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the replace-list command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)

        replacements = porcelain.replace_list(".")
        for object_sha, replacement_sha in replacements:
            sys.stdout.write(
                f"{object_sha.decode('ascii')} -> {replacement_sha.decode('ascii')}\n"
            )


class cmd_replace_delete(Command):
    """Delete a replacement ref."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the replace-delete command.

        Args:
            args: Command line arguments

        Returns:
            Exit code (0 for success, 1 for error)
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("object", help="Object whose replacement should be removed")
        parsed_args = parser.parse_args(args)

        try:
            porcelain.replace_delete(".", parsed_args.object)
            logger.info("Deleted replacement for %s", parsed_args.object)
            return None
        except KeyError as e:
            logger.error(str(e))
            return 1


class cmd_replace(SuperCommand):
    """Create, list, and delete replacement refs."""

    subcommands: ClassVar[dict[str, type[Command]]] = {
        "list": cmd_replace_list,
        "delete": cmd_replace_delete,
    }

    default_command = cmd_replace_list

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the replace command.

        Args:
            args: Command line arguments

        Returns:
            Exit code (0 for success, 1 for error)
        """
        # Special case: if we have exactly 2 args and no subcommand, treat as create
        if len(args) == 2 and args[0] not in self.subcommands:
            # This is the create form: git replace <object> <replacement>
            parser = argparse.ArgumentParser()
            parser.add_argument("object", help="Object to replace")
            parser.add_argument("replacement", help="Replacement object")
            parsed_args = parser.parse_args(args)

            porcelain.replace_create(".", parsed_args.object, parsed_args.replacement)
            logger.info(
                "Created replacement: %s -> %s",
                parsed_args.object,
                parsed_args.replacement,
            )
            return None

        # Otherwise, delegate to supercommand handling
        return super().run(args)


class cmd_cherry(Command):
    """Find commits not merged upstream."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the cherry command.

        Args:
            args: Command line arguments

        Returns:
            Exit code (0 for success, 1 for error)
        """
        parser = argparse.ArgumentParser(description="Find commits not merged upstream")
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Show commit messages",
        )
        parser.add_argument(
            "upstream",
            nargs="?",
            help="Upstream branch (default: tracking branch or HEAD^)",
        )
        parser.add_argument(
            "head",
            nargs="?",
            help="Head branch (default: HEAD)",
        )
        parser.add_argument(
            "limit",
            nargs="?",
            help="Limit commits to those after this ref",
        )
        parsed_args = parser.parse_args(args)

        try:
            results = porcelain.cherry(
                ".",
                upstream=parsed_args.upstream,
                head=parsed_args.head,
                limit=parsed_args.limit,
                verbose=parsed_args.verbose,
            )
        except (NotGitRepository, OSError, FileFormatException, ValueError) as e:
            logger.error(f"Error: {e}")
            return 1

        # Output results
        for status, commit_sha, message in results:
            # Convert commit_sha to hex string
            if isinstance(commit_sha, bytes):
                commit_hex = commit_sha.hex()
            else:
                commit_hex = commit_sha

            if parsed_args.verbose and message:
                message_str = message.decode("utf-8", errors="replace")
                logger.info(f"{status} {commit_hex} {message_str}")
            else:
                logger.info(f"{status} {commit_hex}")

        return 0


class cmd_cherry_pick(Command):
    """Apply the changes introduced by some existing commits."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the cherry-pick command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Apply the changes introduced by some existing commits"
        )
        parser.add_argument("commit", nargs="?", help="Commit to cherry-pick")
        parser.add_argument(
            "-n",
            "--no-commit",
            action="store_true",
            help="Apply changes without making a commit",
        )
        parser.add_argument(
            "--continue",
            dest="continue_",
            action="store_true",
            help="Continue after resolving conflicts",
        )
        parser.add_argument(
            "--abort",
            action="store_true",
            help="Abort the current cherry-pick operation",
        )
        parsed_args = parser.parse_args(args)

        # Check argument validity
        if parsed_args.continue_ or parsed_args.abort:
            if parsed_args.commit is not None:
                parser.error("Cannot specify commit with --continue or --abort")
                return 1
        else:
            if parsed_args.commit is None:
                parser.error("Commit argument is required")
                return 1

        try:
            commit_arg = parsed_args.commit

            result = porcelain.cherry_pick(
                ".",
                commit_arg,
                no_commit=parsed_args.no_commit,
                continue_=parsed_args.continue_,
                abort=parsed_args.abort,
            )

            if parsed_args.abort:
                logger.info("Cherry-pick aborted.")
            elif parsed_args.continue_:
                if result:
                    logger.info("Cherry-pick completed: %s", result.decode())
                else:
                    logger.info("Cherry-pick completed.")
            elif result is None:
                if parsed_args.no_commit:
                    logger.info("Cherry-pick applied successfully (no commit created).")
                else:
                    # This shouldn't happen unless there were conflicts
                    logger.warning("Cherry-pick resulted in conflicts.")
            else:
                logger.info("Cherry-pick successful: %s", result.decode())

            return None
        except porcelain.Error as e:
            logger.error("%s", e)
            return 1


class cmd_merge_tree(Command):
    """Show three-way merge without touching index."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the merge-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Perform a tree-level merge without touching the working directory"
        )
        parser.add_argument(
            "base_tree",
            nargs="?",
            help="The common ancestor tree (optional, defaults to empty tree)",
        )
        parser.add_argument("our_tree", help="Our side of the merge")
        parser.add_argument("their_tree", help="Their side of the merge")
        parser.add_argument(
            "-z",
            "--name-only",
            action="store_true",
            help="Output only conflict paths, null-terminated",
        )
        parsed_args = parser.parse_args(args)

        try:
            # Determine base tree - if only two parsed_args provided, base is None
            if parsed_args.base_tree is None:
                # Only two arguments provided
                base_tree = None
                our_tree = parsed_args.our_tree
                their_tree = parsed_args.their_tree
            else:
                # Three arguments provided
                base_tree = parsed_args.base_tree
                our_tree = parsed_args.our_tree
                their_tree = parsed_args.their_tree

            merged_tree_id, conflicts = porcelain.merge_tree(
                ".", base_tree, our_tree, their_tree
            )

            if parsed_args.name_only:
                # Output only conflict paths, null-terminated
                for conflict_path in conflicts:
                    sys.stdout.buffer.write(conflict_path)
                    sys.stdout.buffer.write(b"\0")
            else:
                # Output the merged tree SHA
                logger.info(merged_tree_id.decode("ascii"))

                # Output conflict information
                if conflicts:
                    logger.warning("\nConflicts in %d file(s):", len(conflicts))
                    for conflict_path in conflicts:
                        logger.warning("  %s", conflict_path.decode())

            return None

        except porcelain.Error as e:
            logger.error("%s", e)
            return 1
        except KeyError as e:
            logger.error("Object not found: %s", e)
            return 1


class cmd_gc(Command):
    """Cleanup unnecessary files and optimize the local repository."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the gc command.

        Args:
            args: Command line arguments
        """
        import datetime
        import time

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--auto",
            action="store_true",
            help="Only run gc if needed",
        )
        parser.add_argument(
            "--aggressive",
            action="store_true",
            help="Use more aggressive settings",
        )
        parser.add_argument(
            "--no-prune",
            action="store_true",
            help="Do not prune unreachable objects",
        )
        parser.add_argument(
            "--prune",
            nargs="?",
            const="now",
            help="Prune unreachable objects older than date (default: 2 weeks ago)",
        )
        parser.add_argument(
            "--dry-run",
            "-n",
            action="store_true",
            help="Only report what would be done",
        )
        parser.add_argument(
            "--quiet",
            "-q",
            action="store_true",
            help="Only report errors",
        )
        parsed_args = parser.parse_args(args)

        # Parse prune grace period
        grace_period = None
        if parsed_args.prune:
            from .approxidate import parse_relative_time

            try:
                grace_period = parse_relative_time(parsed_args.prune)
            except ValueError:
                # Try to parse as absolute date
                try:
                    date = datetime.datetime.strptime(parsed_args.prune, "%Y-%m-%d")
                    grace_period = int(time.time() - date.timestamp())
                except ValueError:
                    logger.error("Invalid prune date: %s", parsed_args.prune)
                    return 1
        elif not parsed_args.no_prune:
            # Default to 2 weeks
            grace_period = 1209600

        # Progress callback
        def progress(msg: str) -> None:
            if not parsed_args.quiet:
                logger.info(msg)

        try:
            stats = porcelain.gc(
                ".",
                auto=parsed_args.auto,
                aggressive=parsed_args.aggressive,
                prune=not parsed_args.no_prune,
                grace_period=grace_period,
                dry_run=parsed_args.dry_run,
                progress=progress if not parsed_args.quiet else None,
            )

            # Report results
            if not parsed_args.quiet:
                if parsed_args.dry_run:
                    logger.info("\nDry run results:")
            if not parsed_args.quiet:
                if parsed_args.dry_run:
                    print("\nDry run results:")
                else:
                    logger.info("\nGarbage collection complete:")

                if stats.pruned_objects:
                    logger.info(
                        "  Pruned %d unreachable objects", len(stats.pruned_objects)
                    )
                    logger.info("  Freed %s", format_bytes(stats.bytes_freed))

                if stats.packs_before != stats.packs_after:
                    logger.info(
                        "  Reduced pack files from %d to %d",
                        stats.packs_before,
                        stats.packs_after,
                    )

        except porcelain.Error as e:
            logger.error("%s", e)
            return 1
        return None


class cmd_maintenance(Command):
    """Run tasks to optimize Git repository data."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the maintenance command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Run tasks to optimize Git repository data"
        )
        subparsers = parser.add_subparsers(
            dest="subcommand", help="Maintenance subcommand"
        )

        # maintenance run subcommand
        run_parser = subparsers.add_parser("run", help="Run maintenance tasks")
        run_parser.add_argument(
            "--task",
            action="append",
            dest="tasks",
            help="Run a specific task (can be specified multiple times)",
        )
        run_parser.add_argument(
            "--auto",
            action="store_true",
            help="Only run tasks if needed",
        )
        run_parser.add_argument(
            "--quiet",
            "-q",
            action="store_true",
            help="Only report errors",
        )

        # maintenance start subcommand (placeholder)
        subparsers.add_parser("start", help="Start background maintenance")

        # maintenance stop subcommand (placeholder)
        subparsers.add_parser("stop", help="Stop background maintenance")

        # maintenance register subcommand
        subparsers.add_parser("register", help="Register repository for maintenance")

        # maintenance unregister subcommand
        unregister_parser = subparsers.add_parser(
            "unregister", help="Unregister repository from maintenance"
        )
        unregister_parser.add_argument(
            "--force",
            action="store_true",
            help="Don't error if repository is not registered",
        )

        parsed_args = parser.parse_args(args)

        if not parsed_args.subcommand:
            parser.print_help()
            return 1

        if parsed_args.subcommand == "run":
            # Progress callback
            def progress(msg: str) -> None:
                if not parsed_args.quiet:
                    logger.info(msg)

            try:
                result = porcelain.maintenance_run(
                    ".",
                    tasks=parsed_args.tasks,
                    auto=parsed_args.auto,
                    progress=progress if not parsed_args.quiet else None,
                )

                # Report results
                if not parsed_args.quiet:
                    if result.tasks_succeeded:
                        logger.info("\nSuccessfully completed tasks:")
                        for task in result.tasks_succeeded:
                            logger.info(f"  - {task}")

                    if result.tasks_failed:
                        logger.error("\nFailed tasks:")
                        for task in result.tasks_failed:
                            error_msg = result.errors.get(task, "Unknown error")
                            logger.error(f"  - {task}: {error_msg}")
                        return 1

            except porcelain.Error as e:
                logger.error("%s", e)
                return 1
        elif parsed_args.subcommand == "register":
            porcelain.maintenance_register(".")
            logger.info("Repository registered for background maintenance")
        elif parsed_args.subcommand == "unregister":
            try:
                force = getattr(parsed_args, "force", False)
                porcelain.maintenance_unregister(".", force=force)
            except ValueError as e:
                logger.error(str(e))
                return 1
            logger.info("Repository unregistered from background maintenance")
        elif parsed_args.subcommand in ("start", "stop"):
            # TODO: Implement background maintenance scheduling
            logger.error(
                f"The '{parsed_args.subcommand}' subcommand is not yet implemented"
            )
            return 1
        else:
            parser.print_help()
            return 1

        return None


class cmd_grep(Command):
    """Search for patterns in tracked files."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the grep command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("pattern", help="Regular expression pattern to search for")
        parser.add_argument(
            "revision",
            nargs="?",
            default=None,
            help="Revision to search (defaults to HEAD)",
        )
        parser.add_argument(
            "pathspecs",
            nargs="*",
            help="Path patterns to limit search",
        )
        parser.add_argument(
            "-i",
            "--ignore-case",
            action="store_true",
            help="Perform case-insensitive matching",
        )
        parser.add_argument(
            "-n",
            "--line-number",
            action="store_true",
            help="Show line numbers for matches",
        )
        parser.add_argument(
            "--max-depth",
            type=int,
            default=None,
            help="Maximum directory depth to search",
        )
        parser.add_argument(
            "--no-ignore",
            action="store_true",
            help="Do not respect .gitignore patterns",
        )
        parsed_args = parser.parse_args(args)

        # Handle the case where revision might be a pathspec
        revision = parsed_args.revision
        pathspecs = parsed_args.pathspecs

        # If revision looks like a pathspec (contains wildcards or slashes),
        # treat it as a pathspec instead
        if revision and ("*" in revision or "/" in revision or "." in revision):
            pathspecs = [revision, *pathspecs]
            revision = None

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="grep") as outstream:
                porcelain.grep(
                    repo,
                    parsed_args.pattern,
                    outstream=outstream,
                    rev=revision,
                    pathspecs=pathspecs if pathspecs else None,
                    ignore_case=parsed_args.ignore_case,
                    line_number=parsed_args.line_number,
                    max_depth=parsed_args.max_depth,
                    respect_ignores=not parsed_args.no_ignore,
                )


class cmd_count_objects(Command):
    """Count unpacked number of objects and their disk consumption."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the count-objects command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Display verbose information.",
        )
        parsed_args = parser.parse_args(args)

        if parsed_args.verbose:
            stats = porcelain.count_objects(".", verbose=True)
            # Display verbose output
            logger.info("count: %d", stats.count)
            logger.info("size: %d", stats.size // 1024)  # Size in KiB
            assert stats.in_pack is not None
            logger.info("in-pack: %d", stats.in_pack)
            assert stats.packs is not None
            logger.info("packs: %d", stats.packs)
            assert stats.size_pack is not None
            logger.info("size-pack: %d", stats.size_pack // 1024)  # Size in KiB
        else:
            # Simple output
            stats = porcelain.count_objects(".", verbose=False)
            logger.info("%d objects, %d kilobytes", stats.count, stats.size // 1024)


class cmd_rebase(Command):
    """Reapply commits on top of another base tip."""

    def run(self, args: Sequence[str]) -> int:
        """Execute the rebase command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "upstream", nargs="?", help="Upstream branch to rebase onto"
        )
        parser.add_argument("--onto", type=str, help="Rebase onto specific commit")
        parser.add_argument(
            "--branch", type=str, help="Branch to rebase (default: current)"
        )
        parser.add_argument(
            "-i", "--interactive", action="store_true", help="Interactive rebase"
        )
        parser.add_argument(
            "--edit-todo",
            action="store_true",
            help="Edit the todo list during an interactive rebase",
        )
        parser.add_argument(
            "--abort", action="store_true", help="Abort an in-progress rebase"
        )
        parser.add_argument(
            "--continue",
            dest="continue_rebase",
            action="store_true",
            help="Continue an in-progress rebase",
        )
        parser.add_argument(
            "--skip", action="store_true", help="Skip current commit and continue"
        )
        parsed_args = parser.parse_args(args)

        # Handle abort/continue/skip first
        if parsed_args.abort:
            try:
                porcelain.rebase(".", parsed_args.upstream or "HEAD", abort=True)
                logger.info("Rebase aborted.")
            except porcelain.Error as e:
                logger.error("%s", e)
                return 1
            return 0

        if parsed_args.continue_rebase:
            try:
                # Check if interactive rebase is in progress
                if porcelain.is_interactive_rebase("."):
                    result = porcelain.rebase(
                        ".",
                        parsed_args.upstream or "HEAD",
                        continue_rebase=True,
                        interactive=True,
                    )
                    if result:
                        logger.info("Rebase complete.")
                    else:
                        logger.info("Rebase paused. Use --continue to resume.")
                else:
                    new_shas = porcelain.rebase(
                        ".", parsed_args.upstream or "HEAD", continue_rebase=True
                    )
                    logger.info("Rebase complete.")
            except porcelain.Error as e:
                logger.error("%s", e)
                return 1
            return 0

        if parsed_args.edit_todo:
            # Edit todo list for interactive rebase
            try:
                porcelain.rebase(".", parsed_args.upstream or "HEAD", edit_todo=True)
                logger.info("Todo list updated.")
            except porcelain.Error as e:
                logger.error("%s", e)
                return 1
            return 0

        # Normal rebase requires upstream
        if not parsed_args.upstream:
            logger.error("Missing required argument 'upstream'")
            return 1

        try:
            if parsed_args.interactive:
                # Interactive rebase
                result = porcelain.rebase(
                    ".",
                    parsed_args.upstream,
                    onto=parsed_args.onto,
                    branch=parsed_args.branch,
                    interactive=True,
                )
                if result:
                    logger.info(
                        "Interactive rebase started. Edit the todo list and save."
                    )
                else:
                    logger.info("No commits to rebase.")
            else:
                # Regular rebase
                new_shas = porcelain.rebase(
                    ".",
                    parsed_args.upstream,
                    onto=parsed_args.onto,
                    branch=parsed_args.branch,
                )

                if new_shas:
                    logger.info("Successfully rebased %d commits.", len(new_shas))
                else:
                    logger.info("Already up to date.")
            return 0

        except porcelain.Error as e:
            logger.error("%s", e)
            return 1


class cmd_filter_branch(Command):
    """Rewrite branches."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the filter-branch command.

        Args:
            args: Command line arguments
        """
        import subprocess

        parser = argparse.ArgumentParser(description="Rewrite branches")

        # Supported Git-compatible options
        parser.add_argument(
            "--subdirectory-filter",
            type=str,
            help="Only include history for subdirectory",
        )
        parser.add_argument("--env-filter", type=str, help="Environment filter command")
        parser.add_argument("--tree-filter", type=str, help="Tree filter command")
        parser.add_argument("--index-filter", type=str, help="Index filter command")
        parser.add_argument("--parent-filter", type=str, help="Parent filter command")
        parser.add_argument("--msg-filter", type=str, help="Message filter command")
        parser.add_argument("--commit-filter", type=str, help="Commit filter command")
        parser.add_argument(
            "--tag-name-filter", type=str, help="Tag name filter command"
        )
        parser.add_argument(
            "--prune-empty", action="store_true", help="Remove empty commits"
        )
        parser.add_argument(
            "--original",
            type=str,
            default="refs/original",
            help="Namespace for original refs",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force operation even if refs/original/* exists",
        )

        # Branch/ref to rewrite (defaults to HEAD)
        parser.add_argument(
            "branch", nargs="?", default="HEAD", help="Branch or ref to rewrite"
        )

        parsed_args = parser.parse_args(args)

        # Track if any filter fails
        filter_error = False

        # Setup environment for filters
        env = os.environ.copy()

        # Helper function to run shell commands
        def run_filter(
            cmd: str,
            input_data: bytes | None = None,
            cwd: str | None = None,
            extra_env: dict[str, str] | None = None,
        ) -> bytes | None:
            nonlocal filter_error
            filter_env = env.copy()
            if extra_env:
                filter_env.update(extra_env)
            result = subprocess.run(
                cmd,
                shell=True,
                input=input_data,
                cwd=cwd,
                env=filter_env,
                capture_output=True,
            )
            if result.returncode != 0:
                filter_error = True
                return None
            return result.stdout

        # Create filter functions based on arguments
        filter_message = None
        if parsed_args.msg_filter:

            def filter_message(message: bytes) -> bytes:
                result = run_filter(parsed_args.msg_filter, input_data=message)
                return result if result is not None else message

        tree_filter = None
        if parsed_args.tree_filter:

            def tree_filter(tree_sha: ObjectID, tmpdir: str) -> ObjectID:
                from dulwich.objects import Blob, Tree

                # Export tree to tmpdir
                with Repo(".") as r:
                    tree = r.object_store[tree_sha]
                    assert isinstance(tree, Tree)
                    for entry in tree.iteritems():
                        assert entry.path is not None
                        assert entry.sha is not None
                        path = Path(tmpdir) / entry.path.decode()
                        obj = r.object_store[entry.sha]
                        if isinstance(obj, Tree):
                            path.mkdir(exist_ok=True)
                        else:
                            assert isinstance(obj, Blob)
                            path.write_bytes(obj.data)

                    # Run the filter command in the temp directory
                    run_filter(parsed_args.tree_filter, cwd=tmpdir)

                    # Rebuild tree from modified temp directory
                    def build_tree_from_dir(dir_path: str) -> ObjectID:
                        tree = Tree()
                        for name in sorted(os.listdir(dir_path)):
                            if name.startswith("."):
                                continue
                            path = os.path.join(dir_path, name)
                            if os.path.isdir(path):
                                subtree_sha = build_tree_from_dir(path)
                                tree.add(name.encode(), 0o040000, subtree_sha)
                            else:
                                with open(path, "rb") as f:
                                    data = f.read()
                                blob = Blob.from_string(data)
                                r.object_store.add_object(blob)
                                # Use appropriate file mode
                                mode = os.stat(path).st_mode
                                if mode & 0o100:
                                    file_mode = 0o100755
                                else:
                                    file_mode = 0o100644
                                tree.add(name.encode(), file_mode, blob.id)
                        r.object_store.add_object(tree)
                        return tree.id

                    return build_tree_from_dir(tmpdir)

        index_filter = None
        if parsed_args.index_filter:

            def index_filter(tree_sha: ObjectID, index_path: str) -> ObjectID | None:
                run_filter(
                    parsed_args.index_filter, extra_env={"GIT_INDEX_FILE": index_path}
                )
                return None  # Read back from index

        parent_filter = None
        if parsed_args.parent_filter:

            def parent_filter(parents: Sequence[ObjectID]) -> list[ObjectID]:
                parent_str = " ".join(p.hex() for p in parents)
                result = run_filter(
                    parsed_args.parent_filter, input_data=parent_str.encode()
                )
                if result is None:
                    return list(parents)

                output = result.decode().strip()
                if not output:
                    return []
                new_parents = []
                for sha in output.split():
                    sha_bytes = sha.encode()
                    if valid_hexsha(sha_bytes):
                        new_parents.append(ObjectID(sha_bytes))
                return new_parents

        commit_filter = None
        if parsed_args.commit_filter:

            def commit_filter(
                commit_obj: Commit, tree_sha: ObjectID
            ) -> ObjectID | None:
                # The filter receives: tree parent1 parent2...
                cmd_input = tree_sha.hex()
                for parent in commit_obj.parents:
                    cmd_input += " " + parent.hex()

                result = run_filter(
                    parsed_args.commit_filter,
                    input_data=cmd_input.encode(),
                    extra_env={"GIT_COMMIT": commit_obj.id.hex()},
                )
                if result is None:
                    return None

                output = result.decode().strip()
                if not output:
                    return None  # Skip commit

                if valid_hexsha(output):
                    return ObjectID(output.encode())
                return None

        tag_name_filter = None
        if parsed_args.tag_name_filter:

            def tag_name_filter(tag_name: bytes) -> bytes:
                result = run_filter(parsed_args.tag_name_filter, input_data=tag_name)
                return result if result is not None else tag_name

        # Open repo once
        with Repo(".") as r:
            # Check for refs/original if not forcing
            if not parsed_args.force:
                original_prefix = parsed_args.original.encode() + b"/"
                for ref in r.refs.allkeys():
                    if ref.startswith(original_prefix):
                        logger.error("Cannot create a new backup.")
                        logger.error(
                            "A previous backup already exists in %s/",
                            parsed_args.original,
                        )
                        logger.error("Force overwriting the backup with -f")
                        print("Cannot create a new backup.")
                        print(
                            f"A previous backup already exists in {parsed_args.original}/"
                        )
                        print("Force overwriting the backup with -f")
                        return 1

            try:
                # Call porcelain.filter_branch with the repo object
                result = porcelain.filter_branch(
                    r,
                    parsed_args.branch,
                    filter_message=filter_message,
                    tree_filter=tree_filter if parsed_args.tree_filter else None,
                    index_filter=index_filter if parsed_args.index_filter else None,
                    parent_filter=parent_filter if parsed_args.parent_filter else None,
                    commit_filter=commit_filter if parsed_args.commit_filter else None,
                    subdirectory_filter=parsed_args.subdirectory_filter,
                    prune_empty=parsed_args.prune_empty,
                    tag_name_filter=tag_name_filter
                    if parsed_args.tag_name_filter
                    else None,
                    force=parsed_args.force,
                    keep_original=True,  # Always keep original with git
                )

                # Check if any filter failed
                if filter_error:
                    logger.error("Filter command failed")
                    return 1

                # Git filter-branch shows progress
                if result:
                    logger.info(
                        "Rewrite %s (%d commits)", parsed_args.branch, len(result)
                    )
                    # Git shows: Ref 'refs/heads/branch' was rewritten
                    if parsed_args.branch != "HEAD":
                        ref_name = (
                            parsed_args.branch
                            if parsed_args.branch.startswith("refs/")
                            else f"refs/heads/{parsed_args.branch}"
                        )
                        logger.info("Ref '%s' was rewritten", ref_name)

                return 0

            except porcelain.Error as e:
                logger.error("%s", e)
                return 1


class cmd_lfs(Command):
    """Git Large File Storage management."""

    """Git LFS management commands."""

    def run(self, argv: Sequence[str]) -> None:
        """Execute the lfs command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser(prog="dulwich lfs")
        subparsers = parser.add_subparsers(dest="subcommand", help="LFS subcommands")

        # lfs init
        subparsers.add_parser("init", help="Initialize Git LFS")

        # lfs track
        parser_track = subparsers.add_parser(
            "track", help="Track file patterns with LFS"
        )
        parser_track.add_argument("patterns", nargs="*", help="File patterns to track")

        # lfs untrack
        parser_untrack = subparsers.add_parser(
            "untrack", help="Untrack file patterns from LFS"
        )
        parser_untrack.add_argument(
            "patterns", nargs="+", help="File patterns to untrack"
        )

        # lfs ls-files
        parser_ls = subparsers.add_parser("ls-files", help="List LFS files")
        parser_ls.add_argument("--ref", help="Git ref to check (defaults to HEAD)")

        # lfs migrate
        parser_migrate = subparsers.add_parser("migrate", help="Migrate files to LFS")
        parser_migrate.add_argument("--include", nargs="+", help="Patterns to include")
        parser_migrate.add_argument("--exclude", nargs="+", help="Patterns to exclude")
        parser_migrate.add_argument(
            "--everything", action="store_true", help="Migrate all files above 100MB"
        )

        # lfs pointer
        parser_pointer = subparsers.add_parser("pointer", help="Check LFS pointers")
        parser_pointer.add_argument(
            "--check", nargs="*", dest="paths", help="Check if files are LFS pointers"
        )

        # lfs clean
        parser_clean = subparsers.add_parser("clean", help="Clean file to LFS pointer")
        parser_clean.add_argument("path", help="File path to clean")

        # lfs smudge
        parser_smudge = subparsers.add_parser(
            "smudge", help="Smudge LFS pointer to content"
        )
        parser_smudge.add_argument(
            "--stdin", action="store_true", help="Read pointer from stdin"
        )

        # lfs fetch
        parser_fetch = subparsers.add_parser(
            "fetch", help="Fetch LFS objects from remote"
        )
        parser_fetch.add_argument(
            "--remote", default="origin", help="Remote to fetch from"
        )
        parser_fetch.add_argument("refs", nargs="*", help="Specific refs to fetch")

        # lfs pull
        parser_pull = subparsers.add_parser(
            "pull", help="Pull LFS objects for current checkout"
        )
        parser_pull.add_argument(
            "--remote", default="origin", help="Remote to pull from"
        )

        # lfs push
        parser_push = subparsers.add_parser("push", help="Push LFS objects to remote")
        parser_push.add_argument("--remote", default="origin", help="Remote to push to")
        parser_push.add_argument("refs", nargs="*", help="Specific refs to push")

        # lfs status
        subparsers.add_parser("status", help="Show status of LFS files")

        args = parser.parse_args(argv)

        if args.subcommand == "init":
            porcelain.lfs_init()
            logger.info("Git LFS initialized.")

        elif args.subcommand == "track":
            if args.patterns:
                tracked = porcelain.lfs_track(patterns=args.patterns)
                logger.info("Tracking patterns:")
            else:
                tracked = porcelain.lfs_track()
                logger.info("Currently tracked patterns:")
            for pattern in tracked:
                logger.info("  %s", pattern)

        elif args.subcommand == "untrack":
            tracked = porcelain.lfs_untrack(patterns=args.patterns)
            logger.info("Remaining tracked patterns:")
            for pattern in tracked:
                logger.info("  %s", to_display_str(pattern))

        elif args.subcommand == "ls-files":
            files = porcelain.lfs_ls_files(ref=args.ref)
            for path, oid, size in files:
                logger.info(
                    "%s * %s (%s)",
                    to_display_str(oid[:12]),
                    to_display_str(path),
                    format_bytes(size),
                )

        elif args.subcommand == "migrate":
            count = porcelain.lfs_migrate(
                include=args.include, exclude=args.exclude, everything=args.everything
            )
            logger.info("Migrated %d file(s) to Git LFS.", count)

        elif args.subcommand == "pointer":
            if args.paths is not None:
                results = porcelain.lfs_pointer_check(paths=args.paths or None)
                for file_path, pointer in results.items():
                    if pointer:
                        logger.info(
                            "%s: LFS pointer (oid: %s, size: %s)",
                            to_display_str(file_path),
                            to_display_str(pointer.oid[:12]),
                            format_bytes(pointer.size),
                        )
                    else:
                        logger.warning(
                            "%s: Not an LFS pointer", to_display_str(file_path)
                        )

        elif args.subcommand == "clean":
            pointer = porcelain.lfs_clean(path=args.path)
            sys.stdout.buffer.write(pointer)

        elif args.subcommand == "smudge":
            if args.stdin:
                pointer_content = sys.stdin.buffer.read()
                content = porcelain.lfs_smudge(pointer_content=pointer_content)
                sys.stdout.buffer.write(content)
            else:
                logger.error("--stdin required for smudge command")
                sys.exit(1)

        elif args.subcommand == "fetch":
            refs = args.refs or None
            count = porcelain.lfs_fetch(remote=args.remote, refs=refs)
            logger.info("Fetched %d LFS object(s).", count)

        elif args.subcommand == "pull":
            count = porcelain.lfs_pull(remote=args.remote)
            logger.info("Pulled %d LFS object(s).", count)

        elif args.subcommand == "push":
            refs = args.refs or None
            count = porcelain.lfs_push(remote=args.remote, refs=refs)
            logger.info("Pushed %d LFS object(s).", count)

        elif args.subcommand == "status":
            status = porcelain.lfs_status()

            if status["tracked"]:
                logger.info("LFS tracked files: %d", len(status["tracked"]))

            if status["missing"]:
                logger.warning("\nMissing LFS objects:")
                for file_path in status["missing"]:
                    logger.warning("  %s", to_display_str(file_path))

            if status["not_staged"]:
                logger.info("\nModified LFS files not staged:")
                for file_path in status["not_staged"]:
                    logger.warning("  %s", to_display_str(file_path))

            if not any(status.values()):
                logger.info("No LFS files found.")

        else:
            parser.print_help()
            sys.exit(1)


class cmd_help(Command):
    """Display help information about git."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the help command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            help="List all commands.",
        )
        parsed_args = parser.parse_args(args)

        if parsed_args.all:
            logger.info("Available commands:")
            for cmd in sorted(commands):
                logger.info("  %s", cmd)
        else:
            logger.info(
                "The dulwich command line tool is currently a very basic frontend for the\n"
                "Dulwich python module. For full functionality, please see the API reference.\n"
                "\n"
                "For a list of supported commands, see 'dulwich help -a'."
            )


class cmd_format_patch(Command):
    """Prepare patches for e-mail submission."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the format-patch command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "committish",
            nargs="?",
            help="Commit or commit range (e.g., HEAD~3..HEAD or origin/master..HEAD)",
        )
        parser.add_argument(
            "-n",
            "--numbered",
            type=int,
            default=1,
            help="Number of commits to format (default: 1)",
        )
        parser.add_argument(
            "-o",
            "--output-directory",
            dest="outdir",
            help="Output directory for patches",
        )
        parser.add_argument(
            "--stdout",
            action="store_true",
            help="Output patches to stdout",
        )
        parsed_args = parser.parse_args(args)

        # Parse committish using the new function
        committish: ObjectID | tuple[ObjectID, ObjectID] | None = None
        if parsed_args.committish:
            with Repo(".") as r:
                range_result = parse_commit_range(r, parsed_args.committish)
                if range_result:
                    # Convert Commit objects to their SHAs
                    committish = (range_result[0].id, range_result[1].id)
                else:
                    committish = ObjectID(
                        parsed_args.committish.encode()
                        if isinstance(parsed_args.committish, str)
                        else parsed_args.committish
                    )

        filenames = porcelain.format_patch(
            ".",
            committish=committish,
            outstream=sys.stdout,
            outdir=parsed_args.outdir,
            n=parsed_args.numbered,
            stdout=parsed_args.stdout,
        )

        if not parsed_args.stdout:
            for filename in filenames:
                logger.info(filename)


class cmd_mailsplit(Command):
    """Split mbox or Maildir into individual message files."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the mailsplit command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "mbox",
            nargs="?",
            help="Path to mbox file or Maildir. If not specified, reads from stdin.",
        )
        parser.add_argument(
            "-o",
            "--output-directory",
            dest="output_dir",
            required=True,
            help="Directory in which to place the individual messages",
        )
        parser.add_argument(
            "-b",
            action="store_true",
            dest="single_mail",
            help="If any file doesn't begin with a From line, assume it is a single mail message",
        )
        parser.add_argument(
            "-d",
            dest="precision",
            type=int,
            default=4,
            help="Number of digits for generated filenames (default: 4)",
        )
        parser.add_argument(
            "-f",
            dest="start_number",
            type=int,
            default=1,
            help="Skip the first <nn> numbers (default: 1)",
        )
        parser.add_argument(
            "--keep-cr",
            action="store_true",
            help="Do not remove \\r from lines ending with \\r\\n",
        )
        parser.add_argument(
            "--mboxrd",
            action="store_true",
            help='Input is of the "mboxrd" format and "^>+From " line escaping is reversed',
        )
        parsed_args = parser.parse_args(args)

        # Determine if input is a Maildir
        is_maildir = False
        if parsed_args.mbox:
            input_path = Path(parsed_args.mbox)
            if input_path.is_dir():
                # Check if it's a Maildir (has cur, tmp, new subdirectories)
                if (
                    (input_path / "cur").exists()
                    and (input_path / "tmp").exists()
                    and (input_path / "new").exists()
                ):
                    is_maildir = True
        else:
            input_path = None

        # Call porcelain function
        output_files = porcelain.mailsplit(
            input_path=input_path,
            output_dir=parsed_args.output_dir,
            start_number=parsed_args.start_number,
            precision=parsed_args.precision,
            keep_cr=parsed_args.keep_cr,
            mboxrd=parsed_args.mboxrd,
            is_maildir=is_maildir,
        )

        # Print information about the split
        logger.info(
            "Split %d messages into %s", len(output_files), parsed_args.output_dir
        )


class cmd_mailinfo(Command):
    """Extract patch information from an email message."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the mailinfo command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "msg",
            help="File to write commit message",
        )
        parser.add_argument(
            "patch",
            help="File to write patch content",
        )
        parser.add_argument(
            "mail",
            nargs="?",
            help="Path to email file. If not specified, reads from stdin.",
        )
        parser.add_argument(
            "-k",
            action="store_true",
            dest="keep_subject",
            help="Pass -k flag to git mailinfo (keeps [PATCH] and other subject tags)",
        )
        parser.add_argument(
            "-b",
            action="store_true",
            dest="keep_non_patch",
            help="Pass -b flag to git mailinfo (only strip [PATCH] tags)",
        )
        parser.add_argument(
            "--encoding",
            dest="encoding",
            help="Character encoding to use (default: detect from message)",
        )
        parser.add_argument(
            "--scissors",
            action="store_true",
            help="Remove everything before scissors line",
        )
        parser.add_argument(
            "-m",
            "--message-id",
            action="store_true",
            dest="message_id",
            help="Copy Message-ID to the end of the commit message",
        )
        parsed_args = parser.parse_args(args)

        # Call porcelain function
        result = porcelain.mailinfo(
            input_path=parsed_args.mail,
            msg_file=parsed_args.msg,
            patch_file=parsed_args.patch,
            keep_subject=parsed_args.keep_subject,
            keep_non_patch=parsed_args.keep_non_patch,
            encoding=parsed_args.encoding,
            scissors=parsed_args.scissors,
            message_id=parsed_args.message_id,
        )

        # Print author info to stdout (as git mailinfo does)
        print(f"Author: {result.author_name}")
        print(f"Email: {result.author_email}")
        print(f"Subject: {result.subject}")
        if result.author_date:
            print(f"Date: {result.author_date}")


class cmd_bundle(Command):
    """Create, unpack, and manipulate bundle files."""

    def run(self, args: Sequence[str]) -> int:
        """Execute the bundle command.

        Args:
            args: Command line arguments
        """
        if not args:
            logger.error("Usage: bundle <create|verify|list-heads|unbundle> <options>")
            return 1

        subcommand = args[0]
        subargs = args[1:]

        if subcommand == "create":
            return self._create(subargs)
        elif subcommand == "verify":
            return self._verify(subargs)
        elif subcommand == "list-heads":
            return self._list_heads(subargs)
        elif subcommand == "unbundle":
            return self._unbundle(subargs)
        else:
            logger.error("Unknown bundle subcommand: %s", subcommand)
            return 1

    def _create(self, args: Sequence[str]) -> int:
        parser = argparse.ArgumentParser(prog="bundle create")
        parser.add_argument(
            "-q", "--quiet", action="store_true", help="Suppress progress"
        )
        parser.add_argument("--progress", action="store_true", help="Show progress")
        parser.add_argument(
            "--version", type=int, choices=[2, 3], help="Bundle version"
        )
        parser.add_argument("--all", action="store_true", help="Include all refs")
        parser.add_argument("--stdin", action="store_true", help="Read refs from stdin")
        parser.add_argument("file", help="Output bundle file (use - for stdout)")
        parser.add_argument("refs", nargs="*", help="References or rev-list args")

        parsed_args = parser.parse_args(args)

        repo = Repo(".")

        progress = None
        if parsed_args.progress and not parsed_args.quiet:

            def progress(*args: str | int) -> None:
                # Handle both progress(msg) and progress(count, msg) signatures
                if len(args) == 1:
                    msg = args[0]
                elif len(args) == 2:
                    _count, msg = args
                else:
                    msg = str(args)
                # Convert bytes to string if needed
                if isinstance(msg, bytes):
                    msg = msg.decode("utf-8", "replace")
                logger.error("%s", msg)

        refs_to_include: list[Ref] = []
        prerequisites = []

        if parsed_args.all:
            refs_to_include = list(repo.refs.keys())
        elif parsed_args.stdin:
            for line in sys.stdin:
                ref = line.strip().encode("utf-8")
                if ref:
                    refs_to_include.append(Ref(ref))
        elif parsed_args.refs:
            for ref_arg in parsed_args.refs:
                if ".." in ref_arg:
                    range_result = parse_commit_range(repo, ref_arg)
                    if range_result:
                        start_commit, _end_commit = range_result
                        prerequisites.append(start_commit.id)
                        # For ranges like A..B, we need to include B if it's a ref
                        # Split the range to get the end part
                        end_part = ref_arg.split("..")[1]
                        if end_part:  # Not empty (not "A..")
                            end_ref = Ref(end_part.encode("utf-8"))
                            if end_ref in repo.refs:
                                refs_to_include.append(end_ref)
                    else:
                        sha = repo.refs[Ref(ref_arg.encode("utf-8"))]
                        refs_to_include.append(Ref(ref_arg.encode("utf-8")))
                else:
                    if ref_arg.startswith("^"):
                        sha = repo.refs[Ref(ref_arg[1:].encode("utf-8"))]
                        prerequisites.append(sha)
                    else:
                        sha = repo.refs[Ref(ref_arg.encode("utf-8"))]
                        refs_to_include.append(Ref(ref_arg.encode("utf-8")))
        else:
            logger.error("No refs specified. Use --all, --stdin, or specify refs")
            return 1

        if not refs_to_include:
            logger.error("fatal: Refusing to create empty bundle.")
            return 1

        bundle = create_bundle_from_repo(
            repo,
            refs=refs_to_include,
            prerequisites=prerequisites,
            version=parsed_args.version,
            progress=progress,
        )

        if parsed_args.file == "-":
            write_bundle(sys.stdout.buffer, bundle)
        else:
            with open(parsed_args.file, "wb") as f:
                write_bundle(f, bundle)

        return 0

    def _verify(self, args: Sequence[str]) -> int:
        parser = argparse.ArgumentParser(prog="bundle verify")
        parser.add_argument(
            "-q", "--quiet", action="store_true", help="Suppress output"
        )
        parser.add_argument("file", help="Bundle file to verify (use - for stdin)")

        parsed_args = parser.parse_args(args)

        repo = Repo(".")

        def verify_bundle(bundle: Bundle) -> int:
            missing_prereqs = []
            for prereq_sha, comment in bundle.prerequisites:
                try:
                    repo.object_store[prereq_sha]
                except KeyError:
                    missing_prereqs.append(prereq_sha)

            if missing_prereqs:
                if not parsed_args.quiet:
                    logger.info("The bundle requires these prerequisite commits:")
                    for sha in missing_prereqs:
                        logger.info("  %s", sha.decode())
                return 1
            else:
                if not parsed_args.quiet:
                    logger.info(
                        "The bundle is valid and can be applied to the current repository"
                    )
                return 0

        if parsed_args.file == "-":
            bundle = read_bundle(sys.stdin.buffer)
            return verify_bundle(bundle)
        else:
            with open(parsed_args.file, "rb") as f:
                bundle = read_bundle(f)
                return verify_bundle(bundle)

    def _list_heads(self, args: Sequence[str]) -> int:
        parser = argparse.ArgumentParser(prog="bundle list-heads")
        parser.add_argument("file", help="Bundle file (use - for stdin)")
        parser.add_argument("refnames", nargs="*", help="Only show these refs")

        parsed_args = parser.parse_args(args)

        def list_heads(bundle: Bundle) -> None:
            for ref, sha in bundle.references.items():
                if not parsed_args.refnames or ref.decode() in parsed_args.refnames:
                    logger.info("%s %s", sha.decode(), ref.decode())

        if parsed_args.file == "-":
            bundle = read_bundle(sys.stdin.buffer)
            list_heads(bundle)
        else:
            with open(parsed_args.file, "rb") as f:
                bundle = read_bundle(f)
                list_heads(bundle)

        return 0

    def _unbundle(self, args: Sequence[str]) -> int:
        parser = argparse.ArgumentParser(prog="bundle unbundle")
        parser.add_argument("--progress", action="store_true", help="Show progress")
        parser.add_argument("file", help="Bundle file (use - for stdin)")
        parser.add_argument("refnames", nargs="*", help="Only unbundle these refs")

        parsed_args = parser.parse_args(args)

        repo = Repo(".")

        progress = None
        if parsed_args.progress:

            def progress(*args: str | int | bytes) -> None:
                # Handle both progress(msg) and progress(count, msg) signatures
                if len(args) == 1:
                    msg = args[0]
                elif len(args) == 2:
                    _count, msg = args
                else:
                    msg = str(args)
                # Convert bytes to string if needed
                if isinstance(msg, bytes):
                    msg = msg.decode("utf-8", "replace")
                elif not isinstance(msg, str):
                    msg = str(msg)
                logger.error("%s", msg)

        if parsed_args.file == "-":
            bundle = read_bundle(sys.stdin.buffer)
            # Process the bundle while file is still available via stdin
            bundle.store_objects(repo.object_store, progress=progress)
        else:
            # Keep the file open during bundle processing
            with open(parsed_args.file, "rb") as f:
                bundle = read_bundle(f)
                # Process pack data while file is still open
                bundle.store_objects(repo.object_store, progress=progress)

        for ref, sha in bundle.references.items():
            if not parsed_args.refnames or ref.decode() in parsed_args.refnames:
                logger.info(ref.decode())

        return 0


class cmd_worktree_add(Command):
    """Create a new worktree."""

    """Add a new worktree to the repository."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-add command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Add a new worktree", prog="dulwich worktree add"
        )
        parser.add_argument("path", help="Path for the new worktree")
        parser.add_argument("committish", nargs="?", help="Commit-ish to checkout")
        parser.add_argument("-b", "--create-branch", help="Create a new branch")
        parser.add_argument(
            "-B", "--force-create-branch", help="Create or reset a branch"
        )
        parser.add_argument(
            "--detach", action="store_true", help="Detach HEAD in new worktree"
        )
        parser.add_argument("--force", action="store_true", help="Force creation")

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        branch = None
        commit = None

        if parsed_args.create_branch or parsed_args.force_create_branch:
            branch = (
                parsed_args.create_branch or parsed_args.force_create_branch
            ).encode()
        elif parsed_args.committish and not parsed_args.detach:
            # If committish is provided and not detaching, treat as branch
            branch = parsed_args.committish.encode()
        elif parsed_args.committish:
            # If committish is provided and detaching, treat as commit
            commit = parsed_args.committish.encode()

        worktree_path = porcelain.worktree_add(
            repo=".",
            path=parsed_args.path,
            branch=branch,
            commit=commit,
            detach=parsed_args.detach,
            force=parsed_args.force or bool(parsed_args.force_create_branch),
        )
        logger.info("Worktree added: %s", worktree_path)
        return 0


class cmd_worktree_list(Command):
    """List worktrees."""

    """List details of each worktree."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-list command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="List worktrees", prog="dulwich worktree list"
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="Show additional information"
        )
        parser.add_argument(
            "--porcelain", action="store_true", help="Machine-readable output"
        )

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        worktrees = porcelain.worktree_list(repo=".")

        for wt in worktrees:
            path = wt.path
            if wt.bare:
                status = "(bare)"
            elif wt.detached:
                status = (
                    f"(detached HEAD {wt.head[:7].decode() if wt.head else 'unknown'})"
                )
            elif wt.branch:
                branch_name = wt.branch.decode().replace("refs/heads/", "")
                status = f"[{branch_name}]"
            else:
                status = "(unknown)"

            if parsed_args.porcelain:
                locked = "locked" if wt.locked else "unlocked"
                prunable = "prunable" if wt.prunable else "unprunable"
                logger.info(
                    "%s %s %s %s %s",
                    path,
                    wt.head.decode() if wt.head else "unknown",
                    status,
                    locked,
                    prunable,
                )
            else:
                line = f"{path}  {status}"
                if wt.locked:
                    line += " locked"
                if wt.prunable:
                    line += " prunable"
                logger.info(line)
        return 0


class cmd_worktree_remove(Command):
    """Remove a worktree."""

    """Remove a worktree."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-remove command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Remove a worktree", prog="dulwich worktree remove"
        )
        parser.add_argument("worktree", help="Path to worktree to remove")
        parser.add_argument("--force", action="store_true", help="Force removal")

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        porcelain.worktree_remove(
            repo=".", path=parsed_args.worktree, force=parsed_args.force
        )
        logger.info("Worktree removed: %s", parsed_args.worktree)
        return 0


class cmd_worktree_prune(Command):
    """Prune worktree information."""

    """Prune worktree information."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-prune command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Prune worktree information", prog="dulwich worktree prune"
        )
        parser.add_argument(
            "--dry-run", action="store_true", help="Do not remove anything"
        )
        parser.add_argument(
            "-v", "--verbose", action="store_true", help="Report all removals"
        )
        parser.add_argument(
            "--expire", type=int, help="Expire worktrees older than time (seconds)"
        )

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        pruned = porcelain.worktree_prune(
            repo=".", dry_run=parsed_args.dry_run, expire=parsed_args.expire
        )

        if pruned:
            if parsed_args.dry_run:
                logger.info("Would prune worktrees:")
            elif parsed_args.verbose:
                logger.info("Pruned worktrees:")

            for wt_id in pruned:
                logger.info("  %s", wt_id)
        elif parsed_args.verbose:
            logger.info("No worktrees to prune")
        return 0


class cmd_worktree_lock(Command):
    """Lock a worktree to prevent it from being pruned."""

    """Lock a worktree."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-lock command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Lock a worktree", prog="dulwich worktree lock"
        )
        parser.add_argument("worktree", help="Path to worktree to lock")
        parser.add_argument("--reason", help="Reason for locking")

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        porcelain.worktree_lock(
            repo=".", path=parsed_args.worktree, reason=parsed_args.reason
        )
        logger.info("Worktree locked: %s", parsed_args.worktree)
        return 0


class cmd_worktree_unlock(Command):
    """Unlock a locked worktree."""

    """Unlock a worktree."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-unlock command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Unlock a worktree", prog="dulwich worktree unlock"
        )
        parser.add_argument("worktree", help="Path to worktree to unlock")

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        porcelain.worktree_unlock(repo=".", path=parsed_args.worktree)
        logger.info("Worktree unlocked: %s", parsed_args.worktree)
        return 0


class cmd_worktree_move(Command):
    """Move a worktree to a new location."""

    """Move a worktree."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-move command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Move a worktree", prog="dulwich worktree move"
        )
        parser.add_argument("worktree", help="Path to worktree to move")
        parser.add_argument("new_path", help="New path for the worktree")

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        porcelain.worktree_move(
            repo=".", old_path=parsed_args.worktree, new_path=parsed_args.new_path
        )
        logger.info(
            "Worktree moved: %s -> %s", parsed_args.worktree, parsed_args.new_path
        )
        return 0


class cmd_worktree_repair(Command):
    """Repair worktree administrative files."""

    """Repair worktree administrative files."""

    def run(self, args: Sequence[str]) -> int | None:
        """Execute the worktree-repair command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser(
            description="Repair worktree administrative files",
            prog="dulwich worktree repair",
        )
        parser.add_argument(
            "path",
            nargs="*",
            help="Paths to worktrees to repair (if not specified, repairs all)",
        )

        parsed_args = parser.parse_args(args)

        from dulwich import porcelain

        paths = parsed_args.path if parsed_args.path else None
        repaired = porcelain.worktree_repair(repo=".", paths=paths)

        if repaired:
            for path in repaired:
                logger.info("Repaired worktree: %s", path)
        else:
            logger.info("No worktrees needed repair")

        return 0


class cmd_worktree(SuperCommand):
    """Manage multiple working trees."""

    """Manage multiple working trees."""

    subcommands: ClassVar[dict[str, type[Command]]] = {
        "add": cmd_worktree_add,
        "list": cmd_worktree_list,
        "remove": cmd_worktree_remove,
        "prune": cmd_worktree_prune,
        "lock": cmd_worktree_lock,
        "unlock": cmd_worktree_unlock,
        "move": cmd_worktree_move,
        "repair": cmd_worktree_repair,
    }
    default_command = cmd_worktree_list


class cmd_rerere(Command):
    """Record and reuse recorded conflict resolutions."""

    def run(self, args: Sequence[str]) -> None:
        """Execute the rerere command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        parser.add_argument(
            "subcommand",
            nargs="?",
            default=None,
            choices=["status", "diff", "forget", "clear", "gc"],
            help="Subcommand to execute (default: record conflicts)",
        )
        parser.add_argument(
            "pathspec", nargs="?", help="Path specification (for forget subcommand)"
        )
        parser.add_argument(
            "--max-age-days",
            type=int,
            default=60,
            help="Maximum age in days for gc (default: 60)",
        )
        parsed_args = parser.parse_args(args)

        if parsed_args.subcommand is None:
            # Record current conflicts
            recorded, resolved = porcelain.rerere(parsed_args.gitdir)
            if not recorded:
                sys.stdout.write("No conflicts to record.\n")
            else:
                for path, conflict_id in recorded:
                    sys.stdout.write(
                        f"Recorded resolution for {path.decode('utf-8')}: {conflict_id}\n"
                    )
                if resolved:
                    sys.stdout.write("\nAutomatically resolved:\n")
                    for path in resolved:
                        sys.stdout.write(f"  {path.decode('utf-8')}\n")

        elif parsed_args.subcommand == "status":
            status_list = porcelain.rerere_status(parsed_args.gitdir)
            if not status_list:
                sys.stdout.write("No recorded resolutions.\n")
            else:
                for conflict_id, has_resolution in status_list:
                    status = "resolved" if has_resolution else "unresolved"
                    sys.stdout.write(f"{conflict_id}\t{status}\n")

        elif parsed_args.subcommand == "diff":
            diff_list = porcelain.rerere_diff(parsed_args.gitdir)
            if not diff_list:
                sys.stdout.write("No recorded conflicts.\n")
            else:
                for conflict_id, preimage, postimage in diff_list:
                    sys.stdout.write(f"--- {conflict_id} (preimage)\n")
                    sys.stdout.buffer.write(preimage)
                    sys.stdout.write("\n")
                    if postimage:
                        sys.stdout.write(f"+++ {conflict_id} (postimage)\n")
                        sys.stdout.buffer.write(postimage)
                        sys.stdout.write("\n")

        elif parsed_args.subcommand == "forget":
            porcelain.rerere_forget(parsed_args.gitdir, parsed_args.pathspec)
            if parsed_args.pathspec:
                sys.stdout.write(f"Forgot resolution for {parsed_args.pathspec}\n")
            else:
                sys.stdout.write("Forgot all resolutions\n")

        elif parsed_args.subcommand == "clear":
            porcelain.rerere_clear(parsed_args.gitdir)
            sys.stdout.write("Cleared all rerere resolutions\n")

        elif parsed_args.subcommand == "gc":
            porcelain.rerere_gc(parsed_args.gitdir, parsed_args.max_age_days)
            sys.stdout.write(
                f"Cleaned up resolutions older than {parsed_args.max_age_days} days\n"
            )


commands = {
    "add": cmd_add,
    "annotate": cmd_annotate,
    "archive": cmd_archive,
    "bisect": cmd_bisect,
    "blame": cmd_blame,
    "branch": cmd_branch,
    "bundle": cmd_bundle,
    "check-ignore": cmd_check_ignore,
    "check-mailmap": cmd_check_mailmap,
    "checkout": cmd_checkout,
    "cherry": cmd_cherry,
    "cherry-pick": cmd_cherry_pick,
    "clone": cmd_clone,
    "column": cmd_column,
    "commit": cmd_commit,
    "commit-tree": cmd_commit_tree,
    "config": cmd_config,
    "count-objects": cmd_count_objects,
    "describe": cmd_describe,
    "diagnose": cmd_diagnose,
    "daemon": cmd_daemon,
    "diff": cmd_diff,
    "diff-tree": cmd_diff_tree,
    "dump-pack": cmd_dump_pack,
    "dump-index": cmd_dump_index,
    "fetch-pack": cmd_fetch_pack,
    "fetch": cmd_fetch,
    "filter-branch": cmd_filter_branch,
    "for-each-ref": cmd_for_each_ref,
    "format-patch": cmd_format_patch,
    "fsck": cmd_fsck,
    "gc": cmd_gc,
    "grep": cmd_grep,
    "help": cmd_help,
    "init": cmd_init,
    "interpret-trailers": cmd_interpret_trailers,
    "lfs": cmd_lfs,
    "log": cmd_log,
    "ls-files": cmd_ls_files,
    "ls-remote": cmd_ls_remote,
    "ls-tree": cmd_ls_tree,
    "maintenance": cmd_maintenance,
    "mailinfo": cmd_mailinfo,
    "mailsplit": cmd_mailsplit,
    "merge": cmd_merge,
    "merge-base": cmd_merge_base,
    "merge-tree": cmd_merge_tree,
    "notes": cmd_notes,
    "pack-objects": cmd_pack_objects,
    "pack-refs": cmd_pack_refs,
    "prune": cmd_prune,
    "pull": cmd_pull,
    "push": cmd_push,
    "rebase": cmd_rebase,
    "receive-pack": cmd_receive_pack,
    "reflog": cmd_reflog,
    "rerere": cmd_rerere,
    "remote": cmd_remote,
    "repack": cmd_repack,
    "replace": cmd_replace,
    "reset": cmd_reset,
    "restore": cmd_restore,
    "revert": cmd_revert,
    "rev-list": cmd_rev_list,
    "rm": cmd_rm,
    "mv": cmd_mv,
    "show": cmd_show,
    "show-branch": cmd_show_branch,
    "show-ref": cmd_show_ref,
    "stash": cmd_stash,
    "status": cmd_status,
    "stripspace": cmd_stripspace,
    "shortlog": cmd_shortlog,
    "switch": cmd_switch,
    "symbolic-ref": cmd_symbolic_ref,
    "submodule": cmd_submodule,
    "tag": cmd_tag,
    "unpack-objects": cmd_unpack_objects,
    "update-server-info": cmd_update_server_info,
    "upload-pack": cmd_upload_pack,
    "var": cmd_var,
    "verify-commit": cmd_verify_commit,
    "verify-tag": cmd_verify_tag,
    "web-daemon": cmd_web_daemon,
    "worktree": cmd_worktree,
    "write-tree": cmd_write_tree,
}


def main(argv: Sequence[str] | None = None) -> int | None:
    """Main entry point for the Dulwich CLI.

    Args:
        argv: Command line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code or None
    """
    # Wrap stdout and stderr to respect GIT_FLUSH environment variable
    sys.stdout = AutoFlushTextIOWrapper.env(sys.stdout)
    sys.stderr = AutoFlushTextIOWrapper.env(sys.stderr)

    if argv is None:
        argv = sys.argv[1:]

    # Parse only the global options and command, stop at first positional
    parser = argparse.ArgumentParser(
        prog="dulwich",
        description="Simple command-line interface to Dulwich",
        add_help=False,  # We'll handle help ourselves
    )
    parser.add_argument("--no-pager", action="store_true", help="Disable pager")
    parser.add_argument("--pager", action="store_true", help="Force enable pager")
    parser.add_argument("--help", "-h", action="store_true", help="Show help")

    # Parse known args to separate global options from command args
    global_args, remaining = parser.parse_known_args(argv)

    # Apply global pager settings
    if global_args.no_pager:
        disable_pager()
    elif global_args.pager:
        enable_pager()

    # Handle help
    if global_args.help or not remaining:
        parser = argparse.ArgumentParser(
            prog="dulwich", description="Simple command-line interface to Dulwich"
        )
        parser.add_argument("--no-pager", action="store_true", help="Disable pager")
        parser.add_argument("--pager", action="store_true", help="Force enable pager")
        parser.add_argument(
            "command",
            nargs="?",
            help=f"Command to run. Available: {', '.join(sorted(commands.keys()))}",
        )
        parser.print_help()
        return 1

    # Try to configure from GIT_TRACE, fall back to default if it fails
    if not _configure_logging_from_trace():
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
        )

    # First remaining arg is the command
    cmd = remaining[0]
    cmd_args = remaining[1:]

    try:
        cmd_kls = commands[cmd]
    except KeyError:
        logging.fatal("No such subcommand: %s", cmd)
        return 1
    # TODO(jelmer): Return non-0 on errors
    return cmd_kls().run(cmd_args)


def _main() -> None:
    if "DULWICH_PDB" in os.environ and getattr(signal, "SIGQUIT", None):
        signal.signal(signal.SIGQUIT, signal_quit)  # type: ignore[attr-defined,unused-ignore]
    signal.signal(signal.SIGINT, signal_int)

    sys.exit(main())


if __name__ == "__main__":
    _main()
