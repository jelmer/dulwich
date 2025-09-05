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

import argparse
import fnmatch
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, Callable, ClassVar, Optional, TextIO, Union

from dulwich import porcelain

from .bundle import create_bundle_from_repo, read_bundle, write_bundle
from .client import GitProtocolError, get_transport_and_path
from .errors import ApplyDeltaError
from .index import Index
from .objects import Commit, valid_hexsha
from .objectspec import parse_commit_range
from .pack import Pack, sha_to_hex
from .patch import DiffAlgorithmNotAvailable
from .repo import Repo

logger = logging.getLogger(__name__)


def to_display_str(value: Union[bytes, str]) -> str:
    """Convert a bytes or string value to a display string.

    Args:
        value: The value to convert (bytes or str)

    Returns:
        A string suitable for display
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


class CommitMessageError(Exception):
    """Raised when there's an issue with the commit message."""


def signal_int(signal: int, frame) -> None:
    """Handle interrupt signal by exiting.

    Args:
        signal: Signal number
        frame: Current stack frame
    """
    sys.exit(1)


def signal_quit(signal: int, frame) -> None:
    """Handle quit signal by entering debugger.

    Args:
        signal: Signal number
        frame: Current stack frame
    """
    import pdb

    pdb.set_trace()


def parse_relative_time(time_str: str) -> int:
    """Parse a relative time string like '2 weeks ago' into seconds.

    Args:
        time_str: String like '2 weeks ago' or 'now'

    Returns:
        Number of seconds

    Raises:
        ValueError: If the time string cannot be parsed
    """
    if time_str == "now":
        return 0

    if not time_str.endswith(" ago"):
        raise ValueError(f"Invalid relative time format: {time_str}")

    parts = time_str[:-4].split()
    if len(parts) != 2:
        raise ValueError(f"Invalid relative time format: {time_str}")

    try:
        num = int(parts[0])
        unit = parts[1]

        multipliers = {
            "second": 1,
            "seconds": 1,
            "minute": 60,
            "minutes": 60,
            "hour": 3600,
            "hours": 3600,
            "day": 86400,
            "days": 86400,
            "week": 604800,
            "weeks": 604800,
        }

        if unit in multipliers:
            return num * multipliers[unit]
        else:
            raise ValueError(f"Unknown time unit: {unit}")
    except ValueError as e:
        if "invalid literal" in str(e):
            raise ValueError(f"Invalid number in relative time: {parts[0]}")
        raise


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
    items: Union[Iterator[bytes], list[bytes]], out: TextIO, width: Optional[int] = None
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

    def columns(names, width, num_cols):
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


class PagerBuffer:
    """Binary buffer wrapper for Pager to mimic sys.stdout.buffer."""

    def __init__(self, pager: "Pager") -> None:
        """Initialize PagerBuffer.

        Args:
            pager: Pager instance to wrap
        """
        self.pager = pager

    def write(self, data: bytes) -> int:
        """Write bytes to pager."""
        if isinstance(data, bytes):
            text = data.decode("utf-8", errors="replace")
            return self.pager.write(text)
        return self.pager.write(data)

    def flush(self) -> None:
        """Flush the pager."""
        return self.pager.flush()

    def writelines(self, lines) -> None:
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


class Pager:
    """File-like object that pages output through external pager programs."""

    def __init__(self, pager_cmd: str = "cat") -> None:
        """Initialize Pager.

        Args:
            pager_cmd: Command to use for paging (default: "cat")
        """
        self.pager_process: Optional[subprocess.Popen] = None
        self.buffer = PagerBuffer(self)
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
                return self.pager_process.stdin.write(text)
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

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    # Additional file-like methods for compatibility
    def writelines(self, lines) -> None:
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


class _StreamContextAdapter:
    """Adapter to make streams work with context manager protocol."""

    def __init__(self, stream) -> None:
        self.stream = stream
        # Expose buffer if it exists
        if hasattr(stream, "buffer"):
            self.buffer = stream.buffer
        else:
            self.buffer = stream

    def __enter__(self):
        return self.stream

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # For stdout/stderr, we don't close them
        pass

    def __getattr__(self, name: str):
        return getattr(self.stream, name)


def get_pager(config=None, cmd_name: Optional[str] = None):
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

    def run(self, args) -> Optional[int]:
        """Run the command."""
        raise NotImplementedError(self.run)


class cmd_archive(Command):
    """Create an archive of files from a named tree."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)
        if args.remote:
            client, path = get_transport_and_path(args.remote)
            client.archive(
                path,
                args.committish,
                sys.stdout.write,
                write_error=sys.stderr.write,
            )
        else:
            # Use binary buffer for archive output
            outstream: BinaryIO = sys.stdout.buffer
            errstream: BinaryIO = sys.stderr.buffer
            porcelain.archive(
                ".",
                args.committish,
                outstream=outstream,
                errstream=errstream,
            )


class cmd_add(Command):
    """Add file contents to the index."""

    def run(self, argv) -> None:
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

    def run(self, argv) -> None:
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

    def run(self, argv) -> None:
        """Execute the blame command.

        Args:
            argv: Command line arguments
        """
        # blame is an alias for annotate
        cmd_annotate().run(argv)


class cmd_rm(Command):
    """Remove files from the working tree and from the index."""

    def run(self, argv) -> None:
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

    def run(self, argv) -> None:
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

    def run(self, argv) -> None:
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

            def determine_wants(refs, depth: Optional[int] = None):
                return [y.encode("utf-8") for y in args.refs if y not in r.object_store]

        client.fetch(path, r, determine_wants)


class cmd_fetch(Command):
    """Download objects and refs from another repository."""

    def run(self, args) -> None:
        """Execute the fetch command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("location", help="Remote location to fetch from")
        args = parser.parse_args(args)
        client, path = get_transport_and_path(args.location)
        r = Repo(".")

        def progress(msg: bytes) -> None:
            sys.stdout.buffer.write(msg)

        result = client.fetch(path, r, progress=progress)
        logger.info("Remote refs:")
        for ref, sha in result.refs.items():
            if sha is not None:
                logger.info("%s → %s", ref.decode(), sha.decode())


class cmd_for_each_ref(Command):
    """Output information on each ref."""

    def run(self, args) -> None:
        """Execute the for-each-ref command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("pattern", type=str, nargs="?")
        args = parser.parse_args(args)
        for sha, object_type, ref in porcelain.for_each_ref(".", args.pattern):
            logger.info("%s %s\t%s", sha.decode(), object_type.decode(), ref.decode())


class cmd_fsck(Command):
    """Verify the connectivity and validity of objects in the database."""

    def run(self, args) -> None:
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

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="log") as outstream:
                porcelain.log(
                    repo,
                    paths=args.paths,
                    reverse=args.reverse,
                    name_status=args.name_status,
                    outstream=outstream,
                )


class cmd_diff(Command):
    """Show changes between commits, commit and working tree, etc."""

    def run(self, args) -> None:
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

        args = parsed_args

        # Determine diff algorithm
        diff_algorithm = args.diff_algorithm
        if args.patience:
            diff_algorithm = "patience"

        # Determine if we should use color
        def _should_use_color():
            if args.color == "always":
                return True
            elif args.color == "never":
                return False
            else:  # auto
                return sys.stdout.isatty()

        def _create_output_stream(outstream):
            """Create output stream, optionally with colorization."""
            if not _should_use_color():
                return outstream.buffer

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
                    return outstream.buffer

            return ColorizedDiffStream(outstream.buffer)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="diff") as outstream:
                output_stream = _create_output_stream(outstream)
                try:
                    if len(args.committish) == 0:
                        # Show diff for working tree or staged changes
                        porcelain.diff(
                            repo,
                            staged=(args.staged or args.cached),
                            paths=args.paths or None,
                            outstream=output_stream,
                            diff_algorithm=diff_algorithm,
                        )
                    elif len(args.committish) == 1:
                        # Show diff between working tree and specified commit
                        if args.staged or args.cached:
                            parser.error(
                                "--staged/--cached cannot be used with commits"
                            )
                        porcelain.diff(
                            repo,
                            commit=args.committish[0],
                            staged=False,
                            paths=args.paths or None,
                            outstream=output_stream,
                            diff_algorithm=diff_algorithm,
                        )
                    elif len(args.committish) == 2:
                        # Show diff between two commits
                        porcelain.diff(
                            repo,
                            commit=args.committish[0],
                            commit2=args.committish[1],
                            paths=args.paths or None,
                            outstream=output_stream,
                            diff_algorithm=diff_algorithm,
                        )
                    else:
                        parser.error("Too many arguments - specify at most two commits")
                except DiffAlgorithmNotAvailable as e:
                    sys.stderr.write(f"fatal: {e}\n")
                    sys.exit(1)

                # Flush any remaining output
                if hasattr(output_stream, "flush"):
                    output_stream.flush()


class cmd_dump_pack(Command):
    """Dump the contents of a pack file for debugging."""

    def run(self, args) -> None:
        """Execute the dump-pack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("filename", help="Pack file to dump")
        args = parser.parse_args(args)

        basename, _ = os.path.splitext(args.filename)
        x = Pack(basename)
        logger.info("Object names checksum: %s", x.name().decode("ascii", "replace"))
        logger.info("Checksum: %r", sha_to_hex(x.get_stored_checksum()))
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

    def run(self, args) -> None:
        """Execute the dump-index command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("filename", help="Index file to dump")
        args = parser.parse_args(args)

        idx = Index(args.filename)

        for o in idx:
            logger.info("%s %s", o, idx[o])


class cmd_init(Command):
    """Create an empty Git repository or reinitialize an existing one."""

    def run(self, args) -> None:
        """Execute the init command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--bare", action="store_true", help="Create a bare repository"
        )
        parser.add_argument(
            "path", nargs="?", default=os.getcwd(), help="Repository path"
        )
        args = parser.parse_args(args)

        porcelain.init(args.path, bare=args.bare)


class cmd_clone(Command):
    """Clone a repository into a new directory."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        try:
            porcelain.clone(
                args.source,
                args.target,
                bare=args.bare,
                depth=args.depth,
                branch=args.branch,
                refspec=args.refspec,
                filter_spec=args.filter_spec,
                protocol_version=args.protocol,
                recurse_submodules=args.recurse_submodules,
            )
        except GitProtocolError as e:
            logging.exception(e)


def _get_commit_message_with_template(initial_message, repo=None, commit=None):
    """Get commit message with an initial message template."""
    # Start with the initial message
    template = initial_message
    if template and not template.endswith(b"\n"):
        template += b"\n"

    template += b"\n"
    template += b"# Please enter the commit message for your changes. Lines starting\n"
    template += b"# with '#' will be ignored, and an empty message aborts the commit.\n"
    template += b"#\n"

    # Add branch info if repo is provided
    if repo:
        try:
            ref_names, ref_sha = repo.refs.follow(b"HEAD")
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


class cmd_commit(Command):
    """Record changes to the repository."""

    def run(self, args) -> Optional[int]:
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
        args = parser.parse_args(args)

        message: Union[bytes, str, Callable]

        if args.message:
            message = args.message
        elif args.amend:
            # For amend, create a callable that opens editor with original message pre-populated
            def get_amend_message(repo, commit):
                # Get the original commit message from current HEAD
                try:
                    head_commit = repo[repo.head()]
                    original_message = head_commit.message
                except KeyError:
                    original_message = b""

                # Open editor with original message
                return _get_commit_message_with_template(original_message, repo, commit)

            message = get_amend_message
        else:
            # For regular commits, use empty template
            def get_regular_message(repo, commit):
                return _get_commit_message_with_template(b"", repo, commit)

            message = get_regular_message

        try:
            porcelain.commit(".", message=message, all=args.all, amend=args.amend)
        except CommitMessageError as e:
            logging.exception(e)
            return 1
        return None


class cmd_commit_tree(Command):
    """Create a new commit object from a tree."""

    def run(self, args) -> None:
        """Execute the commit-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--message", "-m", required=True, help="Commit message")
        parser.add_argument("tree", help="Tree SHA to commit")
        args = parser.parse_args(args)
        porcelain.commit_tree(".", tree=args.tree, message=args.message)


class cmd_update_server_info(Command):
    """Update auxiliary info file to help dumb servers."""

    def run(self, args) -> None:
        """Execute the update-server-info command.

        Args:
            args: Command line arguments
        """
        porcelain.update_server_info(".")


class cmd_symbolic_ref(Command):
    """Read, modify and delete symbolic refs."""

    def run(self, args) -> Optional[int]:
        """Execute the symbolic-ref command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("name", help="Symbolic reference name")
        parser.add_argument("ref", nargs="?", help="Target reference")
        parser.add_argument("--force", action="store_true", help="Force update")
        args = parser.parse_args(args)

        # If ref is provided, we're setting; otherwise we're reading
        if args.ref:
            # Set symbolic reference
            from .repo import Repo

            with Repo(".") as repo:
                repo.refs.set_symbolic_ref(args.name.encode(), args.ref.encode())
            return 0
        else:
            # Read symbolic reference
            from .repo import Repo

            with Repo(".") as repo:
                try:
                    target = repo.refs.read_ref(args.name.encode())
                    if target.startswith(b"ref: "):
                        logger.info(target[5:].decode())
                    else:
                        logger.info(target.decode())
                    return 0
                except KeyError:
                    logging.error("fatal: ref '%s' is not a symbolic ref", args.name)
                    return 1


class cmd_pack_refs(Command):
    """Pack heads and tags for efficient repository access."""

    def run(self, argv) -> None:
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


class cmd_show(Command):
    """Show various types of objects."""

    def run(self, argv) -> None:
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
        def _should_use_color():
            if args.color == "always":
                return True
            elif args.color == "never":
                return False
            else:  # auto
                return sys.stdout.isatty()

        def _create_output_stream(outstream):
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

            return ColorizedDiffStream(outstream.buffer)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="show") as outstream:
                output_stream = _create_output_stream(outstream)
                porcelain.show(repo, args.objectish or None, outstream=output_stream)


class cmd_diff_tree(Command):
    """Compare the content and mode of trees."""

    def run(self, args) -> None:
        """Execute the diff-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("old_tree", help="Old tree SHA")
        parser.add_argument("new_tree", help="New tree SHA")
        args = parser.parse_args(args)
        porcelain.diff_tree(".", args.old_tree, args.new_tree)


class cmd_rev_list(Command):
    """List commit objects in reverse chronological order."""

    def run(self, args) -> None:
        """Execute the rev-list command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("commits", nargs="+", help="Commit IDs to list")
        args = parser.parse_args(args)
        porcelain.rev_list(".", args.commits)


class cmd_tag(Command):
    """Create, list, delete or verify a tag object."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)
        porcelain.tag_create(
            ".", args.tag_name, annotated=args.annotated, sign=args.sign
        )


class cmd_repack(Command):
    """Pack unpacked objects in a repository."""

    def run(self, args) -> None:
        """Execute the repack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        porcelain.repack(".")


class cmd_reflog(Command):
    """Manage reflog information."""

    def run(self, args) -> None:
        """Execute the reflog command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "ref", nargs="?", default="HEAD", help="Reference to show reflog for"
        )
        parser.add_argument(
            "--all", action="store_true", help="Show reflogs for all refs"
        )
        args = parser.parse_args(args)

        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="reflog") as outstream:
                if args.all:
                    # Show reflogs for all refs
                    for ref_bytes, entry in porcelain.reflog(repo, all=True):
                        ref_str = ref_bytes.decode("utf-8", "replace")
                        short_new = entry.new_sha[:8].decode("ascii")
                        outstream.write(
                            f"{short_new} {ref_str}: {entry.message.decode('utf-8', 'replace')}\n"
                        )
                else:
                    ref = (
                        args.ref.encode("utf-8")
                        if isinstance(args.ref, str)
                        else args.ref
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


class cmd_reset(Command):
    """Reset current HEAD to the specified state."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        if args.hard:
            mode = "hard"
        elif args.soft:
            mode = "soft"
        elif args.mixed:
            mode = "mixed"
        else:
            # Default to mixed behavior
            mode = "mixed"

        # Use the porcelain.reset function for all modes
        porcelain.reset(".", mode=mode, treeish=args.treeish)


class cmd_revert(Command):
    """Revert some existing commits."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        result = porcelain.revert(
            ".", commits=args.commits, no_commit=args.no_commit, message=args.message
        )

        if result and not args.no_commit:
            logger.info("[%s] Revert completed", result.decode("ascii")[:7])


class cmd_daemon(Command):
    """Run a simple Git protocol server."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        log_utils.default_logging_config()
        porcelain.daemon(args.gitdir, address=args.listen_address, port=args.port)


class cmd_web_daemon(Command):
    """Run a simple HTTP server for Git repositories."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        log_utils.default_logging_config()
        porcelain.web_daemon(args.gitdir, address=args.listen_address, port=args.port)


class cmd_write_tree(Command):
    """Create a tree object from the current index."""

    def run(self, args) -> None:
        """Execute the write-tree command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        sys.stdout.write("{}\n".format(porcelain.write_tree(".").decode()))


class cmd_receive_pack(Command):
    """Receive what is pushed into the repository."""

    def run(self, args) -> None:
        """Execute the receive-pack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        args = parser.parse_args(args)
        porcelain.receive_pack(args.gitdir)


class cmd_upload_pack(Command):
    """Send objects packed back to git-fetch-pack."""

    def run(self, args) -> None:
        """Execute the upload-pack command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        args = parser.parse_args(args)
        porcelain.upload_pack(args.gitdir)


class cmd_shortlog(Command):
    """Show a shortlog of commits by author."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        shortlog_items: list[dict[str, str]] = porcelain.shortlog(
            repo=args.gitdir,
            summary_only=args.summary,
            sort_by_commits=args.sort,
        )

        for item in shortlog_items:
            author: str = item["author"]
            messages: str = item["messages"]
            if args.summary:
                count = len(messages.splitlines())
                sys.stdout.write(f"{count}\t{author}\n")
            else:
                sys.stdout.write(f"{author} ({len(messages.splitlines())}):\n")
                for msg in messages.splitlines():
                    sys.stdout.write(f"    {msg}\n")
                sys.stdout.write("\n")


class cmd_status(Command):
    """Show the working tree status."""

    def run(self, args) -> None:
        """Execute the status command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("gitdir", nargs="?", default=".", help="Git directory")
        args = parser.parse_args(args)
        status = porcelain.status(args.gitdir)
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
            for name in status.untracked:
                sys.stdout.write(f"\t{name}\n")
            sys.stdout.write("\n")


class cmd_ls_remote(Command):
    """List references in a remote repository."""

    def run(self, args) -> None:
        """Execute the ls-remote command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--symref", action="store_true", help="Show symbolic references"
        )
        parser.add_argument("url", help="Remote URL to list references from")
        args = parser.parse_args(args)
        result = porcelain.ls_remote(args.url)

        if args.symref:
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

    def run(self, args) -> None:
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
        args = parser.parse_args(args)
        with Repo(".") as repo:
            config = repo.get_config_stack()
            with get_pager(config=config, cmd_name="ls-tree") as outstream:
                porcelain.ls_tree(
                    repo,
                    args.treeish,
                    outstream=outstream,
                    recursive=args.recursive,
                    name_only=args.name_only,
                )


class cmd_pack_objects(Command):
    """Create a packed archive of objects."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        if not args.stdout and not args.basename:
            parser.error("basename required when not using --stdout")

        object_ids = [line.strip().encode() for line in sys.stdin.readlines()]
        deltify = args.deltify
        reuse_deltas = not args.no_reuse_deltas

        if args.stdout:
            packf = getattr(sys.stdout, "buffer", sys.stdout)
            assert isinstance(packf, BinaryIO)
            idxf = None
            close = []
        else:
            packf = open(args.basename + ".pack", "wb")
            idxf = open(args.basename + ".idx", "wb")
            close = [packf, idxf]

        porcelain.pack_objects(
            ".", object_ids, packf, idxf, deltify=deltify, reuse_deltas=reuse_deltas
        )
        for f in close:
            f.close()


class cmd_unpack_objects(Command):
    """Unpack objects from a packed archive."""

    def run(self, args) -> None:
        """Execute the unpack-objects command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("pack_file", help="Pack file to unpack")
        args = parser.parse_args(args)

        count = porcelain.unpack_objects(args.pack_file)
        logger.info("Unpacked %d objects", count)


class cmd_prune(Command):
    """Prune all unreachable objects from the object database."""

    def run(self, args) -> Optional[int]:
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
        args = parser.parse_args(args)

        # Parse expire grace period
        grace_period = DEFAULT_TEMPFILE_GRACE_PERIOD
        if args.expire:
            try:
                grace_period = parse_relative_time(args.expire)
            except ValueError:
                # Try to parse as absolute date
                try:
                    date = datetime.datetime.strptime(args.expire, "%Y-%m-%d")
                    grace_period = int(time.time() - date.timestamp())
                except ValueError:
                    logger.error("Invalid expire date: %s", args.expire)
                    return 1

        # Progress callback
        def progress(msg):
            if args.verbose:
                logger.info(msg)

        try:
            porcelain.prune(
                ".",
                grace_period=grace_period,
                dry_run=args.dry_run,
                progress=progress if args.verbose else None,
            )
            return None
        except porcelain.Error as e:
            logger.error("%s", e)
            return 1


class cmd_pull(Command):
    """Fetch from and integrate with another repository or a local branch."""

    def run(self, args) -> None:
        """Execute the pull command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("from_location", type=str)
        parser.add_argument("refspec", type=str, nargs="*")
        parser.add_argument("--filter", type=str, nargs=1)
        parser.add_argument("--protocol", type=int)
        args = parser.parse_args(args)
        porcelain.pull(
            ".",
            args.from_location or None,
            args.refspec or None,
            filter_spec=args.filter,
            protocol_version=args.protocol or None,
        )


class cmd_push(Command):
    """Update remote refs along with associated objects."""

    def run(self, argv) -> Optional[int]:
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

    def run(self, args) -> None:
        """Execute the remote-add command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("name", help="Name of the remote")
        parser.add_argument("url", help="URL of the remote")
        args = parser.parse_args(args)
        porcelain.remote_add(".", args.name, args.url)


class SuperCommand(Command):
    """Base class for commands that have subcommands."""

    subcommands: ClassVar[dict[str, type[Command]]] = {}
    default_command: ClassVar[Optional[type[Command]]] = None

    def run(self, args):
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

    def run(self, argv) -> None:
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

    def run(self, argv) -> None:
        """Execute the submodule-init command.

        Args:
            argv: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(argv)
        porcelain.submodule_init(".")


class cmd_submodule_add(Command):
    """Add a submodule."""

    def run(self, argv) -> None:
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

    def run(self, argv) -> None:
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
            "paths", nargs="*", help="Specific submodule paths to update"
        )
        args = parser.parse_args(argv)
        paths = args.paths if args.paths else None
        porcelain.submodule_update(".", paths=paths, init=args.init, force=args.force)


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

    def run(self, args):
        """Execute the check-ignore command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("paths", nargs="+", help="Paths to check")
        args = parser.parse_args(args)
        ret = 1
        for path in porcelain.check_ignore(".", args.paths):
            logger.info(path)
            ret = 0
        return ret


class cmd_check_mailmap(Command):
    """Show canonical names and email addresses of contacts."""

    def run(self, args) -> None:
        """Execute the check-mailmap command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("identities", nargs="+", help="Identities to check")
        args = parser.parse_args(args)
        for identity in args.identities:
            canonical_identity = porcelain.check_mailmap(".", identity)
            logger.info(canonical_identity)


class cmd_branch(Command):
    """List, create, or delete branches."""

    def run(self, args) -> Optional[int]:
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
        args = parser.parse_args(args)

        def print_branches(
            branches: Union[Iterator[bytes], list[bytes]], use_columns=False
        ) -> None:
            if use_columns:
                write_columns(branches, sys.stdout)
            else:
                for branch in branches:
                    sys.stdout.write(f"{branch.decode()}\n")

        branches: Union[Iterator[bytes], list[bytes], None] = None

        try:
            if args.all:
                branches = porcelain.branch_list(".") + porcelain.branch_remotes_list(
                    "."
                )
            elif args.remotes:
                branches = porcelain.branch_remotes_list(".")
            elif args.merged:
                branches = porcelain.merged_branches(".")
            elif args.no_merged:
                branches = porcelain.no_merged_branches(".")
            elif args.contains:
                try:
                    branches = list(porcelain.branches_containing(".", commit=args.contains))

                except KeyError as e:
                    sys.stderr.write(f"error: object name {e.args[0].decode()} not found\n")
                    return 1

        except porcelain.Error as e:
            sys.stderr.write(f"{e}")
            return 1

        pattern = args.list
        if pattern is not None and branches:
            branches = [
                branch
                for branch in branches
                if fnmatch.fnmatch(branch.decode(), pattern)
            ]

        if branches is not None:
            print_branches(branches, args.column)
            return 0

        if not args.branch:
            logger.error("Usage: dulwich branch [-d] BRANCH_NAME")
            return 1

        if args.delete:
            porcelain.branch_delete(".", name=args.branch)
        else:
            try:
                porcelain.branch_create(".", name=args.branch)
            except porcelain.Error as e:
                sys.stderr.write(f"{e}")
                return 1
        return 0


class cmd_checkout(Command):
    """Switch branches or restore working tree files."""

    def run(self, args) -> Optional[int]:
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
        args = parser.parse_args(args)
        if not args.target:
            logger.error("Usage: dulwich checkout TARGET [--force] [-b NEW_BRANCH]")
            return 1

        try:
            porcelain.checkout(
                ".", target=args.target, force=args.force, new_branch=args.new_branch
            )
        except porcelain.CheckoutError as e:
            sys.stderr.write(f"{e}\n")
            return 1
        return 0


class cmd_stash_list(Command):
    """List stash entries."""

    def run(self, args) -> None:
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

    def run(self, args) -> None:
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

    def run(self, args) -> None:
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

    def run(self, args):
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
                            bad_sha = f.read().strip()
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

    def run(self, args) -> None:
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

    def run(self, args) -> None:
        """Execute the describe command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.parse_args(args)
        logger.info(porcelain.describe("."))


class cmd_merge(Command):
    """Join two or more development histories together."""

    def run(self, args) -> Optional[int]:
        """Execute the merge command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("commit", type=str, help="Commit to merge")
        parser.add_argument(
            "--no-commit", action="store_true", help="Do not create a merge commit"
        )
        parser.add_argument(
            "--no-ff", action="store_true", help="Force create a merge commit"
        )
        parser.add_argument("-m", "--message", type=str, help="Merge commit message")
        args = parser.parse_args(args)

        try:
            merge_commit_id, conflicts = porcelain.merge(
                ".",
                args.commit,
                no_commit=args.no_commit,
                no_ff=args.no_ff,
                message=args.message,
            )

            if conflicts:
                logger.warning("Merge conflicts in %d file(s):", len(conflicts))
                for conflict_path in conflicts:
                    logger.warning("  %s", conflict_path.decode())
                logger.error(
                    "Automatic merge failed; fix conflicts and then commit the result."
                )
                return 1
            elif merge_commit_id is None and not args.no_commit:
                logger.info("Already up to date.")
            elif args.no_commit:
                logger.info("Automatic merge successful; not committing as requested.")
            else:
                assert merge_commit_id is not None
                logger.info(
                    "Merge successful. Created merge commit %s",
                    merge_commit_id.decode(),
                )
            return 0
        except porcelain.Error as e:
            logger.error("%s", e)
            return 1


class cmd_notes_add(Command):
    """Add notes to a commit."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        porcelain.notes_add(".", args.object, args.message, ref=args.ref)


class cmd_notes_show(Command):
    """Show notes for a commit."""

    def run(self, args) -> None:
        """Execute the notes-show command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("object", help="Object to show notes for")
        parser.add_argument(
            "--ref", default="commits", help="Notes ref (default: commits)"
        )
        args = parser.parse_args(args)

        note = porcelain.notes_show(".", args.object, ref=args.ref)
        if note:
            sys.stdout.buffer.write(note)
        else:
            logger.info("No notes found for object %s", args.object)


class cmd_notes_remove(Command):
    """Remove notes for a commit."""

    def run(self, args) -> None:
        """Execute the notes-remove command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("object", help="Object to remove notes from")
        parser.add_argument(
            "--ref", default="commits", help="Notes ref (default: commits)"
        )
        args = parser.parse_args(args)

        result = porcelain.notes_remove(".", args.object, ref=args.ref)
        if result:
            logger.info("Removed notes for object %s", args.object)
        else:
            logger.info("No notes found for object %s", args.object)


class cmd_notes_list(Command):
    """List all note objects."""

    def run(self, args) -> None:
        """Execute the notes-list command.

        Args:
            args: Command line arguments
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--ref", default="commits", help="Notes ref (default: commits)"
        )
        args = parser.parse_args(args)

        notes = porcelain.notes_list(".", ref=args.ref)
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


class cmd_cherry_pick(Command):
    """Apply the changes introduced by some existing commits."""

    def run(self, args) -> Optional[int]:
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
        args = parser.parse_args(args)

        # Check argument validity
        if args.continue_ or args.abort:
            if args.commit is not None:
                parser.error("Cannot specify commit with --continue or --abort")
                return 1
        else:
            if args.commit is None:
                parser.error("Commit argument is required")
                return 1

        try:
            commit_arg = args.commit

            result = porcelain.cherry_pick(
                ".",
                commit_arg,
                no_commit=args.no_commit,
                continue_=args.continue_,
                abort=args.abort,
            )

            if args.abort:
                logger.info("Cherry-pick aborted.")
            elif args.continue_:
                if result:
                    logger.info("Cherry-pick completed: %s", result.decode())
                else:
                    logger.info("Cherry-pick completed.")
            elif result is None:
                if args.no_commit:
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

    def run(self, args) -> Optional[int]:
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
        args = parser.parse_args(args)

        try:
            # Determine base tree - if only two args provided, base is None
            if args.base_tree is None:
                # Only two arguments provided
                base_tree = None
                our_tree = args.our_tree
                their_tree = args.their_tree
            else:
                # Three arguments provided
                base_tree = args.base_tree
                our_tree = args.our_tree
                their_tree = args.their_tree

            merged_tree_id, conflicts = porcelain.merge_tree(
                ".", base_tree, our_tree, their_tree
            )

            if args.name_only:
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

    def run(self, args) -> Optional[int]:
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
        args = parser.parse_args(args)

        # Parse prune grace period
        grace_period = None
        if args.prune:
            try:
                grace_period = parse_relative_time(args.prune)
            except ValueError:
                # Try to parse as absolute date
                try:
                    date = datetime.datetime.strptime(args.prune, "%Y-%m-%d")
                    grace_period = int(time.time() - date.timestamp())
                except ValueError:
                    logger.error("Invalid prune date: %s", args.prune)
                    return 1
        elif not args.no_prune:
            # Default to 2 weeks
            grace_period = 1209600

        # Progress callback
        def progress(msg):
            if not args.quiet:
                logger.info(msg)

        try:
            stats = porcelain.gc(
                ".",
                auto=args.auto,
                aggressive=args.aggressive,
                prune=not args.no_prune,
                grace_period=grace_period,
                dry_run=args.dry_run,
                progress=progress if not args.quiet else None,
            )

            # Report results
            if not args.quiet:
                if args.dry_run:
                    logger.info("\nDry run results:")
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


class cmd_count_objects(Command):
    """Count unpacked number of objects and their disk consumption."""

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        if args.verbose:
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

    def run(self, args) -> int:
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
        args = parser.parse_args(args)

        # Handle abort/continue/skip first
        if args.abort:
            try:
                porcelain.rebase(".", args.upstream or "HEAD", abort=True)
                logger.info("Rebase aborted.")
            except porcelain.Error as e:
                logger.error("%s", e)
                return 1
            return 0

        if args.continue_rebase:
            try:
                # Check if interactive rebase is in progress
                if porcelain.is_interactive_rebase("."):
                    result = porcelain.rebase(
                        ".",
                        args.upstream or "HEAD",
                        continue_rebase=True,
                        interactive=True,
                    )
                    if result:
                        logger.info("Rebase complete.")
                    else:
                        logger.info("Rebase paused. Use --continue to resume.")
                else:
                    new_shas = porcelain.rebase(
                        ".", args.upstream or "HEAD", continue_rebase=True
                    )
                    logger.info("Rebase complete.")
            except porcelain.Error as e:
                logger.error("%s", e)
                return 1
            return 0

        if args.edit_todo:
            # Edit todo list for interactive rebase
            try:
                porcelain.rebase(".", args.upstream or "HEAD", edit_todo=True)
                logger.info("Todo list updated.")
            except porcelain.Error as e:
                logger.error("%s", e)
                return 1
            return 0

        # Normal rebase requires upstream
        if not args.upstream:
            logger.error("Missing required argument 'upstream'")
            return 1

        try:
            if args.interactive:
                # Interactive rebase
                result = porcelain.rebase(
                    ".",
                    args.upstream,
                    onto=args.onto,
                    branch=args.branch,
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
                    args.upstream,
                    onto=args.onto,
                    branch=args.branch,
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

    def run(self, args) -> Optional[int]:
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

        args = parser.parse_args(args)

        # Track if any filter fails
        filter_error = False

        # Setup environment for filters
        env = os.environ.copy()

        # Helper function to run shell commands
        def run_filter(cmd, input_data=None, cwd=None, extra_env=None):
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
        if args.msg_filter:

            def filter_message(message):
                result = run_filter(args.msg_filter, input_data=message)
                return result if result is not None else message

        tree_filter = None
        if args.tree_filter:

            def tree_filter(tree_sha, tmpdir):
                from dulwich.objects import Blob, Tree

                # Export tree to tmpdir
                with Repo(".") as r:
                    tree = r.object_store[tree_sha]
                    for entry in tree.items():
                        path = Path(tmpdir) / entry.path.decode()
                        if entry.mode & 0o040000:  # Directory
                            path.mkdir(exist_ok=True)
                        else:
                            obj = r.object_store[entry.sha]
                            path.write_bytes(obj.data)

                    # Run the filter command in the temp directory
                    run_filter(args.tree_filter, cwd=tmpdir)

                    # Rebuild tree from modified temp directory
                    def build_tree_from_dir(dir_path):
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
        if args.index_filter:

            def index_filter(tree_sha, index_path):
                run_filter(args.index_filter, extra_env={"GIT_INDEX_FILE": index_path})
                return None  # Read back from index

        parent_filter = None
        if args.parent_filter:

            def parent_filter(parents):
                parent_str = " ".join(p.hex() for p in parents)
                result = run_filter(args.parent_filter, input_data=parent_str.encode())
                if result is None:
                    return parents

                output = result.decode().strip()
                if not output:
                    return []
                new_parents = []
                for sha in output.split():
                    if valid_hexsha(sha):
                        new_parents.append(sha)
                return new_parents

        commit_filter = None
        if args.commit_filter:

            def commit_filter(commit_obj, tree_sha):
                # The filter receives: tree parent1 parent2...
                cmd_input = tree_sha.hex()
                for parent in commit_obj.parents:
                    cmd_input += " " + parent.hex()

                result = run_filter(
                    args.commit_filter,
                    input_data=cmd_input.encode(),
                    extra_env={"GIT_COMMIT": commit_obj.id.hex()},
                )
                if result is None:
                    return None

                output = result.decode().strip()
                if not output:
                    return None  # Skip commit

                if valid_hexsha(output):
                    return output
                return None

        tag_name_filter = None
        if args.tag_name_filter:

            def tag_name_filter(tag_name):
                result = run_filter(args.tag_name_filter, input_data=tag_name)
                return result.strip() if result is not None else tag_name

        # Open repo once
        with Repo(".") as r:
            # Check for refs/original if not forcing
            if not args.force:
                original_prefix = args.original.encode() + b"/"
                for ref in r.refs.allkeys():
                    if ref.startswith(original_prefix):
                        logger.error("Cannot create a new backup.")
                        logger.error(
                            "A previous backup already exists in %s/", args.original
                        )
                        logger.error("Force overwriting the backup with -f")
                        return 1

            try:
                # Call porcelain.filter_branch with the repo object
                result = porcelain.filter_branch(
                    r,
                    args.branch,
                    filter_message=filter_message,
                    tree_filter=tree_filter if args.tree_filter else None,
                    index_filter=index_filter if args.index_filter else None,
                    parent_filter=parent_filter if args.parent_filter else None,
                    commit_filter=commit_filter if args.commit_filter else None,
                    subdirectory_filter=args.subdirectory_filter,
                    prune_empty=args.prune_empty,
                    tag_name_filter=tag_name_filter if args.tag_name_filter else None,
                    force=args.force,
                    keep_original=True,  # Always keep original with git
                )

                # Check if any filter failed
                if filter_error:
                    logger.error("Filter command failed")
                    return 1

                # Git filter-branch shows progress
                if result:
                    logger.info("Rewrite %s (%d commits)", args.branch, len(result))
                    # Git shows: Ref 'refs/heads/branch' was rewritten
                    if args.branch != "HEAD":
                        ref_name = (
                            args.branch
                            if args.branch.startswith("refs/")
                            else f"refs/heads/{args.branch}"
                        )
                        logger.info("Ref '%s' was rewritten", ref_name)

                return 0

            except porcelain.Error as e:
                logger.error("%s", e)
                return 1


class cmd_lfs(Command):
    """Git Large File Storage management."""

    """Git LFS management commands."""

    def run(self, argv) -> None:
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

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        if args.all:
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

    def run(self, args) -> None:
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
        args = parser.parse_args(args)

        # Parse committish using the new function
        committish: Optional[Union[bytes, tuple[bytes, bytes]]] = None
        if args.committish:
            with Repo(".") as r:
                range_result = parse_commit_range(r, args.committish)
                if range_result:
                    # Convert Commit objects to their SHAs
                    committish = (range_result[0].id, range_result[1].id)
                else:
                    committish = (
                        args.committish.encode()
                        if isinstance(args.committish, str)
                        else args.committish
                    )

        filenames = porcelain.format_patch(
            ".",
            committish=committish,
            outstream=sys.stdout,
            outdir=args.outdir,
            n=args.numbered,
            stdout=args.stdout,
        )

        if not args.stdout:
            for filename in filenames:
                logger.info(filename)


class cmd_bundle(Command):
    """Create, unpack, and manipulate bundle files."""

    def run(self, args) -> int:
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

    def _create(self, args) -> int:
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

            def progress(msg: str) -> None:
                logger.error(msg)

        refs_to_include = []
        prerequisites = []

        if parsed_args.all:
            refs_to_include = list(repo.refs.keys())
        elif parsed_args.stdin:
            for line in sys.stdin:
                ref = line.strip().encode("utf-8")
                if ref:
                    refs_to_include.append(ref)
        elif parsed_args.refs:
            for ref_arg in parsed_args.refs:
                if ".." in ref_arg:
                    range_result = parse_commit_range(repo, ref_arg)
                    if range_result:
                        start_commit, end_commit = range_result
                        prerequisites.append(start_commit.id)
                        # For ranges like A..B, we need to include B if it's a ref
                        # Split the range to get the end part
                        end_part = ref_arg.split("..")[1]
                        if end_part:  # Not empty (not "A..")
                            end_ref = end_part.encode("utf-8")
                            if end_ref in repo.refs:
                                refs_to_include.append(end_ref)
                    else:
                        sha = repo.refs[ref_arg.encode("utf-8")]
                        refs_to_include.append(ref_arg.encode("utf-8"))
                else:
                    if ref_arg.startswith("^"):
                        sha = repo.refs[ref_arg[1:].encode("utf-8")]
                        prerequisites.append(sha)
                    else:
                        sha = repo.refs[ref_arg.encode("utf-8")]
                        refs_to_include.append(ref_arg.encode("utf-8"))
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

    def _verify(self, args) -> int:
        parser = argparse.ArgumentParser(prog="bundle verify")
        parser.add_argument(
            "-q", "--quiet", action="store_true", help="Suppress output"
        )
        parser.add_argument("file", help="Bundle file to verify (use - for stdin)")

        parsed_args = parser.parse_args(args)

        repo = Repo(".")

        def verify_bundle(bundle):
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

    def _list_heads(self, args) -> int:
        parser = argparse.ArgumentParser(prog="bundle list-heads")
        parser.add_argument("file", help="Bundle file (use - for stdin)")
        parser.add_argument("refnames", nargs="*", help="Only show these refs")

        parsed_args = parser.parse_args(args)

        def list_heads(bundle):
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

    def _unbundle(self, args) -> int:
        parser = argparse.ArgumentParser(prog="bundle unbundle")
        parser.add_argument("--progress", action="store_true", help="Show progress")
        parser.add_argument("file", help="Bundle file (use - for stdin)")
        parser.add_argument("refnames", nargs="*", help="Only unbundle these refs")

        parsed_args = parser.parse_args(args)

        repo = Repo(".")

        progress = None
        if parsed_args.progress:

            def progress(msg: str) -> None:
                logger.error(msg)

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

    def run(self, args) -> Optional[int]:
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

    def run(self, args) -> Optional[int]:
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

    def run(self, args) -> Optional[int]:
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

    def run(self, args) -> Optional[int]:
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

    def run(self, args) -> Optional[int]:
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

    def run(self, args) -> Optional[int]:
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

    def run(self, args) -> Optional[int]:
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
    }
    default_command = cmd_worktree_list


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
    "cherry-pick": cmd_cherry_pick,
    "clone": cmd_clone,
    "commit": cmd_commit,
    "commit-tree": cmd_commit_tree,
    "count-objects": cmd_count_objects,
    "describe": cmd_describe,
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
    "help": cmd_help,
    "init": cmd_init,
    "lfs": cmd_lfs,
    "log": cmd_log,
    "ls-files": cmd_ls_files,
    "ls-remote": cmd_ls_remote,
    "ls-tree": cmd_ls_tree,
    "merge": cmd_merge,
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
    "remote": cmd_remote,
    "repack": cmd_repack,
    "reset": cmd_reset,
    "revert": cmd_revert,
    "rev-list": cmd_rev_list,
    "rm": cmd_rm,
    "mv": cmd_mv,
    "show": cmd_show,
    "stash": cmd_stash,
    "status": cmd_status,
    "shortlog": cmd_shortlog,
    "symbolic-ref": cmd_symbolic_ref,
    "submodule": cmd_submodule,
    "tag": cmd_tag,
    "unpack-objects": cmd_unpack_objects,
    "update-server-info": cmd_update_server_info,
    "upload-pack": cmd_upload_pack,
    "web-daemon": cmd_web_daemon,
    "worktree": cmd_worktree,
    "write-tree": cmd_write_tree,
}


def main(argv=None) -> Optional[int]:
    """Main entry point for the Dulwich CLI.

    Args:
        argv: Command line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code or None
    """
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
        signal.signal(signal.SIGQUIT, signal_quit)  # type: ignore
    signal.signal(signal.SIGINT, signal_int)

    sys.exit(main())


if __name__ == "__main__":
    _main()
