# diff.py -- Diff functionality for Dulwich
# Copyright (C) 2025 Dulwich contributors
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

"""Diff functionality with separate codepaths.

This module provides three main functions for different diff scenarios:

1. diff_index_to_tree: Shows staged changes (index vs commit)
   Used by: git diff --staged, git diff --cached

2. diff_working_tree_to_tree: Shows all changes from a commit to working tree
   Used by: git diff <commit>

3. diff_working_tree_to_index: Shows unstaged changes (working tree vs index)
   Used by: git diff (with no arguments)

Example usage:
    from dulwich.repo import Repo
    from dulwich.diff import diff_index_to_tree
    import sys

    repo = Repo('.')
    # Show staged changes
    diff_index_to_tree(repo, sys.stdout.buffer)

    # Show changes in specific paths only
    diff_index_to_tree(repo, sys.stdout.buffer, paths=[b'src/', b'README.md'])
"""

__all__ = [
    "ColorizedDiffStream",
    "diff_index_to_tree",
    "diff_working_tree_to_index",
    "diff_working_tree_to_tree",
    "should_include_path",
]

import io
import logging
import os
import stat
from collections.abc import Iterable, Sequence
from typing import BinaryIO

from ._typing import Buffer
from .index import ConflictedIndexEntry, commit_index
from .object_store import iter_tree_contents
from .objects import S_ISGITLINK, Blob, Commit, ObjectID
from .patch import write_blob_diff, write_object_diff
from .repo import Repo

logger = logging.getLogger(__name__)


def should_include_path(path: bytes, paths: Sequence[bytes] | None) -> bool:
    """Check if a path should be included based on path filters.

    Args:
        path: The path to check
        paths: List of path filters, or None for no filtering

    Returns:
        True if the path should be included
    """
    if not paths:
        return True
    return any(path == p or path.startswith(p + b"/") for p in paths)


def diff_index_to_tree(
    repo: Repo,
    outstream: BinaryIO,
    commit_sha: ObjectID | None = None,
    paths: Sequence[bytes] | None = None,
    diff_algorithm: str | None = None,
) -> None:
    """Show staged changes (index vs commit).

    Args:
        repo: Repository object
        outstream: Stream to write diff to
        commit_sha: SHA of commit to compare against, or None for HEAD
        paths: Optional list of paths to filter (as bytes)
        diff_algorithm: Algorithm to use for diffing ("myers" or "patience"), defaults to DEFAULT_DIFF_ALGORITHM if None
    """
    if commit_sha is None:
        try:
            from dulwich.refs import HEADREF

            commit_sha = repo.refs[HEADREF]
            old_commit = repo[commit_sha]
            assert isinstance(old_commit, Commit)
            old_tree = old_commit.tree
        except KeyError:
            # No HEAD means no commits yet
            old_tree = None
    else:
        old_commit = repo[commit_sha]
        assert isinstance(old_commit, Commit)
        old_tree = old_commit.tree

    # Get tree from index
    index = repo.open_index()
    new_tree = commit_index(repo.object_store, index)
    changes = repo.object_store.tree_changes(old_tree, new_tree, paths=paths)

    for (oldpath, newpath), (oldmode, newmode), (oldsha, newsha) in changes:
        write_object_diff(
            outstream,
            repo.object_store,
            (oldpath, oldmode, oldsha),
            (newpath, newmode, newsha),
            diff_algorithm=diff_algorithm,
        )


def diff_working_tree_to_tree(
    repo: Repo,
    outstream: BinaryIO,
    commit_sha: ObjectID,
    paths: Sequence[bytes] | None = None,
    diff_algorithm: str | None = None,
) -> None:
    """Compare working tree to a specific commit.

    Args:
        repo: Repository object
        outstream: Stream to write diff to
        commit_sha: SHA of commit to compare against
        paths: Optional list of paths to filter (as bytes)
        diff_algorithm: Algorithm to use for diffing ("myers" or "patience"), defaults to DEFAULT_DIFF_ALGORITHM if None
    """
    commit = repo[commit_sha]
    assert isinstance(commit, Commit)
    tree = commit.tree
    normalizer = repo.get_blob_normalizer()
    filter_callback = normalizer.checkin_normalize if normalizer is not None else None

    # Get index for tracking new files
    index = repo.open_index()
    index_paths = set(index.paths())
    processed_paths = set()

    # Process files from the committed tree lazily
    for entry in iter_tree_contents(repo.object_store, tree):
        assert (
            entry.path is not None and entry.mode is not None and entry.sha is not None
        )
        path = entry.path
        if not should_include_path(path, paths):
            continue

        processed_paths.add(path)
        full_path = os.path.join(repo.path, path.decode("utf-8"))

        # Get the old file from tree
        old_mode = entry.mode
        old_sha = entry.sha
        old_blob = repo.object_store[old_sha]
        assert isinstance(old_blob, Blob)

        try:
            # Use lstat to handle symlinks properly
            st = os.lstat(full_path)
        except FileNotFoundError:
            # File was deleted
            if old_blob is not None:
                write_blob_diff(
                    outstream, (path, old_mode, old_blob), (None, None, None)
                )
        except PermissionError:
            logger.warning("%s: Permission denied", path.decode())
            # Show as deletion if it was in tree
            if old_blob is not None:
                write_blob_diff(
                    outstream, (path, old_mode, old_blob), (None, None, None)
                )
        except OSError as e:
            logger.warning("%s: %s", path.decode(), e)
            # Show as deletion if it was in tree
            if old_blob is not None:
                write_blob_diff(
                    outstream, (path, old_mode, old_blob), (None, None, None)
                )
        else:
            # Handle different file types
            if stat.S_ISDIR(st.st_mode):
                if old_blob is not None:
                    # Directory in working tree where file was expected
                    if stat.S_ISLNK(old_mode):
                        logger.warning("%s: symlink became a directory", path.decode())
                    else:
                        logger.warning("%s: file became a directory", path.decode())
                    # Show as deletion
                    write_blob_diff(
                        outstream, (path, old_mode, old_blob), (None, None, None)
                    )
                # If old_blob is None, it's a new directory - skip it
                continue

            elif stat.S_ISLNK(st.st_mode):
                # Symlink in working tree
                target = os.readlink(full_path).encode("utf-8")
                new_blob = Blob()
                new_blob.data = target

                if old_blob is None:
                    # New symlink
                    write_blob_diff(
                        outstream,
                        (None, None, None),
                        (path, stat.S_IFLNK | 0o777, new_blob),
                    )
                elif not stat.S_ISLNK(old_mode):
                    # Type change: file/submodule -> symlink
                    write_blob_diff(
                        outstream,
                        (path, old_mode, old_blob),
                        (path, stat.S_IFLNK | 0o777, new_blob),
                    )
                elif old_blob is not None and old_blob.data != target:
                    # Symlink target changed
                    write_blob_diff(
                        outstream,
                        (path, old_mode, old_blob),
                        (path, old_mode, new_blob),
                    )

            elif stat.S_ISREG(st.st_mode):
                # Regular file
                with open(full_path, "rb") as f:
                    new_content = f.read()

                # Create a temporary blob for filtering and comparison
                new_blob = Blob()
                new_blob.data = new_content

                # Apply filters if needed (only for regular files, not gitlinks)
                if filter_callback is not None and (
                    old_blob is None or not S_ISGITLINK(old_mode)
                ):
                    new_blob = filter_callback(new_blob, path)

                # Determine the git mode for the new file
                if st.st_mode & stat.S_IXUSR:
                    new_git_mode = stat.S_IFREG | 0o755
                else:
                    new_git_mode = stat.S_IFREG | 0o644

                if old_blob is None:
                    # New file
                    write_blob_diff(
                        outstream, (None, None, None), (path, new_git_mode, new_blob)
                    )
                elif stat.S_ISLNK(old_mode):
                    # Symlink -> file
                    write_blob_diff(
                        outstream,
                        (path, old_mode, old_blob),
                        (path, new_git_mode, new_blob),
                    )
                elif S_ISGITLINK(old_mode):
                    # Submodule -> file
                    write_blob_diff(
                        outstream,
                        (path, old_mode, old_blob),
                        (path, new_git_mode, new_blob),
                    )
                else:
                    # Regular file, check for content or mode changes
                    old_git_mode = old_mode & (stat.S_IFREG | 0o777)
                    if (
                        old_blob is not None and old_blob.data != new_blob.data
                    ) or old_git_mode != new_git_mode:
                        write_blob_diff(
                            outstream,
                            (path, old_mode, old_blob),
                            (path, new_git_mode, new_blob),
                        )

            elif stat.S_ISFIFO(st.st_mode):
                logger.warning("%s: unsupported file type (fifo)", path.decode())
                if old_blob is not None:
                    write_blob_diff(
                        outstream, (path, old_mode, old_blob), (None, None, None)
                    )
            elif stat.S_ISSOCK(st.st_mode):
                logger.warning("%s: unsupported file type (socket)", path.decode())
                if old_blob is not None:
                    write_blob_diff(
                        outstream, (path, old_mode, old_blob), (None, None, None)
                    )
            else:
                logger.warning("%s: unsupported file type", path.decode())
                if old_blob is not None:
                    write_blob_diff(
                        outstream, (path, old_mode, old_blob), (None, None, None)
                    )

    # Now process any new files from index that weren't in the tree
    for path in sorted(index_paths - processed_paths):
        if not should_include_path(path, paths):
            continue

        full_path = os.path.join(repo.path, path.decode("utf-8"))

        try:
            # Use lstat to handle symlinks properly
            st = os.lstat(full_path)
        except FileNotFoundError:
            # New file already deleted, skip
            continue
        except PermissionError:
            logger.warning("%s: Permission denied", path.decode())
            continue
        except OSError as e:
            logger.warning("%s: %s", path.decode(), e)
            continue

        # Handle different file types for new files
        if stat.S_ISDIR(st.st_mode):
            # New directory - skip it
            continue
        elif stat.S_ISLNK(st.st_mode):
            # New symlink
            target = os.readlink(full_path).encode("utf-8")
            new_blob = Blob()
            new_blob.data = target
            write_blob_diff(
                outstream,
                (None, None, None),
                (path, stat.S_IFLNK | 0o777, new_blob),
            )
        elif stat.S_ISREG(st.st_mode):
            # New regular file
            with open(full_path, "rb") as f:
                new_content = f.read()

            new_blob = Blob()
            new_blob.data = new_content

            # Apply filters if needed
            if filter_callback is not None:
                new_blob = filter_callback(new_blob, path)

            # Determine the git mode for the new file
            if st.st_mode & stat.S_IXUSR:
                new_git_mode = 0o100755
            else:
                new_git_mode = 0o100644

            write_blob_diff(
                outstream, (None, None, None), (path, new_git_mode, new_blob)
            )
        elif stat.S_ISFIFO(st.st_mode):
            logger.warning("%s: unsupported file type (fifo)", path.decode())
        elif stat.S_ISSOCK(st.st_mode):
            logger.warning("%s: unsupported file type (socket)", path.decode())
        else:
            logger.warning("%s: unsupported file type", path.decode())


def diff_working_tree_to_index(
    repo: Repo,
    outstream: BinaryIO,
    paths: Sequence[bytes] | None = None,
    diff_algorithm: str | None = None,
) -> None:
    """Compare working tree to index.

    Args:
        repo: Repository object
        outstream: Stream to write diff to
        paths: Optional list of paths to filter (as bytes)
        diff_algorithm: Algorithm to use for diffing ("myers" or "patience"), defaults to DEFAULT_DIFF_ALGORITHM if None
    """
    index = repo.open_index()
    normalizer = repo.get_blob_normalizer()
    filter_callback = normalizer.checkin_normalize if normalizer is not None else None

    # Process each file in the index
    for tree_path, entry in index.iteritems():
        if not should_include_path(tree_path, paths):
            continue

        # Handle conflicted entries by using stage 2 ("ours")
        if isinstance(entry, ConflictedIndexEntry):
            if entry.this is None:
                continue  # No stage 2 entry, skip
            old_mode = entry.this.mode
            old_sha = entry.this.sha
        else:
            # Get file from regular index entry
            old_mode = entry.mode
            old_sha = entry.sha
        old_obj = repo.object_store[old_sha]
        # Type check and cast to Blob
        if isinstance(old_obj, Blob):
            old_blob = old_obj
        else:
            old_blob = None

        full_path = os.path.join(repo.path, tree_path.decode("utf-8"))
        try:
            # Use lstat to handle symlinks properly
            st = os.lstat(full_path)

            # Handle different file types
            if stat.S_ISDIR(st.st_mode):
                # Directory in working tree where file was expected
                if stat.S_ISLNK(old_mode):
                    logger.warning("%s: symlink became a directory", tree_path.decode())
                else:
                    logger.warning("%s: file became a directory", tree_path.decode())
                # Show as deletion
                write_blob_diff(
                    outstream, (tree_path, old_mode, old_blob), (None, None, None)
                )

            elif stat.S_ISLNK(st.st_mode):
                # Symlink in working tree
                target = os.readlink(full_path).encode("utf-8")
                new_blob = Blob()
                new_blob.data = target

                # Check if type changed or content changed
                if not stat.S_ISLNK(old_mode):
                    # Type change: file/submodule -> symlink
                    write_blob_diff(
                        outstream,
                        (tree_path, old_mode, old_blob),
                        (tree_path, stat.S_IFLNK | 0o777, new_blob),
                    )
                elif old_blob is not None and old_blob.data != target:
                    # Symlink target changed
                    write_blob_diff(
                        outstream,
                        (tree_path, old_mode, old_blob),
                        (tree_path, old_mode, new_blob),
                    )

            elif stat.S_ISREG(st.st_mode):
                # Regular file
                with open(full_path, "rb") as f:
                    new_content = f.read()

                # Create a temporary blob for filtering and comparison
                new_blob = Blob()
                new_blob.data = new_content

                # Apply filters if needed (only for regular files)
                if filter_callback is not None and not S_ISGITLINK(old_mode):
                    new_blob = filter_callback(new_blob, tree_path)

                # Determine the git mode for the new file
                if st.st_mode & stat.S_IXUSR:
                    new_git_mode = stat.S_IFREG | 0o755
                else:
                    new_git_mode = stat.S_IFREG | 0o644

                # Check if this was a type change
                if stat.S_ISLNK(old_mode):
                    # Symlink -> file
                    write_blob_diff(
                        outstream,
                        (tree_path, old_mode, old_blob),
                        (tree_path, new_git_mode, new_blob),
                    )
                elif S_ISGITLINK(old_mode):
                    # Submodule -> file
                    write_blob_diff(
                        outstream,
                        (tree_path, old_mode, old_blob),
                        (tree_path, new_git_mode, new_blob),
                    )
                else:
                    # Regular file, check for content or mode changes
                    old_git_mode = old_mode & (stat.S_IFREG | 0o777)
                    if (
                        old_blob is not None and old_blob.data != new_blob.data
                    ) or old_git_mode != new_git_mode:
                        write_blob_diff(
                            outstream,
                            (tree_path, old_mode, old_blob),
                            (tree_path, new_git_mode, new_blob),
                        )

            elif stat.S_ISFIFO(st.st_mode):
                logger.warning("%s: unsupported file type (fifo)", tree_path.decode())
                write_blob_diff(
                    outstream, (tree_path, old_mode, old_blob), (None, None, None)
                )
            elif stat.S_ISSOCK(st.st_mode):
                logger.warning("%s: unsupported file type (socket)", tree_path.decode())
                write_blob_diff(
                    outstream, (tree_path, old_mode, old_blob), (None, None, None)
                )
            else:
                logger.warning("%s: unsupported file type", tree_path.decode())
                write_blob_diff(
                    outstream, (tree_path, old_mode, old_blob), (None, None, None)
                )

        except FileNotFoundError:
            # File was deleted - this is normal, not a warning
            write_blob_diff(
                outstream, (tree_path, old_mode, old_blob), (None, None, None)
            )
        except PermissionError:
            logger.warning("%s: Permission denied", tree_path.decode())
            # Show as deletion since we can't read it
            write_blob_diff(
                outstream, (tree_path, old_mode, old_blob), (None, None, None)
            )
        except OSError as e:
            logger.warning("%s: %s", tree_path.decode(), e)
            # Show as deletion since we can't read it
            write_blob_diff(
                outstream, (tree_path, old_mode, old_blob), (None, None, None)
            )


class ColorizedDiffStream(BinaryIO):
    """Stream wrapper that colorizes diff output line by line using Rich.

    This class wraps a binary output stream and applies color formatting
    to diff output as it's written. It processes data line by line to
    enable streaming colorization without buffering the entire diff.
    """

    @staticmethod
    def is_available() -> bool:
        """Check if Rich is available for colorization.

        Returns:
            bool: True if Rich can be imported, False otherwise
        """
        try:
            import importlib.util

            return importlib.util.find_spec("rich.console") is not None
        except ImportError:
            return False

    def __init__(self, output_stream: BinaryIO) -> None:
        """Initialize the colorized stream wrapper.

        Args:
            output_stream: The underlying binary stream to write to
        """
        self.output_stream = output_stream
        import io

        from rich.console import Console

        # Rich expects a text stream, so we need to wrap our binary stream
        self.text_wrapper = io.TextIOWrapper(
            output_stream, encoding="utf-8", newline=""
        )
        self.console = Console(file=self.text_wrapper, force_terminal=True)
        self.buffer = b""

    def write(self, data: bytes | Buffer) -> int:  # type: ignore[override,unused-ignore]
        """Write data to the stream, applying colorization.

        Args:
            data: Bytes to write

        Returns:
            Number of bytes written
        """
        # Add new data to buffer
        if not isinstance(data, bytes):
            data = bytes(data)
        self.buffer += data

        # Process complete lines
        while b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            self._colorize_and_write_line(line + b"\n")

        return len(data)

    def writelines(self, lines: Iterable[bytes | Buffer]) -> None:  # type: ignore[override,unused-ignore]
        """Write a list of lines to the stream.

        Args:
            lines: Iterable of bytes to write
        """
        for line in lines:
            self.write(line)

    def _colorize_and_write_line(self, line_bytes: bytes) -> None:
        """Apply color formatting to a single line and write it.

        Args:
            line_bytes: The line to colorize and write (as bytes)
        """
        try:
            line = line_bytes.decode("utf-8", errors="replace")

            # Colorize based on diff line type
            if line.startswith("+") and not line.startswith("+++"):
                self.console.print(line, style="green", end="")
            elif line.startswith("-") and not line.startswith("---"):
                self.console.print(line, style="red", end="")
            elif line.startswith("@@"):
                self.console.print(line, style="cyan", end="")
            elif line.startswith(("+++", "---")):
                self.console.print(line, style="bold", end="")
            else:
                self.console.print(line, end="")
        except (UnicodeDecodeError, UnicodeEncodeError):
            # Fallback to raw output if we can't decode/encode the text
            self.output_stream.write(line_bytes)

    def flush(self) -> None:
        """Flush any remaining buffered content and the underlying stream."""
        # Write any remaining buffer content
        if self.buffer:
            self._colorize_and_write_line(self.buffer)
            self.buffer = b""
        # Flush the text wrapper and underlying stream
        if hasattr(self.text_wrapper, "flush"):
            self.text_wrapper.flush()
        if hasattr(self.output_stream, "flush"):
            self.output_stream.flush()

    # BinaryIO interface methods
    def close(self) -> None:
        """Close the stream."""
        self.flush()
        if hasattr(self.output_stream, "close"):
            self.output_stream.close()

    @property
    def closed(self) -> bool:
        """Check if the stream is closed."""
        return getattr(self.output_stream, "closed", False)

    def fileno(self) -> int:
        """Return the file descriptor."""
        return self.output_stream.fileno()

    def isatty(self) -> bool:
        """Check if the stream is a TTY."""
        return getattr(self.output_stream, "isatty", lambda: False)()

    def read(self, n: int = -1) -> bytes:
        """Read is not supported on this write-only stream."""
        raise io.UnsupportedOperation("not readable")

    def readable(self) -> bool:
        """This stream is not readable."""
        return False

    def readline(self, limit: int = -1) -> bytes:
        """Read is not supported on this write-only stream."""
        raise io.UnsupportedOperation("not readable")

    def readlines(self, hint: int = -1) -> list[bytes]:
        """Read is not supported on this write-only stream."""
        raise io.UnsupportedOperation("not readable")

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek is not supported on this stream."""
        raise io.UnsupportedOperation("not seekable")

    def seekable(self) -> bool:
        """This stream is not seekable."""
        return False

    def tell(self) -> int:
        """Tell is not supported on this stream."""
        raise io.UnsupportedOperation("not seekable")

    def truncate(self, size: int | None = None) -> int:
        """Truncate is not supported on this stream."""
        raise io.UnsupportedOperation("not truncatable")

    def writable(self) -> bool:
        """This stream is writable."""
        return True

    def __enter__(self) -> "ColorizedDiffStream":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        """Context manager exit."""
        self.flush()

    def __iter__(self) -> "ColorizedDiffStream":
        """Iterator interface - not supported."""
        raise io.UnsupportedOperation("not iterable")

    def __next__(self) -> bytes:
        """Iterator interface - not supported."""
        raise io.UnsupportedOperation("not iterable")
