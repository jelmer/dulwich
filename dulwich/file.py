# file.py -- Safe access to git files
# Copyright (C) 2010 Google, Inc.
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

"""Safe access to git files."""

__all__ = [
    "FileLocked",
    "GitFile",
    "ensure_dir_exists",
]

import os
import sys
import warnings
from collections.abc import Iterable, Iterator
from types import TracebackType
from typing import IO, Any, ClassVar, Literal, overload

from ._typing import Buffer


def ensure_dir_exists(
    dirname: str | bytes | os.PathLike[str] | os.PathLike[bytes],
) -> None:
    """Ensure a directory exists, creating if necessary."""
    try:
        os.makedirs(dirname)
    except FileExistsError:
        pass


def _fancy_rename(oldname: str | bytes, newname: str | bytes) -> None:
    """Rename file with temporary backup file to rollback if rename fails."""
    if not os.path.exists(newname):
        os.rename(oldname, newname)
        return

    # Defer the tempfile import since it pulls in a lot of other things.
    import tempfile

    # destination file exists
    (fd, tmpfile) = tempfile.mkstemp(".tmp", prefix=str(oldname), dir=".")
    os.close(fd)
    os.remove(tmpfile)
    os.rename(newname, tmpfile)
    try:
        os.rename(oldname, newname)
    except OSError:
        os.rename(tmpfile, newname)
        raise
    os.remove(tmpfile)


@overload
def GitFile(
    filename: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    mode: Literal["wb"],
    bufsize: int = -1,
    mask: int = 0o644,
    fsync: bool = True,
) -> "_GitFile": ...


@overload
def GitFile(
    filename: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    mode: Literal["rb"] = "rb",
    bufsize: int = -1,
    mask: int = 0o644,
    fsync: bool = True,
) -> IO[bytes]: ...


@overload
def GitFile(
    filename: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    mode: str = "rb",
    bufsize: int = -1,
    mask: int = 0o644,
    fsync: bool = True,
) -> "IO[bytes] | _GitFile": ...


def GitFile(
    filename: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    mode: str = "rb",
    bufsize: int = -1,
    mask: int = 0o644,
    fsync: bool = True,
) -> "IO[bytes] | _GitFile":
    """Create a file object that obeys the git file locking protocol.

    Returns: a builtin file object or a _GitFile object

    Note: See _GitFile for a description of the file locking protocol.

    Only read-only and write-only (binary) modes are supported; r+, w+, and a
    are not.  To read and write from the same file, you can take advantage of
    the fact that opening a file for write does not actually open the file you
    request.

    The default file mask makes any created files user-writable and
    world-readable.

    Args:
      filename: Path to the file
      mode: File mode (only 'rb' and 'wb' are supported)
      bufsize: Buffer size for file operations
      mask: File mask for created files
      fsync: Whether to call fsync() before closing (default: True)

    """
    if "a" in mode:
        raise OSError("append mode not supported for Git files")
    if "+" in mode:
        raise OSError("read/write mode not supported for Git files")
    if "b" not in mode:
        raise OSError("text mode not supported for Git files")
    if "w" in mode:
        return _GitFile(filename, mode, bufsize, mask, fsync)
    else:
        return open(filename, mode, bufsize)


class FileLocked(Exception):
    """File is already locked."""

    def __init__(
        self,
        filename: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        lockfilename: str | bytes,
    ) -> None:
        """Initialize FileLocked.

        Args:
          filename: Name of the file that is locked
          lockfilename: Name of the lock file
        """
        self.filename = filename
        self.lockfilename = lockfilename
        super().__init__(filename, lockfilename)


class _GitFile(IO[bytes]):
    """File that follows the git locking protocol for writes.

    All writes to a file foo will be written into foo.lock in the same
    directory, and the lockfile will be renamed to overwrite the original file
    on close.

    Note: You *must* call close() or abort() on a _GitFile for the lock to be
        released. Typically this will happen in a finally block.
    """

    _file: IO[bytes]
    _filename: str | bytes
    _lockfilename: str | bytes
    _closed: bool

    PROXY_PROPERTIES: ClassVar[set[str]] = {
        "encoding",
        "errors",
        "mode",
        "name",
        "newlines",
        "softspace",
    }
    PROXY_METHODS: ClassVar[set[str]] = {
        "__iter__",
        "__next__",
        "flush",
        "fileno",
        "isatty",
        "read",
        "readable",
        "readline",
        "readlines",
        "seek",
        "seekable",
        "tell",
        "truncate",
        "writable",
        "write",
        "writelines",
    }

    def __init__(
        self,
        filename: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: str,
        bufsize: int,
        mask: int,
        fsync: bool = True,
    ) -> None:
        # Convert PathLike to str/bytes for our internal use
        self._filename: str | bytes = os.fspath(filename)
        self._fsync = fsync
        if isinstance(self._filename, bytes):
            self._lockfilename: str | bytes = self._filename + b".lock"
        else:
            self._lockfilename = self._filename + ".lock"
        try:
            fd = os.open(
                self._lockfilename,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
                mask,
            )
        except FileExistsError as exc:
            raise FileLocked(filename, self._lockfilename) from exc
        self._file = os.fdopen(fd, mode, bufsize)
        self._closed = False

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over lines in the file."""
        return iter(self._file)

    def abort(self) -> None:
        """Close and discard the lockfile without overwriting the target.

        If the file is already closed, this is a no-op.
        """
        if self._closed:
            return
        self._file.close()
        try:
            os.remove(self._lockfilename)
            self._closed = True
        except FileNotFoundError:
            # The file may have been removed already, which is ok.
            self._closed = True

    def close(self) -> None:
        """Close this file, saving the lockfile over the original.

        Note: If this method fails, it will attempt to delete the lockfile.
            However, it is not guaranteed to do so (e.g. if a filesystem
            becomes suddenly read-only), which will prevent future writes to
            this file until the lockfile is removed manually.

        Raises:
          OSError: if the original file could not be overwritten. The
            lock file is still closed, so further attempts to write to the same
            file object will raise ValueError.
        """
        if self._closed:
            return
        self._file.flush()
        if self._fsync:
            os.fsync(self._file.fileno())
        self._file.close()
        try:
            if getattr(os, "replace", None) is not None:
                os.replace(self._lockfilename, self._filename)
            else:
                if sys.platform != "win32":
                    os.rename(self._lockfilename, self._filename)
                else:
                    # Windows versions prior to Vista don't support atomic
                    # renames
                    _fancy_rename(self._lockfilename, self._filename)
        finally:
            self.abort()

    def __del__(self) -> None:
        if not getattr(self, "_closed", True):
            warnings.warn(f"unclosed {self!r}", ResourceWarning, stacklevel=2)
            self.abort()

    def __enter__(self) -> "_GitFile":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.abort()
        else:
            self.close()

    def __fspath__(self) -> str | bytes:
        """Return the file path for os.fspath() compatibility."""
        return self._filename

    @property
    def closed(self) -> bool:
        """Return whether the file is closed."""
        return self._closed

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Proxy property calls to the underlying file."""
        if name in self.PROXY_PROPERTIES:
            return getattr(self._file, name)
        raise AttributeError(name)

    # Implement IO[bytes] methods by delegating to the underlying file
    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    # TODO: Remove type: ignore when Python 3.10 support is dropped (Oct 2026)
    # Python 3.10 has issues with IO[bytes] overload signatures
    def write(self, data: Buffer, /) -> int:  # type: ignore[override,unused-ignore]
        return self._file.write(data)

    def readline(self, size: int = -1) -> bytes:
        return self._file.readline(size)

    def readlines(self, hint: int = -1) -> list[bytes]:
        return self._file.readlines(hint)

    # TODO: Remove type: ignore when Python 3.10 support is dropped (Oct 2026)
    # Python 3.10 has issues with IO[bytes] overload signatures
    def writelines(self, lines: Iterable[Buffer], /) -> None:  # type: ignore[override,unused-ignore]
        return self._file.writelines(lines)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._file.seek(offset, whence)

    def tell(self) -> int:
        return self._file.tell()

    def flush(self) -> None:
        return self._file.flush()

    def truncate(self, size: int | None = None) -> int:
        return self._file.truncate(size)

    def fileno(self) -> int:
        return self._file.fileno()

    def isatty(self) -> bool:
        return self._file.isatty()

    def readable(self) -> bool:
        return self._file.readable()

    def writable(self) -> bool:
        return self._file.writable()

    def seekable(self) -> bool:
        return self._file.seekable()

    def __next__(self) -> bytes:
        return next(iter(self._file))
