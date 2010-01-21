# file.py -- Safe access to git files
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.


"""Safe access to git files."""


import errno
import os


def GitFile(filename, mode='r', bufsize=-1):
    """Create a file object that obeys the git file locking protocol.

    See _GitFile for a description of the file locking protocol.

    Only read-only and write-only (binary) modes are supported; r+, w+, and a
    are not.  To read and write from the same file, you can take advantage of
    the fact that opening a file for write does not actually open the file you
    request:

    >>> write_file = GitFile('filename', 'wb')
    >>> read_file = GitFile('filename', 'rb')
    >>> read_file.readlines()
    ['contents\n', 'of\n', 'the\n', 'file\n']
    >>> write_file.write('foo')
    >>> read_file.close()
    >>> write_file.close()
    >>> new_file = GitFile('filename', 'rb')
    'foo'
    >>> new_file.close()
    >>> other_file = GitFile('filename', 'wb')
    Traceback (most recent call last):
        ...
    OSError: [Errno 17] File exists: 'filename.lock'

    :return: a builtin file object or a _GitFile object
    """
    if 'a' in mode:
        raise IOError('append mode not supported for Git files')
    if '+' in mode:
        raise IOError('read/write mode not supported for Git files')
    if 'b' not in mode:
        raise IOError('text mode not supported for Git files')
    if 'w' in mode:
        return _GitFile(filename, mode, bufsize)
    else:
        return file(filename, mode, bufsize)


class _GitFile(object):
    """File that follows the git locking protocol for writes.

    All writes to a file foo will be written into foo.lock in the same
    directory, and the lockfile will be renamed to overwrite the original file
    on close. The lockfile is automatically removed upon filesystem error.

    :note: You *must* call close() or abort() on a _GitFile for the lock to be
        released. Typically this will happen in a finally block.
    """

    PROXY_PROPERTIES = set(['closed', 'encoding', 'errors', 'mode', 'name',
                            'newlines', 'softspace'])
    PROXY_METHODS = ('__iter__', 'flush', 'fileno', 'isatty', 'next', 'read',
                     'readline', 'readlines', 'xreadlines', 'seek', 'tell',
                     'truncate', 'write', 'writelines')
    def __init__(self, filename, mode, bufsize):
        self._filename = filename
        self._lockfilename = '%s.lock' % self._filename
        fd = os.open(self._lockfilename, os.O_RDWR | os.O_CREAT | os.O_EXCL)
        self._file = os.fdopen(fd, mode, bufsize)

        for method in self.PROXY_METHODS:
            setattr(self, method,
                    self._safe_method(getattr(self._file, method)))

    def _safe_method(self, file_method):
        # note that built-in file methods have no kwargs
        def do_safe_method(*args):
            try:
                return file_method(*args)
            except (OSError, IOError):
                self.abort()
                raise
        return do_safe_method

    def abort(self):
        """Close and discard the lockfile without overwriting the target.

        If the file is already closed, this is a no-op.
        """
        self._file.close()
        try:
            os.remove(self._lockfilename)
        except OSError, e:
            # The file may have been removed already, which is ok.
            if e.errno != errno.ENOENT:
                raise

    def close(self):
        """Close this file, saving the lockfile over the original.

        :note: If this method fails, it will attempt to delete the lockfile.
            However, it is not guaranteed to do so (e.g. if a filesystem becomes
            suddenly read-only), which will prevent future writes to this file
            until the lockfile is removed manually.
        :raises OSError: if the original file could not be overwritten. The lock
            file is still closed, so further attempts to write to the same file
            object will raise ValueError.
        """
        self._file.close()
        try:
            os.rename(self._lockfilename, self._filename)
        finally:
            self.abort()

    def __getattr__(self, name):
        """Proxy property calls to the underlying file."""
        if name in self.PROXY_PROPERTIES:
            return getattr(self._file, name)
        raise AttributeError(name)
