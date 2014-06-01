# index.py -- File parser/writer for the git index file
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your opinion) any later version of the license.
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

"""Parser for the git index file format."""

import collections
import errno
import os
import stat
import struct

from dulwich.file import GitFile
from dulwich.objects import (
    Blob,
    S_IFGITLINK,
    S_ISGITLINK,
    Tree,
    hex_to_sha,
    sha_to_hex,
    )
from dulwich.pack import (
    SHA1Reader,
    SHA1Writer,
    )


IndexEntry = collections.namedtuple(
    'IndexEntry', [
        'ctime', 'mtime', 'dev', 'ino', 'mode', 'uid', 'gid', 'size', 'sha',
        'flags'])


def pathsplit(path):
    """Split a /-delimited path into a directory part and a basename.

    :param path: The path to split.
    :return: Tuple with directory name and basename
    """
    try:
        (dirname, basename) = path.rsplit("/", 1)
    except ValueError:
        return ("", path)
    else:
        return (dirname, basename)


def pathjoin(*args):
    """Join a /-delimited path.

    """
    return "/".join([p for p in args if p])


def read_cache_time(f):
    """Read a cache time.

    :param f: File-like object to read from
    :return: Tuple with seconds and nanoseconds
    """
    return struct.unpack(">LL", f.read(8))


def write_cache_time(f, t):
    """Write a cache time.

    :param f: File-like object to write to
    :param t: Time to write (as int, float or tuple with secs and nsecs)
    """
    if isinstance(t, int):
        t = (t, 0)
    elif isinstance(t, float):
        (secs, nsecs) = divmod(t, 1.0)
        t = (int(secs), int(nsecs * 1000000000))
    elif not isinstance(t, tuple):
        raise TypeError(t)
    f.write(struct.pack(">LL", *t))


def read_cache_entry(f):
    """Read an entry from a cache file.

    :param f: File-like object to read from
    :return: tuple with: device, inode, mode, uid, gid, size, sha, flags
    """
    beginoffset = f.tell()
    ctime = read_cache_time(f)
    mtime = read_cache_time(f)
    (dev, ino, mode, uid, gid, size, sha, flags, ) = \
        struct.unpack(">LLLLLL20sH", f.read(20 + 4 * 6 + 2))
    name = f.read((flags & 0x0fff))
    # Padding:
    real_size = ((f.tell() - beginoffset + 8) & ~7)
    f.read((beginoffset + real_size) - f.tell())
    return (name, ctime, mtime, dev, ino, mode, uid, gid, size,
            sha_to_hex(sha), flags & ~0x0fff)


def write_cache_entry(f, entry):
    """Write an index entry to a file.

    :param f: File object
    :param entry: Entry to write, tuple with:
        (name, ctime, mtime, dev, ino, mode, uid, gid, size, sha, flags)
    """
    beginoffset = f.tell()
    (name, ctime, mtime, dev, ino, mode, uid, gid, size, sha, flags) = entry
    write_cache_time(f, ctime)
    write_cache_time(f, mtime)
    flags = len(name) | (flags &~ 0x0fff)
    f.write(struct.pack(">LLLLLL20sH", dev & 0xFFFFFFFF, ino & 0xFFFFFFFF, mode, uid, gid, size, hex_to_sha(sha), flags))
    f.write(name)
    real_size = ((f.tell() - beginoffset + 8) & ~7)
    f.write("\0" * ((beginoffset + real_size) - f.tell()))


def read_index(f):
    """Read an index file, yielding the individual entries."""
    header = f.read(4)
    if header != "DIRC":
        raise AssertionError("Invalid index file header: %r" % header)
    (version, num_entries) = struct.unpack(">LL", f.read(4 * 2))
    assert version in (1, 2)
    for i in range(num_entries):
        yield read_cache_entry(f)


def read_index_dict(f):
    """Read an index file and return it as a dictionary.

    :param f: File object to read from
    """
    ret = {}
    for x in read_index(f):
        ret[x[0]] = IndexEntry(*x[1:])
    return ret


def write_index(f, entries):
    """Write an index file.

    :param f: File-like object to write to
    :param entries: Iterable over the entries to write
    """
    f.write("DIRC")
    f.write(struct.pack(">LL", 2, len(entries)))
    for x in entries:
        write_cache_entry(f, x)


def write_index_dict(f, entries):
    """Write an index file based on the contents of a dictionary.

    """
    entries_list = []
    for name in sorted(entries):
        entries_list.append((name,) + tuple(entries[name]))
    write_index(f, entries_list)


def cleanup_mode(mode):
    """Cleanup a mode value.

    This will return a mode that can be stored in a tree object.

    :param mode: Mode to clean up.
    """
    if stat.S_ISLNK(mode):
        return stat.S_IFLNK
    elif stat.S_ISDIR(mode):
        return stat.S_IFDIR
    elif S_ISGITLINK(mode):
        return S_IFGITLINK
    ret = stat.S_IFREG | 0o644
    ret |= (mode & 0o111)
    return ret


class Index(object):
    """A Git Index file."""

    def __init__(self, filename):
        """Open an index file.

        :param filename: Path to the index file
        """
        self._filename = filename
        self.clear()
        self.read()

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._filename)

    def write(self):
        """Write current contents of index to disk."""
        f = GitFile(self._filename, 'wb')
        try:
            f = SHA1Writer(f)
            write_index_dict(f, self._byname)
        finally:
            f.close()

    def read(self):
        """Read current contents of index from disk."""
        if not os.path.exists(self._filename):
            return
        f = GitFile(self._filename, 'rb')
        try:
            f = SHA1Reader(f)
            for x in read_index(f):
                self[x[0]] = IndexEntry(*x[1:])
            # FIXME: Additional data?
            f.read(os.path.getsize(self._filename)-f.tell()-20)
            f.check_sha()
        finally:
            f.close()

    def __len__(self):
        """Number of entries in this index file."""
        return len(self._byname)

    def __getitem__(self, name):
        """Retrieve entry by relative path.

        :return: tuple with (ctime, mtime, dev, ino, mode, uid, gid, size, sha, flags)
        """
        return self._byname[name]

    def __iter__(self):
        """Iterate over the paths in this index."""
        return iter(self._byname)

    def get_sha1(self, path):
        """Return the (git object) SHA1 for the object at a path."""
        return self[path].sha

    def get_mode(self, path):
        """Return the POSIX file mode for the object at a path."""
        return self[path].mode

    def iterblobs(self):
        """Iterate over path, sha, mode tuples for use with commit_tree."""
        for path in self:
            entry = self[path]
            yield path, entry.sha, cleanup_mode(entry.mode)

    def clear(self):
        """Remove all contents from this index."""
        self._byname = {}

    def __setitem__(self, name, x):
        assert isinstance(name, str)
        assert len(x) == 10
        # Remove the old entry if any
        self._byname[name] = x

    def __delitem__(self, name):
        assert isinstance(name, str)
        del self._byname[name]

    def iteritems(self):
        return self._byname.iteritems()

    def update(self, entries):
        for name, value in entries.iteritems():
            self[name] = value

    def changes_from_tree(self, object_store, tree, want_unchanged=False):
        """Find the differences between the contents of this index and a tree.

        :param object_store: Object store to use for retrieving tree contents
        :param tree: SHA1 of the root tree
        :param want_unchanged: Whether unchanged files should be reported
        :return: Iterator over tuples with (oldpath, newpath), (oldmode, newmode), (oldsha, newsha)
        """
        def lookup_entry(path):
            entry = self[path]
            return entry.sha, entry.mode
        for (name, mode, sha) in changes_from_tree(self._byname.keys(),
                lookup_entry, object_store, tree,
                want_unchanged=want_unchanged):
            yield (name, mode, sha)

    def commit(self, object_store):
        """Create a new tree from an index.

        :param object_store: Object store to save the tree in
        :return: Root tree SHA
        """
        return commit_tree(object_store, self.iterblobs())


def commit_tree(object_store, blobs):
    """Commit a new tree.

    :param object_store: Object store to add trees to
    :param blobs: Iterable over blob path, sha, mode entries
    :return: SHA1 of the created tree.
    """

    trees = {"": {}}

    def add_tree(path):
        if path in trees:
            return trees[path]
        dirname, basename = pathsplit(path)
        t = add_tree(dirname)
        assert isinstance(basename, str)
        newtree = {}
        t[basename] = newtree
        trees[path] = newtree
        return newtree

    for path, sha, mode in blobs:
        tree_path, basename = pathsplit(path)
        tree = add_tree(tree_path)
        tree[basename] = (mode, sha)

    def build_tree(path):
        tree = Tree()
        for basename, entry in trees[path].iteritems():
            if isinstance(entry, dict):
                mode = stat.S_IFDIR
                sha = build_tree(pathjoin(path, basename))
            else:
                (mode, sha) = entry
            tree.add(basename, mode, sha)
        object_store.add_object(tree)
        return tree.id
    return build_tree("")


def commit_index(object_store, index):
    """Create a new tree from an index.

    :param object_store: Object store to save the tree in
    :param index: Index file
    :note: This function is deprecated, use index.commit() instead.
    :return: Root tree sha.
    """
    return commit_tree(object_store, index.iterblobs())


def changes_from_tree(names, lookup_entry, object_store, tree,
        want_unchanged=False):
    """Find the differences between the contents of a tree and
    a working copy.

    :param names: Iterable of names in the working copy
    :param lookup_entry: Function to lookup an entry in the working copy
    :param object_store: Object store to use for retrieving tree contents
    :param tree: SHA1 of the root tree, or None for an empty tree
    :param want_unchanged: Whether unchanged files should be reported
    :return: Iterator over tuples with (oldpath, newpath), (oldmode, newmode),
        (oldsha, newsha)
    """
    other_names = set(names)

    if tree is not None:
        for (name, mode, sha) in object_store.iter_tree_contents(tree):
            try:
                (other_sha, other_mode) = lookup_entry(name)
            except KeyError:
                # Was removed
                yield ((name, None), (mode, None), (sha, None))
            else:
                other_names.remove(name)
                if (want_unchanged or other_sha != sha or other_mode != mode):
                    yield ((name, name), (mode, other_mode), (sha, other_sha))

    # Mention added files
    for name in other_names:
        (other_sha, other_mode) = lookup_entry(name)
        yield ((None, name), (None, other_mode), (None, other_sha))


def index_entry_from_stat(stat_val, hex_sha, flags, mode=None):
    """Create a new index entry from a stat value.

    :param stat_val: POSIX stat_result instance
    :param hex_sha: Hex sha of the object
    :param flags: Index flags
    """
    if mode is None:
        mode = cleanup_mode(stat_val.st_mode)
    return (stat_val.st_ctime, stat_val.st_mtime, stat_val.st_dev,
            stat_val.st_ino, mode, stat_val.st_uid,
            stat_val.st_gid, stat_val.st_size, hex_sha, flags)


def build_index_from_tree(prefix, index_path, object_store, tree_id,
                          honor_filemode=True):
    """Generate and materialize index from a tree

    :param tree_id: Tree to materialize
    :param prefix: Target dir for materialized index files
    :param index_path: Target path for generated index
    :param object_store: Non-empty object store holding tree contents
    :param honor_filemode: An optional flag to honor core.filemode setting in
        config file, default is core.filemode=True, change executable bit

    :note:: existing index is wiped and contents are not merged
        in a working dir. Suiteable only for fresh clones.
    """

    index = Index(index_path)

    for entry in object_store.iter_tree_contents(tree_id):
        full_path = os.path.join(prefix, entry.path)

        if not os.path.exists(os.path.dirname(full_path)):
            os.makedirs(os.path.dirname(full_path))

        # FIXME: Merge new index into working tree
        if stat.S_ISLNK(entry.mode):
            # FIXME: This will fail on Windows. What should we do instead?
            src_path = object_store[entry.sha].as_raw_string()
            try:
                os.symlink(src_path, full_path)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    os.unlink(full_path)
                    os.symlink(src_path, full_path)
                else:
                    raise
        else:
            f = open(full_path, 'wb')
            try:
                # Write out file
                f.write(object_store[entry.sha].as_raw_string())
            finally:
                f.close()

            if honor_filemode:
                os.chmod(full_path, entry.mode)

        # Add file to index
        st = os.lstat(full_path)
        index[entry.path] = index_entry_from_stat(st, entry.sha, 0)

    index.write()


def blob_from_path_and_stat(path, st):
    """Create a blob from a path and a stat object.

    :param path: Full path to file
    :param st: A stat object
    :return: A `Blob` object
    """
    blob = Blob()
    if not stat.S_ISLNK(st.st_mode):
        f = open(path, 'rb')
        try:
            blob.data = f.read()
        finally:
            f.close()
    else:
        blob.data = os.readlink(path)
    return blob


def get_unstaged_changes(index, path):
    """Walk through an index and check for differences against working tree.

    :param index: index to check
    :param path: path in which to find files
    :return: iterator over paths with unstaged changes
    """
    # For each entry in the index check the sha1 & ensure not staged
    for name, entry in index.iteritems():
        fp = os.path.join(path, name)
        blob = blob_from_path_and_stat(fp, os.lstat(fp))
        if blob.id != entry.sha:
            yield name
