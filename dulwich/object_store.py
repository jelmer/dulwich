# object_store.py -- Object store for git objects
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
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


"""Git object store interfaces and implementation."""


import errno
import itertools
import os
import stat
import tempfile

from dulwich.diff_tree import (
    tree_changes,
    walk_trees,
    )
from dulwich.errors import (
    NotTreeError,
    )
from dulwich.file import GitFile
from dulwich.objects import (
    Commit,
    ShaFile,
    Tag,
    Tree,
    ZERO_SHA,
    hex_to_sha,
    sha_to_hex,
    hex_to_filename,
    S_ISGITLINK,
    object_class,
    )
from dulwich.pack import (
    Pack,
    PackData,
    iter_sha1,
    write_pack_header,
    write_pack_index_v2,
    write_pack_object,
    write_pack_objects,
    compute_file_sha,
    PackIndexer,
    PackStreamCopier,
    )

INFODIR = 'info'
PACKDIR = 'pack'


class BaseObjectStore(object):
    """Object store interface."""

    def determine_wants_all(self, refs):
        return [sha for (ref, sha) in refs.iteritems()
                if not sha in self and not ref.endswith("^{}") and
                   not sha == ZERO_SHA]

    def iter_shas(self, shas):
        """Iterate over the objects for the specified shas.

        :param shas: Iterable object with SHAs
        :return: Object iterator
        """
        return ObjectStoreIterator(self, shas)

    def contains_loose(self, sha):
        """Check if a particular object is present by SHA1 and is loose."""
        raise NotImplementedError(self.contains_loose)

    def contains_packed(self, sha):
        """Check if a particular object is present by SHA1 and is packed."""
        raise NotImplementedError(self.contains_packed)

    def __contains__(self, sha):
        """Check if a particular object is present by SHA1.

        This method makes no distinction between loose and packed objects.
        """
        return self.contains_packed(sha) or self.contains_loose(sha)

    @property
    def packs(self):
        """Iterable of pack objects."""
        raise NotImplementedError

    def get_raw(self, name):
        """Obtain the raw text for an object.

        :param name: sha for the object.
        :return: tuple with numeric type and object contents.
        """
        raise NotImplementedError(self.get_raw)

    def __getitem__(self, sha):
        """Obtain an object by SHA1."""
        type_num, uncomp = self.get_raw(sha)
        return ShaFile.from_raw_string(type_num, uncomp)

    def __iter__(self):
        """Iterate over the SHAs that are present in this store."""
        raise NotImplementedError(self.__iter__)

    def add_object(self, obj):
        """Add a single object to this object store.

        """
        raise NotImplementedError(self.add_object)

    def add_objects(self, objects):
        """Add a set of objects to this object store.

        :param objects: Iterable over a list of objects.
        """
        raise NotImplementedError(self.add_objects)

    def tree_changes(self, source, target, want_unchanged=False):
        """Find the differences between the contents of two trees

        :param source: SHA1 of the source tree
        :param target: SHA1 of the target tree
        :param want_unchanged: Whether unchanged files should be reported
        :return: Iterator over tuples with
            (oldpath, newpath), (oldmode, newmode), (oldsha, newsha)
        """
        for change in tree_changes(self, source, target,
                                   want_unchanged=want_unchanged):
            yield ((change.old.path, change.new.path),
                   (change.old.mode, change.new.mode),
                   (change.old.sha, change.new.sha))

    def iter_tree_contents(self, tree_id, include_trees=False):
        """Iterate the contents of a tree and all subtrees.

        Iteration is depth-first pre-order, as in e.g. os.walk.

        :param tree_id: SHA1 of the tree.
        :param include_trees: If True, include tree objects in the iteration.
        :return: Iterator over TreeEntry namedtuples for all the objects in a
            tree.
        """
        for entry, _ in walk_trees(self, tree_id, None):
            if not stat.S_ISDIR(entry.mode) or include_trees:
                yield entry

    def find_missing_objects(self, haves, wants, progress=None,
                             get_tagged=None):
        """Find the missing objects required for a set of revisions.

        :param haves: Iterable over SHAs already in common.
        :param wants: Iterable over SHAs of objects to fetch.
        :param progress: Simple progress function that will be called with
            updated progress strings.
        :param get_tagged: Function that returns a dict of pointed-to sha -> tag
            sha for including tags.
        :return: Iterator over (sha, path) pairs.
        """
        finder = MissingObjectFinder(self, haves, wants, progress, get_tagged)
        return iter(finder.next, None)

    def find_common_revisions(self, graphwalker):
        """Find which revisions this store has in common using graphwalker.

        :param graphwalker: A graphwalker object.
        :return: List of SHAs that are in common
        """
        haves = []
        sha = graphwalker.next()
        while sha:
            if sha in self:
                haves.append(sha)
                graphwalker.ack(sha)
            sha = graphwalker.next()
        return haves

    def get_graph_walker(self, heads):
        """Obtain a graph walker for this object store.

        :param heads: Local heads to start search with
        :return: GraphWalker object
        """
        return ObjectStoreGraphWalker(heads, lambda sha: self[sha].parents)

    def generate_pack_contents(self, have, want, progress=None):
        """Iterate over the contents of a pack file.

        :param have: List of SHA1s of objects that should not be sent
        :param want: List of SHA1s of objects that should be sent
        :param progress: Optional progress reporting method
        """
        return self.iter_shas(self.find_missing_objects(have, want, progress))

    def peel_sha(self, sha):
        """Peel all tags from a SHA.

        :param sha: The object SHA to peel.
        :return: The fully-peeled SHA1 of a tag object, after peeling all
            intermediate tags; if the original ref does not point to a tag, this
            will equal the original SHA1.
        """
        obj = self[sha]
        obj_class = object_class(obj.type_name)
        while obj_class is Tag:
            obj_class, sha = obj.object
            obj = self[sha]
        return obj


class PackBasedObjectStore(BaseObjectStore):

    def __init__(self):
        self._pack_cache = None

    @property
    def alternates(self):
        return []

    def contains_packed(self, sha):
        """Check if a particular object is present by SHA1 and is packed."""
        for pack in self.packs:
            if sha in pack:
                return True
        return False

    def __contains__(self, sha):
        """Check if a particular object is present by SHA1 in the main or alternate stores.

        This method makes no distinction between loose and packed objects.
        """
        if self.contains_packed(sha) or self.contains_loose(sha):
            return True
        for alternate in self.alternates:
            if alternate.contains_packed(sha) or alternate.contains_loose(sha):
                return True
        return False

    def _load_packs(self):
        raise NotImplementedError(self._load_packs)

    def _pack_cache_stale(self):
        """Check whether the pack cache is stale."""
        raise NotImplementedError(self._pack_cache_stale)

    def _add_known_pack(self, pack):
        """Add a newly appeared pack to the cache by path.

        """
        if self._pack_cache is not None:
            self._pack_cache.append(pack)

    @property
    def packs(self):
        """List with pack objects."""
        if self._pack_cache is None or self._pack_cache_stale():
            self._pack_cache = self._load_packs()
        return self._pack_cache

    def _iter_alternate_objects(self):
        """Iterate over the SHAs of all the objects in alternate stores."""
        for alternate in self.alternates:
            for alternate_object in alternate:
                yield alternate_object

    def _iter_loose_objects(self):
        """Iterate over the SHAs of all loose objects."""
        raise NotImplementedError(self._iter_loose_objects)

    def _get_loose_object(self, sha):
        raise NotImplementedError(self._get_loose_object)

    def _remove_loose_object(self, sha):
        raise NotImplementedError(self._remove_loose_object)

    def pack_loose_objects(self):
        """Pack loose objects.

        :return: Number of objects packed
        """
        objects = set()
        for sha in self._iter_loose_objects():
            objects.add((self._get_loose_object(sha), None))
        self.add_objects(list(objects))
        for obj, path in objects:
            self._remove_loose_object(obj.id)
        return len(objects)

    def __iter__(self):
        """Iterate over the SHAs that are present in this store."""
        iterables = self.packs + [self._iter_loose_objects()] + [self._iter_alternate_objects()]
        return itertools.chain(*iterables)

    def contains_loose(self, sha):
        """Check if a particular object is present by SHA1 and is loose."""
        return self._get_loose_object(sha) is not None

    def get_raw(self, name):
        """Obtain the raw text for an object.

        :param name: sha for the object.
        :return: tuple with numeric type and object contents.
        """
        if len(name) == 40:
            sha = hex_to_sha(name)
            hexsha = name
        elif len(name) == 20:
            sha = name
            hexsha = None
        else:
            raise AssertionError("Invalid object name %r" % name)
        for pack in self.packs:
            try:
                return pack.get_raw(sha)
            except KeyError:
                pass
        if hexsha is None:
            hexsha = sha_to_hex(name)
        ret = self._get_loose_object(hexsha)
        if ret is not None:
            return ret.type_num, ret.as_raw_string()
        for alternate in self.alternates:
            try:
                return alternate.get_raw(hexsha)
            except KeyError:
                pass
        raise KeyError(hexsha)

    def add_objects(self, objects):
        """Add a set of objects to this object store.

        :param objects: Iterable over objects, should support __len__.
        :return: Pack object of the objects written.
        """
        if len(objects) == 0:
            # Don't bother writing an empty pack file
            return
        f, commit = self.add_pack()
        write_pack_objects(f, objects)
        return commit()


class DiskObjectStore(PackBasedObjectStore):
    """Git-style object store that exists on disk."""

    def __init__(self, path):
        """Open an object store.

        :param path: Path of the object store.
        """
        super(DiskObjectStore, self).__init__()
        self.path = path
        self.pack_dir = os.path.join(self.path, PACKDIR)
        self._pack_cache_time = 0
        self._alternates = None

    @property
    def alternates(self):
        if self._alternates is not None:
            return self._alternates
        self._alternates = []
        for path in self._read_alternate_paths():
            self._alternates.append(DiskObjectStore(path))
        return self._alternates

    def _read_alternate_paths(self):
        try:
            f = GitFile(os.path.join(self.path, "info", "alternates"),
                    'rb')
        except (OSError, IOError), e:
            if e.errno == errno.ENOENT:
                return []
            raise
        ret = []
        try:
            for l in f.readlines():
                l = l.rstrip("\n")
                if l[0] == "#":
                    continue
                if not os.path.isabs(l):
                    continue
                ret.append(l)
            return ret
        finally:
            f.close()

    def add_alternate_path(self, path):
        """Add an alternate path to this object store.
        """
        try:
            os.mkdir(os.path.join(self.path, "info"))
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        alternates_path = os.path.join(self.path, "info/alternates")
        f = GitFile(alternates_path, 'wb')
        try:
            try:
                orig_f = open(alternates_path, 'rb')
            except (OSError, IOError), e:
                if e.errno != errno.ENOENT:
                    raise
            else:
                try:
                    f.write(orig_f.read())
                finally:
                    orig_f.close()
            f.write("%s\n" % path)
        finally:
            f.close()
        self.alternates.append(DiskObjectStore(path))

    def _load_packs(self):
        pack_files = []
        try:
            self._pack_cache_time = os.stat(self.pack_dir).st_mtime
            pack_dir_contents = os.listdir(self.pack_dir)
            for name in pack_dir_contents:
                # TODO: verify that idx exists first
                if name.startswith("pack-") and name.endswith(".pack"):
                    filename = os.path.join(self.pack_dir, name)
                    pack_files.append((os.stat(filename).st_mtime, filename))
        except OSError, e:
            if e.errno == errno.ENOENT:
                return []
            raise
        pack_files.sort(reverse=True)
        suffix_len = len(".pack")
        return [Pack(f[:-suffix_len]) for _, f in pack_files]

    def _pack_cache_stale(self):
        try:
            return os.stat(self.pack_dir).st_mtime > self._pack_cache_time
        except OSError, e:
            if e.errno == errno.ENOENT:
                return True
            raise

    def _get_shafile_path(self, sha):
        # Check from object dir
        return hex_to_filename(self.path, sha)

    def _iter_loose_objects(self):
        for base in os.listdir(self.path):
            if len(base) != 2:
                continue
            for rest in os.listdir(os.path.join(self.path, base)):
                yield base+rest

    def _get_loose_object(self, sha):
        path = self._get_shafile_path(sha)
        try:
            return ShaFile.from_path(path)
        except (OSError, IOError), e:
            if e.errno == errno.ENOENT:
                return None
            raise

    def _remove_loose_object(self, sha):
        os.remove(self._get_shafile_path(sha))

    def _complete_thin_pack(self, f, path, copier, indexer):
        """Move a specific file containing a pack into the pack directory.

        :note: The file should be on the same file system as the
            packs directory.

        :param f: Open file object for the pack.
        :param path: Path to the pack file.
        :param copier: A PackStreamCopier to use for writing pack data.
        :param indexer: A PackIndexer for indexing the pack.
        """
        entries = list(indexer)

        # Update the header with the new number of objects.
        f.seek(0)
        write_pack_header(f, len(entries) + len(indexer.ext_refs()))

        # Must flush before reading (http://bugs.python.org/issue3207)
        f.flush()

        # Rescan the rest of the pack, computing the SHA with the new header.
        new_sha = compute_file_sha(f, end_ofs=-20)

        # Must reposition before writing (http://bugs.python.org/issue3207)
        f.seek(0, os.SEEK_CUR)

        # Complete the pack.
        for ext_sha in indexer.ext_refs():
            assert len(ext_sha) == 20
            type_num, data = self.get_raw(ext_sha)
            offset = f.tell()
            crc32 = write_pack_object(f, type_num, data, sha=new_sha)
            entries.append((ext_sha, offset, crc32))
        pack_sha = new_sha.digest()
        f.write(pack_sha)
        f.close()

        # Move the pack in.
        entries.sort()
        pack_base_name = os.path.join(
          self.pack_dir, 'pack-' + iter_sha1(e[0] for e in entries))
        os.rename(path, pack_base_name + '.pack')

        # Write the index.
        index_file = GitFile(pack_base_name + '.idx', 'wb')
        try:
            write_pack_index_v2(index_file, entries, pack_sha)
            index_file.close()
        finally:
            index_file.abort()

        # Add the pack to the store and return it.
        final_pack = Pack(pack_base_name)
        final_pack.check_length_and_checksum()
        self._add_known_pack(final_pack)
        return final_pack

    def add_thin_pack(self, read_all, read_some):
        """Add a new thin pack to this object store.

        Thin packs are packs that contain deltas with parents that exist outside
        the pack. They should never be placed in the object store directly, and
        always indexed and completed as they are copied.

        :param read_all: Read function that blocks until the number of requested
            bytes are read.
        :param read_some: Read function that returns at least one byte, but may
            not return the number of bytes requested.
        :return: A Pack object pointing at the now-completed thin pack in the
            objects/pack directory.
        """
        fd, path = tempfile.mkstemp(dir=self.path, prefix='tmp_pack_')
        f = os.fdopen(fd, 'w+b')

        try:
            indexer = PackIndexer(f, resolve_ext_ref=self.get_raw)
            copier = PackStreamCopier(read_all, read_some, f,
                                      delta_iter=indexer)
            copier.verify()
            return self._complete_thin_pack(f, path, copier, indexer)
        finally:
            f.close()

    def move_in_pack(self, path):
        """Move a specific file containing a pack into the pack directory.

        :note: The file should be on the same file system as the
            packs directory.

        :param path: Path to the pack file.
        """
        p = PackData(path)
        entries = p.sorted_entries()
        basename = os.path.join(self.pack_dir,
            "pack-%s" % iter_sha1(entry[0] for entry in entries))
        f = GitFile(basename+".idx", "wb")
        try:
            write_pack_index_v2(f, entries, p.get_stored_checksum())
        finally:
            f.close()
        p.close()
        os.rename(path, basename + ".pack")
        final_pack = Pack(basename)
        self._add_known_pack(final_pack)
        return final_pack

    def add_pack(self):
        """Add a new pack to this object store.

        :return: Fileobject to write to and a commit function to
            call when the pack is finished.
        """
        fd, path = tempfile.mkstemp(dir=self.pack_dir, suffix=".pack")
        f = os.fdopen(fd, 'wb')
        def commit():
            os.fsync(fd)
            f.close()
            if os.path.getsize(path) > 0:
                return self.move_in_pack(path)
            else:
                os.remove(path)
                return None
        return f, commit

    def add_object(self, obj):
        """Add a single object to this object store.

        :param obj: Object to add
        """
        dir = os.path.join(self.path, obj.id[:2])
        try:
            os.mkdir(dir)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        path = os.path.join(dir, obj.id[2:])
        if os.path.exists(path):
            return # Already there, no need to write again
        f = GitFile(path, 'wb')
        try:
            f.write(obj.as_legacy_object())
        finally:
            f.close()

    @classmethod
    def init(cls, path):
        try:
            os.mkdir(path)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        os.mkdir(os.path.join(path, "info"))
        os.mkdir(os.path.join(path, PACKDIR))
        return cls(path)


class MemoryObjectStore(BaseObjectStore):
    """Object store that keeps all objects in memory."""

    def __init__(self):
        super(MemoryObjectStore, self).__init__()
        self._data = {}

    def _to_hexsha(self, sha):
        if len(sha) == 40:
            return sha
        elif len(sha) == 20:
            return sha_to_hex(sha)
        else:
            raise ValueError("Invalid sha %r" % (sha,))

    def contains_loose(self, sha):
        """Check if a particular object is present by SHA1 and is loose."""
        return self._to_hexsha(sha) in self._data

    def contains_packed(self, sha):
        """Check if a particular object is present by SHA1 and is packed."""
        return False

    def __iter__(self):
        """Iterate over the SHAs that are present in this store."""
        return self._data.iterkeys()

    @property
    def packs(self):
        """List with pack objects."""
        return []

    def get_raw(self, name):
        """Obtain the raw text for an object.

        :param name: sha for the object.
        :return: tuple with numeric type and object contents.
        """
        obj = self[self._to_hexsha(name)]
        return obj.type_num, obj.as_raw_string()

    def __getitem__(self, name):
        return self._data[self._to_hexsha(name)]

    def __delitem__(self, name):
        """Delete an object from this store, for testing only."""
        del self._data[self._to_hexsha(name)]

    def add_object(self, obj):
        """Add a single object to this object store.

        """
        self._data[obj.id] = obj

    def add_objects(self, objects):
        """Add a set of objects to this object store.

        :param objects: Iterable over a list of objects.
        """
        for obj, path in objects:
            self._data[obj.id] = obj


class ObjectImporter(object):
    """Interface for importing objects."""

    def __init__(self, count):
        """Create a new ObjectImporter.

        :param count: Number of objects that's going to be imported.
        """
        self.count = count

    def add_object(self, object):
        """Add an object."""
        raise NotImplementedError(self.add_object)

    def finish(self, object):
        """Finish the import and write objects to disk."""
        raise NotImplementedError(self.finish)


class ObjectIterator(object):
    """Interface for iterating over objects."""

    def iterobjects(self):
        raise NotImplementedError(self.iterobjects)


class ObjectStoreIterator(ObjectIterator):
    """ObjectIterator that works on top of an ObjectStore."""

    def __init__(self, store, sha_iter):
        """Create a new ObjectIterator.

        :param store: Object store to retrieve from
        :param sha_iter: Iterator over (sha, path) tuples
        """
        self.store = store
        self.sha_iter = sha_iter
        self._shas = []

    def __iter__(self):
        """Yield tuple with next object and path."""
        for sha, path in self.itershas():
            yield self.store[sha], path

    def iterobjects(self):
        """Iterate over just the objects."""
        for o, path in self:
            yield o

    def itershas(self):
        """Iterate over the SHAs."""
        for sha in self._shas:
            yield sha
        for sha in self.sha_iter:
            self._shas.append(sha)
            yield sha

    def __contains__(self, needle):
        """Check if an object is present.

        :note: This checks if the object is present in
            the underlying object store, not if it would
            be yielded by the iterator.

        :param needle: SHA1 of the object to check for
        """
        return needle in self.store

    def __getitem__(self, key):
        """Find an object by SHA1.

        :note: This retrieves the object from the underlying
            object store. It will also succeed if the object would
            not be returned by the iterator.
        """
        return self.store[key]

    def __len__(self):
        """Return the number of objects."""
        return len(list(self.itershas()))


def tree_lookup_path(lookup_obj, root_sha, path):
    """Look up an object in a Git tree.

    :param lookup_obj: Callback for retrieving object by SHA1
    :param root_sha: SHA1 of the root tree
    :param path: Path to lookup
    :return: A tuple of (mode, SHA) of the resulting path.
    """
    tree = lookup_obj(root_sha)
    if not isinstance(tree, Tree):
        raise NotTreeError(root_sha)
    return tree.lookup_path(lookup_obj, path)


class MissingObjectFinder(object):
    """Find the objects missing from another object store.

    :param object_store: Object store containing at least all objects to be
        sent
    :param haves: SHA1s of commits not to send (already present in target)
    :param wants: SHA1s of commits to send
    :param progress: Optional function to report progress to.
    :param get_tagged: Function that returns a dict of pointed-to sha -> tag
        sha for including tags.
    :param tagged: dict of pointed-to sha -> tag sha for including tags
    """

    def __init__(self, object_store, haves, wants, progress=None,
                 get_tagged=None):
        haves = set(haves)
        self.sha_done = haves
        self.objects_to_send = set([(w, None, False) for w in wants
                                    if w not in haves])
        self.object_store = object_store
        if progress is None:
            self.progress = lambda x: None
        else:
            self.progress = progress
        self._tagged = get_tagged and get_tagged() or {}

    def add_todo(self, entries):
        self.objects_to_send.update([e for e in entries
                                     if not e[0] in self.sha_done])

    def parse_tree(self, tree):
        self.add_todo([(sha, name, not stat.S_ISDIR(mode))
                       for name, mode, sha in tree.iteritems()
                       if not S_ISGITLINK(mode)])

    def parse_commit(self, commit):
        self.add_todo([(commit.tree, "", False)])
        self.add_todo([(p, None, False) for p in commit.parents])

    def parse_tag(self, tag):
        self.add_todo([(tag.object[1], None, False)])

    def next(self):
        while True:
            if not self.objects_to_send:
                return None
            (sha, name, leaf) = self.objects_to_send.pop()
            if sha not in self.sha_done:
                break
        if not leaf:
            o = self.object_store[sha]
            if isinstance(o, Commit):
                self.parse_commit(o)
            elif isinstance(o, Tree):
                self.parse_tree(o)
            elif isinstance(o, Tag):
                self.parse_tag(o)
        if sha in self._tagged:
            self.add_todo([(self._tagged[sha], None, True)])
        self.sha_done.add(sha)
        self.progress("counting objects: %d\r" % len(self.sha_done))
        return (sha, name)


class ObjectStoreGraphWalker(object):
    """Graph walker that finds what commits are missing from an object store.

    :ivar heads: Revisions without descendants in the local repo
    :ivar get_parents: Function to retrieve parents in the local repo
    """

    def __init__(self, local_heads, get_parents):
        """Create a new instance.

        :param local_heads: Heads to start search with
        :param get_parents: Function for finding the parents of a SHA1.
        """
        self.heads = set(local_heads)
        self.get_parents = get_parents
        self.parents = {}

    def ack(self, sha):
        """Ack that a revision and its ancestors are present in the source."""
        ancestors = set([sha])

        # stop if we run out of heads to remove
        while self.heads:
            for a in ancestors:
                if a in self.heads:
                    self.heads.remove(a)

            # collect all ancestors
            new_ancestors = set()
            for a in ancestors:
                ps = self.parents.get(a)
                if ps is not None:
                    new_ancestors.update(ps)
                self.parents[a] = None

            # no more ancestors; stop
            if not new_ancestors:
                break

            ancestors = new_ancestors

    def next(self):
        """Iterate over ancestors of heads in the target."""
        if self.heads:
            ret = self.heads.pop()
            ps = self.get_parents(ret)
            self.parents[ret] = ps
            self.heads.update([p for p in ps if not p in self.parents])
            return ret
        return None
