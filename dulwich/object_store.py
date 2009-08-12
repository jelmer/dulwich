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


import itertools
import os
import stat
import tempfile
import urllib2

from dulwich.errors import (
    NotTreeError,
    )
from dulwich.objects import (
    Commit,
    ShaFile,
    Tag,
    Tree,
    hex_to_sha,
    sha_to_hex,
    S_ISGITLINK,
    )
from dulwich.pack import (
    Pack,
    PackData, 
    iter_sha1, 
    load_pack_index,
    write_pack,
    write_pack_data,
    write_pack_index_v2,
    )

PACKDIR = 'pack'


class BaseObjectStore(object):
    """Object store interface."""

    def determine_wants_all(self, refs):
	    return [sha for (ref, sha) in refs.iteritems() if not sha in self and not ref.endswith("^{}")]

    def iter_shas(self, shas):
        """Iterate over the objects for the specified shas.

        :param shas: Iterable object with SHAs
        :return: Object iterator
        """
        return ObjectStoreIterator(self, shas)

    def __contains__(self, sha):
        """Check if a particular object is present by SHA1."""
        raise NotImplementedError(self.__contains__)

    def get_raw(self, name):
        """Obtain the raw text for an object.
        
        :param name: sha for the object.
        :return: tuple with object type and object contents.
        """
        raise NotImplementedError(self.get_raw)

    def __getitem__(self, sha):
        """Obtain an object by SHA1."""
        type, uncomp = self.get_raw(sha)
        return ShaFile.from_raw_string(type, uncomp)

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

    def find_missing_objects(self, haves, wants, progress=None):
        """Find the missing objects required for a set of revisions.

        :param haves: Iterable over SHAs already in common.
        :param wants: Iterable over SHAs of objects to fetch.
        :param progress: Simple progress function that will be called with 
            updated progress strings.
        :return: Iterator over (sha, path) pairs.
        """
        return iter(MissingObjectFinder(self, haves, wants, progress).next, None)

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

    def generate_pack_contents(self, have, want):
        """Iterate over the contents of a pack file.

        :param have: List of SHA1s of objects that should not be sent
        :param want: List of SHA1s of objects that should be sent
        """
        return self.iter_shas(self.find_missing_objects(have, want))


class DiskObjectStore(BaseObjectStore):
    """Git-style object store that exists on disk."""

    def __init__(self, path):
        """Open an object store.

        :param path: Path of the object store.
        """
        self.path = path
        self._pack_cache = None
        self.pack_dir = os.path.join(self.path, PACKDIR)

    def __contains__(self, sha):
        """Check if a particular object is present by SHA1."""
        for pack in self.packs:
            if sha in pack:
                return True
        ret = self._get_shafile(sha)
        if ret is not None:
            return True
        return False

    def __iter__(self):
        """Iterate over the SHAs that are present in this store."""
        iterables = self.packs + [self._iter_shafile_shas()]
        return itertools.chain(*iterables)

    @property
    def packs(self):
        """List with pack objects."""
        if self._pack_cache is None:
            self._pack_cache = list(self._load_packs())
        return self._pack_cache

    def _load_packs(self):
        if not os.path.exists(self.pack_dir):
            return
        for name in os.listdir(self.pack_dir):
            if name.startswith("pack-") and name.endswith(".pack"):
                yield Pack(os.path.join(self.pack_dir, name[:-len(".pack")]))

    def _add_known_pack(self, path):
        """Add a newly appeared pack to the cache by path.

        """
        if self._pack_cache is not None:
            self._pack_cache.append(Pack(path))

    def _get_shafile_path(self, sha):
        dir = sha[:2]
        file = sha[2:]
        # Check from object dir
        return os.path.join(self.path, dir, file)

    def _iter_shafile_shas(self):
        for base in os.listdir(self.path):
            if len(base) != 2:
                continue
            for rest in os.listdir(os.path.join(self.path, base)):
                yield base+rest

    def _get_shafile(self, sha):
        path = self._get_shafile_path(sha)
        if os.path.exists(path):
          return ShaFile.from_file(path)
        return None

    def _add_shafile(self, sha, o):
        dir = os.path.join(self.path, sha[:2])
        if not os.path.isdir(dir):
            os.mkdir(dir)
        path = os.path.join(dir, sha[2:])
        if os.path.exists(path):
            return # Already there, no need to write again
        f = open(path, 'w+')
        try:
            f.write(o.as_legacy_object())
        finally:
            f.close()

    def get_raw(self, name):
        """Obtain the raw text for an object.
        
        :param name: sha for the object.
        :return: tuple with object type and object contents.
        """
        if len(name) == 40:
            sha = hex_to_sha(name)
            hexsha = name
        elif len(name) == 20:
            sha = name
            hexsha = None
        else:
            raise AssertionError
        for pack in self.packs:
            try:
                return pack.get_raw(sha)
            except KeyError:
                pass
        if hexsha is None: 
            hexsha = sha_to_hex(name)
        ret = self._get_shafile(hexsha)
        if ret is not None:
            return ret.type, ret.as_raw_string()
        raise KeyError(hexsha)

    def move_in_thin_pack(self, path):
        """Move a specific file containing a pack into the pack directory.

        :note: The file should be on the same file system as the 
            packs directory.

        :param path: Path to the pack file.
        """
        data = PackData(path)

        # Write index for the thin pack (do we really need this?)
        temppath = os.path.join(self.pack_dir, 
            sha_to_hex(urllib2.randombytes(20))+".tempidx")
        data.create_index_v2(temppath, self.get_raw)
        p = Pack.from_objects(data, load_pack_index(temppath))

        # Write a full pack version
        temppath = os.path.join(self.pack_dir, 
            sha_to_hex(urllib2.randombytes(20))+".temppack")
        write_pack(temppath, ((o, None) for o in p.iterobjects(self.get_raw)), 
                len(p))
        pack_sha = load_pack_index(temppath+".idx").objects_sha1()
        newbasename = os.path.join(self.pack_dir, "pack-%s" % pack_sha)
        os.rename(temppath+".pack", newbasename+".pack")
        os.rename(temppath+".idx", newbasename+".idx")
        self._add_known_pack(newbasename)

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
        write_pack_index_v2(basename+".idx", entries, p.get_stored_checksum())
        p.close()
        os.rename(path, basename + ".pack")
        self._add_known_pack(basename)

    def add_thin_pack(self):
        """Add a new thin pack to this object store.

        Thin packs are packs that contain deltas with parents that exist 
        in a different pack.
        """
        fd, path = tempfile.mkstemp(dir=self.pack_dir, suffix=".pack")
        f = os.fdopen(fd, 'wb')
        def commit():
            os.fsync(fd)
            f.close()
            if os.path.getsize(path) > 0:
                self.move_in_thin_pack(path)
        return f, commit

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
                self.move_in_pack(path)
        return f, commit

    def add_object(self, obj):
        """Add a single object to this object store.

        :param obj: Object to add
        """
        self._add_shafile(obj.id, obj)

    def add_objects(self, objects):
        """Add a set of objects to this object store.

        :param objects: Iterable over objects, should support __len__.
        """
        if len(objects) == 0:
            # Don't bother writing an empty pack file
            return
        f, commit = self.add_pack()
        write_pack_data(f, objects, len(objects))
        commit()


class MemoryObjectStore(BaseObjectStore):
    """Object store that keeps all objects in memory."""

    def __init__(self):
        super(MemoryObjectStore, self).__init__()
        self._data = {}

    def __contains__(self, sha):
        """Check if the object with a particular SHA is present."""
        return sha in self._data

    def __iter__(self):
        """Iterate over the SHAs that are present in this store."""
        return self._data.iterkeys()

    def get_raw(self, name):
        """Obtain the raw text for an object.
        
        :param name: sha for the object.
        :return: tuple with object type and object contents.
        """
        return self[name].as_raw_string()

    def __getitem__(self, name):
        return self._data[name]

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
        """Finish the imoprt and write objects to disk."""
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
    """Lookup an object in a Git tree.

    :param lookup_obj: Callback for retrieving object by SHA1
    :param root_sha: SHA1 of the root tree
    :param path: Path to lookup
    """
    parts = path.split("/")
    sha = root_sha
    for p in parts:
        obj = lookup_obj(sha)
        if type(obj) is not Tree:
            raise NotTreeError(sha)
        if p == '':
            continue
        mode, sha = obj[p]
    return mode, sha


class MissingObjectFinder(object):
    """Find the objects missing from another object store.

    :param object_store: Object store containing at least all objects to be 
        sent
    :param haves: SHA1s of commits not to send (already present in target)
    :param wants: SHA1s of commits to send
    :param progress: Optional function to report progress to.
    """

    def __init__(self, object_store, haves, wants, progress=None):
        self.sha_done = set(haves)
        self.objects_to_send = set([(w, None, False) for w in wants if w not in haves])
        self.object_store = object_store
        if progress is None:
            self.progress = lambda x: None
        else:
            self.progress = progress

    def add_todo(self, entries):
        self.objects_to_send.update([e for e in entries if not e[0] in self.sha_done])

    def parse_tree(self, tree):
        self.add_todo([(sha, name, not stat.S_ISDIR(mode)) for (mode, name, sha) in tree.entries() if not S_ISGITLINK(mode)])

    def parse_commit(self, commit):
        self.add_todo([(commit.tree, "", False)])
        self.add_todo([(p, None, False) for p in commit.parents])

    def parse_tag(self, tag):
        self.add_todo([(tag.object[1], None, False)])

    def next(self):
        if not self.objects_to_send:
            return None
        (sha, name, leaf) = self.objects_to_send.pop()
        if not leaf:
            o = self.object_store[sha]
            if isinstance(o, Commit):
                self.parse_commit(o)
            elif isinstance(o, Tree):
                self.parse_tree(o)
            elif isinstance(o, Tag):
                self.parse_tag(o)
        self.sha_done.add(sha)
        self.progress("counting objects: %d\r" % len(self.sha_done))
        return (sha, name)


class ObjectStoreGraphWalker(object):
    """Graph walker that finds out what commits are missing from an object store."""

    def __init__(self, local_heads, get_parents):
        """Create a new instance.

        :param local_heads: Heads to start search with
        :param get_parents: Function for finding the parents of a SHA1.
        """
        self.heads = set(local_heads)
        self.get_parents = get_parents
        self.parents = {}

    def ack(self, sha):
        """Ack that a particular revision and its ancestors are present in the source."""
        if sha in self.heads:
            self.heads.remove(sha)
        if sha in self.parents:
            for p in self.parents[sha]:
                self.ack(p)

    def next(self):
        """Iterate over ancestors of heads in the target."""
        if self.heads:
            ret = self.heads.pop()
            ps = self.get_parents(ret)
            self.parents[ret] = ps
            self.heads.update(ps)
            return ret
        return None
