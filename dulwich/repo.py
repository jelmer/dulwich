# repo.py -- For dealing wih git repositories.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of 
# the License.
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


"""Repository access."""


import os
import stat

from dulwich.errors import (
    MissingCommitError, 
    NotBlobError, 
    NotCommitError, 
    NotGitRepository,
    NotTreeError, 
    )
from dulwich.object_store import (
    DiskObjectStore,
    )
from dulwich.objects import (
    Blob,
    Commit,
    ShaFile,
    Tag,
    Tree,
    )

OBJECTDIR = 'objects'
SYMREF = 'ref: '
REFSDIR = 'refs'
REFSDIR_TAGS = 'tags'
REFSDIR_HEADS = 'heads'
INDEX_FILENAME = "index"


def follow_ref(container, name):
    """Follow a ref back to a SHA1.
    
    :param container: Ref container to use for looking up refs.
    :param name: Name of the original ref.
    """
    contents = container[name]
    if contents.startswith(SYMREF):
        ref = contents[len(SYMREF):]
        if ref[-1] == '\n':
            ref = ref[:-1]
        return follow_ref(container, ref)
    assert len(contents) == 40, 'Invalid ref in %s' % name
    return contents


class RefsContainer(object):
    """A container for refs."""

    def as_dict(self, base):
        """Return the contents of this ref container under base as a dict."""
        raise NotImplementedError(self.as_dict)

    def follow(self, name):
        """Follow a ref name back to a SHA1.
        
        :param name: Name of the ref
        """
        return follow_ref(self, name)

    def set_ref(self, name, other):
        """Make a ref point at another ref.

        :param name: Name of the ref to set
        :param other: Name of the ref to point at
        """
        self[name] = "ref: %s\n" % other

    def import_refs(self, base, other):
        for name, value in other.iteritems():
            self["%s/%s" % (base, name)] = value


class DiskRefsContainer(RefsContainer):
    """Refs container that reads refs from disk."""

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.path)

    def keys(self, base=None):
        """Refs present in this container."""
        return list(self.iterkeys(base))

    def iterkeys(self, base=None):
        if base is not None:
            return self.itersubkeys(base)
        else:
            return self.iterallkeys()

    def itersubkeys(self, base):
        path = self.refpath(base)
        for root, dirs, files in os.walk(path):
            dir = root[len(path):].strip("/").replace(os.path.sep, "/")
            for filename in files:
                yield ("%s/%s" % (dir, filename)).strip("/")

    def iterallkeys(self):
        if os.path.exists(self.refpath("HEAD")):
            yield "HEAD"
        path = self.refpath("")
        for root, dirs, files in os.walk(self.refpath("refs")):
            dir = root[len(path):].strip("/").replace(os.path.sep, "/")
            for filename in files:
                yield ("%s/%s" % (dir, filename)).strip("/")

    def as_dict(self, base=None, follow=True):
        """Return the contents of this container as a dictionary.

        """
        ret = {}
        if base is None:
            keys = self.iterkeys()
            base = ""
        else:
            keys = self.itersubkeys(base)
        for key in keys:
                if follow:
                    try:
                        ret[key] = self.follow(("%s/%s" % (base, key)).strip("/"))
                    except KeyError:
                        continue # Unable to resolve
                else:
                    ret[key] = self[("%s/%s" % (base, key)).strip("/")]
        return ret

    def refpath(self, name):
        """Return the disk path of a ref.

        """
        if os.path.sep != "/":
            name = name.replace("/", os.path.sep)
        return os.path.join(self.path, name)

    def __getitem__(self, name):
        file = self.refpath(name)
        if not os.path.exists(file):
            raise KeyError(name)
        f = open(file, 'rb')
        try:
            return f.read().strip("\n")
        finally:
            f.close()

    def __setitem__(self, name, ref):
        file = self.refpath(name)
        dirpath = os.path.dirname(file)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        f = open(file, 'wb')
        try:
            f.write(ref+"\n")
        finally:
            f.close()

    def __delitem__(self, name):
        file = self.refpath(name)
        if os.path.exists(file):
            os.remove(file)


def read_packed_refs(f):
    """Read a packed refs file.

    Yields tuples with ref names and SHA1s.

    :param f: file-like object to read from
    """
    l = f.readline()
    for l in f.readlines():
        if l[0] == "#":
            # Comment
            continue
        if l[0] == "^":
            # FIXME: Return somehow
            continue
        yield tuple(l.rstrip("\n").split(" ", 2))


class Repo(object):
    """A local git repository.
    
    :ivar refs: Dictionary with the refs in this repository
    :ivar object_store: Dictionary-like object for accessing
        the objects
    """

    def __init__(self, root):
        if os.path.isdir(os.path.join(root, ".git", OBJECTDIR)):
            self.bare = False
            self._controldir = os.path.join(root, ".git")
        elif (os.path.isdir(os.path.join(root, OBJECTDIR)) and
              os.path.isdir(os.path.join(root, REFSDIR))):
            self.bare = True
            self._controldir = root
        else:
            raise NotGitRepository(root)
        self.path = root
        self.refs = DiskRefsContainer(self.controldir())
        self.object_store = DiskObjectStore(
            os.path.join(self.controldir(), OBJECTDIR))

    def controldir(self):
        """Return the path of the control directory."""
        return self._controldir

    def index_path(self):
        """Return path to the index file."""
        return os.path.join(self.controldir(), INDEX_FILENAME)

    def open_index(self):
        """Open the index for this repository."""
        from dulwich.index import Index
        return Index(self.index_path())

    def has_index(self):
        """Check if an index is present."""
        return os.path.exists(self.index_path())

    def fetch_objects(self, determine_wants, graph_walker, progress):
        """Fetch the missing objects required for a set of revisions.

        :param determine_wants: Function that takes a dictionary with heads 
            and returns the list of heads to fetch.
        :param graph_walker: Object that can iterate over the list of revisions 
            to fetch and has an "ack" method that will be called to acknowledge 
            that a revision is present.
        :param progress: Simple progress function that will be called with 
            updated progress strings.
        :return: iterator over objects, with __len__ implemented
        """
        wants = determine_wants(self.get_refs())
        haves = self.object_store.find_common_revisions(graph_walker)
        return self.object_store.iter_shas(
            self.object_store.find_missing_objects(haves, wants, progress))

    def get_graph_walker(self, heads=None):
        if heads is None:
            heads = self.refs.as_dict('refs/heads').values()
        return self.object_store.get_graph_walker(heads)

    def ref(self, name):
        """Return the SHA1 a ref is pointing to."""
        try:
            return self.refs.follow(name)
        except KeyError:
            return self.get_packed_refs()[name]

    def get_refs(self):
        """Get dictionary with all refs."""
        ret = {}
        try:
            if self.head():
                ret['HEAD'] = self.head()
        except KeyError:
            pass
        ret.update(self.refs.as_dict())
        ret.update(self.get_packed_refs())
        return ret

    def get_packed_refs(self):
        """Get contents of the packed-refs file.

        :return: Dictionary mapping ref names to SHA1s

        :note: Will return an empty dictionary when no packed-refs file is 
            present.
        """
        path = os.path.join(self.controldir(), 'packed-refs')
        if not os.path.exists(path):
            return {}
        ret = {}
        f = open(path, 'rb')
        try:
            for entry in read_packed_refs(f):
                ret[entry[1]] = entry[0]
            return ret
        finally:
            f.close()

    def head(self):
        """Return the SHA1 pointed at by HEAD."""
        return self.refs.follow('HEAD')

    def _get_object(self, sha, cls):
        assert len(sha) in (20, 40)
        ret = self.get_object(sha)
        if ret._type != cls._type:
            if cls is Commit:
                raise NotCommitError(ret)
            elif cls is Blob:
                raise NotBlobError(ret)
            elif cls is Tree:
                raise NotTreeError(ret)
            else:
                raise Exception("Type invalid: %r != %r" % (ret._type, cls._type))
        return ret

    def get_object(self, sha):
        return self.object_store[sha]

    def get_parents(self, sha):
        return self.commit(sha).parents

    def commit(self, sha):
        return self._get_object(sha, Commit)

    def tree(self, sha):
        return self._get_object(sha, Tree)

    def tag(self, sha):
        return self._get_object(sha, Tag)

    def get_blob(self, sha):
        return self._get_object(sha, Blob)

    def revision_history(self, head):
        """Returns a list of the commits reachable from head.

        Returns a list of commit objects. the first of which will be the commit
        of head, then following theat will be the parents.

        Raises NotCommitError if any no commits are referenced, including if the
        head parameter isn't the sha of a commit.

        XXX: work out how to handle merges.
        """
        # We build the list backwards, as parents are more likely to be older
        # than children
        pending_commits = [head]
        history = []
        while pending_commits != []:
            head = pending_commits.pop(0)
            try:
                commit = self.commit(head)
            except KeyError:
                raise MissingCommitError(head)
            if commit in history:
                continue
            i = 0
            for known_commit in history:
                if known_commit.commit_time > commit.commit_time:
                    break
                i += 1
            history.insert(i, commit)
            parents = commit.parents
            pending_commits += parents
        history.reverse()
        return history

    def __repr__(self):
        return "<Repo at %r>" % self.path

    def __getitem__(self, name):
        if len(name) in (20, 40):
            return self.object_store[name]
        return self.object_store[self.refs[name]]
    
    def __setitem__(self, name, value):
        if name.startswith("refs/") or name == "HEAD":
            if isinstance(value, ShaFile):
                self.refs[name] = value.id
            elif isinstance(value, str):
                self.refs[name] = value
            else:
                raise TypeError(value)
        raise ValueError(name)

    def __delitem__(self, name):
        if name.startswith("refs") or name == "HEAD":
            del self.refs[name]
        raise ValueError(name)

    @classmethod
    def init(cls, path, mkdir=True):
        controldir = os.path.join(path, ".git")
        os.mkdir(controldir)
        cls.init_bare(controldir)
        return cls(path)

    @classmethod
    def init_bare(cls, path, mkdir=True):
        for d in [[OBJECTDIR], 
                  [OBJECTDIR, "info"], 
                  [OBJECTDIR, "pack"],
                  ["branches"],
                  [REFSDIR],
                  [REFSDIR, REFSDIR_TAGS],
                  [REFSDIR, REFSDIR_HEADS],
                  ["hooks"],
                  ["info"]]:
            os.mkdir(os.path.join(path, *d))
        ret = cls(path)
        ret.refs.set_ref("HEAD", "refs/heads/master")
        open(os.path.join(path, 'description'), 'wb').write("Unnamed repository")
        open(os.path.join(path, 'info', 'excludes'), 'wb').write("")
        return ret

    create = init_bare

