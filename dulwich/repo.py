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


class RefsContainer(object):

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.path)

    def refpath(self, name):
        return os.path.join(self.path, name)

    def __setitem__(self, name, ref):
        f = open(self.refpath(name), 'wb')
        try:
            f.write("%s\n" % ref)
        finally:
            f.close()


class Tags(RefsContainer):
    """Tags container."""

    def __init__(self, tagdir, tags):
        super(Tags, self).__init__(tagdir)
        self.tags = tags

    def __setitem__(self, name, value):
        super(Tags, self)[name] = value
        self.tags[name] = value

    def __getitem__(self, name):
        return self.tags[name]
    
    def __len__(self):
        return len(self.tags)

    def iteritems(self):
        for k in self.tags:
            yield k, self[k]


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
    """A local git repository."""

    ref_locs = ['', REFSDIR, 'refs/tags', 'refs/heads', 'refs/remotes']

    def __init__(self, root):
        if os.path.isdir(os.path.join(root, ".git", OBJECTDIR)):
            self.bare = False
            self._controldir = os.path.join(root, ".git")
        elif os.path.isdir(os.path.join(root, OBJECTDIR)):
            self.bare = True
            self._controldir = root
        else:
            raise NotGitRepository(root)
        self.path = root
        self.tags = Tags(self.tagdir(), self.get_tags())
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
        :return: tuple with number of objects, iterator over objects
        """
        wants = determine_wants(self.get_refs())
        haves = self.object_store.find_missing_revisions(graphwalker)
        return self.object_store.iter_shas(
            self.object_store.find_missing_objects(haves, wants, progress))

    def get_graph_walker(self, heads=None):
        if heads is None:
            heads = self.heads().values()
        return self.object_store.get_graph_walker(heads)

    def _get_ref(self, file):
        f = open(file, 'rb')
        try:
            contents = f.read()
            if contents.startswith(SYMREF):
                ref = contents[len(SYMREF):]
                if ref[-1] == '\n':
                    ref = ref[:-1]
                return self.ref(ref)
            assert len(contents) == 41, 'Invalid ref in %s' % file
            return contents[:-1]
        finally:
            f.close()

    def ref(self, name):
        """Return the SHA1 a ref is pointing to."""
        for dir in self.ref_locs:
            file = os.path.join(self.controldir(), dir, name)
            if os.path.exists(file):
                return self._get_ref(file)
        packed_refs = self.get_packed_refs()
        if name in packed_refs:
            return packed_refs[name]

    def get_refs(self):
        """Get dictionary with all refs."""
        ret = {}
        if self.head():
            ret['HEAD'] = self.head()
        for dir in ["refs/heads", "refs/tags"]:
            for name in os.listdir(os.path.join(self.controldir(), dir)):
                path = os.path.join(self.controldir(), dir, name)
                if os.path.isfile(path):
                    ret["/".join([dir, name])] = self._get_ref(path)
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
        f = open(path, 'r')
        try:
            for entry in read_packed_refs(f):
                ret[entry[1]] = entry[0]
            return ret
        finally:
            f.close()

    def set_ref(self, name, value):
        """Set a new ref.

        :param name: Name of the ref
        :param value: SHA1 to point at
        """
        file = os.path.join(self.controldir(), name)
        dirpath = os.path.dirname(file)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        f = open(file, 'w')
        try:
            f.write(value+"\n")
        finally:
            f.close()

    def remove_ref(self, name):
        """Remove a ref.

        :param name: Name of the ref
        """
        file = os.path.join(self.controldir(), name)
        if os.path.exists(file):
            os.remove(file)

    def tagdir(self):
        """Tag directory."""
        return os.path.join(self.controldir(), REFSDIR, REFSDIR_TAGS)

    def get_tags(self):
        ret = {}
        for root, dirs, files in os.walk(self.tagdir()):
            for name in files:
                ret[name] = self._get_ref(os.path.join(root, name))
        return ret

    def heads(self):
        """Return dictionary with heads."""
        ret = {}
        for root, dirs, files in os.walk(os.path.join(self.controldir(), REFSDIR, REFSDIR_HEADS)):
            for name in files:
                ret[name] = self._get_ref(os.path.join(root, name))
        return ret

    def head(self):
        """Return the SHA1 pointed at by HEAD."""
        return self.ref('HEAD')

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
        return self.object_store.get_commit_parents(sha)

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
        open(os.path.join(path, 'HEAD'), 'w').write("ref: refs/heads/master\n")
        open(os.path.join(path, 'description'), 'w').write("Unnamed repository")
        open(os.path.join(path, 'info', 'excludes'), 'w').write("")
        return cls(path)

    create = init_bare

