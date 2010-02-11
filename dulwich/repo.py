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


import errno
import os

from dulwich.errors import (
    MissingCommitError, 
    NotBlobError, 
    NotCommitError, 
    NotGitRepository,
    NotTreeError, 
    PackedRefsException,
    )
from dulwich.file import (
    ensure_dir_exists,
    GitFile,
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
    hex_to_sha,
    )

OBJECTDIR = 'objects'
SYMREF = 'ref: '
REFSDIR = 'refs'
REFSDIR_TAGS = 'tags'
REFSDIR_HEADS = 'heads'
INDEX_FILENAME = "index"


def check_ref_format(refname):
    """Check if a refname is correctly formatted.

    Implements all the same rules as git-check-ref-format[1].

    [1] http://www.kernel.org/pub/software/scm/git/docs/git-check-ref-format.html

    :param refname: The refname to check
    :return: True if refname is valid, False otherwise
    """
    # These could be combined into one big expression, but are listed separately
    # to parallel [1].
    if '/.' in refname or refname.startswith('.'):
        return False
    if '/' not in refname:
        return False
    if '..' in refname:
        return False
    for c in refname:
        if ord(c) < 040 or c in '\177 ~^:?*[':
            return False
    if refname[-1] in '/.':
        return False
    if refname.endswith('.lock'):
        return False
    if '@{' in refname:
        return False
    if '\\' in refname:
        return False
    return True


class RefsContainer(object):
    """A container for refs."""

    def as_dict(self, base):
        """Return the contents of this ref container under base as a dict."""
        raise NotImplementedError(self.as_dict)

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
        self._packed_refs = None
        self._peeled_refs = {}

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.path)

    def keys(self, base=None):
        """Refs present in this container.

        :param base: An optional base to return refs under
        :return: An unsorted set of valid refs in this container, including
            packed refs.
        """
        if base is not None:
            return self.subkeys(base)
        else:
            return self.allkeys()

    def subkeys(self, base):
        keys = set()
        path = self.refpath(base)
        for root, dirs, files in os.walk(path):
            dir = root[len(path):].strip(os.path.sep).replace(os.path.sep, "/")
            for filename in files:
                refname = ("%s/%s" % (dir, filename)).strip("/")
                # check_ref_format requires at least one /, so we prepend the
                # base before calling it.
                if check_ref_format("%s/%s" % (base, refname)):
                    keys.add(refname)
        for key in self.get_packed_refs():
            if key.startswith(base):
                keys.add(key[len(base):].strip("/"))
        return keys

    def allkeys(self):
        keys = set()
        if os.path.exists(self.refpath("HEAD")):
            keys.add("HEAD")
        path = self.refpath("")
        for root, dirs, files in os.walk(self.refpath("refs")):
            dir = root[len(path):].strip(os.path.sep).replace(os.path.sep, "/")
            for filename in files:
                refname = ("%s/%s" % (dir, filename)).strip("/")
                if check_ref_format(refname):
                    keys.add(refname)
        keys.update(self.get_packed_refs())
        return keys

    def as_dict(self, base=None):
        """Return the contents of this container as a dictionary.

        """
        ret = {}
        keys = self.keys(base)
        if base is None:
            base = ""
        for key in keys:
            try:
                ret[key] = self[("%s/%s" % (base, key)).strip("/")]
            except KeyError:
                continue # Unable to resolve

        return ret

    def refpath(self, name):
        """Return the disk path of a ref.

        """
        if os.path.sep != "/":
            name = name.replace("/", os.path.sep)
        return os.path.join(self.path, name)

    def get_packed_refs(self):
        """Get contents of the packed-refs file.

        :return: Dictionary mapping ref names to SHA1s

        :note: Will return an empty dictionary when no packed-refs file is
            present.
        """
        # TODO: invalidate the cache on repacking
        if self._packed_refs is None:
            self._packed_refs = {}
            path = os.path.join(self.path, 'packed-refs')
            try:
                f = GitFile(path, 'rb')
            except IOError, e:
                if e.errno == errno.ENOENT:
                    return {}
                raise
            try:
                first_line = iter(f).next().rstrip()
                if (first_line.startswith("# pack-refs") and " peeled" in
                        first_line):
                    for sha, name, peeled in read_packed_refs_with_peeled(f):
                        self._packed_refs[name] = sha
                        if peeled:
                            self._peeled_refs[name] = peeled
                else:
                    f.seek(0)
                    for sha, name in read_packed_refs(f):
                        self._packed_refs[name] = sha
            finally:
                f.close()
        return self._packed_refs

    def _check_refname(self, name):
        """Ensure a refname is valid and lives in refs or is HEAD.

        HEAD is not a valid refname according to git-check-ref-format, but this
        class needs to be able to touch HEAD. Also, check_ref_format expects
        refnames without the leading 'refs/', but this class requires that
        so it cannot touch anything outside the refs dir (or HEAD).

        :param name: The name of the reference.
        :raises KeyError: if a refname is not HEAD or is otherwise not valid.
        """
        if name == 'HEAD':
            return
        if not name.startswith('refs/') or not check_ref_format(name[5:]):
            raise KeyError(name)

    def _read_ref_file(self, name):
        """Read a reference file and return its contents.

        If the reference file a symbolic reference, only read the first line of
        the file. Otherwise, only read the first 40 bytes.

        :param name: the refname to read, relative to refpath
        :return: The contents of the ref file, or None if the file does not
            exist.
        :raises IOError: if any other error occurs
        """
        filename = self.refpath(name)
        try:
            f = GitFile(filename, 'rb')
            try:
                header = f.read(len(SYMREF))
                if header == SYMREF:
                    # Read only the first line
                    return header + iter(f).next().rstrip("\n")
                else:
                    # Read only the first 40 bytes
                    return header + f.read(40-len(SYMREF))
            finally:
                f.close()
        except IOError, e:
            if e.errno == errno.ENOENT:
                return None
            raise

    def _follow(self, name):
        """Follow a reference name.

        :return: a tuple of (refname, sha), where refname is the name of the
            last reference in the symbolic reference chain
        """
        self._check_refname(name)
        contents = SYMREF + name
        depth = 0
        while contents.startswith(SYMREF):
            refname = contents[len(SYMREF):]
            contents = self._read_ref_file(refname)
            if not contents:
                contents = self.get_packed_refs().get(refname, None)
                if not contents:
                    break
            depth += 1
            if depth > 5:
                raise KeyError(name)
        return refname, contents

    def __getitem__(self, name):
        """Get the SHA1 for a reference name.

        This method follows all symbolic references.
        """
        _, sha = self._follow(name)
        if sha is None:
            raise KeyError(name)
        return sha

    def _remove_packed_ref(self, name):
        if self._packed_refs is None:
            return
        filename = os.path.join(self.path, 'packed-refs')
        # reread cached refs from disk, while holding the lock
        f = GitFile(filename, 'wb')
        try:
            self._packed_refs = None
            self.get_packed_refs()

            if name not in self._packed_refs:
                return

            del self._packed_refs[name]
            if name in self._peeled_refs:
                del self._peeled_refs[name]
            write_packed_refs(f, self._packed_refs, self._peeled_refs)
            f.close()
        finally:
            f.abort()

    def set_if_equals(self, name, old_ref, new_ref):
        """Set a refname to new_ref only if it currently equals old_ref.

        This method follows all symbolic references, and can be used to perform
        an atomic compare-and-swap operation.

        :param name: The refname to set.
        :param old_ref: The old sha the refname must refer to, or None to set
            unconditionally.
        :param new_ref: The new sha the refname will refer to.
        :return: True if the set was successful, False otherwise.
        """
        try:
            realname, _ = self._follow(name)
        except KeyError:
            realname = name
        filename = self.refpath(realname)
        ensure_dir_exists(os.path.dirname(filename))
        f = GitFile(filename, 'wb')
        try:
            if old_ref is not None:
                try:
                    # read again while holding the lock
                    orig_ref = self._read_ref_file(realname)
                    if orig_ref is None:
                        orig_ref = self.get_packed_refs().get(realname, None)
                    if orig_ref != old_ref:
                        f.abort()
                        return False
                except (OSError, IOError):
                    f.abort()
                    raise
            try:
                f.write(new_ref+"\n")
            except (OSError, IOError):
                f.abort()
                raise
        finally:
            f.close()
        return True

    def add_if_new(self, name, ref):
        """Add a new reference only if it does not already exist."""
        self._check_refname(name)
        filename = self.refpath(name)
        ensure_dir_exists(os.path.dirname(filename))
        f = GitFile(filename, 'wb')
        try:
            if os.path.exists(filename) or name in self.get_packed_refs():
                f.abort()
                return False
            try:
                f.write(ref+"\n")
            except (OSError, IOError):
                f.abort()
                raise
        finally:
            f.close()
        return True

    def __setitem__(self, name, ref):
        """Set a reference name to point to the given SHA1.

        This method follows all symbolic references.

        :note: This method unconditionally overwrites the contents of a reference
            on disk. To update atomically only if the reference has not changed
            on disk, use set_if_equals().
        """
        self.set_if_equals(name, None, ref)

    def remove_if_equals(self, name, old_ref):
        """Remove a refname only if it currently equals old_ref.

        This method does not follow symbolic references. It can be used to
        perform an atomic compare-and-delete operation.

        :param name: The refname to delete.
        :param old_ref: The old sha the refname must refer to, or None to delete
            unconditionally.
        :return: True if the delete was successful, False otherwise.
        """
        self._check_refname(name)
        filename = self.refpath(name)
        ensure_dir_exists(os.path.dirname(filename))
        f = GitFile(filename, 'wb')
        try:
            if old_ref is not None:
                orig_ref = self._read_ref_file(name)
                if orig_ref is None:
                    orig_ref = self.get_packed_refs().get(name, None)
                if orig_ref != old_ref:
                    return False
            # may only be packed
            if os.path.exists(filename):
                os.remove(filename)
            self._remove_packed_ref(name)
        finally:
            # never write, we just wanted the lock
            f.abort()
        return True

    def __delitem__(self, name):
        """Remove a refname.

        This method does not follow symbolic references.
        :note: This method unconditionally deletes the contents of a reference
            on disk. To delete atomically only if the reference has not changed
            on disk, use set_if_equals().
        """
        self.remove_if_equals(name, None)


def _split_ref_line(line):
    """Split a single ref line into a tuple of SHA1 and name."""
    fields = line.rstrip("\n").split(" ")
    if len(fields) != 2:
        raise PackedRefsException("invalid ref line '%s'" % line)
    sha, name = fields
    try:
        hex_to_sha(sha)
    except (AssertionError, TypeError), e:
        raise PackedRefsException(e)
    if not check_ref_format(name):
        raise PackedRefsException("invalid ref name '%s'" % name)
    return (sha, name)


def read_packed_refs(f):
    """Read a packed refs file.

    Yields tuples with SHA1s and ref names.

    :param f: file-like object to read from
    """
    for l in f:
        if l[0] == "#":
            # Comment
            continue
        if l[0] == "^":
            raise PackedRefsException(
                "found peeled ref in packed-refs without peeled")
        yield _split_ref_line(l)


def read_packed_refs_with_peeled(f):
    """Read a packed refs file including peeled refs.

    Assumes the "# pack-refs with: peeled" line was already read. Yields tuples
    with ref names, SHA1s, and peeled SHA1s (or None).

    :param f: file-like object to read from, seek'ed to the second line
    """
    last = None
    for l in f:
        if l[0] == "#":
            continue
        l = l.rstrip("\n")
        if l[0] == "^":
            if not last:
                raise PackedRefsException("unexpected peeled ref line")
            try:
                hex_to_sha(l[1:])
            except (AssertionError, TypeError), e:
                raise PackedRefsException(e)
            sha, name = _split_ref_line(last)
            last = None
            yield (sha, name, l[1:])
        else:
            if last:
                sha, name = _split_ref_line(last)
                yield (sha, name, None)
            last = l
    if last:
        sha, name = _split_ref_line(last)
        yield (sha, name, None)


def write_packed_refs(f, packed_refs, peeled_refs=None):
    """Write a packed refs file.

    :param f: empty file-like object to write to
    :param packed_refs: dict of refname to sha of packed refs to write
    """
    if peeled_refs is None:
        peeled_refs = {}
    else:
        f.write('# pack-refs with: peeled\n')
    for refname in sorted(packed_refs.iterkeys()):
        f.write('%s %s\n' % (packed_refs[refname], refname))
        if refname in peeled_refs:
            f.write('^%s\n' % peeled_refs[refname])


class BaseRepo(object):
    """Base class for a git repository.

    :ivar object_store: Dictionary-like object for accessing
        the objects
    :ivar refs: Dictionary-like object with the refs in this repository
    """

    def __init__(self, object_store, refs):
        self.object_store = object_store
        self.refs = refs

    def get_named_file(self, path):
        """Get a file from the control dir with a specific name.

        Although the filename should be interpreted as a filename relative to
        the control dir in a disk-baked Repo, the object returned need not be
        pointing to a file in that location.

        :param path: The path to the file, relative to the control dir.
        :return: An open file object, or None if the file does not exist.
        """
        raise NotImplementedError(self.get_named_file)

    def put_named_file(self, relpath, contents):
        """Write a file in the control directory with specified name and 
        contents.

        Although the filename should be interpreted as a filename relative to
        the control dir in a disk-baked Repo, the object returned need not be
        pointing to a file in that location.

        :param path: The path to the file, relative to the control dir.
        :param contents: Contents of the new file
        """
        raise NotImplementedError(self.put_named_file)

    def fetch(self, target, determine_wants=None, progress=None):
        """Fetch objects into another repository.

        :param target: The target repository
        :param determine_wants: Optional function to determine what refs to 
            fetch.
        :param progress: Optional progress function
        """
        if determine_wants is None:
            determine_wants = lambda heads: heads.values()
        target.object_store.add_objects(
            self.fetch_objects(determine_wants, target.get_graph_walker(),
                progress))
        return self.get_refs()

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
        return self.refs[name]

    def get_refs(self):
        """Get dictionary with all refs."""
        return self.refs.as_dict()

    def head(self):
        """Return the SHA1 pointed at by HEAD."""
        return self.refs['HEAD']

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

    def get_config(self):
        from configobj import ConfigObj
        return ConfigObj(os.path.join(self._controldir, 'config'))

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


class Repo(BaseRepo):
    """A git repository backed by local disk."""

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
        object_store = DiskObjectStore(
            os.path.join(self.controldir(), OBJECTDIR))
        refs = DiskRefsContainer(self.controldir())
        BaseRepo.__init__(self, object_store, refs)

    def controldir(self):
        """Return the path of the control directory."""
        return self._controldir

    def put_named_file(self, path, contents):
        """Write a file from the control dir with a specific name and contents.
        """
        f = GitFile(os.path.join(self.controldir(), path, 'config'), 'wb')
        try:
            f.write(contents)
        finally:
            f.close()

    def get_named_file(self, path):
        """Get a file from the control dir with a specific name.

        Although the filename should be interpreted as a filename relative to
        the control dir in a disk-baked Repo, the object returned need not be
        pointing to a file in that location.

        :param path: The path to the file, relative to the control dir.
        :return: An open file object, or None if the file does not exist.
        """
        try:
            return open(os.path.join(self.controldir(), path.lstrip('/')), 'rb')
        except (IOError, OSError), e:
            if e.errno == errno.ENOENT:
                return None
            raise

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

    def __repr__(self):
        return "<Repo at %r>" % self.path

    def do_commit(self, committer, message,
                  author=None, commit_timestamp=None,
                  commit_timezone=None, author_timestamp=None, 
                  author_timezone=None, tree=None):
        """Create a new commit.

        :param committer: Committer fullname
        :param message: Commit message
        :param author: Author fullname (defaults to committer)
        :param commit_timestamp: Commit timestamp (defaults to now)
        :param commit_timezone: Commit timestamp timezone (defaults to GMT)
        :param author_timestamp: Author timestamp (defaults to commit timestamp)
        :param author_timezone: Author timestamp timezone 
            (defaults to commit timestamp timezone)
        :param tree: SHA1 of the tree root to use (if not specified the current index will be committed).
        :return: New commit SHA1
        """
        from dulwich.index import commit_index
        import time
        index = self.open_index()
        c = Commit()
        if tree is None:
            c.tree = commit_index(self.object_store, index)
        else:
            c.tree = tree
        c.committer = committer
        if commit_timestamp is None:
            commit_timestamp = time.time()
        c.commit_time = int(commit_timestamp)
        if commit_timezone is None:
            commit_timezone = 0
        c.commit_timezone = commit_timezone
        if author is None:
            author = committer
        c.author = author
        if author_timestamp is None:
            author_timestamp = commit_timestamp
        c.author_time = int(author_timestamp)
        if author_timezone is None:
            author_timezone = commit_timezone
        c.author_timezone = author_timezone
        c.message = message
        self.object_store.add_object(c)
        self.refs["HEAD"] = c.id
        return c.id

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
        ret.put_named_file('description', "Unnamed repository")
        ret.put_named_file('config', """[core]
    repositoryformatversion = 0
    filemode = true
    bare = false
    logallrefupdates = true
""")
        ret.put_named_file(os.path.join('info', 'excludes'), '')
        return ret

    create = init_bare
