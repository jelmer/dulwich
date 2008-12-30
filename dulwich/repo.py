# repo.py -- For dealing wih git repositories.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License.
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

import os

from commit import Commit
from errors import (
        MissingCommitError, 
        NotBlobError, 
        NotCommitError, 
        NotGitRepository,
        NotTreeError, 
        )
from objects import (
        ShaFile,
        Commit,
        Tree,
        Blob,
        )
from pack import (
        iter_sha1, 
        load_packs, 
        write_pack_index_v2,
        PackData, 
        )
import tempfile

OBJECTDIR = 'objects'
PACKDIR = 'pack'
SYMREF = 'ref: '


class Tag(object):

    def __init__(self, name, ref):
        self.name = name
        self.ref = ref


class Repo(object):

  ref_locs = ['', 'refs', 'refs/tags', 'refs/heads', 'refs/remotes']

  def __init__(self, root):
    if os.path.isdir(os.path.join(root, ".git", "objects")):
      self.bare = False
      self._controldir = os.path.join(root, ".git")
    elif os.path.isdir(os.path.join(root, "objects")):
      self.bare = True
      self._controldir = root
    else:
      raise NotGitRepository(root)
    self.path = root
    self.tags = [Tag(name, ref) for name, ref in self.get_tags().items()]
    self._object_store = None

  def controldir(self):
    return self._controldir

  def find_missing_objects(self, determine_wants, graph_walker, progress):
    """Fetch the missing objects required for a set of revisions.

    :param determine_wants: Function that takes a dictionary with heads 
        and returns the list of heads to fetch.
    :param graph_walker: Object that can iterate over the list of revisions 
        to fetch and has an "ack" method that will be called to acknowledge 
        that a revision is present.
    :param progress: Simple progress function that will be called with 
        updated progress strings.
    """
    wants = determine_wants(self.get_refs())
    commits_to_send = set(wants)
    sha_done = set()
    ref = graph_walker.next()
    while ref:
        sha_done.add(ref)
        if ref in self.object_store:
            graph_walker.ack(ref)
        ref = graph_walker.next()
    while commits_to_send:
        sha = commits_to_send.pop()
        if sha in sha_done:
            continue

        c = self.commit(sha)
        assert isinstance(c, Commit)
        sha_done.add(sha)

        commits_to_send.update([p for p in c.parents if not p in sha_done])

        def parse_tree(tree, sha_done):
            for mode, name, x in tree.entries():
                if not x in sha_done:
                    try:
                        t = self.tree(x)
                        sha_done.add(x)
                        parse_tree(t, sha_done)
                    except:
                        sha_done.add(x)

        treesha = c.tree
        if treesha not in sha_done:
            t = self.tree(treesha)
            sha_done.add(treesha)
            parse_tree(t, sha_done)

        progress("counting objects: %d\r" % len(sha_done))
    return sha_done

  def fetch_objects(self, determine_wants, graph_walker, progress):
    """Fetch the missing objects required for a set of revisions.

    :param determine_wants: Function that takes a dictionary with heads 
        and returns the list of heads to fetch.
    :param graph_walker: Object that can iterate over the list of revisions 
        to fetch and has an "ack" method that will be called to acknowledge 
        that a revision is present.
    :param progress: Simple progress function that will be called with 
        updated progress strings.
    """
    shas = self.find_missing_objects(determine_wants, graph_walker, progress)
    for sha in shas:
        yield self.get_object(sha)

  def object_dir(self):
    return os.path.join(self.controldir(), OBJECTDIR)

  @property
  def object_store(self):
    if self._object_store is None:
        self._object_store = ObjectStore(self.object_dir())
    return self._object_store

  def pack_dir(self):
    return os.path.join(self.object_dir(), PACKDIR)

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
    for dir in self.ref_locs:
      file = os.path.join(self.controldir(), dir, name)
      if os.path.exists(file):
        return self._get_ref(file)

  def get_refs(self):
    ret = {}
    if self.head():
        ret['HEAD'] = self.head()
    for dir in ["refs/heads", "refs/tags"]:
        for name in os.listdir(os.path.join(self.controldir(), dir)):
          path = os.path.join(self.controldir(), dir, name)
          if os.path.isfile(path):
            ret["/".join([dir, name])] = self._get_ref(path)
    return ret

  def set_ref(self, name, value):
    file = os.path.join(self.controldir(), name)
    open(file, 'w').write(value+"\n")

  def remove_ref(self, name):
    file = os.path.join(self.controldir(), name)
    if os.path.exists(file):
      os.remove(file)
      return

  def get_tags(self):
    ret = {}
    for root, dirs, files in os.walk(os.path.join(self.controldir(), 'refs', 'tags')):
      for name in files:
        ret[name] = self._get_ref(os.path.join(root, name))
    return ret

  def heads(self):
    ret = {}
    for root, dirs, files in os.walk(os.path.join(self.controldir(), 'refs', 'heads')):
      for name in files:
        ret[name] = self._get_ref(os.path.join(root, name))
    return ret

  def head(self):
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
    return self.commit(sha).parents

  def commit(self, sha):
    return self._get_object(sha, Commit)

  def tree(self, sha):
    return self._get_object(sha, Tree)

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
  def init_bare(cls, path, mkdir=True):
      for d in [["objects"], 
                ["objects", "info"], 
                ["objects", "pack"],
                ["branches"],
                ["refs"],
                ["refs", "tags"],
                ["refs", "heads"],
                ["hooks"],
                ["info"]]:
          os.mkdir(os.path.join(path, *d))
      open(os.path.join(path, 'HEAD'), 'w').write("ref: refs/heads/master\n")
      open(os.path.join(path, 'description'), 'w').write("Unnamed repository")
      open(os.path.join(path, 'info', 'excludes'), 'w').write("")

  create = init_bare


class ObjectStore(object):

    def __init__(self, path):
        self.path = path
        self._packs = None

    def pack_dir(self):
        return os.path.join(self.path, PACKDIR)

    def __contains__(self, sha):
        # TODO: This can be more efficient
        try:
            self[sha]
            return True
        except KeyError:
            return False

    @property
    def packs(self):
        """List with pack objects."""
        if self._packs is None:
            self._packs = list(load_packs(self.pack_dir()))
        return self._packs

    def _get_shafile(self, sha):
        dir = sha[:2]
        file = sha[2:]
        # Check from object dir
        path = os.path.join(self.path, dir, file)
        if os.path.exists(path):
          return ShaFile.from_file(path)
        return None

    def get_raw(self, sha):
        """Obtain the raw text for an object.
        
        :param sha: Sha for the object.
        :return: tuple with object type and object contents.
        """
        for pack in self.packs:
            if sha in pack:
                return pack.get_raw(sha, self.get_raw)
        # FIXME: Are pack deltas ever against on-disk shafiles ?
        ret = self._get_shafile(sha)
        if ret is not None:
            return ret.as_raw_string()
        raise KeyError(sha)

    def __getitem__(self, sha):
        assert len(sha) == 40, "Incorrect length sha: %s" % str(sha)
        ret = self._get_shafile(sha)
        if ret is not None:
            return ret
        # Check from packs
        type, uncomp = self.get_raw(sha)
        return ShaFile.from_raw_string(type, uncomp)

    def move_in_pack(self, path):
        """Move a specific file containing a pack into the pack directory.

        :note: The file should be on the same file system as the 
            packs directory.

        :param path: Path to the pack file.
        """
        p = PackData(path)
        entries = p.sorted_entries(self.get_raw)
        basename = os.path.join(self.pack_dir(), 
            "pack-%s" % iter_sha1(entry[0] for entry in entries))
        write_pack_index_v2(basename+".idx", entries, p.calculate_checksum())
        os.rename(path, basename + ".pack")

    def add_pack(self):
        """Add a new pack to this object store. 

        :return: Fileobject to write to and a commit function to 
            call when the pack is finished.
        """
        fd, path = tempfile.mkstemp(dir=self.pack_dir(), suffix=".pack")
        f = os.fdopen(fd, 'w')
        def commit():
            if os.path.getsize(path) > 0:
                self.move_in_pack(path)
        return f, commit
