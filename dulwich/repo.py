# repo.py -- For dealing wih git repositories.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
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

import os, stat

from commit import Commit
from errors import (
        MissingCommitError, 
        NotBlobError, 
        NotCommitError, 
        NotGitRepository,
        NotTreeError, 
        )
from object_store import ObjectStore
from objects import (
        ShaFile,
        Commit,
        Tag,
        Tree,
        Blob,
        )

OBJECTDIR = 'objects'
SYMREF = 'ref: '


class Tags(object):

    def __init__(self, tagdir, tags):
        self.tagdir = tagdir
        self.tags = tags

    def __getitem__(self, name):
        return self.tags[name]
    
    def __setitem__(self, name, ref):
        self.tags[name] = ref
        f = open(os.path.join(self.tagdir, name), 'wb')
        try:
            f.write("%s\n" % ref)
        finally:
            f.close()

    def __len__(self):
        return len(self.tags)

    def iteritems(self):
        for k in self.tags:
            yield k, self[k]


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
    self.tags = Tags(self.tagdir(), self.get_tags())
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

    def parse_tree(tree, sha_done):
        for mode, name, sha in tree.entries():
            if (sha, name) in sha_done:
                continue
            if mode & stat.S_IFDIR:
                parse_tree(self.tree(sha), sha_done)
            sha_done.add((sha, name))

    def parse_commit(commit, sha_done):
        treesha = c.tree
        if c.tree not in sha_done:
            parse_tree(self.tree(c.tree), sha_done)
            sha_done.add((c.tree, None))

    ref = graph_walker.next()
    while ref:
        if ref in self.object_store:
            graph_walker.ack(ref)
        ref = graph_walker.next()
    while commits_to_send:
        sha = commits_to_send.pop()
        if (sha, None) in sha_done:
            continue

        c = self.object_store[sha]
        if isinstance(c, Commit):
            parse_commit(c, sha_done)
            commits_to_send.update([p for p in c.parents if not p in sha_done])
        sha_done.add((sha, None))

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
    :return: tuple with number of objects, iterator over objects
    """
    return self.object_store.iter_shas(
        self.find_missing_objects(determine_wants, graph_walker, progress))

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

  def tagdir(self):
    return os.path.join(self.controldir(), 'refs', 'tags')

  def get_tags(self):
    ret = {}
    for root, dirs, files in os.walk(self.tagdir()):
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

