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
from errors import MissingCommitError, NotBlobError, NotTreeError, NotCommitError
from objects import (ShaFile,
                     Commit,
                     Tree,
                     Blob,
                     )
from pack import load_packs, iter_sha1, PackData, write_pack_index_v2
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
    controldir = os.path.join(root, ".git")
    if os.path.exists(os.path.join(controldir, "objects")):
      self.bare = False
      self._basedir = controldir
    else:
      self.bare = True
      self._basedir = root
    self.path = controldir
    self.tags = [Tag(name, ref) for name, ref in self.get_tags().items()]
    self._object_store = None

  def basedir(self):
    return self._basedir

  def object_dir(self):
    return os.path.join(self.basedir(), OBJECTDIR)

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
      assert len(contents) == 41, 'Invalid ref'
      return contents[:-1]
    finally:
      f.close()

  def ref(self, name):
    for dir in self.ref_locs:
      file = os.path.join(self.basedir(), dir, name)
      if os.path.exists(file):
        return self._get_ref(file)

  def set_ref(self, name, value):
    file = os.path.join(self.basedir(), name)
    open(file, 'w').write(value+"\n")

  def remove_ref(self, name):
    file = os.path.join(self.basedir(), name)
    if os.path.exists(file):
      os.remove(file)
      return

  def get_tags(self):
    ret = {}
    for root, dirs, files in os.walk(os.path.join(self.basedir(), 'refs', 'tags')):
      for name in files:
        ret[name] = self._get_ref(os.path.join(root, name))
    return ret

  def heads(self):
    ret = {}
    for root, dirs, files in os.walk(os.path.join(self.basedir(), 'refs', 'heads')):
      for name in files:
        ret[name] = self._get_ref(os.path.join(root, name))
    return ret

  def head(self):
    return self.ref('HEAD')

  def _get_object(self, sha, cls):
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
        p = PackData(path)
        entries = p.sorted_entries(self.get_raw)
        basename = os.path.join(self.pack_dir(), "pack-%s" % iter_sha1(entry[0] for entry in entries))
        write_pack_index_v2(basename+".idx", entries, p.calculate_checksum())
        os.rename(path, basename + ".pack")

    def add_pack(self):
        fd, path = tempfile.mkstemp(dir=self.pack_dir(), suffix=".pack")
        f = os.fdopen(fd, 'w')
        def commit():
            if os.path.getsize(path) > 0:
                self.move_in_pack(path)
        return f, commit
