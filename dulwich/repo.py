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
from errors import MissingCommitError
from objects import (ShaFile,
                     Commit,
                     Tree,
                     Blob,
                     )

objectdir = 'objects'
symref = 'ref: '


class Tag(object):

    def __init__(self, name, ref):
        self.name = name
        self.commit = Commit(ref)


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

  def basedir(self):
    return self._basedir

  def object_dir(self):
    return os.path.join(self.basedir(), objectdir)

  def _get_ref(self, file):
    f = open(file, 'rb')
    try:
      contents = f.read()
      if contents.startswith(symref):
        ref = contents[len(symref):]
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

  def get_tags(self):
    ret = {}
    for name in os.listdir(os.path.join(self.basedir(), 'refs', 'tags')):
      ret[name] = self._get_ref(os.path.join(self.basedir(), 'refs', 'tags', name))
    return ret

  def heads(self):
    ret = {}
    for name in os.listdir(os.path.join(self.basedir(), 'refs', 'heads')):
      ret[name] = self._get_ref(os.path.join(self.basedir(), 'refs', 'heads', name))
    return ret

  def head(self):
    return self.ref('HEAD')

  def _get_object(self, sha, cls):
    assert len(sha) == 40, "Incorrect length sha: %s" % str(sha)
    dir = sha[:2]
    file = sha[2:]
    path = os.path.join(self.object_dir(), dir, file)
    if not os.path.exists(path):
      # Should this raise instead?
      return None
    return cls.from_file(path)

  def get_object(self, sha):
    return self._get_object(sha, ShaFile)

  def commit(self, sha):
    return self._get_object(sha, Commit)

  def get_tree(self, sha):
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
      commit = self.commit(head)
      if commit is None:
        raise MissingCommitError(head)
      if commit in history:
        continue
      i = 0
      for known_commit in history:
        if known_commit.commit_time() > commit.commit_time():
          break
        i += 1
      history.insert(i, commit)
      parents = commit.parents()
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

