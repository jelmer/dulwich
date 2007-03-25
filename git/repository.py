import os

from objects import ShaFile

objectdir = 'objects'
symref = 'ref: '

class Repository(object):

  ref_locs = ['', 'refs', 'refs/tags', 'refs/heads', 'refs/remotes']

  def __init__(self, root):
    self._basedir = root

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

  def head(self):
    return self.ref('HEAD')

  def get_object(self, sha):
    assert len(sha) == 40, "Incorrect sha length"
    dir = sha[:2]
    file = sha[2:]
    path = os.path.join(self.object_dir(), dir, file)
    if not os.path.exists(path):
      return None
    return ShaFile.from_file(path)

