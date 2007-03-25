import os
import unittest

from git.repository import Repository

class RepositoryTests(unittest.TestCase):

  def open_repo(self, name):
    return Repository(os.path.join(os.path.dirname(__file__),
                      'data/repos', name, '.git'))

  def test_simple_props(self):
    r = self.open_repo('a')
    basedir = os.path.join(os.path.dirname(__file__), 'data/repos/a/.git')
    self.assertEqual(r.basedir(), basedir)
    self.assertEqual(r.object_dir(), os.path.join(basedir, 'objects'))

  def test_ref(self):
    r = self.open_repo('a')
    self.assertEqual(r.ref('master'),
                     'a90fa2d900a17e99b433217e988c4eb4a2e9a097')

  def test_head(self):
    r = self.open_repo('a')
    self.assertEqual(r.head(), 'a90fa2d900a17e99b433217e988c4eb4a2e9a097')

  def test_get_object(self):
    r = self.open_repo('a')
    obj = r.get_object(r.head())
    self.assertEqual(obj._type, 'commit')

  def test_get_object_non_existant(self):
    r = self.open_repo('a')
    obj = r.get_object('b91fa4d900g17e99b433218e988c4eb4a3e9a097')
    self.assertEqual(obj, None)

