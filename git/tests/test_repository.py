# test_repository.py -- tests for repository.py
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
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
import unittest

from git.errors import (NotCommitError,
                        NotTreeError,
                        NotBlobError,
                        )
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

  def test_get_commit(self):
    r = self.open_repo('a')
    obj = r.get_commit(r.head())
    self.assertEqual(obj._type, 'commit')

  def test_get_commit_not_commit(self):
    r = self.open_repo('a')
    self.assertRaises(NotCommitError,
                      r.get_commit, '4f2e6529203aa6d44b5af6e3292c837ceda003f9')

  def test_get_tree(self):
    r = self.open_repo('a')
    commit = r.get_commit(r.head())
    tree = r.get_tree(commit.tree())
    self.assertEqual(tree._type, 'tree')
    self.assertEqual(tree.sha().hexdigest(), commit.tree())

  def test_get_tree_not_tree(self):
    r = self.open_repo('a')
    self.assertRaises(NotTreeError, r.get_tree, r.head())

  def test_get_blob(self):
    r = self.open_repo('a')
    commit = r.get_commit(r.head())
    tree = r.get_tree(commit.tree())
    blob_sha = tree.entries()[0][2]
    blob = r.get_blob(blob_sha)
    self.assertEqual(blob._type, 'blob')
    self.assertEqual(blob.sha().hexdigest(), blob_sha)

  def test_get_blob(self):
    r = self.open_repo('a')
    self.assertRaises(NotBlobError, r.get_blob, r.head())

  def test_linear_history(self):
    r = self.open_repo('a')
    history = r.revision_history(r.head())
    shas = [c.sha().hexdigest() for c in history]
    self.assertEqual(shas, [r.head(),
                            '2a72d929692c41d8554c07f6301757ba18a65d91'])

  def test_merge_history(self):
    r = self.open_repo('simple_merge')
    history = r.revision_history(r.head())
    shas = [c.sha().hexdigest() for c in history]
    self.assertEqual(shas, ['5dac377bdded4c9aeb8dff595f0faeebcc8498cc',
                            'ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                            '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6',
                            '60dacdc733de308bb77bb76ce0fb0f9b44c9769e',
                            '0d89f20333fbb1d2f3a94da77f4981373d8f4310'])


