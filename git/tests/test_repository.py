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

