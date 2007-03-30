# test_pack.py -- Tests for the handling of git packs.
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

from git.pack import (PackIndex,
                      PackData,
                      )

pack1_sha = 'bc63ddad95e7321ee734ea11a7a62d314e0d7481'

a_sha = '6f670c0fb53f9463760b7295fbb814e965fb20c8'
tree_sha = 'b2a2766a2879c209ab1176e7e778b81ae422eeaa'
commit_sha = 'f18faa16531ac570a3fdc8c7ca16682548dafd12'

class PackTests(unittest.TestCase):
  """Base class for testing packs"""

  datadir = os.path.join(os.path.dirname(__file__), 'data/packs')

  def get_pack_index(self, sha):
    """Returns a PackIndex from the datadir with the given sha"""
    return PackIndex(os.path.join(self.datadir, 'pack-%s.idx' % sha))

  def get_pack_data(self, sha):
    """Returns a PackData object from the datadir with the given sha"""
    return PackData(os.path.join(self.datadir, 'pack-%s.pack' % sha))


class PackIndexTests(PackTests):
  """Class that tests the index of packfiles"""

  def test_object_index(self):
    """Tests that the correct object offset is returned from the index."""
    p = self.get_pack_index(pack1_sha)
    self.assertEqual(p.object_index(pack1_sha), None)
    self.assertEqual(p.object_index(a_sha), 178)
    self.assertEqual(p.object_index(tree_sha), 138)
    self.assertEqual(p.object_index(commit_sha), 12)


class TestPackData(PackTests):
  """Tests getting the data from the packfile."""

  def test_create_pack(self):
    p = self.get_pack_data(pack1_sha)

  def test_get_object_at(self):
    """Tests random access for non-delta objects"""
    p = self.get_pack_data(pack1_sha)
    idx = self.get_pack_index(pack1_sha)
    obj = p.get_object_at(idx.object_index(a_sha))
    self.assertEqual(obj._type, 'blob')
    self.assertEqual(obj.sha().hexdigest(), a_sha)
    obj = p.get_object_at(idx.object_index(tree_sha))
    self.assertEqual(obj._type, 'tree')
    self.assertEqual(obj.sha().hexdigest(), tree_sha)
    obj = p.get_object_at(idx.object_index(commit_sha))
    self.assertEqual(obj._type, 'commit')
    self.assertEqual(obj.sha().hexdigest(), commit_sha)

