# test_pack.py -- Tests for the handling of git packs.
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License, or (at your option) any later version of the license.
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

from dulwich.objects import (
        Tree,
        )
from dulwich.pack import (
        Pack,
        PackIndex,
        PackData,
        hex_to_sha,
        multi_ord,
        sha_to_hex,
        write_pack_index_v1,
        write_pack_index_v2,
        write_pack,
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

  def get_pack(self, sha):
    return Pack(os.path.join(self.datadir, 'pack-%s' % sha))


class PackIndexTests(PackTests):
  """Class that tests the index of packfiles"""

  def test_object_index(self):
    """Tests that the correct object offset is returned from the index."""
    p = self.get_pack_index(pack1_sha)
    self.assertEqual(p.object_index(pack1_sha), None)
    self.assertEqual(p.object_index(a_sha), 178)
    self.assertEqual(p.object_index(tree_sha), 138)
    self.assertEqual(p.object_index(commit_sha), 12)

  def test_index_len(self):
    p = self.get_pack_index(pack1_sha)
    self.assertEquals(3, len(p))

  def test_get_stored_checksum(self):
    p = self.get_pack_index(pack1_sha)
    self.assertEquals("\xf2\x84\x8e*\xd1o2\x9a\xe1\xc9.;\x95\xe9\x18\x88\xda\xa5\xbd\x01", str(p.get_stored_checksums()[1]))
    self.assertEquals( 'r\x19\x80\xe8f\xaf\x9a_\x93\xadgAD\xe1E\x9b\x8b\xa3\xe7\xb7' , str(p.get_stored_checksums()[0]))

  def test_index_check(self):
    p = self.get_pack_index(pack1_sha)
    self.assertEquals(True, p.check())


  def test_iterentries(self):
    p = self.get_pack_index(pack1_sha)
    self.assertEquals([('og\x0c\x0f\xb5?\x94cv\x0br\x95\xfb\xb8\x14\xe9e\xfb \xc8', 178, None), ('\xb2\xa2vj(y\xc2\t\xab\x11v\xe7\xe7x\xb8\x1a\xe4"\xee\xaa', 138, None), ('\xf1\x8f\xaa\x16S\x1a\xc5p\xa3\xfd\xc8\xc7\xca\x16h%H\xda\xfd\x12', 12, None)], list(p.iterentries()))

  def test_iter(self):
    p = self.get_pack_index(pack1_sha)
    self.assertEquals(set([tree_sha, commit_sha, a_sha]), set(p))


class TestPackData(PackTests):
  """Tests getting the data from the packfile."""

  def test_create_pack(self):
    p = self.get_pack_data(pack1_sha)

  def test_pack_len(self):
    p = self.get_pack_data(pack1_sha)
    self.assertEquals(3, len(p))

  def test_index_check(self):
    p = self.get_pack_data(pack1_sha)
    self.assertEquals(True, p.check())

  def test_iterobjects(self):
    p = self.get_pack_data(pack1_sha)
    self.assertEquals([(12, 1, 'tree b2a2766a2879c209ab1176e7e778b81ae422eeaa\nauthor James Westby <jw+debian@jameswestby.net> 1174945067 +0100\ncommitter James Westby <jw+debian@jameswestby.net> 1174945067 +0100\n\nTest commit\n'), (138, 2, '100644 a\x00og\x0c\x0f\xb5?\x94cv\x0br\x95\xfb\xb8\x14\xe9e\xfb \xc8'), (178, 3, 'test 1\n')], list(p.iterobjects()))

  def test_create_index_v1(self):
    p = self.get_pack_data(pack1_sha)
    p.create_index_v1("v1test.idx")
    idx1 = PackIndex("v1test.idx")
    idx2 = self.get_pack_index(pack1_sha)
    self.assertEquals(idx1, idx2)

  def test_create_index_v2(self):
    p = self.get_pack_data(pack1_sha)
    p.create_index_v2("v2test.idx")
    idx1 = PackIndex("v2test.idx")
    idx2 = self.get_pack_index(pack1_sha)
    self.assertEquals(idx1, idx2)



class TestPack(PackTests):

    def test_len(self):
        p = self.get_pack(pack1_sha)
        self.assertEquals(3, len(p))

    def test_contains(self):
        p = self.get_pack(pack1_sha)
        self.assertTrue(tree_sha in p)

    def test_get(self):
        p = self.get_pack(pack1_sha)
        self.assertEquals(type(p[tree_sha]), Tree)

    def test_iter(self):
        p = self.get_pack(pack1_sha)
        self.assertEquals(set([tree_sha, commit_sha, a_sha]), set(p))

    def test_get_object_at(self):
        """Tests random access for non-delta objects"""
        p = self.get_pack(pack1_sha)
        obj = p[a_sha]
        self.assertEqual(obj._type, 'blob')
        self.assertEqual(obj.sha().hexdigest(), a_sha)
        obj = p[tree_sha]
        self.assertEqual(obj._type, 'tree')
        self.assertEqual(obj.sha().hexdigest(), tree_sha)
        obj = p[commit_sha]
        self.assertEqual(obj._type, 'commit')
        self.assertEqual(obj.sha().hexdigest(), commit_sha)



class TestHexToSha(unittest.TestCase):

    def test_simple(self):
        self.assertEquals('\xab\xcd\xef', hex_to_sha("abcdef"))

    def test_reverse(self):
        self.assertEquals("abcdef", sha_to_hex('\xab\xcd\xef'))


class TestMultiOrd(unittest.TestCase):

    def test_simple(self):
        self.assertEquals(418262508645L, multi_ord("abcde", 0, 5))


class TestPackIndexWriting(object):

    def test_empty(self):
        pack_checksum = 'r\x19\x80\xe8f\xaf\x9a_\x93\xadgAD\xe1E\x9b\x8b\xa3\xe7\xb7'
        self._write_fn("empty.idx", [], pack_checksum)
        idx = PackIndex("empty.idx")
        self.assertTrue(idx.check())
        self.assertEquals(idx.get_stored_checksums()[0], pack_checksum)
        self.assertEquals(0, len(idx))

    def test_single(self):
        pack_checksum = 'r\x19\x80\xe8f\xaf\x9a_\x93\xadgAD\xe1E\x9b\x8b\xa3\xe7\xb7'
        my_entries = [('og\x0c\x0f\xb5?\x94cv\x0br\x95\xfb\xb8\x14\xe9e\xfb \xc8', 178, 42)]
        self._write_fn("single.idx", my_entries, pack_checksum)
        idx = PackIndex("single.idx")
        self.assertEquals(idx.version, self._expected_version)
        self.assertTrue(idx.check())
        self.assertEquals(idx.get_stored_checksums()[0], pack_checksum)
        self.assertEquals(1, len(idx))
        actual_entries = list(idx.iterentries())
        self.assertEquals(len(my_entries), len(actual_entries))
        for a, b in zip(my_entries, actual_entries):
            self.assertEquals(a[0], b[0])
            self.assertEquals(a[1], b[1])
            if self._has_crc32_checksum:
                self.assertEquals(a[2], b[2])
            else:
                self.assertTrue(b[2] is None)


class TestPackIndexWritingv1(unittest.TestCase, TestPackIndexWriting):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self._has_crc32_checksum = False
        self._expected_version = 1
        self._write_fn = write_pack_index_v1


class TestPackIndexWritingv2(unittest.TestCase, TestPackIndexWriting):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self._has_crc32_checksum = True
        self._expected_version = 2
        self._write_fn = write_pack_index_v2
