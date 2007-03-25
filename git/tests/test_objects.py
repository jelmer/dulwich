import os
import unittest

from git.objects import (Blob,
                         Tree,
                         Commit,
                         )

a_sha = '6f670c0fb53f9463760b7295fbb814e965fb20c8'
b_sha = '2969be3e8ee1c0222396a5611407e4769f14e54b'
c_sha = '954a536f7819d40e6f637f849ee187dd10066349'
tree_sha = '70c190eb48fa8bbb50ddc692a17b44cb781af7f6'

class BlobReadTests(unittest.TestCase):
  """Test decompression of blobs"""

  def get_sha_file(self, obj, base, sha):
    return obj.from_file(os.path.join(os.path.dirname(__file__),
                                      'data', base, sha))

  def get_blob(self, sha):
    """Return the blob named sha from the test data dir"""
    return self.get_sha_file(Blob, 'blobs', sha)

  def get_tree(self, sha):
    return self.get_sha_file(Tree, 'trees', sha)

  def get_commit(self, sha):
    return self.get_sha_file(Commit, 'commits', sha)

  def test_decompress_simple_blob(self):
    b = self.get_blob(a_sha)
    self.assertEqual(b.text(), 'test 1\n')
    self.assertEqual(b.sha().hexdigest(), a_sha)

  def test_parse_empty_blob_object(self):
    sha = 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391'
    b = self.get_blob(sha)
    self.assertEqual(b.text(), '')
    self.assertEqual(b.sha().hexdigest(), sha)

  def test_create_blob_from_string(self):
    string = 'test 2\n'
    b = Blob.from_string(string)
    self.assertEqual(b.text(), string)
    self.assertEqual(b.sha().hexdigest(), b_sha)

  def test_parse_legacy_blob(self):
    string = 'test 3\n'
    b = self.get_blob(c_sha)
    self.assertEqual(b.text(), string)
    self.assertEqual(b.sha().hexdigest(), c_sha)

  def test_read_tree_from_file(self):
    t = self.get_tree(tree_sha)
    self.assertEqual(t.entries()[0], (33188, 'a', a_sha))
    self.assertEqual(t.entries()[1], (33188, 'b', b_sha))

  def test_read_commit_from_file(self):
    sha = '60dacdc733de308bb77bb76ce0fb0f9b44c9769e'
    c = self.get_commit(sha)
    self.assertEqual(c.tree(), tree_sha)
    self.assertEqual(c.parents(), ['0d89f20333fbb1d2f3a94da77f4981373d8f4310'])
    self.assertEqual(c.author(),
        'James Westby <jw+debian@jameswestby.net> 1174759230 +0000')
    self.assertEqual(c.committer(),
        'James Westby <jw+debian@jameswestby.net> 1174759230 +0000')
    self.assertEqual(c.message(), 'Test commit\n')

  def test_read_commit_no_parents(self):
    sha = '0d89f20333fbb1d2f3a94da77f4981373d8f4310'
    c = self.get_commit(sha)
    self.assertEqual(c.tree(), '90182552c4a85a45ec2a835cadc3451bebdfe870')
    self.assertEqual(c.parents(), [])
    self.assertEqual(c.author(),
        'James Westby <jw+debian@jameswestby.net> 1174758034 +0000')
    self.assertEqual(c.committer(),
        'James Westby <jw+debian@jameswestby.net> 1174758034 +0000')
    self.assertEqual(c.message(), 'Test commit\n')

  def test_read_commit_two_parents(self):
    sha = '5dac377bdded4c9aeb8dff595f0faeebcc8498cc'
    c = self.get_commit(sha)
    self.assertEqual(c.tree(), 'd80c186a03f423a81b39df39dc87fd269736ca86')
    self.assertEqual(c.parents(), ['ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                                   '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6'])
    self.assertEqual(c.author(),
        'James Westby <jw+debian@jameswestby.net> 1174773719 +0000')
    self.assertEqual(c.committer(),
        'James Westby <jw+debian@jameswestby.net> 1174773719 +0000')
    self.assertEqual(c.message(), 'Merge ../b\n')

