# objects.py -- Acces to base git objects
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# The header parsing code is based on that from git itself, which is
# Copyright (C) 2005 Linus Torvalds
# and licensed under v2 of the GPL.
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

import mmap
import os
import sha
import zlib

blob_id = "blob"
tree_id = "tree"
commit_id = "commit"
parent_id = "parent"
author_id = "author"
committer_id = "committer"

def _decompress(string):
    dcomp = zlib.decompressobj()
    dcomped = dcomp.decompress(string)
    dcomped += dcomp.flush()
    return dcomped

def sha_to_hex(sha):
  """Takes a string and returns the hex of the sha within"""
  hexsha = ''
  for c in sha:
    if ord(c) < 16:
      hexsha += "0%x" % ord(c)
    else:
      hexsha += "%x" % ord(c)
  assert len(hexsha) == 40, "Incorrect length of sha1 string: %d" % \
         len(hexsha)
  return hexsha

class ShaFile(object):
  """A git SHA file."""

  def _update_contents(self):
    """Update the _contents from the _text"""
    self._contents = [ord(c) for c in self._text]

  @classmethod
  def _parse_legacy_object(cls, map):
    """Parse a legacy object, creating it and setting object._text"""
    text = _decompress(map)
    object = None
    for posstype in type_map.keys():
      if text.startswith(posstype):
        object = type_map[posstype]()
        text = text[len(posstype):]
        break
    assert object is not None, "%s is not a known object type" % text[:9]
    assert text[0] == ' ', "%s is not a space" % text[0]
    text = text[1:]
    size = 0
    i = 0
    while text[0] >= '0' and text[0] <= '9':
      if i > 0 and size == 0:
        assert False, "Size is not in canonical format"
      size = (size * 10) + int(text[0])
      text = text[1:]
      i += 1
    object._size = size
    assert text[0] == "\0", "Size not followed by null"
    text = text[1:]
    object._text = text
    object._update_contents()
    return object

  @classmethod
  def _parse_object(cls, map):
    """Parse a new style object , creating it and setting object._text"""
    used = 0
    byte = ord(map[used])
    used += 1
    num_type = (byte >> 4) & 7
    try:
      object = num_type_map[num_type]()
    except KeyError:
      assert False, "Not a known type: %d" % num_type
    while((byte & 0x80) != 0):
      byte = ord(map[used])
      used += 1
    raw = map[used:]
    object._text = _decompress(raw)
    object._update_contents()
    return object

  @classmethod
  def _parse_file(cls, map):
    word = (ord(map[0]) << 8) + ord(map[1])
    if ord(map[0]) == 0x78 and (word % 31) == 0:
      return cls._parse_legacy_object(map)
    else:
      return cls._parse_object(map)

  def __init__(self):
    """Don't call this directly"""

  def _parse_text(self):
    """For subclasses to do initialistion time parsing"""

  @classmethod
  def from_file(cls, filename):
    """Get the contents of a SHA file on disk"""
    size = os.path.getsize(filename)
    f = open(filename, 'rb+')
    try:
      map = mmap.mmap(f.fileno(), size)
      shafile = cls._parse_file(map)
      shafile._parse_text()
      return shafile
    finally:
      f.close()

  def _header(self):
    return "%s %lu\0" % (self._type, len(self._contents))

  def contents(self):
    """The raw bytes of this object"""
    return self._contents

  def sha(self):
    """The SHA1 object that is the name of this object."""
    ressha = sha.new()
    ressha.update(self._header())
    ressha.update(self._text)
    return ressha

class Blob(ShaFile):
  """A Git Blob object."""

  _type = blob_id

  def text(self):
    """The text contained within the blob object."""
    return self._text

  @classmethod
  def from_file(cls, filename):
    blob = ShaFile.from_file(filename)
    assert blob._type == cls._type, "%s is not a blob object" % filename
    return blob

  @classmethod
  def from_string(cls, string):
    """Create a blob from a string."""
    shafile = cls()
    shafile._text = string
    shafile._update_contents()
    return shafile

class Tree(ShaFile):
  """A Git tree object"""

  _type = tree_id

  @classmethod
  def from_file(cls, filename):
    tree = ShaFile.from_file(filename)
    assert tree._type == cls._type, "%s is not a tree object" % filename
    return tree

  def entries(self):
    """Reutrn a list of tuples describing the tree entries"""
    return self._entries

  def _parse_text(self):
    """Grab the entries in the tree"""
    self._entries = []
    count = 0
    while count < len(self._text):
      mode = 0
      chr = self._text[count]
      while chr != ' ':
        assert chr >= '0' and chr <= '7', "%s is not a valid mode char" % chr
        mode = (mode << 3) + (ord(chr) - ord('0'))
        count += 1
        chr = self._text[count]
      count += 1
      chr = self._text[count]
      name = ''
      while chr != '\0':
        name += chr
        count += 1
        chr = self._text[count]
      count += 1
      chr = self._text[count]
      sha = self._text[count:count+20]
      hexsha = sha_to_hex(sha)
      self._entries.append((mode, name, hexsha))
      count = count + 20

class Commit(ShaFile):
  """A git commit object"""

  _type = commit_id

  @classmethod
  def from_file(cls, filename):
    commit = ShaFile.from_file(filename)
    assert commit._type == cls._type, "%s is not a commit object" % filename
    return commit

  def _parse_text(self):
    text = self._text
    count = 0
    assert text.startswith(tree_id), "Invlid commit object, " \
         "must start with %s" % tree_id
    count += len(tree_id)
    assert text[count] == ' ', "Invalid commit object, " \
         "%s must be followed by space not %s" % (tree_id, text[count])
    count += 1
    self._tree = text[count:count+40]
    count = count + 40
    assert text[count] == "\n", "Invalid commit object, " \
         "tree sha must be followed by newline"
    count += 1
    self._parents = []
    while text[count:].startswith(parent_id):
      count += len(parent_id)
      assert text[count] == ' ', "Invalid commit object, " \
           "%s must be followed by space not %s" % (parent_id, text[count])
      count += 1
      self._parents.append(text[count:count+40])
      count += 40
      assert text[count] == "\n", "Invalid commit object, " \
           "parent sha must be followed by newline"
      count += 1
    self._author = None
    if text[count:].startswith(author_id):
      count += len(author_id)
      assert text[count] == ' ', "Invalid commit object, " \
           "%s must be followed by space not %s" % (author_id, text[count])
      count += 1
      self._author = ''
      while text[count] != '\n':
        self._author += text[count]
        count += 1
      count += 1
    self._committer = None
    if text[count:].startswith(committer_id):
      count += len(committer_id)
      assert text[count] == ' ', "Invalid commit object, " \
           "%s must be followed by space not %s" % (committer_id, text[count])
      count += 1
      self._committer = ''
      while text[count] != '\n':
        self._committer += text[count]
        count += 1
      count += 1
    assert text[count] == '\n', "There must be a new line after the headers"
    count += 1
    self._message = text[count:]

  def tree(self):
    """Returns the tree that is the state of this commit"""
    return self._tree

  def parents(self):
    """Return a list of parents of this commit."""
    return self._parents

  def author(self):
    """Returns the name of the author of the commit"""
    return self._author

  def committer(self):
    """Returns the name of the committer of the commit"""
    return self._committer

  def message(self):
    """Returns the commit message"""
    return self._message

type_map = {
  blob_id : Blob,
  tree_id : Tree,
  commit_id : Commit,
}

num_type_map = {
  1 : Commit,
  2 : Tree,
  3 : Blob,
}

