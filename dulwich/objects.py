# objects.py -- Access to base git objects
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version of the License.
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


"""Access to base git objects."""


import binascii
from cStringIO import (
    StringIO,
    )
import mmap
import os
import stat
import time
import zlib

from dulwich.errors import (
    NotBlobError,
    NotCommitError,
    NotTreeError,
    )
from dulwich.misc import (
    make_sha,
    )

BLOB_ID = "blob"
TAG_ID = "tag"
TREE_ID = "tree"
COMMIT_ID = "commit"
PARENT_ID = "parent"
AUTHOR_ID = "author"
COMMITTER_ID = "committer"
OBJECT_ID = "object"
TYPE_ID = "type"
TAGGER_ID = "tagger"
ENCODING_ID = "encoding"

S_IFGITLINK	= 0160000
def S_ISGITLINK(m):
    return (stat.S_IFMT(m) == S_IFGITLINK)

def _decompress(string):
    dcomp = zlib.decompressobj()
    dcomped = dcomp.decompress(string)
    dcomped += dcomp.flush()
    return dcomped


def sha_to_hex(sha):
    """Takes a string and returns the hex of the sha within"""
    hexsha = binascii.hexlify(sha)
    assert len(hexsha) == 40, "Incorrect length of sha1 string: %d" % hexsha
    return hexsha


def hex_to_sha(hex):
    """Takes a hex sha and returns a binary sha"""
    assert len(hex) == 40, "Incorrent length of hexsha: %s" % hex
    return binascii.unhexlify(hex)


def serializable_property(name, docstring=None):
    def set(obj, value):
        obj._ensure_parsed()
        setattr(obj, "_"+name, value)
        obj._needs_serialization = True
    def get(obj):
        obj._ensure_parsed()
        return getattr(obj, "_"+name)
    return property(get, set, doc=docstring)


class ShaFile(object):
    """A git SHA file."""
  
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
                raise AssertionError("Size is not in canonical format")
            size = (size * 10) + int(text[0])
            text = text[1:]
            i += 1
        object._size = size
        assert text[0] == "\0", "Size not followed by null"
        text = text[1:]
        object.set_raw_string(text)
        return object

    def as_legacy_object(self):
        text = self.as_raw_string()
        return zlib.compress("%s %d\0%s" % (self._type, len(text), text))
  
    def as_raw_string(self):
        if self._needs_serialization:
            self.serialize()
        return self._text

    def __str__(self):
        return self.as_raw_string()

    def __hash__(self):
        return hash(self.id)

    def as_pretty_string(self):
        return self.as_raw_string()

    def _ensure_parsed(self):
        if self._needs_parsing:
            self._parse_text()

    def set_raw_string(self, text):
        if type(text) != str:
            raise TypeError(text)
        self._text = text
        self._sha = None
        self._needs_parsing = True
        self._needs_serialization = False
  
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
            raise AssertionError("Not a known type: %d" % num_type)
        while (byte & 0x80) != 0:
            byte = ord(map[used])
            used += 1
        raw = map[used:]
        object.set_raw_string(_decompress(raw))
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
        self._sha = None
  
    def _parse_text(self):
        """For subclasses to do initialisation time parsing"""
  
    @classmethod
    def from_file(cls, filename):
        """Get the contents of a SHA file on disk"""
        size = os.path.getsize(filename)
        f = open(filename, 'rb')
        try:
            map = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
            shafile = cls._parse_file(map)
            return shafile
        finally:
            f.close()
  
    @classmethod
    def from_raw_string(cls, type, string):
        """Creates an object of the indicated type from the raw string given.
    
        Type is the numeric type of an object. String is the raw uncompressed
        contents.
        """
        real_class = num_type_map[type]
        obj = real_class()
        obj.type = type
        obj.set_raw_string(string)
        return obj
  
    def _header(self):
        return "%s %lu\0" % (self._type, len(self.as_raw_string()))
  
    def sha(self):
        """The SHA1 object that is the name of this object."""
        if self._needs_serialization or self._sha is None:
            self._sha = make_sha()
            self._sha.update(self._header())
            self._sha.update(self.as_raw_string())
        return self._sha
  
    @property
    def id(self):
        return self.sha().hexdigest()
  
    def get_type(self):
        return self._num_type

    def set_type(self, type):
        self._num_type = type

    type = property(get_type, set_type)
  
    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.id)

    def __ne__(self, other):
        return self.id != other.id
  
    def __eq__(self, other):
        """Return true id the sha of the two objects match.
  
        The __le__ etc methods aren't overriden as they make no sense,
        certainly at this level.
        """
        return self.id == other.id


class Blob(ShaFile):
    """A Git Blob object."""

    _type = BLOB_ID
    _num_type = 3
    _needs_serialization = False
    _needs_parsing = False

    def get_data(self):
        return self._text

    def set_data(self, data):
        self._text = data

    data = property(get_data, set_data, 
            "The text contained within the blob object.")

    @classmethod
    def from_file(cls, filename):
        blob = ShaFile.from_file(filename)
        if blob._type != cls._type:
            raise NotBlobError(filename)
        return blob

    @classmethod
    def from_string(cls, string):
        """Create a blob from a string."""
        shafile = cls()
        shafile.set_raw_string(string)
        return shafile


class Tag(ShaFile):
    """A Git Tag object."""

    _type = TAG_ID
    _num_type = 4

    def __init__(self):
        super(Tag, self).__init__()
        self._needs_parsing = False
        self._needs_serialization = True

    @classmethod
    def from_file(cls, filename):
        blob = ShaFile.from_file(filename)
        if blob._type != cls._type:
            raise NotBlobError(filename)
        return blob

    @classmethod
    def from_string(cls, string):
        """Create a blob from a string."""
        shafile = cls()
        shafile.set_raw_string(string)
        return shafile

    def serialize(self):
        f = StringIO()
        f.write("%s %s\n" % (OBJECT_ID, self._object_sha))
        f.write("%s %s\n" % (TYPE_ID, num_type_map[self._object_type]._type))
        f.write("%s %s\n" % (TAG_ID, self._name))
        if self._tagger:
            f.write("%s %s %d %s\n" % (TAGGER_ID, self._tagger, self._tag_time, format_timezone(self._tag_timezone)))
        f.write("\n") # To close headers
        f.write(self._message)
        self._text = f.getvalue()
        self._needs_serialization = False

    def _parse_text(self):
        """Grab the metadata attached to the tag"""
        self._tagger = None
        f = StringIO(self._text)
        for l in f:
            l = l.rstrip("\n")
            if l == "":
                break # empty line indicates end of headers
            (field, value) = l.split(" ", 1)
            if field == OBJECT_ID:
                self._object_sha = value
            elif field == TYPE_ID:
                self._object_type = type_map[value]
            elif field == TAG_ID:
                self._name = value
            elif field == TAGGER_ID:
                sep = value.index("> ")
                self._tagger = value[0:sep+1]
                (timetext, timezonetext) = value[sep+2:].rsplit(" ", 1)
                try:
                    self._tag_time = int(timetext)
                except ValueError: #Not a unix timestamp
                    self._tag_time = time.strptime(timetext)
                self._tag_timezone = parse_timezone(timezonetext)
            else:
                raise AssertionError("Unknown field %s" % field)
        self._message = f.read()
        self._needs_parsing = False

    def get_object(self):
        """Returns the object pointed by this tag, represented as a tuple(type, sha)"""
        self._ensure_parsed()
        return (self._object_type, self._object_sha)

    def set_object(self, value):
        self._ensure_parsed()
        (self._object_type, self._object_sha) = value
        self._needs_serialization = True

    object = property(get_object, set_object)

    name = serializable_property("name", "The name of this tag")
    tagger = serializable_property("tagger", 
        "Returns the name of the person who created this tag")
    tag_time = serializable_property("tag_time", 
        "The creation timestamp of the tag.  As the number of seconds since the epoch")
    tag_timezone = serializable_property("tag_timezone", 
        "The timezone that tag_time is in.")
    message = serializable_property("message", "The message attached to this tag")


def parse_tree(text):
    ret = {}
    count = 0
    while count < len(text):
        mode = 0
        chr = text[count]
        while chr != ' ':
            assert chr >= '0' and chr <= '7', "%s is not a valid mode char" % chr
            mode = (mode << 3) + (ord(chr) - ord('0'))
            count += 1
            chr = text[count]
        count += 1
        chr = text[count]
        name = ''
        while chr != '\0':
            name += chr
            count += 1
            chr = text[count]
        count += 1
        chr = text[count]
        sha = text[count:count+20]
        hexsha = sha_to_hex(sha)
        ret[name] = (mode, hexsha)
        count = count + 20
    return ret


class Tree(ShaFile):
    """A Git tree object"""

    _type = TREE_ID
    _num_type = 2

    def __init__(self):
        super(Tree, self).__init__()
        self._entries = {}
        self._needs_parsing = False
        self._needs_serialization = True

    @classmethod
    def from_file(cls, filename):
        tree = ShaFile.from_file(filename)
        if tree._type != cls._type:
            raise NotTreeError(filename)
        return tree

    def __contains__(self, name):
        self._ensure_parsed()
        return name in self._entries

    def __getitem__(self, name):
        self._ensure_parsed()
        return self._entries[name]

    def __setitem__(self, name, value):
        assert isinstance(value, tuple)
        assert len(value) == 2
        self._ensure_parsed()
        self._entries[name] = value
        self._needs_serialization = True

    def __delitem__(self, name):
        self._ensure_parsed()
        del self._entries[name]
        self._needs_serialization = True

    def __len__(self):
        self._ensure_parsed()
        return len(self._entries)

    def add(self, mode, name, hexsha):
        assert type(mode) == int
        assert type(name) == str
        assert type(hexsha) == str
        self._ensure_parsed()
        self._entries[name] = mode, hexsha
        self._needs_serialization = True

    def entries(self):
        """Return a list of tuples describing the tree entries"""
        self._ensure_parsed()
        # The order of this is different from iteritems() for historical reasons
        return [(mode, name, hexsha) for (name, mode, hexsha) in self.iteritems()]

    def iteritems(self):
        def cmp_entry((name1, value1), (name2, value2)):
            if stat.S_ISDIR(value1[0]):
                name1 += "/"
            if stat.S_ISDIR(value2[0]):
                name2 += "/"
            return cmp(name1, name2)
        self._ensure_parsed()
        for name, entry in sorted(self._entries.iteritems(), cmp=cmp_entry):
            yield name, entry[0], entry[1]

    def _parse_text(self):
        """Grab the entries in the tree"""
        self._entries = parse_tree(self._text)
        self._needs_parsing = False

    def serialize(self):
        f = StringIO()
        for name, mode, hexsha in self.iteritems():
            f.write("%04o %s\0%s" % (mode, name, hex_to_sha(hexsha)))
        self._text = f.getvalue()
        self._needs_serialization = False

    def as_pretty_string(self):
        text = ""
        for name, mode, hexsha in self.iteritems():
            if mode & stat.S_IFDIR:
                kind = "tree"
            else:
                kind = "blob"
            text += "%04o %s %s\t%s\n" % (mode, kind, hexsha, name)
        return text


def parse_timezone(text):
    offset = int(text)
    signum = (offset < 0) and -1 or 1
    offset = abs(offset)
    hours = int(offset / 100)
    minutes = (offset % 100)
    return signum * (hours * 3600 + minutes * 60)


def format_timezone(offset):
    if offset % 60 != 0:
        raise ValueError("Unable to handle non-minute offset.")
    sign = (offset < 0) and '-' or '+'
    offset = abs(offset)
    return '%c%02d%02d' % (sign, offset / 3600, (offset / 60) % 60)


class Commit(ShaFile):
    """A git commit object"""

    _type = COMMIT_ID
    _num_type = 1

    def __init__(self):
        super(Commit, self).__init__()
        self._parents = []
        self._encoding = None
        self._needs_parsing = False
        self._needs_serialization = True

    @classmethod
    def from_file(cls, filename):
        commit = ShaFile.from_file(filename)
        if commit._type != cls._type:
            raise NotCommitError(filename)
        return commit

    def _parse_text(self):
        self._parents = []
        self._author = None
        f = StringIO(self._text)
        for l in f:
            l = l.rstrip("\n")
            if l == "":
                # Empty line indicates end of headers
                break
            (field, value) = l.split(" ", 1)
            if field == TREE_ID:
                self._tree = value
            elif field == PARENT_ID:
                self._parents.append(value)
            elif field == AUTHOR_ID:
                self._author, timetext, timezonetext = value.rsplit(" ", 2)
                self._author_time = int(timetext)
                self._author_timezone = parse_timezone(timezonetext)
            elif field == COMMITTER_ID:
                self._committer, timetext, timezonetext = value.rsplit(" ", 2)
                self._commit_time = int(timetext)
                self._commit_timezone = parse_timezone(timezonetext)
            elif field == ENCODING_ID:
                self._encoding = value
            else:
                raise AssertionError("Unknown field %s" % field)
        self._message = f.read()
        self._needs_parsing = False

    def serialize(self):
        f = StringIO()
        f.write("%s %s\n" % (TREE_ID, self._tree))
        for p in self._parents:
            f.write("%s %s\n" % (PARENT_ID, p))
        f.write("%s %s %s %s\n" % (AUTHOR_ID, self._author, str(self._author_time), format_timezone(self._author_timezone)))
        f.write("%s %s %s %s\n" % (COMMITTER_ID, self._committer, str(self._commit_time), format_timezone(self._commit_timezone)))
        if self.encoding:
            f.write("%s %s\n" % (ENCODING_ID, self.encoding))
        f.write("\n") # There must be a new line after the headers
        f.write(self._message)
        self._text = f.getvalue()
        self._needs_serialization = False

    tree = serializable_property("tree", "Tree that is the state of this commit")

    def get_parents(self):
        """Return a list of parents of this commit."""
        self._ensure_parsed()
        return self._parents

    def set_parents(self, value):
        """Return a list of parents of this commit."""
        self._ensure_parsed()
        self._needs_serialization = True
        self._parents = value

    parents = property(get_parents, set_parents)

    author = serializable_property("author", 
        "The name of the author of the commit")

    committer = serializable_property("committer", 
        "The name of the committer of the commit")

    message = serializable_property("message",
        "The commit message")

    commit_time = serializable_property("commit_time",
        "The timestamp of the commit. As the number of seconds since the epoch.")

    commit_timezone = serializable_property("commit_timezone",
        "The zone the commit time is in")

    author_time = serializable_property("author_time", 
        "The timestamp the commit was written. as the number of seconds since the epoch.")

    author_timezone = serializable_property("author_timezone", 
        "Returns the zone the author time is in.")

    encoding = serializable_property("encoding",
        "Encoding of the commit message.")


type_map = {
    BLOB_ID : Blob,
    TREE_ID : Tree,
    COMMIT_ID : Commit,
    TAG_ID: Tag,
}

num_type_map = {
    0: None,
    1: Commit,
    2: Tree,
    3: Blob,
    4: Tag,
    # 5 Is reserved for further expansion
}

try:
    # Try to import C versions
    from dulwich._objects import parse_tree
except ImportError:
    pass

