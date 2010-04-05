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
from dulwich.file import GitFile
from dulwich.misc import (
    make_sha,
    )


# Header fields for commits
_TREE_HEADER = "tree"
_PARENT_HEADER = "parent"
_AUTHOR_HEADER = "author"
_COMMITTER_HEADER = "committer"
_ENCODING_HEADER = "encoding"


# Header fields for objects
_OBJECT_HEADER = "object"
_TYPE_HEADER = "type"
_TAG_HEADER = "tag"
_TAGGER_HEADER = "tagger"


S_IFGITLINK = 0160000

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


def object_class(type):
    """Get the object class corresponding to the given type.

    :param type: Either a type name string or a numeric type.
    :return: The ShaFile subclass corresponding to the given type.
    """
    return _TYPE_MAP[type]


class ShaFile(object):
    """A git SHA file."""

    @classmethod
    def _parse_legacy_object(cls, map):
        """Parse a legacy object, creating it and setting object._text"""
        text = _decompress(map)
        object = None
        for cls in OBJECT_CLASSES:
            if text.startswith(cls.type_name):
                object = cls()
                text = text[len(cls.type_name):]
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

    def as_legacy_object_chunks(self):
        compobj = zlib.compressobj()
        yield compobj.compress(self._header())
        for chunk in self.as_raw_chunks():
            yield compobj.compress(chunk)
        yield compobj.flush()

    def as_legacy_object(self):
        return "".join(self.as_legacy_object_chunks())

    def as_raw_chunks(self):
        if self._needs_serialization:
            self._chunked_text = self._serialize()
            self._needs_serialization = False
        return self._chunked_text

    def as_raw_string(self):
        return "".join(self.as_raw_chunks())

    def __str__(self):
        return self.as_raw_string()

    def __hash__(self):
        return hash(self.id)

    def as_pretty_string(self):
        return self.as_raw_string()

    def _ensure_parsed(self):
        if self._needs_parsing:
            self._deserialize(self._chunked_text)
            self._needs_parsing = False

    def set_raw_string(self, text):
        if type(text) != str:
            raise TypeError(text)
        self.set_raw_chunks([text])

    def set_raw_chunks(self, chunks):
        self._chunked_text = chunks
        self._sha = None
        self._needs_parsing = True
        self._needs_serialization = False

    @classmethod
    def _parse_object(cls, map):
        """Parse a new style object , creating it and setting object._text"""
        used = 0
        byte = ord(map[used])
        used += 1
        type_num = (byte >> 4) & 7
        try:
            object = object_class(type_num)()
        except KeyError:
            raise AssertionError("Not a known type: %d" % type_num)
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

    def _deserialize(self, chunks):
        raise NotImplementedError(self._deserialize)

    def _serialize(self):
        raise NotImplementedError(self._serialize)

    @classmethod
    def from_file(cls, filename):
        """Get the contents of a SHA file on disk"""
        size = os.path.getsize(filename)
        f = GitFile(filename, 'rb')
        try:
            map = mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)
            shafile = cls._parse_file(map)
            return shafile
        finally:
            f.close()

    @staticmethod
    def from_raw_string(type_num, string):
        """Creates an object of the indicated type from the raw string given.

        :param type_num: The numeric type of the object.
        :param string: The raw uncompressed contents.
        """
        obj = object_class(type_num)()
        obj.set_raw_string(string)
        return obj

    @staticmethod
    def from_raw_chunks(type_num, chunks):
        """Creates an object of the indicated type from the raw chunks given.

        :param type_num: The numeric type of the object.
        :param chunks: An iterable of the raw uncompressed contents.
        """
        obj = object_class(type_num)()
        obj.set_raw_chunks(chunks)
        return obj

    @classmethod
    def from_string(cls, string):
        """Create a blob from a string."""
        obj = cls()
        obj.set_raw_string(string)
        return obj

    def _header(self):
        return "%s %lu\0" % (self.type_name, self.raw_length())

    def raw_length(self):
        """Returns the length of the raw string of this object."""
        ret = 0
        for chunk in self.as_raw_chunks():
            ret += len(chunk)
        return ret

    def _make_sha(self):
        ret = make_sha()
        ret.update(self._header())
        for chunk in self.as_raw_chunks():
            ret.update(chunk)
        return ret

    def sha(self):
        """The SHA1 object that is the name of this object."""
        if self._needs_serialization or self._sha is None:
            self._sha = self._make_sha()
        return self._sha

    @property
    def id(self):
        return self.sha().hexdigest()

    def get_type(self):
        return self.type_num

    def set_type(self, type):
        self.type_num = type

    # DEPRECATED: use type_num or type_name as needed.
    type = property(get_type, set_type)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.id)

    def __ne__(self, other):
        return self.id != other.id

    def __eq__(self, other):
        """Return true if the sha of the two objects match.

        The __le__ etc methods aren't overriden as they make no sense,
        certainly at this level.
        """
        return self.id == other.id


class Blob(ShaFile):
    """A Git Blob object."""

    type_name = 'blob'
    type_num = 3

    def __init__(self):
        super(Blob, self).__init__()
        self._chunked_text = []
        self._needs_parsing = False
        self._needs_serialization = False

    def _get_data(self):
        return self.as_raw_string()

    def _set_data(self, data):
        self.set_raw_string(data)

    data = property(_get_data, _set_data,
            "The text contained within the blob object.")

    def _get_chunked(self):
        return self._chunked_text

    def _set_chunked(self, chunks):
        self._chunked_text = chunks

    chunked = property(_get_chunked, _set_chunked,
        "The text within the blob object, as chunks (not necessarily lines).")

    @classmethod
    def from_file(cls, filename):
        blob = ShaFile.from_file(filename)
        if not isinstance(blob, cls):
            raise NotBlobError(filename)
        return blob


class Tag(ShaFile):
    """A Git Tag object."""

    type_name = 'tag'
    type_num = 4

    def __init__(self):
        super(Tag, self).__init__()
        self._needs_parsing = False
        self._needs_serialization = True

    @classmethod
    def from_file(cls, filename):
        tag = ShaFile.from_file(filename)
        if not isinstance(tag, cls):
            raise NotTagError(filename)
        return tag

    @classmethod
    def from_string(cls, string):
        """Create a blob from a string."""
        shafile = cls()
        shafile.set_raw_string(string)
        return shafile

    def _serialize(self):
        chunks = []
        chunks.append("%s %s\n" % (_OBJECT_HEADER, self._object_sha))
        chunks.append("%s %s\n" % (_TYPE_HEADER, self._object_class.type_name))
        chunks.append("%s %s\n" % (_TAG_HEADER, self._name))
        if self._tagger:
            if self._tag_time is None:
                chunks.append("%s %s\n" % (_TAGGER_HEADER, self._tagger))
            else:
                chunks.append("%s %s %d %s\n" % (
                  _TAGGER_HEADER, self._tagger, self._tag_time,
                  format_timezone(self._tag_timezone)))
        chunks.append("\n") # To close headers
        chunks.append(self._message)
        return chunks

    def _deserialize(self, chunks):
        """Grab the metadata attached to the tag"""
        self._tagger = None
        f = StringIO("".join(chunks))
        for l in f:
            l = l.rstrip("\n")
            if l == "":
                break # empty line indicates end of headers
            (field, value) = l.split(" ", 1)
            if field == _OBJECT_HEADER:
                self._object_sha = value
            elif field == _TYPE_HEADER:
                self._object_class = object_class(value)
            elif field == _TAG_HEADER:
                self._name = value
            elif field == _TAGGER_HEADER:
                try:
                    sep = value.index("> ")
                except ValueError:
                    self._tagger = value
                    self._tag_time = None
                    self._tag_timezone = None
                else:
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

    def _get_object(self):
        """Get the object pointed to by this tag.

        :return: tuple of (object class, sha).
        """
        self._ensure_parsed()
        return (self._object_class, self._object_sha)

    def _set_object(self, value):
        self._ensure_parsed()
        (self._object_class, self._object_sha) = value
        self._needs_serialization = True

    object = property(_get_object, _set_object)

    name = serializable_property("name", "The name of this tag")
    tagger = serializable_property("tagger",
        "Returns the name of the person who created this tag")
    tag_time = serializable_property("tag_time",
        "The creation timestamp of the tag.  As the number of seconds since the epoch")
    tag_timezone = serializable_property("tag_timezone",
        "The timezone that tag_time is in.")
    message = serializable_property("message", "The message attached to this tag")


def parse_tree(text):
    """Parse a tree text.

    :param text: Serialized text to parse
    :yields: tuples of (name, mode, sha)
    """
    count = 0
    l = len(text)
    while count < l:
        mode_end = text.index(' ', count)
        mode = int(text[count:mode_end], 8)
        name_end = text.index('\0', mode_end)
        name = text[mode_end+1:name_end]
        count = name_end+21
        sha = text[name_end+1:count]
        yield (name, mode, sha_to_hex(sha))


def serialize_tree(items):
    """Serialize the items in a tree to a text.

    :param items: Sorted iterable over (name, mode, sha) tuples
    :return: Serialized tree text as chunks
    """
    for name, mode, hexsha in items:
        yield "%04o %s\0%s" % (mode, name, hex_to_sha(hexsha))


def sorted_tree_items(entries):
    """Iterate over a tree entries dictionary in the order in which 
    the items would be serialized.

    :param entries: Dictionary mapping names to (mode, sha) tuples
    :return: Iterator over (name, mode, sha)
    """
    def cmp_entry((name1, value1), (name2, value2)):
        if stat.S_ISDIR(value1[0]):
            name1 += "/"
        if stat.S_ISDIR(value2[0]):
            name2 += "/"
        return cmp(name1, name2)
    for name, entry in sorted(entries.iteritems(), cmp=cmp_entry):
        yield name, entry[0], entry[1]


class Tree(ShaFile):
    """A Git tree object"""

    type_name = 'tree'
    type_num = 2

    def __init__(self):
        super(Tree, self).__init__()
        self._entries = {}
        self._needs_parsing = False
        self._needs_serialization = True

    @classmethod
    def from_file(cls, filename):
        tree = ShaFile.from_file(filename)
        if not isinstance(tree, cls):
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
        # The order of this is different from iteritems() for historical
        # reasons
        return [
            (mode, name, hexsha) for (name, mode, hexsha) in self.iteritems()]

    def iteritems(self):
        """Iterate over all entries in the order in which they would be
        serialized.

        :return: Iterator over (name, mode, sha) tuples
        """
        self._ensure_parsed()
        return sorted_tree_items(self._entries)

    def _deserialize(self, chunks):
        """Grab the entries in the tree"""
        parsed_entries = parse_tree("".join(chunks))
        # TODO: list comprehension is for efficiency in the common (small) case;
        # if memory efficiency in the large case is a concern, use a genexp.
        self._entries = dict([(n, (m, s)) for n, m, s in parsed_entries])
        self._needs_parsing = False

    def _serialize(self):
        return list(serialize_tree(self.iteritems()))

    def as_pretty_string(self):
        text = []
        for name, mode, hexsha in self.iteritems():
            if mode & stat.S_IFDIR:
                kind = "tree"
            else:
                kind = "blob"
            text.append("%04o %s %s\t%s\n" % (mode, kind, hexsha, name))
        return "".join(text)


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

    type_name = 'commit'
    type_num = 1

    def __init__(self):
        super(Commit, self).__init__()
        self._parents = []
        self._encoding = None
        self._needs_parsing = False
        self._needs_serialization = True
        self._extra = {}

    @classmethod
    def from_file(cls, filename):
        commit = ShaFile.from_file(filename)
        if not isinstance(commit, cls):
            raise NotCommitError(filename)
        return commit

    def _deserialize(self, chunks):
        self._parents = []
        self._extra = []
        self._author = None
        f = StringIO("".join(chunks))
        for l in f:
            l = l.rstrip("\n")
            if l == "":
                # Empty line indicates end of headers
                break
            (field, value) = l.split(" ", 1)
            if field == _TREE_HEADER:
                self._tree = value
            elif field == _PARENT_HEADER:
                self._parents.append(value)
            elif field == _AUTHOR_HEADER:
                self._author, timetext, timezonetext = value.rsplit(" ", 2)
                self._author_time = int(timetext)
                self._author_timezone = parse_timezone(timezonetext)
            elif field == _COMMITTER_HEADER:
                self._committer, timetext, timezonetext = value.rsplit(" ", 2)
                self._commit_time = int(timetext)
                self._commit_timezone = parse_timezone(timezonetext)
            elif field == _ENCODING_HEADER:
                self._encoding = value
            else:
                self._extra.append((field, value))
        self._message = f.read()

    def _serialize(self):
        chunks = []
        chunks.append("%s %s\n" % (_TREE_HEADER, self._tree))
        for p in self._parents:
            chunks.append("%s %s\n" % (_PARENT_HEADER, p))
        chunks.append("%s %s %s %s\n" % (
          _AUTHOR_HEADER, self._author, str(self._author_time),
          format_timezone(self._author_timezone)))
        chunks.append("%s %s %s %s\n" % (
          _COMMITTER_HEADER, self._committer, str(self._commit_time),
          format_timezone(self._commit_timezone)))
        if self.encoding:
            chunks.append("%s %s\n" % (_ENCODING_HEADER, self.encoding))
        for k, v in self.extra:
            if "\n" in k or "\n" in v:
                raise AssertionError("newline in extra data: %r -> %r" % (k, v))
            chunks.append("%s %s\n" % (k, v))
        chunks.append("\n") # There must be a new line after the headers
        chunks.append(self._message)
        return chunks

    tree = serializable_property("tree", "Tree that is the state of this commit")

    def _get_parents(self):
        """Return a list of parents of this commit."""
        self._ensure_parsed()
        return self._parents

    def _set_parents(self, value):
        """Set a list of parents of this commit."""
        self._ensure_parsed()
        self._needs_serialization = True
        self._parents = value

    parents = property(_get_parents, _set_parents)

    def _get_extra(self):
        """Return extra settings of this commit."""
        self._ensure_parsed()
        return self._extra

    extra = property(_get_extra)

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


OBJECT_CLASSES = (
    Commit,
    Tree,
    Blob,
    Tag,
    )

_TYPE_MAP = {}

for cls in OBJECT_CLASSES:
    _TYPE_MAP[cls.type_name] = cls
    _TYPE_MAP[cls.type_num] = cls



# Hold on to the pure-python implementations for testing
_parse_tree_py = parse_tree
_sorted_tree_items_py = sorted_tree_items
try:
    # Try to import C versions
    from dulwich._objects import parse_tree, sorted_tree_items
except ImportError:
    pass
