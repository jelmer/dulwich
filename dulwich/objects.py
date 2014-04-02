# objects.py -- Access to base git objects
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@samba.org>
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
from io import BytesIO
from collections import namedtuple
import os
import posixpath
import stat
import warnings
import zlib

from dulwich.errors import (
    ChecksumMismatch,
    NotBlobError,
    NotCommitError,
    NotTagError,
    NotTreeError,
    ObjectFormatException,
    )
from dulwich.file import GitFile
from hashlib import sha1

ZERO_SHA = "0" * 40

# Header fields for commits
_TREE_HEADER = "tree"
_PARENT_HEADER = "parent"
_AUTHOR_HEADER = "author"
_COMMITTER_HEADER = "committer"
_ENCODING_HEADER = "encoding"
_MERGETAG_HEADER = "mergetag"

# Header fields for objects
_OBJECT_HEADER = "object"
_TYPE_HEADER = "type"
_TAG_HEADER = "tag"
_TAGGER_HEADER = "tagger"


S_IFGITLINK = 0o160000

def S_ISGITLINK(m):
    """Check if a mode indicates a submodule.

    :param m: Mode to check
    :return: a ``boolean``
    """
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
    try:
        return binascii.unhexlify(hex)
    except TypeError as exc:
        if not isinstance(hex, str):
            raise
        raise ValueError(exc.args[0])


def hex_to_filename(path, hex):
    """Takes a hex sha and returns its filename relative to the given path."""
    dir = hex[:2]
    file = hex[2:]
    # Check from object dir
    return os.path.join(path, dir, file)


def filename_to_hex(filename):
    """Takes an object filename and returns its corresponding hex sha."""
    # grab the last (up to) two path components
    names = filename.rsplit(os.path.sep, 2)[-2:]
    errmsg = "Invalid object filename: %s" % filename
    assert len(names) == 2, errmsg
    base, rest = names
    assert len(base) == 2 and len(rest) == 38, errmsg
    hex = base + rest
    hex_to_sha(hex)
    return hex


def object_header(num_type, length):
    """Return an object header for the given numeric type and text length."""
    return "%s %d\0" % (object_class(num_type).type_name, length)


def serializable_property(name, docstring=None):
    """A property that helps tracking whether serialization is necessary.
    """
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
    :return: The ShaFile subclass corresponding to the given type, or None if
        type is not a valid type name/number.
    """
    return _TYPE_MAP.get(type, None)


def check_hexsha(hex, error_msg):
    """Check if a string is a valid hex sha string.

    :param hex: Hex string to check
    :param error_msg: Error message to use in exception
    :raise ObjectFormatException: Raised when the string is not valid
    """
    try:
        hex_to_sha(hex)
    except (TypeError, AssertionError, ValueError):
        raise ObjectFormatException("%s %s" % (error_msg, hex))


def check_identity(identity, error_msg):
    """Check if the specified identity is valid.

    This will raise an exception if the identity is not valid.

    :param identity: Identity string
    :param error_msg: Error message to use in exception
    """
    email_start = identity.find("<")
    email_end = identity.find(">")
    if (email_start < 0 or email_end < 0 or email_end <= email_start
        or identity.find("<", email_start + 1) >= 0
        or identity.find(">", email_end + 1) >= 0
        or not identity.endswith(">")):
        raise ObjectFormatException(error_msg)


class FixedSha(object):
    """SHA object that behaves like hashlib's but is given a fixed value."""

    __slots__ = ('_hexsha', '_sha')

    def __init__(self, hexsha):
        self._hexsha = hexsha
        self._sha = hex_to_sha(hexsha)

    def digest(self):
        """Return the raw SHA digest."""
        return self._sha

    def hexdigest(self):
        """Return the hex SHA digest."""
        return self._hexsha


class ShaFile(object):
    """A git SHA file."""

    __slots__ = ('_needs_parsing', '_chunked_text', '_file', '_path',
                 '_sha', '_needs_serialization', '_magic')

    @staticmethod
    def _parse_legacy_object_header(magic, f):
        """Parse a legacy object, creating it but not reading the file."""
        bufsize = 1024
        decomp = zlib.decompressobj()
        header = decomp.decompress(magic)
        start = 0
        end = -1
        while end < 0:
            extra = f.read(bufsize)
            header += decomp.decompress(extra)
            magic += extra
            end = header.find("\0", start)
            start = len(header)
        header = header[:end]
        type_name, size = header.split(" ", 1)
        size = int(size)  # sanity check
        obj_class = object_class(type_name)
        if not obj_class:
            raise ObjectFormatException("Not a known type: %s" % type_name)
        ret = obj_class()
        ret._magic = magic
        return ret

    def _parse_legacy_object(self, map):
        """Parse a legacy object, setting the raw string."""
        text = _decompress(map)
        header_end = text.find('\0')
        if header_end < 0:
            raise ObjectFormatException("Invalid object header, no \\0")
        self.set_raw_string(text[header_end+1:])

    def as_legacy_object_chunks(self):
        """Return chunks representing the object in the experimental format.

        :return: List of strings
        """
        compobj = zlib.compressobj()
        yield compobj.compress(self._header())
        for chunk in self.as_raw_chunks():
            yield compobj.compress(chunk)
        yield compobj.flush()

    def as_legacy_object(self):
        """Return string representing the object in the experimental format.
        """
        return "".join(self.as_legacy_object_chunks())

    def as_raw_chunks(self):
        """Return chunks with serialization of the object.

        :return: List of strings, not necessarily one per line
        """
        if self._needs_parsing:
            self._ensure_parsed()
        elif self._needs_serialization:
            self._chunked_text = self._serialize()
        return self._chunked_text

    def as_raw_string(self):
        """Return raw string with serialization of the object.

        :return: String object
        """
        return "".join(self.as_raw_chunks())

    def __str__(self):
        """Return raw string serialization of this object."""
        return self.as_raw_string()

    def __hash__(self):
        """Return unique hash for this object."""
        return hash(self.id)

    def as_pretty_string(self):
        """Return a string representing this object, fit for display."""
        return self.as_raw_string()

    def _ensure_parsed(self):
        if self._needs_parsing:
            if not self._chunked_text:
                if self._file is not None:
                    self._parse_file(self._file)
                    self._file = None
                elif self._path is not None:
                    self._parse_path()
                else:
                    raise AssertionError(
                        "ShaFile needs either text or filename")
            self._deserialize(self._chunked_text)
            self._needs_parsing = False

    def set_raw_string(self, text, sha=None):
        """Set the contents of this object from a serialized string."""
        if not isinstance(text, str):
            raise TypeError(text)
        self.set_raw_chunks([text], sha)

    def set_raw_chunks(self, chunks, sha=None):
        """Set the contents of this object from a list of chunks."""
        self._chunked_text = chunks
        self._deserialize(chunks)
        if sha is None:
            self._sha = None
        else:
            self._sha = FixedSha(sha)
        self._needs_parsing = False
        self._needs_serialization = False

    @staticmethod
    def _parse_object_header(magic, f):
        """Parse a new style object, creating it but not reading the file."""
        num_type = (ord(magic[0]) >> 4) & 7
        obj_class = object_class(num_type)
        if not obj_class:
            raise ObjectFormatException("Not a known type %d" % num_type)
        ret = obj_class()
        ret._magic = magic
        return ret

    def _parse_object(self, map):
        """Parse a new style object, setting self._text."""
        # skip type and size; type must have already been determined, and
        # we trust zlib to fail if it's otherwise corrupted
        byte = ord(map[0])
        used = 1
        while (byte & 0x80) != 0:
            byte = ord(map[used])
            used += 1
        raw = map[used:]
        self.set_raw_string(_decompress(raw))

    @classmethod
    def _is_legacy_object(cls, magic):
        b0, b1 = map(ord, magic)
        word = (b0 << 8) + b1
        return (b0 & 0x8F) == 0x08 and (word % 31) == 0

    @classmethod
    def _parse_file_header(cls, f):
        magic = f.read(2)
        if cls._is_legacy_object(magic):
            return cls._parse_legacy_object_header(magic, f)
        else:
            return cls._parse_object_header(magic, f)

    def __init__(self):
        """Don't call this directly"""
        self._sha = None
        self._path = None
        self._file = None
        self._magic = None
        self._chunked_text = []
        self._needs_parsing = False
        self._needs_serialization = True

    def _deserialize(self, chunks):
        raise NotImplementedError(self._deserialize)

    def _serialize(self):
        raise NotImplementedError(self._serialize)

    def _parse_path(self):
        f = GitFile(self._path, 'rb')
        try:
            self._parse_file(f)
        finally:
            f.close()

    def _parse_file(self, f):
        magic = self._magic
        if magic is None:
            magic = f.read(2)
        map = magic + f.read()
        if self._is_legacy_object(magic[:2]):
            self._parse_legacy_object(map)
        else:
            self._parse_object(map)

    @classmethod
    def from_path(cls, path):
        """Open a SHA file from disk."""
        f = GitFile(path, 'rb')
        try:
            obj = cls.from_file(f)
            obj._path = path
            obj._sha = FixedSha(filename_to_hex(path))
            obj._file = None
            obj._magic = None
            return obj
        finally:
            f.close()

    @classmethod
    def from_file(cls, f):
        """Get the contents of a SHA file on disk."""
        try:
            obj = cls._parse_file_header(f)
            obj._sha = None
            obj._needs_parsing = True
            obj._needs_serialization = True
            obj._file = f
            return obj
        except (IndexError, ValueError) as e:
            raise ObjectFormatException("invalid object header")

    @staticmethod
    def from_raw_string(type_num, string, sha=None):
        """Creates an object of the indicated type from the raw string given.

        :param type_num: The numeric type of the object.
        :param string: The raw uncompressed contents.
        :param sha: Optional known sha for the object
        """
        obj = object_class(type_num)()
        obj.set_raw_string(string, sha)
        return obj

    @staticmethod
    def from_raw_chunks(type_num, chunks, sha=None):
        """Creates an object of the indicated type from the raw chunks given.

        :param type_num: The numeric type of the object.
        :param chunks: An iterable of the raw uncompressed contents.
        :param sha: Optional known sha for the object
        """
        obj = object_class(type_num)()
        obj.set_raw_chunks(chunks, sha)
        return obj

    @classmethod
    def from_string(cls, string):
        """Create a ShaFile from a string."""
        obj = cls()
        obj.set_raw_string(string)
        return obj

    def _check_has_member(self, member, error_msg):
        """Check that the object has a given member variable.

        :param member: the member variable to check for
        :param error_msg: the message for an error if the member is missing
        :raise ObjectFormatException: with the given error_msg if member is
            missing or is None
        """
        if getattr(self, member, None) is None:
            raise ObjectFormatException(error_msg)

    def check(self):
        """Check this object for internal consistency.

        :raise ObjectFormatException: if the object is malformed in some way
        :raise ChecksumMismatch: if the object was created with a SHA that does
            not match its contents
        """
        # TODO: if we find that error-checking during object parsing is a
        # performance bottleneck, those checks should be moved to the class's
        # check() method during optimization so we can still check the object
        # when necessary.
        old_sha = self.id
        try:
            self._deserialize(self.as_raw_chunks())
            self._sha = None
            new_sha = self.id
        except Exception as e:
            raise ObjectFormatException(e)
        if old_sha != new_sha:
            raise ChecksumMismatch(new_sha, old_sha)

    def _header(self):
        return object_header(self.type, self.raw_length())

    def raw_length(self):
        """Returns the length of the raw string of this object."""
        ret = 0
        for chunk in self.as_raw_chunks():
            ret += len(chunk)
        return ret

    def _make_sha(self):
        ret = sha1()
        ret.update(self._header())
        for chunk in self.as_raw_chunks():
            ret.update(chunk)
        return ret

    def sha(self):
        """The SHA1 object that is the name of this object."""
        if self._sha is None or self._needs_serialization:
            # this is a local because as_raw_chunks() overwrites self._sha
            new_sha = sha1()
            new_sha.update(self._header())
            for chunk in self.as_raw_chunks():
                new_sha.update(chunk)
            self._sha = new_sha
        return self._sha

    @property
    def id(self):
        """The hex SHA of this object."""
        return self.sha().hexdigest()

    def get_type(self):
        """Return the type number for this object class."""
        return self.type_num

    def set_type(self, type):
        """Set the type number for this object class."""
        self.type_num = type

    # DEPRECATED: use type_num or type_name as needed.
    type = property(get_type, set_type)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.id)

    def __ne__(self, other):
        return not isinstance(other, ShaFile) or self.id != other.id

    def __eq__(self, other):
        """Return True if the SHAs of the two objects match.

        It doesn't make sense to talk about an order on ShaFiles, so we don't
        override the rich comparison methods (__le__, etc.).
        """
        return isinstance(other, ShaFile) and self.id == other.id


class Blob(ShaFile):
    """A Git Blob object."""

    __slots__ = ()

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
        self._ensure_parsed()
        return self._chunked_text

    def _set_chunked(self, chunks):
        self._chunked_text = chunks

    def _serialize(self):
        if not self._chunked_text:
            self._ensure_parsed()
        self._needs_serialization = False
        return self._chunked_text

    def _deserialize(self, chunks):
        self._chunked_text = chunks

    chunked = property(_get_chunked, _set_chunked,
        "The text within the blob object, as chunks (not necessarily lines).")

    @classmethod
    def from_path(cls, path):
        blob = ShaFile.from_path(path)
        if not isinstance(blob, cls):
            raise NotBlobError(path)
        return blob

    def check(self):
        """Check this object for internal consistency.

        :raise ObjectFormatException: if the object is malformed in some way
        """
        super(Blob, self).check()


def _parse_message(chunks):
    """Parse a message with a list of fields and a body.

    :param chunks: the raw chunks of the tag or commit object.
    :return: iterator of tuples of (field, value), one per header line, in the
        order read from the text, possibly including duplicates. Includes a
        field named None for the freeform tag/commit text.
    """
    f = BytesIO("".join(chunks))
    k = None
    v = ""
    for l in f:
        if l.startswith(" "):
            v += l[1:]
        else:
            if k is not None:
                yield (k, v.rstrip("\n"))
            if l == "\n":
                # Empty line indicates end of headers
                break
            (k, v) = l.split(" ", 1)
    yield (None, f.read())
    f.close()


class Tag(ShaFile):
    """A Git Tag object."""

    type_name = 'tag'
    type_num = 4

    __slots__ = ('_tag_timezone_neg_utc', '_name', '_object_sha',
                 '_object_class', '_tag_time', '_tag_timezone',
                 '_tagger', '_message')

    def __init__(self):
        super(Tag, self).__init__()
        self._tag_timezone_neg_utc = False

    @classmethod
    def from_path(cls, filename):
        tag = ShaFile.from_path(filename)
        if not isinstance(tag, cls):
            raise NotTagError(filename)
        return tag

    def check(self):
        """Check this object for internal consistency.

        :raise ObjectFormatException: if the object is malformed in some way
        """
        super(Tag, self).check()
        self._check_has_member("_object_sha", "missing object sha")
        self._check_has_member("_object_class", "missing object type")
        self._check_has_member("_name", "missing tag name")

        if not self._name:
            raise ObjectFormatException("empty tag name")

        check_hexsha(self._object_sha, "invalid object sha")

        if getattr(self, "_tagger", None):
            check_identity(self._tagger, "invalid tagger")

        last = None
        for field, _ in _parse_message(self._chunked_text):
            if field == _OBJECT_HEADER and last is not None:
                raise ObjectFormatException("unexpected object")
            elif field == _TYPE_HEADER and last != _OBJECT_HEADER:
                raise ObjectFormatException("unexpected type")
            elif field == _TAG_HEADER and last != _TYPE_HEADER:
                raise ObjectFormatException("unexpected tag name")
            elif field == _TAGGER_HEADER and last != _TAG_HEADER:
                raise ObjectFormatException("unexpected tagger")
            last = field

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
                  format_timezone(self._tag_timezone,
                    self._tag_timezone_neg_utc)))
        chunks.append("\n") # To close headers
        chunks.append(self._message)
        return chunks

    def _deserialize(self, chunks):
        """Grab the metadata attached to the tag"""
        self._tagger = None
        for field, value in _parse_message(chunks):
            if field == _OBJECT_HEADER:
                self._object_sha = value
            elif field == _TYPE_HEADER:
                obj_class = object_class(value)
                if not obj_class:
                    raise ObjectFormatException("Not a known type: %s" % value)
                self._object_class = obj_class
            elif field == _TAG_HEADER:
                self._name = value
            elif field == _TAGGER_HEADER:
                try:
                    sep = value.index("> ")
                except ValueError:
                    self._tagger = value
                    self._tag_time = None
                    self._tag_timezone = None
                    self._tag_timezone_neg_utc = False
                else:
                    self._tagger = value[0:sep+1]
                    try:
                        (timetext, timezonetext) = value[sep+2:].rsplit(" ", 1)
                        self._tag_time = int(timetext)
                        self._tag_timezone, self._tag_timezone_neg_utc = \
                                parse_timezone(timezonetext)
                    except ValueError as e:
                        raise ObjectFormatException(e)
            elif field is None:
                self._message = value
            else:
                raise ObjectFormatException("Unknown field %s" % field)

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


class TreeEntry(namedtuple('TreeEntry', ['path', 'mode', 'sha'])):
    """Named tuple encapsulating a single tree entry."""

    def in_path(self, path):
        """Return a copy of this entry with the given path prepended."""
        if not isinstance(self.path, str):
            raise TypeError
        return TreeEntry(posixpath.join(path, self.path), self.mode, self.sha)


def parse_tree(text, strict=False):
    """Parse a tree text.

    :param text: Serialized text to parse
    :return: iterator of tuples of (name, mode, sha)
    :raise ObjectFormatException: if the object was malformed in some way
    """
    count = 0
    l = len(text)
    while count < l:
        mode_end = text.index(' ', count)
        mode_text = text[count:mode_end]
        if strict and mode_text.startswith('0'):
            raise ObjectFormatException("Invalid mode '%s'" % mode_text)
        try:
            mode = int(mode_text, 8)
        except ValueError:
            raise ObjectFormatException("Invalid mode '%s'" % mode_text)
        name_end = text.index('\0', mode_end)
        name = text[mode_end+1:name_end]
        count = name_end+21
        sha = text[name_end+1:count]
        if len(sha) != 20:
            raise ObjectFormatException("Sha has invalid length")
        hexsha = sha_to_hex(sha)
        yield (name, mode, hexsha)


def serialize_tree(items):
    """Serialize the items in a tree to a text.

    :param items: Sorted iterable over (name, mode, sha) tuples
    :return: Serialized tree text as chunks
    """
    for name, mode, hexsha in items:
        yield "%04o %s\0%s" % (mode, name, hex_to_sha(hexsha))


def sorted_tree_items(entries, name_order):
    """Iterate over a tree entries dictionary.

    :param name_order: If True, iterate entries in order of their name. If
        False, iterate entries in tree order, that is, treat subtree entries as
        having '/' appended.
    :param entries: Dictionary mapping names to (mode, sha) tuples
    :return: Iterator over (name, mode, hexsha)
    """
    key_func = name_order and key_entry_name_order or key_entry
    for name, entry in sorted(entries.iteritems(), key=key_func):
        mode, hexsha = entry
        # Stricter type checks than normal to mirror checks in the C version.
        if not isinstance(mode, int) and not isinstance(mode, long):
            raise TypeError('Expected integer/long for mode, got %r' % mode)
        mode = int(mode)
        if not isinstance(hexsha, str):
            raise TypeError('Expected a string for SHA, got %r' % hexsha)
        yield TreeEntry(name, mode, hexsha)


def key_entry(entry):
    """Sort key for tree entry.

    :param entry: (name, value) tuplee
    """
    (name, value) = entry
    if stat.S_ISDIR(value[0]):
        name += "/"
    return name


def key_entry_name_order(entry):
    """Sort key for tree entry in name order."""
    return entry[0]


class Tree(ShaFile):
    """A Git tree object"""

    type_name = 'tree'
    type_num = 2

    __slots__ = ('_entries')

    def __init__(self):
        super(Tree, self).__init__()
        self._entries = {}

    @classmethod
    def from_path(cls, filename):
        tree = ShaFile.from_path(filename)
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
        """Set a tree entry by name.

        :param name: The name of the entry, as a string.
        :param value: A tuple of (mode, hexsha), where mode is the mode of the
            entry as an integral type and hexsha is the hex SHA of the entry as
            a string.
        """
        mode, hexsha = value
        self._ensure_parsed()
        self._entries[name] = (mode, hexsha)
        self._needs_serialization = True

    def __delitem__(self, name):
        self._ensure_parsed()
        del self._entries[name]
        self._needs_serialization = True

    def __len__(self):
        self._ensure_parsed()
        return len(self._entries)

    def __iter__(self):
        self._ensure_parsed()
        return iter(self._entries)

    def add(self, name, mode, hexsha):
        """Add an entry to the tree.

        :param mode: The mode of the entry as an integral type. Not all
            possible modes are supported by git; see check() for details.
        :param name: The name of the entry, as a string.
        :param hexsha: The hex SHA of the entry as a string.
        """
        if isinstance(name, int) and isinstance(mode, str):
            (name, mode) = (mode, name)
            warnings.warn("Please use Tree.add(name, mode, hexsha)",
                category=DeprecationWarning, stacklevel=2)
        self._ensure_parsed()
        self._entries[name] = mode, hexsha
        self._needs_serialization = True

    def iteritems(self, name_order=False):
        """Iterate over entries.

        :param name_order: If True, iterate in name order instead of tree order.
        :return: Iterator over (name, mode, sha) tuples
        """
        self._ensure_parsed()
        return sorted_tree_items(self._entries, name_order)

    def items(self):
        """Return the sorted entries in this tree.

        :return: List with (name, mode, sha) tuples
        """
        return list(self.iteritems())

    def _deserialize(self, chunks):
        """Grab the entries in the tree"""
        try:
            parsed_entries = parse_tree("".join(chunks))
        except ValueError as e:
            raise ObjectFormatException(e)
        # TODO: list comprehension is for efficiency in the common (small) case;
        # if memory efficiency in the large case is a concern, use a genexp.
        self._entries = dict([(n, (m, s)) for n, m, s in parsed_entries])

    def check(self):
        """Check this object for internal consistency.

        :raise ObjectFormatException: if the object is malformed in some way
        """
        super(Tree, self).check()
        last = None
        allowed_modes = (stat.S_IFREG | 0o755, stat.S_IFREG | 0o644,
                         stat.S_IFLNK, stat.S_IFDIR, S_IFGITLINK,
                         # TODO: optionally exclude as in git fsck --strict
                         stat.S_IFREG | 0o664)
        for name, mode, sha in parse_tree(''.join(self._chunked_text),
                                          True):
            check_hexsha(sha, 'invalid sha %s' % sha)
            if '/' in name or name in ('', '.', '..'):
                raise ObjectFormatException('invalid name %s' % name)

            if mode not in allowed_modes:
                raise ObjectFormatException('invalid mode %06o' % mode)

            entry = (name, (mode, sha))
            if last:
                if key_entry(last) > key_entry(entry):
                    raise ObjectFormatException('entries not sorted')
                if name == last[0]:
                    raise ObjectFormatException('duplicate entry %s' % name)
            last = entry

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

    def lookup_path(self, lookup_obj, path):
        """Look up an object in a Git tree.

        :param lookup_obj: Callback for retrieving object by SHA1
        :param path: Path to lookup
        :return: A tuple of (mode, SHA) of the resulting path.
        """
        parts = path.split('/')
        sha = self.id
        mode = None
        for p in parts:
            if not p:
                continue
            obj = lookup_obj(sha)
            if not isinstance(obj, Tree):
                raise NotTreeError(sha)
            mode, sha = obj[p]
        return mode, sha


def parse_timezone(text):
    """Parse a timezone text fragment (e.g. '+0100').

    :param text: Text to parse.
    :return: Tuple with timezone as seconds difference to UTC
        and a boolean indicating whether this was a UTC timezone
        prefixed with a negative sign (-0000).
    """
    # cgit parses the first character as the sign, and the rest
    #  as an integer (using strtol), which could also be negative.
    #  We do the same for compatibility. See #697828.
    if not text[0] in '+-':
        raise ValueError("Timezone must start with + or - (%(text)s)" % vars())
    sign = text[0]
    offset = int(text[1:])
    if sign == '-':
        offset = -offset
    unnecessary_negative_timezone = (offset >= 0 and sign == '-')
    signum = (offset < 0) and -1 or 1
    offset = abs(offset)
    hours = int(offset / 100)
    minutes = (offset % 100)
    return (signum * (hours * 3600 + minutes * 60),
            unnecessary_negative_timezone)


def format_timezone(offset, unnecessary_negative_timezone=False):
    """Format a timezone for Git serialization.

    :param offset: Timezone offset as seconds difference to UTC
    :param unnecessary_negative_timezone: Whether to use a minus sign for
        UTC or positive timezones (-0000 and --700 rather than +0000 / +0700).
    """
    if offset % 60 != 0:
        raise ValueError("Unable to handle non-minute offset.")
    if offset < 0 or unnecessary_negative_timezone:
        sign = '-'
        offset = -offset
    else:
        sign = '+'
    return '%c%02d%02d' % (sign, offset / 3600, (offset / 60) % 60)


def parse_commit(chunks):
    """Parse a commit object from chunks.

    :param chunks: Chunks to parse
    :return: Tuple of (tree, parents, author_info, commit_info,
        encoding, mergetag, message, extra)
    """
    parents = []
    extra = []
    tree = None
    author_info = (None, None, (None, None))
    commit_info = (None, None, (None, None))
    encoding = None
    mergetag = []
    message = None

    for field, value in _parse_message(chunks):
        if field == _TREE_HEADER:
            tree = value
        elif field == _PARENT_HEADER:
            parents.append(value)
        elif field == _AUTHOR_HEADER:
            author, timetext, timezonetext = value.rsplit(" ", 2)
            author_time = int(timetext)
            author_info = (author, author_time, parse_timezone(timezonetext))
        elif field == _COMMITTER_HEADER:
            committer, timetext, timezonetext = value.rsplit(" ", 2)
            commit_time = int(timetext)
            commit_info = (committer, commit_time, parse_timezone(timezonetext))
        elif field == _ENCODING_HEADER:
            encoding = value
        elif field == _MERGETAG_HEADER:
            mergetag.append(Tag.from_string(value + "\n"))
        elif field is None:
            message = value
        else:
            extra.append((field, value))
    return (tree, parents, author_info, commit_info, encoding, mergetag,
            message, extra)


class Commit(ShaFile):
    """A git commit object"""

    type_name = 'commit'
    type_num = 1

    __slots__ = ('_parents', '_encoding', '_extra', '_author_timezone_neg_utc',
                 '_commit_timezone_neg_utc', '_commit_time',
                 '_author_time', '_author_timezone', '_commit_timezone',
                 '_author', '_committer', '_parents', '_extra',
                 '_encoding', '_tree', '_message', '_mergetag')

    def __init__(self):
        super(Commit, self).__init__()
        self._parents = []
        self._encoding = None
        self._mergetag = []
        self._extra = []
        self._author_timezone_neg_utc = False
        self._commit_timezone_neg_utc = False

    @classmethod
    def from_path(cls, path):
        commit = ShaFile.from_path(path)
        if not isinstance(commit, cls):
            raise NotCommitError(path)
        return commit

    def _deserialize(self, chunks):
        (self._tree, self._parents, author_info, commit_info, self._encoding,
                self._mergetag, self._message, self._extra) = \
                        parse_commit(chunks)
        (self._author, self._author_time, (self._author_timezone,
            self._author_timezone_neg_utc)) = author_info
        (self._committer, self._commit_time, (self._commit_timezone,
            self._commit_timezone_neg_utc)) = commit_info

    def check(self):
        """Check this object for internal consistency.

        :raise ObjectFormatException: if the object is malformed in some way
        """
        super(Commit, self).check()
        self._check_has_member("_tree", "missing tree")
        self._check_has_member("_author", "missing author")
        self._check_has_member("_committer", "missing committer")
        # times are currently checked when set

        for parent in self._parents:
            check_hexsha(parent, "invalid parent sha")
        check_hexsha(self._tree, "invalid tree sha")

        check_identity(self._author, "invalid author")
        check_identity(self._committer, "invalid committer")

        last = None
        for field, _ in _parse_message(self._chunked_text):
            if field == _TREE_HEADER and last is not None:
                raise ObjectFormatException("unexpected tree")
            elif field == _PARENT_HEADER and last not in (_PARENT_HEADER,
                                                          _TREE_HEADER):
                raise ObjectFormatException("unexpected parent")
            elif field == _AUTHOR_HEADER and last not in (_TREE_HEADER,
                                                          _PARENT_HEADER):
                raise ObjectFormatException("unexpected author")
            elif field == _COMMITTER_HEADER and last != _AUTHOR_HEADER:
                raise ObjectFormatException("unexpected committer")
            elif field == _ENCODING_HEADER and last != _COMMITTER_HEADER:
                raise ObjectFormatException("unexpected encoding")
            last = field

        # TODO: optionally check for duplicate parents

    def _serialize(self):
        chunks = []
        chunks.append("%s %s\n" % (_TREE_HEADER, self._tree))
        for p in self._parents:
            chunks.append("%s %s\n" % (_PARENT_HEADER, p))
        chunks.append("%s %s %s %s\n" % (
          _AUTHOR_HEADER, self._author, str(self._author_time),
          format_timezone(self._author_timezone,
                          self._author_timezone_neg_utc)))
        chunks.append("%s %s %s %s\n" % (
          _COMMITTER_HEADER, self._committer, str(self._commit_time),
          format_timezone(self._commit_timezone,
                          self._commit_timezone_neg_utc)))
        if self.encoding:
            chunks.append("%s %s\n" % (_ENCODING_HEADER, self.encoding))
        for mergetag in self.mergetag:
            mergetag_chunks = mergetag.as_raw_string().split("\n")

            chunks.append("%s %s\n" % (_MERGETAG_HEADER, mergetag_chunks[0]))
            # Embedded extra header needs leading space
            for chunk in mergetag_chunks[1:]:
                chunks.append(" %s\n" % chunk)

            # No trailing empty line
            chunks[-1] = chunks[-1].rstrip(" \n")
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

    mergetag = serializable_property("mergetag",
        "Associated signed tag.")


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
