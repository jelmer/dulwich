# _compat.py -- For dealing with python2.4 oddness
# Copyright (C) 2008 Canonical Ltd.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version.
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

"""Misc utilities to work with python <2.6.

These utilities can all be deleted when dulwich decides it wants to stop
support for python <2.6.
"""
try:
    import hashlib
except ImportError:
    import sha

try:
    from urlparse import parse_qs
except ImportError:
    from cgi import parse_qs

try:
    from os import SEEK_CUR, SEEK_END
except ImportError:
    SEEK_CUR = 1
    SEEK_END = 2

import struct


class defaultdict(dict):
    """A python 2.4 equivalent of collections.defaultdict."""

    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
            not hasattr(default_factory, '__call__')):
            raise TypeError('first argument must be callable')
        dict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))
    def __repr__(self):
        return 'defaultdict(%s, %s)' % (self.default_factory,
                                        dict.__repr__(self))


def make_sha(source=''):
    """A python2.4 workaround for the sha/hashlib module fiasco."""
    try:
        return hashlib.sha1(source)
    except NameError:
        sha1 = sha.sha(source)
        return sha1


def unpack_from(fmt, buf, offset=0):
    """A python2.4 workaround for struct missing unpack_from."""
    try:
        return struct.unpack_from(fmt, buf, offset)
    except AttributeError:
        b = buf[offset:offset+struct.calcsize(fmt)]
        return struct.unpack(fmt, b)


try:
    from itertools import permutations
except ImportError:
    # Implementation of permutations from Python 2.6 documentation:
    # http://docs.python.org/2.6/library/itertools.html#itertools.permutations
    # Copyright (c) 2001-2010 Python Software Foundation; All Rights Reserved
    # Modified syntax slightly to run under Python 2.4.
    def permutations(iterable, r=None):
        # permutations('ABCD', 2) --> AB AC AD BA BC BD CA CB CD DA DB DC
        # permutations(range(3)) --> 012 021 102 120 201 210
        pool = tuple(iterable)
        n = len(pool)
        if r is None:
            r = n
        if r > n:
            return
        indices = range(n)
        cycles = range(n, n-r, -1)
        yield tuple(pool[i] for i in indices[:r])
        while n:
            for i in reversed(range(r)):
                cycles[i] -= 1
                if cycles[i] == 0:
                    indices[i:] = indices[i+1:] + indices[i:i+1]
                    cycles[i] = n - i
                else:
                    j = cycles[i]
                    indices[i], indices[-j] = indices[-j], indices[i]
                    yield tuple(pool[i] for i in indices[:r])
                    break
            else:
                return


try:
    from collections import namedtuple

    TreeEntryTuple = namedtuple('TreeEntryTuple', ['path', 'mode', 'sha'])
    TreeChangeTuple = namedtuple('TreeChangeTuple', ['type', 'old', 'new'])
except ImportError:
    # Provide manual implementations of namedtuples for Python <2.5.
    # If the class definitions change, be sure to keep these in sync by running
    # namedtuple(..., verbose=True) in a recent Python and pasting the output.

    # Necessary globals go here.
    _tuple = tuple
    _property = property
    from operator import itemgetter as _itemgetter

    class TreeEntryTuple(tuple):
            'TreeEntryTuple(path, mode, sha)'

            __slots__ = ()

            _fields = ('path', 'mode', 'sha')

            def __new__(_cls, path, mode, sha):
                return _tuple.__new__(_cls, (path, mode, sha))

            @classmethod
            def _make(cls, iterable, new=tuple.__new__, len=len):
                'Make a new TreeEntryTuple object from a sequence or iterable'
                result = new(cls, iterable)
                if len(result) != 3:
                    raise TypeError('Expected 3 arguments, got %d' % len(result))
                return result

            def __repr__(self):
                return 'TreeEntryTuple(path=%r, mode=%r, sha=%r)' % self

            def _asdict(t):
                'Return a new dict which maps field names to their values'
                return {'path': t[0], 'mode': t[1], 'sha': t[2]}

            def _replace(_self, **kwds):
                'Return a new TreeEntryTuple object replacing specified fields with new values'
                result = _self._make(map(kwds.pop, ('path', 'mode', 'sha'), _self))
                if kwds:
                    raise ValueError('Got unexpected field names: %r' % kwds.keys())
                return result

            def __getnewargs__(self):
                return tuple(self)

            path = _property(_itemgetter(0))
            mode = _property(_itemgetter(1))
            sha = _property(_itemgetter(2))


    class TreeChangeTuple(tuple):
            'TreeChangeTuple(type, old, new)'

            __slots__ = ()

            _fields = ('type', 'old', 'new')

            def __new__(_cls, type, old, new):
                return _tuple.__new__(_cls, (type, old, new))

            @classmethod
            def _make(cls, iterable, new=tuple.__new__, len=len):
                'Make a new TreeChangeTuple object from a sequence or iterable'
                result = new(cls, iterable)
                if len(result) != 3:
                    raise TypeError('Expected 3 arguments, got %d' % len(result))
                return result

            def __repr__(self):
                return 'TreeChangeTuple(type=%r, old=%r, new=%r)' % self

            def _asdict(t):
                'Return a new dict which maps field names to their values'
                return {'type': t[0], 'old': t[1], 'new': t[2]}

            def _replace(_self, **kwds):
                'Return a new TreeChangeTuple object replacing specified fields with new values'
                result = _self._make(map(kwds.pop, ('type', 'old', 'new'), _self))
                if kwds:
                    raise ValueError('Got unexpected field names: %r' % kwds.keys())
                return result

            def __getnewargs__(self):
                return tuple(self)

            type = _property(_itemgetter(0))
            old = _property(_itemgetter(1))
            new = _property(_itemgetter(2))
