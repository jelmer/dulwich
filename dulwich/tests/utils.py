# utils.py -- Test utilities for Dulwich.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
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

"""Utility functions common to Dulwich tests."""


from cStringIO import StringIO
import datetime
import os
import shutil
import tempfile
import time
import types

from dulwich.objects import (
    FixedSha,
    Commit,
    )
from dulwich.pack import (
    OFS_DELTA,
    REF_DELTA,
    DELTA_TYPES,
    obj_sha,
    SHA1Writer,
    write_pack_header,
    write_pack_object,
    create_delta,
    )
from dulwich.repo import Repo
from dulwich.tests import (
    TestSkipped,
    )


def open_repo(name):
    """Open a copy of a repo in a temporary directory.

    Use this function for accessing repos in dulwich/tests/data/repos to avoid
    accidentally or intentionally modifying those repos in place. Use
    tear_down_repo to delete any temp files created.

    :param name: The name of the repository, relative to
        dulwich/tests/data/repos
    :returns: An initialized Repo object that lives in a temporary directory.
    """
    temp_dir = tempfile.mkdtemp()
    repo_dir = os.path.join(os.path.dirname(__file__), 'data', 'repos', name)
    temp_repo_dir = os.path.join(temp_dir, name)
    shutil.copytree(repo_dir, temp_repo_dir, symlinks=True)
    return Repo(temp_repo_dir)


def tear_down_repo(repo):
    """Tear down a test repository."""
    temp_dir = os.path.dirname(repo.path.rstrip(os.sep))
    shutil.rmtree(temp_dir)


def make_object(cls, **attrs):
    """Make an object for testing and assign some members.

    This method creates a new subclass to allow arbitrary attribute
    reassignment, which is not otherwise possible with objects having __slots__.

    :param attrs: dict of attributes to set on the new object.
    :return: A newly initialized object of type cls.
    """

    class TestObject(cls):
        """Class that inherits from the given class, but without __slots__.

        Note that classes with __slots__ can't have arbitrary attributes monkey-
        patched in, so this is a class that is exactly the same only with a
        __dict__ instead of __slots__.
        """
        pass

    obj = TestObject()
    for name, value in attrs.iteritems():
        if name == 'id':
            # id property is read-only, so we overwrite sha instead.
            sha = FixedSha(value)
            obj.sha = lambda: sha
        else:
            setattr(obj, name, value)
    return obj


def make_commit(**attrs):
    """Make a Commit object with a default set of members.

    :param attrs: dict of attributes to overwrite from the default values.
    :return: A newly initialized Commit object.
    """
    default_time = int(time.mktime(datetime.datetime(2010, 1, 1).timetuple()))
    all_attrs = {'author': 'Test Author <test@nodomain.com>',
                 'author_time': default_time,
                 'author_timezone': 0,
                 'committer': 'Test Committer <test@nodomain.com>',
                 'commit_time': default_time,
                 'commit_timezone': 0,
                 'message': 'Test message.',
                 'parents': [],
                 'tree': '0' * 40}
    all_attrs.update(attrs)
    return make_object(Commit, **all_attrs)


def functest_builder(method, func):
    """Generate a test method that tests the given function."""

    def do_test(self):
        method(self, func)

    return do_test


def ext_functest_builder(method, func):
    """Generate a test method that tests the given extension function.

    This is intended to generate test methods that test both a pure-Python
    version and an extension version using common test code. The extension test
    will raise TestSkipped if the extension is not found.

    Sample usage:

    class MyTest(TestCase);
        def _do_some_test(self, func_impl):
            self.assertEqual('foo', func_impl())

        test_foo = functest_builder(_do_some_test, foo_py)
        test_foo_extension = ext_functest_builder(_do_some_test, _foo_c)

    :param method: The method to run. It must must two parameters, self and the
        function implementation to test.
    :param func: The function implementation to pass to method.
    """

    def do_test(self):
        if not isinstance(func, types.BuiltinFunctionType):
            raise TestSkipped("%s extension not found", func.func_name)
        method(self, func)

    return do_test


def build_pack(objects_spec, store=None):
    """Write test pack data from a concise spec.

    :param objects_spec: A list of (type_num, obj). For non-delta types, obj
        is the string of that object's data.

        For delta types, obj is a tuple of (base, data), where:
          base can be either an index in objects_spec of the base for that
          delta; or for a ref delta, a SHA, in which case the resulting pack
          will be thin and the base will be an external ref.
          data is a string of the full, non-deltified data for that object.
        Note that offsets/refs and deltas are computed within this function.
    :param store: An optional ObjectStore for looking up external refs.

    :return: A tuple of (f, entries), where f is a file-like object pointed
        at the beginning of a pack with the requested data, and entries is a
        list of tuples in the order specified by objects_spec:
          (offset, type num, data, sha, CRC32)
    """
    f = StringIO()
    sf = SHA1Writer(f)
    num_objects = len(objects_spec)
    write_pack_header(sf, num_objects)

    full_objects = {}
    offsets = {}
    crc32s = {}

    while len(full_objects) < num_objects:
        for i, (type_num, data) in enumerate(objects_spec):
            if type_num not in DELTA_TYPES:
                full_objects[i] = (type_num, data,
                                   obj_sha(type_num, [data]))
                continue
            base, data = data
            if isinstance(base, int):
                if base not in full_objects:
                    continue
                base_type_num, _, _ = full_objects[base]
            else:
                base_type_num, _ = store.get_raw(base)
            full_objects[i] = (base_type_num, data,
                               obj_sha(base_type_num, [data]))

    for i, (type_num, obj) in enumerate(objects_spec):
        offset = f.tell()
        if type_num == OFS_DELTA:
            base_index, data = obj
            base = offset - offsets[base_index]
            _, base_data, _ = full_objects[base_index]
            obj = (base, create_delta(base_data, data))
        elif type_num == REF_DELTA:
            base_ref, data = obj
            if isinstance(base_ref, int):
                _, base_data, base = full_objects[base_ref]
            else:
                base_type_num, base_data = store.get_raw(base_ref)
                base = obj_sha(base_type_num, base_data)
            obj = (base, create_delta(base_data, data))

        crc32 = write_pack_object(sf, type_num, obj)
        offsets[i] = offset
        crc32s[i] = crc32

    expected = []
    for i in xrange(num_objects):
        type_num, data, sha = full_objects[i]
        expected.append((offsets[i], type_num, data, sha, crc32s[i]))

    sf.write_sha()
    f.seek(0)
    return f, expected
