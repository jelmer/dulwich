# test_file.py -- Test for git files
# Copyright (C) 2010 Google, Inc.
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

import errno
import io
import os
import shutil
import sys
import tempfile
from unittest import SkipTest

from dulwich.file import GitFile, fancy_rename
from dulwich.tests import (
    TestCase,
    )


class FancyRenameTests(TestCase):

    def setUp(self):
        super(FancyRenameTests, self).setUp()
        self._tempdir = tempfile.mkdtemp()
        self.foo = self.path('foo')
        self.bar = self.path('bar')
        self.create(self.foo, 'foo contents')

    def tearDown(self):
        shutil.rmtree(self._tempdir)
        super(FancyRenameTests, self).tearDown()

    def path(self, filename):
        return os.path.join(self._tempdir, filename)

    def create(self, path, contents):
        f = open(path, 'wb')
        f.write(contents)
        f.close()

    def test_no_dest_exists(self):
        self.assertFalse(os.path.exists(self.bar))
        fancy_rename(self.foo, self.bar)
        self.assertFalse(os.path.exists(self.foo))

        new_f = open(self.bar, 'rb')
        self.assertEqual('foo contents', new_f.read())
        new_f.close()

    def test_dest_exists(self):
        self.create(self.bar, 'bar contents')
        fancy_rename(self.foo, self.bar)
        self.assertFalse(os.path.exists(self.foo))

        new_f = open(self.bar, 'rb')
        self.assertEqual('foo contents', new_f.read())
        new_f.close()

    def test_dest_opened(self):
        if sys.platform != "win32":
            raise SkipTest("platform allows overwriting open files")
        self.create(self.bar, 'bar contents')
        dest_f = open(self.bar, 'rb')
        self.assertRaises(OSError, fancy_rename, self.foo, self.bar)
        dest_f.close()
        self.assertTrue(os.path.exists(self.path('foo')))

        new_f = open(self.foo, 'rb')
        self.assertEqual('foo contents', new_f.read())
        new_f.close()

        new_f = open(self.bar, 'rb')
        self.assertEqual('bar contents', new_f.read())
        new_f.close()


class GitFileTests(TestCase):

    def setUp(self):
        super(GitFileTests, self).setUp()
        self._tempdir = tempfile.mkdtemp()
        f = open(self.path('foo'), 'wb')
        f.write('foo contents')
        f.close()

    def tearDown(self):
        shutil.rmtree(self._tempdir)
        super(GitFileTests, self).tearDown()

    def path(self, filename):
        return os.path.join(self._tempdir, filename)

    def test_invalid(self):
        foo = self.path('foo')
        self.assertRaises(IOError, GitFile, foo, mode='r')
        self.assertRaises(IOError, GitFile, foo, mode='ab')
        self.assertRaises(IOError, GitFile, foo, mode='r+b')
        self.assertRaises(IOError, GitFile, foo, mode='w+b')
        self.assertRaises(IOError, GitFile, foo, mode='a+bU')

    def test_readonly(self):
        f = GitFile(self.path('foo'), 'rb')
        self.assertTrue(isinstance(f, io.IOBase))
        self.assertEqual('foo contents', f.read())
        self.assertEqual('', f.read())
        f.seek(4)
        self.assertEqual('contents', f.read())
        f.close()

    def test_default_mode(self):
        f = GitFile(self.path('foo'))
        self.assertEqual('foo contents', f.read())
        f.close()

    def test_write(self):
        foo = self.path('foo')
        foo_lock = '%s.lock' % foo

        orig_f = open(foo, 'rb')
        self.assertEqual(orig_f.read(), 'foo contents')
        orig_f.close()

        self.assertFalse(os.path.exists(foo_lock))
        f = GitFile(foo, 'wb')
        self.assertFalse(f.closed)
        self.assertRaises(AttributeError, getattr, f, 'not_a_file_property')

        self.assertTrue(os.path.exists(foo_lock))
        f.write('new stuff')
        f.seek(4)
        f.write('contents')
        f.close()
        self.assertFalse(os.path.exists(foo_lock))

        new_f = open(foo, 'rb')
        self.assertEqual('new contents', new_f.read())
        new_f.close()

    def test_open_twice(self):
        foo = self.path('foo')
        f1 = GitFile(foo, 'wb')
        f1.write('new')
        try:
            f2 = GitFile(foo, 'wb')
            self.fail()
        except OSError as e:
            self.assertEqual(errno.EEXIST, e.errno)
        else:
            f2.close()
        f1.write(' contents')
        f1.close()

        # Ensure trying to open twice doesn't affect original.
        f = open(foo, 'rb')
        self.assertEqual('new contents', f.read())
        f.close()

    def test_abort(self):
        foo = self.path('foo')
        foo_lock = '%s.lock' % foo

        orig_f = open(foo, 'rb')
        self.assertEqual(orig_f.read(), 'foo contents')
        orig_f.close()

        f = GitFile(foo, 'wb')
        f.write('new contents')
        f.abort()
        self.assertTrue(f.closed)
        self.assertFalse(os.path.exists(foo_lock))

        new_orig_f = open(foo, 'rb')
        self.assertEqual(new_orig_f.read(), 'foo contents')
        new_orig_f.close()

    def test_abort_close(self):
        foo = self.path('foo')
        f = GitFile(foo, 'wb')
        f.abort()
        try:
            f.close()
        except (IOError, OSError):
            self.fail()

        f = GitFile(foo, 'wb')
        f.close()
        try:
            f.abort()
        except (IOError, OSError):
            self.fail()

    def test_abort_close_removed(self):
        foo = self.path('foo')
        f = GitFile(foo, 'wb')

        f._file.close()
        os.remove(foo+".lock")

        f.abort()
        self.assertTrue(f._closed)
