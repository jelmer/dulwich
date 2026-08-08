# test_file.py -- Test for git files
# Copyright (C) 2010 Google, Inc.
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# You should have received a copy of the licenses; if not, see
# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
# License, Version 2.0.
#

import io
import os
import shutil
import stat
import sys
import tempfile

from dulwich.file import (
    PERM_EVERYBODY,
    PERM_GROUP,
    FileLocked,
    GitFile,
    _fancy_rename,
)

from . import SkipTest, TestCase


class FancyRenameTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._tempdir = tempfile.mkdtemp()
        self.foo = self.path("foo")
        self.bar = self.path("bar")
        self.create(self.foo, b"foo contents")

    def tearDown(self) -> None:
        shutil.rmtree(self._tempdir)
        super().tearDown()

    def path(self, filename):
        return os.path.join(self._tempdir, filename)

    def create(self, path, contents) -> None:
        f = open(path, "wb")
        f.write(contents)
        f.close()

    def test_no_dest_exists(self) -> None:
        self.assertFalse(os.path.exists(self.bar))
        _fancy_rename(self.foo, self.bar)
        self.assertFalse(os.path.exists(self.foo))

        new_f = open(self.bar, "rb")
        self.assertEqual(b"foo contents", new_f.read())
        new_f.close()

    def test_dest_exists(self) -> None:
        self.create(self.bar, b"bar contents")
        _fancy_rename(self.foo, self.bar)
        self.assertFalse(os.path.exists(self.foo))

        new_f = open(self.bar, "rb")
        self.assertEqual(b"foo contents", new_f.read())
        new_f.close()

    def test_dest_opened(self) -> None:
        if sys.platform != "win32":
            raise SkipTest("platform allows overwriting open files")
        self.create(self.bar, b"bar contents")
        dest_f = open(self.bar, "rb")
        self.assertRaises(OSError, _fancy_rename, self.foo, self.bar)
        dest_f.close()
        self.assertTrue(os.path.exists(self.path("foo")))

        new_f = open(self.foo, "rb")
        self.assertEqual(b"foo contents", new_f.read())
        new_f.close()

        new_f = open(self.bar, "rb")
        self.assertEqual(b"bar contents", new_f.read())
        new_f.close()


class GitFileTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._tempdir = tempfile.mkdtemp()
        f = open(self.path("foo"), "wb")
        f.write(b"foo contents")
        f.close()

    def tearDown(self) -> None:
        shutil.rmtree(self._tempdir)
        super().tearDown()

    def path(self, filename):
        return os.path.join(self._tempdir, filename)

    def test_invalid(self) -> None:
        foo = self.path("foo")
        self.assertRaises(IOError, GitFile, foo, mode="r")
        self.assertRaises(IOError, GitFile, foo, mode="ab")
        self.assertRaises(IOError, GitFile, foo, mode="r+b")
        self.assertRaises(IOError, GitFile, foo, mode="w+b")
        self.assertRaises(IOError, GitFile, foo, mode="a+bU")

    def test_readonly(self) -> None:
        f = GitFile(self.path("foo"), "rb")
        self.assertIsInstance(f, io.IOBase)
        self.assertEqual(b"foo contents", f.read())
        self.assertEqual(b"", f.read())
        f.seek(4)
        self.assertEqual(b"contents", f.read())
        f.close()

    def test_default_mode(self) -> None:
        f = GitFile(self.path("foo"))
        self.assertEqual(b"foo contents", f.read())
        f.close()

    def test_write(self) -> None:
        foo = self.path("foo")
        foo_lock = f"{foo}.lock"

        orig_f = open(foo, "rb")
        self.assertEqual(orig_f.read(), b"foo contents")
        orig_f.close()

        self.assertFalse(os.path.exists(foo_lock))
        f = GitFile(foo, "wb")
        self.assertFalse(f.closed)
        self.assertRaises(AttributeError, getattr, f, "not_a_file_property")

        self.assertTrue(os.path.exists(foo_lock))
        f.write(b"new stuff")
        f.seek(4)
        f.write(b"contents")
        f.close()
        self.assertFalse(os.path.exists(foo_lock))

        new_f = open(foo, "rb")
        self.assertEqual(b"new contents", new_f.read())
        new_f.close()

    def test_open_twice(self) -> None:
        foo = self.path("foo")
        f1 = GitFile(foo, "wb")
        f1.write(b"new")
        try:
            f2 = GitFile(foo, "wb")
            self.fail()
        except FileLocked:
            pass
        else:
            f2.close()
        f1.write(b" contents")
        f1.close()

        # Ensure trying to open twice doesn't affect original.
        f = open(foo, "rb")
        self.assertEqual(b"new contents", f.read())
        f.close()

    def test_abort(self) -> None:
        foo = self.path("foo")
        foo_lock = f"{foo}.lock"

        orig_f = open(foo, "rb")
        self.assertEqual(orig_f.read(), b"foo contents")
        orig_f.close()

        f = GitFile(foo, "wb")
        f.write(b"new contents")
        f.abort()
        self.assertTrue(f.closed)
        self.assertFalse(os.path.exists(foo_lock))

        new_orig_f = open(foo, "rb")
        self.assertEqual(new_orig_f.read(), b"foo contents")
        new_orig_f.close()

    def test_abort_close(self) -> None:
        foo = self.path("foo")
        f = GitFile(foo, "wb")
        f.abort()
        try:
            f.close()
        except OSError:
            self.fail()

        f = GitFile(foo, "wb")
        f.close()
        try:
            f.abort()
        except OSError:
            self.fail()

    def test_abort_close_removed(self) -> None:
        foo = self.path("foo")
        f = GitFile(foo, "wb")

        f._file.close()
        os.remove(foo + ".lock")

        f.abort()
        self.assertTrue(f._closed)

    def test_shared_perm_applied_on_close(self) -> None:
        if sys.platform == "win32":
            self.skipTest("Windows does not support Unix file permissions")
        foo = self.path("foo")
        old = os.umask(0o022)
        self.addCleanup(os.umask, old)
        with GitFile(foo, "wb", shared_perm=PERM_GROUP) as f:
            f.write(b"data")
        self.assertEqual(0o664, stat.S_IMODE(os.stat(foo).st_mode))

    def test_shared_perm_only_widens(self) -> None:
        if sys.platform == "win32":
            self.skipTest("Windows does not support Unix file permissions")
        foo = self.path("foo")
        old = os.umask(0o077)
        self.addCleanup(os.umask, old)
        # "all" widens a 0o600 file to group write and other read, matching
        # git init --shared=all under the same umask.
        with GitFile(foo, "wb", shared_perm=PERM_EVERYBODY) as f:
            f.write(b"data")
        self.assertEqual(0o664, stat.S_IMODE(os.stat(foo).st_mode))

    def test_shared_perm_keeps_read_only_files_read_only(self) -> None:
        if sys.platform == "win32":
            self.skipTest("Windows does not support Unix file permissions")
        foo = self.path("foo")
        old = os.umask(0o022)
        self.addCleanup(os.umask, old)
        # Loose objects and packs are written read-only; sharing must only
        # widen who may read them, never grant write.
        with GitFile(foo, "wb", mask=0o444, shared_perm=PERM_EVERYBODY) as f:
            f.write(b"data")
        self.assertEqual(0o444, stat.S_IMODE(os.stat(foo).st_mode))

    def test_no_shared_perm_leaves_mask(self) -> None:
        if sys.platform == "win32":
            self.skipTest("Windows does not support Unix file permissions")
        foo = self.path("foo")
        old = os.umask(0o022)
        self.addCleanup(os.umask, old)
        with GitFile(foo, "wb") as f:
            f.write(b"data")
        self.assertEqual(0o644, stat.S_IMODE(os.stat(foo).st_mode))
