# test_refs.py -- tests for refs.py
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.refs."""

import os
import sys
import tempfile
from io import BytesIO
from typing import ClassVar

from dulwich import errors
from dulwich.file import GitFile
from dulwich.objects import ZERO_SHA
from dulwich.protocol import split_peeled_refs, strip_peeled_refs
from dulwich.refs import (
    DictRefsContainer,
    NamespacedRefsContainer,
    SymrefLoop,
    _split_ref_line,
    check_ref_format,
    is_per_worktree_ref,
    parse_remote_ref,
    parse_symref_value,
    read_packed_refs,
    read_packed_refs_with_peeled,
    shorten_ref_name,
    write_packed_refs,
)
from dulwich.repo import Repo
from dulwich.tests.utils import open_repo, tear_down_repo
from dulwich.worktree import add_worktree

from . import SkipTest, TestCase


class CheckRefFormatTests(TestCase):
    """Tests for the check_ref_format function.

    These are the same tests as in the git test suite.
    """

    def test_valid(self) -> None:
        self.assertTrue(check_ref_format(b"heads/foo"))
        self.assertTrue(check_ref_format(b"foo/bar/baz"))
        self.assertTrue(check_ref_format(b"refs///heads/foo"))
        self.assertTrue(check_ref_format(b"foo./bar"))
        self.assertTrue(check_ref_format(b"heads/foo@bar"))
        self.assertTrue(check_ref_format(b"heads/fix.lock.error"))

    def test_invalid(self) -> None:
        self.assertFalse(check_ref_format(b"foo"))
        self.assertFalse(check_ref_format(b"heads/foo/"))
        self.assertFalse(check_ref_format(b"./foo"))
        self.assertFalse(check_ref_format(b".refs/foo"))
        self.assertFalse(check_ref_format(b"heads/foo..bar"))
        self.assertFalse(check_ref_format(b"heads/foo?bar"))
        self.assertFalse(check_ref_format(b"heads/foo.lock"))
        self.assertFalse(check_ref_format(b"heads/v@{ation"))
        self.assertFalse(check_ref_format(b"heads/foo\bar"))


ONES = b"1" * 40
TWOS = b"2" * 40
THREES = b"3" * 40
FOURS = b"4" * 40


class PackedRefsFileTests(TestCase):
    def test_split_ref_line_errors(self) -> None:
        self.assertRaises(errors.PackedRefsException, _split_ref_line, b"singlefield")
        self.assertRaises(errors.PackedRefsException, _split_ref_line, b"badsha name")
        self.assertRaises(
            errors.PackedRefsException,
            _split_ref_line,
            ONES + b" bad/../refname",
        )

    def test_read_without_peeled(self) -> None:
        f = BytesIO(b"\n".join([b"# comment", ONES + b" ref/1", TWOS + b" ref/2"]))
        self.assertEqual(
            [(ONES, b"ref/1"), (TWOS, b"ref/2")], list(read_packed_refs(f))
        )

    def test_read_without_peeled_errors(self) -> None:
        f = BytesIO(b"\n".join([ONES + b" ref/1", b"^" + TWOS]))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

    def test_read_with_peeled(self) -> None:
        f = BytesIO(
            b"\n".join(
                [
                    ONES + b" ref/1",
                    TWOS + b" ref/2",
                    b"^" + THREES,
                    FOURS + b" ref/4",
                ]
            )
        )
        self.assertEqual(
            [
                (ONES, b"ref/1", None),
                (TWOS, b"ref/2", THREES),
                (FOURS, b"ref/4", None),
            ],
            list(read_packed_refs_with_peeled(f)),
        )

    def test_read_with_peeled_errors(self) -> None:
        f = BytesIO(b"\n".join([b"^" + TWOS, ONES + b" ref/1"]))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

        f = BytesIO(b"\n".join([ONES + b" ref/1", b"^" + TWOS, b"^" + THREES]))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

    def test_write_with_peeled(self) -> None:
        f = BytesIO()
        write_packed_refs(f, {b"ref/1": ONES, b"ref/2": TWOS}, {b"ref/1": THREES})
        self.assertEqual(
            b"\n".join(
                [
                    b"# pack-refs with: peeled",
                    ONES + b" ref/1",
                    b"^" + THREES,
                    TWOS + b" ref/2",
                ]
            )
            + b"\n",
            f.getvalue(),
        )

    def test_write_without_peeled(self) -> None:
        f = BytesIO()
        write_packed_refs(f, {b"ref/1": ONES, b"ref/2": TWOS})
        self.assertEqual(
            b"\n".join([ONES + b" ref/1", TWOS + b" ref/2"]) + b"\n",
            f.getvalue(),
        )


# Dict of refs that we expect all RefsContainerTests subclasses to define.
_TEST_REFS = {
    b"HEAD": b"42d06bd4b77fed026b154d16493e5deab78f02ec",
    b"refs/heads/40-char-ref-aaaaaaaaaaaaaaaaaa": b"42d06bd4b77fed026b154d16493e5deab78f02ec",
    b"refs/heads/master": b"42d06bd4b77fed026b154d16493e5deab78f02ec",
    b"refs/heads/packed": b"42d06bd4b77fed026b154d16493e5deab78f02ec",
    b"refs/tags/refs-0.1": b"df6800012397fb85c56e7418dd4eb9405dee075c",
    b"refs/tags/refs-0.2": b"3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8",
    b"refs/heads/loop": b"ref: refs/heads/loop",
}


class RefsContainerTests:
    def test_keys(self) -> None:
        actual_keys = set(self._refs.keys())
        self.assertEqual(set(self._refs.allkeys()), actual_keys)
        self.assertEqual(set(_TEST_REFS.keys()), actual_keys)

        actual_keys = self._refs.keys(b"refs/heads")
        actual_keys.discard(b"loop")
        self.assertEqual(
            [b"40-char-ref-aaaaaaaaaaaaaaaaaa", b"master", b"packed"],
            sorted(actual_keys),
        )
        self.assertEqual(
            [b"refs-0.1", b"refs-0.2"], sorted(self._refs.keys(b"refs/tags"))
        )

    def test_iter(self) -> None:
        actual_keys = set(self._refs.keys())
        self.assertEqual(set(self._refs), actual_keys)
        self.assertEqual(set(_TEST_REFS.keys()), actual_keys)

    def test_as_dict(self) -> None:
        # refs/heads/loop does not show up even if it exists
        expected_refs = dict(_TEST_REFS)
        del expected_refs[b"refs/heads/loop"]
        self.assertEqual(expected_refs, self._refs.as_dict())

    def test_get_symrefs(self) -> None:
        self._refs.set_symbolic_ref(b"refs/heads/src", b"refs/heads/dst")
        symrefs = self._refs.get_symrefs()
        if b"HEAD" in symrefs:
            symrefs.pop(b"HEAD")
        self.assertEqual(
            {
                b"refs/heads/src": b"refs/heads/dst",
                b"refs/heads/loop": b"refs/heads/loop",
            },
            symrefs,
        )

    def test_setitem(self) -> None:
        self._refs[b"refs/some/ref"] = b"42d06bd4b77fed026b154d16493e5deab78f02ec"
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/some/ref"],
        )

        # should accept symref
        self._refs[b"refs/heads/symbolic"] = b"ref: refs/heads/master"
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/heads/symbolic"],
        )

        # should not accept bad ref names
        self.assertRaises(
            errors.RefFormatError,
            self._refs.__setitem__,
            b"notrefs/foo",
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
        )

        # should not accept short sha
        self.assertRaises(
            ValueError,
            self._refs.__setitem__,
            b"refs/some/ref",
            b"42d06bd",
        )

    def test_set_if_equals(self) -> None:
        nines = b"9" * 40
        self.assertFalse(self._refs.set_if_equals(b"HEAD", b"c0ffee", nines))
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec", self._refs[b"HEAD"]
        )

        self.assertTrue(
            self._refs.set_if_equals(
                b"HEAD", b"42d06bd4b77fed026b154d16493e5deab78f02ec", nines
            )
        )
        self.assertEqual(nines, self._refs[b"HEAD"])

        # Setting the ref again is a no-op, but will return True.
        self.assertTrue(self._refs.set_if_equals(b"HEAD", nines, nines))
        self.assertEqual(nines, self._refs[b"HEAD"])

        self.assertTrue(self._refs.set_if_equals(b"refs/heads/master", None, nines))
        self.assertEqual(nines, self._refs[b"refs/heads/master"])

        self.assertTrue(
            self._refs.set_if_equals(b"refs/heads/nonexistent", ZERO_SHA, nines)
        )
        self.assertEqual(nines, self._refs[b"refs/heads/nonexistent"])

    def test_add_if_new(self) -> None:
        nines = b"9" * 40
        self.assertFalse(self._refs.add_if_new(b"refs/heads/master", nines))
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/heads/master"],
        )

        self.assertTrue(self._refs.add_if_new(b"refs/some/ref", nines))
        self.assertEqual(nines, self._refs[b"refs/some/ref"])

    def test_set_symbolic_ref(self) -> None:
        self._refs.set_symbolic_ref(b"refs/heads/symbolic", b"refs/heads/master")
        self.assertEqual(
            b"ref: refs/heads/master",
            self._refs.read_loose_ref(b"refs/heads/symbolic"),
        )
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/heads/symbolic"],
        )

    def test_set_symbolic_ref_overwrite(self) -> None:
        nines = b"9" * 40
        self.assertNotIn(b"refs/heads/symbolic", self._refs)
        self._refs[b"refs/heads/symbolic"] = nines
        self.assertEqual(nines, self._refs.read_loose_ref(b"refs/heads/symbolic"))
        self._refs.set_symbolic_ref(b"refs/heads/symbolic", b"refs/heads/master")
        self.assertEqual(
            b"ref: refs/heads/master",
            self._refs.read_loose_ref(b"refs/heads/symbolic"),
        )
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/heads/symbolic"],
        )

    def test_check_refname(self) -> None:
        self._refs._check_refname(b"HEAD")
        self._refs._check_refname(b"refs/stash")
        self._refs._check_refname(b"refs/heads/foo")

        self.assertRaises(errors.RefFormatError, self._refs._check_refname, b"refs")
        self.assertRaises(
            errors.RefFormatError, self._refs._check_refname, b"notrefs/foo"
        )

    def test_contains(self) -> None:
        self.assertIn(b"refs/heads/master", self._refs)
        self.assertNotIn(b"refs/heads/bar", self._refs)

    def test_delitem(self) -> None:
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/heads/master"],
        )
        del self._refs[b"refs/heads/master"]
        self.assertRaises(KeyError, lambda: self._refs[b"refs/heads/master"])

    def test_remove_if_equals(self) -> None:
        self.assertFalse(self._refs.remove_if_equals(b"HEAD", b"c0ffee"))
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec", self._refs[b"HEAD"]
        )
        self.assertTrue(
            self._refs.remove_if_equals(
                b"refs/tags/refs-0.2",
                b"3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8",
            )
        )
        self.assertTrue(self._refs.remove_if_equals(b"refs/tags/refs-0.2", ZERO_SHA))
        self.assertNotIn(b"refs/tags/refs-0.2", self._refs)

    def test_import_refs_name(self) -> None:
        self._refs[b"refs/remotes/origin/other"] = (
            b"48d01bd4b77fed026b154d16493e5deab78f02ec"
        )
        self._refs.import_refs(
            b"refs/remotes/origin",
            {b"master": b"42d06bd4b77fed026b154d16493e5deab78f02ec"},
        )
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/remotes/origin/master"],
        )
        self.assertEqual(
            b"48d01bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/remotes/origin/other"],
        )

    def test_import_refs_name_prune(self) -> None:
        self._refs[b"refs/remotes/origin/other"] = (
            b"48d01bd4b77fed026b154d16493e5deab78f02ec"
        )
        self._refs.import_refs(
            b"refs/remotes/origin",
            {b"master": b"42d06bd4b77fed026b154d16493e5deab78f02ec"},
            prune=True,
        )
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/remotes/origin/master"],
        )
        self.assertNotIn(b"refs/remotes/origin/other", self._refs)


class DictRefsContainerTests(RefsContainerTests, TestCase):
    def setUp(self) -> None:
        TestCase.setUp(self)
        self._refs = DictRefsContainer(dict(_TEST_REFS))

    def test_invalid_refname(self) -> None:
        # FIXME: Move this test into RefsContainerTests, but requires
        # some way of injecting invalid refs.
        self._refs._refs[b"refs/stash"] = b"00" * 20
        expected_refs = dict(_TEST_REFS)
        del expected_refs[b"refs/heads/loop"]
        expected_refs[b"refs/stash"] = b"00" * 20
        self.assertEqual(expected_refs, self._refs.as_dict())

    def test_set_if_equals_with_symbolic_ref(self) -> None:
        # Test that set_if_equals only updates the requested ref,
        # not all refs in a symbolic reference chain

        # The bug in the original implementation was that when follow()
        # was called on a ref, it would return all refs in the chain,
        # and set_if_equals would update ALL of them instead of just the
        # requested ref.

        # Set up refs
        master_sha = b"1" * 40
        feature_sha = b"2" * 40
        new_sha = b"3" * 40

        self._refs[b"refs/heads/master"] = master_sha
        self._refs[b"refs/heads/feature"] = feature_sha
        # Create a second symbolic ref pointing to feature
        self._refs.set_symbolic_ref(b"refs/heads/other", b"refs/heads/feature")

        # Update refs/heads/other through set_if_equals
        # With the bug, this would update BOTH refs/heads/other AND refs/heads/feature
        # Without the bug, only refs/heads/other should be updated
        # Note: old_ref needs to be the actual stored value (the symref)
        self.assertTrue(
            self._refs.set_if_equals(
                b"refs/heads/other", b"ref: refs/heads/feature", new_sha
            )
        )

        # refs/heads/other should now directly point to new_sha
        self.assertEqual(self._refs.read_ref(b"refs/heads/other"), new_sha)

        # refs/heads/feature should remain unchanged
        # With the bug, refs/heads/feature would also be incorrectly updated to new_sha
        self.assertEqual(self._refs[b"refs/heads/feature"], feature_sha)
        self.assertEqual(self._refs[b"refs/heads/master"], master_sha)


class DiskRefsContainerTests(RefsContainerTests, TestCase):
    def setUp(self) -> None:
        TestCase.setUp(self)
        self._repo = open_repo("refs.git")
        self.addCleanup(tear_down_repo, self._repo)
        self._refs = self._repo.refs

    def test_get_packed_refs(self) -> None:
        self.assertEqual(
            {
                b"refs/heads/packed": b"42d06bd4b77fed026b154d16493e5deab78f02ec",
                b"refs/tags/refs-0.1": b"df6800012397fb85c56e7418dd4eb9405dee075c",
            },
            self._refs.get_packed_refs(),
        )

    def test_get_peeled_not_packed(self) -> None:
        # not packed
        self.assertEqual(None, self._refs.get_peeled(b"refs/tags/refs-0.2"))
        self.assertEqual(
            b"3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8",
            self._refs[b"refs/tags/refs-0.2"],
        )

        # packed, known not peelable
        self.assertEqual(
            self._refs[b"refs/heads/packed"],
            self._refs.get_peeled(b"refs/heads/packed"),
        )

        # packed, peeled
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs.get_peeled(b"refs/tags/refs-0.1"),
        )

    def test_setitem(self) -> None:
        RefsContainerTests.test_setitem(self)
        path = os.path.join(self._refs.path, b"refs", b"some", b"ref")
        with open(path, "rb") as f:
            self.assertEqual(b"42d06bd4b77fed026b154d16493e5deab78f02ec", f.read()[:40])

        self.assertRaises(
            OSError,
            self._refs.__setitem__,
            b"refs/some/ref/sub",
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
        )

    def test_delete_refs_container(self) -> None:
        # We shouldn't delete the refs directory
        self._refs[b"refs/heads/blah"] = b"42d06bd4b77fed026b154d16493e5deab78f02ec"
        for ref in self._refs.allkeys():
            del self._refs[ref]
        self.assertTrue(os.path.exists(os.path.join(self._refs.path, b"refs")))

    def test_setitem_packed(self) -> None:
        with open(os.path.join(self._refs.path, b"packed-refs"), "w") as f:
            f.write("# pack-refs with: peeled fully-peeled sorted \n")
            f.write("42d06bd4b77fed026b154d16493e5deab78f02ec refs/heads/packed\n")

        # It's allowed to set a new ref on a packed ref, the new ref will be
        # placed outside on refs/
        self._refs[b"refs/heads/packed"] = b"3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8"
        packed_ref_path = os.path.join(self._refs.path, b"refs", b"heads", b"packed")
        with open(packed_ref_path, "rb") as f:
            self.assertEqual(b"3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8", f.read()[:40])

        self.assertRaises(
            OSError,
            self._refs.__setitem__,
            b"refs/heads/packed/sub",
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
        )

        # this shouldn't overwrite the packed refs
        self.assertEqual(
            {b"refs/heads/packed": b"42d06bd4b77fed026b154d16493e5deab78f02ec"},
            self._refs.get_packed_refs(),
        )

    def test_add_packed_refs(self) -> None:
        # first, create a non-packed ref
        self._refs[b"refs/heads/packed"] = b"3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8"

        packed_ref_path = os.path.join(self._refs.path, b"refs", b"heads", b"packed")
        self.assertTrue(os.path.exists(packed_ref_path))

        # now overwrite that with a packed ref
        packed_refs_file_path = os.path.join(self._refs.path, b"packed-refs")
        self._refs.add_packed_refs(
            {
                b"refs/heads/packed": b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            }
        )

        # that should kill the file
        self.assertFalse(os.path.exists(packed_ref_path))

        # now delete the packed ref
        self._refs.add_packed_refs(
            {
                b"refs/heads/packed": None,
            }
        )

        # and it's gone!
        self.assertFalse(os.path.exists(packed_ref_path))

        self.assertRaises(
            KeyError,
            self._refs.__getitem__,
            b"refs/heads/packed",
        )

        # just in case, make sure we can't pack HEAD
        self.assertRaises(
            ValueError,
            self._refs.add_packed_refs,
            {b"HEAD": "02ac81614bcdbd585a37b4b0edf8cb8a"},
        )

        # delete all packed refs
        self._refs.add_packed_refs({ref: None for ref in self._refs.get_packed_refs()})

        self.assertEqual({}, self._refs.get_packed_refs())

        # remove the packed ref file, and check that adding nothing doesn't affect that
        os.remove(packed_refs_file_path)

        # adding nothing doesn't make it reappear
        self._refs.add_packed_refs({})

        self.assertFalse(os.path.exists(packed_refs_file_path))

    def test_setitem_symbolic(self) -> None:
        ones = b"1" * 40
        self._refs[b"HEAD"] = ones
        self.assertEqual(ones, self._refs[b"HEAD"])

        # ensure HEAD was not modified
        f = open(os.path.join(self._refs.path, b"HEAD"), "rb")
        v = next(iter(f)).rstrip(b"\n\r")
        f.close()
        self.assertEqual(b"ref: refs/heads/master", v)

        # ensure the symbolic link was written through
        f = open(os.path.join(self._refs.path, b"refs", b"heads", b"master"), "rb")
        self.assertEqual(ones, f.read()[:40])
        f.close()

    def test_set_if_equals(self) -> None:
        RefsContainerTests.test_set_if_equals(self)

        # ensure symref was followed
        self.assertEqual(b"9" * 40, self._refs[b"refs/heads/master"])

        # ensure lockfile was deleted
        self.assertFalse(
            os.path.exists(
                os.path.join(self._refs.path, b"refs", b"heads", b"master.lock")
            )
        )
        self.assertFalse(os.path.exists(os.path.join(self._refs.path, b"HEAD.lock")))

    def test_add_if_new_packed(self) -> None:
        # don't overwrite packed ref
        self.assertFalse(self._refs.add_if_new(b"refs/tags/refs-0.1", b"9" * 40))
        self.assertEqual(
            b"df6800012397fb85c56e7418dd4eb9405dee075c",
            self._refs[b"refs/tags/refs-0.1"],
        )

    def test_add_if_new_symbolic(self) -> None:
        # Use an empty repo instead of the default.
        repo_dir = os.path.join(tempfile.mkdtemp(), "test")
        os.makedirs(repo_dir)
        repo = Repo.init(repo_dir)
        self.addCleanup(tear_down_repo, repo)
        refs = repo.refs

        nines = b"9" * 40
        self.assertEqual(b"ref: refs/heads/master", refs.read_ref(b"HEAD"))
        self.assertNotIn(b"refs/heads/master", refs)
        self.assertTrue(refs.add_if_new(b"HEAD", nines))
        self.assertEqual(b"ref: refs/heads/master", refs.read_ref(b"HEAD"))
        self.assertEqual(nines, refs[b"HEAD"])
        self.assertEqual(nines, refs[b"refs/heads/master"])
        self.assertFalse(refs.add_if_new(b"HEAD", b"1" * 40))
        self.assertEqual(nines, refs[b"HEAD"])
        self.assertEqual(nines, refs[b"refs/heads/master"])

    def test_follow(self) -> None:
        self.assertEqual(
            (
                [b"HEAD", b"refs/heads/master"],
                b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            ),
            self._refs.follow(b"HEAD"),
        )
        self.assertEqual(
            (
                [b"refs/heads/master"],
                b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            ),
            self._refs.follow(b"refs/heads/master"),
        )
        self.assertRaises(SymrefLoop, self._refs.follow, b"refs/heads/loop")

    def test_set_overwrite_loop(self) -> None:
        self.assertRaises(SymrefLoop, self._refs.follow, b"refs/heads/loop")
        self._refs[b"refs/heads/loop"] = b"42d06bd4b77fed026b154d16493e5deab78f02ec"
        self.assertEqual(
            ([b"refs/heads/loop"], b"42d06bd4b77fed026b154d16493e5deab78f02ec"),
            self._refs.follow(b"refs/heads/loop"),
        )

    def test_delitem(self) -> None:
        RefsContainerTests.test_delitem(self)
        ref_file = os.path.join(self._refs.path, b"refs", b"heads", b"master")
        self.assertFalse(os.path.exists(ref_file))
        self.assertNotIn(b"refs/heads/master", self._refs.get_packed_refs())

    def test_delitem_symbolic(self) -> None:
        self.assertEqual(b"ref: refs/heads/master", self._refs.read_loose_ref(b"HEAD"))
        del self._refs[b"HEAD"]
        self.assertRaises(KeyError, lambda: self._refs[b"HEAD"])
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs[b"refs/heads/master"],
        )
        self.assertFalse(os.path.exists(os.path.join(self._refs.path, b"HEAD")))

    def test_remove_if_equals_symref(self) -> None:
        # HEAD is a symref, so shouldn't equal its dereferenced value
        self.assertFalse(
            self._refs.remove_if_equals(
                b"HEAD", b"42d06bd4b77fed026b154d16493e5deab78f02ec"
            )
        )
        self.assertTrue(
            self._refs.remove_if_equals(
                b"refs/heads/master",
                b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            )
        )
        self.assertRaises(KeyError, lambda: self._refs[b"refs/heads/master"])

        # HEAD is now a broken symref
        self.assertRaises(KeyError, lambda: self._refs[b"HEAD"])
        self.assertEqual(b"ref: refs/heads/master", self._refs.read_loose_ref(b"HEAD"))

        self.assertFalse(
            os.path.exists(
                os.path.join(self._refs.path, b"refs", b"heads", b"master.lock")
            )
        )
        self.assertFalse(os.path.exists(os.path.join(self._refs.path, b"HEAD.lock")))

    def test_remove_packed_without_peeled(self) -> None:
        refs_file = os.path.join(self._repo.path, "packed-refs")
        f = GitFile(refs_file)
        refs_data = f.read()
        f.close()
        f = GitFile(refs_file, "wb")
        f.write(
            b"\n".join(
                line
                for line in refs_data.split(b"\n")
                if not line or line[0] not in b"#^"
            )
        )
        f.close()
        self._repo = Repo(self._repo.path)
        refs = self._repo.refs
        self.assertTrue(
            refs.remove_if_equals(
                b"refs/heads/packed",
                b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            )
        )

    def test_remove_if_equals_packed(self) -> None:
        # test removing ref that is only packed
        self.assertEqual(
            b"df6800012397fb85c56e7418dd4eb9405dee075c",
            self._refs[b"refs/tags/refs-0.1"],
        )
        self.assertTrue(
            self._refs.remove_if_equals(
                b"refs/tags/refs-0.1",
                b"df6800012397fb85c56e7418dd4eb9405dee075c",
            )
        )
        self.assertRaises(KeyError, lambda: self._refs[b"refs/tags/refs-0.1"])

    def test_remove_parent(self) -> None:
        self._refs[b"refs/heads/foo/bar"] = b"df6800012397fb85c56e7418dd4eb9405dee075c"
        del self._refs[b"refs/heads/foo/bar"]
        ref_file = os.path.join(
            self._refs.path,
            b"refs",
            b"heads",
            b"foo",
            b"bar",
        )
        self.assertFalse(os.path.exists(ref_file))
        ref_file = os.path.join(self._refs.path, b"refs", b"heads", b"foo")
        self.assertFalse(os.path.exists(ref_file))
        ref_file = os.path.join(self._refs.path, b"refs", b"heads")
        self.assertTrue(os.path.exists(ref_file))
        self._refs[b"refs/heads/foo"] = b"df6800012397fb85c56e7418dd4eb9405dee075c"

    def test_read_ref(self) -> None:
        self.assertEqual(b"ref: refs/heads/master", self._refs.read_ref(b"HEAD"))
        self.assertEqual(
            b"42d06bd4b77fed026b154d16493e5deab78f02ec",
            self._refs.read_ref(b"refs/heads/packed"),
        )
        self.assertEqual(None, self._refs.read_ref(b"nonexistent"))

    def test_read_loose_ref(self) -> None:
        self._refs[b"refs/heads/foo"] = b"df6800012397fb85c56e7418dd4eb9405dee075c"

        self.assertEqual(None, self._refs.read_ref(b"refs/heads/foo/bar"))

    def test_non_ascii(self) -> None:
        try:
            encoded_ref = os.fsencode("refs/tags/schön")
        except UnicodeEncodeError as exc:
            raise SkipTest(
                "filesystem encoding doesn't support special character"
            ) from exc
        p = os.path.join(os.fsencode(self._repo.path), encoded_ref)
        with open(p, "w") as f:
            f.write("00" * 20)

        expected_refs = dict(_TEST_REFS)
        expected_refs[encoded_ref] = b"00" * 20
        del expected_refs[b"refs/heads/loop"]

        self.assertEqual(expected_refs, self._repo.get_refs())

    def test_cyrillic(self) -> None:
        if sys.platform in ("darwin", "win32"):
            raise SkipTest("filesystem encoding doesn't support arbitrary bytes")
        # reported in https://github.com/dulwich/dulwich/issues/608
        name = b"\xcd\xee\xe2\xe0\xff\xe2\xe5\xf2\xea\xe01"
        encoded_ref = b"refs/heads/" + name
        with open(os.path.join(os.fsencode(self._repo.path), encoded_ref), "w") as f:
            f.write("00" * 20)

        expected_refs = set(_TEST_REFS.keys())
        expected_refs.add(encoded_ref)

        self.assertEqual(expected_refs, set(self._repo.refs.allkeys()))
        self.assertEqual(
            {r[len(b"refs/") :] for r in expected_refs if r.startswith(b"refs/")},
            set(self._repo.refs.subkeys(b"refs/")),
        )
        expected_refs.remove(b"refs/heads/loop")
        expected_refs.add(b"HEAD")
        self.assertEqual(expected_refs, set(self._repo.get_refs().keys()))

    def test_write_unchanged_ref_optimization(self):
        # Test that writing unchanged ref avoids fsync but still checks locks
        ref_name = b"refs/heads/unchanged"
        ref_value = b"a" * 40

        # Set initial ref value
        self._refs[ref_name] = ref_value

        # Test 1: Writing same value should succeed without changes
        result = self._refs.set_if_equals(ref_name, ref_value, ref_value)
        self.assertTrue(result)

        # Test 2: Writing same value with wrong old_ref should fail
        wrong_old = b"b" * 40
        result = self._refs.set_if_equals(ref_name, wrong_old, ref_value)
        self.assertFalse(result)

        # Test 3: Writing different value should update normally
        new_value = b"c" * 40
        result = self._refs.set_if_equals(ref_name, ref_value, new_value)
        self.assertTrue(result)
        self.assertEqual(new_value, self._refs[ref_name])

    def test_write_unchanged_ref_with_lock(self):
        # Test that file locking is still detected when ref unchanged

        from dulwich.file import FileLocked

        ref_name = b"refs/heads/locktest"
        ref_value = b"d" * 40

        # Set initial ref value
        self._refs[ref_name] = ref_value

        # Get the actual file path
        ref_file = os.path.join(os.fsencode(self._refs.path), ref_name)
        lock_file = ref_file + b".lock"

        # Create lock file to simulate another process holding lock
        with open(lock_file, "wb") as f:
            f.write(b"locked by another process")

            # Try to write same value - should raise FileLocked
            with self.assertRaises(FileLocked):
                self._refs[ref_name] = ref_value

        # Clean up lock file
        if os.path.exists(lock_file):
            os.unlink(lock_file)

        # Now it should work
        self._refs[ref_name] = ref_value


class IsPerWorktreeRefsTests(TestCase):
    def test(self) -> None:
        cases = [
            (b"HEAD", True),
            (b"refs/bisect/good", True),
            (b"refs/worktree/foo", True),
            (b"refs/rewritten/onto", True),
            (b"refs/stash", False),
            (b"refs/heads/main", False),
            (b"refs/tags/v1.0", False),
            (b"refs/remotes/origin/main", False),
            (b"refs/custom/foo", False),
            (b"refs/replace/aaaaaa", False),
        ]
        for ref, expected in cases:
            with self.subTest(ref=ref, expected=expected):
                self.assertEqual(is_per_worktree_ref(ref), expected)


class DiskRefsContainerWorktreeRefsTest(TestCase):
    def setUp(self) -> None:
        # Create temporary directories
        temp_dir = tempfile.mkdtemp()
        test_dir = os.path.join(temp_dir, "main")
        os.makedirs(test_dir)

        repo = Repo.init(test_dir, default_branch=b"main")
        main_worktree = repo.get_worktree()
        with open(os.path.join(test_dir, "test.txt"), "wb") as f:
            f.write(b"test content")
        main_worktree.stage(["test.txt"])
        self.first_commit = main_worktree.commit(message=b"Initial commit")

        worktree_dir = os.path.join(temp_dir, "worktree")
        wt_repo = add_worktree(repo, worktree_dir, branch="wt-main")
        linked_worktree = wt_repo.get_worktree()
        with open(os.path.join(test_dir, "test2.txt"), "wb") as f:
            f.write(b"test content")
        linked_worktree.stage(["test2.txt"])
        self.second_commit = linked_worktree.commit(message=b"second commit")

        self.refs = repo.refs
        self.wt_refs = wt_repo.refs

    def test_refpath(self) -> None:
        main_path = self.refs.path
        common = self.wt_refs.path
        wt_path = self.wt_refs.worktree_path

        cases = [
            (self.refs, b"HEAD", main_path),
            (self.refs, b"refs/heads/main", main_path),
            (self.refs, b"refs/heads/wt-main", main_path),
            (self.refs, b"refs/worktree/foo", main_path),
            (self.refs, b"refs/bisect/good", main_path),
            (self.wt_refs, b"HEAD", wt_path),
            (self.wt_refs, b"refs/heads/main", common),
            (self.wt_refs, b"refs/heads/wt-main", common),
            (self.wt_refs, b"refs/worktree/foo", wt_path),
            (self.wt_refs, b"refs/bisect/good", wt_path),
        ]

        for refs, refname, git_dir in cases:
            with self.subTest(refs=refs, refname=refname, git_dir=git_dir):
                refpath = refs.refpath(refname)
                expected_path = os.path.join(
                    git_dir, refname.replace(b"/", os.fsencode(os.sep))
                )
                self.assertEqual(refpath, expected_path)

    def test_shared_ref(self) -> None:
        self.assertEqual(self.refs[b"refs/heads/main"], self.first_commit)
        self.assertEqual(self.refs[b"refs/heads/wt-main"], self.second_commit)
        self.assertEqual(self.wt_refs[b"refs/heads/main"], self.first_commit)
        self.assertEqual(self.wt_refs[b"refs/heads/wt-main"], self.second_commit)

        expected = {b"HEAD", b"refs/heads/main", b"refs/heads/wt-main"}
        self.assertEqual(expected, self.refs.keys())
        self.assertEqual(expected, self.wt_refs.keys())

        self.assertEqual({b"main", b"wt-main"}, set(self.refs.keys(b"refs/heads/")))
        self.assertEqual({b"main", b"wt-main"}, set(self.wt_refs.keys(b"refs/heads/")))

        ref_path = os.path.join(self.refs.path, b"refs", b"heads", b"main")
        self.assertTrue(os.path.exists(ref_path))

        ref_path = os.path.join(self.wt_refs.worktree_path, b"refs", b"heads", b"main")
        self.assertFalse(os.path.exists(ref_path))

    def test_per_worktree_ref(self) -> None:
        path = self.refs.path
        wt_path = self.wt_refs.worktree_path

        self.assertEqual(self.refs[b"HEAD"], self.first_commit)
        self.assertEqual(self.wt_refs[b"HEAD"], self.second_commit)

        self.refs[b"refs/bisect/good"] = self.first_commit
        self.wt_refs[b"refs/bisect/good"] = self.second_commit

        self.refs[b"refs/bisect/start"] = self.first_commit
        self.wt_refs[b"refs/bisect/bad"] = self.second_commit

        self.assertEqual(self.refs[b"refs/bisect/good"], self.first_commit)
        self.assertEqual(self.wt_refs[b"refs/bisect/good"], self.second_commit)

        self.assertTrue(os.path.exists(os.path.join(path, b"refs", b"bisect", b"good")))
        self.assertTrue(
            os.path.exists(os.path.join(wt_path, b"refs", b"bisect", b"good"))
        )

        self.assertEqual(self.refs[b"refs/bisect/start"], self.first_commit)
        with self.assertRaises(KeyError):
            self.wt_refs[b"refs/bisect/start"]
        self.assertTrue(
            os.path.exists(os.path.join(path, b"refs", b"bisect", b"start"))
        )
        self.assertFalse(
            os.path.exists(os.path.join(wt_path, b"refs", b"bisect", b"start"))
        )

        with self.assertRaises(KeyError):
            self.refs[b"refs/bisect/bad"]
        self.assertEqual(self.wt_refs[b"refs/bisect/bad"], self.second_commit)
        self.assertFalse(os.path.exists(os.path.join(path, b"refs", b"bisect", b"bad")))
        self.assertTrue(
            os.path.exists(os.path.join(wt_path, b"refs", b"bisect", b"bad"))
        )

        expected_refs = {
            b"HEAD",
            b"refs/heads/main",
            b"refs/heads/wt-main",
            b"refs/bisect/good",
            b"refs/bisect/start",
        }
        self.assertEqual(self.refs.keys(), expected_refs)
        self.assertEqual({b"good", b"start"}, self.refs.keys(b"refs/bisect/"))

        expected_wt_refs = {
            b"HEAD",
            b"refs/heads/main",
            b"refs/heads/wt-main",
            b"refs/bisect/good",
            b"refs/bisect/bad",
        }
        self.assertEqual(self.wt_refs.keys(), expected_wt_refs)
        self.assertEqual({b"good", b"bad"}, self.wt_refs.keys(b"refs/bisect/"))

    def test_delete_per_worktree_ref(self) -> None:
        self.refs[b"refs/worktree/foo"] = self.first_commit
        self.wt_refs[b"refs/worktree/foo"] = self.second_commit

        del self.wt_refs[b"refs/worktree/foo"]
        with self.assertRaises(KeyError):
            self.wt_refs[b"refs/worktree/foo"]

        del self.refs[b"refs/worktree/foo"]
        with self.assertRaises(KeyError):
            self.refs[b"refs/worktree/foo"]

    def test_delete_shared_ref(self) -> None:
        self.refs[b"refs/heads/branch"] = self.first_commit

        del self.wt_refs[b"refs/heads/branch"]

        with self.assertRaises(KeyError):
            self.wt_refs[b"refs/heads/branch"]
        with self.assertRaises(KeyError):
            self.refs[b"refs/heads/branch"]

    def test_contains_shared_ref(self):
        self.assertIn(b"refs/heads/main", self.refs)
        self.assertIn(b"refs/heads/main", self.wt_refs)
        self.assertIn(b"refs/heads/wt-main", self.refs)
        self.assertIn(b"refs/heads/wt-main", self.wt_refs)

    def test_contains_per_worktree_ref(self):
        self.refs[b"refs/worktree/foo"] = self.first_commit
        self.wt_refs[b"refs/worktree/bar"] = self.second_commit

        self.assertIn(b"refs/worktree/foo", self.refs)
        self.assertNotIn(b"refs/worktree/bar", self.refs)
        self.assertNotIn(b"refs/worktree/foo", self.wt_refs)
        self.assertIn(b"refs/worktree/bar", self.wt_refs)


_TEST_REFS_SERIALIZED = (
    b"42d06bd4b77fed026b154d16493e5deab78f02ec\t"
    b"refs/heads/40-char-ref-aaaaaaaaaaaaaaaaaa\n"
    b"42d06bd4b77fed026b154d16493e5deab78f02ec\trefs/heads/master\n"
    b"42d06bd4b77fed026b154d16493e5deab78f02ec\trefs/heads/packed\n"
    b"df6800012397fb85c56e7418dd4eb9405dee075c\trefs/tags/refs-0.1\n"
    b"3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8\trefs/tags/refs-0.2\n"
)


class DiskRefsContainerPathlibTests(TestCase):
    def test_pathlib_init(self) -> None:
        from pathlib import Path

        from dulwich.refs import DiskRefsContainer

        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(os.rmdir, temp_dir)

        # Test with pathlib.Path
        path_obj = Path(temp_dir)
        refs = DiskRefsContainer(path_obj)
        self.assertEqual(refs.path, temp_dir.encode())

        # Test refpath with pathlib initialized container
        ref_path = refs.refpath(b"HEAD")
        self.assertTrue(isinstance(ref_path, bytes))
        self.assertEqual(ref_path, os.path.join(temp_dir.encode(), b"HEAD"))

    def test_pathlib_worktree_path(self) -> None:
        from pathlib import Path

        from dulwich.refs import DiskRefsContainer

        # Create temporary directories
        temp_dir = tempfile.mkdtemp()
        worktree_dir = tempfile.mkdtemp()
        self.addCleanup(os.rmdir, temp_dir)
        self.addCleanup(os.rmdir, worktree_dir)

        # Test with pathlib.Path for both paths
        path_obj = Path(temp_dir)
        worktree_obj = Path(worktree_dir)
        refs = DiskRefsContainer(path_obj, worktree_path=worktree_obj)
        self.assertEqual(refs.path, temp_dir.encode())
        self.assertEqual(refs.worktree_path, worktree_dir.encode())

        # Test refpath returns worktree path for HEAD
        ref_path = refs.refpath(b"HEAD")
        self.assertEqual(ref_path, os.path.join(worktree_dir.encode(), b"HEAD"))


class ParseSymrefValueTests(TestCase):
    def test_valid(self) -> None:
        self.assertEqual(b"refs/heads/foo", parse_symref_value(b"ref: refs/heads/foo"))

    def test_invalid(self) -> None:
        self.assertRaises(ValueError, parse_symref_value, b"foobar")


class ParseRemoteRefTests(TestCase):
    def test_valid(self) -> None:
        # Test simple case
        remote, branch = parse_remote_ref(b"refs/remotes/origin/main")
        self.assertEqual(b"origin", remote)
        self.assertEqual(b"main", branch)

        # Test with branch containing slashes
        remote, branch = parse_remote_ref(b"refs/remotes/upstream/feature/new-ui")
        self.assertEqual(b"upstream", remote)
        self.assertEqual(b"feature/new-ui", branch)

    def test_invalid_not_remote_ref(self) -> None:
        # Not a remote ref
        with self.assertRaises(ValueError) as cm:
            parse_remote_ref(b"refs/heads/main")
        self.assertIn("Not a remote ref", str(cm.exception))

    def test_invalid_format(self) -> None:
        # Missing branch name
        with self.assertRaises(ValueError) as cm:
            parse_remote_ref(b"refs/remotes/origin")
        self.assertIn("Invalid remote ref format", str(cm.exception))

        # Just the prefix
        with self.assertRaises(ValueError) as cm:
            parse_remote_ref(b"refs/remotes/")
        self.assertIn("Invalid remote ref format", str(cm.exception))


class StripPeeledRefsTests(TestCase):
    all_refs: ClassVar[dict[bytes, bytes]] = {
        b"refs/heads/master": b"8843d7f92416211de9ebb963ff4ce28125932878",
        b"refs/heads/testing": b"186a005b134d8639a58b6731c7c1ea821a6eedba",
        b"refs/tags/1.0.0": b"a93db4b0360cc635a2b93675010bac8d101f73f0",
        b"refs/tags/1.0.0^{}": b"a93db4b0360cc635a2b93675010bac8d101f73f0",
        b"refs/tags/2.0.0": b"0749936d0956c661ac8f8d3483774509c165f89e",
        b"refs/tags/2.0.0^{}": b"0749936d0956c661ac8f8d3483774509c165f89e",
    }
    non_peeled_refs: ClassVar[dict[bytes, bytes]] = {
        b"refs/heads/master": b"8843d7f92416211de9ebb963ff4ce28125932878",
        b"refs/heads/testing": b"186a005b134d8639a58b6731c7c1ea821a6eedba",
        b"refs/tags/1.0.0": b"a93db4b0360cc635a2b93675010bac8d101f73f0",
        b"refs/tags/2.0.0": b"0749936d0956c661ac8f8d3483774509c165f89e",
    }

    def test_strip_peeled_refs(self) -> None:
        # Simple check of two dicts
        self.assertEqual(strip_peeled_refs(self.all_refs), self.non_peeled_refs)

    def test_split_peeled_refs(self) -> None:
        (regular, peeled) = split_peeled_refs(self.all_refs)
        self.assertEqual(regular, self.non_peeled_refs)
        self.assertEqual(
            peeled,
            {
                b"refs/tags/2.0.0": b"0749936d0956c661ac8f8d3483774509c165f89e",
                b"refs/tags/1.0.0": b"a93db4b0360cc635a2b93675010bac8d101f73f0",
            },
        )


class ShortenRefNameTests(TestCase):
    """Tests for shorten_ref_name function."""

    def test_branch_ref(self) -> None:
        """Test shortening branch references."""
        self.assertEqual(b"master", shorten_ref_name(b"refs/heads/master"))
        self.assertEqual(b"develop", shorten_ref_name(b"refs/heads/develop"))
        self.assertEqual(
            b"feature/new-ui", shorten_ref_name(b"refs/heads/feature/new-ui")
        )

    def test_remote_ref(self) -> None:
        """Test shortening remote references."""
        self.assertEqual(b"origin/main", shorten_ref_name(b"refs/remotes/origin/main"))
        self.assertEqual(
            b"upstream/master", shorten_ref_name(b"refs/remotes/upstream/master")
        )
        self.assertEqual(
            b"origin/feature/test",
            shorten_ref_name(b"refs/remotes/origin/feature/test"),
        )

    def test_tag_ref(self) -> None:
        """Test shortening tag references."""
        self.assertEqual(b"v1.0", shorten_ref_name(b"refs/tags/v1.0"))
        self.assertEqual(b"release-2.0", shorten_ref_name(b"refs/tags/release-2.0"))

    def test_special_refs(self) -> None:
        """Test that special refs are not shortened."""
        self.assertEqual(b"HEAD", shorten_ref_name(b"HEAD"))
        self.assertEqual(b"FETCH_HEAD", shorten_ref_name(b"FETCH_HEAD"))
        self.assertEqual(b"ORIG_HEAD", shorten_ref_name(b"ORIG_HEAD"))

    def test_other_refs(self) -> None:
        """Test refs that don't match standard prefixes."""
        # Refs that don't match any standard prefix are returned as-is
        self.assertEqual(b"refs/stash", shorten_ref_name(b"refs/stash"))
        self.assertEqual(b"refs/bisect/good", shorten_ref_name(b"refs/bisect/good"))


class RefUtilityFunctionsTests(TestCase):
    """Tests for the new ref utility functions."""

    def test_local_branch_name(self) -> None:
        """Test local_branch_name function."""
        from dulwich.refs import local_branch_name

        # Test adding prefix to branch name
        self.assertEqual(b"refs/heads/master", local_branch_name(b"master"))
        self.assertEqual(b"refs/heads/develop", local_branch_name(b"develop"))
        self.assertEqual(
            b"refs/heads/feature/new-ui", local_branch_name(b"feature/new-ui")
        )

        # Test idempotency - already has prefix
        self.assertEqual(b"refs/heads/master", local_branch_name(b"refs/heads/master"))

    def test_local_tag_name(self) -> None:
        """Test local_tag_name function."""
        from dulwich.refs import local_tag_name

        # Test adding prefix to tag name
        self.assertEqual(b"refs/tags/v1.0", local_tag_name(b"v1.0"))
        self.assertEqual(b"refs/tags/release-2.0", local_tag_name(b"release-2.0"))

        # Test idempotency - already has prefix
        self.assertEqual(b"refs/tags/v1.0", local_tag_name(b"refs/tags/v1.0"))

    def test_extract_branch_name(self) -> None:
        """Test extract_branch_name function."""
        from dulwich.refs import extract_branch_name

        # Test extracting branch name from full ref
        self.assertEqual(b"master", extract_branch_name(b"refs/heads/master"))
        self.assertEqual(b"develop", extract_branch_name(b"refs/heads/develop"))
        self.assertEqual(
            b"feature/new-ui", extract_branch_name(b"refs/heads/feature/new-ui")
        )

        # Test error on invalid ref
        with self.assertRaises(ValueError) as cm:
            extract_branch_name(b"refs/tags/v1.0")
        self.assertIn("Not a local branch ref", str(cm.exception))

        with self.assertRaises(ValueError):
            extract_branch_name(b"master")

    def test_extract_tag_name(self) -> None:
        """Test extract_tag_name function."""
        from dulwich.refs import extract_tag_name

        # Test extracting tag name from full ref
        self.assertEqual(b"v1.0", extract_tag_name(b"refs/tags/v1.0"))
        self.assertEqual(b"release-2.0", extract_tag_name(b"refs/tags/release-2.0"))

        # Test error on invalid ref
        with self.assertRaises(ValueError) as cm:
            extract_tag_name(b"refs/heads/master")
        self.assertIn("Not a local tag ref", str(cm.exception))

        with self.assertRaises(ValueError):
            extract_tag_name(b"v1.0")


class NamespacedRefsContainerTests(TestCase):
    """Tests for NamespacedRefsContainer."""

    def setUp(self) -> None:
        TestCase.setUp(self)
        # Create an underlying refs container
        self._underlying_refs = DictRefsContainer(dict(_TEST_REFS))
        # Create a namespaced view
        self._refs = NamespacedRefsContainer(self._underlying_refs, b"foo")

    def test_namespace_prefix_simple(self) -> None:
        """Test simple namespace prefix."""
        refs = NamespacedRefsContainer(self._underlying_refs, b"foo")
        self.assertEqual(b"refs/namespaces/foo/", refs._namespace_prefix)

    def test_namespace_prefix_nested(self) -> None:
        """Test nested namespace prefix."""
        refs = NamespacedRefsContainer(self._underlying_refs, b"foo/bar")
        self.assertEqual(
            b"refs/namespaces/foo/refs/namespaces/bar/", refs._namespace_prefix
        )

    def test_allkeys_empty_namespace(self) -> None:
        """Test that newly created namespace has no refs except HEAD."""
        # HEAD is shared across namespaces, so it appears even in empty namespace
        self.assertEqual({b"HEAD"}, self._refs.allkeys())

    def test_setitem_and_getitem(self) -> None:
        """Test setting and getting refs in namespace."""
        sha = b"9" * 40
        self._refs[b"refs/heads/master"] = sha
        self.assertEqual(sha, self._refs[b"refs/heads/master"])

        # Verify it's stored with the namespace prefix in underlying container
        self.assertIn(
            b"refs/namespaces/foo/refs/heads/master", self._underlying_refs.allkeys()
        )
        self.assertEqual(
            sha, self._underlying_refs[b"refs/namespaces/foo/refs/heads/master"]
        )

    def test_head_not_namespaced(self) -> None:
        """Test that HEAD is not namespaced."""
        sha = b"a" * 40
        self._refs[b"HEAD"] = sha
        self.assertEqual(sha, self._refs[b"HEAD"])

        # HEAD should be directly in the underlying container, not namespaced
        self.assertIn(b"HEAD", self._underlying_refs.allkeys())
        self.assertNotIn(b"refs/namespaces/foo/HEAD", self._underlying_refs.allkeys())

    def test_isolation_between_namespaces(self) -> None:
        """Test that different namespaces are isolated."""
        sha1 = b"a" * 40
        sha2 = b"b" * 40

        # Create two different namespaces
        refs_foo = NamespacedRefsContainer(self._underlying_refs, b"foo")
        refs_bar = NamespacedRefsContainer(self._underlying_refs, b"bar")

        # Set ref in foo namespace
        refs_foo[b"refs/heads/master"] = sha1

        # Set ref in bar namespace
        refs_bar[b"refs/heads/master"] = sha2

        # Each namespace should only see its own refs (plus shared HEAD)
        self.assertEqual(sha1, refs_foo[b"refs/heads/master"])
        self.assertEqual(sha2, refs_bar[b"refs/heads/master"])
        self.assertEqual({b"HEAD", b"refs/heads/master"}, refs_foo.allkeys())
        self.assertEqual({b"HEAD", b"refs/heads/master"}, refs_bar.allkeys())

    def test_allkeys_filters_namespace(self) -> None:
        """Test that allkeys only returns refs in the namespace."""
        # Add refs in multiple namespaces
        self._underlying_refs[b"refs/namespaces/foo/refs/heads/master"] = b"a" * 40
        self._underlying_refs[b"refs/namespaces/foo/refs/heads/develop"] = b"b" * 40
        self._underlying_refs[b"refs/namespaces/bar/refs/heads/feature"] = b"c" * 40
        self._underlying_refs[b"refs/heads/global"] = b"d" * 40

        # Only refs in 'foo' namespace should be visible (plus HEAD which is shared)
        foo_refs = NamespacedRefsContainer(self._underlying_refs, b"foo")
        self.assertEqual(
            {b"HEAD", b"refs/heads/master", b"refs/heads/develop"}, foo_refs.allkeys()
        )

    def test_set_symbolic_ref(self) -> None:
        """Test symbolic ref creation in namespace."""
        sha = b"e" * 40
        self._refs[b"refs/heads/develop"] = sha
        self._refs.set_symbolic_ref(b"refs/heads/main", b"refs/heads/develop")

        # Both target and link should be namespaced
        self.assertIn(
            b"refs/namespaces/foo/refs/heads/main", self._underlying_refs.allkeys()
        )
        self.assertEqual(
            b"ref: refs/namespaces/foo/refs/heads/develop",
            self._underlying_refs.read_loose_ref(
                b"refs/namespaces/foo/refs/heads/main"
            ),
        )

    def test_remove_if_equals(self) -> None:
        """Test removing refs from namespace."""
        sha = b"f" * 40
        self._refs[b"refs/heads/temp"] = sha

        # Remove the ref
        self.assertTrue(self._refs.remove_if_equals(b"refs/heads/temp", sha))
        self.assertNotIn(b"refs/heads/temp", self._refs.allkeys())
        self.assertNotIn(
            b"refs/namespaces/foo/refs/heads/temp", self._underlying_refs.allkeys()
        )

    def test_get_packed_refs(self) -> None:
        """Test get_packed_refs returns empty dict for DictRefsContainer."""
        # DictRefsContainer doesn't support packed refs, so just verify
        # the wrapper returns an empty dict
        packed = self._refs.get_packed_refs()
        self.assertEqual({}, packed)

    def test_add_if_new(self) -> None:
        """Test add_if_new in namespace."""
        sha = b"1" * 40
        # Should succeed - ref doesn't exist
        self.assertTrue(self._refs.add_if_new(b"refs/heads/new", sha))
        self.assertEqual(sha, self._refs[b"refs/heads/new"])

        # Should fail - ref already exists
        self.assertFalse(self._refs.add_if_new(b"refs/heads/new", b"2" * 40))
        self.assertEqual(sha, self._refs[b"refs/heads/new"])

    def test_set_if_equals(self) -> None:
        """Test set_if_equals in namespace."""
        sha1 = b"a" * 40
        sha2 = b"b" * 40
        self._refs[b"refs/heads/test"] = sha1

        # Should fail with wrong old value
        self.assertFalse(self._refs.set_if_equals(b"refs/heads/test", b"c" * 40, sha2))
        self.assertEqual(sha1, self._refs[b"refs/heads/test"])

        # Should succeed with correct old value
        self.assertTrue(self._refs.set_if_equals(b"refs/heads/test", sha1, sha2))
        self.assertEqual(sha2, self._refs[b"refs/heads/test"])
