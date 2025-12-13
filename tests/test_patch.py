# test_patch.py -- tests for patch.py
# Copyright (C) 2010 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for patch.py."""

from io import BytesIO, StringIO
from typing import NoReturn

from dulwich.object_store import MemoryObjectStore
from dulwich.objects import S_IFGITLINK, ZERO_SHA, Blob, Commit, Tree
from dulwich.patch import (
    DiffAlgorithmNotAvailable,
    commit_patch_id,
    get_summary,
    git_am_patch_split,
    patch_id,
    unified_diff_with_algorithm,
    write_blob_diff,
    write_commit_patch,
    write_object_diff,
    write_tree_diff,
)
from dulwich.tests.utils import make_commit

from . import DependencyMissing, SkipTest, TestCase


class WriteCommitPatchTests(TestCase):
    def test_simple_bytesio(self) -> None:
        f = BytesIO()
        c = make_commit(
            author=b"Jelmer <jelmer@samba.org>",
            committer=b"Jelmer <jelmer@samba.org>",
            author_time=1271350201,
            commit_time=1271350201,
            author_timezone=0,
            commit_timezone=0,
            message=b"This is the first line\nAnd this is the second line.\n",
            tree=Tree().id,
        )
        write_commit_patch(f, c, b"CONTENTS", (1, 1), version="custom")
        f.seek(0)
        lines = f.readlines()
        self.assertTrue(
            lines[0].startswith(b"From 0b0d34d1b5b596c928adc9a727a4b9e03d025298")
        )
        self.assertEqual(lines[1], b"From: Jelmer <jelmer@samba.org>\n")
        self.assertTrue(lines[2].startswith(b"Date: "))
        self.assertEqual(
            [
                b"Subject: [PATCH 1/1] This is the first line\n",
                b"And this is the second line.\n",
                b"\n",
                b"\n",
                b"---\n",
            ],
            lines[3:8],
        )
        self.assertEqual([b"CONTENTS-- \n", b"custom\n"], lines[-2:])
        if len(lines) >= 12:
            # diffstat may not be present
            self.assertEqual(lines[8], b" 0 files changed\n")


class ReadGitAmPatch(TestCase):
    def test_extract_string(self) -> None:
        text = b"""\
From ff643aae102d8870cac88e8f007e70f58f3a7363 Mon Sep 17 00:00:00 2001
From: Jelmer Vernooij <jelmer@samba.org>
Date: Thu, 15 Apr 2010 15:40:28 +0200
Subject: [PATCH 1/2] Remove executable bit from prey.ico (triggers a warning).

---
 pixmaps/prey.ico |  Bin 9662 -> 9662 bytes
 1 files changed, 0 insertions(+), 0 deletions(-)
 mode change 100755 => 100644 pixmaps/prey.ico

-- 
1.7.0.4
"""
        c, diff, version = git_am_patch_split(StringIO(text.decode("utf-8")), "utf-8")
        self.assertEqual(b"Jelmer Vernooij <jelmer@samba.org>", c.committer)
        self.assertEqual(b"Jelmer Vernooij <jelmer@samba.org>", c.author)
        self.assertEqual(
            b"Remove executable bit from prey.ico (triggers a warning).\n",
            c.message,
        )
        self.assertEqual(
            b""" pixmaps/prey.ico |  Bin 9662 -> 9662 bytes
 1 files changed, 0 insertions(+), 0 deletions(-)
 mode change 100755 => 100644 pixmaps/prey.ico

""",
            diff,
        )
        self.assertEqual(b"1.7.0.4", version)

    def test_extract_bytes(self) -> None:
        text = b"""\
From ff643aae102d8870cac88e8f007e70f58f3a7363 Mon Sep 17 00:00:00 2001
From: Jelmer Vernooij <jelmer@samba.org>
Date: Thu, 15 Apr 2010 15:40:28 +0200
Subject: [PATCH 1/2] Remove executable bit from prey.ico (triggers a warning).

---
 pixmaps/prey.ico |  Bin 9662 -> 9662 bytes
 1 files changed, 0 insertions(+), 0 deletions(-)
 mode change 100755 => 100644 pixmaps/prey.ico

-- 
1.7.0.4
"""
        c, diff, version = git_am_patch_split(BytesIO(text))
        self.assertEqual(b"Jelmer Vernooij <jelmer@samba.org>", c.committer)
        self.assertEqual(b"Jelmer Vernooij <jelmer@samba.org>", c.author)
        self.assertEqual(
            b"Remove executable bit from prey.ico (triggers a warning).\n",
            c.message,
        )
        self.assertEqual(
            b""" pixmaps/prey.ico |  Bin 9662 -> 9662 bytes
 1 files changed, 0 insertions(+), 0 deletions(-)
 mode change 100755 => 100644 pixmaps/prey.ico

""",
            diff,
        )
        self.assertEqual(b"1.7.0.4", version)

    def test_extract_spaces(self) -> None:
        text = b"""From ff643aae102d8870cac88e8f007e70f58f3a7363 Mon Sep 17 00:00:00 2001
From: Jelmer Vernooij <jelmer@samba.org>
Date: Thu, 15 Apr 2010 15:40:28 +0200
Subject:  [Dulwich-users] [PATCH] Added unit tests for
 dulwich.object_store.tree_lookup_path.

* dulwich/tests/test_object_store.py
  (TreeLookupPathTests): This test case contains a few tests that ensure the
   tree_lookup_path function works as expected.
---
 pixmaps/prey.ico |  Bin 9662 -> 9662 bytes
 1 files changed, 0 insertions(+), 0 deletions(-)
 mode change 100755 => 100644 pixmaps/prey.ico

-- 
1.7.0.4
"""
        c, _diff, _version = git_am_patch_split(BytesIO(text), "utf-8")
        self.assertEqual(
            b"""\
Added unit tests for dulwich.object_store.tree_lookup_path.

* dulwich/tests/test_object_store.py
  (TreeLookupPathTests): This test case contains a few tests that ensure the
   tree_lookup_path function works as expected.
""",
            c.message,
        )

    def test_extract_pseudo_from_header(self) -> None:
        text = b"""From ff643aae102d8870cac88e8f007e70f58f3a7363 Mon Sep 17 00:00:00 2001
From: Jelmer Vernooij <jelmer@samba.org>
Date: Thu, 15 Apr 2010 15:40:28 +0200
Subject:  [Dulwich-users] [PATCH] Added unit tests for
 dulwich.object_store.tree_lookup_path.

From: Jelmer Vernooij <jelmer@debian.org>

* dulwich/tests/test_object_store.py
  (TreeLookupPathTests): This test case contains a few tests that ensure the
   tree_lookup_path function works as expected.
---
 pixmaps/prey.ico |  Bin 9662 -> 9662 bytes
 1 files changed, 0 insertions(+), 0 deletions(-)
 mode change 100755 => 100644 pixmaps/prey.ico

-- 
1.7.0.4
"""
        c, _diff, _version = git_am_patch_split(BytesIO(text), "utf-8")
        self.assertEqual(b"Jelmer Vernooij <jelmer@debian.org>", c.author)
        self.assertEqual(
            b"""\
Added unit tests for dulwich.object_store.tree_lookup_path.

* dulwich/tests/test_object_store.py
  (TreeLookupPathTests): This test case contains a few tests that ensure the
   tree_lookup_path function works as expected.
""",
            c.message,
        )

    def test_extract_no_version_tail(self) -> None:
        text = b"""\
From ff643aae102d8870cac88e8f007e70f58f3a7363 Mon Sep 17 00:00:00 2001
From: Jelmer Vernooij <jelmer@samba.org>
Date: Thu, 15 Apr 2010 15:40:28 +0200
Subject:  [Dulwich-users] [PATCH] Added unit tests for
 dulwich.object_store.tree_lookup_path.

From: Jelmer Vernooij <jelmer@debian.org>

---
 pixmaps/prey.ico |  Bin 9662 -> 9662 bytes
 1 files changed, 0 insertions(+), 0 deletions(-)
 mode change 100755 => 100644 pixmaps/prey.ico

"""
        _c, _diff, version = git_am_patch_split(BytesIO(text), "utf-8")
        self.assertEqual(None, version)

    def test_extract_mercurial(self) -> NoReturn:
        raise SkipTest(
            "git_am_patch_split doesn't handle Mercurial patches properly yet"
        )
        expected_diff = """\
diff --git a/dulwich/tests/test_patch.py b/dulwich/tests/test_patch.py
--- a/dulwich/tests/test_patch.py
+++ b/dulwich/tests/test_patch.py
@@ -158,7 +158,7 @@
 
 '''
         c, diff, version = git_am_patch_split(BytesIO(text))
-        self.assertIs(None, version)
+        self.assertEqual(None, version)
 
 
 class DiffTests(TestCase):
"""
        text = f"""\
From dulwich-users-bounces+jelmer=samba.org@lists.launchpad.net \
Mon Nov 29 00:58:18 2010
Date: Sun, 28 Nov 2010 17:57:27 -0600
From: Augie Fackler <durin42@gmail.com>
To: dulwich-users <dulwich-users@lists.launchpad.net>
Subject: [Dulwich-users] [PATCH] test_patch: fix tests on Python 2.6
Content-Transfer-Encoding: 8bit

Change-Id: I5e51313d4ae3a65c3f00c665002a7489121bb0d6

{expected_diff}

_______________________________________________
Mailing list: https://launchpad.net/~dulwich-users
Post to     : dulwich-users@lists.launchpad.net
Unsubscribe : https://launchpad.net/~dulwich-users
More help   : https://help.launchpad.net/ListHelp

"""
        _c, diff, version = git_am_patch_split(BytesIO(text))
        self.assertEqual(expected_diff, diff)
        self.assertEqual(None, version)


class DiffTests(TestCase):
    """Tests for write_blob_diff and write_tree_diff."""

    def test_blob_diff(self) -> None:
        f = BytesIO()
        write_blob_diff(
            f,
            (b"foo.txt", 0o644, Blob.from_string(b"old\nsame\n")),
            (b"bar.txt", 0o644, Blob.from_string(b"new\nsame\n")),
        )
        self.assertEqual(
            [
                b"diff --git a/foo.txt b/bar.txt",
                b"index 3b0f961..a116b51 644",
                b"--- a/foo.txt",
                b"+++ b/bar.txt",
                b"@@ -1,2 +1,2 @@",
                b"-old",
                b"+new",
                b" same",
            ],
            f.getvalue().splitlines(),
        )

    def test_blob_add(self) -> None:
        f = BytesIO()
        write_blob_diff(
            f,
            (None, None, None),
            (b"bar.txt", 0o644, Blob.from_string(b"new\nsame\n")),
        )
        self.assertEqual(
            [
                b"diff --git a/bar.txt b/bar.txt",
                b"new file mode 644",
                b"index 0000000..a116b51",
                b"--- /dev/null",
                b"+++ b/bar.txt",
                b"@@ -0,0 +1,2 @@",
                b"+new",
                b"+same",
            ],
            f.getvalue().splitlines(),
        )

    def test_blob_remove(self) -> None:
        f = BytesIO()
        write_blob_diff(
            f,
            (b"bar.txt", 0o644, Blob.from_string(b"new\nsame\n")),
            (None, None, None),
        )
        self.assertEqual(
            [
                b"diff --git a/bar.txt b/bar.txt",
                b"deleted file mode 644",
                b"index a116b51..0000000",
                b"--- a/bar.txt",
                b"+++ /dev/null",
                b"@@ -1,2 +0,0 @@",
                b"-new",
                b"-same",
            ],
            f.getvalue().splitlines(),
        )

    def test_tree_diff(self) -> None:
        f = BytesIO()
        store = MemoryObjectStore()
        added = Blob.from_string(b"add\n")
        removed = Blob.from_string(b"removed\n")
        changed1 = Blob.from_string(b"unchanged\nremoved\n")
        changed2 = Blob.from_string(b"unchanged\nadded\n")
        unchanged = Blob.from_string(b"unchanged\n")
        tree1 = Tree()
        tree1.add(b"removed.txt", 0o644, removed.id)
        tree1.add(b"changed.txt", 0o644, changed1.id)
        tree1.add(b"unchanged.txt", 0o644, changed1.id)
        tree2 = Tree()
        tree2.add(b"added.txt", 0o644, added.id)
        tree2.add(b"changed.txt", 0o644, changed2.id)
        tree2.add(b"unchanged.txt", 0o644, changed1.id)
        store.add_objects(
            [
                (o, None)
                for o in [
                    tree1,
                    tree2,
                    added,
                    removed,
                    changed1,
                    changed2,
                    unchanged,
                ]
            ]
        )
        write_tree_diff(f, store, tree1.id, tree2.id)
        self.assertEqual(
            [
                b"diff --git a/added.txt b/added.txt",
                b"new file mode 644",
                b"index 0000000..76d4bb8",
                b"--- /dev/null",
                b"+++ b/added.txt",
                b"@@ -0,0 +1 @@",
                b"+add",
                b"diff --git a/changed.txt b/changed.txt",
                b"index bf84e48..1be2436 644",
                b"--- a/changed.txt",
                b"+++ b/changed.txt",
                b"@@ -1,2 +1,2 @@",
                b" unchanged",
                b"-removed",
                b"+added",
                b"diff --git a/removed.txt b/removed.txt",
                b"deleted file mode 644",
                b"index 2c3f0b3..0000000",
                b"--- a/removed.txt",
                b"+++ /dev/null",
                b"@@ -1 +0,0 @@",
                b"-removed",
            ],
            f.getvalue().splitlines(),
        )

    def test_tree_diff_submodule(self) -> None:
        f = BytesIO()
        store = MemoryObjectStore()
        tree1 = Tree()
        tree1.add(
            b"asubmodule",
            S_IFGITLINK,
            b"06d0bdd9e2e20377b3180e4986b14c8549b393e4",
        )
        tree2 = Tree()
        tree2.add(
            b"asubmodule",
            S_IFGITLINK,
            b"cc975646af69f279396d4d5e1379ac6af80ee637",
        )
        store.add_objects([(o, None) for o in [tree1, tree2]])
        write_tree_diff(f, store, tree1.id, tree2.id)
        self.assertEqual(
            [
                b"diff --git a/asubmodule b/asubmodule",
                b"index 06d0bdd..cc97564 160000",
                b"--- a/asubmodule",
                b"+++ b/asubmodule",
                b"@@ -1 +1 @@",
                b"-Subproject commit 06d0bdd9e2e20377b3180e4986b14c8549b393e4",
                b"+Subproject commit cc975646af69f279396d4d5e1379ac6af80ee637",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_blob(self) -> None:
        f = BytesIO()
        b1 = Blob.from_string(b"old\nsame\n")
        b2 = Blob.from_string(b"new\nsame\n")
        store = MemoryObjectStore()
        store.add_objects([(b1, None), (b2, None)])
        write_object_diff(
            f, store, (b"foo.txt", 0o644, b1.id), (b"bar.txt", 0o644, b2.id)
        )
        self.assertEqual(
            [
                b"diff --git a/foo.txt b/bar.txt",
                b"index 3b0f961..a116b51 644",
                b"--- a/foo.txt",
                b"+++ b/bar.txt",
                b"@@ -1,2 +1,2 @@",
                b"-old",
                b"+new",
                b" same",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_add_blob(self) -> None:
        f = BytesIO()
        store = MemoryObjectStore()
        b2 = Blob.from_string(b"new\nsame\n")
        store.add_object(b2)
        write_object_diff(f, store, (None, None, None), (b"bar.txt", 0o644, b2.id))
        self.assertEqual(
            [
                b"diff --git a/bar.txt b/bar.txt",
                b"new file mode 644",
                b"index 0000000..a116b51",
                b"--- /dev/null",
                b"+++ b/bar.txt",
                b"@@ -0,0 +1,2 @@",
                b"+new",
                b"+same",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_remove_blob(self) -> None:
        f = BytesIO()
        b1 = Blob.from_string(b"new\nsame\n")
        store = MemoryObjectStore()
        store.add_object(b1)
        write_object_diff(f, store, (b"bar.txt", 0o644, b1.id), (None, None, None))
        self.assertEqual(
            [
                b"diff --git a/bar.txt b/bar.txt",
                b"deleted file mode 644",
                b"index a116b51..0000000",
                b"--- a/bar.txt",
                b"+++ /dev/null",
                b"@@ -1,2 +0,0 @@",
                b"-new",
                b"-same",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_bin_blob_force(self) -> None:
        f = BytesIO()
        # Prepare two slightly different PNG headers
        b1 = Blob.from_string(
            b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"
            b"\x00\x00\x00\x0d\x49\x48\x44\x52"
            b"\x00\x00\x01\xd5\x00\x00\x00\x9f"
            b"\x08\x04\x00\x00\x00\x05\x04\x8b"
        )
        b2 = Blob.from_string(
            b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"
            b"\x00\x00\x00\x0d\x49\x48\x44\x52"
            b"\x00\x00\x01\xd5\x00\x00\x00\x9f"
            b"\x08\x03\x00\x00\x00\x98\xd3\xb3"
        )
        store = MemoryObjectStore()
        store.add_objects([(b1, None), (b2, None)])
        write_object_diff(
            f,
            store,
            (b"foo.png", 0o644, b1.id),
            (b"bar.png", 0o644, b2.id),
            diff_binary=True,
        )
        self.assertEqual(
            [
                b"diff --git a/foo.png b/bar.png",
                b"index f73e47d..06364b7 644",
                b"--- a/foo.png",
                b"+++ b/bar.png",
                b"@@ -1,4 +1,4 @@",
                b" \x89PNG",
                b" \x1a",
                b" \x00\x00\x00",
                b"-IHDR\x00\x00\x01\xd5\x00\x00\x00"
                b"\x9f\x08\x04\x00\x00\x00\x05\x04\x8b",
                b"\\ No newline at end of file",
                b"+IHDR\x00\x00\x01\xd5\x00\x00\x00\x9f"
                b"\x08\x03\x00\x00\x00\x98\xd3\xb3",
                b"\\ No newline at end of file",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_bin_blob(self) -> None:
        f = BytesIO()
        # Prepare two slightly different PNG headers
        b1 = Blob.from_string(
            b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"
            b"\x00\x00\x00\x0d\x49\x48\x44\x52"
            b"\x00\x00\x01\xd5\x00\x00\x00\x9f"
            b"\x08\x04\x00\x00\x00\x05\x04\x8b"
        )
        b2 = Blob.from_string(
            b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"
            b"\x00\x00\x00\x0d\x49\x48\x44\x52"
            b"\x00\x00\x01\xd5\x00\x00\x00\x9f"
            b"\x08\x03\x00\x00\x00\x98\xd3\xb3"
        )
        store = MemoryObjectStore()
        store.add_objects([(b1, None), (b2, None)])
        write_object_diff(
            f, store, (b"foo.png", 0o644, b1.id), (b"bar.png", 0o644, b2.id)
        )
        self.assertEqual(
            [
                b"diff --git a/foo.png b/bar.png",
                b"index f73e47d..06364b7 644",
                b"Binary files a/foo.png and b/bar.png differ",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_add_bin_blob(self) -> None:
        f = BytesIO()
        b2 = Blob.from_string(
            b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"
            b"\x00\x00\x00\x0d\x49\x48\x44\x52"
            b"\x00\x00\x01\xd5\x00\x00\x00\x9f"
            b"\x08\x03\x00\x00\x00\x98\xd3\xb3"
        )
        store = MemoryObjectStore()
        store.add_object(b2)
        write_object_diff(f, store, (None, None, None), (b"bar.png", 0o644, b2.id))
        self.assertEqual(
            [
                b"diff --git a/bar.png b/bar.png",
                b"new file mode 644",
                b"index 0000000..06364b7",
                b"Binary files /dev/null and b/bar.png differ",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_remove_bin_blob(self) -> None:
        f = BytesIO()
        b1 = Blob.from_string(
            b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"
            b"\x00\x00\x00\x0d\x49\x48\x44\x52"
            b"\x00\x00\x01\xd5\x00\x00\x00\x9f"
            b"\x08\x04\x00\x00\x00\x05\x04\x8b"
        )
        store = MemoryObjectStore()
        store.add_object(b1)
        write_object_diff(f, store, (b"foo.png", 0o644, b1.id), (None, None, None))
        self.assertEqual(
            [
                b"diff --git a/foo.png b/foo.png",
                b"deleted file mode 644",
                b"index f73e47d..0000000",
                b"Binary files a/foo.png and /dev/null differ",
            ],
            f.getvalue().splitlines(),
        )

    def test_object_diff_kind_change(self) -> None:
        f = BytesIO()
        b1 = Blob.from_string(b"new\nsame\n")
        store = MemoryObjectStore()
        store.add_object(b1)
        write_object_diff(
            f,
            store,
            (b"bar.txt", 0o644, b1.id),
            (
                b"bar.txt",
                0o160000,
                b"06d0bdd9e2e20377b3180e4986b14c8549b393e4",
            ),
        )
        self.assertEqual(
            [
                b"diff --git a/bar.txt b/bar.txt",
                b"old file mode 644",
                b"new file mode 160000",
                b"index a116b51..06d0bdd 160000",
                b"--- a/bar.txt",
                b"+++ b/bar.txt",
                b"@@ -1,2 +1 @@",
                b"-new",
                b"-same",
                b"+Subproject commit 06d0bdd9e2e20377b3180e4986b14c8549b393e4",
            ],
            f.getvalue().splitlines(),
        )


class GetSummaryTests(TestCase):
    def test_simple(self) -> None:
        c = make_commit(
            author=b"Jelmer <jelmer@samba.org>",
            committer=b"Jelmer <jelmer@samba.org>",
            author_time=1271350201,
            commit_time=1271350201,
            author_timezone=0,
            commit_timezone=0,
            message=b"This is the first line\nAnd this is the second line.\n",
            tree=Tree().id,
        )
        self.assertEqual("This-is-the-first-line", get_summary(c))


class DiffAlgorithmTests(TestCase):
    """Tests for diff algorithm selection."""

    def test_unified_diff_with_myers(self) -> None:
        """Test unified_diff_with_algorithm with default myers algorithm."""
        a = [b"line1\n", b"line2\n", b"line3\n"]
        b = [b"line1\n", b"line2 modified\n", b"line3\n"]

        result = list(
            unified_diff_with_algorithm(
                a, b, fromfile=b"a.txt", tofile=b"b.txt", algorithm="myers"
            )
        )

        # Should contain diff headers and the change
        self.assertTrue(any(b"---" in line for line in result))
        self.assertTrue(any(b"+++" in line for line in result))
        self.assertTrue(any(b"-line2" in line for line in result))
        self.assertTrue(any(b"+line2 modified" in line for line in result))

    def test_unified_diff_with_patience_not_available(self) -> None:
        """Test that DiffAlgorithmNotAvailable is raised when patience not available."""
        # Temporarily mock _get_sequence_matcher to simulate ImportError
        import dulwich.patch

        original = dulwich.patch._get_sequence_matcher

        def mock_get_sequence_matcher(algorithm, a, b):
            if algorithm == "patience":
                raise DiffAlgorithmNotAvailable(
                    "patience", "Install with: pip install 'dulwich[patiencediff]'"
                )
            return original(algorithm, a, b)

        try:
            dulwich.patch._get_sequence_matcher = mock_get_sequence_matcher

            a = [b"line1\n", b"line2\n", b"line3\n"]
            b = [b"line1\n", b"line2 modified\n", b"line3\n"]

            with self.assertRaises(DiffAlgorithmNotAvailable) as cm:
                list(
                    unified_diff_with_algorithm(
                        a, b, fromfile=b"a.txt", tofile=b"b.txt", algorithm="patience"
                    )
                )

            self.assertIn("patience", str(cm.exception))
            self.assertIn("pip install", str(cm.exception))
        finally:
            dulwich.patch._get_sequence_matcher = original


class PatienceDiffTests(TestCase):
    """Tests for patience diff algorithm support."""

    def setUp(self) -> None:
        super().setUp()
        # Skip all patience diff tests if patiencediff is not available
        try:
            import patiencediff  # noqa: F401
        except ImportError:
            raise DependencyMissing("patiencediff")

    def test_unified_diff_with_patience_available(self) -> None:
        """Test unified_diff_with_algorithm with patience if available."""
        a = [b"line1\n", b"line2\n", b"line3\n"]
        b = [b"line1\n", b"line2 modified\n", b"line3\n"]

        result = list(
            unified_diff_with_algorithm(
                a, b, fromfile=b"a.txt", tofile=b"b.txt", algorithm="patience"
            )
        )

        # Should contain diff headers and the change
        self.assertTrue(any(b"---" in line for line in result))
        self.assertTrue(any(b"+++" in line for line in result))
        self.assertTrue(any(b"-line2" in line for line in result))
        self.assertTrue(any(b"+line2 modified" in line for line in result))

    def test_unified_diff_with_patience_not_available(self) -> None:
        """Test that DiffAlgorithmNotAvailable is raised when patience not available."""
        # Temporarily mock _get_sequence_matcher to simulate ImportError
        import dulwich.patch

        original = dulwich.patch._get_sequence_matcher

        def mock_get_sequence_matcher(algorithm, a, b):
            if algorithm == "patience":
                raise DiffAlgorithmNotAvailable(
                    "patience", "Install with: pip install 'dulwich[patiencediff]'"
                )
            return original(algorithm, a, b)

        try:
            dulwich.patch._get_sequence_matcher = mock_get_sequence_matcher

            a = [b"line1\n", b"line2\n", b"line3\n"]
            b = [b"line1\n", b"line2 modified\n", b"line3\n"]

            with self.assertRaises(DiffAlgorithmNotAvailable) as cm:
                list(
                    unified_diff_with_algorithm(
                        a, b, fromfile=b"a.txt", tofile=b"b.txt", algorithm="patience"
                    )
                )

            self.assertIn("patience", str(cm.exception))
            self.assertIn("pip install", str(cm.exception))
        finally:
            dulwich.patch._get_sequence_matcher = original

    def test_write_blob_diff_with_patience(self) -> None:
        """Test write_blob_diff with patience algorithm if available."""
        f = BytesIO()
        old_blob = Blob()
        old_blob.data = b"line1\nline2\nline3\n"
        new_blob = Blob()
        new_blob.data = b"line1\nline2 modified\nline3\n"

        write_blob_diff(
            f,
            (b"file.txt", 0o100644, old_blob),
            (b"file.txt", 0o100644, new_blob),
            diff_algorithm="patience",
        )

        diff = f.getvalue()
        self.assertIn(b"diff --git", diff)
        self.assertIn(b"-line2", diff)
        self.assertIn(b"+line2 modified", diff)

    def test_write_object_diff_with_patience(self) -> None:
        """Test write_object_diff with patience algorithm if available."""
        f = BytesIO()
        store = MemoryObjectStore()

        old_blob = Blob()
        old_blob.data = b"line1\nline2\nline3\n"
        store.add_object(old_blob)

        new_blob = Blob()
        new_blob.data = b"line1\nline2 modified\nline3\n"
        store.add_object(new_blob)

        write_object_diff(
            f,
            store,
            (b"file.txt", 0o100644, old_blob.id),
            (b"file.txt", 0o100644, new_blob.id),
            diff_algorithm="patience",
        )

        diff = f.getvalue()
        self.assertIn(b"diff --git", diff)
        self.assertIn(b"-line2", diff)
        self.assertIn(b"+line2 modified", diff)


class PatchIdTests(TestCase):
    """Tests for patch_id and commit_patch_id functions."""

    def test_patch_id_simple(self) -> None:
        """Test patch_id computation with a simple diff."""
        diff = b"""diff --git a/file.txt b/file.txt
index 3b0f961..a116b51 644
--- a/file.txt
+++ b/file.txt
@@ -1,2 +1,2 @@
-old
+new
 same
"""
        pid = patch_id(diff)
        # Patch ID should be a 40-byte hex string
        self.assertEqual(40, len(pid))
        self.assertTrue(all(c in b"0123456789abcdef" for c in pid))

    def test_patch_id_same_for_equivalent_diffs(self) -> None:
        """Test that equivalent patches have the same ID."""
        # Two diffs with different line numbers but same changes
        diff1 = b"""diff --git a/file.txt b/file.txt
--- a/file.txt
+++ b/file.txt
@@ -1,3 +1,3 @@
 context
-old line
+new line
 context
"""
        diff2 = b"""diff --git a/file.txt b/file.txt
--- a/file.txt
+++ b/file.txt
@@ -10,3 +10,3 @@
 context
-old line
+new line
 context
"""
        pid1 = patch_id(diff1)
        pid2 = patch_id(diff2)
        # Same patch content should give same patch ID
        self.assertEqual(pid1, pid2)

    def test_commit_patch_id(self) -> None:
        """Test commit_patch_id computation."""
        store = MemoryObjectStore()

        # Create two trees
        blob1 = Blob.from_string(b"content1\n")
        blob2 = Blob.from_string(b"content2\n")
        store.add_objects([(blob1, None), (blob2, None)])

        tree1 = Tree()
        tree1.add(b"file.txt", 0o644, blob1.id)
        store.add_object(tree1)

        tree2 = Tree()
        tree2.add(b"file.txt", 0o644, blob2.id)
        store.add_object(tree2)

        # Create a commit
        commit = Commit()
        commit.tree = tree2.id
        commit.parents = [ZERO_SHA]  # Fake parent
        commit.author = commit.committer = b"Test <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.message = b"Test commit\n"
        commit.encoding = b"UTF-8"
        store.add_object(commit)

        # Create parent commit
        parent_commit = Commit()
        parent_commit.tree = tree1.id
        parent_commit.parents = []
        parent_commit.author = parent_commit.committer = b"Test <test@example.com>"
        parent_commit.author_time = parent_commit.commit_time = 1234567880
        parent_commit.author_timezone = parent_commit.commit_timezone = 0
        parent_commit.message = b"Parent commit\n"
        parent_commit.encoding = b"UTF-8"
        store.add_object(parent_commit)

        # Update commit to have real parent
        commit.parents = [parent_commit.id]
        store.add_object(commit)

        # Compute patch ID
        pid = commit_patch_id(store, commit.id)
        self.assertEqual(40, len(pid))
        self.assertTrue(all(c in b"0123456789abcdef" for c in pid))


class MailinfoTests(TestCase):
    """Tests for mailinfo functionality."""

    def test_basic_parsing(self):
        """Test basic email parsing."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email_content = b"""From: John Doe <john@example.com>
Date: Mon, 1 Jan 2024 12:00:00 +0000
Subject: [PATCH] Add new feature
Message-ID: <test@example.com>

This is the commit message.

More details here.

---
 file.txt | 1 +
 1 file changed, 1 insertion(+)

diff --git a/file.txt b/file.txt
--- a/file.txt
+++ b/file.txt
@@ -1 +1,2 @@
 line1
+line2
--
2.39.0
"""
        result = mailinfo(BytesIO(email_content))

        self.assertEqual("John Doe", result.author_name)
        self.assertEqual("john@example.com", result.author_email)
        self.assertEqual("Add new feature", result.subject)
        self.assertIn("This is the commit message.", result.message)
        self.assertIn("More details here.", result.message)
        self.assertIn("diff --git a/file.txt b/file.txt", result.patch)

    def test_subject_munging(self):
        """Test subject line munging."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        # Test with [PATCH] tag
        email = b"""From: Test <test@example.com>
Subject: [PATCH 1/2] Fix bug

Body
"""
        result = mailinfo(BytesIO(email))
        self.assertEqual("Fix bug", result.subject)

        # Test with Re: prefix
        email = b"""From: Test <test@example.com>
Subject: Re: [PATCH] Fix bug

Body
"""
        result = mailinfo(BytesIO(email))
        self.assertEqual("Fix bug", result.subject)

        # Test with multiple brackets
        email = b"""From: Test <test@example.com>
Subject: [RFC][PATCH] New feature

Body
"""
        result = mailinfo(BytesIO(email))
        self.assertEqual("New feature", result.subject)

    def test_keep_subject(self):
        """Test -k flag (keep subject intact)."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email = b"""From: Test <test@example.com>
Subject: [PATCH 1/2] Fix bug

Body
"""
        result = mailinfo(BytesIO(email), keep_subject=True)
        self.assertEqual("[PATCH 1/2] Fix bug", result.subject)

    def test_keep_non_patch(self):
        """Test -b flag (only strip [PATCH])."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email = b"""From: Test <test@example.com>
Subject: [RFC][PATCH] New feature

Body
"""
        result = mailinfo(BytesIO(email), keep_non_patch=True)
        self.assertEqual("[RFC] New feature", result.subject)

    def test_scissors(self):
        """Test scissors line handling."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email = b"""From: Test <test@example.com>
Subject: Test

Ignore this part

-- >8 --

Keep this part

---
diff --git a/file.txt b/file.txt
"""
        result = mailinfo(BytesIO(email), scissors=True)
        self.assertIn("Keep this part", result.message)
        self.assertNotIn("Ignore this part", result.message)

    def test_message_id(self):
        """Test -m flag (include Message-ID)."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email = b"""From: Test <test@example.com>
Subject: Test
Message-ID: <12345@example.com>

Body text
"""
        result = mailinfo(BytesIO(email), message_id=True)
        self.assertIn("Message-ID: <12345@example.com>", result.message)
        self.assertEqual("<12345@example.com>", result.message_id)

    def test_encoding(self):
        """Test encoding handling."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        # Use explicit UTF-8 bytes with MIME encoded subject
        email = (
            b"From: Test <test@example.com>\n"
            b"Subject: =?utf-8?q?Test_with_UTF-8=3A_caf=C3=A9?=\n"
            b"Content-Type: text/plain; charset=utf-8\n"
            b"Content-Transfer-Encoding: 8bit\n"
            b"\n"
            b"Body with UTF-8: " + "naïve".encode() + b"\n"
        )

        result = mailinfo(BytesIO(email), encoding="utf-8")
        # The subject should be decoded from MIME encoding
        self.assertIn("caf", result.subject)
        self.assertIn("na", result.message)

    def test_patch_separation(self):
        """Test separation of message from patch."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email = b"""From: Test <test@example.com>
Subject: Test

Commit message line 1
Commit message line 2

---
 file.txt | 1 +
 1 file changed, 1 insertion(+)

diff --git a/file.txt b/file.txt
"""
        result = mailinfo(BytesIO(email))
        self.assertIn("Commit message line 1", result.message)
        self.assertIn("Commit message line 2", result.message)
        self.assertIn("---", result.patch)
        self.assertIn("diff --git", result.patch)
        self.assertNotIn("---", result.message)

    def test_no_subject(self):
        """Test handling of missing subject."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email = b"""From: Test <test@example.com>

Body text
"""
        result = mailinfo(BytesIO(email))
        self.assertEqual("(no subject)", result.subject)

    def test_missing_from_header(self):
        """Test error on missing From header."""
        from io import BytesIO

        from dulwich.patch import mailinfo

        email = b"""Subject: Test

Body text
"""
        with self.assertRaises(ValueError) as cm:
            mailinfo(BytesIO(email))
        self.assertIn("From", str(cm.exception))
