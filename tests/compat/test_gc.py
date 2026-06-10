# test_gc.py -- Compatibility tests for garbage collection.
# Copyright (C) 2026 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Compatibility tests for dulwich garbage collection against C git."""

import os
import tempfile

from dulwich import porcelain
from dulwich.repo import Repo

from .utils import CompatTestCase, rmtree_ro, run_git_or_fail


class GcReadonlyPackCompatTests(CompatTestCase):
    """Test that dulwich can garbage collect packs produced by C git.

    C git writes pack files read-only. On Windows os.remove refuses to delete
    a read-only file, so dulwich must clear the attribute before unlinking;
    otherwise repack (and porcelain.gc) fails with PermissionError. See the
    bug report referenced in dulwich/object_store.py:_remove_readonly.
    """

    def setUp(self) -> None:
        super().setUp()
        self.path = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.path)

    def _git(self, *args, **kwargs):
        run_git_or_fail(list(args), cwd=self.path, **kwargs)

    def test_gc_removes_readonly_git_packs(self) -> None:
        self._git("init", ".")
        self._git("config", "user.email", "test@example.com")
        self._git("config", "user.name", "Test")

        # Two commits, each packed separately so git leaves two read-only packs
        # for dulwich to consolidate.
        with open(os.path.join(self.path, "a"), "w") as f:
            f.write("first\n")
        self._git("add", "a")
        self._git("commit", "-m", "one")
        self._git("repack", "-d")

        with open(os.path.join(self.path, "b"), "w") as f:
            f.write("second\n")
        self._git("add", "b")
        self._git("commit", "-m", "two")
        self._git("repack", "-d")

        pack_dir = os.path.join(self.path, ".git", "objects", "pack")
        packs = [n for n in os.listdir(pack_dir) if n.endswith(".pack")]
        self.assertEqual(2, len(packs))
        # Confirm git wrote them read-only, the precondition for the bug.
        for name in packs:
            mode = os.stat(os.path.join(pack_dir, name)).st_mode
            self.assertFalse(mode & 0o200, f"{name} is unexpectedly writable")

        repo = Repo(self.path)
        self.addCleanup(repo.close)
        before = {p.name() for p in repo.object_store.packs}
        self.assertEqual(2, len(before))

        porcelain.gc(repo, grace_period=0)

        after = {p.name() for p in repo.object_store.packs}
        self.assertEqual(1, len(after))
        # The original git packs are gone; a single consolidated pack remains.
        self.assertEqual(set(), before & after)

    def test_gc_prunes_unreachable_from_readonly_pack(self) -> None:
        self._git("init", ".")
        self._git("config", "user.email", "test@example.com")
        self._git("config", "user.name", "Test")

        with open(os.path.join(self.path, "a"), "w") as f:
            f.write("reachable\n")
        self._git("add", "a")
        self._git("commit", "-m", "one")

        # Create a dangling blob and pack everything so it lands in a
        # read-only pack alongside the reachable objects.
        blob_sha = run_git_or_fail(
            ["hash-object", "-w", "--stdin"],
            input=b"unreachable\n",
            cwd=self.path,
        ).strip()
        self._git("repack", "-ad")

        repo = Repo(self.path)
        self.addCleanup(repo.close)
        self.assertIn(blob_sha, repo.object_store)

        porcelain.gc(repo, grace_period=0)

        self.assertNotIn(blob_sha, repo.object_store)
