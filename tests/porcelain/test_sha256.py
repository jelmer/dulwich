# test_sha256.py -- Tests for porcelain operations on SHA256 repositories
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests that porcelain keeps SHA256 repositories in a consistent format."""

import os
import shutil
import tempfile
from unittest import TestCase

from dulwich import porcelain
from dulwich.object_format import SHA256


class TestPorcelainSHA256(TestCase):
    """Porcelain must not mix SHA1 identities into a SHA256 repository."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)

    def _init(self, name="repo"):
        path = os.path.join(self.test_dir, name)
        repo = porcelain.init(path, object_format="sha256")
        self.addCleanup(repo.close)
        return path, repo

    def _write(self, path, name, contents):
        with open(os.path.join(path, name), "w") as f:
            f.write(contents)

    def _commit(self, path, message=b"initial"):
        return porcelain.commit(
            path,
            message=message,
            author=b"Test <test@example.com>",
            committer=b"Test <test@example.com>",
            no_verify=True,
        )

    def test_add_writes_sha256_index_entries(self):
        path, repo = self._init()
        self._write(path, "a", "contents\n")
        porcelain.add(path, paths=["a"])

        index = repo.open_index()
        self.assertEqual(SHA256, index.object_format)
        self.assertEqual(64, len(index[b"a"].sha))

    def test_commit_publishes_resolvable_sha256_ref(self):
        path, repo = self._init()
        self._write(path, "a", "contents\n")
        porcelain.add(path, paths=["a"])
        commit_id = self._commit(path)

        self.assertEqual(64, len(commit_id))
        self.assertEqual(commit_id, repo.refs[b"HEAD"])
        commit = repo[commit_id]
        self.assertEqual(commit_id, commit.get_id(SHA256))
        self.assertEqual(64, len(commit.tree))
        self.assertEqual(commit.tree, repo[commit.tree].get_id(SHA256))

    def test_committed_tree_holds_32_byte_ids(self):
        path, repo = self._init()
        self._write(path, "a", "contents\n")
        porcelain.add(path, paths=["a"])
        commit_id = self._commit(path)

        raw = repo[repo[commit_id].tree].as_raw_string()
        self.assertEqual(len(b"100644 a\0") + 32, len(raw))

    def test_status_clean_after_commit(self):
        path, _repo = self._init()
        self._write(path, "a", "contents\n")
        porcelain.add(path, paths=["a"])
        self._commit(path)

        status = porcelain.status(path, untracked_files="no")
        self.assertEqual([], status.unstaged)
        self.assertEqual([], status.staged["modify"])

    def test_clone_checkout_keeps_sha256(self):
        source_path, _source = self._init("source")
        self._write(source_path, "a", "contents\n")
        porcelain.add(source_path, paths=["a"])
        self._commit(source_path)

        target_path = os.path.join(self.test_dir, "target")
        target = porcelain.clone(source_path, target_path, checkout=True)
        self.addCleanup(target.close)

        self.assertEqual(SHA256, target.object_format)
        self.assertEqual(64, len(target.head()))
        self.assertEqual(64, len(target.open_index()[b"a"].sha))

        status = porcelain.status(target_path, untracked_files="no")
        self.assertEqual([], status.unstaged)
        self.assertEqual([], status.staged["modify"])

    def test_second_commit_keeps_sha256(self):
        path, repo = self._init()
        self._write(path, "a", "contents\n")
        porcelain.add(path, paths=["a"])
        first = self._commit(path)

        self._write(path, "a", "contents\nmore\n")
        porcelain.add(path, paths=["a"])
        second = self._commit(path, message=b"second")

        self.assertEqual(64, len(second))
        self.assertNotEqual(first, second)
        self.assertEqual([first], repo[second].parents)

    def test_sha1_repository_unaffected(self):
        path = os.path.join(self.test_dir, "sha1")
        repo = porcelain.init(path)
        self.addCleanup(repo.close)
        self._write(path, "a", "contents\n")
        porcelain.add(path, paths=["a"])
        commit_id = self._commit(path)

        self.assertEqual(40, len(commit_id))
        self.assertEqual(40, len(repo.open_index()[b"a"].sha))
