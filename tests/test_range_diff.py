# test_range_diff.py -- Tests for range-diff
# Copyright (C) 2026 Jelmer Vernooĳ <jelmer@jelmer.uk>
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

"""Tests for the range-diff implementation."""

import importlib.util
import shutil
import sys
import tempfile
import unittest
from unittest import mock

from dulwich.objects import Blob, Commit, Tree
from dulwich.range_diff import (
    _normalize_patch,
    _solve_assignment,
    format_range_diff,
    range_diff,
)
from dulwich.repo import Repo

from . import TestCase

missing_munkres = unittest.skipUnless(
    importlib.util.find_spec("munkres") is not None,
    "munkres not available",
)


@missing_munkres
class SolveAssignmentTests(TestCase):
    """Tests for the assignment solver."""

    def test_empty(self):
        self.assertEqual([], _solve_assignment([]))

    def test_identity(self):
        cost = [[0, 5, 5], [5, 0, 5], [5, 5, 0]]
        self.assertEqual([0, 1, 2], _solve_assignment(cost))

    def test_permutation(self):
        # Cheapest assignment is the off-diagonal permutation.
        cost = [[5, 0, 5], [5, 5, 0], [0, 5, 5]]
        self.assertEqual([1, 2, 0], _solve_assignment(cost))

    def test_rectangular(self):
        # Two rows, three columns: each row picks its zero-cost column.
        cost = [[0, 9, 9], [9, 9, 0]]
        self.assertEqual([0, 2], _solve_assignment(cost))


class MunkresMissingTests(TestCase):
    """Tests for behavior when the munkres dependency is missing."""

    def test_solve_assignment_raises(self):
        # Setting the entry to None makes "import munkres" raise ImportError.
        with mock.patch.dict(sys.modules, {"munkres": None}):
            self.assertRaises(ImportError, _solve_assignment, [[0, 1], [1, 0]])


class NormalizePatchTests(TestCase):
    """Tests for patch normalization."""

    def test_strips_index_line(self):
        patch = (
            b"diff --git a/f b/f\n"
            b"index 1234567..89abcde 100644\n"
            b"--- a/f\n"
            b"+++ b/f\n"
            b"@@ -1 +1 @@\n"
            b"-old\n"
            b"+new\n"
        )
        normalized = _normalize_patch(patch)
        self.assertNotIn(b"index ", normalized)

    def test_rewrites_hunk_header(self):
        patch = (
            b"diff --git a/f b/f\n"
            b"--- a/f\n"
            b"+++ b/f\n"
            b"@@ -10,3 +10,4 @@ def foo():\n"
            b" ctx\n"
            b"+added\n"
        )
        normalized = _normalize_patch(patch)
        self.assertIn(b"@@ f: def foo():\n", normalized)
        self.assertNotIn(b"-10,3", normalized)


class RangeDiffTestCase(TestCase):
    """Base class providing a repository and commit helper.

    Commits are built directly through the object store rather than through
    porcelain so the tests for the range-diff module do not depend on it.
    """

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup)
        self.repo = Repo.init_bare(self.tmpdir)
        self.addCleanup(self.repo.close)
        # The path -> content of the tree the next commit builds on, and the
        # commit it is parented on.
        self._tree: dict[bytes, bytes] = {}
        self._head: bytes | None = None
        self._commit_time = 0

    def _cleanup(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def commit(self, message, files):
        store = self.repo.object_store
        for name, content in files.items():
            blob = Blob.from_string(content)
            store.add_object(blob)
            self._tree[name.encode() if isinstance(name, str) else name] = blob.id
        tree = Tree()
        for path, blob_id in self._tree.items():
            tree.add(path, 0o100644, blob_id)
        store.add_object(tree)
        commit = Commit()
        commit.tree = tree.id
        commit.parents = [] if self._head is None else [self._head]
        commit.author = commit.committer = b"Test <test@example.com>"
        commit.author_time = commit.commit_time = self._commit_time
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = message
        store.add_object(commit)
        self._commit_time += 100
        self._head = commit.id
        return commit.id

    def reset(self, commit_id):
        """Reset the working state to the tree of the given commit."""
        commit = self.repo[commit_id]
        assert isinstance(commit, Commit)
        tree = self.repo[commit.tree]
        assert isinstance(tree, Tree)
        self._tree = {path: sha for path, _mode, sha in tree.iteritems()}
        self._head = commit_id


@missing_munkres
class RangeDiffCommitsTests(RangeDiffTestCase):
    """Tests for range_diff over commit ranges."""

    def test_identical_ranges(self):
        base = self.commit(b"base", {"a": b"a\n"})
        c1 = self.commit(b"one", {"b": b"b\n"})
        entries = range_diff(self.repo, base, c1, base, c1)
        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual("=", entry.status)
        self.assertEqual(1, entry.old_idx)
        self.assertEqual(1, entry.new_idx)
        self.assertEqual([], entry.diff)

    def test_commit_object_endpoints(self):
        base = self.commit(b"base", {"a": b"a\n"})
        c1 = self.commit(b"one", {"b": b"b\n"})
        # Resolve the endpoints to Commit objects and pass them directly.
        base_commit = self.repo[base]
        c1_commit = self.repo[c1]
        entries = range_diff(self.repo, base_commit, c1_commit, base_commit, c1_commit)
        self.assertEqual(["="], [e.status for e in entries])

    def test_added_commit(self):
        base = self.commit(b"base", {"a": b"a\n"})
        old_tip = self.commit(b"one", {"b": b"b\n"})
        new_tip = self.commit(b"two", {"c": b"c\n"})
        entries = range_diff(self.repo, base, old_tip, base, new_tip)
        statuses = [e.status for e in entries]
        self.assertEqual(["=", ">"], statuses)
        self.assertEqual((1, 1), (entries[0].old_idx, entries[0].new_idx))
        self.assertEqual((None, 2), (entries[1].old_idx, entries[1].new_idx))

    def test_dropped_commit(self):
        base = self.commit(b"base", {"a": b"a\n"})
        c1 = self.commit(b"one", {"b": b"b\n"})
        old_tip = self.commit(b"two", {"c": b"c\n"})
        # Second range only has the first commit.
        entries = range_diff(self.repo, base, old_tip, base, c1)
        statuses = [e.status for e in entries]
        self.assertEqual(["=", "<"], statuses)
        self.assertEqual((2, None), (entries[1].old_idx, entries[1].new_idx))

    def test_modified_commit(self):
        # A larger commit with a small tweak should be matched (status "!")
        # rather than treated as an unrelated add/drop pair.
        base_content = b"".join(b"%d\n" % i for i in range(1, 21))
        base = self.commit(b"base", {"f": base_content})
        old_tip = self.commit(b"append", {"f": base_content + b"variant A\n"})
        self.reset(base)
        new_tip = self.commit(b"append", {"f": base_content + b"variant B\n"})
        entries = range_diff(self.repo, base, old_tip, base, new_tip)
        self.assertEqual(1, len(entries))
        entry = entries[0]
        self.assertEqual("!", entry.status)
        diff_text = b"".join(entry.diff)
        self.assertIn(b"-+variant A\n", diff_text)
        self.assertIn(b"++variant B\n", diff_text)
        # The volatile index and file-header lines are not shown.
        self.assertNotIn(b"index ", diff_text)
        self.assertNotIn(b"--- ", diff_text)

    def test_empty_ranges(self):
        base = self.commit(b"base", {"a": b"a\n"})
        entries = range_diff(self.repo, base, base, base, base)
        self.assertEqual([], entries)

    def test_reordered_commits(self):
        base = self.commit(b"base", {"a": b"a\n"})
        self.commit(b"commit X", {"x": b"x\n"})
        old_tip = self.commit(b"commit Y", {"y": b"y\n"})
        self.reset(base)
        self.commit(b"commit Y", {"y": b"y\n"})
        new_tip = self.commit(b"commit X", {"x": b"x\n"})
        entries = range_diff(self.repo, base, old_tip, base, new_tip)
        # Both commits still match, just in swapped positions.
        self.assertEqual(["=", "="], [e.status for e in entries])
        self.assertEqual((2, 1), (entries[0].old_idx, entries[0].new_idx))
        self.assertEqual((1, 2), (entries[1].old_idx, entries[1].new_idx))

    def test_creation_factor_prevents_match(self):
        base_content = b"".join(b"%d\n" % i for i in range(1, 21))
        base = self.commit(b"base", {"f": base_content})
        old_tip = self.commit(b"append", {"f": base_content + b"variant A\n"})
        self.reset(base)
        new_tip = self.commit(b"append", {"f": base_content + b"variant B\n"})
        # With the default factor these match.
        default = range_diff(self.repo, base, old_tip, base, new_tip)
        self.assertEqual(["!"], [e.status for e in default])
        # With a tiny factor the commits are treated as unrelated.
        strict = range_diff(self.repo, base, old_tip, base, new_tip, creation_factor=1)
        self.assertEqual(["<", ">"], [e.status for e in strict])


@missing_munkres
class FormatRangeDiffTests(RangeDiffTestCase):
    """Tests for rendering range-diff entries."""

    def test_format_added_and_dropped(self):
        base = self.commit(b"base", {"a": b"a\n"})
        c1 = self.commit(b"keep me", {"b": b"b\n"})
        old_tip = self.commit(b"drop me", {"c": b"c\n"})
        self.reset(c1)
        new_tip = self.commit(b"brand new", {"d": b"d\n"})
        entries = range_diff(self.repo, base, old_tip, base, new_tip)
        lines = format_range_diff(entries)
        text = b"".join(lines).decode()
        self.assertIn("keep me", text)
        self.assertIn("drop me", text)
        self.assertIn("brand new", text)
        # The matched commit uses "=", the dropped one "<", the new one ">".
        self.assertIn(" = ", text)
        self.assertIn(" < ", text)
        self.assertIn(" > ", text)
