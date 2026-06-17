# test_range_diff.py -- Tests for porcelain range-diff
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

"""Tests for the porcelain range-diff functionality."""

import importlib.util
import os
import shutil
import tempfile
import unittest
from io import BytesIO

from dulwich import porcelain
from dulwich.repo import Repo

from .. import TestCase


@unittest.skipUnless(
    importlib.util.find_spec("munkres") is not None, "munkres not available"
)
class PorcelainRangeDiffTests(TestCase):
    """Tests for the porcelain range_diff function."""

    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup)
        self.repo = Repo.init(self.tmpdir)
        self.addCleanup(self.repo.close)

    def _cleanup(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def commit(self, message, files):
        for name, content in files.items():
            path = os.path.join(self.tmpdir, name)
            with open(path, "wb") as f:
                f.write(content)
            porcelain.add(self.repo, paths=[path])
        return porcelain.commit(
            self.repo,
            message=message,
            author=b"Test <test@example.com>",
            committer=b"Test <test@example.com>",
        )

    def _build_two_ranges(self):
        base = self.commit(b"base", {"a": b"a\n"})
        old1 = self.commit(b"shared", {"b": b"b\n"})
        old_tip = self.commit(b"dropped", {"c": b"c\n"})
        porcelain.reset(self.repo, mode="hard", treeish=old1)
        new_tip = self.commit(b"added", {"d": b"d\n"})
        return base, old_tip, new_tip

    def test_two_range_form(self):
        base, old_tip, new_tip = self._build_two_ranges()
        out = BytesIO()
        porcelain.range_diff(
            self.repo,
            base + b".." + old_tip,
            base + b".." + new_tip,
            outstream=out,
        )
        text = out.getvalue().decode()
        self.assertIn("shared", text)
        self.assertIn("dropped", text)
        self.assertIn("added", text)

    def test_base_form(self):
        base, old_tip, new_tip = self._build_two_ranges()
        out = BytesIO()
        porcelain.range_diff(self.repo, old_tip, new_tip, base=base, outstream=out)
        text = out.getvalue().decode()
        self.assertIn("shared", text)

    def test_symmetric_form(self):
        _base, old_tip, new_tip = self._build_two_ranges()
        out = BytesIO()
        porcelain.range_diff(self.repo, old_tip + b"..." + new_tip, outstream=out)
        text = out.getvalue().decode()
        self.assertIn("dropped", text)
        self.assertIn("added", text)

    def test_single_revision_requires_symmetric(self):
        base = self.commit(b"base", {"a": b"a\n"})
        out = BytesIO()
        self.assertRaises(
            porcelain.Error,
            porcelain.range_diff,
            self.repo,
            base,
            outstream=out,
        )

    def test_base_form_requires_two_revisions(self):
        base = self.commit(b"base", {"a": b"a\n"})
        out = BytesIO()
        self.assertRaises(
            porcelain.Error,
            porcelain.range_diff,
            self.repo,
            base,
            base=base,
            outstream=out,
        )

    def test_str_arguments(self):
        base, old_tip, new_tip = self._build_two_ranges()
        out = BytesIO()
        porcelain.range_diff(
            self.repo,
            (base + b".." + old_tip).decode(),
            (base + b".." + new_tip).decode(),
            outstream=out,
        )
        self.assertIn(b"shared", out.getvalue())

    def test_commit_object_arguments(self):
        base, old_tip, new_tip = self._build_two_ranges()
        # Pass parsed Commit objects rather than revision strings.
        base_commit = self.repo[base]
        old_commit = self.repo[old_tip]
        new_commit = self.repo[new_tip]
        out = BytesIO()
        porcelain.range_diff(
            self.repo,
            old_commit,
            new_commit,
            base=base_commit,
            outstream=out,
        )
        self.assertIn(b"shared", out.getvalue())
