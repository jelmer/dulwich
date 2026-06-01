# test_range_diff.py -- Tests for CLI range-diff command
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

"""Tests for CLI range-diff command."""

import importlib.util
import io
import os
import sys
import tempfile
import unittest

from dulwich import porcelain
from dulwich.cli import cmd_range_diff

from .. import TestCase


@unittest.skipUnless(
    importlib.util.find_spec("munkres") is not None, "munkres not available"
)
class RangeDiffCommandTests(TestCase):
    """Tests for the range-diff CLI command."""

    def _make_stdout(self):
        raw = io.BytesIO()
        text = io.TextIOWrapper(raw, encoding="utf-8")
        # Disabling pager keeps output on this stream rather than spawning one.
        return raw, text

    def test_two_range_form(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_cwd = os.getcwd()
            orig_stdout = sys.stdout
            try:
                os.chdir(tmpdir)
                porcelain.init(".")

                def commit(message, name, content):
                    with open(name, "wb") as f:
                        f.write(content)
                    porcelain.add(".", paths=[name])
                    return porcelain.commit(
                        ".",
                        message=message,
                        author=b"Test <test@example.com>",
                        committer=b"Test <test@example.com>",
                    )

                base = commit(b"base", "a", b"a\n")
                shared = commit(b"shared", "b", b"b\n")
                old_tip = commit(b"dropped", "c", b"c\n")
                porcelain.reset(".", mode="hard", treeish=shared)
                new_tip = commit(b"added", "d", b"d\n")

                raw, text = self._make_stdout()
                sys.stdout = text
                cmd = cmd_range_diff()
                result = cmd.run(
                    [
                        (base + b".." + old_tip).decode(),
                        (base + b".." + new_tip).decode(),
                    ]
                )
                text.flush()
                output = raw.getvalue().decode()
            finally:
                sys.stdout = orig_stdout
                os.chdir(orig_cwd)

            self.assertEqual(0, result)
            self.assertIn("shared", output)
            self.assertIn("dropped", output)
            self.assertIn("added", output)
            self.assertIn(" < ", output)
            self.assertIn(" > ", output)
