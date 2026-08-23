# test_hooks.py -- Tests for hook porcelain functions
# Copyright (C) 2024 Jelmer Vernooij
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

"""Tests for hook porcelain functions."""

import os
import shutil
import tempfile

from dulwich import porcelain
from dulwich.errors import HookError
from dulwich.repo import Repo
from tests import TestCase


class HookPorcelainTestCase(TestCase):
    """Test case for hook porcelain functions."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup_test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def test_hook_run_update(self) -> None:
        test_file = os.path.join(self.repo.controldir(), "hooks", "update")
        with open(test_file, "w") as f:
            f.write(
                '#!/bin/sh\necho "stdout: $1 $2 $3"\necho "stderr output" >&2\nexit 0\n'
            )
        os.chmod(test_file, 0o755)

        result = porcelain.hook_run(
            self.repo, "update", ["refs/heads/main", "abc123", "def456"]
        )
        self.assertEqual(
            result,
            (
                b"stdout: refs/heads/main abc123 def456\n",
                b"stderr output\n",
            ),
        )

    def test_hook_run_unrecognized(self) -> None:
        self.assertRaises(
            HookError, porcelain.hook_run, self.repo, "not-a-real-hook", []
        )

    def test_hook_run_rejects_receive_hooks(self) -> None:
        self.assertRaises(HookError, porcelain.hook_run, self.repo, "pre-receive", [])

    def _cleanup_test_dir(self):
        """Clean up test directory recursively."""
        shutil.rmtree(self.test_dir, ignore_errors=True)
