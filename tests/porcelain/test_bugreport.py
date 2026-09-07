# test_bugreport.py -- tests for porcelain bugreport
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

"""Tests for the porcelain bugreport function."""

import os
import shutil
import sys
import tempfile
from unittest import TestCase, skipIf

from dulwich.porcelain import bugreport
from dulwich.repo import Repo


def _section(report, name):
    """Return the body of a named section of a report."""
    _, _, rest = report.partition(f"[{name}]\n")
    body, _, _ = rest.partition("\n\n[")
    return body.strip()


class BugreportSystemInfoTests(TestCase):
    """The parts of the report that do not need a repository."""

    def test_reports_without_a_repository(self) -> None:
        """A bug report about failing to open a repository must still work."""
        report = bugreport(None, env={})
        self.assertIn("[System Info]", report)
        self.assertEqual("<no repository>", _section(report, "Enabled Hooks"))

    def test_names_the_dulwich_version_and_interpreter(self) -> None:
        from dulwich import __version__

        report = bugreport(None, env={})
        self.assertIn(".".join(str(p) for p in __version__), report)
        self.assertIn(sys.version.splitlines()[0], report)

    def test_records_the_shell_from_the_supplied_environment(self) -> None:
        """The environment is passed in, never read from os.environ here.

        Porcelain must not peek at the environment (CONTRIBUTING.rst,
        "Layering"), so this asserts the value comes from the argument.
        """
        report = bugreport(None, env={"SHELL": "/bin/somesh"})
        self.assertIn("/bin/somesh", report)

    def test_an_unset_shell_is_reported_as_unset(self) -> None:
        self.assertIn(
            "$SHELL (typically, interactive shell): <unset>", bugreport(None, env={})
        )

    def test_ends_with_a_newline(self) -> None:
        self.assertTrue(bugreport(None, env={}).endswith("\n"))


class BugreportHookTests(TestCase):
    """The [Enabled Hooks] section."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)
        self.hooks_dir = os.path.join(self.repo.controldir(), "hooks")
        os.makedirs(self.hooks_dir, exist_ok=True)

    def _write_hook(self, name, executable=True):
        path = os.path.join(self.hooks_dir, name)
        with open(path, "w") as f:
            f.write("#!/bin/sh\nexit 0\n")
        if executable:
            os.chmod(path, 0o755)
        else:
            os.chmod(path, 0o644)
        return path

    def test_a_repository_with_no_hooks(self) -> None:
        self.assertEqual(
            "<none>", _section(bugreport(self.test_dir, env={}), "Enabled Hooks")
        )

    def test_lists_an_enabled_hook(self) -> None:
        self._write_hook("pre-commit")
        self.assertEqual(
            "pre-commit", _section(bugreport(self.test_dir, env={}), "Enabled Hooks")
        )

    def test_ignores_the_sample_hooks_git_ships(self) -> None:
        """`git init` leaves a directory full of *.sample files.

        Listing those would report every fresh repository as having a dozen
        hooks enabled, which is the opposite of useful.
        """
        self._write_hook("pre-commit.sample")
        self.assertEqual(
            "<none>", _section(bugreport(self.test_dir, env={}), "Enabled Hooks")
        )

    def test_lists_a_name_githooks_does_not_define(self) -> None:
        """A file git will not run is worth reporting, not hiding.

        The report says what is in the hooks directory. Filtering it against a
        list of the names githooks(5) defines would drop exactly the two cases
        a bug report needs to surface: a hook that is misspelled and therefore
        never fires, and one from a git newer than that list.
        """
        self._write_hook("pre-comit")
        self.assertEqual(
            "pre-comit", _section(bugreport(self.test_dir, env={}), "Enabled Hooks")
        )

    @skipIf(sys.platform == "win32", "no execute bit on Windows")
    def test_a_non_executable_hook_is_disabled(self) -> None:
        """Git will not run it, so the report must not claim it is enabled."""
        self._write_hook("pre-commit", executable=False)
        self.assertEqual(
            "<none>", _section(bugreport(self.test_dir, env={}), "Enabled Hooks")
        )

    def test_lists_hooks_in_a_stable_order(self) -> None:
        """Directory order is filesystem-dependent; the report should not be."""
        for name in ("post-commit", "pre-commit", "commit-msg"):
            self._write_hook(name)
        self.assertEqual(
            ["commit-msg", "post-commit", "pre-commit"],
            _section(bugreport(self.test_dir, env={}), "Enabled Hooks").split("\n"),
        )

    def test_a_path_that_is_not_a_repository(self) -> None:
        not_a_repo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, not_a_repo)
        self.assertEqual(
            "<not a git repository>",
            _section(bugreport(not_a_repo, env={}), "Enabled Hooks"),
        )
