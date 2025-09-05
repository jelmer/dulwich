# __init__.py -- The tests for dulwich
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
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

"""Tests for Dulwich."""

__all__ = [
    "BlackboxTestCase",
    "SkipTest",
    "TestCase",
    "expectedFailure",
    "skipIf",
]

import doctest
import os
import shutil
import subprocess
import sys
import tempfile

# If Python itself provides an exception, use that
import unittest
from typing import ClassVar, Optional
from unittest import SkipTest, expectedFailure, skipIf
from unittest import TestCase as _TestCase


class TestCase(_TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.overrideEnv("HOME", "/nonexistent")
        self.overrideEnv("GIT_CONFIG_NOSYSTEM", "1")

    def overrideEnv(self, name: str, value: Optional[str]) -> None:
        def restore() -> None:
            if oldval is not None:
                os.environ[name] = oldval
            elif name in os.environ:
                del os.environ[name]

        oldval = os.environ.get(name)
        if value is not None:
            os.environ[name] = value
        elif name in os.environ:
            del os.environ[name]
        self.addCleanup(restore)


class BlackboxTestCase(TestCase):
    """Blackbox testing."""

    # TODO(jelmer): Include more possible binary paths.
    bin_directories: ClassVar[list[str]] = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "bin")),
        "/usr/bin",
        "/usr/local/bin",
    ]

    def bin_path(self, name: str) -> str:
        """Determine the full path of a binary.

        Args:
          name: Name of the script
        Returns: Full path
        """
        for d in self.bin_directories:
            p = os.path.join(d, name)
            if os.path.isfile(p):
                return p
        else:
            raise SkipTest(f"Unable to find binary {name}")

    def run_command(self, name: str, args: list[str]) -> subprocess.Popen[bytes]:
        """Run a Dulwich command.

        Args:
          name: Name of the command, as it exists in bin/
          args: Arguments to the command
        """
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(sys.path)

        # Since they don't have any extensions, Windows can't recognize
        # executablility of the Python files in /bin. Even then, we'd have to
        # expect the user to set up file associations for .py files.
        #
        # Save us from all that headache and call python with the bin script.
        argv = [sys.executable, self.bin_path(name), *args]
        return subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )


def self_test_suite() -> unittest.TestSuite:
    names = [
        "annotate",
        "archive",
        "attrs",
        "bisect",
        "blackbox",
        "bundle",
        "cli",
        "cli_cherry_pick",
        "cli_merge",
        "client",
        "cloud_gcs",
        "commit_graph",
        "config",
        "credentials",
        "diff",
        "diff_tree",
        "dumb",
        "fastexport",
        "file",
        "filter_branch",
        "filters",
        "gc",
        "grafts",
        "graph",
        "greenthreads",
        "hooks",
        "ignore",
        "index",
        "lfs",
        "lfs_integration",
        "line_ending",
        "log_utils",
        "lru_cache",
        "mailmap",
        "merge",
        "merge_drivers",
        "missing_obj_finder",
        "notes",
        "objects",
        "objectspec",
        "object_store",
        "pack",
        "patch",
        "porcelain",
        "porcelain_cherry_pick",
        "porcelain_filters",
        "porcelain_lfs",
        "porcelain_merge",
        "porcelain_notes",
        "protocol",
        "rebase",
        "reflog",
        "refs",
        "reftable",
        "repository",
        "server",
        "sparse_patterns",
        "stash",
        "submodule",
        "utils",
        "walk",
        "web",
        "whitespace",
        "worktree",
    ]
    module_names = ["tests.test_" + name for name in names]
    loader = unittest.TestLoader()
    return loader.loadTestsFromNames(module_names)


def tutorial_test_suite() -> unittest.TestSuite:
    tutorial = [
        "introduction",
        "file-format",
        "repo",
        "object-store",
        "remote",
        "conclusion",
    ]
    tutorial_files = [f"../docs/tutorial/{name}.txt" for name in tutorial]

    to_restore = []

    def overrideEnv(name: str, value: Optional[str]) -> None:
        oldval = os.environ.get(name)
        if value is not None:
            os.environ[name] = value
        else:
            del os.environ[name]
        to_restore.append((name, oldval))

    def setup(test: doctest.DocTest) -> None:
        test.__old_cwd = os.getcwd()  # type: ignore[attr-defined]
        test.tempdir = tempfile.mkdtemp()  # type: ignore[attr-defined]
        test.globs.update({"tempdir": test.tempdir})  # type: ignore[attr-defined]
        os.chdir(test.tempdir)  # type: ignore[attr-defined]
        overrideEnv("HOME", "/nonexistent")
        overrideEnv("GIT_CONFIG_NOSYSTEM", "1")

    def teardown(test: doctest.DocTest) -> None:
        os.chdir(test.__old_cwd)  # type: ignore[attr-defined]
        shutil.rmtree(test.tempdir)  # type: ignore[attr-defined]
        for name, oldval in to_restore:
            if oldval is not None:
                os.environ[name] = oldval
            else:
                del os.environ[name]
        to_restore.clear()

    return doctest.DocFileSuite(
        module_relative=True,
        package="tests",
        setUp=setup,
        tearDown=teardown,
        *tutorial_files,
    )


def nocompat_test_suite() -> unittest.TestSuite:
    result = unittest.TestSuite()
    result.addTests(self_test_suite())
    result.addTests(tutorial_test_suite())
    from .contrib import test_suite as contrib_test_suite

    result.addTests(contrib_test_suite())
    return result


def compat_test_suite() -> unittest.TestSuite:
    result = unittest.TestSuite()
    from .compat import test_suite as compat_test_suite

    result.addTests(compat_test_suite())
    return result


def test_suite() -> unittest.TestSuite:
    result = unittest.TestSuite()
    result.addTests(self_test_suite())
    if sys.platform != "win32":
        result.addTests(tutorial_test_suite())
    from .compat import test_suite as compat_test_suite

    result.addTests(compat_test_suite())
    from .contrib import test_suite as contrib_test_suite

    result.addTests(contrib_test_suite())
    return result
