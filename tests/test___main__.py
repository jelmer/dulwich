# test___main__.py -- tests for __main__.py
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for __main__.py module entry point."""

import subprocess
import sys

from . import TestCase


class MainModuleTests(TestCase):
    """Tests for the __main__.py module entry point."""

    def test_main_module_help_flag(self) -> None:
        """Test that running dulwich as a module with --help works."""
        # Run dulwich as a module using python -m
        result = subprocess.run(
            [sys.executable, "-m", "dulwich", "--help"],
            capture_output=True,
            text=True,
        )

        # Help command exits with code 1 (standard behavior when no command is given)
        self.assertEqual(1, result.returncode)

        # Should start with usage line
        self.assertTrue(result.stdout.startswith("usage: dulwich"))

    def test_main_module_help_command(self) -> None:
        """Test that running dulwich as a module with help command works."""
        result = subprocess.run(
            [sys.executable, "-m", "dulwich", "help"],
            capture_output=True,
            text=True,
        )

        # Help command should succeed
        self.assertEqual(0, result.returncode)

        # Check exact output (help goes to stderr)
        expected = (
            "The dulwich command line tool is currently a very basic frontend for the\n"
            "Dulwich python module. For full functionality, please see the API reference.\n"
            "\n"
            "For a list of supported commands, see 'dulwich help -a'.\n"
        )
        self.assertEqual(expected, result.stderr)

    def test_main_module_no_args(self) -> None:
        """Test that running dulwich as a module with no arguments shows help."""
        result = subprocess.run(
            [sys.executable, "-m", "dulwich"],
            capture_output=True,
            text=True,
        )

        # No arguments should show help and exit with code 1
        self.assertEqual(1, result.returncode)

        # Should start with usage line
        self.assertTrue(result.stdout.startswith("usage: dulwich"))
