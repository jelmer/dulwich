# test_source.py -- Tests for scanning dulwich source code
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

"""Tests for scanning dulwich source code for compliance."""

import os
import re
import unittest
from pathlib import Path

# Files that are allowed to not have the standard preamble
PREAMBLE_EXCEPTIONS = [
    "dulwich/diffstat.py",  # MIT licensed file
]

# Files that are allowed to use os.environ (beyond cli.py and porcelain/)
OS_ENVIRON_EXCEPTIONS = [
    "dulwich/client.py",  # Git protocol environment variables
    "dulwich/repo.py",  # User identity environment variables
    "dulwich/log_utils.py",  # GIT_TRACE environment variable
    "dulwich/config.py",  # Git configuration environment variables
    "dulwich/gc.py",  # GIT_AUTO_GC environment variable
    "dulwich/contrib/swift.py",  # DULWICH_SWIFT_CFG environment variable
    "dulwich/hooks.py",  # Git hooks environment setup
]

# Standard license block that must appear in all files
STANDARD_LICENSE_BLOCK = [
    "# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later\n",
    "# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU\n",
    "# General Public License as published by the Free Software Foundation; version 2.0\n",
    "# or (at your option) any later version. You can redistribute it and/or\n",
    "# modify it under the terms of either of these two licenses.\n",
    "#\n",
    "# Unless required by applicable law or agreed to in writing, software\n",
    '# distributed under the License is distributed on an "AS IS" BASIS,\n',
    "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n",
    "# See the License for the specific language governing permissions and\n",
    "# limitations under the License.\n",
    "#\n",
    "# You should have received a copy of the licenses; if not, see\n",
    "# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License\n",
    "# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache\n",
    "# License, Version 2.0.\n",
    "#\n",
]


class SourceCodeComplianceTests(unittest.TestCase):
    """Tests to ensure dulwich source code follows project standards."""

    @staticmethod
    def _get_dulwich_python_files():
        """Get all Python files in the dulwich package.

        Returns:
            List of tuples of (Path object, relative path from project root)
        """
        project_root = Path(__file__).parent.parent
        dulwich_dir = project_root / "dulwich"
        if not dulwich_dir.exists():
            raise RuntimeError(f"dulwich directory not found at {dulwich_dir}")

        python_files = []
        for root, dirs, files in os.walk(dulwich_dir):
            # Skip build directories
            if "build" in root or "__pycache__" in root:
                continue

            for file in files:
                if file.endswith(".py"):
                    file_path = Path(root) / file
                    rel_path = file_path.relative_to(project_root)
                    python_files.append((file_path, rel_path))

        return python_files

    @classmethod
    def _has_standard_preamble(cls, file_path: Path) -> tuple[bool, str]:
        """Check if a file has the standard dulwich preamble.

        The standard preamble consists of:
        - First line: # filename -- Description (or similar)
        - Copyright line(s): # Copyright (C) ...
        - Empty comment: #
        - Standard license block (exact match required)

        Args:
            file_path: Path to the Python file to check

        Returns:
            Tuple of (has_preamble, error_message)
        """
        with open(file_path, encoding="utf-8") as f:
            lines = f.readlines()

        if len(lines) < 21:
            return False, "File too short to contain standard preamble"

        # Check first line starts with #
        if not lines[0].startswith("#"):
            return False, "First line does not start with #"

        # Find the SPDX line (should be within first 10 lines)
        spdx_line_idx = None
        for i in range(min(10, len(lines))):
            if "SPDX-License-Identifier" in lines[i]:
                spdx_line_idx = i
                break

        if spdx_line_idx is None:
            return False, "SPDX-License-Identifier line not found in first 10 lines"

        # Check that we have enough lines after the SPDX line
        if len(lines) < spdx_line_idx + len(STANDARD_LICENSE_BLOCK):
            return (
                False,
                "File too short to contain complete license block after SPDX line",
            )

        # Extract the license block from the file
        file_license_block = lines[
            spdx_line_idx : spdx_line_idx + len(STANDARD_LICENSE_BLOCK)
        ]

        # Compare with standard license block
        for i, (expected, actual) in enumerate(
            zip(STANDARD_LICENSE_BLOCK, file_license_block)
        ):
            if expected != actual:
                return (
                    False,
                    f"License block mismatch at line {spdx_line_idx + i + 1}: expected {expected!r}, got {actual!r}",
                )

        return True, ""

    def test_all_files_have_preamble(self):
        """Test that all dulwich Python files have the standard preamble."""
        python_files = self._get_dulwich_python_files()
        self.assertGreater(len(python_files), 0, "No Python files found in dulwich/")

        files_without_preamble = []

        for file_path, rel_path in python_files:
            # Convert to forward slashes for consistency
            rel_path_str = str(rel_path).replace(os.sep, "/")

            # Skip exceptions
            if rel_path_str in PREAMBLE_EXCEPTIONS:
                continue

            has_preamble, error_msg = self._has_standard_preamble(file_path)
            if not has_preamble:
                files_without_preamble.append(f"{rel_path_str}: {error_msg}")

        if files_without_preamble:
            self.fail(
                "The following files are missing the standard preamble:\n"
                + "\n".join(f"  - {f}" for f in files_without_preamble)
            )

    def test_os_environ_usage_restricted(self):
        """Test that os.environ is only used in allowed files."""
        python_files = self._get_dulwich_python_files()
        self.assertGreater(len(python_files), 0, "No Python files found in dulwich/")

        # Files allowed to use os.environ
        allowed_files = {
            "dulwich/cli.py",
            "dulwich/porcelain/",
        }
        # Add exception files
        allowed_files.update(OS_ENVIRON_EXCEPTIONS)

        files_with_violations = []

        # Pattern to match os.environ usage
        os_environ_pattern = re.compile(r"\bos\.environ\b")

        for file_path, rel_path in python_files:
            # Convert to forward slashes for consistency
            rel_path_str = str(rel_path).replace(os.sep, "/")

            # Skip allowed files
            if any(rel_path_str.startswith(f) for f in allowed_files):
                continue

            with open(file_path, encoding="utf-8") as f:
                content = f.read()

            matches = os_environ_pattern.findall(content)
            if matches:
                # Count occurrences
                line_numbers = []
                for line_num, line in enumerate(content.split("\n"), 1):
                    if os_environ_pattern.search(line):
                        line_numbers.append(line_num)

                files_with_violations.append(
                    f"{rel_path_str}: os.environ used on line(s) {', '.join(map(str, line_numbers))}"
                )

        if files_with_violations:
            self.fail(
                "The following files use os.environ but are not in the allowed list:\n"
                + "\n".join(f"  - {f}" for f in files_with_violations)
                + "\n\nFiles allowed to use os.environ:\n"
                + "\n".join(f"  - {f}" for f in sorted(allowed_files))
            )
