# test_ignore.py -- tests for porcelain ignore (check_ignore)
# Copyright (C) 2017 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for porcelain ignore (check_ignore) functions."""

import os
import shutil
import tempfile

from dulwich.porcelain import _quote_path, check_ignore
from dulwich.repo import Repo

from .. import TestCase


class CheckIgnoreQuotePathTests(TestCase):
    """Integration tests for check_ignore with quote_path parameter."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)

    def test_quote_path_true_unicode_filenames(self) -> None:
        """Test that quote_path=True returns quoted unicode filenames."""
        # Create a repository
        repo = Repo.init(self.test_dir)
        self.addCleanup(repo.close)

        # Create .gitignore with unicode patterns
        gitignore_path = os.path.join(self.test_dir, ".gitignore")
        with open(gitignore_path, "w", encoding="utf-8") as f:
            f.write("тест*\n")
            f.write("*.测试\n")

        # Create unicode files
        test_files = ["тест.txt", "файл.测试", "normal.txt"]
        for filename in test_files:
            filepath = os.path.join(self.test_dir, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write("test content")

        # Test with quote_path=True (default)
        abs_paths = [os.path.join(self.test_dir, f) for f in test_files]
        ignored_quoted = set(check_ignore(self.test_dir, abs_paths, quote_path=True))

        # Test with quote_path=False
        ignored_unquoted = set(check_ignore(self.test_dir, abs_paths, quote_path=False))

        # Verify quoted results
        expected_quoted = {
            '"\\321\\202\\320\\265\\321\\201\\321\\202.txt"',  # тест.txt
            '"\\321\\204\\320\\260\\320\\271\\320\\273.\\346\\265\\213\\350\\257\\225"',  # файл.测试
        }
        self.assertEqual(ignored_quoted, expected_quoted)

        # Verify unquoted results
        expected_unquoted = {"тест.txt", "файл.测试"}
        self.assertEqual(ignored_unquoted, expected_unquoted)

    def test_quote_path_ascii_filenames(self) -> None:
        """Test that ASCII filenames are unaffected by quote_path setting."""
        # Create a repository
        repo = Repo.init(self.test_dir)
        self.addCleanup(repo.close)

        # Create .gitignore
        gitignore_path = os.path.join(self.test_dir, ".gitignore")
        with open(gitignore_path, "w") as f:
            f.write("*.tmp\n")
            f.write("test*\n")

        # Create ASCII files
        test_files = ["test.txt", "file.tmp", "normal.txt"]
        for filename in test_files:
            filepath = os.path.join(self.test_dir, filename)
            with open(filepath, "w") as f:
                f.write("test content")

        # Test both settings
        abs_paths = [os.path.join(self.test_dir, f) for f in test_files]
        ignored_quoted = set(check_ignore(self.test_dir, abs_paths, quote_path=True))
        ignored_unquoted = set(check_ignore(self.test_dir, abs_paths, quote_path=False))

        # Both should return the same results for ASCII filenames
        expected = {"test.txt", "file.tmp"}
        self.assertEqual(ignored_quoted, expected)
        self.assertEqual(ignored_unquoted, expected)


class QuotePathTests(TestCase):
    """Tests for _quote_path function."""

    def test_ascii_paths(self) -> None:
        """Test that ASCII paths are not quoted."""
        self.assertEqual(_quote_path("file.txt"), "file.txt")
        self.assertEqual(_quote_path("dir/file.txt"), "dir/file.txt")
        self.assertEqual(_quote_path("path with spaces.txt"), "path with spaces.txt")

    def test_unicode_paths(self) -> None:
        """Test that unicode paths are quoted with C-style escapes."""
        # Russian characters
        self.assertEqual(
            _quote_path("тест.txt"), '"\\321\\202\\320\\265\\321\\201\\321\\202.txt"'
        )
        # Chinese characters
        self.assertEqual(
            _quote_path("файл.测试"),
            '"\\321\\204\\320\\260\\320\\271\\320\\273.\\346\\265\\213\\350\\257\\225"',
        )
        # Mixed ASCII and unicode
        self.assertEqual(
            _quote_path("test-тест.txt"),
            '"test-\\321\\202\\320\\265\\321\\201\\321\\202.txt"',
        )

    def test_special_characters(self) -> None:
        """Test that special characters are properly escaped."""
        # Quotes in filename
        self.assertEqual(
            _quote_path('file"with"quotes.txt'), '"file\\"with\\"quotes.txt"'
        )
        # Backslashes in filename
        self.assertEqual(
            _quote_path("file\\with\\backslashes.txt"),
            '"file\\\\with\\\\backslashes.txt"',
        )
        # Mixed special chars and unicode
        self.assertEqual(
            _quote_path('тест"файл.txt'),
            '"\\321\\202\\320\\265\\321\\201\\321\\202\\"\\321\\204\\320\\260\\320\\271\\320\\273.txt"',
        )

    def test_empty_and_edge_cases(self) -> None:
        """Test edge cases."""
        self.assertEqual(_quote_path(""), "")
        self.assertEqual(_quote_path("a"), "a")  # Single ASCII char
        self.assertEqual(_quote_path("я"), '"\\321\\217"')  # Single unicode char
