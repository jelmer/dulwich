# test_check_ignore.py -- Compatibility tests for git check-ignore
# Copyright (C) 2025 Jelmer Vernooĳ <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

"""Compatibility tests for git check-ignore functionality."""

import os
import tempfile

from dulwich import porcelain
from dulwich.repo import Repo

from .utils import CompatTestCase, run_git_or_fail


class CheckIgnoreCompatTestCase(CompatTestCase):
    """Test git check-ignore compatibility between dulwich and git."""

    min_git_version = (1, 8, 5)  # git check-ignore was added in 1.8.5

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup_test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def _cleanup_test_dir(self) -> None:
        import shutil

        shutil.rmtree(self.test_dir)

    def _write_gitignore(self, content: str) -> None:
        """Write .gitignore file with given content."""
        gitignore_path = os.path.join(self.test_dir, ".gitignore")
        with open(gitignore_path, "w") as f:
            f.write(content)

    def _create_file(self, path: str, content: str = "") -> None:
        """Create a file with given content."""
        full_path = os.path.join(self.test_dir, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w") as f:
            f.write(content)

    def _create_dir(self, path: str) -> None:
        """Create a directory."""
        full_path = os.path.join(self.test_dir, path)
        os.makedirs(full_path, exist_ok=True)

    def _git_check_ignore(self, paths: list[str]) -> set[str]:
        """Run git check-ignore and return set of ignored paths."""
        try:
            output = run_git_or_fail(
                ["-c", "core.quotePath=false", "check-ignore", *paths],
                cwd=self.test_dir,
            )
            # git check-ignore returns paths separated by newlines
            return set(
                line.decode("utf-8") for line in output.strip().split(b"\n") if line
            )
        except AssertionError:
            # git check-ignore returns non-zero when no paths are ignored
            return set()

    def _dulwich_check_ignore(self, paths: list[str]) -> set[str]:
        """Run dulwich check_ignore and return set of ignored paths."""
        # Convert to absolute paths relative to the test directory
        abs_paths = [os.path.join(self.test_dir, path) for path in paths]
        ignored = set(
            porcelain.check_ignore(self.test_dir, abs_paths, quote_path=False)
        )
        # Convert back to relative paths and preserve original path format
        result = set()
        path_mapping = {}
        for orig_path, abs_path in zip(paths, abs_paths):
            path_mapping[abs_path] = orig_path

        for path in ignored:
            if path.startswith(self.test_dir + "/"):
                rel_path = path[len(self.test_dir) + 1 :]
                # Find the original path format that was requested
                orig_path = None
                for requested_path in paths:
                    if requested_path.rstrip("/") == rel_path.rstrip("/"):
                        orig_path = requested_path
                        break
                result.add(orig_path if orig_path else rel_path)
            else:
                result.add(path)
        return result

    def _assert_ignore_match(self, paths: list[str]) -> None:
        """Assert that dulwich and git return the same ignored paths."""
        git_ignored = self._git_check_ignore(paths)
        dulwich_ignored = self._dulwich_check_ignore(paths)
        self.assertEqual(
            git_ignored,
            dulwich_ignored,
            f"Mismatch for paths {paths}: git={git_ignored}, dulwich={dulwich_ignored}",
        )

    def test_issue_1203_directory_negation(self) -> None:
        """Test issue #1203: directory negation patterns with data/**,!data/*/."""
        self._write_gitignore("data/**\n!data/*/\n")
        self._create_file("data/test.dvc", "content")
        self._create_dir("data/subdir")

        # Based on dulwich's own test for issue #1203, the expected behavior is:
        # data/test.dvc: ignored, data/: not ignored, data/subdir/: not ignored
        # But git check-ignore might behave differently...

        # Test the core case that issue #1203 was about
        self._assert_ignore_match(["data/test.dvc"])

    def test_basic_patterns(self) -> None:
        """Test basic gitignore patterns."""
        self._write_gitignore("*.tmp\n*.log\n")
        self._create_file("test.tmp")
        self._create_file("debug.log")
        self._create_file("readme.txt")

        paths = ["test.tmp", "debug.log", "readme.txt"]
        self._assert_ignore_match(paths)

    def test_directory_patterns(self) -> None:
        """Test directory-specific patterns."""
        self._write_gitignore("build/\nnode_modules/\n")
        self._create_dir("build")
        self._create_dir("node_modules")
        self._create_file("build.txt")

        paths = ["build/", "node_modules/", "build.txt"]
        self._assert_ignore_match(paths)

    def test_issue_972_directory_pattern_with_slash(self) -> None:
        """Test issue #972: /data/ pattern should match both 'data' and 'data/'."""
        self._write_gitignore("/data/\n")
        self._create_dir("data")
        self._create_file("data/file.txt")

        # Both 'data' and 'data/' should be matched by /data/ pattern
        paths = ["data", "data/", "data/file.txt"]
        self._assert_ignore_match(paths)

    def test_wildcard_patterns(self) -> None:
        """Test wildcard patterns."""
        self._write_gitignore("*.py[cod]\n__pycache__/\n*.so\n")
        self._create_file("test.pyc")
        self._create_file("test.pyo")
        self._create_file("test.pyd")
        self._create_file("test.py")
        self._create_dir("__pycache__")

        paths = ["test.pyc", "test.pyo", "test.pyd", "test.py", "__pycache__/"]
        self._assert_ignore_match(paths)

    def test_negation_patterns(self) -> None:
        """Test negation patterns with !."""
        self._write_gitignore("*.log\n!important.log\n")
        self._create_file("debug.log")
        self._create_file("error.log")
        self._create_file("important.log")

        paths = ["debug.log", "error.log", "important.log"]
        self._assert_ignore_match(paths)

    def test_double_asterisk_patterns(self) -> None:
        """Test double asterisk ** patterns."""
        self._write_gitignore("**/temp\nvendor/**/cache\n")
        self._create_file("temp")
        self._create_file("src/temp")
        self._create_file("deep/nested/temp")
        self._create_file("vendor/lib/cache")
        self._create_file("vendor/gem/deep/cache")

        paths = [
            "temp",
            "src/temp",
            "deep/nested/temp",
            "vendor/lib/cache",
            "vendor/gem/deep/cache",
        ]
        self._assert_ignore_match(paths)

    def test_subdirectory_gitignore(self) -> None:
        """Test .gitignore files in subdirectories."""
        # Root .gitignore
        self._write_gitignore("*.tmp\n")

        # Subdirectory .gitignore
        self._create_dir("subdir")
        subdir_gitignore = os.path.join(self.test_dir, "subdir", ".gitignore")
        with open(subdir_gitignore, "w") as f:
            f.write("*.local\n!important.local\n")

        self._create_file("test.tmp")
        self._create_file("subdir/test.tmp")
        self._create_file("subdir/config.local")
        self._create_file("subdir/important.local")

        paths = [
            "test.tmp",
            "subdir/test.tmp",
            "subdir/config.local",
            "subdir/important.local",
        ]
        self._assert_ignore_match(paths)

    def test_complex_directory_negation(self) -> None:
        """Test complex directory negation patterns."""
        self._write_gitignore("dist/\n!dist/assets/\ndist/assets/*.tmp\n")
        self._create_dir("dist/assets")
        self._create_file("dist/main.js")
        self._create_file("dist/assets/style.css")
        self._create_file("dist/assets/temp.tmp")

        paths = [
            "dist/",
            "dist/main.js",
            "dist/assets/",
            "dist/assets/style.css",
            "dist/assets/temp.tmp",
        ]
        self._assert_ignore_match(paths)

    def test_leading_slash_patterns(self) -> None:
        """Test patterns with leading slash."""
        self._write_gitignore("/root-only.txt\nsubdir/specific.txt\n")
        self._create_file("root-only.txt")
        self._create_file("deep/root-only.txt")  # Should not be ignored
        self._create_file("subdir/specific.txt")
        self._create_file("deep/subdir/specific.txt")  # Should also be ignored

        paths = [
            "root-only.txt",
            "deep/root-only.txt",
            "subdir/specific.txt",
            "deep/subdir/specific.txt",
        ]
        self._assert_ignore_match(paths)

    def test_empty_directory_edge_case(self) -> None:
        """Test edge case with empty directories."""
        self._write_gitignore("empty/\n!empty/keep\n")
        self._create_dir("empty")
        self._create_file("empty/keep", "keep this")

        paths = ["empty/", "empty/keep"]
        self._assert_ignore_match(paths)

    def test_nested_wildcard_negation(self) -> None:
        """Test nested wildcard patterns with negation."""
        self._write_gitignore("docs/**\n!docs/*/\n!docs/**/*.md\n")
        self._create_file("docs/readme.txt")  # Should be ignored
        self._create_file("docs/guide.md")  # Should not be ignored
        self._create_dir("docs/api")  # Should not be ignored
        self._create_file("docs/api/index.md")  # Should not be ignored
        self._create_file("docs/api/temp.txt")  # Should be ignored

        paths = [
            "docs/readme.txt",
            "docs/guide.md",
            "docs/api/",
            "docs/api/index.md",
            "docs/api/temp.txt",
        ]
        self._assert_ignore_match(paths)

    def test_case_sensitivity(self) -> None:
        """Test case sensitivity in patterns."""
        self._write_gitignore("*.TMP\nREADME\n")
        self._create_file("test.tmp")  # Lowercase
        self._create_file("test.TMP")  # Uppercase
        self._create_file("readme")  # Lowercase
        self._create_file("README")  # Uppercase

        paths = ["test.tmp", "test.TMP", "readme", "README"]
        self._assert_ignore_match(paths)

    def test_unicode_filenames(self) -> None:
        """Test unicode filenames in patterns."""
        try:
            self._write_gitignore("тест*\n*.测试\n")
            self._create_file("тест.txt")
            self._create_file("файл.测试")
            self._create_file("normal.txt")

            paths = ["тест.txt", "файл.测试", "normal.txt"]
            self._assert_ignore_match(paths)
        except (UnicodeEncodeError, OSError):
            # Skip test if filesystem doesn't support unicode
            self.skipTest("Filesystem doesn't support unicode filenames")

    def test_double_asterisk_edge_cases(self) -> None:
        """Test edge cases with ** patterns."""
        self._write_gitignore("**/afile\ndir1/**/b\n**/*.tmp\n")

        # Test **/afile pattern
        self._create_file("afile")  # Root level
        self._create_file("dir/afile")  # One level deep
        self._create_file("deep/nested/afile")  # Multiple levels deep

        # Test dir1/**/b pattern
        self._create_file("dir1/b")  # Direct child
        self._create_file("dir1/subdir/b")  # One level deep in dir1/
        self._create_file("dir1/deep/nested/b")  # Multiple levels deep in dir1/
        self._create_file("other/dir1/b")  # Should not match (dir1/ not at start)

        # Test **/*.tmp pattern
        self._create_file("test.tmp")  # Root level
        self._create_file("dir/test.tmp")  # One level deep
        self._create_file("deep/nested/test.tmp")  # Multiple levels deep

        paths = [
            "afile",
            "dir/afile",
            "deep/nested/afile",
            "dir1/b",
            "dir1/subdir/b",
            "dir1/deep/nested/b",
            "other/dir1/b",
            "test.tmp",
            "dir/test.tmp",
            "deep/nested/test.tmp",
        ]
        self._assert_ignore_match(paths)

    def test_double_asterisk_with_negation(self) -> None:
        """Test ** patterns combined with negation."""
        self._write_gitignore(
            "**/build/**\n!**/build/assets/**\n**/build/assets/*.tmp\n"
        )

        # Create build directories at different levels
        self._create_file("build/main.js")
        self._create_file("build/assets/style.css")
        self._create_file("build/assets/temp.tmp")
        self._create_file("src/build/app.js")
        self._create_file("src/build/assets/logo.png")
        self._create_file("src/build/assets/cache.tmp")
        self._create_file("deep/nested/build/lib.js")
        self._create_file("deep/nested/build/assets/icon.svg")
        self._create_file("deep/nested/build/assets/debug.tmp")

        paths = [
            "build/main.js",
            "build/assets/style.css",
            "build/assets/temp.tmp",
            "src/build/app.js",
            "src/build/assets/logo.png",
            "src/build/assets/cache.tmp",
            "deep/nested/build/lib.js",
            "deep/nested/build/assets/icon.svg",
            "deep/nested/build/assets/debug.tmp",
        ]
        self._assert_ignore_match(paths)

    def test_double_asterisk_middle_patterns(self) -> None:
        """Test ** patterns in the middle of paths."""
        self._write_gitignore("src/**/test/**\nlib/**/node_modules\n**/cache/**/temp\n")

        # Test src/**/test/** pattern
        self._create_file("src/test/unit.js")
        self._create_file("src/components/test/unit.js")
        self._create_file("src/deep/nested/test/integration.js")
        self._create_file("other/src/test/unit.js")  # Should not match

        # Test lib/**/node_modules pattern
        self._create_file("lib/node_modules/package.json")
        self._create_file("lib/vendor/node_modules/package.json")
        self._create_file("lib/deep/path/node_modules/package.json")
        self._create_file("other/lib/node_modules/package.json")  # Should not match

        # Test **/cache/**/temp pattern
        self._create_file("cache/temp")
        self._create_file("cache/data/temp")
        self._create_file("app/cache/temp")
        self._create_file("app/cache/nested/temp")
        self._create_file("deep/cache/very/nested/temp")

        paths = [
            "src/test/unit.js",
            "src/components/test/unit.js",
            "src/deep/nested/test/integration.js",
            "other/src/test/unit.js",
            "lib/node_modules/package.json",
            "lib/vendor/node_modules/package.json",
            "lib/deep/path/node_modules/package.json",
            "other/lib/node_modules/package.json",
            "cache/temp",
            "cache/data/temp",
            "app/cache/temp",
            "app/cache/nested/temp",
            "deep/cache/very/nested/temp",
        ]
        self._assert_ignore_match(paths)

    def test_multiple_double_asterisks(self) -> None:
        """Test patterns with multiple ** segments."""
        self._write_gitignore("**/**/test/**/*.js\n**/src/**/build/**/dist\n")

        # Test **/**/test/**/*.js pattern (multiple ** in one pattern)
        self._create_file("test/file.js")
        self._create_file("a/test/file.js")
        self._create_file("a/b/test/file.js")
        self._create_file("test/c/file.js")
        self._create_file("test/c/d/file.js")
        self._create_file("a/b/test/c/d/file.js")
        self._create_file("a/b/test/c/d/file.txt")  # Different extension

        # Test **/src/**/build/**/dist pattern
        self._create_file("src/build/dist")
        self._create_file("app/src/build/dist")
        self._create_file("src/lib/build/dist")
        self._create_file("src/build/prod/dist")
        self._create_file("app/src/lib/build/prod/dist")

        paths = [
            "test/file.js",
            "a/test/file.js",
            "a/b/test/file.js",
            "test/c/file.js",
            "test/c/d/file.js",
            "a/b/test/c/d/file.js",
            "a/b/test/c/d/file.txt",
            "src/build/dist",
            "app/src/build/dist",
            "src/lib/build/dist",
            "src/build/prod/dist",
            "app/src/lib/build/prod/dist",
        ]
        self._assert_ignore_match(paths)

    def test_double_asterisk_directory_traversal(self) -> None:
        """Test ** patterns with directory traversal edge cases."""
        self._write_gitignore("**/.*\n!**/.gitkeep\n**/.git/**\n")

        # Test **/.*  pattern (hidden files at any level)
        self._create_file(".hidden")
        self._create_file("dir/.hidden")
        self._create_file("deep/nested/.hidden")
        self._create_file(".gitkeep")  # Should be negated
        self._create_file("dir/.gitkeep")  # Should be negated

        # Test **/.git/** pattern
        self._create_file(".git/config")
        self._create_file(".git/objects/abc123")
        self._create_file("submodule/.git/config")
        self._create_file("deep/submodule/.git/refs/heads/master")

        paths = [
            ".hidden",
            "dir/.hidden",
            "deep/nested/.hidden",
            ".gitkeep",
            "dir/.gitkeep",
            ".git/config",
            ".git/objects/abc123",
            "submodule/.git/config",
            "deep/submodule/.git/refs/heads/master",
        ]
        self._assert_ignore_match(paths)

    def test_double_asterisk_empty_segments(self) -> None:
        """Test ** patterns with edge cases around empty path segments."""
        self._write_gitignore("a/**//b\n**//**/test\nc/**/**/\n")

        # These patterns test edge cases with path separator handling
        self._create_file("a/b")
        self._create_file("a/x/b")
        self._create_file("a/x/y/b")
        self._create_file("test")
        self._create_file("dir/test")
        self._create_file("dir/nested/test")
        self._create_file("c/file")
        self._create_file("c/dir/file")
        self._create_file("c/deep/nested/file")

        paths = [
            "a/b",
            "a/x/b",
            "a/x/y/b",
            "test",
            "dir/test",
            "dir/nested/test",
            "c/file",
            "c/dir/file",
            "c/deep/nested/file",
        ]
        self._assert_ignore_match(paths)

    def test_double_asterisk_root_patterns(self) -> None:
        """Test ** patterns at repository root with complex negations."""
        self._write_gitignore("/**\n!/**/\n!/**/*.md\n/**/*.tmp\n")

        # Pattern explanation:
        # /**        - Ignore everything at any depth
        # !/**/      - But don't ignore directories
        # !/**/*.md  - And don't ignore .md files
        # /**/*.tmp  - But do ignore .tmp files (overrides .md negation for .tmp.md files)

        self._create_file("file.txt")
        self._create_file("readme.md")
        self._create_file("temp.tmp")
        self._create_file("backup.tmp.md")  # Edge case: both .tmp and .md
        self._create_dir("dir")
        self._create_file("dir/file.txt")
        self._create_file("dir/guide.md")
        self._create_file("dir/cache.tmp")
        self._create_file("deep/nested/doc.md")
        self._create_file("deep/nested/log.tmp")

        paths = [
            "file.txt",
            "readme.md",
            "temp.tmp",
            "backup.tmp.md",
            "dir/",
            "dir/file.txt",
            "dir/guide.md",
            "dir/cache.tmp",
            "deep/nested/doc.md",
            "deep/nested/log.tmp",
        ]
        self._assert_ignore_match(paths)

    def test_single_asterisk_patterns(self) -> None:
        """Test single asterisk * patterns in various positions."""
        self._write_gitignore("src/*/build\n*.log\ntest*/\n*_backup\nlib/*\n*/temp/*\n")

        # Test src/*/build pattern
        self._create_file("src/app/build")
        self._create_file("src/lib/build")
        self._create_file("src/nested/deep/build")  # Should not match (only one level)
        self._create_file("other/src/app/build")  # Should not match

        # Test *.log pattern
        self._create_file("app.log")
        self._create_file("error.log")
        self._create_file("logs/debug.log")  # Should match
        self._create_file("app.log.old")  # Should not match

        # Test test*/ pattern (directories starting with test)
        self._create_dir("test")
        self._create_dir("testing")
        self._create_dir("test_data")
        self._create_file("test_file")  # Should not match (not a directory)

        # Test *_backup pattern
        self._create_file("db_backup")
        self._create_file("config_backup")
        self._create_file("old_backup_file")  # Should not match (backup not at end)

        # Test lib/* pattern
        self._create_file("lib/module.js")
        self._create_file("lib/utils.py")
        self._create_file("lib/nested/deep.js")  # Should not match (only one level)

        # Test */temp/* pattern
        self._create_file("app/temp/cache")
        self._create_file("src/temp/logs")
        self._create_file("deep/nested/temp/file")  # Should not match (nested too deep)
        self._create_file("temp/file")  # Should not match (temp at root)

        paths = [
            "src/app/build",
            "src/lib/build",
            "src/nested/deep/build",
            "other/src/app/build",
            "app.log",
            "error.log",
            "logs/debug.log",
            "app.log.old",
            "test/",
            "testing/",
            "test_data/",
            "test_file",
            "db_backup",
            "config_backup",
            "old_backup_file",
            "lib/module.js",
            "lib/utils.py",
            "lib/nested/deep.js",
            "app/temp/cache",
            "src/temp/logs",
            "deep/nested/temp/file",
            "temp/file",
        ]
        self._assert_ignore_match(paths)

    def test_single_asterisk_edge_cases(self) -> None:
        """Test edge cases with single asterisk patterns."""
        self._write_gitignore("*\n!*/\n!*.txt\n*.*.*\n")

        # Pattern explanation:
        # *      - Ignore everything
        # !*/    - But don't ignore directories
        # !*.txt - And don't ignore .txt files
        # *.*.*  - But ignore files with multiple dots

        self._create_file("file")
        self._create_file("readme.txt")
        self._create_file("config.json")
        self._create_file("archive.tar.gz")  # Multiple dots
        self._create_file("backup.sql.old")  # Multiple dots
        self._create_dir("folder")
        self._create_file("folder/nested.txt")
        self._create_file("folder/data.json")

        paths = [
            "file",
            "readme.txt",
            "config.json",
            "archive.tar.gz",
            "backup.sql.old",
            "folder/",
            "folder/nested.txt",
            "folder/data.json",
        ]
        self._assert_ignore_match(paths)

    def test_single_asterisk_with_character_classes(self) -> None:
        """Test single asterisk with character classes and special patterns."""
        self._write_gitignore("*.[oa]\n*~\n.*\n!.gitignore\n[Tt]emp*\n")

        # Test *.[oa] pattern (object and archive files)
        self._create_file("main.o")
        self._create_file("lib.a")
        self._create_file("app.so")  # Should not match
        self._create_file("test.c")  # Should not match

        # Test *~ pattern (backup files)
        self._create_file("file~")
        self._create_file("config~")
        self._create_file("~file")  # Should not match (~ at start)

        # Test .* pattern with negation
        self._create_file(".hidden")
        self._create_file(".secret")
        self._create_file(".gitignore")  # Should be negated

        # Test [Tt]emp* pattern (case variations)
        self._create_file("temp_file")
        self._create_file("Temp_data")
        self._create_file("TEMP_LOG")  # Should not match (not T or t)
        self._create_file("temporary")

        paths = [
            "main.o",
            "lib.a",
            "app.so",
            "test.c",
            "file~",
            "config~",
            "~file",
            ".hidden",
            ".secret",
            ".gitignore",
            "temp_file",
            "Temp_data",
            "TEMP_LOG",
            "temporary",
        ]
        self._assert_ignore_match(paths)

    def test_mixed_single_double_asterisk_patterns(self) -> None:
        """Test patterns that mix single (*) and double (**) asterisks."""
        self._write_gitignore(
            "src/**/test/*.js\n**/build/*\n*/cache/**\nlib/*/vendor/**/*.min.*\n"
        )

        # Test src/**/test/*.js - double asterisk in middle, single at end
        self._create_file("src/test/unit.js")
        self._create_file("src/components/test/spec.js")
        self._create_file("src/deep/nested/test/integration.js")
        self._create_file(
            "src/test/nested/unit.js"
        )  # Should not match (nested after test)
        self._create_file(
            "src/components/test/unit.ts"
        )  # Should not match (wrong extension)

        # Test **/build/* - double asterisk at start, single at end
        self._create_file("build/app.js")
        self._create_file("src/build/main.js")
        self._create_file("deep/nested/build/lib.js")
        self._create_file("build/dist/app.js")  # Should not match (nested after build)

        # Test */cache/** - single at start, double at end
        self._create_file("app/cache/temp")
        self._create_file("src/cache/data/file")
        self._create_file("lib/cache/deep/nested/item")
        self._create_file(
            "nested/deep/cache/file"
        )  # Should not match (cache not at second level)
        self._create_file("cache/file")  # Should not match (cache at root)

        # Test lib/*/vendor/**/*.min.* - complex mixed pattern
        self._create_file("lib/app/vendor/jquery.min.js")
        self._create_file("lib/ui/vendor/bootstrap.min.css")
        self._create_file("lib/core/vendor/deep/nested/lib.min.map")
        self._create_file("lib/app/vendor/jquery.js")  # Should not match (not .min.)
        self._create_file(
            "lib/nested/deep/vendor/lib.min.js"
        )  # Should not match (too deep before vendor)

        paths = [
            "src/test/unit.js",
            "src/components/test/spec.js",
            "src/deep/nested/test/integration.js",
            "src/test/nested/unit.js",
            "src/components/test/unit.ts",
            "build/app.js",
            "src/build/main.js",
            "deep/nested/build/lib.js",
            "build/dist/app.js",
            "app/cache/temp",
            "src/cache/data/file",
            "lib/cache/deep/nested/item",
            "nested/deep/cache/file",
            "cache/file",
            "lib/app/vendor/jquery.min.js",
            "lib/ui/vendor/bootstrap.min.css",
            "lib/core/vendor/deep/nested/lib.min.map",
            "lib/app/vendor/jquery.js",
            "lib/nested/deep/vendor/lib.min.js",
        ]
        self._assert_ignore_match(paths)

    def test_asterisk_pattern_overlaps(self) -> None:
        """Test overlapping single and double asterisk patterns with negations."""
        self._write_gitignore(
            "**/*.tmp\n!src/**/*.tmp\nsrc/*/cache/*.tmp\n**/test/*\n!**/test/*.spec.*\n"
        )

        # Pattern explanation:
        # **/*.tmp - Ignore all .tmp files anywhere
        # !src/**/*.tmp - But don't ignore .tmp files under src/
        # src/*/cache/*.tmp - But do ignore .tmp files in src/*/cache/ (overrides negation)
        # **/test/* - Ignore everything directly in test directories
        # !**/test/*.spec.* - But don't ignore spec files in test directories

        # Test tmp file patterns with src/ negation
        self._create_file("temp.tmp")  # Should be ignored
        self._create_file("build/cache.tmp")  # Should be ignored
        self._create_file("src/app.tmp")  # Should not be ignored (src negation)
        self._create_file("src/lib/utils.tmp")  # Should not be ignored (src negation)
        self._create_file(
            "src/app/cache/data.tmp"
        )  # Should be ignored (cache override)
        self._create_file(
            "src/lib/cache/temp.tmp"
        )  # Should be ignored (cache override)

        # Test test directory patterns with spec negation
        self._create_file("test/unit.js")  # Should be ignored
        self._create_file("src/test/helper.js")  # Should be ignored
        self._create_file("test/app.spec.js")  # Should not be ignored (spec negation)
        self._create_file(
            "src/test/lib.spec.ts"
        )  # Should not be ignored (spec negation)
        self._create_file(
            "test/nested/file.js"
        )  # Should not be ignored (not direct child)

        paths = [
            "temp.tmp",
            "build/cache.tmp",
            "src/app.tmp",
            "src/lib/utils.tmp",
            "src/app/cache/data.tmp",
            "src/lib/cache/temp.tmp",
            "test/unit.js",
            "src/test/helper.js",
            "test/app.spec.js",
            "src/test/lib.spec.ts",
            "test/nested/file.js",
        ]
        self._assert_ignore_match(paths)

    def test_asterisk_boundary_conditions(self) -> None:
        """Test boundary conditions between single and double asterisk patterns."""
        self._write_gitignore("a/**/b/*\nc/**/**/d\n*/e/**/*\nf/*/g/**\n")

        # Test a/**/b/* - ** in middle, * at end
        self._create_file("a/b/file")  # Direct path
        self._create_file("a/x/b/file")  # One level between a and b
        self._create_file("a/x/y/b/file")  # Multiple levels between a and b
        self._create_file("a/b/nested/file")  # Should not match (nested after b)

        # Test c/**/**/d - multiple ** separated by single level
        self._create_file("c/d")  # Minimal match
        self._create_file("c/x/d")  # One level before d
        self._create_file("c/x/y/d")  # Multiple levels before d
        self._create_file("c/x/y/z/d")  # Even more levels

        # Test */e/**/* - * at start, ** in middle, * at end
        self._create_file("a/e/file")  # Minimal match
        self._create_file("x/e/nested/file")  # Nested after e
        self._create_file("y/e/deep/nested/file")  # Deep nesting after e
        self._create_file(
            "nested/path/e/file"
        )  # Should not match (path before e too deep)

        # Test f/*/g/** - * in middle, ** at end
        self._create_file("f/x/g/file")  # Basic match
        self._create_file("f/y/g/nested/file")  # Nested after g
        self._create_file("f/z/g/deep/nested/file")  # Deep nesting after g
        self._create_file(
            "f/nested/path/g/file"
        )  # Should not match (path between f and g too deep)

        paths = [
            "a/b/file",
            "a/x/b/file",
            "a/x/y/b/file",
            "a/b/nested/file",
            "c/d",
            "c/x/d",
            "c/x/y/d",
            "c/x/y/z/d",
            "a/e/file",
            "x/e/nested/file",
            "y/e/deep/nested/file",
            "nested/path/e/file",
            "f/x/g/file",
            "f/y/g/nested/file",
            "f/z/g/deep/nested/file",
            "f/nested/path/g/file",
        ]
        self._assert_ignore_match(paths)

    def test_asterisk_edge_case_combinations(self) -> None:
        """Test really tricky edge cases with asterisk combinations."""
        self._write_gitignore("***\n**/*\n*/**\n*/*/\n**/*/*\n*/*/**\n")

        # Test *** pattern (should behave like **)
        self._create_file("file1")
        self._create_file("dir/file2")
        self._create_file("deep/nested/file3")

        # Test **/* pattern (anything with at least one path segment)
        self._create_file("path1/item1")
        self._create_file("path2/sub/item2")

        # Test */** pattern (anything under a single-level directory)
        self._create_file("single/file4")
        self._create_file("single/nested/deep")

        # Test */*/ pattern (directories exactly two levels deep)
        self._create_dir("level1/level2")
        self._create_dir("dir1/dir2")
        self._create_dir("path3/sub1/sub2")  # Should not match (too deep)

        # Test **/*/* pattern (at least two path segments after any prefix)
        self._create_file("test1/test2/test3")
        self._create_file("deep/nested/item3/item4")
        self._create_file(
            "simple/item"
        )  # Should not match (only one segment after any prefix at root)

        # Test */*/** pattern (single/single/anything)
        self._create_file("part1/part2/anything")
        self._create_file("seg1/seg2/deep/nested")

        paths = [
            "file1",
            "dir/file2",
            "deep/nested/file3",
            "path1/item1",
            "path2/sub/item2",
            "single/file4",
            "single/nested/deep",
            "level1/level2/",
            "dir1/dir2/",
            "path3/sub1/sub2/",
            "test1/test2/test3",
            "deep/nested/item3/item4",
            "simple/item",
            "part1/part2/anything",
            "seg1/seg2/deep/nested",
        ]
        self._assert_ignore_match(paths)

    def test_asterisk_consecutive_patterns(self) -> None:
        """Test patterns with consecutive asterisks and weird spacing."""
        self._write_gitignore("a*/b*\n*x*y*\n**z**\n**/.*/**\n*.*./*\n")

        # Test a*/b* pattern
        self._create_file("a/b")  # Minimal match
        self._create_file("app/build")  # Both have suffixes
        self._create_file("api/backup")  # Both have suffixes
        self._create_file("a/build")  # a exact, b with suffix
        self._create_file("app/b")  # a with suffix, b exact
        self._create_file("x/a/b")  # Should not match (a not at start)

        # Test *x*y* pattern
        self._create_file("xy")  # Minimal
        self._create_file("axby")  # x and y in middle
        self._create_file("prefixsuffyend")  # x and y with text around
        self._create_file("xyz")  # Should not match (no y after x)
        self._create_file("axy")  # x and y consecutive

        # Test **z** pattern
        self._create_file("z")  # Just z
        self._create_file("az")  # z at end
        self._create_file("za")  # z at start
        self._create_file("aza")  # z in middle
        self._create_file("dir/z")  # z at any depth
        self._create_file("deep/nested/prefix_z_suffix")  # z anywhere in name

        # Test **/.*/** pattern (hidden files in any directory structure)
        self._create_file("dir/.hidden/file")
        self._create_file("deep/nested/.secret/data")
        self._create_file(".visible/file")  # At root level
        self._create_file("other/.config")  # Should not match (no trailing path)

        # Test *.*./* pattern (files with dots in specific structure)
        self._create_file("app.min.js/file")  # Two dots, then directory
        self._create_file("lib.bundle.css/asset")  # Two dots, then directory
        self._create_file("simple.js")  # Should not match (only one dot, no directory)
        self._create_file("no.dots.here")  # Should not match (no trailing directory)

        paths = [
            "a/b",
            "app/build",
            "api/backup",
            "a/build",
            "app/b",
            "x/a/b",
            "xy",
            "axby",
            "prefixsuffyend",
            "xyz",
            "axy",
            "z",
            "az",
            "za",
            "aza",
            "dir/z",
            "deep/nested/prefix_z_suffix",
            "dir/.hidden/file",
            "deep/nested/.secret/data",
            ".visible/file",
            "other/.config",
            "app.min.js/file",
            "lib.bundle.css/asset",
            "simple.js",
            "no.dots.here",
        ]
        self._assert_ignore_match(paths)

    def test_asterisk_escaping_and_special_chars(self) -> None:
        """Test asterisk patterns with special characters and potential escaping."""
        self._write_gitignore(
            "\\*literal\n**/*.\\*\n[*]bracket\n*\\[escape\\]\n*.{tmp,log}\n"
        )

        # Test \*literal pattern (literal asterisk)
        self._create_file("*literal")  # Literal asterisk at start
        self._create_file("xliteral")  # Should not match (no literal asterisk)
        self._create_file("prefix*literal")  # Literal asterisk in middle

        # Test **/*.* pattern (files with .* extension)
        self._create_file("file.*")  # Literal .* extension
        self._create_file("dir/test.*")  # At any depth
        self._create_file("file.txt")  # Should not match (not .* extension)

        # Test [*]bracket pattern (bracket containing asterisk)
        self._create_file("*bracket")  # Literal asterisk from bracket
        self._create_file("xbracket")  # Should not match
        self._create_file("abracket")  # Should not match

        # Test *\[escape\] pattern (literal brackets)
        self._create_file("test[escape]")  # Literal brackets
        self._create_file("prefix[escape]")  # With prefix
        self._create_file("test[other]")  # Should not match (wrong brackets)

        # Test *.{tmp,log} pattern (brace expansion - may not work in gitignore)
        self._create_file("file.{tmp,log}")  # Literal braces
        self._create_file("test.tmp")  # Might match if braces are expanded
        self._create_file("test.log")  # Might match if braces are expanded
        self._create_file("test.{other}")  # Should not match

        paths = [
            "*literal",
            "xliteral",
            "prefix*literal",
            "file.*",
            "dir/test.*",
            "file.txt",
            "*bracket",
            "xbracket",
            "abracket",
            "test[escape]",
            "prefix[escape]",
            "test[other]",
            "file.{tmp,log}",
            "test.tmp",
            "test.log",
            "test.{other}",
        ]
        self._assert_ignore_match(paths)

    def test_quote_path_true_unicode_filenames(self) -> None:
        """Test quote_path=True functionality with unicode filenames."""
        try:
            self._write_gitignore("тест*\n*.测试\n")
            self._create_file("тест.txt")
            self._create_file("файл.测试")
            self._create_file("normal.txt")

            paths = ["тест.txt", "файл.测试", "normal.txt"]

            # Test that dulwich with quote_path=True matches git's quoted output
            git_ignored = self._git_check_ignore_quoted(paths)
            dulwich_ignored = self._dulwich_check_ignore_quoted(paths)

            self.assertEqual(
                git_ignored,
                dulwich_ignored,
                f"Mismatch for quoted paths {paths}: git={git_ignored}, dulwich={dulwich_ignored}",
            )
        except (UnicodeEncodeError, OSError):
            # Skip test if filesystem doesn't support unicode
            self.skipTest("Filesystem doesn't support unicode filenames")

    def test_quote_path_consistency(self) -> None:
        """Test that quote_path=True and quote_path=False are consistent."""
        try:
            self._write_gitignore("тест*\n*.测试\nmixed_тест*\n")
            self._create_file("тест.txt")
            self._create_file("файл.测试")
            self._create_file("normal.txt")
            self._create_file("mixed_тест.log")

            paths = ["тест.txt", "файл.测试", "normal.txt", "mixed_тест.log"]

            # Get both quoted and unquoted results from dulwich
            quoted_ignored = self._dulwich_check_ignore_quoted(paths)
            unquoted_ignored = self._dulwich_check_ignore(paths)

            # Verify that the number of ignored files is the same
            self.assertEqual(
                len(quoted_ignored),
                len(unquoted_ignored),
                "Quote path setting should not change which files are ignored",
            )

            # Verify quoted paths contain the expected files
            expected_quoted = {
                '"\\321\\202\\320\\265\\321\\201\\321\\202.txt"',
                '"\\321\\204\\320\\260\\320\\271\\320\\273.\\346\\265\\213\\350\\257\\225"',
                '"mixed_\\321\\202\\320\\265\\321\\201\\321\\202.log"',
            }
            self.assertEqual(quoted_ignored, expected_quoted)

            # Verify unquoted paths contain the expected files
            expected_unquoted = {"тест.txt", "файл.测试", "mixed_тест.log"}
            self.assertEqual(unquoted_ignored, expected_unquoted)

        except (UnicodeEncodeError, OSError):
            # Skip test if filesystem doesn't support unicode
            self.skipTest("Filesystem doesn't support unicode filenames")

    def _git_check_ignore_quoted(self, paths: list[str]) -> set[str]:
        """Run git check-ignore with default quoting and return set of ignored paths."""
        try:
            # Use default git settings (core.quotePath=true by default)
            output = run_git_or_fail(
                ["check-ignore", *paths],
                cwd=self.test_dir,
            )
            # git check-ignore returns paths separated by newlines
            return set(
                line.decode("utf-8") for line in output.strip().split(b"\n") if line
            )
        except AssertionError:
            # git check-ignore returns non-zero when no paths are ignored
            return set()

    def _dulwich_check_ignore_quoted(self, paths: list[str]) -> set[str]:
        """Run dulwich check_ignore with quote_path=True and return set of ignored paths."""
        # Convert to absolute paths relative to the test directory
        abs_paths = [os.path.join(self.test_dir, path) for path in paths]
        ignored = set(porcelain.check_ignore(self.test_dir, abs_paths, quote_path=True))
        # Convert back to relative paths and preserve original path format
        result = set()
        path_mapping = {}
        for orig_path, abs_path in zip(paths, abs_paths):
            path_mapping[abs_path] = orig_path

        for path in ignored:
            if path.startswith(self.test_dir + "/"):
                rel_path = path[len(self.test_dir) + 1 :]
                # Find the original path format that was requested
                orig_path = None
                for requested_path in paths:
                    if requested_path.rstrip("/") == rel_path.rstrip("/"):
                        orig_path = requested_path
                        break
                result.add(orig_path if orig_path else rel_path)
            else:
                result.add(path)
        return result
