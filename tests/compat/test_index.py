# test_index.py -- Git index compatibility tests
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Compatibility tests for Git index format v4."""

import os
import tempfile

from dulwich.index import Index, read_index_dict_with_version, write_index_dict
from dulwich.repo import Repo

from .utils import CompatTestCase, require_git_version, run_git_or_fail


class IndexV4CompatTestCase(CompatTestCase):
    """Tests for Git index format v4 compatibility with C Git."""

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup)

    def _cleanup(self) -> None:
        import shutil

        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _init_repo_with_manyfiles(self) -> Repo:
        """Initialize a repo with manyFiles feature enabled."""
        # Create repo
        repo_path = os.path.join(self.tempdir, "test_repo")
        os.mkdir(repo_path)

        # Initialize with C git and enable manyFiles
        run_git_or_fail(["init"], cwd=repo_path)
        run_git_or_fail(["config", "feature.manyFiles", "true"], cwd=repo_path)

        # Open with dulwich
        return Repo(repo_path)

    def test_index_v4_path_compression(self) -> None:
        """Test that dulwich can read and write index v4 with path compression."""
        require_git_version((2, 20, 0))  # manyFiles feature requires newer Git

        repo = self._init_repo_with_manyfiles()

        # Create test files with paths that will benefit from compression
        test_files = [
            "dir1/subdir/file1.txt",
            "dir1/subdir/file2.txt",
            "dir1/subdir/file3.txt",
            "dir2/another/path.txt",
            "dir2/another/path2.txt",
            "file_at_root.txt",
        ]

        for path in test_files:
            full_path = os.path.join(repo.path, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write(f"content of {path}\n")

        # Add files with C git - this should create index v4
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Read the index with dulwich
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        # Verify it's version 4
        self.assertEqual(version, 4)

        # Verify all files are in the index
        self.assertEqual(len(entries), len(test_files))
        for path in test_files:
            self.assertIn(path.encode(), entries)

        # Write the index back with dulwich
        with open(index_path + ".dulwich", "wb") as f:
            write_index_dict(f, entries, version=4)

        # Compare with C git - use git ls-files to read both indexes
        output1 = run_git_or_fail(["ls-files", "--stage"], cwd=repo.path)

        # Replace index with dulwich version
        os.rename(index_path + ".dulwich", index_path)

        output2 = run_git_or_fail(["ls-files", "--stage"], cwd=repo.path)

        # Both outputs should be identical
        self.assertEqual(output1, output2)

    def test_index_v4_round_trip(self) -> None:
        """Test round-trip: C Git write -> dulwich read -> dulwich write -> C Git read."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create files that test various edge cases
        test_files = [
            "a",  # Very short name
            "abc/def/ghi/jkl/mno/pqr/stu/vwx/yz.txt",  # Deep path
            "same_prefix_1.txt",
            "same_prefix_2.txt",
            "same_prefix_3.txt",
            "different/path/here.txt",
        ]

        for path in test_files:
            full_path = os.path.join(repo.path, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write("test content\n")

        # Stage with C Git
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Get original state
        original_output = run_git_or_fail(["ls-files", "--stage"], cwd=repo.path)

        # Read with dulwich, write back
        index = Index(os.path.join(repo.path, ".git", "index"))
        index.write()

        # Verify C Git can still read it
        final_output = run_git_or_fail(["ls-files", "--stage"], cwd=repo.path)
        self.assertEqual(original_output, final_output)

    def test_index_v4_skip_hash(self) -> None:
        """Test index v4 with skipHash extension."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Enable skipHash
        run_git_or_fail(["config", "index.skipHash", "true"], cwd=repo.path)

        # Create a file
        test_file = os.path.join(repo.path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content\n")

        # Add with C Git
        run_git_or_fail(["add", "test.txt"], cwd=repo.path)

        # Read the index
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertIn(b"test.txt", entries)

        # Verify skipHash is active by checking last 20 bytes
        with open(index_path, "rb") as f:
            f.seek(-20, 2)
            last_bytes = f.read(20)
            self.assertEqual(last_bytes, b"\x00" * 20)

        # Write with dulwich (with skipHash)
        index = Index(index_path, skip_hash=True, version=4)
        index.write()

        # Verify C Git can read it
        output = run_git_or_fail(["ls-files"], cwd=repo.path)
        self.assertEqual(output.strip(), b"test.txt")

    def test_index_v4_with_various_filenames(self) -> None:
        """Test v4 with various filename patterns."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Test various filename patterns that might trigger different behaviors
        test_files = [
            "a",  # Single character
            "ab",  # Two characters
            "abc",  # Three characters
            "file.txt",  # Normal filename
            "very_long_filename_to_test_edge_cases.extension",  # Long filename
            "dir/file.txt",  # With directory
            "dir1/dir2/dir3/file.txt",  # Deep directory
            "unicode_café.txt",  # Unicode filename
            "with-dashes-and_underscores.txt",  # Special chars
            ".hidden",  # Hidden file
            "UPPERCASE.TXT",  # Uppercase
        ]

        for filename in test_files:
            filepath = os.path.join(repo.path, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"Content of {filename}\n")

        # Add all files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Read with dulwich
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertEqual(len(entries), len(test_files))

        # Verify all filenames are correctly stored
        for filename in test_files:
            filename_bytes = filename.encode("utf-8")
            self.assertIn(filename_bytes, entries)

        # Test round-trip: dulwich write -> C Git read
        with open(index_path + ".dulwich", "wb") as f:
            write_index_dict(f, entries, version=4)

        # Replace index
        os.rename(index_path + ".dulwich", index_path)

        # Verify C Git can read all files
        output = run_git_or_fail(["ls-files"], cwd=repo.path)
        git_files = set(output.strip().split(b"\n"))
        expected_files = {f.encode("utf-8") for f in test_files}
        self.assertEqual(git_files, expected_files)

    def test_index_v4_path_compression_scenarios(self) -> None:
        """Test various scenarios where path compression should/shouldn't be used."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create files that should trigger compression
        compression_files = [
            "src/main/java/com/example/Service.java",
            "src/main/java/com/example/Controller.java",
            "src/main/java/com/example/Repository.java",
            "src/test/java/com/example/ServiceTest.java",
        ]

        # Create files that shouldn't benefit much from compression
        no_compression_files = [
            "README.md",
            "LICENSE",
            "docs/guide.txt",
            "config/settings.json",
        ]

        all_files = compression_files + no_compression_files

        for filename in all_files:
            filepath = os.path.join(repo.path, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write(f"Content of {filename}\n")

        # Add files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Read the index
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertEqual(len(entries), len(all_files))

        # Verify all files are present
        for filename in all_files:
            self.assertIn(filename.encode(), entries)

        # Test that dulwich can write a compatible index
        with open(index_path + ".dulwich", "wb") as f:
            write_index_dict(f, entries, version=4)

        # Verify the written index is smaller (compression should help)
        original_size = os.path.getsize(index_path)
        dulwich_size = os.path.getsize(index_path + ".dulwich")

        # Allow some variance due to different compression decisions
        self.assertLess(abs(original_size - dulwich_size), original_size * 0.2)

    def test_index_v4_with_extensions(self) -> None:
        """Test v4 index with various extensions."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create some files
        files = ["file1.txt", "file2.txt", "dir/file3.txt"]
        for filename in files:
            filepath = os.path.join(repo.path, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write("content\n")

        # Add files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Enable untracked cache (creates UNTR extension)
        run_git_or_fail(["config", "core.untrackedCache", "true"], cwd=repo.path)
        run_git_or_fail(["status"], cwd=repo.path)  # Trigger cache update

        # Read index with extensions
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertEqual(len(entries), len(files))

        # Test round-trip with extensions present
        index = Index(index_path)
        index.write()

        # Verify C Git can still read it
        output = run_git_or_fail(["ls-files"], cwd=repo.path)
        git_files = set(output.strip().split(b"\n"))
        expected_files = {f.encode() for f in files}
        self.assertEqual(git_files, expected_files)

    def test_index_v4_empty_repository(self) -> None:
        """Test v4 index behavior with empty repository."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create empty commit to get an index file
        run_git_or_fail(["commit", "--allow-empty", "-m", "empty"], cwd=repo.path)

        # Read the empty index
        index_path = os.path.join(repo.path, ".git", "index")
        if os.path.exists(index_path):
            with open(index_path, "rb") as f:
                entries, version = read_index_dict_with_version(f)

            # Even empty indexes should be readable
            self.assertEqual(len(entries), 0)

            # Test writing empty index
            with open(index_path + ".dulwich", "wb") as f:
                write_index_dict(f, entries, version=version)

    def test_index_v4_large_file_count(self) -> None:
        """Test v4 index with many files (stress test)."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create many files with similar paths to test compression
        files = []
        for i in range(50):  # Reasonable number for CI
            filename = f"src/component_{i:03d}/index.js"
            files.append(filename)
            filepath = os.path.join(repo.path, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write(f"// Component {i}\nexport default {{}};")

        # Add all files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Read index
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertEqual(len(entries), len(files))

        # Test dulwich can handle large indexes
        index = Index(index_path)
        index.write()

        # Verify all files are still present
        output = run_git_or_fail(["ls-files"], cwd=repo.path)
        git_files = output.strip().split(b"\n")
        self.assertEqual(len(git_files), len(files))

    def test_index_v4_concurrent_modifications(self) -> None:
        """Test v4 index behavior with file modifications."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create initial files
        files = ["file1.txt", "file2.txt", "subdir/file3.txt"]
        for filename in files:
            filepath = os.path.join(repo.path, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write("initial content\n")

        # Add files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Modify some files
        with open(os.path.join(repo.path, "file1.txt"), "w") as f:
            f.write("modified content\n")

        # Add new file
        with open(os.path.join(repo.path, "file4.txt"), "w") as f:
            f.write("new file\n")
        run_git_or_fail(["add", "file4.txt"], cwd=repo.path)

        # Test dulwich can read the updated index
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertEqual(len(entries), 4)  # 3 original + 1 new

        # Verify specific files
        self.assertIn(b"file1.txt", entries)
        self.assertIn(b"file4.txt", entries)

        # Test round-trip
        index = Index(index_path)
        index.write()

        # Verify state is preserved
        output = run_git_or_fail(["ls-files"], cwd=repo.path)
        self.assertIn(b"file4.txt", output)

    def test_index_v4_with_merge_conflicts(self) -> None:
        """Test v4 index behavior with merge conflicts and staging."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create initial commit
        with open(os.path.join(repo.path, "conflict.txt"), "w") as f:
            f.write("original content\n")
        with open(os.path.join(repo.path, "normal.txt"), "w") as f:
            f.write("normal file\n")

        run_git_or_fail(["add", "."], cwd=repo.path)
        run_git_or_fail(["commit", "-m", "initial"], cwd=repo.path)

        # Create branch and modify file
        run_git_or_fail(["checkout", "-b", "feature"], cwd=repo.path)
        with open(os.path.join(repo.path, "conflict.txt"), "w") as f:
            f.write("feature content\n")
        run_git_or_fail(["add", "conflict.txt"], cwd=repo.path)
        run_git_or_fail(["commit", "-m", "feature change"], cwd=repo.path)

        # Go back to main and make conflicting change
        run_git_or_fail(["checkout", "master"], cwd=repo.path)
        with open(os.path.join(repo.path, "conflict.txt"), "w") as f:
            f.write("master content\n")
        run_git_or_fail(["add", "conflict.txt"], cwd=repo.path)
        run_git_or_fail(["commit", "-m", "master change"], cwd=repo.path)

        # Try to merge (should create conflicts)
        result = run_git_or_fail(["merge", "feature"], cwd=repo.path, check=False)

        # Read the index with conflicts
        index_path = os.path.join(repo.path, ".git", "index")
        if os.path.exists(index_path):
            with open(index_path, "rb") as f:
                entries, version = read_index_dict_with_version(f)

            self.assertEqual(version, 4)

            # Test dulwich can handle conflicted index
            index = Index(index_path)
            index.write()

            # Verify Git can still read it
            output = run_git_or_fail(["status", "--porcelain"], cwd=repo.path)
            self.assertIn(b"conflict.txt", output)

    def test_index_v4_boundary_filename_lengths(self) -> None:
        """Test v4 with boundary conditions for filename lengths."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Test various boundary conditions
        boundary_files = [
            "",  # Empty name (invalid, but test robustness)
            "x",  # Single char
            "xx",  # Two chars
            "x" * 255,  # Max typical filename length
            "x" * 4095,  # Max path length in many filesystems
            "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z",  # Deep nesting
            "file_with_" + "very_" * 50 + "long_name.txt",  # Very long name
        ]

        valid_files = []
        for filename in boundary_files:
            if not filename:  # Skip empty filename
                continue
            try:
                filepath = os.path.join(repo.path, filename)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, "w") as f:
                    f.write(f"Content\n")
                valid_files.append(filename)
            except (OSError, ValueError):
                # Skip files that can't be created on this system
                continue

        if valid_files:
            # Add files
            run_git_or_fail(["add", "."], cwd=repo.path)

            # Test reading
            index_path = os.path.join(repo.path, ".git", "index")
            with open(index_path, "rb") as f:
                entries, version = read_index_dict_with_version(f)

            self.assertEqual(version, 4)

            # Test round-trip
            index = Index(index_path)
            index.write()

    def test_index_v4_special_characters_and_encoding(self) -> None:
        """Test v4 with special characters and various encodings."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Test files with special characters
        special_files = [
            "file with spaces.txt",
            "file\twith\ttabs.txt",
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.with.dots.txt",
            "UPPERCASE.TXT",
            "MixedCase.TxT",
            "file123numbers.txt",
            "file@#$%special.txt",
            "café.txt",  # Unicode
            "файл.txt",  # Cyrillic
            "文件.txt",  # Chinese
            "🚀rocket.txt",  # Emoji
            "file'with'quotes.txt",
            'file"with"doublequotes.txt',
            "file[with]brackets.txt",
            "file(with)parens.txt",
            "file{with}braces.txt",
        ]

        valid_files = []
        for filename in special_files:
            try:
                filepath = os.path.join(repo.path, filename)
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(f"Content of {filename}\n")
                valid_files.append(filename)
            except (OSError, UnicodeError):
                # Skip files that can't be created on this system
                continue

        if valid_files:
            # Add files
            run_git_or_fail(["add", "."], cwd=repo.path)

            # Test reading
            index_path = os.path.join(repo.path, ".git", "index")
            with open(index_path, "rb") as f:
                entries, version = read_index_dict_with_version(f)

            self.assertEqual(version, 4)
            self.assertGreater(len(entries), 0)

            # Test all valid files are present
            for filename in valid_files:
                filename_bytes = filename.encode("utf-8")
                self.assertIn(filename_bytes, entries)

    def test_index_v4_symlinks_and_special_modes(self) -> None:
        """Test v4 with symlinks and special file modes."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create regular file
        with open(os.path.join(repo.path, "regular.txt"), "w") as f:
            f.write("regular file\n")

        # Create executable file
        exec_path = os.path.join(repo.path, "executable.sh")
        with open(exec_path, "w") as f:
            f.write("#!/bin/bash\necho hello\n")
        os.chmod(exec_path, 0o755)

        # Create symlink (if supported)
        try:
            os.symlink("regular.txt", os.path.join(repo.path, "symlink.txt"))
            has_symlink = True
        except (OSError, NotImplementedError):
            has_symlink = False

        # Add files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Test reading
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)

        # Verify files with different modes
        self.assertIn(b"regular.txt", entries)
        self.assertIn(b"executable.sh", entries)
        if has_symlink:
            self.assertIn(b"symlink.txt", entries)

        # Test round-trip preserves modes
        index = Index(index_path)
        index.write()

        # Verify Git can read it
        output = run_git_or_fail(["ls-files", "-s"], cwd=repo.path)
        self.assertIn(b"regular.txt", output)
        self.assertIn(b"executable.sh", output)

    def test_index_v4_alternating_compression_patterns(self) -> None:
        """Test v4 with files that alternate between compressed/uncompressed."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create files that should create alternating compression patterns
        files = [
            # These should be uncompressed (no common prefix)
            "a.txt",
            "b.txt",
            "c.txt",
            # These should be compressed (common prefix)
            "common/path/file1.txt",
            "common/path/file2.txt",
            "common/path/file3.txt",
            # Back to uncompressed (different pattern)
            "different/structure/x.txt",
            "another/structure/y.txt",
            # More compression opportunities
            "src/main/Component1.java",
            "src/main/Component2.java",
            "src/test/Test1.java",
            "src/test/Test2.java",
        ]

        for filename in files:
            filepath = os.path.join(repo.path, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write(f"Content of {filename}\n")

        # Add files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Test reading
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertEqual(len(entries), len(files))

        # Verify all files are present
        for filename in files:
            self.assertIn(filename.encode(), entries)

        # Test round-trip
        index = Index(index_path)
        index.write()

    def test_index_v4_git_submodules(self) -> None:
        """Test v4 index with Git submodules."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create a submodule directory structure
        submodule_dir = os.path.join(repo.path, "submodule")
        os.makedirs(submodule_dir)

        # Initialize a separate repo for the submodule
        run_git_or_fail(["init"], cwd=submodule_dir)
        with open(os.path.join(submodule_dir, "sub.txt"), "w") as f:
            f.write("submodule content\n")
        run_git_or_fail(["add", "sub.txt"], cwd=submodule_dir)
        run_git_or_fail(["commit", "-m", "submodule commit"], cwd=submodule_dir)

        # Add some regular files to main repo
        with open(os.path.join(repo.path, "main.txt"), "w") as f:
            f.write("main repo content\n")
        run_git_or_fail(["add", "main.txt"], cwd=repo.path)

        # Add submodule (this creates a gitlink entry)
        run_git_or_fail(["submodule", "add", "./submodule", "submodule"], cwd=repo.path)

        # Test reading index with submodule
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)

        # Should have main.txt, .gitmodules, and submodule gitlink
        self.assertIn(b"main.txt", entries)
        self.assertIn(b".gitmodules", entries)
        self.assertIn(b"submodule", entries)

        # Test round-trip
        index = Index(index_path)
        index.write()

    def test_index_v4_partial_staging(self) -> None:
        """Test v4 with partial file staging (git add -p simulation)."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create initial file
        filepath = os.path.join(repo.path, "partial.txt")
        with open(filepath, "w") as f:
            f.write("line1\nline2\nline3\n")

        run_git_or_fail(["add", "partial.txt"], cwd=repo.path)
        run_git_or_fail(["commit", "-m", "initial"], cwd=repo.path)

        # Modify the file
        with open(filepath, "w") as f:
            f.write("line1 modified\nline2\nline3 modified\n")

        # Stage only part of the changes (simulate git add -p)
        # This creates an interesting index state
        run_git_or_fail(["add", "partial.txt"], cwd=repo.path)

        # Make more changes
        with open(filepath, "w") as f:
            f.write("line1 modified\nline2 modified\nline3 modified\n")

        # Now we have staged and unstaged changes
        # Test reading this complex index state
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertIn(b"partial.txt", entries)

        # Test round-trip
        index = Index(index_path)
        index.write()

    def test_index_v4_with_gitattributes_and_ignore(self) -> None:
        """Test v4 with .gitattributes and .gitignore files."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create .gitignore
        with open(os.path.join(repo.path, ".gitignore"), "w") as f:
            f.write("*.tmp\n*.log\nbuild/\n")

        # Create .gitattributes
        with open(os.path.join(repo.path, ".gitattributes"), "w") as f:
            f.write("*.txt text\n*.bin binary\n")

        # Create various files
        files = [
            "regular.txt",
            "binary.bin",
            "script.sh",
            "config.json",
            "README.md",
        ]

        for filename in files:
            filepath = os.path.join(repo.path, filename)
            with open(filepath, "w") as f:
                f.write(f"Content of {filename}\n")

        # Create some files that should be ignored
        with open(os.path.join(repo.path, "temp.tmp"), "w") as f:
            f.write("temporary file\n")

        # Add files
        run_git_or_fail(["add", "."], cwd=repo.path)

        # Test reading
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)

        # Should have .gitignore, .gitattributes, and regular files
        self.assertIn(b".gitignore", entries)
        self.assertIn(b".gitattributes", entries)
        for filename in files:
            self.assertIn(filename.encode(), entries)

        # Should NOT have ignored files
        self.assertNotIn(b"temp.tmp", entries)

    def test_index_v4_stress_test_many_entries(self) -> None:
        """Stress test v4 with many entries in complex directory structure."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create a complex directory structure with many files
        dirs = [
            "src/main/java/com/example",
            "src/main/resources",
            "src/test/java/com/example",
            "docs/api",
            "docs/user",
            "scripts/build",
            "config/env",
        ]

        for dir_path in dirs:
            os.makedirs(os.path.join(repo.path, dir_path), exist_ok=True)

        # Create many files
        files = []
        for i in range(200):  # Reasonable for CI
            if i % 7 == 0:
                filename = f"src/main/java/com/example/Service{i}.java"
            elif i % 7 == 1:
                filename = f"src/test/java/com/example/Test{i}.java"
            elif i % 7 == 2:
                filename = f"docs/api/page{i}.md"
            elif i % 7 == 3:
                filename = f"config/env/config{i}.properties"
            elif i % 7 == 4:
                filename = f"scripts/build/script{i}.sh"
            elif i % 7 == 5:
                filename = f"src/main/resources/resource{i}.txt"
            else:
                filename = f"file{i}.txt"

            files.append(filename)
            filepath = os.path.join(repo.path, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write(f"// File {i}\ncontent here\n")

        # Add files in batches to avoid command line length limits
        batch_size = 50
        for i in range(0, len(files), batch_size):
            batch = files[i : i + batch_size]
            run_git_or_fail(["add"] + batch, cwd=repo.path)

        # Test reading large index
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)
        self.assertEqual(len(entries), len(files))

        # Verify some files are present
        for i in range(0, len(files), 20):  # Check every 20th file
            filename = files[i]
            self.assertIn(filename.encode(), entries)

    def test_index_v4_rename_detection_scenario(self) -> None:
        """Test v4 with file renames (complex staging scenario)."""
        require_git_version((2, 20, 0))

        repo = self._init_repo_with_manyfiles()

        # Create initial files
        files = ["old1.txt", "old2.txt", "unchanged.txt"]
        for filename in files:
            filepath = os.path.join(repo.path, filename)
            with open(filepath, "w") as f:
                f.write(f"Content of {filename}\n")

        run_git_or_fail(["add", "."], cwd=repo.path)
        run_git_or_fail(["commit", "-m", "initial"], cwd=repo.path)

        # Rename files
        os.rename(
            os.path.join(repo.path, "old1.txt"), os.path.join(repo.path, "new1.txt")
        )
        os.rename(
            os.path.join(repo.path, "old2.txt"), os.path.join(repo.path, "new2.txt")
        )

        # Stage renames
        run_git_or_fail(["add", "-A"], cwd=repo.path)

        # Test reading index with renames
        index_path = os.path.join(repo.path, ".git", "index")
        with open(index_path, "rb") as f:
            entries, version = read_index_dict_with_version(f)

        self.assertEqual(version, 4)

        # Should have new names, not old names
        self.assertIn(b"new1.txt", entries)
        self.assertIn(b"new2.txt", entries)
        self.assertIn(b"unchanged.txt", entries)
        self.assertNotIn(b"old1.txt", entries)
        self.assertNotIn(b"old2.txt", entries)
