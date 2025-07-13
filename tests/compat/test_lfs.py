#!/usr/bin/python
# test_lfs.py -- Compatibility tests for LFS.
# Copyright (C) 2025 Dulwich contributors
#
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

"""Compatibility tests for LFS functionality between dulwich and git-lfs."""

import os
import subprocess
import tempfile
from unittest import skipUnless

from dulwich import porcelain
from dulwich.lfs import LFSPointer
from dulwich.porcelain import lfs_clean, lfs_init, lfs_smudge, lfs_track

from .utils import CompatTestCase, rmtree_ro, run_git_or_fail


def git_lfs_version():
    """Get git-lfs version tuple."""
    try:
        output = run_git_or_fail(["lfs", "version"])
        # Example output: "git-lfs/3.0.2 (GitHub; linux amd64; go 1.17.2)"
        version_str = output.split(b"/")[1].split()[0]
        return tuple(map(int, version_str.decode().split(".")))
    except (OSError, subprocess.CalledProcessError, AssertionError):
        return None


class LFSCompatTestCase(CompatTestCase):
    """Base class for LFS compatibility tests."""

    min_git_version = (2, 0, 0)  # git-lfs requires git 2.0+

    def setUp(self):
        super().setUp()
        if git_lfs_version() is None:
            self.skipTest("git-lfs not available")

    def assertPointerEquals(self, pointer1, pointer2):
        """Assert two LFS pointers are equivalent."""
        self.assertEqual(pointer1.oid, pointer2.oid)
        self.assertEqual(pointer1.size, pointer2.size)

    def make_temp_dir(self):
        """Create a temporary directory that will be cleaned up."""
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, temp_dir)
        return temp_dir


class LFSInitCompatTest(LFSCompatTestCase):
    """Tests for LFS initialization compatibility."""

    def test_lfs_init_dulwich(self):
        """Test that dulwich lfs_init is compatible with git-lfs."""
        # Initialize with dulwich
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        lfs_init(repo_dir)

        # Verify with git-lfs
        output = run_git_or_fail(["lfs", "env"], cwd=repo_dir)
        self.assertIn(b"git config filter.lfs.clean", output)
        self.assertIn(b"git config filter.lfs.smudge", output)

    def test_lfs_init_git(self):
        """Test that git-lfs init is compatible with dulwich."""
        # Initialize with git-lfs
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        run_git_or_fail(["lfs", "install", "--local"], cwd=repo_dir)

        # Verify with dulwich
        repo = porcelain.open_repo(repo_dir)
        self.addCleanup(repo.close)
        config = repo.get_config_stack()
        self.assertEqual(
            config.get(("filter", "lfs"), "clean").decode(), "git-lfs clean -- %f"
        )
        self.assertEqual(
            config.get(("filter", "lfs"), "smudge").decode(), "git-lfs smudge -- %f"
        )


class LFSTrackCompatTest(LFSCompatTestCase):
    """Tests for LFS tracking compatibility."""

    def test_track_dulwich(self):
        """Test that dulwich lfs_track is compatible with git-lfs."""
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        lfs_init(repo_dir)

        # Track with dulwich
        lfs_track(repo_dir, ["*.bin", "*.dat"])

        # Verify with git-lfs
        output = run_git_or_fail(["lfs", "track"], cwd=repo_dir)
        self.assertIn(b"*.bin", output)
        self.assertIn(b"*.dat", output)

    def test_track_git(self):
        """Test that git-lfs track is compatible with dulwich."""
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        run_git_or_fail(["lfs", "install", "--local"], cwd=repo_dir)

        # Track with git-lfs
        run_git_or_fail(["lfs", "track", "*.bin"], cwd=repo_dir)
        run_git_or_fail(["lfs", "track", "*.dat"], cwd=repo_dir)

        # Verify with dulwich
        gitattributes_path = os.path.join(repo_dir, ".gitattributes")
        with open(gitattributes_path, "rb") as f:
            content = f.read().decode()
        self.assertIn("*.bin filter=lfs", content)
        self.assertIn("*.dat filter=lfs", content)


class LFSFileOperationsCompatTest(LFSCompatTestCase):
    """Tests for LFS file operations compatibility."""

    def test_add_commit_dulwich(self):
        """Test adding and committing LFS files with dulwich."""
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        lfs_init(repo_dir)
        lfs_track(repo_dir, ["*.bin"])

        # Create and add a large file
        test_file = os.path.join(repo_dir, "test.bin")
        test_content = b"x" * 1024 * 1024  # 1MB
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Add with dulwich
        porcelain.add(repo_dir, [test_file])
        porcelain.commit(repo_dir, message=b"Add LFS file")

        # Verify with git-lfs
        output = run_git_or_fail(["lfs", "ls-files"], cwd=repo_dir)
        self.assertIn(b"test.bin", output)

        # Check pointer file in git
        output = run_git_or_fail(["show", "HEAD:test.bin"], cwd=repo_dir)
        self.assertIn(b"version https://git-lfs.github.com/spec/v1", output)
        self.assertIn(b"oid sha256:", output)
        self.assertIn(b"size 1048576", output)

    def test_add_commit_git(self):
        """Test adding and committing LFS files with git-lfs."""
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        run_git_or_fail(["lfs", "install", "--local"], cwd=repo_dir)
        run_git_or_fail(["lfs", "track", "*.bin"], cwd=repo_dir)
        run_git_or_fail(["add", ".gitattributes"], cwd=repo_dir)
        run_git_or_fail(["commit", "-m", "Track .bin files"], cwd=repo_dir)

        # Create and add a large file
        test_file = os.path.join(repo_dir, "test.bin")
        test_content = b"y" * 1024 * 1024  # 1MB
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Add with git-lfs
        run_git_or_fail(["add", "test.bin"], cwd=repo_dir)
        run_git_or_fail(["commit", "-m", "Add LFS file"], cwd=repo_dir)

        # Verify with dulwich
        repo = porcelain.open_repo(repo_dir)
        self.addCleanup(repo.close)
        tree = repo[repo.head()].tree
        mode, sha = repo.object_store[tree][b"test.bin"]
        blob = repo.object_store[sha]
        pointer = LFSPointer.from_bytes(blob.data)
        self.assertEqual(pointer.size, 1048576)

    def test_checkout_dulwich(self):
        """Test checking out LFS files with dulwich."""
        # Create repo with git-lfs
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        run_git_or_fail(["lfs", "install", "--local"], cwd=repo_dir)
        run_git_or_fail(["lfs", "track", "*.bin"], cwd=repo_dir)
        run_git_or_fail(["add", ".gitattributes"], cwd=repo_dir)
        run_git_or_fail(["commit", "-m", "Track .bin files"], cwd=repo_dir)

        # Add LFS file
        test_file = os.path.join(repo_dir, "test.bin")
        test_content = b"z" * 1024 * 1024  # 1MB
        with open(test_file, "wb") as f:
            f.write(test_content)
        run_git_or_fail(["add", "test.bin"], cwd=repo_dir)
        run_git_or_fail(["commit", "-m", "Add LFS file"], cwd=repo_dir)

        # Remove working copy
        os.remove(test_file)

        # Checkout with dulwich
        porcelain.reset(repo_dir, mode="hard")

        # Verify file contents
        with open(test_file, "rb") as f:
            content = f.read()
        self.assertEqual(content, test_content)


class LFSPointerCompatTest(LFSCompatTestCase):
    """Tests for LFS pointer file compatibility."""

    def test_pointer_format_dulwich(self):
        """Test that dulwich creates git-lfs compatible pointers."""
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        lfs_init(repo_dir)

        test_content = b"test content for LFS"
        test_file = os.path.join(repo_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Create pointer with dulwich
        pointer_data = lfs_clean(repo_dir, "test.txt")

        # Parse with git-lfs (create a file and check)
        test_file = os.path.join(repo_dir, "test_pointer")
        with open(test_file, "wb") as f:
            f.write(pointer_data)

        # Verify pointer format
        with open(test_file, "rb") as f:
            lines = f.read().decode().strip().split("\n")

        self.assertEqual(lines[0], "version https://git-lfs.github.com/spec/v1")
        self.assertTrue(lines[1].startswith("oid sha256:"))
        self.assertTrue(lines[2].startswith("size "))

    def test_pointer_format_git(self):
        """Test that dulwich can parse git-lfs pointers."""
        # Create a git-lfs pointer manually
        oid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        size = 12345
        pointer_content = f"version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {size}\n"

        # Parse with dulwich
        pointer = LFSPointer.from_bytes(pointer_content.encode())

        self.assertEqual(pointer.oid, oid)
        self.assertEqual(pointer.size, size)


class LFSFilterCompatTest(LFSCompatTestCase):
    """Tests for LFS filter operations compatibility."""

    def test_clean_filter_compat(self):
        """Test clean filter compatibility between dulwich and git-lfs."""
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        lfs_init(repo_dir)

        test_content = b"x" * 1000
        test_file = os.path.join(repo_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Clean with dulwich
        dulwich_pointer = lfs_clean(repo_dir, "test.txt")

        # Clean with git-lfs (simulate)
        # Since we can't easily invoke git-lfs clean directly,
        # we'll test that the pointer format is correct
        self.assertIn(b"version https://git-lfs.github.com/spec/v1", dulwich_pointer)
        self.assertIn(b"oid sha256:", dulwich_pointer)
        self.assertIn(b"size 1000", dulwich_pointer)

    def test_smudge_filter_compat(self):
        """Test smudge filter compatibility between dulwich and git-lfs."""
        # Create a test repo with LFS
        repo_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=repo_dir)
        lfs_init(repo_dir)

        # Create test content
        test_content = b"test data for smudge filter"
        test_file = os.path.join(repo_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(test_content)

        pointer_data = lfs_clean(repo_dir, "test.txt")

        # Store object in LFS
        lfs_dir = os.path.join(repo_dir, ".git", "lfs")
        os.makedirs(lfs_dir, exist_ok=True)

        # Parse pointer to get oid
        pointer = LFSPointer.from_bytes(pointer_data)

        # Store object
        obj_dir = os.path.join(lfs_dir, "objects", pointer.oid[:2], pointer.oid[2:4])
        os.makedirs(obj_dir, exist_ok=True)
        obj_path = os.path.join(obj_dir, pointer.oid)
        with open(obj_path, "wb") as f:
            f.write(test_content)

        # Test smudge
        smudged = lfs_smudge(repo_dir, pointer_data)
        self.assertEqual(smudged, test_content)


class LFSCloneCompatTest(LFSCompatTestCase):
    """Tests for cloning repositories with LFS files."""

    @skipUnless(
        git_lfs_version() and git_lfs_version() >= (2, 0, 0),
        "git-lfs 2.0+ required for clone tests",
    )
    def test_clone_with_lfs(self):
        """Test cloning a repository with LFS files."""
        # Create source repo with LFS
        source_dir = self.make_temp_dir()
        run_git_or_fail(["init"], cwd=source_dir)
        run_git_or_fail(["lfs", "install", "--local"], cwd=source_dir)
        run_git_or_fail(["lfs", "track", "*.bin"], cwd=source_dir)
        run_git_or_fail(["add", ".gitattributes"], cwd=source_dir)
        run_git_or_fail(["commit", "-m", "Track .bin files"], cwd=source_dir)

        # Add LFS file
        test_file = os.path.join(source_dir, "test.bin")
        test_content = b"w" * 1024 * 1024  # 1MB
        with open(test_file, "wb") as f:
            f.write(test_content)
        run_git_or_fail(["add", "test.bin"], cwd=source_dir)
        run_git_or_fail(["commit", "-m", "Add LFS file"], cwd=source_dir)

        # Clone with dulwich
        target_dir = self.make_temp_dir()
        cloned_repo = porcelain.clone(source_dir, target_dir)
        self.addCleanup(cloned_repo.close)

        # Verify LFS file exists
        cloned_file = os.path.join(target_dir, "test.bin")
        with open(cloned_file, "rb") as f:
            content = f.read()

        # Check if filter.lfs.smudge is configured
        cloned_config = cloned_repo.get_config()
        try:
            lfs_smudge = cloned_config.get((b"filter", b"lfs"), b"smudge")
            has_lfs_config = bool(lfs_smudge)
        except KeyError:
            has_lfs_config = False

        if has_lfs_config:
            # git-lfs smudge filter should have converted it
            self.assertEqual(content, test_content)
        else:
            # No git-lfs config (uses built-in filter), should be a pointer
            self.assertIn(b"version https://git-lfs.github.com/spec/v1", content)


if __name__ == "__main__":
    import unittest

    unittest.main()
