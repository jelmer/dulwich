# test_worktree.py -- Tests for dulwich.worktree
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.worktree."""

import os
import stat
import tempfile
from unittest import skipIf

from dulwich import porcelain
from dulwich.object_store import tree_lookup_path
from dulwich.repo import Repo
from dulwich.worktree import WorkTree

from . import TestCase


class WorkTreeTestCase(TestCase):
    """Base test case for WorkTree tests."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo = Repo.init(self.test_dir)

        # Create initial commit with a file
        with open(os.path.join(self.test_dir, "a"), "wb") as f:
            f.write(b"contents of file a")
        self.repo.stage(["a"])
        self.root_commit = self.repo.do_commit(
            b"Initial commit",
            committer=b"Test Committer <test@nodomain.com>",
            author=b"Test Author <test@nodomain.com>",
            commit_timestamp=12345,
            commit_timezone=0,
            author_timestamp=12345,
            author_timezone=0,
        )
        self.worktree = self.repo.get_worktree()

    def tearDown(self):
        self.repo.close()
        super().tearDown()


class WorkTreeInitTests(TestCase):
    """Tests for WorkTree initialization."""

    def test_init_with_repo_path(self):
        """Test WorkTree initialization with same path as repo."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repo.init(tmpdir)
            worktree = WorkTree(repo, tmpdir)

            self.assertEqual(worktree.path, tmpdir)
            self.assertEqual(worktree._repo, repo)
            self.assertTrue(os.path.isabs(worktree.path))

    def test_init_with_different_path(self):
        """Test WorkTree initialization with different path from repo."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = os.path.join(tmpdir, "repo")
            worktree_path = os.path.join(tmpdir, "worktree")

            os.makedirs(repo_path)
            os.makedirs(worktree_path)

            repo = Repo.init(repo_path)
            worktree = WorkTree(repo, worktree_path)

            self.assertNotEqual(worktree.path, repo.path)
            self.assertEqual(worktree.path, worktree_path)
            self.assertEqual(worktree._repo, repo)
            self.assertTrue(os.path.isabs(worktree.path))

    def test_init_with_bytes_path(self):
        """Test WorkTree initialization with bytes path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Repo.init(tmpdir)
            worktree = WorkTree(repo, tmpdir.encode("utf-8"))

            self.assertEqual(worktree.path, tmpdir)
            self.assertIsInstance(worktree.path, str)


class WorkTreeStagingTests(WorkTreeTestCase):
    """Tests for WorkTree staging operations."""

    def test_stage_absolute(self):
        """Test that staging with absolute paths raises ValueError."""
        r = self.repo
        os.remove(os.path.join(r.path, "a"))
        self.assertRaises(ValueError, self.worktree.stage, [os.path.join(r.path, "a")])

    def test_stage_deleted(self):
        """Test staging a deleted file."""
        r = self.repo
        os.remove(os.path.join(r.path, "a"))
        self.worktree.stage(["a"])
        self.worktree.stage(["a"])  # double-stage a deleted path
        self.assertEqual([], list(r.open_index()))

    def test_stage_directory(self):
        """Test staging a directory."""
        r = self.repo
        os.mkdir(os.path.join(r.path, "c"))
        self.worktree.stage(["c"])
        self.assertEqual([b"a"], list(r.open_index()))

    def test_stage_submodule(self):
        """Test staging a submodule."""
        r = self.repo
        s = Repo.init(os.path.join(r.path, "sub"), mkdir=True)
        s.do_commit(b"message")
        self.worktree.stage(["sub"])
        self.assertEqual([b"a", b"sub"], list(r.open_index()))


class WorkTreeUnstagingTests(WorkTreeTestCase):
    """Tests for WorkTree unstaging operations."""

    def test_unstage_modify_file_with_dir(self):
        """Test unstaging a modified file in a directory."""
        os.mkdir(os.path.join(self.repo.path, "new_dir"))
        full_path = os.path.join(self.repo.path, "new_dir", "foo")

        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "a") as f:
            f.write("something new")
        self.worktree.unstage(["new_dir/foo"])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": []}, [b"new_dir/foo"], []], status
        )

    def test_unstage_while_no_commit(self):
        """Test unstaging when there are no commits."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], ["foo"]], status)

    def test_unstage_add_file(self):
        """Test unstaging a newly added file."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))
        self.assertEqual([{"add": [], "delete": [], "modify": []}, [], ["foo"]], status)

    def test_unstage_modify_file(self):
        """Test unstaging a modified file."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        with open(full_path, "a") as f:
            f.write("broken")
        porcelain.add(self.repo, paths=[full_path])
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))

        self.assertEqual(
            [{"add": [], "delete": [], "modify": []}, [b"foo"], []], status
        )

    def test_unstage_remove_file(self):
        """Test unstaging a removed file."""
        file = "foo"
        full_path = os.path.join(self.repo.path, file)
        with open(full_path, "w") as f:
            f.write("hello")
        porcelain.add(self.repo, paths=[full_path])
        porcelain.commit(
            self.repo,
            message=b"unittest",
            committer=b"Jane <jane@example.com>",
            author=b"John <john@example.com>",
        )
        os.remove(full_path)
        self.worktree.unstage([file])
        status = list(porcelain.status(self.repo))
        self.assertEqual(
            [{"add": [], "delete": [], "modify": []}, [b"foo"], []], status
        )


class WorkTreeCommitTests(WorkTreeTestCase):
    """Tests for WorkTree commit operations."""

    def test_commit_modified(self):
        """Test committing a modified file."""
        r = self.repo
        with open(os.path.join(r.path, "a"), "wb") as f:
            f.write(b"new contents")
        self.worktree.stage(["a"])
        commit_sha = self.worktree.commit(
            b"modified a",
            committer=b"Test Committer <test@nodomain.com>",
            author=b"Test Author <test@nodomain.com>",
            commit_timestamp=12395,
            commit_timezone=0,
            author_timestamp=12395,
            author_timezone=0,
        )
        self.assertEqual([self.root_commit], r[commit_sha].parents)
        a_mode, a_id = tree_lookup_path(r.get_object, r[commit_sha].tree, b"a")
        self.assertEqual(stat.S_IFREG | 0o644, a_mode)
        self.assertEqual(b"new contents", r[a_id].data)

    @skipIf(not getattr(os, "symlink", None), "Requires symlink support")
    def test_commit_symlink(self):
        """Test committing a symlink."""
        r = self.repo
        os.symlink("a", os.path.join(r.path, "b"))
        self.worktree.stage(["a", "b"])
        commit_sha = self.worktree.commit(
            b"Symlink b",
            committer=b"Test Committer <test@nodomain.com>",
            author=b"Test Author <test@nodomain.com>",
            commit_timestamp=12395,
            commit_timezone=0,
            author_timestamp=12395,
            author_timezone=0,
        )
        self.assertEqual([self.root_commit], r[commit_sha].parents)
        b_mode, b_id = tree_lookup_path(r.get_object, r[commit_sha].tree, b"b")
        self.assertEqual(stat.S_IFLNK, b_mode)
        self.assertEqual(b"a", r[b_id].data)


class WorkTreeResetTests(WorkTreeTestCase):
    """Tests for WorkTree reset operations."""

    def test_reset_index(self):
        """Test resetting the index."""
        # Make some changes and stage them
        with open(os.path.join(self.repo.path, "a"), "wb") as f:
            f.write(b"modified contents")
        self.worktree.stage(["a"])

        # Reset index should restore to HEAD
        self.worktree.reset_index()

        # Check that the working tree file was restored
        with open(os.path.join(self.repo.path, "a"), "rb") as f:
            contents = f.read()
        self.assertEqual(b"contents of file a", contents)


class WorkTreeSparseCheckoutTests(WorkTreeTestCase):
    """Tests for WorkTree sparse checkout operations."""

    def test_get_sparse_checkout_patterns_empty(self):
        """Test getting sparse checkout patterns when file doesn't exist."""
        patterns = self.worktree.get_sparse_checkout_patterns()
        self.assertEqual([], patterns)

    def test_set_sparse_checkout_patterns(self):
        """Test setting sparse checkout patterns."""
        patterns = ["*.py", "docs/"]
        self.worktree.set_sparse_checkout_patterns(patterns)

        # Read back the patterns
        retrieved_patterns = self.worktree.get_sparse_checkout_patterns()
        self.assertEqual(patterns, retrieved_patterns)

    def test_configure_for_cone_mode(self):
        """Test configuring repository for cone mode."""
        self.worktree.configure_for_cone_mode()

        config = self.repo.get_config()
        self.assertEqual(b"true", config.get((b"core",), b"sparseCheckout"))
        self.assertEqual(b"true", config.get((b"core",), b"sparseCheckoutCone"))

    def test_infer_cone_mode_false(self):
        """Test inferring cone mode when not configured."""
        self.assertFalse(self.worktree.infer_cone_mode())

    def test_infer_cone_mode_true(self):
        """Test inferring cone mode when configured."""
        self.worktree.configure_for_cone_mode()
        self.assertTrue(self.worktree.infer_cone_mode())

    def test_set_cone_mode_patterns(self):
        """Test setting cone mode patterns."""
        dirs = ["src", "tests"]
        self.worktree.set_cone_mode_patterns(dirs)

        patterns = self.worktree.get_sparse_checkout_patterns()
        expected = ["/*", "!/*/", "/src/", "/tests/"]
        self.assertEqual(expected, patterns)

    def test_set_cone_mode_patterns_empty(self):
        """Test setting cone mode patterns with empty list."""
        self.worktree.set_cone_mode_patterns([])

        patterns = self.worktree.get_sparse_checkout_patterns()
        expected = ["/*", "!/*/"]
        self.assertEqual(expected, patterns)

    def test_set_cone_mode_patterns_duplicates(self):
        """Test that duplicate patterns are not added."""
        dirs = ["src", "src"]  # duplicate
        self.worktree.set_cone_mode_patterns(dirs)

        patterns = self.worktree.get_sparse_checkout_patterns()
        expected = ["/*", "!/*/", "/src/"]
        self.assertEqual(expected, patterns)

    def test_sparse_checkout_file_path(self):
        """Test getting the sparse checkout file path."""
        expected_path = os.path.join(self.repo.controldir(), "info", "sparse-checkout")
        actual_path = self.worktree._sparse_checkout_file_path()
        self.assertEqual(expected_path, actual_path)


class WorkTreeBackwardCompatibilityTests(WorkTreeTestCase):
    """Tests for backward compatibility of deprecated Repo methods."""

    def test_deprecated_stage_delegates_to_worktree(self):
        """Test that deprecated Repo.stage delegates to WorkTree."""
        with open(os.path.join(self.repo.path, "new_file"), "w") as f:
            f.write("test content")

        # This should show a deprecation warning but still work
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.repo.stage(["new_file"])
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

    def test_deprecated_unstage_delegates_to_worktree(self):
        """Test that deprecated Repo.unstage delegates to WorkTree."""
        # This should show a deprecation warning but still work
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.repo.unstage(["a"])
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

    def test_deprecated_sparse_checkout_methods(self):
        """Test that deprecated sparse checkout methods delegate to WorkTree."""
        import warnings

        # Test get_sparse_checkout_patterns
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            patterns = self.repo.get_sparse_checkout_patterns()
            self.assertEqual([], patterns)
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))

        # Test set_sparse_checkout_patterns
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.repo.set_sparse_checkout_patterns(["*.py"])
            self.assertTrue(len(w) > 0)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
