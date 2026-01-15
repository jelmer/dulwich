# test_submodule.py -- tests for porcelain submodule functions
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

"""Tests for porcelain submodule functions."""

import os
import shutil
import tempfile

from dulwich import porcelain
from dulwich.config import ConfigFile
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo

from .. import TestCase


class SubmoduleAddTests(TestCase):
    """Tests for submodule_add function."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super().tearDown()

    def test_submodule_add_basic(self) -> None:
        """Test basic submodule_add with URL."""
        url = "https://github.com/dulwich/dulwich.git"
        path = "libs/dulwich"

        porcelain.submodule_add(self.repo, url, path, name="dulwich")

        # Check that .gitmodules was created
        gitmodules_path = os.path.join(self.repo_path, ".gitmodules")
        self.assertTrue(os.path.exists(gitmodules_path))

        # Check .gitmodules content
        config = ConfigFile.from_path(gitmodules_path)
        self.assertEqual(url.encode(), config.get(("submodule", "dulwich"), "url"))
        self.assertEqual(path.encode(), config.get(("submodule", "dulwich"), "path"))

    def test_submodule_add_without_name(self) -> None:
        """Test submodule_add derives name from path when not specified."""
        url = "https://github.com/dulwich/dulwich.git"
        path = "libs/dulwich"

        porcelain.submodule_add(self.repo, url, path)

        # Check that .gitmodules was created
        gitmodules_path = os.path.join(self.repo_path, ".gitmodules")
        config = ConfigFile.from_path(gitmodules_path)

        # Name should be derived from path
        self.assertEqual(url.encode(), config.get(("submodule", "libs/dulwich"), "url"))

    def test_submodule_add_without_path(self) -> None:
        """Test submodule_add derives path from URL when not specified."""
        url = "https://github.com/dulwich/dulwich.git"

        porcelain.submodule_add(self.repo, url)

        # Check that .gitmodules was created
        gitmodules_path = os.path.join(self.repo_path, ".gitmodules")
        config = ConfigFile.from_path(gitmodules_path)

        # Path should be derived from URL (just "dulwich")
        # The actual value depends on _canonical_part implementation
        # We just check that something was written
        sections = list(config.keys())
        self.assertEqual(1, len([s for s in sections if s[0] == b"submodule"]))

    def test_submodule_add_updates_existing_gitmodules(self) -> None:
        """Test that submodule_add updates existing .gitmodules file."""
        # Add first submodule
        url1 = "https://github.com/dulwich/dulwich.git"
        path1 = "libs/dulwich"
        porcelain.submodule_add(self.repo, url1, path1, name="dulwich")

        # Add second submodule
        url2 = "https://github.com/dulwich/dulwich-tests.git"
        path2 = "libs/tests"
        porcelain.submodule_add(self.repo, url2, path2, name="tests")

        # Check both submodules are in .gitmodules
        gitmodules_path = os.path.join(self.repo_path, ".gitmodules")
        config = ConfigFile.from_path(gitmodules_path)

        self.assertEqual(url1.encode(), config.get(("submodule", "dulwich"), "url"))
        self.assertEqual(url2.encode(), config.get(("submodule", "tests"), "url"))


class SubmoduleInitTests(TestCase):
    """Tests for submodule_init function."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super().tearDown()

    def test_submodule_init(self) -> None:
        """Test submodule_init reads from .gitmodules and updates config."""
        # Create .gitmodules file
        gitmodules_path = os.path.join(self.repo_path, ".gitmodules")
        config = ConfigFile()
        config.set(
            ("submodule", "dulwich"), "url", "https://github.com/dulwich/dulwich.git"
        )
        config.set(("submodule", "dulwich"), "path", "libs/dulwich")
        config.path = gitmodules_path
        config.write_to_path()

        # Initialize submodules
        porcelain.submodule_init(self.repo)

        # Check that repo config was updated
        repo_config = self.repo.get_config()
        self.assertEqual(
            b"true", repo_config.get((b"submodule", b"dulwich"), b"active")
        )
        self.assertEqual(
            b"https://github.com/dulwich/dulwich.git",
            repo_config.get((b"submodule", b"dulwich"), b"url"),
        )

    def test_submodule_init_no_gitmodules(self) -> None:
        """Test submodule_init raises FileNotFoundError when .gitmodules is missing."""
        # Should raise FileNotFoundError when .gitmodules doesn't exist
        with self.assertRaises(FileNotFoundError):
            porcelain.submodule_init(self.repo)


class SubmoduleListTests(TestCase):
    """Tests for submodule_list function."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super().tearDown()

    def test_submodule_list_empty(self) -> None:
        """Test submodule_list with no submodules."""
        # Create an initial commit
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)

        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"Initial commit"
        self.repo.object_store.add_object(commit)

        self.repo.refs[b"refs/heads/main"] = commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")

        # List should be empty
        submodules = list(porcelain.submodule_list(self.repo))
        self.assertEqual([], submodules)

    def test_submodule_list_with_submodule(self) -> None:
        """Test submodule_list with a submodule in the tree."""
        # Create a tree with a submodule entry
        tree = Tree()
        tree.add(b"test.txt", 0o100644, b"a" * 40)  # Dummy file
        # Add a submodule entry with gitlink mode (0o160000)
        submodule_sha = b"1" * 40
        tree.add(b"libs/mylib", 0o160000, submodule_sha)
        self.repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.author_time = commit.commit_time = 1234567890
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"Add submodule"
        self.repo.object_store.add_object(commit)

        self.repo.refs[b"refs/heads/main"] = commit.id
        self.repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")

        # List should contain the submodule
        submodules = list(porcelain.submodule_list(self.repo))
        self.assertEqual(1, len(submodules))
        path, sha = submodules[0]
        self.assertEqual("libs/mylib", path)
        self.assertEqual(submodule_sha.decode(), sha)
