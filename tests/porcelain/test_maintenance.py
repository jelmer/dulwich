# test_maintenance.py -- tests for porcelain maintenance
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

"""Tests for porcelain maintenance functions."""

import tempfile

from dulwich import porcelain
from dulwich.objects import Blob
from dulwich.repo import Repo

from .. import TestCase


class PorcelainMaintenanceTestCase(TestCase):
    """Base class for porcelain maintenance tests."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup_test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def _cleanup_test_dir(self):
        import shutil

        shutil.rmtree(self.test_dir)

    def _create_commit(self):
        """Helper to create a test commit."""
        blob = Blob.from_string(b"test content\n")
        self.repo.object_store.add_object(blob)

        from dulwich.objects import Commit, Tree

        tree = Tree()
        tree.add(b"testfile", 0o100644, blob.id)
        self.repo.object_store.add_object(tree)

        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Test <test@example.com>"
        commit.author_time = commit.commit_time = 1000000000
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"Test commit"
        commit.parents = []
        self.repo.object_store.add_object(commit)
        self.repo.refs[b"refs/heads/master"] = commit.id
        return commit.id


class PorcelainMaintenanceTest(PorcelainMaintenanceTestCase):
    """Tests for porcelain.maintenance_run function."""

    def test_maintenance_run(self):
        """Test porcelain maintenance_run function."""
        self._create_commit()
        result = porcelain.maintenance_run(self.test_dir)
        self.assertIn("gc", result.tasks_succeeded)
        self.assertIn("commit-graph", result.tasks_succeeded)

    def test_maintenance_run_with_tasks(self):
        """Test porcelain maintenance_run with specific tasks."""
        result = porcelain.maintenance_run(self.test_dir, tasks=["pack-refs"])
        self.assertEqual(result.tasks_run, ["pack-refs"])
        self.assertEqual(result.tasks_succeeded, ["pack-refs"])


class MaintenanceRegisterTest(PorcelainMaintenanceTestCase):
    """Tests for maintenance register/unregister."""

    def setUp(self):
        super().setUp()
        # Set up a temporary HOME for testing global config
        self.temp_home = tempfile.mkdtemp()
        self.addCleanup(self._cleanup_temp_home)
        self.overrideEnv("HOME", self.temp_home)

    def _cleanup_temp_home(self):
        import shutil

        shutil.rmtree(self.temp_home)

    def test_register_repository(self):
        """Test registering a repository for maintenance."""
        porcelain.maintenance_register(self.test_dir)

        # Verify repository was added to global config
        import os

        from dulwich.config import ConfigFile

        global_config_path = os.path.expanduser("~/.gitconfig")
        global_config = ConfigFile.from_path(global_config_path)

        repos = list(global_config.get_multivar((b"maintenance",), b"repo"))
        self.assertIn(self.test_dir.encode(), repos)

        # Verify strategy was set
        strategy = global_config.get((b"maintenance",), b"strategy")
        self.assertEqual(strategy, b"incremental")

        # Verify auto maintenance was disabled in repo
        repo_config = self.repo.get_config()
        auto = repo_config.get_boolean((b"maintenance",), b"auto")
        self.assertFalse(auto)

    def test_register_already_registered(self):
        """Test registering an already registered repository."""
        porcelain.maintenance_register(self.test_dir)
        # Should not error when registering again
        porcelain.maintenance_register(self.test_dir)

    def test_unregister_repository(self):
        """Test unregistering a repository."""
        # First register
        porcelain.maintenance_register(self.test_dir)

        # Then unregister
        porcelain.maintenance_unregister(self.test_dir)

        # Verify repository was removed from global config
        import os

        from dulwich.config import ConfigFile

        global_config_path = os.path.expanduser("~/.gitconfig")
        global_config = ConfigFile.from_path(global_config_path)

        try:
            repos = list(global_config.get_multivar((b"maintenance",), b"repo"))
            self.assertNotIn(self.test_dir.encode(), repos)
        except KeyError:
            # No repos registered, which is fine
            pass

    def test_unregister_not_registered(self):
        """Test unregistering a repository that is not registered."""
        with self.assertRaises(ValueError):
            porcelain.maintenance_unregister(self.test_dir)

    def test_unregister_not_registered_force(self):
        """Test unregistering with force flag."""
        # Should not error with force=True
        porcelain.maintenance_unregister(self.test_dir, force=True)
