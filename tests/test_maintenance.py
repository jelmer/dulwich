# test_maintenance.py -- tests for maintenance functionality
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

"""Tests for dulwich.maintenance."""

import tempfile

from dulwich import porcelain
from dulwich.maintenance import (
    CommitGraphTask,
    GcTask,
    IncrementalRepackTask,
    LooseObjectsTask,
    PackRefsTask,
    PrefetchTask,
    get_enabled_tasks,
    run_maintenance,
)
from dulwich.objects import Blob
from dulwich.repo import Repo

from . import TestCase


class MaintenanceTaskTestCase(TestCase):
    """Base class for maintenance task tests."""

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
        """Create a simple commit in the test repository."""
        blob = Blob.from_string(b"test content")
        self.repo.object_store.add_object(blob)
        return blob


class GcTaskTest(MaintenanceTaskTestCase):
    """Tests for GcTask."""

    def test_default_enabled(self):
        """Test that GC task is enabled by default."""
        task = GcTask(self.repo)
        self.assertTrue(task.default_enabled())
        self.assertTrue(task.is_enabled())

    def test_run(self):
        """Test running GC task."""
        self._create_commit()
        task = GcTask(self.repo)
        result = task.run()
        self.assertTrue(result)


class CommitGraphTaskTest(MaintenanceTaskTestCase):
    """Tests for CommitGraphTask."""

    def test_default_enabled(self):
        """Test that commit-graph task is enabled by default."""
        task = CommitGraphTask(self.repo)
        self.assertTrue(task.default_enabled())
        self.assertTrue(task.is_enabled())

    def test_run(self):
        """Test running commit-graph task."""
        self._create_commit()
        task = CommitGraphTask(self.repo)
        result = task.run()
        self.assertTrue(result)


class LooseObjectsTaskTest(MaintenanceTaskTestCase):
    """Tests for LooseObjectsTask."""

    def test_default_enabled(self):
        """Test that loose-objects task is disabled by default."""
        task = LooseObjectsTask(self.repo)
        self.assertFalse(task.default_enabled())

    def test_run(self):
        """Test running loose-objects task."""
        self._create_commit()
        task = LooseObjectsTask(self.repo)
        result = task.run()
        self.assertTrue(result)


class IncrementalRepackTaskTest(MaintenanceTaskTestCase):
    """Tests for IncrementalRepackTask."""

    def test_default_enabled(self):
        """Test that incremental-repack task is disabled by default."""
        task = IncrementalRepackTask(self.repo)
        self.assertFalse(task.default_enabled())

    def test_run_no_packs(self):
        """Test running incremental-repack with no packs."""
        task = IncrementalRepackTask(self.repo)
        result = task.run()
        self.assertTrue(result)

    def test_run_auto_few_packs(self):
        """Test that auto mode skips repacking when there are few packs."""
        self._create_commit()
        task = IncrementalRepackTask(self.repo, auto=True)
        result = task.run()
        self.assertTrue(result)


class PackRefsTaskTest(MaintenanceTaskTestCase):
    """Tests for PackRefsTask."""

    def test_default_enabled(self):
        """Test that pack-refs task is disabled by default."""
        task = PackRefsTask(self.repo)
        self.assertFalse(task.default_enabled())

    def test_run(self):
        """Test running pack-refs task."""
        task = PackRefsTask(self.repo)
        result = task.run()
        self.assertTrue(result)


class PrefetchTaskTest(MaintenanceTaskTestCase):
    """Tests for PrefetchTask."""

    def test_default_enabled(self):
        """Test that prefetch task is disabled by default."""
        task = PrefetchTask(self.repo)
        self.assertFalse(task.default_enabled())

    def test_run_no_remotes(self):
        """Test running prefetch with no remotes configured."""
        task = PrefetchTask(self.repo)
        result = task.run()
        self.assertTrue(result)


class MaintenanceFunctionsTest(MaintenanceTaskTestCase):
    """Tests for maintenance module functions."""

    def test_get_enabled_tasks_default(self):
        """Test getting enabled tasks with defaults."""
        enabled = get_enabled_tasks(self.repo)
        # By default, only gc and commit-graph are enabled
        self.assertIn("gc", enabled)
        self.assertIn("commit-graph", enabled)
        self.assertNotIn("loose-objects", enabled)
        self.assertNotIn("incremental-repack", enabled)
        self.assertNotIn("pack-refs", enabled)
        self.assertNotIn("prefetch", enabled)

    def test_get_enabled_tasks_with_filter(self):
        """Test getting enabled tasks with a filter."""
        enabled = get_enabled_tasks(self.repo, ["gc", "pack-refs"])
        self.assertEqual(set(enabled), {"gc", "pack-refs"})

    def test_get_enabled_tasks_invalid(self):
        """Test that invalid task names are ignored."""
        enabled = get_enabled_tasks(self.repo, ["gc", "invalid-task"])
        self.assertEqual(enabled, ["gc"])

    def test_run_maintenance(self):
        """Test running maintenance tasks."""
        self._create_commit()
        result = run_maintenance(self.repo)
        self.assertIn("gc", result.tasks_run)
        self.assertIn("commit-graph", result.tasks_run)
        self.assertIn("gc", result.tasks_succeeded)
        self.assertIn("commit-graph", result.tasks_succeeded)
        self.assertEqual(len(result.tasks_failed), 0)

    def test_run_maintenance_specific_tasks(self):
        """Test running specific maintenance tasks."""
        result = run_maintenance(self.repo, tasks=["pack-refs"])
        self.assertEqual(result.tasks_run, ["pack-refs"])
        self.assertEqual(result.tasks_succeeded, ["pack-refs"])
        self.assertEqual(len(result.tasks_failed), 0)

    def test_run_maintenance_with_progress(self):
        """Test running maintenance with progress callback."""
        messages = []

        def progress(msg):
            messages.append(msg)

        self._create_commit()
        result = run_maintenance(self.repo, progress=progress)
        self.assertGreater(len(messages), 0)
        self.assertIn("gc", result.tasks_succeeded)


class PorcelainMaintenanceTest(MaintenanceTaskTestCase):
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


class MaintenanceRegisterTest(MaintenanceTaskTestCase):
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
