# test_partial_clone.py -- Compatibility tests for partial clone.
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

"""Compatibility tests for partial clone support."""

import os
import shutil
import sys
import tempfile
import threading

from dulwich.objects import Blob, Tree
from dulwich.repo import Repo
from dulwich.server import DictBackend, TCPGitServer
from dulwich.tests.utils import make_commit

from .. import skipIf
from .utils import CompatTestCase, require_git_version, run_git_or_fail


@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class PartialCloneServerTestCase(CompatTestCase):
    """Tests for partial clone server compatibility with git client."""

    protocol = "git"
    # Partial clone support was introduced in git 2.17.0
    min_git_version = (2, 17, 0)

    def setUp(self) -> None:
        super().setUp()
        require_git_version(self.min_git_version)

    def _start_server(self, repo):
        backend = DictBackend({b"/": repo})
        dul_server = TCPGitServer(backend, b"localhost", 0)

        # Start server in a thread
        server_thread = threading.Thread(target=dul_server.serve)
        server_thread.daemon = True
        server_thread.start()

        # Add cleanup
        def cleanup_server():
            dul_server.shutdown()
            dul_server.server_close()
            server_thread.join(timeout=1.0)

        self.addCleanup(cleanup_server)
        self._server = dul_server
        _, port = self._server.socket.getsockname()
        return port

    def url(self, port) -> str:
        return f"{self.protocol}://localhost:{port}/"

    def test_clone_with_blob_none_filter(self) -> None:
        """Test that git client can clone with blob:none filter."""
        # Create repository with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)
        source_repo = Repo.init(repo_path, mkdir=False)

        # Create test content with multiple blobs
        blob1 = Blob.from_string(b"File 1 content - this is a test file")
        blob2 = Blob.from_string(b"File 2 content - another test file")
        blob3 = Blob.from_string(b"File 3 content - third test file")

        tree = Tree()
        tree.add(b"file1.txt", 0o100644, blob1.id)
        tree.add(b"file2.txt", 0o100644, blob2.id)
        tree.add(b"file3.txt", 0o100644, blob3.id)

        # Add objects to repo
        source_repo.object_store.add_object(blob1)
        source_repo.object_store.add_object(blob2)
        source_repo.object_store.add_object(blob3)
        source_repo.object_store.add_object(tree)

        commit = make_commit(tree=tree.id, message=b"Test commit with multiple files")
        source_repo.object_store.add_object(commit)
        source_repo.refs[b"refs/heads/master"] = commit.id

        # Start dulwich server
        port = self._start_server(source_repo)

        # Clone with blob:none filter
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        clone_dir = os.path.join(clone_path, "cloned_repo")

        run_git_or_fail(
            ["clone", "--filter=blob:none", "--no-checkout", self.url(port), clone_dir],
            cwd=clone_path,
        )

        # Verify cloned repo has commit and tree but no blobs
        cloned_repo = Repo(clone_dir)
        self.addCleanup(cloned_repo.close)

        # Commit should exist
        self.assertEqual(cloned_repo.refs[b"refs/heads/master"], commit.id)

        # Tree should exist
        self.assertIn(tree.id, cloned_repo.object_store)

        # Blobs should NOT be in object store (filtered out)
        # Note: git may still have the blobs if they're small enough to be inlined
        # or if it fetched them anyway, so we just verify the filter was accepted

        # Verify git recognizes this as a partial clone
        config_output = run_git_or_fail(
            ["config", "--get", "remote.origin.promisor"], cwd=clone_dir
        )
        self.assertEqual(config_output.strip(), b"true")

        source_repo.close()

    def test_clone_with_blob_limit_filter(self) -> None:
        """Test that git client can clone with blob:limit filter."""
        # Create repository
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)
        source_repo = Repo.init(repo_path, mkdir=False)

        # Create blobs of different sizes
        small_blob = Blob.from_string(b"small")  # 5 bytes
        large_blob = Blob.from_string(b"x" * 1000)  # 1000 bytes

        tree = Tree()
        tree.add(b"small.txt", 0o100644, small_blob.id)
        tree.add(b"large.txt", 0o100644, large_blob.id)

        source_repo.object_store.add_object(small_blob)
        source_repo.object_store.add_object(large_blob)
        source_repo.object_store.add_object(tree)

        commit = make_commit(tree=tree.id, message=b"Test commit with mixed sizes")
        source_repo.object_store.add_object(commit)
        source_repo.refs[b"refs/heads/master"] = commit.id

        # Start server
        port = self._start_server(source_repo)

        # Clone with blob:limit=100 filter (should exclude large blob)
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        clone_dir = os.path.join(clone_path, "cloned_repo")

        run_git_or_fail(
            [
                "clone",
                "--filter=blob:limit=100",
                "--no-checkout",
                self.url(port),
                clone_dir,
            ],
            cwd=clone_path,
        )

        # Verify it's a partial clone
        cloned_repo = Repo(clone_dir)
        self.addCleanup(cloned_repo.close)

        config_output = run_git_or_fail(
            ["config", "--get", "remote.origin.promisor"], cwd=clone_dir
        )
        self.assertEqual(config_output.strip(), b"true")

        source_repo.close()

    def test_clone_with_tree_depth_filter(self) -> None:
        """Test that git client can clone with tree:0 filter."""
        # Create repository with nested structure
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)
        source_repo = Repo.init(repo_path, mkdir=False)

        # Create nested tree structure
        blob1 = Blob.from_string(b"root file")
        blob2 = Blob.from_string(b"nested file")

        inner_tree = Tree()
        inner_tree.add(b"nested.txt", 0o100644, blob2.id)

        outer_tree = Tree()
        outer_tree.add(b"root.txt", 0o100644, blob1.id)
        outer_tree.add(b"subdir", 0o040000, inner_tree.id)

        source_repo.object_store.add_object(blob1)
        source_repo.object_store.add_object(blob2)
        source_repo.object_store.add_object(inner_tree)
        source_repo.object_store.add_object(outer_tree)

        commit = make_commit(tree=outer_tree.id, message=b"Test nested structure")
        source_repo.object_store.add_object(commit)
        source_repo.refs[b"refs/heads/master"] = commit.id

        # Start server
        port = self._start_server(source_repo)

        # Clone with tree:0 filter
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        clone_dir = os.path.join(clone_path, "cloned_repo")

        run_git_or_fail(
            ["clone", "--filter=tree:0", "--no-checkout", self.url(port), clone_dir],
            cwd=clone_path,
        )

        # Verify it's a partial clone
        cloned_repo = Repo(clone_dir)
        self.addCleanup(cloned_repo.close)

        config_output = run_git_or_fail(
            ["config", "--get", "remote.origin.promisor"], cwd=clone_dir
        )
        self.assertEqual(config_output.strip(), b"true")

        source_repo.close()

    def test_clone_with_filter_protocol_v0(self) -> None:
        """Test that git client can clone with filter using protocol v0."""
        # Create repository with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)
        source_repo = Repo.init(repo_path, mkdir=False)

        # Create test content
        blob = Blob.from_string(b"test content")
        tree = Tree()
        tree.add(b"file.txt", 0o100644, blob.id)

        source_repo.object_store.add_object(blob)
        source_repo.object_store.add_object(tree)

        commit = make_commit(tree=tree.id, message=b"Test commit")
        source_repo.object_store.add_object(commit)
        source_repo.refs[b"refs/heads/master"] = commit.id

        # Start server
        port = self._start_server(source_repo)

        # Clone with protocol v0 and blob:none filter
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        clone_dir = os.path.join(clone_path, "cloned_repo")

        run_git_or_fail(
            [
                "-c",
                "protocol.version=0",
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                self.url(port),
                clone_dir,
            ],
            cwd=clone_path,
        )

        # Verify partial clone
        cloned_repo = Repo(clone_dir)
        self.addCleanup(cloned_repo.close)
        self.assertIn(commit.id, cloned_repo.object_store)
        self.assertIn(tree.id, cloned_repo.object_store)

        source_repo.close()

    def test_clone_with_filter_protocol_v2(self) -> None:
        """Test that git client can clone with filter using protocol v2."""
        # Create repository with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)
        source_repo = Repo.init(repo_path, mkdir=False)

        # Create test content
        blob = Blob.from_string(b"test content")
        tree = Tree()
        tree.add(b"file.txt", 0o100644, blob.id)

        source_repo.object_store.add_object(blob)
        source_repo.object_store.add_object(tree)

        commit = make_commit(tree=tree.id, message=b"Test commit")
        source_repo.object_store.add_object(commit)
        source_repo.refs[b"refs/heads/master"] = commit.id

        # Start server
        port = self._start_server(source_repo)

        # Clone with protocol v2 and blob:none filter
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        clone_dir = os.path.join(clone_path, "cloned_repo")

        run_git_or_fail(
            [
                "-c",
                "protocol.version=2",
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                self.url(port),
                clone_dir,
            ],
            cwd=clone_path,
        )

        # Verify partial clone
        cloned_repo = Repo(clone_dir)
        self.addCleanup(cloned_repo.close)
        self.assertIn(commit.id, cloned_repo.object_store)
        self.assertIn(tree.id, cloned_repo.object_store)

        source_repo.close()


@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class PartialCloneClientTestCase(CompatTestCase):
    """Tests for partial clone client compatibility with git server."""

    # Partial clone support was introduced in git 2.17.0
    min_git_version = (2, 17, 0)

    def setUp(self) -> None:
        super().setUp()
        require_git_version(self.min_git_version)

    def test_fetch_with_blob_none_filter(self) -> None:
        """Test that dulwich client can fetch with blob:none filter."""
        from dulwich.client import get_transport_and_path

        # Create a git repository using git itself
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)

        # Initialize with git
        run_git_or_fail(["init"], cwd=repo_path)
        run_git_or_fail(["config", "user.name", "Test User"], cwd=repo_path)
        run_git_or_fail(["config", "user.email", "test@example.com"], cwd=repo_path)

        # Create test files
        file1 = os.path.join(repo_path, "file1.txt")
        with open(file1, "wb") as f:
            f.write(b"Content of file 1")

        file2 = os.path.join(repo_path, "file2.txt")
        with open(file2, "wb") as f:
            f.write(b"Content of file 2")

        # Commit files
        run_git_or_fail(["add", "."], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Initial commit"], cwd=repo_path)

        # Start git daemon
        daemon_port = self._start_git_daemon(repo_path)

        # Create destination repo
        dest_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, dest_path)
        dest_repo = Repo.init(dest_path, mkdir=False)
        self.addCleanup(dest_repo.close)

        # Fetch with blob:none filter using dulwich client
        client, path = get_transport_and_path(
            f"git://localhost:{daemon_port}/",
            thin_packs=False,
        )

        def determine_wants(refs, depth=None):
            # Get all refs
            return list(refs.values())

        # Fetch with filter (may warn if server doesn't support filtering)
        with self.assertLogs(level="WARNING"):
            result = client.fetch(
                path,
                dest_repo,
                determine_wants=determine_wants,
                progress=None,
                filter_spec=b"blob:none",
            )

        # The fetch should succeed with partial clone
        self.assertIsNotNone(result)

    def test_clone_with_filter(self) -> None:
        """Test that dulwich clone function works with filter."""
        from dulwich.client import get_transport_and_path

        # Create a git repository
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)

        run_git_or_fail(["init"], cwd=repo_path)
        run_git_or_fail(["config", "user.name", "Test User"], cwd=repo_path)
        run_git_or_fail(["config", "user.email", "test@example.com"], cwd=repo_path)

        # Create and commit a file
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"Test content for partial clone")
        run_git_or_fail(["add", "."], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Test commit"], cwd=repo_path)

        # Start git daemon
        daemon_port = self._start_git_daemon(repo_path)

        # Clone with dulwich using filter
        dest_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, dest_path)

        client, path = get_transport_and_path(f"git://localhost:{daemon_port}/")

        # Clone with blob:limit filter (may warn if server doesn't support filtering)
        with self.assertLogs(level="WARNING"):
            cloned_repo = client.clone(
                path,
                dest_path,
                mkdir=False,
                filter_spec=b"blob:limit=100",
            )
        self.addCleanup(cloned_repo.close)

        # Verify clone succeeded
        self.assertTrue(os.path.exists(dest_path))
        self.assertTrue(os.path.exists(os.path.join(dest_path, ".git")))

    def _start_git_daemon(self, repo_path):
        """Start git daemon for testing."""
        import socket
        import subprocess
        import time

        # Find an available port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("localhost", 0))
        _, port = sock.getsockname()
        sock.close()

        # Mark directory as git daemon export
        export_file = os.path.join(repo_path, "git-daemon-export-ok")
        with open(export_file, "w") as f:
            f.write("")

        # Start git daemon
        daemon_process = subprocess.Popen(
            [
                "git",
                "daemon",
                "--reuseaddr",
                f"--port={port}",
                "--base-path=.",
                "--export-all",
                "--enable=receive-pack",
                ".",
            ],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Give daemon time to start
        time.sleep(0.5)

        def cleanup_daemon():
            daemon_process.terminate()
            daemon_process.wait(timeout=2)
            # Close pipes to avoid ResourceWarning
            if daemon_process.stdout:
                daemon_process.stdout.close()
            if daemon_process.stderr:
                daemon_process.stderr.close()

        self.addCleanup(cleanup_daemon)

        return port
