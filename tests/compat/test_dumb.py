# test_dumb.py -- Compatibility tests for dumb HTTP git repositories
# Copyright (C) 2025 Dulwich contributors
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

"""Compatibility tests for dumb HTTP git repositories."""

import os
import sys
import tempfile
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from unittest import skipUnless

from dulwich.client import HttpGitClient
from dulwich.repo import Repo
from tests.compat.utils import (
    CompatTestCase,
    rmtree_ro,
    run_git_or_fail,
)


class DumbHTTPRequestHandler(SimpleHTTPRequestHandler):
    """HTTP request handler for dumb git protocol."""

    def __init__(self, *args, directory=None, **kwargs):
        self.directory = directory
        super().__init__(*args, directory=directory, **kwargs)

    def log_message(self, format, *args):
        # Suppress logging during tests
        pass


class DumbHTTPGitServer:
    """Simple HTTP server for serving git repositories."""

    def __init__(self, root_path, port=0):
        self.root_path = root_path

        def handler(*args, **kwargs):
            return DumbHTTPRequestHandler(*args, directory=root_path, **kwargs)

        self.server = HTTPServer(("127.0.0.1", port), handler)
        self.server.allow_reuse_address = True
        self.port = self.server.server_port
        self.thread = None

    def start(self):
        """Start the HTTP server in a background thread."""
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()

        # Give the server a moment to start and verify it's listening
        import socket
        import time

        for i in range(50):  # Try for up to 5 seconds
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.1)
                result = sock.connect_ex(("127.0.0.1", self.port))
                sock.close()
                if result == 0:
                    return  # Server is ready
            except OSError:
                pass
            time.sleep(0.1)

        # If we get here, server failed to start
        raise RuntimeError(f"HTTP server failed to start on port {self.port}")

    def stop(self):
        """Stop the HTTP server."""
        self.server.shutdown()
        if self.thread:
            self.thread.join()

    @property
    def url(self):
        """Get the base URL for this server."""
        return f"http://127.0.0.1:{self.port}"


class DumbHTTPClientTests(CompatTestCase):
    """Tests for dumb HTTP client against real git repositories."""

    def setUp(self):
        super().setUp()
        # Create a temporary directory for test repos
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.temp_dir)

        # Create origin repository
        self.origin_path = os.path.join(self.temp_dir, "origin.git")
        os.mkdir(self.origin_path)
        run_git_or_fail(["init", "--bare"], cwd=self.origin_path)

        # Create a working repository to push from
        self.work_path = os.path.join(self.temp_dir, "work")
        os.mkdir(self.work_path)
        run_git_or_fail(["init"], cwd=self.work_path)
        run_git_or_fail(
            ["config", "user.email", "test@example.com"], cwd=self.work_path
        )
        run_git_or_fail(["config", "user.name", "Test User"], cwd=self.work_path)

        # Create initial commit
        test_file = os.path.join(self.work_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("Hello, world!\n")
        run_git_or_fail(["add", "test.txt"], cwd=self.work_path)
        run_git_or_fail(["commit", "-m", "Initial commit"], cwd=self.work_path)

        # Push to origin
        run_git_or_fail(
            ["remote", "add", "origin", self.origin_path], cwd=self.work_path
        )
        run_git_or_fail(["push", "origin", "master"], cwd=self.work_path)

        # Update server info for dumb HTTP
        run_git_or_fail(["update-server-info"], cwd=self.origin_path)

        # Start HTTP server
        self.server = DumbHTTPGitServer(self.origin_path)
        self.server.start()
        self.addCleanup(self.server.stop)

    def test_clone_from_dumb_http(self):
        """Test cloning from a dumb HTTP server."""
        dest_path = os.path.join(self.temp_dir, "cloned")

        # Use dulwich to clone via dumb HTTP
        client = HttpGitClient(self.server.url)

        # Create destination repo
        dest_repo = Repo.init(dest_path, mkdir=True)

        try:
            # Fetch from dumb HTTP
            def determine_wants(refs):
                return [
                    sha for ref, sha in refs.items() if ref.startswith(b"refs/heads/")
                ]

            result = client.fetch("/", dest_repo, determine_wants=determine_wants)

            # Update refs
            for ref, sha in result.refs.items():
                if ref.startswith(b"refs/heads/"):
                    dest_repo.refs[ref] = sha

            # Checkout files
            dest_repo.reset_index()

            # Verify the clone
            test_file = os.path.join(dest_path, "test.txt")
            self.assertTrue(os.path.exists(test_file))
            with open(test_file) as f:
                self.assertEqual("Hello, world!\n", f.read())
        finally:
            # Ensure repo is closed before cleanup
            dest_repo.close()

    @skipUnless(
        sys.platform != "win32", "git clone from Python HTTPServer fails on Windows"
    )
    def test_fetch_new_commit_from_dumb_http(self):
        """Test fetching new commits from a dumb HTTP server."""
        # First clone the repository
        dest_path = os.path.join(self.temp_dir, "cloned")

        run_git_or_fail(["clone", self.server.url, dest_path])

        # Make a new commit in the origin
        test_file2 = os.path.join(self.work_path, "test2.txt")
        with open(test_file2, "w") as f:
            f.write("Second file\n")
        run_git_or_fail(["add", "test2.txt"], cwd=self.work_path)
        run_git_or_fail(["commit", "-m", "Second commit"], cwd=self.work_path)
        run_git_or_fail(["push", "origin", "master"], cwd=self.work_path)

        # Update server info again
        run_git_or_fail(["update-server-info"], cwd=self.origin_path)

        # Fetch with dulwich client
        client = HttpGitClient(self.server.url)
        dest_repo = Repo(dest_path)

        try:
            old_refs = dest_repo.get_refs()

            def determine_wants(refs):
                wants = []
                for ref, sha in refs.items():
                    if ref.startswith(b"refs/heads/") and sha != old_refs.get(ref):
                        wants.append(sha)
                return wants

            result = client.fetch("/", dest_repo, determine_wants=determine_wants)

            # Update refs
            for ref, sha in result.refs.items():
                if ref.startswith(b"refs/heads/"):
                    dest_repo.refs[ref] = sha

            # Reset to new commit
            dest_repo.reset_index()

            # Verify the new file exists
            test_file2_dest = os.path.join(dest_path, "test2.txt")
            self.assertTrue(os.path.exists(test_file2_dest))
            with open(test_file2_dest) as f:
                self.assertEqual("Second file\n", f.read())
        finally:
            # Ensure repo is closed before cleanup
            dest_repo.close()

    @skipUnless(
        os.name == "posix", "Skipping on non-POSIX systems due to permission handling"
    )
    def test_fetch_from_dumb_http_with_tags(self):
        """Test fetching tags from a dumb HTTP server."""
        # Create a tag in origin
        run_git_or_fail(["tag", "-a", "v1.0", "-m", "Version 1.0"], cwd=self.work_path)
        run_git_or_fail(["push", "origin", "v1.0"], cwd=self.work_path)

        # Update server info
        run_git_or_fail(["update-server-info"], cwd=self.origin_path)

        # Clone with dulwich
        dest_path = os.path.join(self.temp_dir, "cloned_with_tags")
        dest_repo = Repo.init(dest_path, mkdir=True)

        try:
            client = HttpGitClient(self.server.url)

            def determine_wants(refs):
                return [
                    sha
                    for ref, sha in refs.items()
                    if ref.startswith((b"refs/heads/", b"refs/tags/"))
                ]

            result = client.fetch("/", dest_repo, determine_wants=determine_wants)

            # Update refs
            for ref, sha in result.refs.items():
                dest_repo.refs[ref] = sha

            # Check that the tag exists
            self.assertIn(b"refs/tags/v1.0", dest_repo.refs)

            # Verify tag points to the right commit
            tag_sha = dest_repo.refs[b"refs/tags/v1.0"]
            tag_obj = dest_repo[tag_sha]
            self.assertEqual(b"tag", tag_obj.type_name)
        finally:
            # Ensure repo is closed before cleanup
            dest_repo.close()
