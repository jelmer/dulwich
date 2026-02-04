# test_bundle_uri.py -- test bundle URI compatibility with CGit
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

"""Tests for bundle URI compatibility with CGit.

Bundle URIs were introduced in Git 2.38.
"""

import os
import tempfile
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler

from dulwich.bundle import create_bundle_from_repo, write_bundle
from dulwich.bundle_uri import parse_bundle_list
from dulwich.client import BundleClient
from dulwich.repo import MemoryRepo, Repo

from .utils import CompatTestCase, rmtree_ro, run_git_or_fail


class BundleURICompatTestCase(CompatTestCase):
    """Test bundle URI compatibility with CGit.

    Bundle URIs require Git 2.38 or later.
    """

    min_git_version = (2, 38, 0)

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.test_dir)

    def test_bundle_list_format_compat(self) -> None:
        """Test that bundle list format is compatible with git's expectations."""
        # Create a bundle list in the format git expects
        bundle_list_content = b"""[bundle]
\tversion = 1
\tmode = all

[bundle "main"]
\turi = https://example.com/main.bundle
\tcreationToken = 1700000000
"""
        # Parse with dulwich
        bundle_list = parse_bundle_list(bundle_list_content)

        # Verify parsed correctly
        self.assertEqual(bundle_list.version, 1)
        self.assertEqual(bundle_list.mode, "all")
        self.assertEqual(len(bundle_list.entries), 1)
        self.assertEqual(bundle_list.entries[0].id, "main")
        self.assertEqual(bundle_list.entries[0].uri, "https://example.com/main.bundle")
        self.assertEqual(bundle_list.entries[0].creation_token, 1700000000)

    def test_bundle_list_any_mode_compat(self) -> None:
        """Test bundle list with 'any' mode (geographic distribution)."""
        bundle_list_content = b"""[bundle]
\tversion = 1
\tmode = any

[bundle "us-east"]
\turi = https://us-east.example.com/repo.bundle
\tlocation = us-east

[bundle "eu-west"]
\turi = https://eu-west.example.com/repo.bundle
\tlocation = eu-west
"""
        bundle_list = parse_bundle_list(bundle_list_content)

        self.assertEqual(bundle_list.mode, "any")
        self.assertEqual(len(bundle_list.entries), 2)

        locations = {e.location for e in bundle_list.entries}
        self.assertEqual(locations, {"us-east", "eu-west"})

    def test_bundle_list_with_filter_compat(self) -> None:
        """Test bundle list with partial clone filters."""
        bundle_list_content = b"""[bundle]
\tversion = 1
\tmode = all

[bundle "full"]
\turi = https://example.com/full.bundle
\tcreationToken = 1700000000

[bundle "blobless"]
\turi = https://example.com/blobless.bundle
\tcreationToken = 1700000000
\tfilter = blob:none
"""
        bundle_list = parse_bundle_list(bundle_list_content)

        self.assertEqual(len(bundle_list.entries), 2)

        full = next(e for e in bundle_list.entries if e.id == "full")
        self.assertIsNone(full.filter)

        blobless = next(e for e in bundle_list.entries if e.id == "blobless")
        self.assertEqual(blobless.filter, "blob:none")

    def test_bundle_list_with_heuristic_compat(self) -> None:
        """Test bundle list with creationToken heuristic."""
        bundle_list_content = b"""[bundle]
\tversion = 1
\tmode = all
\theuristic = creationToken

[bundle "2024-01-daily"]
\turi = https://example.com/2024-01-daily.bundle
\tcreationToken = 1704067200

[bundle "2024-01-weekly"]
\turi = https://example.com/2024-01-weekly.bundle
\tcreationToken = 1703462400

[bundle "base"]
\turi = https://example.com/base.bundle
\tcreationToken = 1700000000
"""
        bundle_list = parse_bundle_list(bundle_list_content)

        self.assertEqual(bundle_list.heuristic, "creationToken")
        self.assertEqual(len(bundle_list.entries), 3)

        # Verify creation tokens are parsed correctly
        tokens = {e.id: e.creation_token for e in bundle_list.entries}
        self.assertEqual(tokens["2024-01-daily"], 1704067200)
        self.assertEqual(tokens["2024-01-weekly"], 1703462400)
        self.assertEqual(tokens["base"], 1700000000)

    def test_bundle_from_git_fetch_with_bundle_client(self) -> None:
        """Test fetching a bundle created by git using BundleClient."""
        # Create a repository with git
        repo_path = os.path.join(self.test_dir, "source_repo")
        os.makedirs(repo_path)
        run_git_or_fail(["init"], cwd=repo_path)
        run_git_or_fail(["config", "user.name", "Test User"], cwd=repo_path)
        run_git_or_fail(["config", "user.email", "test@example.com"], cwd=repo_path)

        # Create some commits
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("Hello, World!\n")
        run_git_or_fail(["add", "test.txt"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Initial commit"], cwd=repo_path)

        with open(test_file, "a") as f:
            f.write("More content\n")
        run_git_or_fail(["add", "test.txt"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Second commit"], cwd=repo_path)

        # Create bundle with git
        bundle_path = os.path.join(self.test_dir, "test.bundle")
        run_git_or_fail(
            ["bundle", "create", bundle_path, "HEAD", "master"], cwd=repo_path
        )

        # Use BundleClient to fetch from the bundle
        client = BundleClient()
        target_repo = MemoryRepo()
        self.addCleanup(target_repo.close)

        result = client.fetch(bundle_path, target_repo)

        # Verify refs were fetched
        self.assertIn(b"refs/heads/master", result.refs)

        # Verify objects were fetched
        master_sha = result.refs[b"refs/heads/master"]
        self.assertIn(master_sha, target_repo.object_store)

        # Verify commit content
        commit = target_repo.object_store[master_sha]
        self.assertEqual(commit.message, b"Second commit\n")

    def test_dulwich_bundle_readable_by_git(self) -> None:
        """Test that bundles created by dulwich can be read by git."""
        # Create a repository with dulwich
        repo_path = os.path.join(self.test_dir, "dulwich_repo")
        repo = Repo.init(repo_path, mkdir=True)
        self.addCleanup(repo.close)

        # Create objects using git (easier for creating valid commits)
        run_git_or_fail(["config", "user.name", "Test User"], cwd=repo_path)
        run_git_or_fail(["config", "user.email", "test@example.com"], cwd=repo_path)

        test_file = os.path.join(repo_path, "file.txt")
        with open(test_file, "w") as f:
            f.write("Test content\n")
        run_git_or_fail(["add", "file.txt"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Test commit"], cwd=repo_path)

        # Re-open repo to pick up changes
        repo.close()
        repo = Repo(repo_path)
        self.addCleanup(repo.close)

        # Create bundle with dulwich
        bundle_path = os.path.join(self.test_dir, "dulwich.bundle")
        bundle = create_bundle_from_repo(repo)
        self.addCleanup(bundle.close)

        with open(bundle_path, "wb") as f:
            write_bundle(f, bundle)

        # Verify git can verify the bundle (run_git_or_fail will raise if it fails)
        run_git_or_fail(["bundle", "verify", bundle_path], cwd=repo_path)

        # Clone from the dulwich-created bundle using git - this is the real test
        clone_path = os.path.join(self.test_dir, "git_clone")
        run_git_or_fail(["clone", bundle_path, clone_path])

        # Verify clone succeeded by checking the file content
        cloned_file = os.path.join(clone_path, "file.txt")
        with open(cloned_file) as f:
            self.assertEqual(f.read(), "Test content\n")

    def test_bundle_list_relative_uris(self) -> None:
        """Test that relative URIs in bundle lists are resolved correctly."""
        bundle_list_content = b"""[bundle]
\tversion = 1
\tmode = all

[bundle "relative"]
\turi = bundles/main.bundle
\tcreationToken = 1700000000

[bundle "absolute-path"]
\turi = /repo/bundles/backup.bundle
\tcreationToken = 1699000000
"""
        base_uri = "https://example.com/git/myrepo/"
        bundle_list = parse_bundle_list(bundle_list_content, base_uri=base_uri)

        relative_entry = next(e for e in bundle_list.entries if e.id == "relative")
        self.assertEqual(
            relative_entry.uri, "https://example.com/git/myrepo/bundles/main.bundle"
        )

        absolute_entry = next(e for e in bundle_list.entries if e.id == "absolute-path")
        self.assertEqual(
            absolute_entry.uri, "https://example.com/repo/bundles/backup.bundle"
        )

    def test_incremental_bundle_with_prerequisites(self) -> None:
        """Test incremental bundles with prerequisites work correctly."""
        # Create a repository with multiple commits
        repo_path = os.path.join(self.test_dir, "incremental_repo")
        os.makedirs(repo_path)
        run_git_or_fail(["init"], cwd=repo_path)
        run_git_or_fail(["config", "user.name", "Test User"], cwd=repo_path)
        run_git_or_fail(["config", "user.email", "test@example.com"], cwd=repo_path)

        # Create base commit
        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("Line 1\n")
        run_git_or_fail(["add", "test.txt"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Commit 1"], cwd=repo_path)

        base_commit = run_git_or_fail(["rev-parse", "HEAD"], cwd=repo_path).strip()

        # Create full bundle (base) - must include a ref
        base_bundle_path = os.path.join(self.test_dir, "base.bundle")
        run_git_or_fail(
            ["bundle", "create", base_bundle_path, "HEAD", "master"], cwd=repo_path
        )

        # Create more commits for incremental bundle
        with open(test_file, "a") as f:
            f.write("Line 2\n")
        run_git_or_fail(["add", "test.txt"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Commit 2"], cwd=repo_path)

        with open(test_file, "a") as f:
            f.write("Line 3\n")
        run_git_or_fail(["add", "test.txt"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Commit 3"], cwd=repo_path)

        # Create incremental bundle with prerequisite
        incremental_bundle_path = os.path.join(self.test_dir, "incremental.bundle")
        run_git_or_fail(
            [
                "bundle",
                "create",
                incremental_bundle_path,
                f"{base_commit.decode()}..HEAD",
            ],
            cwd=repo_path,
        )

        # Clone using base bundle first, then apply incremental
        clone_path = os.path.join(self.test_dir, "incremental_clone")
        run_git_or_fail(["clone", base_bundle_path, clone_path])

        # Fetch from incremental bundle
        run_git_or_fail(["fetch", incremental_bundle_path, "HEAD"], cwd=clone_path)

        # Verify both bundles were applied
        output = run_git_or_fail(["log", "--oneline", "FETCH_HEAD"], cwd=clone_path)
        # Parse log output - format is "<short-sha> <message>" per line
        log_lines = output.strip().split(b"\n")
        commit_messages = [line.split(b" ", 1)[1] for line in log_lines if b" " in line]
        # Should have all 3 commits in reverse chronological order
        self.assertEqual(commit_messages[0], b"Commit 3")
        self.assertEqual(commit_messages[1], b"Commit 2")
        self.assertEqual(commit_messages[2], b"Commit 1")


class BundleURIHTTPCompatTestCase(CompatTestCase):
    """Test bundle URI over HTTP compatibility."""

    min_git_version = (2, 38, 0)

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self.test_dir)
        self.server = None
        self.server_thread = None

    def tearDown(self) -> None:
        if self.server:
            self.server.shutdown()
        if self.server_thread:
            self.server_thread.join(timeout=5)
        super().tearDown()

    def _start_http_server(self, serve_dir: str) -> str:
        """Start a simple HTTP server and return its URL."""
        import socket
        import time

        class QuietHandler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=serve_dir, **kwargs)

            def log_message(self, format, *args):
                pass  # Suppress logging

        self.server = HTTPServer(("127.0.0.1", 0), QuietHandler)
        port = self.server.server_address[1]

        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

        # Wait for server to be ready
        url = f"http://127.0.0.1:{port}"
        for _ in range(50):  # Try for up to 5 seconds
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.1)
                sock.connect(("127.0.0.1", port))
                sock.close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError("HTTP server failed to start")

        return url

    def test_fetch_bundle_from_http(self) -> None:
        """Test fetching a bundle from HTTP URL."""
        # Create a repository and bundle
        repo_path = os.path.join(self.test_dir, "source")
        os.makedirs(repo_path)
        run_git_or_fail(["init"], cwd=repo_path)
        run_git_or_fail(["config", "user.name", "Test User"], cwd=repo_path)
        run_git_or_fail(["config", "user.email", "test@example.com"], cwd=repo_path)

        test_file = os.path.join(repo_path, "README.md")
        with open(test_file, "w") as f:
            f.write("# Test Repository\n")
        run_git_or_fail(["add", "README.md"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Initial commit"], cwd=repo_path)

        # Create bundle
        serve_dir = os.path.join(self.test_dir, "serve")
        os.makedirs(serve_dir)
        bundle_path = os.path.join(serve_dir, "repo.bundle")
        run_git_or_fail(
            ["bundle", "create", bundle_path, "HEAD", "master"], cwd=repo_path
        )

        # Start HTTP server
        base_url = self._start_http_server(serve_dir)

        # Fetch bundle using dulwich
        from dulwich.bundle_uri import fetch_bundle_uri

        bundle, bundle_list = fetch_bundle_uri(f"{base_url}/repo.bundle")

        # Should return a bundle, not a bundle list
        self.assertIsNotNone(bundle)
        self.assertIsNone(bundle_list)
        self.addCleanup(bundle.close)

        # Verify bundle contents
        self.assertIn(b"refs/heads/master", bundle.references)

    def test_fetch_bundle_list_from_http(self) -> None:
        """Test fetching a bundle list from HTTP URL."""
        # Create serve directory with bundle list
        serve_dir = os.path.join(self.test_dir, "serve")
        os.makedirs(serve_dir)

        # Create a simple repository and bundle
        repo_path = os.path.join(self.test_dir, "source")
        os.makedirs(repo_path)
        run_git_or_fail(["init"], cwd=repo_path)
        run_git_or_fail(["config", "user.name", "Test User"], cwd=repo_path)
        run_git_or_fail(["config", "user.email", "test@example.com"], cwd=repo_path)

        test_file = os.path.join(repo_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test\n")
        run_git_or_fail(["add", "test.txt"], cwd=repo_path)
        run_git_or_fail(["commit", "-m", "Test"], cwd=repo_path)

        bundle_path = os.path.join(serve_dir, "main.bundle")
        run_git_or_fail(
            ["bundle", "create", bundle_path, "HEAD", "master"], cwd=repo_path
        )

        # Start HTTP server first to get URL
        base_url = self._start_http_server(serve_dir)

        # Create bundle list file
        bundle_list_content = f"""[bundle]
\tversion = 1
\tmode = all

[bundle "main"]
\turi = {base_url}/main.bundle
\tcreationToken = 1700000000
"""
        bundle_list_path = os.path.join(serve_dir, "bundle-list")
        with open(bundle_list_path, "w") as f:
            f.write(bundle_list_content)

        # Fetch bundle list
        from dulwich.bundle_uri import fetch_bundle_uri

        bundle, bundle_list = fetch_bundle_uri(f"{base_url}/bundle-list")

        # Should return a bundle list, not a bundle
        self.assertIsNone(bundle)
        self.assertIsNotNone(bundle_list)

        # Verify bundle list contents
        self.assertEqual(bundle_list.version, 1)
        self.assertEqual(bundle_list.mode, "all")
        self.assertEqual(len(bundle_list.entries), 1)
        self.assertEqual(bundle_list.entries[0].id, "main")
