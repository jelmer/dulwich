# test_server.py -- Compatibility tests for git server.
# Copyright (C) 2010 Google, Inc.
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

"""Compatibility tests between Dulwich and the cgit server.

Warning: these tests should be fairly stable, but when writing/debugging new
    tests, deadlocks may freeze the test process such that it cannot be
    Ctrl-C'ed. On POSIX systems, you can kill the tests with Ctrl-Z, "kill %".
"""

import os
import shutil
import sys
import tempfile
import threading

from dulwich.object_format import SHA256
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo
from dulwich.server import DictBackend, TCPGitServer

from .. import skipIf
from .server_utils import NoSideBand64kReceivePackHandler, ServerTests
from .utils import CompatTestCase, require_git_version, run_git_or_fail


@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class GitServerTestCase(ServerTests, CompatTestCase):
    """Tests for client/server compatibility.

    This server test case does not use side-band-64k in git-receive-pack.
    """

    protocol = "git"

    def _handlers(self):
        return {b"git-receive-pack": NoSideBand64kReceivePackHandler}

    def _check_server(self, dul_server, repo) -> None:
        from dulwich.protocol import Protocol

        receive_pack_handler_cls = dul_server.handlers[b"git-receive-pack"]
        # Create a handler instance to check capabilities
        handler = receive_pack_handler_cls(
            dul_server.backend,
            [b"/"],
            Protocol(lambda x: b"", lambda x: None),
        )
        caps = handler.capabilities()
        self.assertNotIn(b"side-band-64k", caps)

    def _start_server(self, repo):
        backend = DictBackend({b"/": repo})
        dul_server = TCPGitServer(backend, b"localhost", 0, handlers=self._handlers())
        self._check_server(dul_server, repo)

        # Start server in a thread
        server_thread = threading.Thread(target=dul_server.serve)
        server_thread.daemon = True  # Make thread daemon so it dies with main thread
        server_thread.start()

        # Add cleanup in the correct order
        def cleanup_server():
            dul_server.shutdown()
            dul_server.server_close()
            # Give thread a moment to exit cleanly
            server_thread.join(timeout=1.0)

        self.addCleanup(cleanup_server)
        self._server = dul_server
        _, port = self._server.socket.getsockname()
        return port


@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class GitServerSideBand64kTestCase(GitServerTestCase):
    """Tests for client/server compatibility with side-band-64k support."""

    # side-band-64k in git-receive-pack was introduced in git 1.7.0.2
    min_git_version = (1, 7, 0, 2)

    def setUp(self) -> None:
        super().setUp()
        # side-band-64k is broken in the windows client.
        # https://github.com/msysgit/git/issues/101
        # Fix has landed for the 1.9.3 release.
        if os.name == "nt":
            require_git_version((1, 9, 3))

    def _handlers(self) -> None:
        return None  # default handlers include side-band-64k

    def _check_server(self, server, repo) -> None:
        from dulwich.protocol import Protocol

        receive_pack_handler_cls = server.handlers[b"git-receive-pack"]
        # Create a handler instance to check capabilities
        handler = receive_pack_handler_cls(
            server.backend,
            [b"/"],
            Protocol(lambda x: b"", lambda x: None),
        )
        caps = handler.capabilities()
        self.assertIn(b"side-band-64k", caps)


@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class GitServerSHA256TestCase(CompatTestCase):
    """Tests for SHA-256 repository server compatibility with git client."""

    protocol = "git"
    # SHA-256 support was introduced in git 2.29.0
    min_git_version = (2, 29, 0)

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

    def test_clone_sha256_repo_from_dulwich_server(self) -> None:
        """Test that git client can clone SHA-256 repo from dulwich server."""
        # Create SHA-256 repository with dulwich
        repo_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_path)
        source_repo = Repo.init(repo_path, mkdir=False, object_format="sha256")

        # Create test content
        blob = Blob.from_string(b"Test SHA-256 content from dulwich server")
        tree = Tree()
        tree.add(b"test.txt", 0o100644, blob.get_id(SHA256))

        commit = Commit()
        commit.tree = tree.get_id(SHA256)
        commit.author = commit.committer = b"Test User <test@example.com>"
        commit.commit_time = commit.author_time = 1234567890
        commit.commit_timezone = commit.author_timezone = 0
        commit.message = b"Test SHA-256 commit"

        # Add objects to repo
        source_repo.object_store.add_object(blob)
        source_repo.object_store.add_object(tree)
        source_repo.object_store.add_object(commit)

        # Set master ref
        source_repo.refs[b"refs/heads/master"] = commit.get_id(SHA256)

        # Start dulwich server
        port = self._start_server(source_repo)

        # Clone with git client
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        clone_dir = os.path.join(clone_path, "cloned_repo")

        run_git_or_fail(["clone", self.url(port), clone_dir], cwd=clone_path)

        # Verify cloned repo is SHA-256
        cloned_repo = Repo(clone_dir)
        self.addCleanup(cloned_repo.close)
        self.assertEqual(cloned_repo.object_format, SHA256)

        # Verify object format config
        output = run_git_or_fail(
            ["config", "--get", "extensions.objectformat"], cwd=clone_dir
        )
        self.assertEqual(output.strip(), b"sha256")

        # Verify commit was cloned
        cloned_head = cloned_repo.refs[b"refs/heads/master"]
        self.assertEqual(len(cloned_head), 64)  # SHA-256 length
        self.assertEqual(cloned_head, commit.get_id(SHA256))

        # Verify git can read the commit
        log_output = run_git_or_fail(["log", "--format=%s", "-n", "1"], cwd=clone_dir)
        self.assertEqual(log_output.strip(), b"Test SHA-256 commit")

        source_repo.close()
