# test_aiohttp.py -- Compatibility tests for the aiohttp HTTP server.
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

"""Compatibility tests between Dulwich and the cgit HTTP client using aiohttp server.

warning: these tests should be fairly stable, but when writing/debugging new
    tests, deadlocks may freeze the test process such that it cannot be
    Ctrl-C'ed. On POSIX systems, you can kill the tests with Ctrl-Z, "kill %".
"""

import asyncio
import sys
import threading
from typing import NoReturn

try:
    from aiohttp import web

    aiohttp_missing = False
except ImportError:
    web = None  # type: ignore
    aiohttp_missing = True

from dulwich.server import ReceivePackHandler

from .. import SkipTest, skipIf
from .server_utils import NoSideBand64kReceivePackHandler, ServerTests
from .utils import CompatTestCase

if not aiohttp_missing:
    from dulwich.aiohttp.server import create_repo_app


@skipIf(aiohttp_missing, "aiohttp not available")
@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class AiohttpServerTests(ServerTests):
    """Base tests for aiohttp server tests.

    Contains utility and setUp/tearDown methods, but does not inherit from
    TestCase so tests are not automatically run.
    """

    protocol = "http"

    def _start_server(self, repo):
        app = self._make_app(repo)
        runner = web.AppRunner(app)

        loop = asyncio.new_event_loop()

        async def start():
            await runner.setup()
            site = web.TCPSite(runner, "localhost", 0)
            await site.start()
            return site

        site = loop.run_until_complete(start())

        # Get the actual port
        port = site._server.sockets[0].getsockname()[1]

        # Run the event loop in a separate thread
        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()

        # Cleanup function
        def cleanup():
            async def stop():
                await runner.cleanup()

            future = asyncio.run_coroutine_threadsafe(stop(), loop)
            future.result(timeout=5)
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=1.0)

        self.addCleanup(cleanup)
        self._server = runner
        return port


@skipIf(aiohttp_missing, "aiohttp not available")
@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class SmartAiohttpTestCase(AiohttpServerTests, CompatTestCase):
    """Test cases for smart HTTP server using aiohttp.

    This server test case does not use side-band-64k in git-receive-pack.
    """

    min_git_version: tuple[int, ...] = (1, 6, 6)

    def _handlers(self):
        return {b"git-receive-pack": NoSideBand64kReceivePackHandler}

    def _make_app(self, repo):
        from dulwich.server import DEFAULT_HANDLERS

        handlers = dict(DEFAULT_HANDLERS)
        handlers.update(self._handlers())
        return create_repo_app(repo, handlers=handlers)


def patch_capabilities(handler, caps_removed):
    # Patch a handler's capabilities by specifying a list of them to be
    # removed, and return the original method for restoration.
    original_capabilities = handler.capabilities

    def capabilities(self):
        # Call original to get base capabilities (including object-format)
        base_caps = original_capabilities(self)
        # Filter out the capabilities we want to remove
        return [i for i in base_caps if i not in caps_removed]

    handler.capabilities = capabilities
    return original_capabilities


@skipIf(aiohttp_missing, "aiohttp not available")
@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class SmartAiohttpSideBand64kTestCase(SmartAiohttpTestCase):
    """Test cases for smart HTTP server with side-band-64k support using aiohttp."""

    # side-band-64k in git-receive-pack was introduced in git 1.7.0.2
    min_git_version = (1, 7, 0, 2)

    def setUp(self) -> None:
        from dulwich.server import UploadPackHandler

        self.o_uph_cap = patch_capabilities(UploadPackHandler, (b"no-done",))
        self.o_rph_cap = patch_capabilities(ReceivePackHandler, (b"no-done",))
        super().setUp()

    def tearDown(self) -> None:
        from dulwich.server import UploadPackHandler

        super().tearDown()
        UploadPackHandler.capabilities = self.o_uph_cap
        ReceivePackHandler.capabilities = self.o_rph_cap

    def _make_app(self, repo):
        return create_repo_app(repo)


@skipIf(aiohttp_missing, "aiohttp not available")
@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class SmartAiohttpSideBand64kNoDoneTestCase(SmartAiohttpTestCase):
    """Test cases for smart HTTP server with side-band-64k and no-done
    support using aiohttp.
    """

    # no-done was introduced in git 1.7.4
    min_git_version = (1, 7, 4)

    def _make_app(self, repo):
        return create_repo_app(repo)


@skipIf(aiohttp_missing, "aiohttp not available")
@skipIf(sys.platform == "win32", "Broken on windows, with very long fail time.")
class DumbAiohttpTestCase(AiohttpServerTests, CompatTestCase):
    """Test cases for dumb HTTP server using aiohttp."""

    def _make_app(self, repo):
        return create_repo_app(repo, dumb=True)

    def test_push_to_dulwich(self) -> NoReturn:
        # Note: remove this if dulwich implements dumb web pushing.
        raise SkipTest("Dumb web pushing not supported.")

    def test_push_to_dulwich_remove_branch(self) -> NoReturn:
        # Note: remove this if dumb pushing is supported
        raise SkipTest("Dumb web pushing not supported.")

    def test_new_shallow_clone_from_dulwich(self) -> NoReturn:
        # Note: remove this if C git and dulwich implement dumb web shallow
        # clones.
        raise SkipTest("Dumb web shallow cloning not supported.")

    def test_shallow_clone_from_git_is_identical(self) -> NoReturn:
        # Note: remove this if C git and dulwich implement dumb web shallow
        # clones.
        raise SkipTest("Dumb web shallow cloning not supported.")

    def test_fetch_same_depth_into_shallow_clone_from_dulwich(self) -> NoReturn:
        # Note: remove this if C git and dulwich implement dumb web shallow
        # clones.
        raise SkipTest("Dumb web shallow cloning not supported.")

    def test_fetch_full_depth_into_shallow_clone_from_dulwich(self) -> NoReturn:
        # Note: remove this if C git and dulwich implement dumb web shallow
        # clones.
        raise SkipTest("Dumb web shallow cloning not supported.")

    def test_push_to_dulwich_issue_88_standard(self) -> NoReturn:
        raise SkipTest("Dumb web pushing not supported.")
