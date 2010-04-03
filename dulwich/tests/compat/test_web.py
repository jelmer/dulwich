# test_web.py -- Compatibilty tests for the git web server.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

"""Compatibilty tests between Dulwich and the cgit HTTP server.

Warning: these tests should be fairly stable, but when writing/debugging new
tests, deadlocks may freeze the test process such that it cannot be Ctrl-C'ed.
On *nix, you can kill the tests with Ctrl-Z, "kill %".
"""

import threading
from wsgiref import simple_server

from dulwich.server import (
    DictBackend,
    )
from dulwich.web import (
    HTTPGitApplication,
    )

from server_utils import (
    ServerTests,
    ShutdownServerMixIn,
    )
from utils import (
    CompatTestCase,
    SkipTest,
    )


if getattr(simple_server.WSGIServer, 'shutdown', None):
    WSGIServer = simple_server.WSGIServer
else:
    class WSGIServer(ShutdownServerMixIn, simple_server.WSGIServer):
        """Subclass of WSGIServer that can be shut down."""

        def __init__(self, *args, **kwargs):
            # BaseServer is old-style so we have to call both __init__s
            ShutdownServerMixIn.__init__(self)
            simple_server.WSGIServer.__init__(self, *args, **kwargs)

        serve = ShutdownServerMixIn.serve_forever


class WebTests(ServerTests):
    """Base tests for web server tests.

    Contains utility and setUp/tearDown methods, but does non inherit from
    TestCase so tests are not automatically run.
    """

    protocol = 'http'

    def _start_server(self, repo):
        backend = DictBackend({'/': repo})
        app = self._make_app(backend)
        dul_server = simple_server.make_server('localhost', 0, app,
                                               server_class=WSGIServer)
        threading.Thread(target=dul_server.serve_forever).start()
        self._server = dul_server
        _, port = dul_server.socket.getsockname()
        return port


class SmartWebTestCase(WebTests, CompatTestCase):
    """Test cases for smart HTTP server."""

    min_git_version = (1, 6, 6)

    def setUp(self):
        WebTests.setUp(self)
        CompatTestCase.setUp(self)

    def tearDown(self):
        WebTests.tearDown(self)
        CompatTestCase.tearDown(self)

    def _make_app(self, backend):
        return HTTPGitApplication(backend)

    def test_push_to_dulwich(self):
        # TODO(dborowitz): enable after merging thin pack fixes.
        raise SkipTest('Skipping push test due to known pack bug.')


class DumbWebTestCase(WebTests, CompatTestCase):
    """Test cases for dumb HTTP server."""

    def setUp(self):
        WebTests.setUp(self)
        CompatTestCase.setUp(self)

    def tearDown(self):
        WebTests.tearDown(self)
        CompatTestCase.tearDown(self)

    def _make_app(self, backend):
        return HTTPGitApplication(backend, dumb=True)

    def test_push_to_dulwich(self):
        # Note: remove this if dumb pushing is supported
        raise SkipTest('Dumb web pushing not supported.')
