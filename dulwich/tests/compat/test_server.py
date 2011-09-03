# test_server.py -- Compatibility tests for git server.
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

"""Compatibility tests between Dulwich and the cgit server.

Warning: these tests should be fairly stable, but when writing/debugging new
    tests, deadlocks may freeze the test process such that it cannot be
    Ctrl-C'ed. On POSIX systems, you can kill the tests with Ctrl-Z, "kill %".
"""

import threading

from dulwich.server import (
    DictBackend,
    TCPGitServer,
    )
from dulwich.tests.compat.server_utils import (
    ServerTests,
    ShutdownServerMixIn,
    NoSideBand64kReceivePackHandler,
    )
from dulwich.tests.compat.utils import (
    CompatTestCase,
    )


if not getattr(TCPGitServer, 'shutdown', None):
    _TCPGitServer = TCPGitServer

    class TCPGitServer(ShutdownServerMixIn, TCPGitServer):
        """Subclass of TCPGitServer that can be shut down."""

        def __init__(self, *args, **kwargs):
            # BaseServer is old-style so we have to call both __init__s
            ShutdownServerMixIn.__init__(self)
            _TCPGitServer.__init__(self, *args, **kwargs)

        serve = ShutdownServerMixIn.serve_forever


class GitServerTestCase(ServerTests, CompatTestCase):
    """Tests for client/server compatibility.

    This server test case does not use side-band-64k in git-receive-pack.
    """

    protocol = 'git'

    def _handlers(self):
        return {'git-receive-pack': NoSideBand64kReceivePackHandler}

    def _check_server(self, dul_server):
        receive_pack_handler_cls = dul_server.handlers['git-receive-pack']
        caps = receive_pack_handler_cls.capabilities()
        self.assertFalse('side-band-64k' in caps)

    def _start_server(self, repo):
        backend = DictBackend({'/': repo})
        dul_server = TCPGitServer(backend, 'localhost', 0,
                                  handlers=self._handlers())
        self._check_server(dul_server)
        self.addCleanup(dul_server.shutdown)
        threading.Thread(target=dul_server.serve).start()
        self._server = dul_server
        _, port = self._server.socket.getsockname()
        return port


class GitServerSideBand64kTestCase(GitServerTestCase):
    """Tests for client/server compatibility with side-band-64k support."""

    # side-band-64k in git-receive-pack was introduced in git 1.7.0.2
    min_git_version = (1, 7, 0, 2)

    def _handlers(self):
        return None  # default handlers include side-band-64k

    def _check_server(self, server):
        receive_pack_handler_cls = server.handlers['git-receive-pack']
        caps = receive_pack_handler_cls.capabilities()
        self.assertTrue('side-band-64k' in caps)
