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
import os
import sys

from dulwich.server import (
    DictBackend,
    TCPGitServer,
    )
from dulwich.tests import skipIf
from dulwich.tests.compat.server_utils import (
    ServerTests,
    NoSideBand64kReceivePackHandler,
    )
from dulwich.tests.compat.utils import (
    CompatTestCase,
    require_git_version,
    )

@skipIf(sys.platform == 'win32', 'Broken on windows, with very long fail time.')
class GitServerTestCase(ServerTests, CompatTestCase):
    """Tests for client/server compatibility.

    This server test case does not use side-band-64k in git-receive-pack.
    """

    protocol = 'git'

    def _handlers(self):
        return {b'git-receive-pack': NoSideBand64kReceivePackHandler}

    def _check_server(self, dul_server):
        receive_pack_handler_cls = dul_server.handlers[b'git-receive-pack']
        caps = receive_pack_handler_cls.capabilities()
        self.assertFalse(b'side-band-64k' in caps)

    def _start_server(self, repo):
        backend = DictBackend({b'/': repo})
        dul_server = TCPGitServer(backend, b'localhost', 0,
                                  handlers=self._handlers())
        self._check_server(dul_server)
        self.addCleanup(dul_server.shutdown)
        self.addCleanup(dul_server.server_close)
        threading.Thread(target=dul_server.serve).start()
        self._server = dul_server
        _, port = self._server.socket.getsockname()
        return port


@skipIf(sys.platform == 'win32', 'Broken on windows, with very long fail time.')
class GitServerSideBand64kTestCase(GitServerTestCase):
    """Tests for client/server compatibility with side-band-64k support."""

    # side-band-64k in git-receive-pack was introduced in git 1.7.0.2
    min_git_version = (1, 7, 0, 2)

    def setUp(self):
        super(GitServerSideBand64kTestCase, self).setUp()
        # side-band-64k is broken in the widows client.
        # https://github.com/msysgit/git/issues/101
        # Fix has landed for the 1.9.3 release.
        if os.name == 'nt':
            require_git_version((1, 9, 3))


    def _handlers(self):
        return None  # default handlers include side-band-64k

    def _check_server(self, server):
        receive_pack_handler_cls = server.handlers[b'git-receive-pack']
        caps = receive_pack_handler_cls.capabilities()
        self.assertTrue(b'side-band-64k' in caps)
