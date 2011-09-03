# server_utils.py -- Git server compatibility utilities
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

"""Utilities for testing git server compatibility."""


import os
import select
import shutil
import socket
import tempfile
import threading

from dulwich.repo import Repo
from dulwich.server import (
    ReceivePackHandler,
    )
from dulwich.tests.utils import (
    tear_down_repo,
    )
from dulwich.tests.compat.utils import (
    import_repo,
    run_git_or_fail,
    )


class ServerTests(object):
    """Base tests for testing servers.

    Does not inherit from TestCase so tests are not automatically run.
    """

    def import_repos(self):
        self._old_repo = import_repo('server_old.export')
        self.addCleanup(tear_down_repo, self._old_repo)
        self._new_repo = import_repo('server_new.export')
        self.addCleanup(tear_down_repo, self._new_repo)

    def url(self, port):
        return '%s://localhost:%s/' % (self.protocol, port)

    def branch_args(self, branches=None):
        if branches is None:
            branches = ['master', 'branch']
        return ['%s:%s' % (b, b) for b in branches]

    def test_push_to_dulwich(self):
        self.import_repos()
        self.assertReposNotEqual(self._old_repo, self._new_repo)
        port = self._start_server(self._old_repo)

        run_git_or_fail(['push', self.url(port)] + self.branch_args(),
                        cwd=self._new_repo.path)
        self.assertReposEqual(self._old_repo, self._new_repo)

    def test_fetch_from_dulwich(self):
        self.import_repos()
        self.assertReposNotEqual(self._old_repo, self._new_repo)
        port = self._start_server(self._new_repo)

        run_git_or_fail(['fetch', self.url(port)] + self.branch_args(),
                        cwd=self._old_repo.path)
        # flush the pack cache so any new packs are picked up
        self._old_repo.object_store._pack_cache = None
        self.assertReposEqual(self._old_repo, self._new_repo)

    def test_fetch_from_dulwich_no_op(self):
        self._old_repo = import_repo('server_old.export')
        self._new_repo = import_repo('server_old.export')
        self.assertReposEqual(self._old_repo, self._new_repo)
        port = self._start_server(self._new_repo)

        run_git_or_fail(['fetch', self.url(port)] + self.branch_args(),
                        cwd=self._old_repo.path)
        # flush the pack cache so any new packs are picked up
        self._old_repo.object_store._pack_cache = None
        self.assertReposEqual(self._old_repo, self._new_repo)

    def test_clone_from_dulwich_empty(self):
        old_repo_dir = os.path.join(tempfile.mkdtemp(), 'empty_old')
        run_git_or_fail(['init', '--quiet', '--bare', old_repo_dir])
        self._old_repo = Repo(old_repo_dir)
        port = self._start_server(self._old_repo)

        new_repo_base_dir = tempfile.mkdtemp()
        try:
            new_repo_dir = os.path.join(new_repo_base_dir, 'empty_new')
            run_git_or_fail(['clone', self.url(port), new_repo_dir],
                            cwd=new_repo_base_dir)
            new_repo = Repo(new_repo_dir)
            self.assertReposEqual(self._old_repo, new_repo)
        finally:
            # We don't create a Repo from new_repo_dir until after some errors
            # may have occurred, so don't depend on tearDown to clean it up.
            shutil.rmtree(new_repo_base_dir)


class ShutdownServerMixIn:
    """Mixin that allows serve_forever to be shut down.

    The methods in this mixin are backported from SocketServer.py in the Python
    2.6.4 standard library. The mixin is unnecessary in 2.6 and later, when
    BaseServer supports the shutdown method directly.
    """

    def __init__(self):
        self.__is_shut_down = threading.Event()
        self.__serving = False

    def serve_forever(self, poll_interval=0.5):
        """Handle one request at a time until shutdown.

        Polls for shutdown every poll_interval seconds. Ignores
        self.timeout. If you need to do periodic tasks, do them in
        another thread.
        """
        self.__serving = True
        self.__is_shut_down.clear()
        while self.__serving:
            # XXX: Consider using another file descriptor or
            # connecting to the socket to wake this up instead of
            # polling. Polling reduces our responsiveness to a
            # shutdown request and wastes cpu at all other times.
            r, w, e = select.select([self], [], [], poll_interval)
            if r:
                self._handle_request_noblock()
        self.__is_shut_down.set()

    serve = serve_forever  # override alias from TCPGitServer

    def shutdown(self):
        """Stops the serve_forever loop.

        Blocks until the loop has finished. This must be called while
        serve_forever() is running in another thread, or it will deadlock.
        """
        self.__serving = False
        self.__is_shut_down.wait()

    def handle_request(self):
        """Handle one request, possibly blocking.

        Respects self.timeout.
        """
        # Support people who used socket.settimeout() to escape
        # handle_request before self.timeout was available.
        timeout = self.socket.gettimeout()
        if timeout is None:
            timeout = self.timeout
        elif self.timeout is not None:
            timeout = min(timeout, self.timeout)
        fd_sets = select.select([self], [], [], timeout)
        if not fd_sets[0]:
            self.handle_timeout()
            return
        self._handle_request_noblock()

    def _handle_request_noblock(self):
        """Handle one request, without blocking.

        I assume that select.select has returned that the socket is
        readable before this function was called, so there should be
        no risk of blocking in get_request().
        """
        try:
            request, client_address = self.get_request()
        except socket.error:
            return
        if self.verify_request(request, client_address):
            try:
                self.process_request(request, client_address)
            except:
                self.handle_error(request, client_address)
                self.close_request(request)


# TODO(dborowitz): Come up with a better way of testing various permutations of
# capabilities. The only reason it is the way it is now is that side-band-64k
# was only recently introduced into git-receive-pack.
class NoSideBand64kReceivePackHandler(ReceivePackHandler):
    """ReceivePackHandler that does not support side-band-64k."""

    @classmethod
    def capabilities(cls):
        return tuple(c for c in ReceivePackHandler.capabilities()
                     if c != 'side-band-64k')
