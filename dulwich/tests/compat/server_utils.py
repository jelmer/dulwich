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

import errno
import os
import shutil
import socket
import tempfile

from dulwich.repo import Repo
from dulwich.objects import hex_to_sha
from dulwich.server import (
    ReceivePackHandler,
    )
from dulwich.tests.utils import (
    tear_down_repo,
    )
from dulwich.tests.compat.utils import (
    run_git_or_fail,
    )
from dulwich.tests.compat.utils import require_git_version


class _StubRepo(object):
    """A stub repo that just contains a path to tear down."""

    def __init__(self, name):
        temp_dir = tempfile.mkdtemp()
        self.path = os.path.join(temp_dir, name)
        os.mkdir(self.path)

    def close(self):
        pass


def _get_shallow(repo):
    shallow_file = repo.get_named_file('shallow')
    if not shallow_file:
        return []
    shallows = []
    with shallow_file:
        for line in shallow_file:
            sha = line.strip()
            if not sha:
                continue
            hex_to_sha(sha)
            shallows.append(sha)
    return shallows


class ServerTests(object):
    """Base tests for testing servers.

    Does not inherit from TestCase so tests are not automatically run.
    """

    min_single_branch_version = (1, 7, 10,)

    def import_repos(self):
        self._old_repo = self.import_repo('server_old.export')
        self._new_repo = self.import_repo('server_new.export')

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

    def test_push_to_dulwich_no_op(self):
        self._old_repo = self.import_repo('server_old.export')
        self._new_repo = self.import_repo('server_old.export')
        self.assertReposEqual(self._old_repo, self._new_repo)
        port = self._start_server(self._old_repo)

        run_git_or_fail(['push', self.url(port)] + self.branch_args(),
                        cwd=self._new_repo.path)
        self.assertReposEqual(self._old_repo, self._new_repo)

    def test_push_to_dulwich_remove_branch(self):
        self._old_repo = self.import_repo('server_old.export')
        self._new_repo = self.import_repo('server_old.export')
        self.assertReposEqual(self._old_repo, self._new_repo)
        port = self._start_server(self._old_repo)

        run_git_or_fail(['push', self.url(port), ":master"],
                        cwd=self._new_repo.path)

        self.assertEqual(
            list(self._old_repo.get_refs().keys()), [b"refs/heads/branch"])

    def test_fetch_from_dulwich(self):
        self.import_repos()
        self.assertReposNotEqual(self._old_repo, self._new_repo)
        port = self._start_server(self._new_repo)

        run_git_or_fail(['fetch', self.url(port)] + self.branch_args(),
                        cwd=self._old_repo.path)
        # flush the pack cache so any new packs are picked up
        self._old_repo.object_store._pack_cache_time = 0
        self.assertReposEqual(self._old_repo, self._new_repo)

    def test_fetch_from_dulwich_no_op(self):
        self._old_repo = self.import_repo('server_old.export')
        self._new_repo = self.import_repo('server_old.export')
        self.assertReposEqual(self._old_repo, self._new_repo)
        port = self._start_server(self._new_repo)

        run_git_or_fail(['fetch', self.url(port)] + self.branch_args(),
                        cwd=self._old_repo.path)
        # flush the pack cache so any new packs are picked up
        self._old_repo.object_store._pack_cache_time = 0
        self.assertReposEqual(self._old_repo, self._new_repo)

    def test_clone_from_dulwich_empty(self):
        old_repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, old_repo_dir)
        self._old_repo = Repo.init_bare(old_repo_dir)
        port = self._start_server(self._old_repo)

        new_repo_base_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, new_repo_base_dir)
        new_repo_dir = os.path.join(new_repo_base_dir, 'empty_new')
        run_git_or_fail(['clone', self.url(port), new_repo_dir],
                        cwd=new_repo_base_dir)
        new_repo = Repo(new_repo_dir)
        self.assertReposEqual(self._old_repo, new_repo)

    def test_lsremote_from_dulwich(self):
        self._repo = self.import_repo('server_old.export')
        port = self._start_server(self._repo)
        o = run_git_or_fail(['ls-remote', self.url(port)])
        self.assertEqual(len(o.split(b'\n')), 4)

    def test_new_shallow_clone_from_dulwich(self):
        require_git_version(self.min_single_branch_version)
        self._source_repo = self.import_repo('server_new.export')
        self._stub_repo = _StubRepo('shallow')
        self.addCleanup(tear_down_repo, self._stub_repo)
        port = self._start_server(self._source_repo)

        # Fetch at depth 1
        run_git_or_fail(['clone', '--mirror', '--depth=1', '--no-single-branch',
                        self.url(port), self._stub_repo.path])
        clone = self._stub_repo = Repo(self._stub_repo.path)
        expected_shallow = [b'35e0b59e187dd72a0af294aedffc213eaa4d03ff',
                            b'514dc6d3fbfe77361bcaef320c4d21b72bc10be9']
        self.assertEqual(expected_shallow, _get_shallow(clone))
        self.assertReposNotEqual(clone, self._source_repo)

    def test_shallow_clone_from_git_is_identical(self):
        require_git_version(self.min_single_branch_version)
        self._source_repo = self.import_repo('server_new.export')
        self._stub_repo_git = _StubRepo('shallow-git')
        self.addCleanup(tear_down_repo, self._stub_repo_git)
        self._stub_repo_dw = _StubRepo('shallow-dw')
        self.addCleanup(tear_down_repo, self._stub_repo_dw)

        # shallow clone using stock git, then using dulwich
        run_git_or_fail(['clone', '--mirror', '--depth=1', '--no-single-branch',
                         'file://' + self._source_repo.path,
                         self._stub_repo_git.path])

        port = self._start_server(self._source_repo)
        run_git_or_fail(['clone', '--mirror', '--depth=1', '--no-single-branch',
                        self.url(port), self._stub_repo_dw.path])

        # compare the two clones; they should be equal
        self.assertReposEqual(Repo(self._stub_repo_git.path),
                              Repo(self._stub_repo_dw.path))

    def test_fetch_same_depth_into_shallow_clone_from_dulwich(self):
        require_git_version(self.min_single_branch_version)
        self._source_repo = self.import_repo('server_new.export')
        self._stub_repo = _StubRepo('shallow')
        self.addCleanup(tear_down_repo, self._stub_repo)
        port = self._start_server(self._source_repo)

        # Fetch at depth 2
        run_git_or_fail(['clone', '--mirror', '--depth=2', '--no-single-branch',
                        self.url(port), self._stub_repo.path])
        clone = self._stub_repo = Repo(self._stub_repo.path)

        # Fetching at the same depth is a no-op.
        run_git_or_fail(
          ['fetch', '--depth=2', self.url(port)] + self.branch_args(),
          cwd=self._stub_repo.path)
        expected_shallow = [b'94de09a530df27ac3bb613aaecdd539e0a0655e1',
                            b'da5cd81e1883c62a25bb37c4d1f8ad965b29bf8d']
        self.assertEqual(expected_shallow, _get_shallow(clone))
        self.assertReposNotEqual(clone, self._source_repo)

    def test_fetch_full_depth_into_shallow_clone_from_dulwich(self):
        require_git_version(self.min_single_branch_version)
        self._source_repo = self.import_repo('server_new.export')
        self._stub_repo = _StubRepo('shallow')
        self.addCleanup(tear_down_repo, self._stub_repo)
        port = self._start_server(self._source_repo)

        # Fetch at depth 2
        run_git_or_fail(['clone', '--mirror', '--depth=2', '--no-single-branch',
                        self.url(port), self._stub_repo.path])
        clone = self._stub_repo = Repo(self._stub_repo.path)

        # Fetching at the same depth is a no-op.
        run_git_or_fail(
          ['fetch', '--depth=2', self.url(port)] + self.branch_args(),
          cwd=self._stub_repo.path)

        # The whole repo only has depth 4, so it should equal server_new.
        run_git_or_fail(
          ['fetch', '--depth=4', self.url(port)] + self.branch_args(),
          cwd=self._stub_repo.path)
        self.assertEqual([], _get_shallow(clone))
        self.assertReposEqual(clone, self._source_repo)

    def test_fetch_from_dulwich_issue_88_standard(self):
        # Basically an integration test to see that the ACK/NAK
        # generation works on repos with common head.
        self._source_repo = self.import_repo('issue88_expect_ack_nak_server.export')
        self._client_repo = self.import_repo('issue88_expect_ack_nak_client.export')
        port = self._start_server(self._source_repo)

        run_git_or_fail(['fetch', self.url(port), 'master',],
                        cwd=self._client_repo.path)
        self.assertObjectStoreEqual(
            self._source_repo.object_store,
            self._client_repo.object_store)

    def test_fetch_from_dulwich_issue_88_alternative(self):
        # likewise, but the case where the two repos have no common parent
        self._source_repo = self.import_repo('issue88_expect_ack_nak_other.export')
        self._client_repo = self.import_repo('issue88_expect_ack_nak_client.export')
        port = self._start_server(self._source_repo)

        self.assertRaises(KeyError, self._client_repo.get_object,
            b'02a14da1fc1fc13389bbf32f0af7d8899f2b2323')
        run_git_or_fail(['fetch', self.url(port), 'master',],
                        cwd=self._client_repo.path)
        self.assertEqual(b'commit', self._client_repo.get_object(
            b'02a14da1fc1fc13389bbf32f0af7d8899f2b2323').type_name)

    def test_push_to_dulwich_issue_88_standard(self):
        # Same thing, but we reverse the role of the server/client
        # and do a push instead.
        self._source_repo = self.import_repo('issue88_expect_ack_nak_client.export')
        self._client_repo = self.import_repo('issue88_expect_ack_nak_server.export')
        port = self._start_server(self._source_repo)

        run_git_or_fail(['push', self.url(port), 'master',],
                        cwd=self._client_repo.path)
        self.assertReposEqual(self._source_repo, self._client_repo)


# TODO(dborowitz): Come up with a better way of testing various permutations of
# capabilities. The only reason it is the way it is now is that side-band-64k
# was only recently introduced into git-receive-pack.
class NoSideBand64kReceivePackHandler(ReceivePackHandler):
    """ReceivePackHandler that does not support side-band-64k."""

    @classmethod
    def capabilities(cls):
        return tuple(c for c in ReceivePackHandler.capabilities()
                     if c != b'side-band-64k')


def ignore_error(error):
    """Check whether this error is safe to ignore."""
    (e_type, e_value, e_tb) = error
    return (issubclass(e_type, socket.error) and
            e_value[0] in (errno.ECONNRESET, errno.EPIPE))

