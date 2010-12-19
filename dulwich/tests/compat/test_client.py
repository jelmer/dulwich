# test_client.py -- Compatibilty tests for git client.
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

"""Compatibilty tests between the Dulwich client and the cgit server."""

import os
import shutil
import signal
import subprocess
import tempfile

from dulwich import client
from dulwich import errors
from dulwich import file
from dulwich import index
from dulwich import protocol
from dulwich import objects
from dulwich import repo
from dulwich.tests import (
    TestSkipped,
    )

from utils import (
    CompatTestCase,
    check_for_daemon,
    import_repo_to_dir,
    run_git_or_fail,
    )

class DulwichClientTestBase(object):
    """Tests for client/server compatibility."""

    def setUp(self):
        self.gitroot = os.path.dirname(import_repo_to_dir('server_new.export'))
        dest = os.path.join(self.gitroot, 'dest')
        file.ensure_dir_exists(dest)
        run_git_or_fail(['init', '--quiet', '--bare'], cwd=dest)

    def tearDown(self):
        shutil.rmtree(self.gitroot)

    def assertDestEqualsSrc(self):
        src = repo.Repo(os.path.join(self.gitroot, 'server_new.export'))
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        self.assertReposEqual(src, dest)

    def _client(self):
        raise NotImplementedError()

    def _build_path(self):
        raise NotImplementedError()

    def _do_send_pack(self):
        c = self._client()
        srcpath = os.path.join(self.gitroot, 'server_new.export')
        src = repo.Repo(srcpath)
        sendrefs = dict(src.get_refs())
        del sendrefs['HEAD']
        c.send_pack(self._build_path('/dest'), lambda _: sendrefs,
                    src.object_store.generate_pack_contents)

    def test_send_pack(self):
        self._do_send_pack()
        self.assertDestEqualsSrc()

    def test_send_pack_nothing_to_send(self):
        self._do_send_pack()
        self.assertDestEqualsSrc()
        # nothing to send, but shouldn't raise either.
        self._do_send_pack()

    def test_send_without_report_status(self):
        c = self._client()
        c._send_capabilities.remove('report-status')
        srcpath = os.path.join(self.gitroot, 'server_new.export')
        src = repo.Repo(srcpath)
        sendrefs = dict(src.get_refs())
        del sendrefs['HEAD']
        c.send_pack(self._build_path('/dest'), lambda _: sendrefs,
                    src.object_store.generate_pack_contents)
        self.assertDestEqualsSrc()

    def disable_ff_and_make_dummy_commit(self):
        # disable non-fast-forward pushes to the server
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        run_git_or_fail(['config', 'receive.denyNonFastForwards', 'true'],
                        cwd=dest.path)
        b = objects.Blob.from_string('hi')
        dest.object_store.add_object(b)
        t = index.commit_tree(dest.object_store, [('hi', b.id, 0100644)])
        c = objects.Commit()
        c.author = c.committer = 'Foo Bar <foo@example.com>'
        c.author_time = c.commit_time = 0
        c.author_timezone = c.commit_timezone = 0
        c.message = 'hi'
        c.tree = t
        dest.object_store.add_object(c)
        return dest, c.id

    def compute_send(self):
        srcpath = os.path.join(self.gitroot, 'server_new.export')
        src = repo.Repo(srcpath)
        sendrefs = dict(src.get_refs())
        del sendrefs['HEAD']
        return sendrefs, src.object_store.generate_pack_contents

    def test_send_pack_one_error(self):
        dest, dummy_commit = self.disable_ff_and_make_dummy_commit()
        dest.refs['refs/heads/master'] = dummy_commit
        sendrefs, gen_pack = self.compute_send()
        c = self._client()
        try:
            c.send_pack(self._build_path('/dest'), lambda _: sendrefs, gen_pack)
        except errors.UpdateRefsError, e:
            self.assertEqual('refs/heads/master failed to update', str(e))
            self.assertEqual({'refs/heads/branch': 'ok',
                              'refs/heads/master': 'non-fast-forward'},
                             e.ref_status)

    def test_send_pack_multiple_errors(self):
        dest, dummy = self.disable_ff_and_make_dummy_commit()
        # set up for two non-ff errors
        dest.refs['refs/heads/branch'] = dest.refs['refs/heads/master'] = dummy
        sendrefs, gen_pack = self.compute_send()
        c = self._client()
        try:
            c.send_pack(self._build_path('/dest'), lambda _: sendrefs, gen_pack)
        except errors.UpdateRefsError, e:
            self.assertEqual('refs/heads/branch, refs/heads/master failed to '
                             'update', str(e))
            self.assertEqual({'refs/heads/branch': 'non-fast-forward',
                              'refs/heads/master': 'non-fast-forward'},
                             e.ref_status)

    def test_fetch_pack(self):
        c = self._client()
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        refs = c.fetch(self._build_path('/server_new.export'), dest)
        map(lambda r: dest.refs.set_if_equals(r[0], None, r[1]), refs.items())
        self.assertDestEqualsSrc()

    def test_incremental_fetch_pack(self):
        self.test_fetch_pack()
        dest, dummy = self.disable_ff_and_make_dummy_commit()
        dest.refs['refs/heads/master'] = dummy
        c = self._client()
        dest = repo.Repo(os.path.join(self.gitroot, 'server_new.export'))
        refs = c.fetch(self._build_path('/dest'), dest)
        map(lambda r: dest.refs.set_if_equals(r[0], None, r[1]), refs.items())
        self.assertDestEqualsSrc()


class DulwichTCPClientTest(CompatTestCase, DulwichClientTestBase):

    def setUp(self):
        CompatTestCase.setUp(self)
        DulwichClientTestBase.setUp(self)
        if check_for_daemon(limit=1):
            raise TestSkipped('git-daemon was already running on port %s' %
                              protocol.TCP_GIT_PORT)
        fd, self.pidfile = tempfile.mkstemp(prefix='dulwich-test-git-client',
                                            suffix=".pid")
        os.fdopen(fd).close()
        run_git_or_fail(
            ['daemon', '--verbose', '--export-all',
             '--pid-file=%s' % self.pidfile, '--base-path=%s' % self.gitroot,
             '--detach', '--reuseaddr', '--enable=receive-pack',
             '--listen=localhost', self.gitroot], cwd=self.gitroot)
        if not check_for_daemon():
            raise TestSkipped('git-daemon failed to start')

    def tearDown(self):
        try:
            os.kill(int(open(self.pidfile).read().strip()), signal.SIGKILL)
            os.unlink(self.pidfile)
        except (OSError, IOError):
            pass
        DulwichClientTestBase.tearDown(self)
        CompatTestCase.tearDown(self)

    def _client(self):
        return client.TCPGitClient('localhost')

    def _build_path(self, path):
        return path


class TestSSHVendor(object):
    @staticmethod
    def connect_ssh(host, command, username=None, port=None):
        cmd, path = command[0].replace("'", '').split(' ')
        cmd = cmd.split('-', 1)
        p = subprocess.Popen(cmd + [path], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return client.SubprocessWrapper(p)


class DulwichMockSSHClientTest(CompatTestCase, DulwichClientTestBase):

    def setUp(self):
        CompatTestCase.setUp(self)
        DulwichClientTestBase.setUp(self)
        self.real_vendor = client.get_ssh_vendor
        client.get_ssh_vendor = TestSSHVendor

    def tearDown(self):
        DulwichClientTestBase.tearDown(self)
        CompatTestCase.tearDown(self)
        client.get_ssh_vendor = self.real_vendor

    def _client(self):
        return client.SSHGitClient('localhost')

    def _build_path(self, path):
        return self.gitroot + path


class DulwichSubprocessClientTest(CompatTestCase, DulwichClientTestBase):

    def setUp(self):
        CompatTestCase.setUp(self)
        DulwichClientTestBase.setUp(self)

    def tearDown(self):
        DulwichClientTestBase.tearDown(self)
        CompatTestCase.tearDown(self)

    def _client(self):
        return client.SubprocessGitClient()

    def _build_path(self, path):
        return self.gitroot + path
