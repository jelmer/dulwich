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

from io import BytesIO
import BaseHTTPServer
import SimpleHTTPServer
import copy
import os
import select
import shutil
import signal
import subprocess
import tarfile
import tempfile
import threading
import urllib
from unittest import SkipTest

from dulwich import (
    client,
    errors,
    file,
    index,
    protocol,
    objects,
    repo,
    )
from dulwich.tests import (
    get_safe_env,
    )

from dulwich.tests.compat.utils import (
    CompatTestCase,
    check_for_daemon,
    import_repo_to_dir,
    run_git_or_fail,
    )


class DulwichClientTestBase(object):
    """Tests for client/server compatibility."""

    def setUp(self):
        self.gitroot = os.path.dirname(import_repo_to_dir('server_new.export'))
        self.dest = os.path.join(self.gitroot, 'dest')
        file.ensure_dir_exists(self.dest)
        run_git_or_fail(['init', '--quiet', '--bare'], cwd=self.dest)

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

    def make_dummy_commit(self, dest):
        b = objects.Blob.from_string('hi')
        dest.object_store.add_object(b)
        t = index.commit_tree(dest.object_store, [('hi', b.id, 0o100644)])
        c = objects.Commit()
        c.author = c.committer = 'Foo Bar <foo@example.com>'
        c.author_time = c.commit_time = 0
        c.author_timezone = c.commit_timezone = 0
        c.message = 'hi'
        c.tree = t
        dest.object_store.add_object(c)
        return c.id

    def disable_ff_and_make_dummy_commit(self):
        # disable non-fast-forward pushes to the server
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        run_git_or_fail(['config', 'receive.denyNonFastForwards', 'true'],
                        cwd=dest.path)
        commit_id = self.make_dummy_commit(dest)
        return dest, commit_id

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
        except errors.UpdateRefsError as e:
            self.assertEqual('refs/heads/master failed to update', str(e))
            self.assertEqual({'refs/heads/branch': 'ok',
                              'refs/heads/master': 'non-fast-forward'},
                             e.ref_status)

    def test_send_pack_multiple_errors(self):
        dest, dummy = self.disable_ff_and_make_dummy_commit()
        # set up for two non-ff errors
        branch, master = 'refs/heads/branch', 'refs/heads/master'
        dest.refs[branch] = dest.refs[master] = dummy
        sendrefs, gen_pack = self.compute_send()
        c = self._client()
        try:
            c.send_pack(self._build_path('/dest'), lambda _: sendrefs, gen_pack)
        except errors.UpdateRefsError as e:
            self.assertIn(str(e),
                          ['{0}, {1} failed to update'.format(branch, master),
                           '{1}, {0} failed to update'.format(branch, master)])
            self.assertEqual({branch: 'non-fast-forward',
                              master: 'non-fast-forward'},
                             e.ref_status)

    def test_archive(self):
        c = self._client()
        f = BytesIO()
        c.archive(self._build_path('/server_new.export'), 'HEAD', f.write)
        f.seek(0)
        tf = tarfile.open(fileobj=f)
        self.assertEqual(['baz', 'foo'], tf.getnames())

    def test_fetch_pack(self):
        c = self._client()
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        refs = c.fetch(self._build_path('/server_new.export'), dest)
        for r in refs.items():
            dest.refs.set_if_equals(r[0], None, r[1])
        self.assertDestEqualsSrc()

    def test_incremental_fetch_pack(self):
        self.test_fetch_pack()
        dest, dummy = self.disable_ff_and_make_dummy_commit()
        dest.refs['refs/heads/master'] = dummy
        c = self._client()
        dest = repo.Repo(os.path.join(self.gitroot, 'server_new.export'))
        refs = c.fetch(self._build_path('/dest'), dest)
        for r in refs.items():
            dest.refs.set_if_equals(r[0], None, r[1])
        self.assertDestEqualsSrc()

    def test_fetch_pack_no_side_band_64k(self):
        c = self._client()
        c._fetch_capabilities.remove('side-band-64k')
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        refs = c.fetch(self._build_path('/server_new.export'), dest)
        for r in refs.items():
            dest.refs.set_if_equals(r[0], None, r[1])
        self.assertDestEqualsSrc()

    def test_fetch_pack_zero_sha(self):
        # zero sha1s are already present on the client, and should
        # be ignored
        c = self._client()
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        refs = c.fetch(self._build_path('/server_new.export'), dest,
            lambda refs: [protocol.ZERO_SHA])
        for r in refs.items():
            dest.refs.set_if_equals(r[0], None, r[1])

    def test_send_remove_branch(self):
        dest = repo.Repo(os.path.join(self.gitroot, 'dest'))
        dummy_commit = self.make_dummy_commit(dest)
        dest.refs['refs/heads/master'] = dummy_commit
        dest.refs['refs/heads/abranch'] = dummy_commit
        sendrefs = dict(dest.refs)
        sendrefs['refs/heads/abranch'] = "00" * 20
        del sendrefs['HEAD']
        gen_pack = lambda have, want: []
        c = self._client()
        self.assertEqual(dest.refs["refs/heads/abranch"], dummy_commit)
        c.send_pack(self._build_path('/dest'), lambda _: sendrefs, gen_pack)
        self.assertFalse("refs/heads/abranch" in dest.refs)


class DulwichTCPClientTest(CompatTestCase, DulwichClientTestBase):

    def setUp(self):
        CompatTestCase.setUp(self)
        DulwichClientTestBase.setUp(self)
        if check_for_daemon(limit=1):
            raise SkipTest('git-daemon was already running on port %s' %
                              protocol.TCP_GIT_PORT)
        fd, self.pidfile = tempfile.mkstemp(prefix='dulwich-test-git-client',
                                            suffix=".pid")
        os.fdopen(fd).close()
        run_git_or_fail(
            ['daemon', '--verbose', '--export-all',
             '--pid-file=%s' % self.pidfile, '--base-path=%s' % self.gitroot,
             '--detach', '--reuseaddr', '--enable=receive-pack',
             '--enable=upload-archive', '--listen=localhost', self.gitroot], cwd=self.gitroot)
        if not check_for_daemon():
            raise SkipTest('git-daemon failed to start')

    def tearDown(self):
        try:
            with open(self.pidfile) as f:
                pid = f.read()
            os.kill(int(pid.strip()), signal.SIGKILL)
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
    def run_command(host, command, username=None, port=None):
        cmd, path = command[0].replace("'", '').split(' ')
        cmd = cmd.split('-', 1)
        p = subprocess.Popen(cmd + [path], env=get_safe_env(), stdin=subprocess.PIPE,
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
        return client.SubprocessGitClient(stderr=subprocess.PIPE)

    def _build_path(self, path):
        return self.gitroot + path


class GitHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    """HTTP Request handler that calls out to 'git http-backend'."""

    # Make rfile unbuffered -- we need to read one line and then pass
    # the rest to a subprocess, so we can't use buffered input.
    rbufsize = 0

    def do_POST(self):
        self.run_backend()

    def do_GET(self):
        self.run_backend()

    def send_head(self):
        return self.run_backend()

    def log_request(self, code='-', size='-'):
        # Let's be quiet, the test suite is noisy enough already
        pass

    def run_backend(self):
        """Call out to git http-backend."""
        # Based on CGIHTTPServer.CGIHTTPRequestHandler.run_cgi:
        # Copyright (c) 2001-2010 Python Software Foundation; All Rights Reserved
        # Licensed under the Python Software Foundation License.
        rest = self.path
        # find an explicit query string, if present.
        i = rest.rfind('?')
        if i >= 0:
            rest, query = rest[:i], rest[i+1:]
        else:
            query = ''

        env = copy.deepcopy(os.environ)
        env['SERVER_SOFTWARE'] = self.version_string()
        env['SERVER_NAME'] = self.server.server_name
        env['GATEWAY_INTERFACE'] = 'CGI/1.1'
        env['SERVER_PROTOCOL'] = self.protocol_version
        env['SERVER_PORT'] = str(self.server.server_port)
        env['GIT_PROJECT_ROOT'] = self.server.root_path
        env["GIT_HTTP_EXPORT_ALL"] = "1"
        env['REQUEST_METHOD'] = self.command
        uqrest = urllib.unquote(rest)
        env['PATH_INFO'] = uqrest
        env['SCRIPT_NAME'] = "/"
        if query:
            env['QUERY_STRING'] = query
        host = self.address_string()
        if host != self.client_address[0]:
            env['REMOTE_HOST'] = host
        env['REMOTE_ADDR'] = self.client_address[0]
        authorization = self.headers.getheader("authorization")
        if authorization:
            authorization = authorization.split()
            if len(authorization) == 2:
                import base64, binascii
                env['AUTH_TYPE'] = authorization[0]
                if authorization[0].lower() == "basic":
                    try:
                        authorization = base64.decodestring(authorization[1])
                    except binascii.Error:
                        pass
                    else:
                        authorization = authorization.split(':')
                        if len(authorization) == 2:
                            env['REMOTE_USER'] = authorization[0]
        # XXX REMOTE_IDENT
        if self.headers.typeheader is None:
            env['CONTENT_TYPE'] = self.headers.type
        else:
            env['CONTENT_TYPE'] = self.headers.typeheader
        length = self.headers.getheader('content-length')
        if length:
            env['CONTENT_LENGTH'] = length
        referer = self.headers.getheader('referer')
        if referer:
            env['HTTP_REFERER'] = referer
        accept = []
        for line in self.headers.getallmatchingheaders('accept'):
            if line[:1] in "\t\n\r ":
                accept.append(line.strip())
            else:
                accept = accept + line[7:].split(',')
        env['HTTP_ACCEPT'] = ','.join(accept)
        ua = self.headers.getheader('user-agent')
        if ua:
            env['HTTP_USER_AGENT'] = ua
        co = filter(None, self.headers.getheaders('cookie'))
        if co:
            env['HTTP_COOKIE'] = ', '.join(co)
        # XXX Other HTTP_* headers
        # Since we're setting the env in the parent, provide empty
        # values to override previously set values
        for k in ('QUERY_STRING', 'REMOTE_HOST', 'CONTENT_LENGTH',
                  'HTTP_USER_AGENT', 'HTTP_COOKIE', 'HTTP_REFERER'):
            env.setdefault(k, "")

        self.send_response(200, "Script output follows")

        decoded_query = query.replace('+', ' ')

        try:
            nbytes = int(length)
        except (TypeError, ValueError):
            nbytes = 0
        if self.command.lower() == "post" and nbytes > 0:
            data = self.rfile.read(nbytes)
        else:
            data = None
        # throw away additional data [see bug #427345]
        while select.select([self.rfile._sock], [], [], 0)[0]:
            if not self.rfile._sock.recv(1):
                break
        args = ['http-backend']
        if '=' not in decoded_query:
            args.append(decoded_query)
        stdout = run_git_or_fail(args, input=data, env=env, stderr=subprocess.PIPE)
        self.wfile.write(stdout)


class HTTPGitServer(BaseHTTPServer.HTTPServer):

    allow_reuse_address = True

    def __init__(self, server_address, root_path):
        BaseHTTPServer.HTTPServer.__init__(self, server_address, GitHTTPRequestHandler)
        self.root_path = root_path
        self.server_name = "localhost"

    def get_url(self):
        return 'http://%s:%s/' % (self.server_name, self.server_port)


class DulwichHttpClientTest(CompatTestCase, DulwichClientTestBase):

    min_git_version = (1, 7, 0, 2)

    def setUp(self):
        CompatTestCase.setUp(self)
        DulwichClientTestBase.setUp(self)
        self._httpd = HTTPGitServer(("localhost", 0), self.gitroot)
        self.addCleanup(self._httpd.shutdown)
        threading.Thread(target=self._httpd.serve_forever).start()
        run_git_or_fail(['config', 'http.uploadpack', 'true'],
                        cwd=self.dest)
        run_git_or_fail(['config', 'http.receivepack', 'true'],
                        cwd=self.dest)

    def tearDown(self):
        DulwichClientTestBase.tearDown(self)
        CompatTestCase.tearDown(self)
        self._httpd.shutdown()
        self._httpd.socket.close()

    def _client(self):
        return client.HttpGitClient(self._httpd.get_url())

    def _build_path(self, path):
        return path

    def test_archive(self):
        raise SkipTest("exporting archives not supported over http")
