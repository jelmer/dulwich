# test_client.py -- Tests for the git protocol, client side
# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# or (at your option) any later version of the License.
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

from io import BytesIO
import sys
import shutil
import tempfile

try:
    from unittest import skipIf
except ImportError:
    from unittest2 import skipIf

from dulwich import (
    client,
    )
from dulwich.client import (
    LocalGitClient,
    TraditionalGitClient,
    TCPGitClient,
    SubprocessGitClient,
    SSHGitClient,
    HttpGitClient,
    ReportStatusParser,
    SendPackError,
    UpdateRefsError,
    get_transport_and_path,
    get_transport_and_path_from_url,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.protocol import (
    TCP_GIT_PORT,
    Protocol,
    )
from dulwich.pack import (
    write_pack_objects,
    )
from dulwich.objects import (
    Commit,
    Tree
    )
from dulwich.repo import (
    MemoryRepo,
    Repo,
    )
from dulwich.tests.utils import (
    open_repo,
    skipIfPY3,
    )


class DummyClient(TraditionalGitClient):

    def __init__(self, can_read, read, write):
        self.can_read = can_read
        self.read = read
        self.write = write
        TraditionalGitClient.__init__(self)

    def _connect(self, service, path):
        return Protocol(self.read, self.write), self.can_read


# TODO(durin42): add unit-level tests of GitClient
@skipIfPY3
class GitClientTests(TestCase):

    def setUp(self):
        super(GitClientTests, self).setUp()
        self.rout = BytesIO()
        self.rin = BytesIO()
        self.client = DummyClient(lambda x: True, self.rin.read,
                                  self.rout.write)

    def test_caps(self):
        self.assertEqual(set(['multi_ack', 'side-band-64k', 'ofs-delta',
                               'thin-pack', 'multi_ack_detailed']),
                          set(self.client._fetch_capabilities))
        self.assertEqual(set(['ofs-delta', 'report-status', 'side-band-64k']),
                          set(self.client._send_capabilities))

    def test_archive_ack(self):
        self.rin.write(
            '0009NACK\n'
            '0000')
        self.rin.seek(0)
        self.client.archive('bla', 'HEAD', None, None)
        self.assertEqual(self.rout.getvalue(), '0011argument HEAD0000')

    def test_fetch_empty(self):
        self.rin.write('0000')
        self.rin.seek(0)
        self.client.fetch_pack('/', lambda heads: [], None, None)

    def test_fetch_pack_none(self):
        self.rin.write(
            '008855dcc6bf963f922e1ed5c4bbaaefcfacef57b1d7 HEAD.multi_ack '
            'thin-pack side-band side-band-64k ofs-delta shallow no-progress '
            'include-tag\n'
            '0000')
        self.rin.seek(0)
        self.client.fetch_pack('bla', lambda heads: [], None, None, None)
        self.assertEqual(self.rout.getvalue(), '0000')

    def test_send_pack_no_sideband64k_with_update_ref_error(self):
        # No side-bank-64k reported by server shouldn't try to parse
        # side band data
        pkts = ['55dcc6bf963f922e1ed5c4bbaaefcfacef57b1d7 capabilities^{}'
                '\x00 report-status delete-refs ofs-delta\n',
                '',
                "unpack ok",
                "ng refs/foo/bar pre-receive hook declined",
                '']
        for pkt in pkts:
            if pkt == '':
                self.rin.write("0000")
            else:
                self.rin.write("%04x%s" % (len(pkt)+4, pkt))
        self.rin.seek(0)

        tree = Tree()
        commit = Commit()
        commit.tree = tree
        commit.parents = []
        commit.author = commit.committer = 'test user'
        commit.commit_time = commit.author_time = 1174773719
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = 'UTF-8'
        commit.message = 'test message'

        def determine_wants(refs):
            return {'refs/foo/bar': commit.id, }

        def generate_pack_contents(have, want):
            return [(commit, None), (tree, ''), ]

        self.assertRaises(UpdateRefsError,
                          self.client.send_pack, "blah",
                          determine_wants, generate_pack_contents)

    def test_send_pack_none(self):
        self.rin.write(
            '0078310ca9477129b8586fa2afc779c1f57cf64bba6c '
            'refs/heads/master\x00 report-status delete-refs '
            'side-band-64k quiet ofs-delta\n'
            '0000')
        self.rin.seek(0)

        def determine_wants(refs):
            return {
                'refs/heads/master': '310ca9477129b8586fa2afc779c1f57cf64bba6c'
            }

        def generate_pack_contents(have, want):
            return {}

        self.client.send_pack('/', determine_wants, generate_pack_contents)
        self.assertEqual(self.rout.getvalue(), '0000')

    def test_send_pack_delete_only(self):
        self.rin.write(
            '0063310ca9477129b8586fa2afc779c1f57cf64bba6c '
            'refs/heads/master\x00report-status delete-refs ofs-delta\n'
            '0000000eunpack ok\n'
            '0019ok refs/heads/master\n'
            '0000')
        self.rin.seek(0)

        def determine_wants(refs):
            return {'refs/heads/master': '0' * 40}

        def generate_pack_contents(have, want):
            return {}

        self.client.send_pack('/', determine_wants, generate_pack_contents)
        self.assertIn(
            self.rout.getvalue(),
            ['007f310ca9477129b8586fa2afc779c1f57cf64bba6c '
             '0000000000000000000000000000000000000000 '
             'refs/heads/master\x00report-status ofs-delta0000',
             '007f310ca9477129b8586fa2afc779c1f57cf64bba6c '
             '0000000000000000000000000000000000000000 '
             'refs/heads/master\x00ofs-delta report-status0000'])

    def test_send_pack_new_ref_only(self):
        self.rin.write(
            '0063310ca9477129b8586fa2afc779c1f57cf64bba6c '
            'refs/heads/master\x00report-status delete-refs ofs-delta\n'
            '0000000eunpack ok\n'
            '0019ok refs/heads/blah12\n'
            '0000')
        self.rin.seek(0)

        def determine_wants(refs):
            return {
                'refs/heads/blah12':
                '310ca9477129b8586fa2afc779c1f57cf64bba6c',
                'refs/heads/master': '310ca9477129b8586fa2afc779c1f57cf64bba6c'
            }

        def generate_pack_contents(have, want):
            return {}

        f = BytesIO()
        write_pack_objects(f, {})
        self.client.send_pack('/', determine_wants, generate_pack_contents)
        self.assertIn(
            self.rout.getvalue(),
            ['007f0000000000000000000000000000000000000000 '
             '310ca9477129b8586fa2afc779c1f57cf64bba6c '
             'refs/heads/blah12\x00report-status ofs-delta0000%s'
             % f.getvalue(),
             '007f0000000000000000000000000000000000000000 '
             '310ca9477129b8586fa2afc779c1f57cf64bba6c '
             'refs/heads/blah12\x00ofs-delta report-status0000%s'
             % f.getvalue()])

    def test_send_pack_new_ref(self):
        self.rin.write(
            '0064310ca9477129b8586fa2afc779c1f57cf64bba6c '
            'refs/heads/master\x00 report-status delete-refs ofs-delta\n'
            '0000000eunpack ok\n'
            '0019ok refs/heads/blah12\n'
            '0000')
        self.rin.seek(0)

        tree = Tree()
        commit = Commit()
        commit.tree = tree
        commit.parents = []
        commit.author = commit.committer = 'test user'
        commit.commit_time = commit.author_time = 1174773719
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = 'UTF-8'
        commit.message = 'test message'

        def determine_wants(refs):
            return {
                'refs/heads/blah12': commit.id,
                'refs/heads/master': '310ca9477129b8586fa2afc779c1f57cf64bba6c'
            }

        def generate_pack_contents(have, want):
            return [(commit, None), (tree, ''), ]

        f = BytesIO()
        write_pack_objects(f, generate_pack_contents(None, None))
        self.client.send_pack('/', determine_wants, generate_pack_contents)
        self.assertIn(
            self.rout.getvalue(),
            ['007f0000000000000000000000000000000000000000 %s '
             'refs/heads/blah12\x00report-status ofs-delta0000%s'
             % (commit.id, f.getvalue()),
             '007f0000000000000000000000000000000000000000 %s '
             'refs/heads/blah12\x00ofs-delta report-status0000%s'
             % (commit.id, f.getvalue())])

    def test_send_pack_no_deleteref_delete_only(self):
        pkts = ['310ca9477129b8586fa2afc779c1f57cf64bba6c refs/heads/master'
                '\x00 report-status ofs-delta\n',
                '',
                '']
        for pkt in pkts:
            if pkt == '':
                self.rin.write("0000")
            else:
                self.rin.write("%04x%s" % (len(pkt)+4, pkt))
        self.rin.seek(0)

        def determine_wants(refs):
            return {'refs/heads/master': '0' * 40}

        def generate_pack_contents(have, want):
            return {}

        self.assertRaises(UpdateRefsError,
                          self.client.send_pack, "/",
                          determine_wants, generate_pack_contents)
        self.assertEqual(self.rout.getvalue(), '0000')


@skipIfPY3
class TestGetTransportAndPath(TestCase):

    def test_tcp(self):
        c, path = get_transport_and_path('git://foo.com/bar/baz')
        self.assertTrue(isinstance(c, TCPGitClient))
        self.assertEqual('foo.com', c._host)
        self.assertEqual(TCP_GIT_PORT, c._port)
        self.assertEqual('/bar/baz', path)

    def test_tcp_port(self):
        c, path = get_transport_and_path('git://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(c, TCPGitClient))
        self.assertEqual('foo.com', c._host)
        self.assertEqual(1234, c._port)
        self.assertEqual('/bar/baz', path)

    def test_ssh_explicit(self):
        c, path = get_transport_and_path('git+ssh://foo.com/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('bar/baz', path)

    def test_ssh_port_explicit(self):
        c, path = get_transport_and_path(
            'git+ssh://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(1234, c.port)
        self.assertEqual('bar/baz', path)

    def test_ssh_abspath_explicit(self):
        c, path = get_transport_and_path('git+ssh://foo.com//bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('/bar/baz', path)

    def test_ssh_port_abspath_explicit(self):
        c, path = get_transport_and_path(
            'git+ssh://foo.com:1234//bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(1234, c.port)
        self.assertEqual('/bar/baz', path)

    def test_ssh_implicit(self):
        c, path = get_transport_and_path('foo:/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('/bar/baz', path)

    def test_ssh_host(self):
        c, path = get_transport_and_path('foo.com:/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('/bar/baz', path)

    def test_ssh_user_host(self):
        c, path = get_transport_and_path('user@foo.com:/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual('user', c.username)
        self.assertEqual('/bar/baz', path)

    def test_ssh_relpath(self):
        c, path = get_transport_and_path('foo:bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('bar/baz', path)

    def test_ssh_host_relpath(self):
        c, path = get_transport_and_path('foo.com:bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('bar/baz', path)

    def test_ssh_user_host_relpath(self):
        c, path = get_transport_and_path('user@foo.com:bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual('user', c.username)
        self.assertEqual('bar/baz', path)

    def test_subprocess(self):
        c, path = get_transport_and_path('foo.bar/baz')
        self.assertTrue(isinstance(c, SubprocessGitClient))
        self.assertEqual('foo.bar/baz', path)

    @skipIf(sys.platform != 'win32', 'Behaviour only happens on windows.')
    def test_local_abs_windows_path(self):
        c, path = get_transport_and_path('C:\\foo.bar\\baz')
        self.assertTrue(isinstance(c, SubprocessGitClient))
        self.assertEqual('C:\\foo.bar\\baz', path)

    def test_error(self):
        # Need to use a known urlparse.uses_netloc URL scheme to get the
        # expected parsing of the URL on Python versions less than 2.6.5
        c, path = get_transport_and_path('prospero://bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))

    def test_http(self):
        url = 'https://github.com/jelmer/dulwich'
        c, path = get_transport_and_path(url)
        self.assertTrue(isinstance(c, HttpGitClient))
        self.assertEqual('/jelmer/dulwich', path)


@skipIfPY3
class TestGetTransportAndPathFromUrl(TestCase):

    def test_tcp(self):
        c, path = get_transport_and_path_from_url('git://foo.com/bar/baz')
        self.assertTrue(isinstance(c, TCPGitClient))
        self.assertEqual('foo.com', c._host)
        self.assertEqual(TCP_GIT_PORT, c._port)
        self.assertEqual('/bar/baz', path)

    def test_tcp_port(self):
        c, path = get_transport_and_path_from_url('git://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(c, TCPGitClient))
        self.assertEqual('foo.com', c._host)
        self.assertEqual(1234, c._port)
        self.assertEqual('/bar/baz', path)

    def test_ssh_explicit(self):
        c, path = get_transport_and_path_from_url('git+ssh://foo.com/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('bar/baz', path)

    def test_ssh_port_explicit(self):
        c, path = get_transport_and_path_from_url(
            'git+ssh://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(1234, c.port)
        self.assertEqual('bar/baz', path)

    def test_ssh_abspath_explicit(self):
        c, path = get_transport_and_path_from_url('git+ssh://foo.com//bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('/bar/baz', path)

    def test_ssh_port_abspath_explicit(self):
        c, path = get_transport_and_path_from_url(
            'git+ssh://foo.com:1234//bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(1234, c.port)
        self.assertEqual('/bar/baz', path)

    def test_ssh_host_relpath(self):
        self.assertRaises(ValueError, get_transport_and_path_from_url,
            'foo.com:bar/baz')

    def test_ssh_user_host_relpath(self):
        self.assertRaises(ValueError, get_transport_and_path_from_url,
            'user@foo.com:bar/baz')

    def test_local_path(self):
        self.assertRaises(ValueError, get_transport_and_path_from_url,
            'foo.bar/baz')

    def test_error(self):
        # Need to use a known urlparse.uses_netloc URL scheme to get the
        # expected parsing of the URL on Python versions less than 2.6.5
        self.assertRaises(ValueError, get_transport_and_path_from_url,
            'prospero://bar/baz')

    def test_http(self):
        url = 'https://github.com/jelmer/dulwich'
        c, path = get_transport_and_path_from_url(url)
        self.assertTrue(isinstance(c, HttpGitClient))
        self.assertEqual('/jelmer/dulwich', path)

    def test_file(self):
        c, path = get_transport_and_path_from_url('file:///home/jelmer/foo')
        self.assertTrue(isinstance(c, SubprocessGitClient))
        self.assertEqual('/home/jelmer/foo', path)


@skipIfPY3
class TestSSHVendor(object):

    def __init__(self):
        self.host = None
        self.command = ""
        self.username = None
        self.port = None

    def run_command(self, host, command, username=None, port=None):
        self.host = host
        self.command = command
        self.username = username
        self.port = port

        class Subprocess: pass
        setattr(Subprocess, 'read', lambda: None)
        setattr(Subprocess, 'write', lambda: None)
        setattr(Subprocess, 'close', lambda: None)
        setattr(Subprocess, 'can_read', lambda: None)
        return Subprocess()


@skipIfPY3
class SSHGitClientTests(TestCase):

    def setUp(self):
        super(SSHGitClientTests, self).setUp()

        self.server = TestSSHVendor()
        self.real_vendor = client.get_ssh_vendor
        client.get_ssh_vendor = lambda: self.server

        self.client = SSHGitClient('git.samba.org')

    def tearDown(self):
        super(SSHGitClientTests, self).tearDown()
        client.get_ssh_vendor = self.real_vendor

    def test_default_command(self):
        self.assertEqual('git-upload-pack',
                self.client._get_cmd_path('upload-pack'))

    def test_alternative_command_path(self):
        self.client.alternative_paths['upload-pack'] = (
            '/usr/lib/git/git-upload-pack')
        self.assertEqual('/usr/lib/git/git-upload-pack',
            self.client._get_cmd_path('upload-pack'))

    def test_connect(self):
        server = self.server
        client = self.client

        client.username = "username"
        client.port = 1337

        client._connect("command", "/path/to/repo")
        self.assertEqual("username", server.username)
        self.assertEqual(1337, server.port)
        self.assertEqual(["git-command '/path/to/repo'"], server.command)

        client._connect("relative-command", "/~/path/to/repo")
        self.assertEqual(["git-relative-command '~/path/to/repo'"],
                          server.command)


@skipIfPY3
class ReportStatusParserTests(TestCase):

    def test_invalid_pack(self):
        parser = ReportStatusParser()
        parser.handle_packet("unpack error - foo bar")
        parser.handle_packet("ok refs/foo/bar")
        parser.handle_packet(None)
        self.assertRaises(SendPackError, parser.check)

    def test_update_refs_error(self):
        parser = ReportStatusParser()
        parser.handle_packet("unpack ok")
        parser.handle_packet("ng refs/foo/bar need to pull")
        parser.handle_packet(None)
        self.assertRaises(UpdateRefsError, parser.check)

    def test_ok(self):
        parser = ReportStatusParser()
        parser.handle_packet("unpack ok")
        parser.handle_packet("ok refs/foo/bar")
        parser.handle_packet(None)
        parser.check()


@skipIfPY3
class LocalGitClientTests(TestCase):

    def test_fetch_into_empty(self):
        c = LocalGitClient()
        t = MemoryRepo()
        s = open_repo('a.git')
        self.assertEqual(s.get_refs(), c.fetch(s.path, t))

    def test_fetch_empty(self):
        c = LocalGitClient()
        s = open_repo('a.git')
        out = BytesIO()
        walker = {}
        c.fetch_pack(s.path, lambda heads: [], graph_walker=walker,
            pack_data=out.write)
        self.assertEqual("PACK\x00\x00\x00\x02\x00\x00\x00\x00\x02\x9d\x08"
            "\x82;\xd8\xa8\xea\xb5\x10\xadj\xc7\\\x82<\xfd>\xd3\x1e", out.getvalue())

    def test_fetch_pack_none(self):
        c = LocalGitClient()
        s = open_repo('a.git')
        out = BytesIO()
        walker = MemoryRepo().get_graph_walker()
        c.fetch_pack(s.path,
            lambda heads: ["a90fa2d900a17e99b433217e988c4eb4a2e9a097"],
            graph_walker=walker, pack_data=out.write)
        # Hardcoding is not ideal, but we'll fix that some other day..
        self.assertTrue(out.getvalue().startswith('PACK\x00\x00\x00\x02\x00\x00\x00\x07'))

    def test_send_pack_without_changes(self):
        local = open_repo('a.git')
        target = open_repo('a.git')
        self.send_and_verify("master", local, target)

    def test_send_pack_with_changes(self):
        local = open_repo('a.git')
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target = Repo.init_bare(target_path)
        self.send_and_verify("master", local, target)

    def send_and_verify(self, branch, local, target):
        client = LocalGitClient()
        ref_name = "refs/heads/" + branch
        new_refs = client.send_pack(target.path,
                                    lambda _: { ref_name: local.refs[ref_name] },
                                    local.object_store.generate_pack_contents)

        self.assertEqual(local.refs[ref_name], new_refs[ref_name])

        for name, sha in new_refs.iteritems():
            self.assertEqual(new_refs[name], target.refs[name])

        obj_local = local.get_object(new_refs[ref_name])
        obj_target = target.get_object(new_refs[ref_name])
        self.assertEqual(obj_local, obj_target)
