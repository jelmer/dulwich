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

from cStringIO import StringIO

from dulwich import (
    client,
    )
from dulwich.client import (
    TraditionalGitClient,
    TCPGitClient,
    SubprocessGitClient,
    SSHGitClient,
    HttpGitClient,
    ReportStatusParser,
    SendPackError,
    UpdateRefsError,
    get_transport_and_path,
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


class DummyClient(TraditionalGitClient):

    def __init__(self, can_read, read, write):
        self.can_read = can_read
        self.read = read
        self.write = write
        TraditionalGitClient.__init__(self)

    def _connect(self, service, path):
        return Protocol(self.read, self.write), self.can_read


# TODO(durin42): add unit-level tests of GitClient
class GitClientTests(TestCase):

    def setUp(self):
        super(GitClientTests, self).setUp()
        self.rout = StringIO()
        self.rin = StringIO()
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

    def test_get_transport_and_path_tcp(self):
        client, path = get_transport_and_path('git://foo.com/bar/baz')
        self.assertTrue(isinstance(client, TCPGitClient))
        self.assertEqual('foo.com', client._host)
        self.assertEqual(TCP_GIT_PORT, client._port)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_tcp_port(self):
        client, path = get_transport_and_path('git://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(client, TCPGitClient))
        self.assertEqual('foo.com', client._host)
        self.assertEqual(1234, client._port)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_explicit(self):
        client, path = get_transport_and_path('git+ssh://foo.com/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual(None, client.username)
        self.assertEqual('bar/baz', path)

    def test_get_transport_and_path_ssh_port_explicit(self):
        client, path = get_transport_and_path(
            'git+ssh://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(1234, client.port)
        self.assertEqual('bar/baz', path)

    def test_get_transport_and_path_ssh_abspath_explicit(self):
        client, path = get_transport_and_path('git+ssh://foo.com//bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual(None, client.username)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_port_abspath_explicit(self):
        client, path = get_transport_and_path(
            'git+ssh://foo.com:1234//bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(1234, client.port)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_implicit(self):
        client, path = get_transport_and_path('foo:/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual(None, client.username)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_host(self):
        client, path = get_transport_and_path('foo.com:/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual(None, client.username)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_user_host(self):
        client, path = get_transport_and_path('user@foo.com:/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual('user', client.username)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_relpath(self):
        client, path = get_transport_and_path('foo:bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual(None, client.username)
        self.assertEqual('bar/baz', path)

    def test_get_transport_and_path_ssh_host_relpath(self):
        client, path = get_transport_and_path('foo.com:bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual(None, client.username)
        self.assertEqual('bar/baz', path)

    def test_get_transport_and_path_ssh_user_host_relpath(self):
        client, path = get_transport_and_path('user@foo.com:bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEqual('foo.com', client.host)
        self.assertEqual(None, client.port)
        self.assertEqual('user', client.username)
        self.assertEqual('bar/baz', path)

    def test_get_transport_and_path_subprocess(self):
        client, path = get_transport_and_path('foo.bar/baz')
        self.assertTrue(isinstance(client, SubprocessGitClient))
        self.assertEqual('foo.bar/baz', path)

    def test_get_transport_and_path_error(self):
        # Need to use a known urlparse.uses_netloc URL scheme to get the
        # expected parsing of the URL on Python versions less than 2.6.5
        self.assertRaises(ValueError, get_transport_and_path,
        'prospero://bar/baz')

    def test_get_transport_and_path_http(self):
        url = 'https://github.com/jelmer/dulwich'
        client, path = get_transport_and_path(url)
        self.assertTrue(isinstance(client, HttpGitClient))
        self.assertEqual('/jelmer/dulwich', path)

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
        self.assertEqual(
            self.rout.getvalue(),
            '007f310ca9477129b8586fa2afc779c1f57cf64bba6c '
            '0000000000000000000000000000000000000000 '
            'refs/heads/master\x00report-status ofs-delta0000')

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

        f = StringIO()
        empty_pack = write_pack_objects(f, {})
        self.client.send_pack('/', determine_wants, generate_pack_contents)
        self.assertEqual(
            self.rout.getvalue(),
            '007f0000000000000000000000000000000000000000 '
            '310ca9477129b8586fa2afc779c1f57cf64bba6c '
            'refs/heads/blah12\x00report-status ofs-delta0000%s'
            % f.getvalue())

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

        f = StringIO()
        pack = write_pack_objects(f, generate_pack_contents(None, None))
        self.client.send_pack('/', determine_wants, generate_pack_contents)
        self.assertEqual(
            self.rout.getvalue(),
            '007f0000000000000000000000000000000000000000 %s '
            'refs/heads/blah12\x00report-status ofs-delta0000%s'
            % (commit.id, f.getvalue()))

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


class TestSSHVendor(object):

    def __init__(self):
        self.host = None
        self.command = ""
        self.username = None
        self.port = None

    def connect_ssh(self, host, command, username=None, port=None, **kwargs):
        self.host = host
        self.command = command
        self.username = username
        self.port = port

        class Subprocess: pass
        setattr(Subprocess, 'read', lambda: None)
        setattr(Subprocess, 'write', lambda: None)
        setattr(Subprocess, 'can_read', lambda: None)
        return Subprocess()


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
        self.assertEquals("username", server.username)
        self.assertEquals(1337, server.port)
        self.assertEquals(["git-command '/path/to/repo'"], server.command)

        client._connect("relative-command", "/~/path/to/repo")
        self.assertEquals(["git-relative-command '~/path/to/repo'"],
                          server.command)

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
