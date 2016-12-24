# test_client.py -- Tests for the git protocol, client side
# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>
#
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

from contextlib import closing
from io import BytesIO
import sys
import shutil
import tempfile


import dulwich
from dulwich import (
    client,
    )
from dulwich.client import (
    LocalGitClient,
    TraditionalGitClient,
    TCPGitClient,
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
from dulwich.tests import skipIf
from dulwich.tests.utils import (
    open_repo,
    tear_down_repo,
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
        self.rout = BytesIO()
        self.rin = BytesIO()
        self.client = DummyClient(lambda x: True, self.rin.read,
                                  self.rout.write)

    def test_caps(self):
        agent_cap = ('agent=dulwich/%d.%d.%d' % dulwich.__version__).encode('ascii')
        self.assertEqual(set([b'multi_ack', b'side-band-64k', b'ofs-delta',
                               b'thin-pack', b'multi_ack_detailed',
                               agent_cap]),
                          set(self.client._fetch_capabilities))
        self.assertEqual(set([b'ofs-delta', b'report-status', b'side-band-64k',
                              agent_cap]),
                          set(self.client._send_capabilities))

    def test_archive_ack(self):
        self.rin.write(
            b'0009NACK\n'
            b'0000')
        self.rin.seek(0)
        self.client.archive(b'bla', b'HEAD', None, None)
        self.assertEqual(self.rout.getvalue(), b'0011argument HEAD0000')

    def test_fetch_empty(self):
        self.rin.write(b'0000')
        self.rin.seek(0)
        def check_heads(heads):
            self.assertIs(heads, None)
            return []
        ret = self.client.fetch_pack(b'/', check_heads, None, None)
        self.assertIs(None, ret)

    def test_fetch_pack_ignores_magic_ref(self):
        self.rin.write(
            b'00000000000000000000000000000000000000000000 capabilities^{}\x00 multi_ack '
            b'thin-pack side-band side-band-64k ofs-delta shallow no-progress '
            b'include-tag\n'
            b'0000')
        self.rin.seek(0)
        def check_heads(heads):
            self.assertEquals({}, heads)
            return []
        ret = self.client.fetch_pack(b'bla', check_heads, None, None, None)
        self.assertIs(None, ret)
        self.assertEqual(self.rout.getvalue(), b'0000')

    def test_fetch_pack_none(self):
        self.rin.write(
            b'008855dcc6bf963f922e1ed5c4bbaaefcfacef57b1d7 HEAD.multi_ack '
            b'thin-pack side-band side-band-64k ofs-delta shallow no-progress '
            b'include-tag\n'
            b'0000')
        self.rin.seek(0)
        self.client.fetch_pack(b'bla', lambda heads: [], None, None, None)
        self.assertEqual(self.rout.getvalue(), b'0000')

    def test_send_pack_no_sideband64k_with_update_ref_error(self):
        # No side-bank-64k reported by server shouldn't try to parse
        # side band data
        pkts = [b'55dcc6bf963f922e1ed5c4bbaaefcfacef57b1d7 capabilities^{}'
                b'\x00 report-status delete-refs ofs-delta\n',
                b'',
                b"unpack ok",
                b"ng refs/foo/bar pre-receive hook declined",
                b'']
        for pkt in pkts:
            if pkt ==  b'':
                self.rin.write(b"0000")
            else:
                self.rin.write(("%04x" % (len(pkt)+4)).encode('ascii') + pkt)
        self.rin.seek(0)

        tree = Tree()
        commit = Commit()
        commit.tree = tree
        commit.parents = []
        commit.author = commit.committer = b'test user'
        commit.commit_time = commit.author_time = 1174773719
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = b'UTF-8'
        commit.message = b'test message'

        def determine_wants(refs):
            return {b'refs/foo/bar': commit.id, }

        def generate_pack_contents(have, want):
            return [(commit, None), (tree, ''), ]

        self.assertRaises(UpdateRefsError,
                          self.client.send_pack, "blah",
                          determine_wants, generate_pack_contents)

    def test_send_pack_none(self):
        self.rin.write(
            b'0078310ca9477129b8586fa2afc779c1f57cf64bba6c '
            b'refs/heads/master\x00 report-status delete-refs '
            b'side-band-64k quiet ofs-delta\n'
            b'0000')
        self.rin.seek(0)

        def determine_wants(refs):
            return {
                b'refs/heads/master': b'310ca9477129b8586fa2afc779c1f57cf64bba6c'
            }

        def generate_pack_contents(have, want):
            return {}

        self.client.send_pack(b'/', determine_wants, generate_pack_contents)
        self.assertEqual(self.rout.getvalue(), b'0000')

    def test_send_pack_keep_and_delete(self):
        self.rin.write(
            b'0063310ca9477129b8586fa2afc779c1f57cf64bba6c '
            b'refs/heads/master\x00report-status delete-refs ofs-delta\n'
            b'003f310ca9477129b8586fa2afc779c1f57cf64bba6c refs/heads/keepme\n'
            b'0000000eunpack ok\n'
            b'0019ok refs/heads/master\n'
            b'0000')
        self.rin.seek(0)

        def determine_wants(refs):
            return {b'refs/heads/master': b'0' * 40}

        def generate_pack_contents(have, want):
            return {}

        self.client.send_pack(b'/', determine_wants, generate_pack_contents)
        self.assertIn(
            self.rout.getvalue(),
            [b'007f310ca9477129b8586fa2afc779c1f57cf64bba6c '
             b'0000000000000000000000000000000000000000 '
             b'refs/heads/master\x00report-status ofs-delta0000',
             b'007f310ca9477129b8586fa2afc779c1f57cf64bba6c '
             b'0000000000000000000000000000000000000000 '
             b'refs/heads/master\x00ofs-delta report-status0000'])

    def test_send_pack_delete_only(self):
        self.rin.write(
            b'0063310ca9477129b8586fa2afc779c1f57cf64bba6c '
            b'refs/heads/master\x00report-status delete-refs ofs-delta\n'
            b'0000000eunpack ok\n'
            b'0019ok refs/heads/master\n'
            b'0000')
        self.rin.seek(0)

        def determine_wants(refs):
            return {b'refs/heads/master': b'0' * 40}

        def generate_pack_contents(have, want):
            return {}

        self.client.send_pack(b'/', determine_wants, generate_pack_contents)
        self.assertIn(
            self.rout.getvalue(),
            [b'007f310ca9477129b8586fa2afc779c1f57cf64bba6c '
             b'0000000000000000000000000000000000000000 '
             b'refs/heads/master\x00report-status ofs-delta0000',
             b'007f310ca9477129b8586fa2afc779c1f57cf64bba6c '
             b'0000000000000000000000000000000000000000 '
             b'refs/heads/master\x00ofs-delta report-status0000'])

    def test_send_pack_new_ref_only(self):
        self.rin.write(
            b'0063310ca9477129b8586fa2afc779c1f57cf64bba6c '
            b'refs/heads/master\x00report-status delete-refs ofs-delta\n'
            b'0000000eunpack ok\n'
            b'0019ok refs/heads/blah12\n'
            b'0000')
        self.rin.seek(0)

        def determine_wants(refs):
            return {
                b'refs/heads/blah12':
                b'310ca9477129b8586fa2afc779c1f57cf64bba6c',
                b'refs/heads/master': b'310ca9477129b8586fa2afc779c1f57cf64bba6c'
            }

        def generate_pack_contents(have, want):
            return {}

        f = BytesIO()
        write_pack_objects(f, {})
        self.client.send_pack('/', determine_wants, generate_pack_contents)
        self.assertIn(
            self.rout.getvalue(),
            [b'007f0000000000000000000000000000000000000000 '
             b'310ca9477129b8586fa2afc779c1f57cf64bba6c '
             b'refs/heads/blah12\x00report-status ofs-delta0000' +
             f.getvalue(),
             b'007f0000000000000000000000000000000000000000 '
             b'310ca9477129b8586fa2afc779c1f57cf64bba6c '
             b'refs/heads/blah12\x00ofs-delta report-status0000' +
             f.getvalue()])

    def test_send_pack_new_ref(self):
        self.rin.write(
            b'0064310ca9477129b8586fa2afc779c1f57cf64bba6c '
            b'refs/heads/master\x00 report-status delete-refs ofs-delta\n'
            b'0000000eunpack ok\n'
            b'0019ok refs/heads/blah12\n'
            b'0000')
        self.rin.seek(0)

        tree = Tree()
        commit = Commit()
        commit.tree = tree
        commit.parents = []
        commit.author = commit.committer = b'test user'
        commit.commit_time = commit.author_time = 1174773719
        commit.commit_timezone = commit.author_timezone = 0
        commit.encoding = b'UTF-8'
        commit.message = b'test message'

        def determine_wants(refs):
            return {
                b'refs/heads/blah12': commit.id,
                b'refs/heads/master': b'310ca9477129b8586fa2afc779c1f57cf64bba6c'
            }

        def generate_pack_contents(have, want):
            return [(commit, None), (tree, b''), ]

        f = BytesIO()
        write_pack_objects(f, generate_pack_contents(None, None))
        self.client.send_pack(b'/', determine_wants, generate_pack_contents)
        self.assertIn(
            self.rout.getvalue(),
            [b'007f0000000000000000000000000000000000000000 ' + commit.id +
             b' refs/heads/blah12\x00report-status ofs-delta0000' + f.getvalue(),
             b'007f0000000000000000000000000000000000000000 ' + commit.id +
             b' refs/heads/blah12\x00ofs-delta report-status0000' + f.getvalue()])

    def test_send_pack_no_deleteref_delete_only(self):
        pkts = [b'310ca9477129b8586fa2afc779c1f57cf64bba6c refs/heads/master'
                b'\x00 report-status ofs-delta\n',
                b'',
                b'']
        for pkt in pkts:
            if pkt == b'':
                self.rin.write(b"0000")
            else:
                self.rin.write(("%04x" % (len(pkt)+4)).encode('ascii') + pkt)
        self.rin.seek(0)

        def determine_wants(refs):
            return {b'refs/heads/master': b'0' * 40}

        def generate_pack_contents(have, want):
            return {}

        self.assertRaises(UpdateRefsError,
                          self.client.send_pack, b"/",
                          determine_wants, generate_pack_contents)
        self.assertEqual(self.rout.getvalue(), b'0000')


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

    def test_git_ssh_explicit(self):
        c, path = get_transport_and_path('git+ssh://foo.com/bar/baz')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('foo.com', c.host)
        self.assertEqual(None, c.port)
        self.assertEqual(None, c.username)
        self.assertEqual('bar/baz', path)

    def test_ssh_explicit(self):
        c, path = get_transport_and_path('ssh://foo.com/bar/baz')
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

    def test_username_and_port_explicit_unknown_scheme(self):
        c, path = get_transport_and_path(
            'unknown://git@server:7999/dply/stuff.git')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('unknown', c.host)
        self.assertEqual('//git@server:7999/dply/stuff.git', path)

    def test_username_and_port_explicit(self):
        c, path = get_transport_and_path(
            'ssh://git@server:7999/dply/stuff.git')
        self.assertTrue(isinstance(c, SSHGitClient))
        self.assertEqual('git', c.username)
        self.assertEqual('server', c.host)
        self.assertEqual(7999, c.port)
        self.assertEqual('dply/stuff.git', path)

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

    def test_local(self):
        c, path = get_transport_and_path('foo.bar/baz')
        self.assertTrue(isinstance(c, LocalGitClient))
        self.assertEqual('foo.bar/baz', path)

    @skipIf(sys.platform != 'win32', 'Behaviour only happens on windows.')
    def test_local_abs_windows_path(self):
        c, path = get_transport_and_path('C:\\foo.bar\\baz')
        self.assertTrue(isinstance(c, LocalGitClient))
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

    def test_http_auth(self):
        url = 'https://user:passwd@github.com/jelmer/dulwich'

        c, path = get_transport_and_path(url)

        self.assertTrue(isinstance(c, HttpGitClient))
        self.assertEqual('/jelmer/dulwich', path)
        self.assertEqual('user', c._username)
        self.assertEqual('passwd', c._password)

    def test_http_no_auth(self):
        url = 'https://github.com/jelmer/dulwich'

        c, path = get_transport_and_path(url)

        self.assertTrue(isinstance(c, HttpGitClient))
        self.assertEqual('/jelmer/dulwich', path)
        self.assertIs(None, c._username)
        self.assertIs(None, c._password)


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
        self.assertTrue(isinstance(c, LocalGitClient))
        self.assertEqual('/home/jelmer/foo', path)


class TestSSHVendor(object):

    def __init__(self):
        self.host = None
        self.command = ""
        self.username = None
        self.port = None

    def run_command(self, host, command, username=None, port=None):
        if not isinstance(command, bytes):
            raise TypeError(command)

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

    def test_get_url(self):
        path = '/tmp/repo.git'
        c = SSHGitClient('git.samba.org')

        url = c.get_url(path)
        self.assertEqual('ssh://git.samba.org/tmp/repo.git', url)

    def test_get_url_with_username_and_port(self):
        path = '/tmp/repo.git'
        c = SSHGitClient('git.samba.org', port=2222, username='user')

        url = c.get_url(path)
        self.assertEqual('ssh://user@git.samba.org:2222/tmp/repo.git', url)

    def test_default_command(self):
        self.assertEqual(b'git-upload-pack',
                self.client._get_cmd_path(b'upload-pack'))

    def test_alternative_command_path(self):
        self.client.alternative_paths[b'upload-pack'] = (
            b'/usr/lib/git/git-upload-pack')
        self.assertEqual(b'/usr/lib/git/git-upload-pack',
            self.client._get_cmd_path(b'upload-pack'))

    def test_alternative_command_path_spaces(self):
        self.client.alternative_paths[b'upload-pack'] = (
            b'/usr/lib/git/git-upload-pack -ibla')
        self.assertEqual(b"/usr/lib/git/git-upload-pack -ibla",
            self.client._get_cmd_path(b'upload-pack'))

    def test_connect(self):
        server = self.server
        client = self.client

        client.username = b"username"
        client.port = 1337

        client._connect(b"command", b"/path/to/repo")
        self.assertEqual(b"username", server.username)
        self.assertEqual(1337, server.port)
        self.assertEqual(b"git-command '/path/to/repo'", server.command)

        client._connect(b"relative-command", b"/~/path/to/repo")
        self.assertEqual(b"git-relative-command '~/path/to/repo'",
                          server.command)


class ReportStatusParserTests(TestCase):

    def test_invalid_pack(self):
        parser = ReportStatusParser()
        parser.handle_packet(b"unpack error - foo bar")
        parser.handle_packet(b"ok refs/foo/bar")
        parser.handle_packet(None)
        self.assertRaises(SendPackError, parser.check)

    def test_update_refs_error(self):
        parser = ReportStatusParser()
        parser.handle_packet(b"unpack ok")
        parser.handle_packet(b"ng refs/foo/bar need to pull")
        parser.handle_packet(None)
        self.assertRaises(UpdateRefsError, parser.check)

    def test_ok(self):
        parser = ReportStatusParser()
        parser.handle_packet(b"unpack ok")
        parser.handle_packet(b"ok refs/foo/bar")
        parser.handle_packet(None)
        parser.check()


class LocalGitClientTests(TestCase):

    def test_get_url(self):
        path = "/tmp/repo.git"
        c = LocalGitClient()

        url = c.get_url(path)
        self.assertEqual('file:///tmp/repo.git', url)

    def test_fetch_into_empty(self):
        c = LocalGitClient()
        t = MemoryRepo()
        s = open_repo('a.git')
        self.addCleanup(tear_down_repo, s)
        self.assertEqual(s.get_refs(), c.fetch(s.path, t))

    def test_fetch_empty(self):
        c = LocalGitClient()
        s = open_repo('a.git')
        self.addCleanup(tear_down_repo, s)
        out = BytesIO()
        walker = {}
        ret = c.fetch_pack(s.path, lambda heads: [], graph_walker=walker,
            pack_data=out.write)
        self.assertEqual({
            b'HEAD': b'a90fa2d900a17e99b433217e988c4eb4a2e9a097',
            b'refs/heads/master': b'a90fa2d900a17e99b433217e988c4eb4a2e9a097',
            b'refs/tags/mytag': b'28237f4dc30d0d462658d6b937b08a0f0b6ef55a',
            b'refs/tags/mytag-packed': b'b0931cadc54336e78a1d980420e3268903b57a50'
            }, ret)
        self.assertEqual(b"PACK\x00\x00\x00\x02\x00\x00\x00\x00\x02\x9d\x08"
            b"\x82;\xd8\xa8\xea\xb5\x10\xadj\xc7\\\x82<\xfd>\xd3\x1e", out.getvalue())

    def test_fetch_pack_none(self):
        c = LocalGitClient()
        s = open_repo('a.git')
        self.addCleanup(tear_down_repo, s)
        out = BytesIO()
        walker = MemoryRepo().get_graph_walker()
        c.fetch_pack(s.path,
            lambda heads: [b"a90fa2d900a17e99b433217e988c4eb4a2e9a097"],
            graph_walker=walker, pack_data=out.write)
        # Hardcoding is not ideal, but we'll fix that some other day..
        self.assertTrue(out.getvalue().startswith(b'PACK\x00\x00\x00\x02\x00\x00\x00\x07'))

    def test_send_pack_without_changes(self):
        local = open_repo('a.git')
        self.addCleanup(tear_down_repo, local)

        target = open_repo('a.git')
        self.addCleanup(tear_down_repo, target)

        self.send_and_verify(b"master", local, target)

    def test_send_pack_with_changes(self):
        local = open_repo('a.git')
        self.addCleanup(tear_down_repo, local)

        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        with closing(Repo.init_bare(target_path)) as target:
            self.send_and_verify(b"master", local, target)

    def test_get_refs(self):
        local = open_repo('refs.git')
        self.addCleanup(tear_down_repo, local)

        client = LocalGitClient()
        refs = client.get_refs(local.path)
        self.assertDictEqual(local.refs.as_dict(), refs)

    def send_and_verify(self, branch, local, target):
        """Send a branch from local to remote repository and verify it worked."""
        client = LocalGitClient()
        ref_name = b"refs/heads/" + branch
        new_refs = client.send_pack(target.path,
                                    lambda _: { ref_name: local.refs[ref_name] },
                                    local.object_store.generate_pack_contents)

        self.assertEqual(local.refs[ref_name], new_refs[ref_name])

        obj_local = local.get_object(new_refs[ref_name])
        obj_target = target.get_object(new_refs[ref_name])
        self.assertEqual(obj_local, obj_target)


class HttpGitClientTests(TestCase):

    def test_get_url(self):
        base_url = 'https://github.com/jelmer/dulwich'
        path = '/jelmer/dulwich'
        c = HttpGitClient(base_url)

        url = c.get_url(path)
        self.assertEqual('https://github.com/jelmer/dulwich', url)

    def test_get_url_with_username_and_passwd(self):
        base_url = 'https://github.com/jelmer/dulwich'
        path = '/jelmer/dulwich'
        c = HttpGitClient(base_url, username='USERNAME', password='PASSWD')

        url = c.get_url(path)
        self.assertEqual('https://github.com/jelmer/dulwich', url)

    def test_init_username_passwd_set(self):
        url = 'https://github.com/jelmer/dulwich'

        c = HttpGitClient(url, config=None, username='user', password='passwd')
        self.assertEqual('user', c._username)
        self.assertEqual('passwd', c._password)
        [pw_handler] = [
            h for h in c.opener.handlers if getattr(h, 'passwd', None) is not None]
        self.assertEqual(
            ('user', 'passwd'),
            pw_handler.passwd.find_user_password(
                None, 'https://github.com/jelmer/dulwich'))

    def test_init_no_username_passwd(self):
        url = 'https://github.com/jelmer/dulwich'

        c = HttpGitClient(url, config=None)
        self.assertIs(None, c._username)
        self.assertIs(None, c._password)
        pw_handler = [
            h for h in c.opener.handlers if getattr(h, 'passwd', None) is not None]
        self.assertEqual(0, len(pw_handler))


class TCPGitClientTests(TestCase):

    def test_get_url(self):
        host = 'github.com'
        path = '/jelmer/dulwich'
        c = TCPGitClient(host)

        url = c.get_url(path)
        self.assertEqual('git://github.com/jelmer/dulwich', url)

    def test_get_url_with_port(self):
        host = 'github.com'
        path = '/jelmer/dulwich'
        port = 9090
        c = TCPGitClient(host, port=9090)

        url = c.get_url(path)
        self.assertEqual('git://github.com:9090/jelmer/dulwich', url)
