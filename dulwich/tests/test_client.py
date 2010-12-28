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

from dulwich.client import (
    GitClient,
    TCPGitClient,
    SubprocessGitClient,
    SSHGitClient,
    get_transport_and_path,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.protocol import (
    TCP_GIT_PORT,
    Protocol,
    )


class DummyClient(GitClient):
    def __init__(self, can_read, read, write):
        self.can_read = can_read
        self.read = read
        self.write = write
        GitClient.__init__(self)

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
        self.assertEquals(set(['multi_ack', 'side-band-64k', 'ofs-delta',
                               'thin-pack']),
                          set(self.client._fetch_capabilities))
        self.assertEquals(set(['ofs-delta', 'report-status']),
                          set(self.client._send_capabilities))

    def test_fetch_pack_none(self):
        self.rin.write(
            "008855dcc6bf963f922e1ed5c4bbaaefcfacef57b1d7 HEAD.multi_ack thin-pack side-band side-band-64k ofs-delta shallow no-progress include-tag\n"
            "0000")
        self.rin.seek(0)
        self.client.fetch_pack("bla", lambda heads: [], None, None, None)
        self.assertEquals(self.rout.getvalue(), "0000")

    def test_get_transport_and_path_tcp(self):
        client, path = get_transport_and_path('git://foo.com/bar/baz')
        self.assertTrue(isinstance(client, TCPGitClient))
        self.assertEquals('foo.com', client._host)
        self.assertEquals(TCP_GIT_PORT, client._port)
        self.assertEqual('/bar/baz', path)

        client, path = get_transport_and_path('git://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(client, TCPGitClient))
        self.assertEquals('foo.com', client._host)
        self.assertEquals(1234, client._port)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_explicit(self):
        client, path = get_transport_and_path('git+ssh://foo.com/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEquals('foo.com', client.host)
        self.assertEquals(None, client.port)
        self.assertEquals(None, client.username)
        self.assertEqual('/bar/baz', path)

        client, path = get_transport_and_path('git+ssh://foo.com:1234/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEquals('foo.com', client.host)
        self.assertEquals(1234, client.port)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_ssh_implicit(self):
        client, path = get_transport_and_path('foo:/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEquals('foo', client.host)
        self.assertEquals(None, client.port)
        self.assertEquals(None, client.username)
        self.assertEqual('/bar/baz', path)

        client, path = get_transport_and_path('foo.com:/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEquals('foo.com', client.host)
        self.assertEquals(None, client.port)
        self.assertEquals(None, client.username)
        self.assertEqual('/bar/baz', path)

        client, path = get_transport_and_path('user@foo.com:/bar/baz')
        self.assertTrue(isinstance(client, SSHGitClient))
        self.assertEquals('foo.com', client.host)
        self.assertEquals(None, client.port)
        self.assertEquals('user', client.username)
        self.assertEqual('/bar/baz', path)

    def test_get_transport_and_path_subprocess(self):
        client, path = get_transport_and_path('foo.bar/baz')
        self.assertTrue(isinstance(client, SubprocessGitClient))
        self.assertEquals('foo.bar/baz', path)

    def test_get_transport_and_path_error(self):
        self.assertRaises(ValueError, get_transport_and_path, 'foo://bar/baz')


class SSHGitClientTests(TestCase):

    def setUp(self):
        super(SSHGitClientTests, self).setUp()
        self.client = SSHGitClient("git.samba.org")

    def test_default_command(self):
        self.assertEquals("git-upload-pack", self.client._get_cmd_path("upload-pack"))

    def test_alternative_command_path(self):
        self.client.alternative_paths["upload-pack"] = "/usr/lib/git/git-upload-pack"
        self.assertEquals("/usr/lib/git/git-upload-pack", self.client._get_cmd_path("upload-pack"))

