# client.py -- Implementation of the server side git protocols
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
# Copyright (C) 2008 John Carr
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
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

"""Client side support for the Git protocol."""

__docformat__ = 'restructuredText'

import os
import select
import socket
import subprocess

from dulwich.errors import (
    ChecksumMismatch,
    SendPackError,
    UpdateRefsError,
    )
from dulwich.protocol import (
    Protocol,
    TCP_GIT_PORT,
    ZERO_SHA,
    extract_capabilities,
    )
from dulwich.pack import (
    write_pack_data,
    )


def _fileno_can_read(fileno):
    """Check if a file descriptor is readable."""
    return len(select.select([fileno], [], [], 0)[0]) > 0

COMMON_CAPABILITIES = ["ofs-delta"]
FETCH_CAPABILITIES = ["multi_ack", "side-band-64k"] + COMMON_CAPABILITIES
SEND_CAPABILITIES = ['report-status'] + COMMON_CAPABILITIES

# TODO(durin42): this doesn't correctly degrade if the server doesn't
# support some capabilities. This should work properly with servers
# that don't support side-band-64k and multi_ack.
class GitClient(object):
    """Git smart server client.

    """

    def __init__(self, can_read, read, write, thin_packs=True,
        report_activity=None):
        """Create a new GitClient instance.

        :param can_read: Function that returns True if there is data available
            to be read.
        :param read: Callback for reading data, takes number of bytes to read
        :param write: Callback for writing data
        :param thin_packs: Whether or not thin packs should be retrieved
        :param report_activity: Optional callback for reporting transport
            activity.
        """
        self.proto = Protocol(read, write, report_activity)
        self._can_read = can_read
        self._fetch_capabilities = list(FETCH_CAPABILITIES)
        self._send_capabilities = list(SEND_CAPABILITIES)
        if thin_packs:
            self._fetch_capabilities.append("thin-pack")

    def read_refs(self):
        server_capabilities = None
        refs = {}
        # Receive refs from server
        for pkt in self.proto.read_pkt_seq():
            (sha, ref) = pkt.rstrip("\n").split(" ", 1)
            if server_capabilities is None:
                (ref, server_capabilities) = extract_capabilities(ref)
            refs[ref] = sha
        return refs, server_capabilities

    # TODO(durin42): add side-band-64k capability support here and advertise it
    def send_pack(self, path, determine_wants, generate_pack_contents):
        """Upload a pack to a remote repository.

        :param path: Repository path
        :param generate_pack_contents: Function that can return the shas of the
            objects to upload.

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        """
        old_refs, server_capabilities = self.read_refs()
        if 'report-status' not in server_capabilities:
            self._send_capabilities.remove('report-status')
        new_refs = determine_wants(old_refs)
        if not new_refs:
            self.proto.write_pkt_line(None)
            return {}
        want = []
        have = [x for x in old_refs.values() if not x == ZERO_SHA]
        sent_capabilities = False
        for refname in set(new_refs.keys() + old_refs.keys()):
            old_sha1 = old_refs.get(refname, ZERO_SHA)
            new_sha1 = new_refs.get(refname, ZERO_SHA)
            if old_sha1 != new_sha1:
                if sent_capabilities:
                    self.proto.write_pkt_line("%s %s %s" % (old_sha1, new_sha1,
                                                            refname))
                else:
                    self.proto.write_pkt_line(
                      "%s %s %s\0%s" % (old_sha1, new_sha1, refname,
                                        ' '.join(self._send_capabilities)))
                    sent_capabilities = True
            if not new_sha1 in (have, ZERO_SHA):
                want.append(new_sha1)
        self.proto.write_pkt_line(None)
        if not want:
            return new_refs
        objects = generate_pack_contents(have, want)
        (entries, sha) = write_pack_data(self.proto.write_file(), objects,
                                         len(objects))

        if 'report-status' in self._send_capabilities:
            unpack = self.proto.read_pkt_line().strip()
            if unpack != 'unpack ok':
                st = True
                # flush remaining error data
                while st is not None:
                    st = self.proto.read_pkt_line()
                raise SendPackError(unpack)
            statuses = []
            errs = False
            ref_status = self.proto.read_pkt_line()
            while ref_status:
                ref_status = ref_status.strip()
                statuses.append(ref_status)
                if not ref_status.startswith('ok '):
                    errs = True
                ref_status = self.proto.read_pkt_line()

            if errs:
                ref_status = {}
                ok = set()
                for status in statuses:
                    if ' ' not in status:
                        # malformed response, move on to the next one
                        continue
                    status, ref = status.split(' ', 1)

                    if status == 'ng':
                        if ' ' in ref:
                            ref, status = ref.split(' ', 1)
                    else:
                        ok.add(ref)
                    ref_status[ref] = status
                raise UpdateRefsError('%s failed to update' %
                                      ', '.join([ref for ref in ref_status
                                                 if ref not in ok]),
                                      ref_status=ref_status)
        # wait for EOF before returning
        data = self.proto.read()
        if data:
            raise SendPackError('Unexpected response %r' % data)
        return new_refs

    def fetch(self, path, target, determine_wants=None, progress=None):
        """Fetch into a target repository.

        :param path: Path to fetch from
        :param target: Target repository to fetch into
        :param determine_wants: Optional function to determine what refs
            to fetch
        :param progress: Optional progress function
        :return: remote refs
        """
        if determine_wants is None:
            determine_wants = target.object_store.determine_wants_all
        f, commit = target.object_store.add_pack()
        try:
            return self.fetch_pack(path, determine_wants,
                target.get_graph_walker(), f.write, progress)
        finally:
            commit()

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress):
        """Retrieve a pack from a git smart server.

        :param determine_wants: Callback that returns list of commits to fetch
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        """
        (refs, server_capabilities) = self.read_refs()
        wants = determine_wants(refs)
        if not wants:
            self.proto.write_pkt_line(None)
            return refs
        assert isinstance(wants, list) and type(wants[0]) == str
        self.proto.write_pkt_line("want %s %s\n" % (
            wants[0], ' '.join(self._fetch_capabilities)))
        for want in wants[1:]:
            self.proto.write_pkt_line("want %s\n" % want)
        self.proto.write_pkt_line(None)
        have = graph_walker.next()
        while have:
            self.proto.write_pkt_line("have %s\n" % have)
            if self._can_read():
                pkt = self.proto.read_pkt_line()
                parts = pkt.rstrip("\n").split(" ")
                if parts[0] == "ACK":
                    graph_walker.ack(parts[1])
                    assert parts[2] == "continue"
            have = graph_walker.next()
        self.proto.write_pkt_line("done\n")
        pkt = self.proto.read_pkt_line()
        while pkt:
            parts = pkt.rstrip("\n").split(" ")
            if parts[0] == "ACK":
                graph_walker.ack(pkt.split(" ")[1])
            if len(parts) < 3 or parts[2] != "continue":
                break
            pkt = self.proto.read_pkt_line()
        # TODO(durin42): this is broken if the server didn't support the
        # side-band-64k capability.
        for pkt in self.proto.read_pkt_seq():
            channel = ord(pkt[0])
            pkt = pkt[1:]
            if channel == 1:
                pack_data(pkt)
            elif channel == 2:
                progress(pkt)
            else:
                raise AssertionError("Invalid sideband channel %d" % channel)
        return refs


class TCPGitClient(GitClient):
    """A Git Client that works over TCP directly (i.e. git://)."""

    def __init__(self, host, port=None, *args, **kwargs):
        self._socket = socket.socket(type=socket.SOCK_STREAM)
        if port is None:
            port = TCP_GIT_PORT
        self._socket.connect((host, port))
        self.rfile = self._socket.makefile('rb', -1)
        self.wfile = self._socket.makefile('wb', 0)
        self.host = host
        super(TCPGitClient, self).__init__(lambda: _fileno_can_read(self._socket.fileno()), self.rfile.read, self.wfile.write, *args, **kwargs)

    def send_pack(self, path, changed_refs, generate_pack_contents):
        """Send a pack to a remote host.

        :param path: Path of the repository on the remote host
        """
        self.proto.send_cmd("git-receive-pack", path, "host=%s" % self.host)
        return super(TCPGitClient, self).send_pack(path, changed_refs, generate_pack_contents)

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data, progress):
        """Fetch a pack from the remote host.

        :param path: Path of the reposiutory on the remote host
        :param determine_wants: Callback that receives available refs dict and
            should return list of sha's to fetch.
        :param graph_walker: GraphWalker instance used to find missing shas
        :param pack_data: Callback for writing pack data
        :param progress: Callback for writing progress
        """
        self.proto.send_cmd("git-upload-pack", path, "host=%s" % self.host)
        return super(TCPGitClient, self).fetch_pack(path, determine_wants,
            graph_walker, pack_data, progress)


class SubprocessGitClient(GitClient):
    """Git client that talks to a server using a subprocess."""

    def __init__(self, *args, **kwargs):
        self.proc = None
        self._args = args
        self._kwargs = kwargs

    def _connect(self, service, *args, **kwargs):
        argv = [service] + list(args)
        self.proc = subprocess.Popen(argv, bufsize=0,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        def read_fn(size):
            return self.proc.stdout.read(size)
        def write_fn(data):
            self.proc.stdin.write(data)
            self.proc.stdin.flush()
        return GitClient(lambda: _fileno_can_read(self.proc.stdout.fileno()), read_fn, write_fn, *args, **kwargs)

    def send_pack(self, path, changed_refs, generate_pack_contents):
        """Upload a pack to the server.

        :param path: Path to the git repository on the server
        :param changed_refs: Dictionary with new values for the refs
        :param generate_pack_contents: Function that returns an iterator over
            objects to send
        """
        client = self._connect("git-receive-pack", path)
        return client.send_pack(path, changed_refs, generate_pack_contents)

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
        progress):
        """Retrieve a pack from the server

        :param path: Path to the git repository on the server
        :param determine_wants: Function that receives existing refs
            on the server and returns a list of desired shas
        :param graph_walker: GraphWalker instance
        :param pack_data: Function that can write pack data
        :param progress: Function that can write progress texts
        """
        client = self._connect("git-upload-pack", path)
        return client.fetch_pack(path, determine_wants, graph_walker, pack_data,
                                 progress)


class SSHSubprocess(object):
    """A socket-like object that talks to an ssh subprocess via pipes."""

    def __init__(self, proc):
        self.proc = proc
        self.read = self.recv = proc.stdout.read
        self.write = self.send = proc.stdin.write

    def close(self):
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.proc.wait()


class SSHVendor(object):

    def connect_ssh(self, host, command, username=None, port=None):
        #FIXME: This has no way to deal with passwords..
        args = ['ssh', '-x']
        if port is not None:
            args.extend(['-p', str(port)])
        if username is not None:
            host = "%s@%s" % (username, host)
        args.append(host)
        proc = subprocess.Popen(args + command,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        return SSHSubprocess(proc)

# Can be overridden by users
get_ssh_vendor = SSHVendor


class SSHGitClient(GitClient):

    def __init__(self, host, port=None, username=None, *args, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        self._args = args
        self._kwargs = kwargs

    def send_pack(self, path, determine_wants, generate_pack_contents):
        remote = get_ssh_vendor().connect_ssh(
            self.host, ["git-receive-pack '%s'" % path],
            port=self.port, username=self.username)
        client = GitClient(lambda: _fileno_can_read(remote.proc.stdout.fileno()), remote.recv, remote.send, *self._args, **self._kwargs)
        return client.send_pack(path, determine_wants, generate_pack_contents)

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
        progress):
        remote = get_ssh_vendor().connect_ssh(self.host, ["git-upload-pack '%s'" % path], port=self.port, username=self.username)
        client = GitClient(lambda: _fileno_can_read(remote.proc.stdout.fileno()), remote.recv, remote.send, *self._args, **self._kwargs)
        return client.fetch_pack(path, determine_wants, graph_walker, pack_data,
                                 progress)


def get_transport_and_path(uri):
    """Obtain a git client from a URI or path.

    :param uri: URI or path
    :return: Tuple with client instance and relative path.
    """
    from dulwich.client import TCPGitClient, SSHGitClient, SubprocessGitClient
    for handler, transport in (("git://", TCPGitClient), ("git+ssh://", SSHGitClient)):
        if uri.startswith(handler):
            host, path = uri[len(handler):].split("/", 1)
            return transport(host), "/"+path
    # if its not git or git+ssh, try a local url..
    return SubprocessGitClient(), uri
