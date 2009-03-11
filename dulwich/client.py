# server.py -- Implementation of the server side git protocols
# Copryight (C) 2008 Jelmer Vernooij <jelmer@samba.org>
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

import os
import select
import socket
import subprocess

from dulwich.protocol import (
    Protocol,
    TCP_GIT_PORT,
    extract_capabilities,
    )
from dulwich.pack import (
    write_pack_data,
    )


def _fileno_can_read(fileno):
    return len(select.select([fileno], [], [], 0)[0]) > 0


class SimpleFetchGraphWalker(object):

    def __init__(self, local_heads, get_parents):
        self.heads = set(local_heads)
        self.get_parents = get_parents
        self.parents = {}

    def ack(self, ref):
        if ref in self.heads:
            self.heads.remove(ref)
        if ref in self.parents:
            for p in self.parents[ref]:
                self.ack(p)

    def next(self):
        if self.heads:
            ret = self.heads.pop()
            ps = self.get_parents(ret)
            self.parents[ret] = ps
            self.heads.update(ps)
            return ret
        return None


CAPABILITIES = ["multi_ack", "side-band-64k", "ofs-delta"]


class GitClient(object):
    """Git smart server client.

    """

    def __init__(self, can_read, read, write, thin_packs=True):
        self.proto = Protocol(read, write)
        self._can_read = can_read
        self._capabilities = list(CAPABILITIES)
        if thin_packs:
            self._capabilities.append("thin-pack")

    def capabilities(self):
        return " ".join(self._capabilities)

    def read_refs(self):
        server_capabilities = None
        refs = {}
        # Receive refs from server
        for pkt in self.proto.read_pkt_seq():
            (sha, ref) = pkt.rstrip("\n").split(" ", 1)
            if server_capabilities is None:
                (ref, server_capabilities) = extract_capabilities(ref)
            if not (ref == "capabilities^{}" and sha == "0" * 40):
                refs[ref] = sha
        return refs, server_capabilities

    def send_pack(self, path, generate_pack_contents):
        refs, server_capabilities = self.read_refs()
        changed_refs = [] # FIXME
        if not changed_refs:
            self.proto.write_pkt_line(None)
            return
        self.proto.write_pkt_line("%s %s %s\0%s" % (changed_refs[0][0], changed_refs[0][1], changed_refs[0][2], self.capabilities()))
        want = []
        have = []
        for changed_ref in changed_refs[:]:
            self.proto.write_pkt_line("%s %s %s" % changed_refs)
            want.append(changed_refs[1])
            if changed_refs[0] != "0"*40:
                have.append(changed_refs[0])
        self.proto.write_pkt_line(None)
        shas = generate_pack_contents(want, have, None)
        write_pack_data(self.write, shas, len(shas))

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data, progress):
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
            return
        self.proto.write_pkt_line("want %s %s\n" % (wants[0], self.capabilities()))
        for want in wants[1:]:
            self.proto.write_pkt_line("want %s\n" % want)
        self.proto.write_pkt_line(None)
        have = graph_walker.next()
        while have:
            self.proto.write_pkt_line("have %s\n" % have)
            if self.can_read():
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
        for pkt in self.proto.read_pkt_seq():
            channel = ord(pkt[0])
            pkt = pkt[1:]
            if channel == 1:
                pack_data(pkt)
            elif channel == 2:
                progress(pkt)
            else:
                raise AssertionError("Invalid sideband channel %d" % channel)


class TCPGitClient(GitClient):

    def __init__(self, host, port=TCP_GIT_PORT, *args, **kwargs):
        self._socket = socket.socket(type=socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self.rfile = self._socket.makefile('rb', -1)
        self.wfile = self._socket.makefile('wb', 0)
        self.host = host
        super(TCPGitClient, self).__init__(lambda: _fileno_can_read(self._socket.fileno()), self.rfile.read, self.wfile.write, *args, **kwargs)

    def send_pack(self, path):
        self.proto.send_cmd("git-receive-pack", path, "host=%s" % self.host)
        super(TCPGitClient, self).send_pack(path)

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data, progress):
        self.proto.send_cmd("git-upload-pack", path, "host=%s" % self.host)
        super(TCPGitClient, self).fetch_pack(path, determine_wants, graph_walker, pack_data, progress)


class SubprocessGitClient(GitClient):

    def __init__(self, *args, **kwargs):
        self.proc = None
        self._args = args
        self._kwargs = kwargs

    def _connect(self, service, *args):
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

    def send_pack(self, path):
        client = self._connect("git-receive-pack", path)
        client.send_pack(path)

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data, progress):
        client = self._connect("git-upload-pack", path)
        client.fetch_pack(path, determine_wants, graph_walker, pack_data, progress)


class SSHSubprocess(object):
    """A socket-like object that talks to an ssh subprocess via pipes."""

    def __init__(self, proc):
        self.proc = proc

    def send(self, data):
        return os.write(self.proc.stdin.fileno(), data)

    def recv(self, count):
        return self.proc.stdout.read(count)

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

    def __init__(self, host, port=None):
        self.host = host
        self.port = port

    def send_pack(self, path):
        remote = get_ssh_vendor().connect_ssh(self.host, ["git-receive-pack %s" % path], port=self.port)
        client = GitClient(lambda: _fileno_can_read(remote.proc.stdout.fileno()), remote.recv, remote.send)
        client.send_pack(path)

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data, progress):
        remote = get_ssh_vendor().connect_ssh(self.host, ["git-upload-pack %s" % path], port=self.port)
        client = GitClient(lambda: _fileno_can_read(remote.proc.stdout.fileno()), remote.recv, remote.send)
        client.fetch_pack(path, determine_wants, graph_walker, pack_data, progress)

