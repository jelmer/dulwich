# server.py -- Implementation of the server side git protocols
# Copryight (C) 2008 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License.
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

import select
import socket
from dulwich.protocol import Protocol, TCP_GIT_PORT, extract_capabilities

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


class GitClient(object):
    """Git smart server client.

    """

    def __init__(self, fileno, read, write):
        self.proto = Protocol(read, write)
        self.fileno = fileno

    def capabilities(self):
        return "multi_ack side-band-64k thin-pack ofs-delta"

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

    def send_pack(self, path):
        refs, server_capabilities = self.read_refs()
        changed_refs = [] # FIXME
        if not changed_refs:
            self.proto.write_pkt_line(None)
            return
        self.proto.write_pkt_line("%s %s %s\0%s" % (changed_refs[0][0], changed_refs[0][1], changed_refs[0][2], self.capabilities()))
        for changed_ref in changed_refs[:]:
            self.proto.write_pkt_line("%s %s %s" % changed_refs)
        self.proto.write_pkt_line(None)
        # FIXME: Send pack

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
            if len(select.select([self.fileno], [], [], 0)[0]) > 0:
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

    def __init__(self, host, port=TCP_GIT_PORT):
        self._socket = socket.socket()
        self._socket.connect((host, port))
        self.rfile = self._socket.makefile('rb', -1)
        self.wfile = self._socket.makefile('wb', 0)
        self.host = host
        super(TCPGitClient, self).__init__(self._socket.fileno(), self.rfile.read, self.wfile.write)

    def send_pack(self, path):
        self.proto.send_cmd("git-receive-pack", path, "host=%s" % self.host)
        super(TCPGitClient, self).send_pack(path)

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data, progress):
        self.proto.send_cmd("git-upload-pack", path, "host=%s" % self.host)
        super(TCPGitClient, self).fetch_pack(path, determine_wants, graph_walker, pack_data, progress)
