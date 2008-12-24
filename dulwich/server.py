# server.py -- Implementation of the server side git protocols
# Copryight (C) 2008 John Carr <john.carr@unrouted.co.uk>
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

import SocketServer
from dulwich.protocol import Protocol, TCP_GIT_PORT, extract_capabilities
from dulwich.repo import Repo
from dulwich.pack import PackData, Pack, write_pack_data
import os, sha, tempfile

class Backend(object):

    def get_refs(self):
        """
        Get all the refs in the repository

        :return: list of tuple(name, sha)
        """
        raise NotImplementedError

    def has_revision(self, sha):
        """
        Is a given sha in this repository?

        :return: True or False
        """
        raise NotImplementedError

    def apply_pack(self, refs, read):
        """ Import a set of changes into a repository and update the refs

        :param refs: list of tuple(name, sha)
        :param read: callback to read from the incoming pack
        """
        raise NotImplementedError

    def generate_pack(self, want, have, write, progress):
        """
        Generate a pack containing all commits a client is missing

        :param want: is a list of sha's the client desires
        :param have: is a list of sha's the client has (allowing us to send the minimal pack)
        :param write: is a callback to write pack data to the client
        :param progress: is a callback to send progress messages to the client
        """
        raise NotImplementedError


class GitBackend(Backend):

    def __init__(self, gitdir=None):
        self.gitdir = gitdir

        if not self.gitdir:
            self.gitdir = tempfile.mkdtemp()
            Repo.create(self.gitdir)

        self.repo = Repo(self.gitdir)

    def get_refs(self):
        refs = []
        if self.repo.head():
            refs.append(('HEAD', self.repo.head()))
        for ref, sha in self.repo.heads().items():
            refs.append(('refs/heads/'+ref,sha))
        return refs

    def has_revision(self, sha):
        return self.repo.get_object(sha) != None

    def apply_pack(self, refs, read):
        # store the incoming pack in the repository
        fd, name = tempfile.mkstemp(suffix='.pack', prefix='', dir=self.repo.pack_dir())
        os.write(fd, read())
        os.close(fd)

        # strip '.pack' off our filename
        basename = name[:-5]

        # generate an index for it
        pd = PackData(name)
        pd.create_index_v2(basename+".idx")

        for oldsha, sha, ref in refs:
            if ref == "0" * 40:
                self.repo.remove_ref(ref)
            else:
                self.repo.set_ref(ref, sha)

        print "pack applied"

    def generate_pack(self, want, have, write, progress):
        progress("dul-daemon says what\n")

        sha_queue = []

        commits_to_send = want[:]
        for sha in commits_to_send:
            if sha in sha_queue:
                continue

            sha_queue.append((1,sha))

            c = self.repo.commit(sha)
            for p in c.parents():
                if not p in commits_to_send:
                    commits_to_send.append(p)

            def parse_tree(tree, sha_queue):
                for mode, name, x in tree.entries():
                    if not x in sha_queue:
                        try:
                            t = self.repo.get_tree(x)
                            sha_queue.append((2, x))
                            parse_tree(t, sha_queue)
                        except:
                            sha_queue.append((3, x))

            treesha = c.tree()
            if treesha not in sha_queue:
                sha_queue.append((2, treesha))
                t = self.repo.get_tree(treesha)
                parse_tree(t, sha_queue)

            progress("counting objects: %d\r" % len(sha_queue))

        progress("counting objects: %d, done.\n" % len(sha_queue))

        write_pack_data(write, (self.repo.get_object(sha).as_raw_string() for sha in sha_queue))

        progress("how was that, then?\n")


class Handler(object):

    def __init__(self, backend, read, write):
        self.backend = backend
        self.proto = Protocol(read, write)

    def capabilities(self):
        return " ".join(self.default_capabilities())


class UploadPackHandler(Handler):

    def default_capabilities(self):
        return ("multi_ack", "side-band-64k", "thin-pack", "ofs-delta")

    def handle(self):
        refs = self.backend.get_refs()

        if refs:
            self.proto.write_pkt_line("%s %s\x00%s\n" % (refs[0][1], refs[0][0], self.capabilities()))
            for i in range(1, len(refs)):
                ref = refs[i]
                self.proto.write_pkt_line("%s %s\n" % (ref[1], ref[0]))

        # i'm done..
        self.proto.write("0000")

        # Now client will either send "0000", meaning that it doesnt want to pull.
        # or it will start sending want want want commands
        want = self.proto.read_pkt_line()
        if want == None:
            return

        want, client_capabilities = extract_capabilities(want)

        # Keep reading the list of demands until we hit another "0000" 
        want_revs = []
        while want and want[:4] == 'want':
            want_rev = want[5:45]
            # FIXME: This check probably isnt needed?
            if self.backend.has_revision(want_rev):
               want_revs.append(want_rev)
            want = self.proto.read_pkt_line()
        
        # Client will now tell us which commits it already has - if we have them we ACK them
        # this allows client to stop looking at that commits parents (main reason why git pull is fast)
        last_sha = None
        have_revs = []
        have = self.proto.read_pkt_line()
        while have and have[:4] == 'have':
            have_ref = have[5:45]
            if self.backend.has_revision(have_ref):
                self.proto.write_pkt_line("ACK %s continue\n" % have_ref)
                last_sha = have_ref
                have_revs.append(have_ref)
            have = self.proto.read_pkt_line()

        # At some point client will stop sending commits and will tell us it is done
        assert(have[:4] == "done")

        # Oddness: Git seems to resend the last ACK, without the "continue" statement
        if last_sha:
            self.proto.write_pkt_line("ACK %s\n" % last_sha)

        # The exchange finishes with a NAK
        self.proto.write_pkt_line("NAK\n")
      
        self.backend.generate_pack(want_revs, have_revs, lambda x: self.proto.write_sideband(1, x), lambda x: self.proto.write_sideband(2, x))

        # we are done
        self.proto.write("0000")


class ReceivePackHandler(Handler):

    def default_capabilities(self):
        return ("report-status", "delete-refs")

    def handle(self):
        refs = self.backend.get_refs()

        if refs:
            self.proto.write_pkt_line("%s %s\x00%s\n" % (refs[0][1], refs[0][0], self.capabilities()))
            for i in range(1, len(refs)):
                ref = refs[i]
                self.proto.write_pkt_line("%s %s\n" % (ref[1], ref[0]))
        else:
            self.proto.write_pkt_line("0000000000000000000000000000000000000000 capabilities^{} %s" % self.capabilities())

        self.proto.write("0000")

        client_refs = []
        ref = self.proto.read_pkt_line()

        # if ref is none then client doesnt want to send us anything..
        if ref is None:
            return

        ref, client_capabilities = extract_capabilities(ref)

        # client will now send us a list of (oldsha, newsha, ref)
        while ref:
            client_refs.append(ref.split())
            ref = self.proto.read_pkt_line()

        # backend can now deal with this refs and read a pack using self.read
        self.backend.apply_pack(client_refs, self.proto.read)

        # when we have read all the pack from the client, it assumes everything worked OK
        # there is NO ack from the server before it reports victory.


class TCPGitRequestHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        proto = Protocol(self.rfile.read, self.wfile.write)
        cmd, args = proto.read_cmd()

        # switch case to handle the specific git command
        if command == 'git-upload-pack':
            cls = UploadPackHandler
        elif command == 'git-receive-pack':
            cls = ReceivePackHandler
        else:
            return

        h = cls(self.backend, self.proto.read, self.proto.write)
        h.handle()


class TCPGitServer(SocketServer.TCPServer):

    allow_reuse_address = True
    serve = SocketServer.TCPServer.serve_forever

    def __init__(self, backend, listen_addr, port=TCP_GIT_PORT):
        self.backend = backend
        SocketServer.TCPServer.__init__(self, (listen_addr, port), TCPGitRequestHandler)


