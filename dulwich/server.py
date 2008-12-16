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
# import dulwich as git


class Backend(object):

    def __init__(self):
        pass

    def get_refs(self):
        raise NotImplementedError

    def has_revision(self):
        raise NotImplementedError

    def apply_pack(self, refs, read):
        raise NotImplementedError

    def generate_pack(self, want, have, write, progress):
        raise NotImplementedError


class Handler(object):

    def __init__(self, backend, read, write):
        self.backend = backend
        self.read = read
        self.write = write

    def read_pkt_line(self):
        """
        reads a 'git line' of info from the stream
        """
        size = int(self.read(4), 16)
        if size == 0:
            return None
        return self.read(size-4)

    def write_pkt_line(self, line):
        self.write("%04x%s" % (len(line)+4, line))

    def write_sideband(self, channel, blob):
        # a pktline can be a max of 65535. a sideband line can therefore be
        # 65535-5 = 65530
        # WTF: Why have the len in ASCII, but the channel in binary.
        while blob:
            self.write_pkt_line("%s%s" % (chr(channel), blob[:65530]))
            blob = blob[65530:]

    def handle(self):
        raise NotImplementedError


class UploadPackHandler(Handler):

    def handle(self):
        refs = self.backend.get_refs()

        self.write_pkt_line("%s %s\x00multi_ack side-band-64k thin-pack ofs-delta\0x0a" % (refs[0][1], refs[0][0]))
        for i in range(1, len(refs)-1):
            ref = refs[i]
            self.write_pkt_line("%s %s\0x0a" % (ref[1], ref[0]))

        # i'm done...
        self.write("0000")

        # Now client will either send "0000", meaning that it doesnt want to pull.
        # or it will start sending want want want commands
        want = self.read_pkt_line()
        if want == None:
            return
       
        # Keep reading the list of demands until we hit another "0000" 
        want_revs = []
        while want and want[:4] == 'want':
            want_rev = want[5:]
            # FIXME: This check probably isnt needed?
            if self.backend.has_revision(want_rev):
               want_revs.append(want_rev)
            want = self.read_pkt_line()
        
        # Client will now tell us which commits it already has - if we have them we ACK them
        # this allows client to stop looking at that commits parents (main reason why git pull is fast)
        last_sha = None
        have_revs = []
        have = self.read_pkt_line()
        while have and have[:4] == 'have':
            have_ref = have[6:40]
            if self.backend.has_revision(hav_rev):
                self.write_pkt_line("ACK %s continue\n" % sha)
                last_sha = sha
                have_revs.append(rev_id)
            have = self.read_pkt_line()

        # At some point client will stop sending commits and will tell us it is done
        assert(have[:4] == "done")

        # Oddness: Git seems to resend the last ACK, without the "continue" statement
        if last_sha:
            self.write_pkt_line("ACK %s\n" % last_sha)

        # The exchange finishes with a NAK
        self.write_pkt_line("NAK\n")
      
        #if True: # False: #self.no_progress == False:
        #    self.write_sideband(2, "Bazaar is preparing your pack, plz hold.\n")

        #    for x in range(1,200):
        #        self.write_sideband(2, "Counting objects: %d\x0d" % x*2)
        #    self.write_sideband(2, "Counting objects: 200, done.\n")

        #    for x in range(1,100):
        #        self.write_sideband(2, "Compressiong objects: %d (%d/%d)\x0d" % (x, x*2, 200))
        #    self.write_sideband(2, "Compressing objects: 100% (200/200), done.\n")

        self.backend.generate_pack(want, need, self.write)


class ReceivePackHandler(Handler):

    def handle(self):
        refs = self.backend.get_refs()

        self.write_pkt_line("%s %s\x00multi_ack side-band-64k thin-pack ofs-delta\0x0a" % (refs[0][1], refs[0][0]))
        for i in range(1, len(refs)-1):
            ref = refs[i]
            self.write_pkt_line("%s %s\0x0a" % (ref[1], ref[0]))

        self.write("0000")

        client_refs = []
        ref = self.read_pkt_line()
        while ref:
            client_refs.append(ref.split())
            ref = self.read_pkt_line()

        self.backend.apply_pack(client_refs, self.read)


class TCPGitRequestHandler(SocketServer.StreamRequestHandler, Handler):

    def __init__(self, request, client_address, server):
        SocketServer.StreamRequestHandler.__init__(self, request, client_address, server)
        Handler.__init__(self, server.backend, self.rfile.read, self.wfile.write)

    def handle(self):
        request = self.read_pkt_line()

        # up until the space is the command to run, everything after is parameters
        splice_point = request.find(' ')
        command, params = request[:splice_point], request[splice_point+1:]

        # params are null seperated
        params = params.split(chr(0))

        # switch case to handle the specific git command
        if command == 'git-upload-pack':
            cls = UploadPackHandler
        elif command == 'git-receive-pack':
            cls = ReceivePackHandler
        else:
            return

        h = cls(self.backend, self.read, self.write)
        h.handle()


class TCPGitServer(SocketServer.TCPServer):

    allow_reuse_address = True
    serve = SocketServer.TCPServer.serve_forever

    def __init__(self, backend, addr):
        self.backend = backend
        SocketServer.TCPServer.__init__(self, addr, TCPGitRequestHandler)


