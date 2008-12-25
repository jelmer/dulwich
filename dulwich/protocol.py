# protocol.py -- Shared parts of the git protocols
# Copryight (C) 2008 John Carr <john.carr@unrouted.co.uk>
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
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

TCP_GIT_PORT = 9418

class Protocol(object):

    def __init__(self, read, write):
        self.read = read
        self.write = write

    def read_pkt_line(self):
        """
        Reads a 'pkt line' from the remote git process

        :return: The next string from the stream
        """
        sizestr = self.read(4)
        if not sizestr:
            return None
        size = int(sizestr, 16)
        if size == 0:
            return None
        return self.read(size-4)

    def read_pkt_seq(self):
        pkt = self.read_pkt_line()
        while pkt:
            yield pkt
            pkt = self.read_pkt_line()

    def write_pkt_line(self, line):
        """
        Sends a 'pkt line' to the remote git process

        :param line: A string containing the data to send
        """
        if line is None:
            self.write("0000")
        else:
            self.write("%04x%s" % (len(line)+4, line))

    def write_sideband(self, channel, blob):
        """
        Write data to the sideband (a git multiplexing method)

        :param channel: int specifying which channel to write to
        :param blob: a blob of data (as a string) to send on this channel
        """
        # a pktline can be a max of 65535. a sideband line can therefore be
        # 65535-5 = 65530
        # WTF: Why have the len in ASCII, but the channel in binary.
        while blob:
            self.write_pkt_line("%s%s" % (chr(channel), blob[:65530]))
            blob = blob[65530:]

    def send_cmd(self, cmd, *args):
        """
        Send a command and some arguments to a git server

        Only used for git://

        :param cmd: The remote service to access
        :param args: List of arguments to send to remove service
        """
        self.proto.write_pkt_line("%s %s" % (name, "".join(["%s\0" % a for a in args])))

    def read_cmd(self):
        """
        Read a command and some arguments from the git client

        Only used for git://

        :return: A tuple of (command, [list of arguments])
        """
        line = self.read_pkt_line()
        splice_at = line.find(" ")
        cmd, args = line[:splice_at], line[splice_at+1:]
        return cmd, args.split(chr(0))
 

def extract_capabilities(text):
    if not "\0" in text:
        return text, None
    capabilities = text.split("\0")
    return (capabilities[0], capabilities[1:])

