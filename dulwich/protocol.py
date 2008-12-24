# protocol.py -- Shared parts of the git protocols
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


