# protocol.py -- Shared parts of the git protocols
# Copyright (C) 2008 John Carr <john.carr@unrouted.co.uk>
# Copyright (C) 2008-2012 Jelmer Vernooij <jelmer@samba.org>
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

"""Generic functions for talking the git smart server protocol."""

from io import BytesIO
from os import (
    SEEK_END,
    )
import socket

from dulwich.errors import (
    HangupException,
    GitProtocolError,
    )

TCP_GIT_PORT = 9418

ZERO_SHA = "0" * 40

SINGLE_ACK = 0
MULTI_ACK = 1
MULTI_ACK_DETAILED = 2


class ProtocolFile(object):
    """A dummy file for network ops that expect file-like objects."""

    def __init__(self, read, write):
        self.read = read
        self.write = write

    def tell(self):
        pass

    def close(self):
        pass


def pkt_line(data):
    """Wrap data in a pkt-line.

    :param data: The data to wrap, as a str or None.
    :return: The data prefixed with its length in pkt-line format; if data was
        None, returns the flush-pkt ('0000').
    """
    if data is None:
        return '0000'
    return '%04x%s' % (len(data) + 4, data)


class Protocol(object):
    """Class for interacting with a remote git process over the wire.

    Parts of the git wire protocol use 'pkt-lines' to communicate. A pkt-line
    consists of the length of the line as a 4-byte hex string, followed by the
    payload data. The length includes the 4-byte header. The special line '0000'
    indicates the end of a section of input and is called a 'flush-pkt'.

    For details on the pkt-line format, see the cgit distribution:
        Documentation/technical/protocol-common.txt
    """

    def __init__(self, read, write, report_activity=None):
        self.read = read
        self.write = write
        self.report_activity = report_activity
        self._readahead = None

    def read_pkt_line(self):
        """Reads a pkt-line from the remote git process.

        This method may read from the readahead buffer; see unread_pkt_line.

        :return: The next string from the stream, without the length prefix, or
            None for a flush-pkt ('0000').
        """
        if self._readahead is None:
            read = self.read
        else:
            read = self._readahead.read
            self._readahead = None

        try:
            sizestr = read(4)
            if not sizestr:
                raise HangupException()
            size = int(sizestr, 16)
            if size == 0:
                if self.report_activity:
                    self.report_activity(4, 'read')
                return None
            if self.report_activity:
                self.report_activity(size, 'read')
            return read(size-4)
        except socket.error as e:
            raise GitProtocolError(e)

    def eof(self):
        """Test whether the protocol stream has reached EOF.

        Note that this refers to the actual stream EOF and not just a flush-pkt.

        :return: True if the stream is at EOF, False otherwise.
        """
        try:
            next_line = self.read_pkt_line()
        except HangupException:
            return True
        self.unread_pkt_line(next_line)
        return False

    def unread_pkt_line(self, data):
        """Unread a single line of data into the readahead buffer.

        This method can be used to unread a single pkt-line into a fixed
        readahead buffer.

        :param data: The data to unread, without the length prefix.
        :raise ValueError: If more than one pkt-line is unread.
        """
        if self._readahead is not None:
            raise ValueError('Attempted to unread multiple pkt-lines.')
        self._readahead = BytesIO(pkt_line(data))

    def read_pkt_seq(self):
        """Read a sequence of pkt-lines from the remote git process.

        :return: Yields each line of data up to but not including the next flush-pkt.
        """
        pkt = self.read_pkt_line()
        while pkt:
            yield pkt
            pkt = self.read_pkt_line()

    def write_pkt_line(self, line):
        """Sends a pkt-line to the remote git process.

        :param line: A string containing the data to send, without the length
            prefix.
        """
        try:
            line = pkt_line(line)
            self.write(line)
            if self.report_activity:
                self.report_activity(len(line), 'write')
        except socket.error as e:
            raise GitProtocolError(e)

    def write_file(self):
        """Return a writable file-like object for this protocol."""

        class ProtocolFile(object):

            def __init__(self, proto):
                self._proto = proto
                self._offset = 0

            def write(self, data):
                self._proto.write(data)
                self._offset += len(data)

            def tell(self):
                return self._offset

            def close(self):
                pass

        return ProtocolFile(self)

    def write_sideband(self, channel, blob):
        """Write multiplexed data to the sideband.

        :param channel: An int specifying the channel to write to.
        :param blob: A blob of data (as a string) to send on this channel.
        """
        # a pktline can be a max of 65520. a sideband line can therefore be
        # 65520-5 = 65515
        # WTF: Why have the len in ASCII, but the channel in binary.
        while blob:
            self.write_pkt_line("%s%s" % (chr(channel), blob[:65515]))
            blob = blob[65515:]

    def send_cmd(self, cmd, *args):
        """Send a command and some arguments to a git server.

        Only used for the TCP git protocol (git://).

        :param cmd: The remote service to access.
        :param args: List of arguments to send to remove service.
        """
        self.write_pkt_line("%s %s" % (cmd, "".join(["%s\0" % a for a in args])))

    def read_cmd(self):
        """Read a command and some arguments from the git client

        Only used for the TCP git protocol (git://).

        :return: A tuple of (command, [list of arguments]).
        """
        line = self.read_pkt_line()
        splice_at = line.find(" ")
        cmd, args = line[:splice_at], line[splice_at+1:]
        assert args[-1] == "\x00"
        return cmd, args[:-1].split(chr(0))


_RBUFSIZE = 8192  # Default read buffer size.


class ReceivableProtocol(Protocol):
    """Variant of Protocol that allows reading up to a size without blocking.

    This class has a recv() method that behaves like socket.recv() in addition
    to a read() method.

    If you want to read n bytes from the wire and block until exactly n bytes
    (or EOF) are read, use read(n). If you want to read at most n bytes from the
    wire but don't care if you get less, use recv(n). Note that recv(n) will
    still block until at least one byte is read.
    """

    def __init__(self, recv, write, report_activity=None, rbufsize=_RBUFSIZE):
        super(ReceivableProtocol, self).__init__(self.read, write,
                                                 report_activity)
        self._recv = recv
        self._rbuf = BytesIO()
        self._rbufsize = rbufsize

    def read(self, size):
        # From _fileobj.read in socket.py in the Python 2.6.5 standard library,
        # with the following modifications:
        #  - omit the size <= 0 branch
        #  - seek back to start rather than 0 in case some buffer has been
        #    consumed.
        #  - use SEEK_END instead of the magic number.
        # Copyright (c) 2001-2010 Python Software Foundation; All Rights Reserved
        # Licensed under the Python Software Foundation License.
        # TODO: see if buffer is more efficient than cBytesIO.
        assert size > 0

        # Our use of BytesIO rather than lists of string objects returned by
        # recv() minimizes memory usage and fragmentation that occurs when
        # rbufsize is large compared to the typical return value of recv().
        buf = self._rbuf
        start = buf.tell()
        buf.seek(0, SEEK_END)
        # buffer may have been partially consumed by recv()
        buf_len = buf.tell() - start
        if buf_len >= size:
            # Already have size bytes in our buffer?  Extract and return.
            buf.seek(start)
            rv = buf.read(size)
            self._rbuf = BytesIO()
            self._rbuf.write(buf.read())
            self._rbuf.seek(0)
            return rv

        self._rbuf = BytesIO()  # reset _rbuf.  we consume it via buf.
        while True:
            left = size - buf_len
            # recv() will malloc the amount of memory given as its
            # parameter even though it often returns much less data
            # than that.  The returned data string is short lived
            # as we copy it into a BytesIO and free it.  This avoids
            # fragmentation issues on many platforms.
            data = self._recv(left)
            if not data:
                break
            n = len(data)
            if n == size and not buf_len:
                # Shortcut.  Avoid buffer data copies when:
                # - We have no data in our buffer.
                # AND
                # - Our call to recv returned exactly the
                #   number of bytes we were asked to read.
                return data
            if n == left:
                buf.write(data)
                del data  # explicit free
                break
            assert n <= left, "_recv(%d) returned %d bytes" % (left, n)
            buf.write(data)
            buf_len += n
            del data  # explicit free
            #assert buf_len == buf.tell()
        buf.seek(start)
        return buf.read()

    def recv(self, size):
        assert size > 0

        buf = self._rbuf
        start = buf.tell()
        buf.seek(0, SEEK_END)
        buf_len = buf.tell()
        buf.seek(start)

        left = buf_len - start
        if not left:
            # only read from the wire if our read buffer is exhausted
            data = self._recv(self._rbufsize)
            if len(data) == size:
                # shortcut: skip the buffer if we read exactly size bytes
                return data
            buf = BytesIO()
            buf.write(data)
            buf.seek(0)
            del data  # explicit free
            self._rbuf = buf
        return buf.read(size)


def extract_capabilities(text):
    """Extract a capabilities list from a string, if present.

    :param text: String to extract from
    :return: Tuple with text with capabilities removed and list of capabilities
    """
    if not "\0" in text:
        return text, []
    text, capabilities = text.rstrip().split("\0")
    return (text, capabilities.strip().split(" "))


def extract_want_line_capabilities(text):
    """Extract a capabilities list from a want line, if present.

    Note that want lines have capabilities separated from the rest of the line
    by a space instead of a null byte. Thus want lines have the form:

        want obj-id cap1 cap2 ...

    :param text: Want line to extract from
    :return: Tuple with text with capabilities removed and list of capabilities
    """
    split_text = text.rstrip().split(" ")
    if len(split_text) < 3:
        return text, []
    return (" ".join(split_text[:2]), split_text[2:])


def ack_type(capabilities):
    """Extract the ack type from a capabilities list."""
    if 'multi_ack_detailed' in capabilities:
        return MULTI_ACK_DETAILED
    elif 'multi_ack' in capabilities:
        return MULTI_ACK
    return SINGLE_ACK


class BufferedPktLineWriter(object):
    """Writer that wraps its data in pkt-lines and has an independent buffer.

    Consecutive calls to write() wrap the data in a pkt-line and then buffers it
    until enough lines have been written such that their total length (including
    length prefix) reach the buffer size.
    """

    def __init__(self, write, bufsize=65515):
        """Initialize the BufferedPktLineWriter.

        :param write: A write callback for the underlying writer.
        :param bufsize: The internal buffer size, including length prefixes.
        """
        self._write = write
        self._bufsize = bufsize
        self._wbuf = BytesIO()
        self._buflen = 0

    def write(self, data):
        """Write data, wrapping it in a pkt-line."""
        line = pkt_line(data)
        line_len = len(line)
        over = self._buflen + line_len - self._bufsize
        if over >= 0:
            start = line_len - over
            self._wbuf.write(line[:start])
            self.flush()
        else:
            start = 0
        saved = line[start:]
        self._wbuf.write(saved)
        self._buflen += len(saved)

    def flush(self):
        """Flush all data from the buffer."""
        data = self._wbuf.getvalue()
        if data:
            self._write(data)
        self._len = 0
        self._wbuf = BytesIO()


class PktLineParser(object):
    """Packet line parser that hands completed packets off to a callback.
    """

    def __init__(self, handle_pkt):
        self.handle_pkt = handle_pkt
        self._readahead = BytesIO()

    def parse(self, data):
        """Parse a fragment of data and call back for any completed packets.
        """
        self._readahead.write(data)
        buf = self._readahead.getvalue()
        if len(buf) < 4:
            return
        while len(buf) >= 4:
            size = int(buf[:4], 16)
            if size == 0:
                self.handle_pkt(None)
                buf = buf[4:]
            elif size <= len(buf):
                self.handle_pkt(buf[4:size])
                buf = buf[size:]
            else:
                break
        self._readahead = BytesIO()
        self._readahead.write(buf)

    def get_tail(self):
        """Read back any unused data."""
        return self._readahead.getvalue()
