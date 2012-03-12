# gzip.py -- Implementation of gzip decoder, using the consumer pattern.
# GzipConsumer Copyright (C) 1995-2010 by Fredrik Lundh
#
# By obtaining, using, and/or copying this software and/or its associated
# documentation, you agree that you have read, understood, and will comply with
# the following terms and conditions:
#
# Permission to use, copy, modify, and distribute this software and its
# associated documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appears in all copies, and that both
# that copyright notice and this permission notice appear in supporting
# documentation, and that the name of Secret Labs AB or the author not be used in
# advertising or publicity pertaining to distribution of the software without
# specific, written prior permission.
#
# SECRET LABS AB AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS
# SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN
# NO EVENT SHALL SECRET LABS AB OR THE AUTHOR BE LIABLE FOR ANY SPECIAL, INDIRECT
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

"""Implementation of gzip decoder, using the consumer pattern."""

from cStringIO import StringIO

class StringConsumer(object):

    def __init__(self):
        self._data = StringIO()

    def feed(self, data):
        self._data.write(data)

    def close(self):
        # We don't want to close the underlying StringIO instance
        return self._data

# The below courtesy of Fredrik Lundh
# http://effbot.org/zone/consumer-gzip.htm
class GzipConsumer(object):
    """Consumer class to provide gzip decoding on the fly.
    The consumer acts like a filter, passing decoded data on to another
    consumer object.
    """
    def __init__(self, consumer=None):
        if consumer is None:
            consumer = StringConsumer()
        self._consumer = consumer
        self._decoder = None
        self._data = ''

    def feed(self, data):
        if self._decoder is None:
            # check if we have a full gzip header
            data = self._data + data
            try:
                i = 10
                flag = ord(data[3])
                if flag & 4: # extra
                    x = ord(data[i]) + 256*ord(data[i+1])
                    i = i + 2 + x
                if flag & 8: # filename
                    while ord(data[i]):
                        i = i + 1
                    i = i + 1
                if flag & 16: # comment
                    while ord(data[i]):
                        i = i + 1
                    i = i + 1
                if flag & 2: # crc
                    i = i + 2
                if len(data) < i:
                    raise IndexError('not enough data')
                if data[:3] != '\x1f\x8b\x08':
                    raise IOError('invalid gzip data')
                data = data[i:]
            except IndexError:
                self.__data = data
                return # need more data
            import zlib
            self._data = ''
            self._decoder = zlib.decompressobj(-zlib.MAX_WBITS)
        data = self._decoder.decompress(data)
        if data:
            self._consumer.feed(data)

    def close(self):
        if self._decoder:
            data = self._decoder.flush()
            if data:
                self._consumer.feed(data)
        return self._consumer.close()

