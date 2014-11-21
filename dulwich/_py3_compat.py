# _py3_compat.py -- for dealing with python3 compatibility
# Copyright (C) 2012-2014 Jelmer Vernooij and others.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version of the License.
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


import sys
import operator

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    text_type = unicode
    #string_types = (str, unicode)
    #unichr = unichr
    integer_types = (int, long)
else:
    text_type = str
    #string_types = (str,)
    #unichr = chr
    integer_types = (int, )


if PY2:
    int2byte = chr
    def byte2int(bs):
        return ord(bs[0])
    def indexbytes(buf, i):
        return ord(buf[i])
    def iterbytes(buf):
        return (ord(byte) for byte in buf)
else:
    int2byte = operator.methodcaller("to_bytes", 1, "big")
    byte2int = operator.itemgetter(0)
    indexbytes = operator.getitem
    iterbytes = iter


if PY2:
    keys = lambda d: d.iterkeys()
    values = lambda d: d.itervalues()
    items = lambda d: d.iteritems()
else:
    keys = lambda d: d.keys()
    values = lambda d: d.values()
    items = lambda d: d.items()

