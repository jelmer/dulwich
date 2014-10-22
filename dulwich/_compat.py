# _compat.py -- for dealing with python3 compatibility
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
PY2 = sys.version_info[0] == 2

#if not PY2:
#    text_type = str
#    string_types = (str,)
#    unichr = chr
#else:
#    text_type = unicode
#    string_types = (str, unicode)
#    unichr = unichr

if PY2:
    iterkeys = lambda d: d.iterkeys()
    itervalues = lambda d: d.itervalues()
    iteritems = lambda d: d.iteritems()
else:
    iterkeys = lambda d: d.keys()
    itervalues = lambda d: d.values()
    iteritems = lambda d: d.items()

if PY2:
    from itertools import izip, imap
else:
    izip = zip
    imap = map
