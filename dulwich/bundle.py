# bundle.py -- Bundle format support
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
#
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# You should have received a copy of the licenses; if not, see
# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
# License, Version 2.0.
#

"""Bundle format support.
"""

from .pack import PackData
from typing import Dict, List, Tuple


class Bundle(object):

    version: int

    capabilities: Dict[str, str]
    prerequisites: List[Tuple[bytes, str]]
    references: Dict[str, bytes]
    pack_data: PackData


def _read_bundle(f, version):
    capabilities = {}
    prerequisites = []
    references = {}
    line = f.readline()
    if version >= 3:
        while line.startswith(b'@'):
            line = line[1:].rstrip(b'\n')
            try:
                key, value = line.split(b'=', 1)
            except IndexError:
                key = line
                value = None
            capabilities[key] = value
            line = f.readline()
    while line.startswith(b'-'):
        (obj_id, comment) = line[1:].split(b' ', 1)
        prerequisites.append((obj_id, comment.decode('utf-8')))
        line = f.readline()
    while line != b'\n':
        (obj_id, ref) = line.rstrip(b'\n').split(b' ', 1)
        references[ref] = obj_id
        line = f.readline()
    pack_data = PackData.from_file(f)
    ret = Bundle()
    ret.references = references
    ret.capabilities = capabilities
    ret.prerequisites = prerequisites
    ret.pack_data = pack_data
    return ret


def read_bundle(f):
    """Read a bundle file."""
    firstline = f.readline()
    if firstline == b'# v2 git bundle\n':
        return _read_bundle(f, 2)
    if firstline == b'# v3 git bundle\n':
        return _read_bundle(f, 3)
    raise AssertionError(
        'unsupported bundle format header: %r' % firstline)
