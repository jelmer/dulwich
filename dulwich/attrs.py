# attrs.py -- Git attributes for dulwich
# Copyright (C) 2019-2020 Collabora Ltd
# Copyright (C) 2019-2020 Andrej Shadura <andrew.shadura@collabora.co.uk>
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

"""Parse .gitattributes file.
"""

import sys
from typing import (
    Generator,
    IO,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)


AttributeValue = Union[str, bool, None]


def _parse_attr(attr: str) -> Tuple[str, AttributeValue]:
    """Parse a git attribute into its value:

    >>> _parse_attr('attr')
    ('attr', True)
    >>> _parse_attr('-attr')
    ('attr', False)
    >>> _parse_attr('!attr')
    ('attr', None)
    >>> _parse_attr('attr=text')
    ('attr', 'text')
    """
    if attr.startswith('!'):
        return attr[1:], None
    if attr.startswith('-'):
        return attr[1:], False
    if '=' not in attr:
        return attr, True
    return tuple(attr.split('='))


def parse_git_attributes(f: IO[bytes]) -> Generator[Tuple[bytes, Mapping[str, AttributeValue]], None, None]:
    """Parse a Git attributes string

    Args:
      f: File-like object to read bytes from
    Returns:
      List of patterns and corresponding patterns in the order or them being encountered
    >>> from io import BytesIO
    >>> list(parse_git_attributes(BytesIO(b'''*.tar.* filter=lfs diff=lfs merge=lfs -text
    ... 
    ... # store signatures in Git
    ... *.tar.*.asc -filter -diff merge=binary -text
    ... 
    ... # store .dsc verbatim
    ... *.dsc -filter !diff merge=binary !text
    ... '''))) #doctest: +NORMALIZE_WHITESPACE
    [(b'*.tar.*', {'filter': 'lfs', 'diff': 'lfs', 'merge': 'lfs', 'text': False}),
     (b'*.tar.*.asc', {'filter': False, 'diff': False, 'merge': 'binary', 'text': False}),
     (b'*.dsc', {'filter': False, 'diff': None, 'merge': 'binary', 'text': None})]
    """
    for line in f:
        line = line.strip()

        # Ignore blank lines, they're used for readability.
        if not line:
            continue

        if line.startswith(b'#'):
            # Comment
            continue

        pattern, *attrs = line.split()

        yield (pattern, {k: v for k, v in (_parse_attr(a.decode()) for a in attrs)})
