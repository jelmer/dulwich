# line_ending.py -- Line ending conversion functions
# Copyright (C) 2018-2018 Boris Feld <boris.feld@comet.ml>
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

""" All line-ending related functions, from conversions to config processing
"""

CRLF = b"\r\n"
LF = b"\n"


def convert_crlf_to_lf(text_hunk):
    """Convert CRLF in text hunk into LF

    :param text_hunk: A bytes string representing a text hunk
    :return: The text hunk with the same type, with CRLF replaced into LF
    """
    return text_hunk.replace(CRLF, LF)


def convert_lf_to_crlf(text_hunk):
    """Convert LF in text hunk into CRLF

    :param text_hunk: A bytes string representing a text hunk
    :return: The text hunk with the same type, with LF replaced into CRLF
    """
    # TODO find a more efficient way of doing it
    intermediary = text_hunk.replace(CRLF, LF)
    return intermediary.replace(LF, CRLF)
