# wildmatch.py -- Bracket expressions from Git's wildmatch() pattern language
# Copyright (C) 2026 Vincent Gao <gaobing1230@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
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

r"""Bracket expressions from Git's wildmatch() pattern language.

Git matches ``.gitignore`` and ``.gitattributes`` patterns with the same
``wildmatch()`` (``wildmatch.c``), so the bracket grammar is shared between
:mod:`dulwich.ignore` and :mod:`dulwich.attrs`. It differs from
:mod:`fnmatch`, whose semantics neither file follows:

* ``^`` negates a class exactly like ``!`` (``NEGATE_CLASS2``).
* ``[:alpha:]`` and the eleven other POSIX classes are supported.
* A backslash escapes the following member, so ``[a\-c]`` is ``a``, ``-``, ``c``
  rather than the range ``\`` to ``c``.
* A bracket expression never matches ``/``, not even a negated one.
* A malformed bracket expression makes the whole pattern match nothing.
"""

__all__ = ["NO_MATCH", "MalformedPattern", "translate_bracket_expression"]

import re

# Regex that never matches, for patterns wildmatch() rejects outright.
NO_MATCH = b"(?!.*)"

_SLASH = 0x2F

# Git's character classes come from sane-ctype.h and are ASCII-only; the
# sane_ctype[] table has no entries in the 128.. range. '/' is left out of
# every class because a bracket expression can never match it.
_POSIX_CLASSES = {
    b"alnum": rb"0-9A-Za-z",
    b"alpha": rb"A-Za-z",
    b"blank": b"\\t ",
    b"cntrl": b"\\x00-\\x1f\\x7f",
    b"digit": rb"0-9",
    b"graph": rb"!-.0-~",
    b"lower": rb"a-z",
    b"print": rb" -.0-~",
    b"punct": rb"!-.:-@\[-`{-~",
    b"space": b"\\t\\n\\r ",
    b"upper": rb"A-Z",
    b"xdigit": rb"0-9A-Fa-f",
}


class MalformedPattern(Exception):
    """A pattern Git's wildmatch() gives up on (WM_ABORT_ALL).

    Such a pattern matches nothing at all, not even literally.
    """


def _render(members: list[tuple[int, int]]) -> bytes:
    """Render member ranges, dropping '/' and ranges that cannot match."""
    out = []
    for low, high in members:
        for a, b in ((low, min(high, _SLASH - 1)), (max(low, _SLASH + 1), high)):
            if a > b:
                continue
            piece = re.escape(bytes([a]))
            if a != b:
                piece += b"-" + re.escape(bytes([b]))
            out.append(piece)
    return b"".join(out)


def _posix_class(
    pattern: bytes, i: int, members: list[tuple[int, int]], classes: list[bytes]
) -> tuple[int, int | None]:
    """Consume a ``[:name:]`` starting at ``pattern[i]``."""
    start = i + 2
    j = start
    while j < len(pattern) and pattern[j : j + 1] != b"]":
        j += 1
    if j >= len(pattern):
        raise MalformedPattern(pattern)
    if j == start or pattern[j - 1 : j] != b":":
        # No closing ":]", so wildmatch() backs up and takes '[' as a member.
        members.append((0x5B, 0x5B))
        return start - 2, 0x5B
    try:
        classes.append(_POSIX_CLASSES[pattern[start : j - 1]])
    except KeyError:
        raise MalformedPattern(pattern) from None
    return j, None


def translate_bracket_expression(pattern: bytes, i: int) -> tuple[int, bytes]:
    """Translate the bracket expression opened by ``pattern[i - 1]``.

    Args:
      pattern: Pattern being translated
      i: Index just past the opening ``[``
    Returns:
      Tuple of the index just past the closing ``]`` and the regex fragment
    Raises:
      MalformedPattern: if wildmatch() would refuse the pattern outright
    """
    n = len(pattern)
    negated = pattern[i : i + 1] in (b"!", b"^")
    if negated:
        i += 1
    members: list[tuple[int, int]] = []
    classes: list[bytes] = []
    prev: int | None = None
    first = True
    while True:
        if i >= n:
            raise MalformedPattern(pattern)
        c = pattern[i : i + 1]
        if c == b"]" and not first:
            break
        first = False
        if c == b"\\":
            i += 1
            if i >= n:
                raise MalformedPattern(pattern)
            prev = pattern[i]
            members.append((prev, prev))
        elif (
            c == b"-"
            and prev is not None
            and i + 1 < n
            and pattern[i + 1 : i + 2] != b"]"
        ):
            i += 1
            if pattern[i : i + 1] == b"\\":
                i += 1
                if i >= n:
                    raise MalformedPattern(pattern)
            if prev <= pattern[i]:
                members[-1] = (prev, pattern[i])
            # An inverted range matches nothing, but wildmatch() has already
            # taken the low end as a plain member by the time it sees the '-',
            # so "[z-a]" still matches "z"; leave that member in place.
            prev = None
        elif c == b"[" and pattern[i + 1 : i + 2] == b":":
            i, prev = _posix_class(pattern, i, members, classes)
        else:
            prev = pattern[i]
            members.append((prev, prev))
        i += 1
    body = _render(members) + b"".join(classes)
    if negated:
        return i + 1, b"[^/" + body + b"]"
    if not body:
        # Well-formed but unmatchable, e.g. [z-a] or [/].
        return i + 1, b"(?!)"
    return i + 1, b"[" + body + b"]"
