# mailmap.py -- Mailmap reader
# Copyright (C) 2018 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

"""Mailmap file reader."""

from collections.abc import Iterator
from typing import IO, Optional, Union


def parse_identity(text: bytes) -> tuple[Optional[bytes], Optional[bytes]]:
    # TODO(jelmer): Integrate this with dulwich.fastexport.split_email and
    # dulwich.repo.check_user_identity
    (name_str, email_str) = text.rsplit(b"<", 1)
    name_str = name_str.strip()
    email_str = email_str.rstrip(b">").strip()
    name: Optional[bytes] = name_str if name_str else None
    email: Optional[bytes] = email_str if email_str else None
    return (name, email)


def read_mailmap(
    f: IO[bytes],
) -> Iterator[
    tuple[
        tuple[Optional[bytes], Optional[bytes]],
        Optional[tuple[Optional[bytes], Optional[bytes]]],
    ]
]:
    """Read a mailmap.

    Args:
      f: File-like object to read from
    Returns: Iterator over
        ((canonical_name, canonical_email), (from_name, from_email)) tuples
    """
    for line in f:
        # Remove comments
        line = line.split(b"#")[0]
        line = line.strip()
        if not line:
            continue
        (canonical_identity, from_identity) = line.split(b">", 1)
        canonical_identity += b">"
        if from_identity.strip():
            parsed_from_identity = parse_identity(from_identity)
        else:
            parsed_from_identity = None
        parsed_canonical_identity = parse_identity(canonical_identity)
        yield parsed_canonical_identity, parsed_from_identity


class Mailmap:
    """Class for accessing a mailmap file."""

    def __init__(
        self,
        map: Optional[
            Iterator[
                tuple[
                    tuple[Optional[bytes], Optional[bytes]],
                    Optional[tuple[Optional[bytes], Optional[bytes]]],
                ]
            ]
        ] = None,
    ) -> None:
        self._table: dict[
            tuple[Optional[bytes], Optional[bytes]],
            tuple[Optional[bytes], Optional[bytes]],
        ] = {}
        if map:
            for canonical_identity, from_identity in map:
                self.add_entry(canonical_identity, from_identity)

    def add_entry(
        self,
        canonical_identity: tuple[Optional[bytes], Optional[bytes]],
        from_identity: Optional[tuple[Optional[bytes], Optional[bytes]]] = None,
    ) -> None:
        """Add an entry to the mail mail.

        Any of the fields can be None, but at least one of them needs to be
        set.

        Args:
          canonical_identity: The canonical identity (tuple)
          from_identity: The from identity (tuple)
        """
        if from_identity is None:
            from_name, from_email = None, None
        else:
            (from_name, from_email) = from_identity
        (canonical_name, canonical_email) = canonical_identity
        if from_name is None and from_email is None:
            self._table[canonical_name, None] = canonical_identity
            self._table[None, canonical_email] = canonical_identity
        else:
            self._table[from_name, from_email] = canonical_identity

    def lookup(
        self, identity: Union[bytes, tuple[Optional[bytes], Optional[bytes]]]
    ) -> Union[bytes, tuple[Optional[bytes], Optional[bytes]]]:
        """Lookup an identity in this mailmail."""
        if not isinstance(identity, tuple):
            was_tuple = False
            identity = parse_identity(identity)
        else:
            was_tuple = True
        for query in [identity, (None, identity[1]), (identity[0], None)]:
            canonical_identity = self._table.get(query)
            if canonical_identity is not None:
                identity = (
                    canonical_identity[0] or identity[0],
                    canonical_identity[1] or identity[1],
                )
                break
        if was_tuple:
            return identity
        else:
            name, email = identity
            if name is None:
                name = b""
            if email is None:
                email = b""
            return name + b" <" + email + b">"

    @classmethod
    def from_path(cls, path: str) -> "Mailmap":
        with open(path, "rb") as f:
            return cls(read_mailmap(f))
