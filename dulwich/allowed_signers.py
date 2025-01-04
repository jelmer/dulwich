# Copyright (C) 2024 E. Castedo Ellerman <castedo@castedo.com>
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

"""Parsing of the ssh-keygen allowed signers format."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

if TYPE_CHECKING:
    AllowedSignerOptions = dict[str, str]


@dataclass
class AllowedSigner:
    principals: str
    options: AllowedSignerOptions | None
    key_type: str
    base64_key: str
    comment: str | None = None  # "patterned after" sshd authorized keys file format

    @staticmethod
    def parse(line: str) -> AllowedSigner:
        """Parse a line of an ssh-keygen "allowed signers" file.

        Raises:
            ValueError: If the line is not properly formatted.
            NotImplementedError: If the public key algorithm is not supported.
        """
        (principals, line) = lop_principals(line)
        options = None
        if detect_options(line):
            (options, line) = lop_options(line)
        parts = line.split(maxsplit=2)
        if len(parts) < 2:
            msg = "Not space-separated OpenSSH format public key ('{}')."
            raise ValueError(msg.format(line))
        return AllowedSigner(principals, options, *parts)


def lop_principals(line: str) -> tuple[str, str]:
    """Return (principals, rest_of_line)."""
    if line[0] == '"':
        (principals, _, line) = line[1:].partition('"')
        if not line:
            msg = "No matching double quote character for line ('{}')."
            raise ValueError(msg.format(line))
        return (principals, line.lstrip())
    parts = line.split(maxsplit=1)
    if len(parts) < 2:
        raise ValueError(f"Invalid line ('{line}').")
    return (parts[0], parts[1])


def detect_options(line: str) -> bool:
    start = line.split(maxsplit=1)[0]
    return "=" in start or "," in start or start.lower() == "cert-authority"


def lop_options(line: str) -> tuple[AllowedSignerOptions, str]:
    """Return (options, rest_of_line).

    Raises:
        ValueError
    """
    options: AllowedSignerOptions = dict()
    while line and not line[0].isspace():
        line = lop_one_option(options, line)
    return (options, line)


def lop_one_option(options: AllowedSignerOptions, line: str) -> str:
    if lopped := lop_flag(options, line, "cert-authority"):
        return lopped
    if lopped := lop_option(options, line, "namespaces"):
        return lopped
    if lopped := lop_option(options, line, "valid-after"):
        return lopped
    if lopped := lop_option(options, line, "valid-before"):
        return lopped
    raise ValueError(f"Invalid option ('{line}').")


def lop_flag(options: AllowedSignerOptions, line: str, opt_name: str) -> str | None:
    i = len(opt_name)
    if line[:i].lower() != opt_name:
        return None
    options[opt_name] = ""
    if line[i : i + 1] == ",":
        i += 1
    return line[i:]


def lop_option(options: AllowedSignerOptions, line: str, opt_name: str) -> str | None:
    i = len(opt_name)
    if line[:i].lower() != opt_name:
        return None
    if opt_name in options:
        raise ValueError(f"Multiple '{opt_name}' clauses ('{line}')")
    if line[i : i + 2] != '="':
        raise ValueError(f"Option '{opt_name}' missing '=\"' ('{line}')")
    (value, _, line) = line[i + 2 :].partition('"')
    if not line:
        raise ValueError(f"No matching quote for option '{opt_name}' ('{line}')")
    options[opt_name] = value
    return line[1:] if line[0] == "," else line


def load_allowed_signers_file(file: TextIO | Path) -> Iterable[AllowedSigner]:
    """Read public keys in "allowed signers" format per ssh-keygen.

    Raises:
        ValueError: If the file is not properly formatted.
    """
    # The intention of this implementation is to reproduce the behaviour of the
    # parse_principals_key_and_options function of the following sshsig.c file:
    # https://archive.softwareheritage.org/
    # swh:1:cnt:470b286a3a982875a48a5262b7057c4710b17fed

    if isinstance(file, Path):
        with open(file, encoding="ascii") as f:
            return load_allowed_signers_file(f)
    ret = list()
    for line in file.readlines():
        if "\f" in line:
            raise ValueError(f"Form feed character not supported: ('{line}').")
        if "\v" in line:
            raise ValueError(f"Vertical tab character not supported: ('{line}').")
        line = line.strip("\n\r")
        if line and line[0] not in ["#", "\0"]:
            ret.append(AllowedSigner.parse(line))
    return ret
