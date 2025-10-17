# mbox.py -- For dealing with mbox files
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Classes for dealing with mbox files and Maildir.

This module provides functionality to split mbox files and Maildir
into individual message files, similar to git mailsplit.
"""

import mailbox
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import BinaryIO, Union


def split_mbox(
    input_file: Union[str, bytes, BinaryIO],
    output_dir: Union[str, bytes, Path],
    start_number: int = 1,
    precision: int = 4,
    keep_cr: bool = False,
    mboxrd: bool = False,
) -> list[str]:
    r"""Split an mbox file into individual message files.

    Args:
        input_file: Path to mbox file or file-like object. If None, reads from stdin.
        output_dir: Directory where individual messages will be written
        start_number: Starting number for output files (default: 1)
        precision: Number of digits for output filenames (default: 4)
        keep_cr: If True, preserve \r in lines ending with \r\n (default: False)
        mboxrd: If True, treat input as mboxrd format and reverse escaping (default: False)

    Returns:
        List of output file paths that were created

    Raises:
        ValueError: If output_dir doesn't exist or isn't a directory
        OSError: If there are issues reading/writing files
    """
    # Convert output_dir to Path for easier manipulation
    if isinstance(output_dir, bytes):
        output_dir = output_dir.decode("utf-8")
    output_path = Path(output_dir)

    if not output_path.exists():
        raise ValueError(f"Output directory does not exist: {output_dir}")
    if not output_path.is_dir():
        raise ValueError(f"Output path is not a directory: {output_dir}")

    # Open the mbox file
    mbox_iter: Iterable[mailbox.mboxMessage]
    if isinstance(input_file, (str, bytes)):
        if isinstance(input_file, bytes):
            input_file = input_file.decode("utf-8")
        mbox_iter = mailbox.mbox(input_file)
    else:
        # For file-like objects, we need to read and parse manually
        mbox_iter = _parse_mbox_from_file(input_file)

    output_files = []
    msg_number = start_number

    for message in mbox_iter:
        # Format the output filename with the specified precision
        output_filename = f"{msg_number:0{precision}d}"
        output_file_path = output_path / output_filename

        # Write the message to the output file
        with open(output_file_path, "wb") as f:
            message_bytes = bytes(message)

            # Handle mboxrd format - reverse the escaping
            if mboxrd:
                message_bytes = _reverse_mboxrd_escaping(message_bytes)

            # Handle CR/LF if needed
            if not keep_cr:
                message_bytes = message_bytes.replace(b"\r\n", b"\n")

            # Strip trailing newlines (mailbox module adds separator newlines)
            message_bytes = message_bytes.rstrip(b"\n")
            if message_bytes:
                message_bytes += b"\n"

            f.write(message_bytes)

        output_files.append(str(output_file_path))
        msg_number += 1

    return output_files


def split_maildir(
    maildir_path: Union[str, bytes, Path],
    output_dir: Union[str, bytes, Path],
    start_number: int = 1,
    precision: int = 4,
    keep_cr: bool = False,
) -> list[str]:
    r"""Split a Maildir into individual message files.

    Maildir splitting relies upon filenames being sorted to output
    patches in the correct order.

    Args:
        maildir_path: Path to the Maildir directory (should contain cur, tmp, new subdirectories)
        output_dir: Directory where individual messages will be written
        start_number: Starting number for output files (default: 1)
        precision: Number of digits for output filenames (default: 4)
        keep_cr: If True, preserve \r in lines ending with \r\n (default: False)

    Returns:
        List of output file paths that were created

    Raises:
        ValueError: If maildir_path or output_dir don't exist or aren't valid
        OSError: If there are issues reading/writing files
    """
    # Convert paths to Path objects
    if isinstance(maildir_path, bytes):
        maildir_path = maildir_path.decode("utf-8")
    if isinstance(output_dir, bytes):
        output_dir = output_dir.decode("utf-8")

    maildir = Path(maildir_path)
    output_path = Path(output_dir)

    if not maildir.exists():
        raise ValueError(f"Maildir does not exist: {maildir_path}")
    if not maildir.is_dir():
        raise ValueError(f"Maildir path is not a directory: {maildir_path}")
    if not output_path.exists():
        raise ValueError(f"Output directory does not exist: {output_dir}")
    if not output_path.is_dir():
        raise ValueError(f"Output path is not a directory: {output_dir}")

    # Open the Maildir
    md = mailbox.Maildir(str(maildir), factory=None)

    # Get all messages and sort by their keys to ensure consistent ordering
    sorted_keys = sorted(md.keys())

    output_files = []
    msg_number = start_number

    for key in sorted_keys:
        message = md[key]

        # Format the output filename with the specified precision
        output_filename = f"{msg_number:0{precision}d}"
        output_file_path = output_path / output_filename

        # Write the message to the output file
        with open(output_file_path, "wb") as f:
            message_bytes = bytes(message)

            # Handle CR/LF if needed
            if not keep_cr:
                message_bytes = message_bytes.replace(b"\r\n", b"\n")

            f.write(message_bytes)

        output_files.append(str(output_file_path))
        msg_number += 1

    return output_files


def _parse_mbox_from_file(file_obj: BinaryIO) -> Iterator[mailbox.mboxMessage]:
    """Parse mbox format from a file-like object.

    Args:
        file_obj: Binary file-like object containing mbox data

    Yields:
        Individual mboxMessage objects
    """
    import tempfile

    # Create a temporary file to hold the mbox data
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp:
        tmp.write(file_obj.read())
        tmp_path = tmp.name

    mbox = mailbox.mbox(tmp_path)
    try:
        yield from mbox
    finally:
        mbox.close()
        os.unlink(tmp_path)


def _reverse_mboxrd_escaping(message_bytes: bytes) -> bytes:
    """Reverse mboxrd escaping (^>+From lines).

    In mboxrd format, lines matching ^>+From have one leading ">" removed.

    Args:
        message_bytes: Message content with mboxrd escaping

    Returns:
        Message content with escaping reversed
    """
    lines = message_bytes.split(b"\n")
    result_lines = []

    for line in lines:
        # Check if line matches the pattern ^>+From (one or more > followed by From)
        if line.startswith(b">") and line.lstrip(b">").startswith(b"From "):
            # Remove one leading ">"
            result_lines.append(line[1:])
        else:
            result_lines.append(line)

    return b"\n".join(result_lines)
