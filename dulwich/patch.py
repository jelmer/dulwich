# patch.py -- For dealing with packed-style patches.
# Copyright (C) 2009-2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Classes for dealing with git am-style patches.

These patches are basically unified diffs with some extra metadata tacked
on.
"""

__all__ = [
    "DEFAULT_DIFF_ALGORITHM",
    "FIRST_FEW_BYTES",
    "DiffAlgorithmNotAvailable",
    "MailinfoResult",
    "PatchApplicationFailure",
    "apply_patch_hunks",
    "apply_patches",
    "commit_patch_id",
    "gen_diff_header",
    "get_summary",
    "git_am_patch_split",
    "is_binary",
    "mailinfo",
    "parse_patch_message",
    "patch_filename",
    "patch_id",
    "shortid",
    "unified_diff",
    "unified_diff_with_algorithm",
    "write_blob_diff",
    "write_commit_patch",
    "write_object_diff",
    "write_tree_diff",
]

import email.message
import email.parser
import email.utils
import os
import re
import time
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import (
    IO,
    TYPE_CHECKING,
    BinaryIO,
    TextIO,
)

if TYPE_CHECKING:
    from .object_store import BaseObjectStore
    from .repo import Repo

from .objects import S_ISGITLINK, Blob, Commit, ObjectID, RawObjectID

FIRST_FEW_BYTES = 8000

DEFAULT_DIFF_ALGORITHM = "myers"


class PatchApplicationFailure(Exception):
    """Raised when a patch does not apply cleanly."""


class DiffAlgorithmNotAvailable(Exception):
    """Raised when a requested diff algorithm is not available."""

    def __init__(self, algorithm: str, install_hint: str = "") -> None:
        """Initialize exception.

        Args:
            algorithm: Name of the unavailable algorithm
            install_hint: Optional installation hint
        """
        self.algorithm = algorithm
        self.install_hint = install_hint
        if install_hint:
            super().__init__(
                f"Diff algorithm '{algorithm}' requested but not available. {install_hint}"
            )
        else:
            super().__init__(
                f"Diff algorithm '{algorithm}' requested but not available."
            )


def write_commit_patch(
    f: IO[bytes],
    commit: "Commit",
    contents: str | bytes,
    progress: tuple[int, int],
    version: str | None = None,
    encoding: str | None = None,
) -> None:
    """Write a individual file patch.

    Args:
      f: File-like object to write to
      commit: Commit object
      contents: Contents of the patch
      progress: tuple with current patch number and total.
      version: Version string to include in patch header
      encoding: Encoding to use for the patch

    Returns:
      tuple with filename and contents
    """
    encoding = encoding or getattr(f, "encoding", "ascii")
    if encoding is None:
        encoding = "ascii"
    if isinstance(contents, str):
        contents = contents.encode(encoding)
    (num, total) = progress
    f.write(
        b"From "
        + commit.id
        + b" "
        + time.ctime(commit.commit_time).encode(encoding)
        + b"\n"
    )
    f.write(b"From: " + commit.author + b"\n")
    f.write(
        b"Date: " + time.strftime("%a, %d %b %Y %H:%M:%S %Z").encode(encoding) + b"\n"
    )
    f.write(
        (f"Subject: [PATCH {num}/{total}] ").encode(encoding) + commit.message + b"\n"
    )
    f.write(b"\n")
    f.write(b"---\n")
    try:
        import subprocess

        p = subprocess.Popen(
            ["diffstat"], stdout=subprocess.PIPE, stdin=subprocess.PIPE
        )
    except (ImportError, OSError):
        pass  # diffstat not available?
    else:
        (diffstat, _) = p.communicate(contents)
        f.write(diffstat)
        f.write(b"\n")
    f.write(contents)
    f.write(b"-- \n")
    if version is None:
        from dulwich import __version__ as dulwich_version

        f.write(b"Dulwich %d.%d.%d\n" % dulwich_version)
    else:
        if encoding is None:
            encoding = "ascii"
        f.write(version.encode(encoding) + b"\n")


def get_summary(commit: "Commit") -> str:
    """Determine the summary line for use in a filename.

    Args:
      commit: Commit
    Returns: Summary string
    """
    decoded = commit.message.decode(errors="replace")
    lines = decoded.splitlines()
    return lines[0].replace(" ", "-") if lines else ""


#  Unified Diff
def _format_range_unified(start: int, stop: int) -> str:
    """Convert range to the "ed" format."""
    # Per the diff spec at http://www.unix.org/single_unix_specification/
    beginning = start + 1  # lines start numbering with one
    length = stop - start
    if length == 1:
        return f"{beginning}"
    if not length:
        beginning -= 1  # empty ranges begin at line just before the range
    return f"{beginning},{length}"


def unified_diff(
    a: Sequence[bytes],
    b: Sequence[bytes],
    fromfile: bytes = b"",
    tofile: bytes = b"",
    fromfiledate: str = "",
    tofiledate: str = "",
    n: int = 3,
    lineterm: str = "\n",
    tree_encoding: str = "utf-8",
    output_encoding: str = "utf-8",
) -> Generator[bytes, None, None]:
    """difflib.unified_diff that can detect "No newline at end of file" as original "git diff" does.

    Based on the same function in Python2.7 difflib.py
    """
    started = False
    for group in SequenceMatcher(a=a, b=b).get_grouped_opcodes(n):
        if not started:
            started = True
            fromdate = f"\t{fromfiledate}" if fromfiledate else ""
            todate = f"\t{tofiledate}" if tofiledate else ""
            yield f"--- {fromfile.decode(tree_encoding)}{fromdate}{lineterm}".encode(
                output_encoding
            )
            yield f"+++ {tofile.decode(tree_encoding)}{todate}{lineterm}".encode(
                output_encoding
            )

        first, last = group[0], group[-1]
        file1_range = _format_range_unified(first[1], last[2])
        file2_range = _format_range_unified(first[3], last[4])
        yield f"@@ -{file1_range} +{file2_range} @@{lineterm}".encode(output_encoding)

        for tag, i1, i2, j1, j2 in group:
            if tag == "equal":
                for line in a[i1:i2]:
                    yield b" " + line
                continue
            if tag in ("replace", "delete"):
                for line in a[i1:i2]:
                    if not line[-1:] == b"\n":
                        line += b"\n\\ No newline at end of file\n"
                    yield b"-" + line
            if tag in ("replace", "insert"):
                for line in b[j1:j2]:
                    if not line[-1:] == b"\n":
                        line += b"\n\\ No newline at end of file\n"
                    yield b"+" + line


def _get_sequence_matcher(
    algorithm: str, a: Sequence[bytes], b: Sequence[bytes]
) -> SequenceMatcher[bytes]:
    """Get appropriate sequence matcher for the given algorithm.

    Args:
        algorithm: Diff algorithm ("myers" or "patience")
        a: First sequence
        b: Second sequence

    Returns:
        Configured sequence matcher instance

    Raises:
        DiffAlgorithmNotAvailable: If patience requested but not available
    """
    if algorithm == "patience":
        try:
            from patiencediff import PatienceSequenceMatcher

            return PatienceSequenceMatcher(None, a, b)  # type: ignore[no-any-return,unused-ignore]
        except ImportError:
            raise DiffAlgorithmNotAvailable(
                "patience", "Install with: pip install 'dulwich[patiencediff]'"
            )
    else:
        return SequenceMatcher(a=a, b=b)


def unified_diff_with_algorithm(
    a: Sequence[bytes],
    b: Sequence[bytes],
    fromfile: bytes = b"",
    tofile: bytes = b"",
    fromfiledate: str = "",
    tofiledate: str = "",
    n: int = 3,
    lineterm: str = "\n",
    tree_encoding: str = "utf-8",
    output_encoding: str = "utf-8",
    algorithm: str | None = None,
) -> Generator[bytes, None, None]:
    """Generate unified diff with specified algorithm.

    Args:
        a: First sequence of lines
        b: Second sequence of lines
        fromfile: Name of first file
        tofile: Name of second file
        fromfiledate: Date of first file
        tofiledate: Date of second file
        n: Number of context lines
        lineterm: Line terminator
        tree_encoding: Encoding for tree paths
        output_encoding: Encoding for output
        algorithm: Diff algorithm to use ("myers" or "patience")

    Returns:
        Generator yielding diff lines

    Raises:
        DiffAlgorithmNotAvailable: If patience algorithm requested but patiencediff not available
    """
    if algorithm is None:
        algorithm = DEFAULT_DIFF_ALGORITHM

    matcher = _get_sequence_matcher(algorithm, a, b)

    started = False
    for group in matcher.get_grouped_opcodes(n):
        if not started:
            started = True
            fromdate = f"\t{fromfiledate}" if fromfiledate else ""
            todate = f"\t{tofiledate}" if tofiledate else ""
            yield f"--- {fromfile.decode(tree_encoding)}{fromdate}{lineterm}".encode(
                output_encoding
            )
            yield f"+++ {tofile.decode(tree_encoding)}{todate}{lineterm}".encode(
                output_encoding
            )

        first, last = group[0], group[-1]
        file1_range = _format_range_unified(first[1], last[2])
        file2_range = _format_range_unified(first[3], last[4])
        yield f"@@ -{file1_range} +{file2_range} @@{lineterm}".encode(output_encoding)

        for tag, i1, i2, j1, j2 in group:
            if tag == "equal":
                for line in a[i1:i2]:
                    yield b" " + line
                continue
            if tag in ("replace", "delete"):
                for line in a[i1:i2]:
                    if not line[-1:] == b"\n":
                        line += b"\n\\ No newline at end of file\n"
                    yield b"-" + line
            if tag in ("replace", "insert"):
                for line in b[j1:j2]:
                    if not line[-1:] == b"\n":
                        line += b"\n\\ No newline at end of file\n"
                    yield b"+" + line


def is_binary(content: bytes) -> bool:
    """See if the first few bytes contain any null characters.

    Args:
      content: Bytestring to check for binary content
    """
    return b"\0" in content[:FIRST_FEW_BYTES]


def shortid(hexsha: bytes | None) -> bytes:
    """Get short object ID.

    Args:
        hexsha: Full hex SHA or None

    Returns:
        7-character short ID
    """
    if hexsha is None:
        return b"0" * 7
    else:
        return hexsha[:7]


def patch_filename(p: bytes | None, root: bytes) -> bytes:
    """Generate patch filename.

    Args:
        p: Path or None
        root: Root directory

    Returns:
        Full patch filename
    """
    if p is None:
        return b"/dev/null"
    else:
        return root + b"/" + p


def write_object_diff(
    f: IO[bytes],
    store: "BaseObjectStore",
    old_file: tuple[bytes | None, int | None, ObjectID | None],
    new_file: tuple[bytes | None, int | None, ObjectID | None],
    diff_binary: bool = False,
    diff_algorithm: str | None = None,
) -> None:
    """Write the diff for an object.

    Args:
      f: File-like object to write to
      store: Store to retrieve objects from, if necessary
      old_file: (path, mode, hexsha) tuple
      new_file: (path, mode, hexsha) tuple
      diff_binary: Whether to diff files even if they
        are considered binary files by is_binary().
      diff_algorithm: Algorithm to use for diffing ("myers" or "patience")

    Note: the tuple elements should be None for nonexistent files
    """
    (old_path, old_mode, old_id) = old_file
    (new_path, new_mode, new_id) = new_file
    patched_old_path = patch_filename(old_path, b"a")
    patched_new_path = patch_filename(new_path, b"b")

    def content(mode: int | None, hexsha: ObjectID | None) -> Blob:
        """Get blob content for a file.

        Args:
            mode: File mode
            hexsha: Object SHA

        Returns:
            Blob object
        """
        if hexsha is None:
            return Blob.from_string(b"")
        elif mode is not None and S_ISGITLINK(mode):
            return Blob.from_string(b"Subproject commit " + hexsha + b"\n")
        else:
            obj = store[hexsha]
            if isinstance(obj, Blob):
                return obj
            else:
                # Fallback for non-blob objects
                return Blob.from_string(obj.as_raw_string())

    def lines(content: "Blob") -> list[bytes]:
        """Split blob content into lines.

        Args:
            content: Blob content

        Returns:
            List of lines
        """
        if not content:
            return []
        else:
            return content.splitlines()

    f.writelines(
        gen_diff_header((old_path, new_path), (old_mode, new_mode), (old_id, new_id))
    )
    old_content = content(old_mode, old_id)
    new_content = content(new_mode, new_id)
    if not diff_binary and (is_binary(old_content.data) or is_binary(new_content.data)):
        binary_diff = (
            b"Binary files "
            + patched_old_path
            + b" and "
            + patched_new_path
            + b" differ\n"
        )
        f.write(binary_diff)
    else:
        f.writelines(
            unified_diff_with_algorithm(
                lines(old_content),
                lines(new_content),
                patched_old_path,
                patched_new_path,
                algorithm=diff_algorithm,
            )
        )


# TODO(jelmer): Support writing unicode, rather than bytes.
def gen_diff_header(
    paths: tuple[bytes | None, bytes | None],
    modes: tuple[int | None, int | None],
    shas: tuple[bytes | None, bytes | None],
) -> Generator[bytes, None, None]:
    """Write a blob diff header.

    Args:
      paths: Tuple with old and new path
      modes: Tuple with old and new modes
      shas: Tuple with old and new shas
    """
    (old_path, new_path) = paths
    (old_mode, new_mode) = modes
    (old_sha, new_sha) = shas
    if old_path is None and new_path is not None:
        old_path = new_path
    if new_path is None and old_path is not None:
        new_path = old_path
    old_path = patch_filename(old_path, b"a")
    new_path = patch_filename(new_path, b"b")
    yield b"diff --git " + old_path + b" " + new_path + b"\n"

    if old_mode != new_mode:
        if new_mode is not None:
            if old_mode is not None:
                yield (f"old file mode {old_mode:o}\n").encode("ascii")
            yield (f"new file mode {new_mode:o}\n").encode("ascii")
        else:
            yield (f"deleted file mode {old_mode:o}\n").encode("ascii")
    yield b"index " + shortid(old_sha) + b".." + shortid(new_sha)
    if new_mode is not None and old_mode is not None:
        yield (f" {new_mode:o}").encode("ascii")
    yield b"\n"


# TODO(jelmer): Support writing unicode, rather than bytes.
def write_blob_diff(
    f: IO[bytes],
    old_file: tuple[bytes | None, int | None, "Blob | None"],
    new_file: tuple[bytes | None, int | None, "Blob | None"],
    diff_algorithm: str | None = None,
) -> None:
    """Write blob diff.

    Args:
      f: File-like object to write to
      old_file: (path, mode, hexsha) tuple (None if nonexisting)
      new_file: (path, mode, hexsha) tuple (None if nonexisting)
      diff_algorithm: Algorithm to use for diffing ("myers" or "patience")

    Note: The use of write_object_diff is recommended over this function.
    """
    (old_path, old_mode, old_blob) = old_file
    (new_path, new_mode, new_blob) = new_file
    patched_old_path = patch_filename(old_path, b"a")
    patched_new_path = patch_filename(new_path, b"b")

    def lines(blob: "Blob | None") -> list[bytes]:
        """Split blob content into lines.

        Args:
            blob: Blob object or None

        Returns:
            List of lines
        """
        if blob is not None:
            return blob.splitlines()
        else:
            return []

    f.writelines(
        gen_diff_header(
            (old_path, new_path),
            (old_mode, new_mode),
            (getattr(old_blob, "id", None), getattr(new_blob, "id", None)),
        )
    )
    old_contents = lines(old_blob)
    new_contents = lines(new_blob)
    f.writelines(
        unified_diff_with_algorithm(
            old_contents,
            new_contents,
            patched_old_path,
            patched_new_path,
            algorithm=diff_algorithm,
        )
    )


def write_tree_diff(
    f: IO[bytes],
    store: "BaseObjectStore",
    old_tree: ObjectID | None,
    new_tree: ObjectID | None,
    diff_binary: bool = False,
    diff_algorithm: str | None = None,
) -> None:
    """Write tree diff.

    Args:
      f: File-like object to write to.
      store: Object store to read from
      old_tree: Old tree id
      new_tree: New tree id
      diff_binary: Whether to diff files even if they
        are considered binary files by is_binary().
      diff_algorithm: Algorithm to use for diffing ("myers" or "patience")
    """
    changes = store.tree_changes(old_tree, new_tree)
    for (oldpath, newpath), (oldmode, newmode), (oldsha, newsha) in changes:
        write_object_diff(
            f,
            store,
            (oldpath, oldmode, oldsha),
            (newpath, newmode, newsha),
            diff_binary=diff_binary,
            diff_algorithm=diff_algorithm,
        )


def git_am_patch_split(
    f: TextIO | BinaryIO, encoding: str | None = None
) -> tuple["Commit", bytes, bytes | None]:
    """Parse a git-am-style patch and split it up into bits.

    Args:
      f: File-like object to parse
      encoding: Encoding to use when creating Git objects
    Returns: Tuple with commit object, diff contents and git version
    """
    encoding = encoding or getattr(f, "encoding", "ascii")
    encoding = encoding or "ascii"
    contents = f.read()
    if isinstance(contents, bytes):
        bparser = email.parser.BytesParser()
        msg = bparser.parsebytes(contents)
    else:
        uparser = email.parser.Parser()
        msg = uparser.parsestr(contents)
    return parse_patch_message(msg, encoding)


def parse_patch_message(
    msg: email.message.Message, encoding: str | None = None
) -> tuple["Commit", bytes, bytes | None]:
    """Extract a Commit object and patch from an e-mail message.

    Args:
      msg: An email message (email.message.Message)
      encoding: Encoding to use to encode Git commits
    Returns: Tuple with commit object, diff contents and git version
    """
    c = Commit()
    if encoding is None:
        encoding = "ascii"
    c.author = msg["from"].encode(encoding)
    c.committer = msg["from"].encode(encoding)
    try:
        patch_tag_start = msg["subject"].index("[PATCH")
    except ValueError:
        subject = msg["subject"]
    else:
        close = msg["subject"].index("] ", patch_tag_start)
        subject = msg["subject"][close + 2 :]
    c.message = (subject.replace("\n", "") + "\n").encode(encoding)
    first = True

    body = msg.get_payload(decode=True)
    if isinstance(body, str):
        body = body.encode(encoding)
    if isinstance(body, bytes):
        lines = body.splitlines(True)
    else:
        # Handle other types by converting to string first
        lines = str(body).encode(encoding).splitlines(True)
    line_iter = iter(lines)

    for line in line_iter:
        if line == b"---\n":
            break
        if first:
            if line.startswith(b"From: "):
                c.author = line[len(b"From: ") :].rstrip()
            else:
                c.message += b"\n" + line
            first = False
        else:
            c.message += line
    diff = b""
    for line in line_iter:
        if line == b"-- \n":
            break
        diff += line
    try:
        version = next(line_iter).rstrip(b"\n")
    except StopIteration:
        version = None
    return c, diff, version


def patch_id(diff_data: bytes) -> bytes:
    """Compute patch ID for a diff.

    The patch ID is computed by normalizing the diff and computing a SHA1 hash.
    This follows git's patch-id algorithm which:
    1. Removes whitespace from lines starting with + or -
    2. Replaces line numbers in @@ headers with a canonical form
    3. Computes SHA1 of the result

    Args:
        diff_data: Raw diff data as bytes

    Returns:
        SHA1 hash of normalized diff (40-byte hex string)

    TODO: This implementation uses a simple line-by-line approach. For better
    compatibility with git's patch-id, consider using proper patch parsing that:
    - Handles edge cases in diff format (binary diffs, mode changes, etc.)
    - Properly parses unified diff format according to the spec
    - Matches git's exact normalization algorithm byte-for-byte
    See git's patch-id.c for reference implementation.
    """
    import hashlib
    import re

    # Normalize the diff for patch-id computation
    normalized_lines = []

    for line in diff_data.split(b"\n"):
        # Skip diff headers (diff --git, index, ---, +++)
        if line.startswith(
            (
                b"diff --git ",
                b"index ",
                b"--- ",
                b"+++ ",
                b"new file mode ",
                b"old file mode ",
                b"deleted file mode ",
                b"new mode ",
                b"old mode ",
                b"similarity index ",
                b"dissimilarity index ",
                b"rename from ",
                b"rename to ",
                b"copy from ",
                b"copy to ",
            )
        ):
            continue

        # Normalize @@ headers to a canonical form
        if line.startswith(b"@@"):
            # Replace line numbers with canonical form
            match = re.match(rb"^@@\s+-\d+(?:,\d+)?\s+\+\d+(?:,\d+)?\s+@@", line)
            if match:
                # Use canonical hunk header without line numbers
                normalized_lines.append(b"@@")
                continue

        # For +/- lines, strip all whitespace
        if line.startswith((b"+", b"-")):
            # Keep the +/- prefix but remove all whitespace from the rest
            if len(line) > 1:
                # Remove all whitespace from the content
                content = line[1:].replace(b" ", b"").replace(b"\t", b"")
                normalized_lines.append(line[:1] + content)
            else:
                # Just +/- alone
                normalized_lines.append(line[:1])
            continue

        # Keep context lines and other content as-is
        if line.startswith(b" ") or line == b"":
            normalized_lines.append(line)

    # Join normalized lines and compute SHA1
    normalized = b"\n".join(normalized_lines)
    return hashlib.sha1(normalized).hexdigest().encode("ascii")


def commit_patch_id(
    store: "BaseObjectStore", commit_id: ObjectID | RawObjectID
) -> bytes:
    """Compute patch ID for a commit.

    Args:
        store: Object store to read objects from
        commit_id: Commit ID (40-byte hex string)

    Returns:
        Patch ID (40-byte hex string)
    """
    from io import BytesIO

    commit = store[commit_id]
    assert isinstance(commit, Commit)

    # Get the parent tree (or empty tree for root commit)
    if commit.parents:
        parent = store[commit.parents[0]]
        assert isinstance(parent, Commit)
        parent_tree = parent.tree
    else:
        # Root commit - compare against empty tree
        parent_tree = None

    # Generate diff
    diff_output = BytesIO()
    write_tree_diff(diff_output, store, parent_tree, commit.tree)

    return patch_id(diff_output.getvalue())


@dataclass
class MailinfoResult:
    """Result of mailinfo parsing.

    Attributes:
        author_name: Author's name
        author_email: Author's email address
        author_date: Author's date (if present in the email)
        subject: Processed subject line
        message: Commit message body
        patch: Patch content
        message_id: Message-ID header (if -m/--message-id was used)
    """

    author_name: str
    author_email: str
    author_date: str | None
    subject: str
    message: str
    patch: str
    message_id: str | None = None


def _munge_subject(subject: str, keep_subject: bool, keep_non_patch: bool) -> str:
    """Munge email subject line for commit message.

    Args:
        subject: Original subject line
        keep_subject: If True, keep subject intact (-k option)
        keep_non_patch: If True, only strip [PATCH] (-b option)

    Returns:
        Processed subject line
    """
    if keep_subject:
        return subject

    result = subject

    # First remove Re: prefixes (they can appear before brackets)
    while True:
        new_result = re.sub(r"^\s*(?:re|RE|Re):\s*", "", result, flags=re.IGNORECASE)
        if new_result == result:
            break
        result = new_result

    # Remove bracketed strings
    if keep_non_patch:
        # Only remove brackets containing "PATCH"
        # Match each bracket individually anywhere in the string
        while True:
            # Remove PATCH bracket, but be careful with whitespace
            new_result = re.sub(
                r"\[[^\]]*?PATCH[^\]]*?\](\s+)?", r"\1", result, flags=re.IGNORECASE
            )
            if new_result == result:
                break
            result = new_result
    else:
        # Remove all bracketed strings
        while True:
            new_result = re.sub(r"^\s*\[.*?\]\s*", "", result)
            if new_result == result:
                break
            result = new_result

    # Remove leading/trailing whitespace
    result = result.strip()

    # Normalize multiple whitespace to single space
    result = re.sub(r"\s+", " ", result)

    return result


def _find_scissors_line(lines: list[bytes]) -> int | None:
    """Find the scissors line in message body.

    Args:
        lines: List of lines in the message body

    Returns:
        Index of scissors line, or None if not found
    """
    scissors_pattern = re.compile(
        rb"^(?:>?\s*-+\s*)?(?:8<|>8)?\s*-+\s*$|^(?:>?\s*-+\s*)(?:cut here|scissors)(?:\s*-+)?$",
        re.IGNORECASE,
    )

    for i, line in enumerate(lines):
        if scissors_pattern.match(line.strip()):
            return i

    return None


def git_base85_decode(data: bytes) -> bytes:
    """Decode Git's base85-encoded binary data.

    Git uses a custom base85 encoding with its own alphabet and line format.
    Each line starts with a length byte followed by base85-encoded data.

    Args:
        data: Base85-encoded data as bytes (may contain multiple lines)

    Returns:
        Decoded binary data

    Raises:
        ValueError: If the data is invalid
    """
    # Git's base85 alphabet (different from RFC 1924)
    alphabet = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~"

    # Create decode table
    decode_table = {}
    for i, c in enumerate(alphabet):
        decode_table[c] = i

    result = bytearray()
    lines = data.strip().split(b"\n")

    for line in lines:
        if not line:
            continue

        # First character encodes the length of decoded data for this line
        if line[0] not in decode_table:
            continue

        encoded_len = decode_table[line[0]]
        if encoded_len == 0:
            continue

        # Decode the rest of the line
        encoded_data = line[1:]

        # Process in groups of 5 characters (which encode 4 bytes)
        i = 0
        decoded_this_line = 0
        while i < len(encoded_data) and decoded_this_line < encoded_len:
            # Get up to 5 characters
            group = encoded_data[i : i + 5]
            if len(group) == 0:
                break

            # Decode 5 base85 digits to a 32-bit value
            value = 0
            for c in group:
                if c not in decode_table:
                    raise ValueError(f"Invalid base85 character: {chr(c)}")
                value = value * 85 + decode_table[c]

            # Convert to 4 bytes (big-endian)
            bytes_to_add = min(4, encoded_len - decoded_this_line)
            decoded_bytes = value.to_bytes(4, byteorder="big")
            result.extend(decoded_bytes[:bytes_to_add])
            decoded_this_line += bytes_to_add
            i += 5

    return bytes(result)


@dataclass
class PatchHunk:
    """Represents a single hunk in a unified diff.

    Attributes:
        old_start: Starting line number in old file
        old_count: Number of lines in old file
        new_start: Starting line number in new file
        new_count: Number of lines in new file
        lines: List of diff lines (prefixed with ' ', '+', or '-')
    """

    old_start: int
    old_count: int
    new_start: int
    new_count: int
    lines: list[bytes]


@dataclass
class FilePatch:
    """Represents a patch for a single file.

    Attributes:
        old_path: Path to old file (None for new files)
        new_path: Path to new file (None for deleted files)
        old_mode: Mode of old file (None for new files)
        new_mode: Mode of new file (None for deleted files)
        hunks: List of PatchHunk objects
        binary: True if this is a binary patch
        rename_from: Original path for renames (None if not a rename)
        rename_to: New path for renames (None if not a rename)
        copy_from: Source path for copies (None if not a copy)
        copy_to: Destination path for copies (None if not a copy)
        binary_old: Old binary content for binary patches (base85 encoded)
        binary_new: New binary content for binary patches (base85 encoded)
    """

    old_path: bytes | None
    new_path: bytes | None
    old_mode: int | None
    new_mode: int | None
    hunks: list[PatchHunk]
    binary: bool = False
    rename_from: bytes | None = None
    rename_to: bytes | None = None
    copy_from: bytes | None = None
    copy_to: bytes | None = None
    binary_old: bytes | None = None
    binary_new: bytes | None = None


def parse_unified_diff(diff_text: bytes) -> list[FilePatch]:
    """Parse a unified diff into FilePatch objects.

    Args:
        diff_text: Unified diff content as bytes

    Returns:
        List of FilePatch objects
    """
    patches: list[FilePatch] = []
    lines = diff_text.split(b"\n")
    i = 0

    while i < len(lines):
        line = lines[i]

        # Look for diff header
        if line.startswith(b"diff --git "):
            # Parse file patch
            old_path = None
            new_path = None
            old_mode = None
            new_mode = None
            hunks: list[PatchHunk] = []
            binary = False
            rename_from = None
            rename_to = None
            copy_from = None
            copy_to = None
            binary_old = None
            binary_new = None

            # Parse extended headers
            i += 1
            while i < len(lines):
                line = lines[i]

                if line.startswith(b"old file mode "):
                    old_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"new file mode "):
                    new_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"deleted file mode "):
                    old_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"new mode "):
                    new_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"old mode "):
                    old_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"rename from "):
                    rename_from = line[12:].strip()
                    i += 1
                elif line.startswith(b"rename to "):
                    rename_to = line[10:].strip()
                    i += 1
                elif line.startswith(b"copy from "):
                    copy_from = line[10:].strip()
                    i += 1
                elif line.startswith(b"copy to "):
                    copy_to = line[8:].strip()
                    i += 1
                elif line.startswith(b"similarity index "):
                    # Just skip similarity index for now
                    i += 1
                elif line.startswith(b"dissimilarity index "):
                    # Just skip dissimilarity index for now
                    i += 1
                elif line.startswith(b"index "):
                    i += 1
                elif line.startswith(b"--- "):
                    # Parse old file path
                    path = line[4:].split(b"\t")[0]
                    if path != b"/dev/null":
                        old_path = path
                    i += 1
                elif line.startswith(b"+++ "):
                    # Parse new file path
                    path = line[4:].split(b"\t")[0]
                    if path != b"/dev/null":
                        new_path = path
                    i += 1
                    break
                elif line.startswith(b"Binary files"):
                    binary = True
                    i += 1
                    break
                elif line.startswith(b"GIT binary patch"):
                    binary = True
                    i += 1
                    # Parse binary patch data
                    while i < len(lines):
                        line = lines[i]
                        if line.startswith(b"literal "):
                            # New binary data
                            # size = int(line[8:].strip())  # Size information, not currently used
                            i += 1
                            binary_data = b""
                            while i < len(lines):
                                line = lines[i]
                                if (
                                    line.startswith(
                                        (b"literal ", b"delta ", b"diff --git ")
                                    )
                                    or not line.strip()
                                ):
                                    break
                                binary_data += line + b"\n"
                                i += 1
                            binary_new = binary_data
                        elif line.startswith(b"delta "):
                            # Delta patch (not supported yet)
                            i += 1
                            while i < len(lines):
                                line = lines[i]
                                if (
                                    line.startswith(
                                        (b"literal ", b"delta ", b"diff --git ")
                                    )
                                    or not line.strip()
                                ):
                                    break
                                i += 1
                        else:
                            break
                    break
                else:
                    i += 1
                    break

            # Parse hunks
            if not binary:
                while i < len(lines):
                    line = lines[i]

                    if line.startswith(b"@@ "):
                        # Parse hunk header
                        match = re.match(
                            rb"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line
                        )
                        if match:
                            old_start = int(match.group(1))
                            old_count = int(match.group(2)) if match.group(2) else 1
                            new_start = int(match.group(3))
                            new_count = int(match.group(4)) if match.group(4) else 1

                            # Parse hunk lines
                            hunk_lines: list[bytes] = []
                            i += 1
                            while i < len(lines):
                                line = lines[i]
                                if line.startswith((b" ", b"+", b"-", b"\\")):
                                    hunk_lines.append(line)
                                    i += 1
                                else:
                                    break

                            hunks.append(
                                PatchHunk(
                                    old_start=old_start,
                                    old_count=old_count,
                                    new_start=new_start,
                                    new_count=new_count,
                                    lines=hunk_lines,
                                )
                            )
                        else:
                            i += 1
                    elif line.startswith(b"diff --git "):
                        # Next file patch
                        break
                    else:
                        i += 1
                        if not line.strip():
                            # Empty line, might be end of patch or separator
                            break

            patches.append(
                FilePatch(
                    old_path=old_path,
                    new_path=new_path,
                    old_mode=old_mode,
                    new_mode=new_mode,
                    hunks=hunks,
                    binary=binary,
                    rename_from=rename_from,
                    rename_to=rename_to,
                    copy_from=copy_from,
                    copy_to=copy_to,
                    binary_old=binary_old,
                    binary_new=binary_new,
                )
            )
        else:
            i += 1

    return patches


def apply_patch_hunks(
    patch: FilePatch,
    original_lines: list[bytes],
) -> list[bytes] | None:
    """Apply patch hunks to file content.

    Args:
        patch: FilePatch object to apply
        original_lines: Original file content as list of lines

    Returns:
        Patched file content as list of lines, or None if patch cannot be applied
    """
    result = original_lines[:]
    offset = 0  # Track line offset as we apply hunks

    for hunk in patch.hunks:
        # Adjust hunk position by offset
        # old_start is 1-indexed; 0 means the hunk inserts at the beginning
        target_line = max(hunk.old_start - 1, 0) + offset

        # Extract old and new content from hunk
        old_content: list[bytes] = []
        new_content: list[bytes] = []

        for line in hunk.lines:
            if line.startswith(b"\\"):
                # Skip "\ No newline at end of file" markers
                continue
            elif line.startswith(b" "):
                # Context line - add newline if not present
                content = line[1:]
                if not content.endswith(b"\n"):
                    content += b"\n"
                old_content.append(content)
                new_content.append(content)
            elif line.startswith(b"-"):
                # Deletion - add newline if not present
                content = line[1:]
                if not content.endswith(b"\n"):
                    content += b"\n"
                old_content.append(content)
            elif line.startswith(b"+"):
                # Addition - add newline if not present
                content = line[1:]
                if not content.endswith(b"\n"):
                    content += b"\n"
                new_content.append(content)

        # Verify context matches
        if target_line < 0 or target_line + len(old_content) > len(result):
            # TODO: Implement fuzzy matching
            return None

        for i, old_line in enumerate(old_content):
            if result[target_line + i] != old_line:
                # Context doesn't match
                # TODO: Implement fuzzy matching
                return None

        # Apply the patch
        result[target_line : target_line + len(old_content)] = new_content

        # Update offset for next hunk
        offset += len(new_content) - len(old_content)

    return result


def _apply_rename_or_copy(
    r: "Repo",
    src_path: bytes,
    dst_path: bytes,
    strip: int,
    patch: FilePatch,
    is_rename: bool,
    cached: bool,
    check: bool,
) -> tuple[list[bytes] | None, bool]:
    """Apply a rename or copy operation.

    Args:
        r: Repository object
        src_path: Source path
        dst_path: Destination path
        strip: Number of path components to strip
        patch: FilePatch object
        is_rename: True for rename, False for copy
        cached: Apply to index only, not working tree
        check: Check only, don't apply

    Returns:
        A tuple of (``original_lines``, ``should_continue``) where:
        - ``original_lines``: Content lines if hunks need to be applied, None otherwise
        - ``should_continue``: True to skip to next patch, False to continue processing
    """
    from .index import ConflictedIndexEntry, IndexEntry, index_entry_from_stat

    # Strip path components
    src_stripped = src_path
    dst_stripped = dst_path
    if strip > 0:
        src_parts = src_path.split(b"/")
        if len(src_parts) > strip:
            src_stripped = b"/".join(src_parts[strip:])
        dst_parts = dst_path.split(b"/")
        if len(dst_parts) > strip:
            dst_stripped = b"/".join(dst_parts[strip:])

    repo_path_bytes = r.path.encode("utf-8") if isinstance(r.path, str) else r.path
    src_fs_path = os.path.join(repo_path_bytes, src_stripped)
    dst_fs_path = os.path.join(repo_path_bytes, dst_stripped)

    # Read content from source file
    op_name = "rename" if is_rename else "copy"
    if os.path.exists(src_fs_path):
        with open(src_fs_path, "rb") as f:
            content = f.read()
    else:
        # Try to read from index
        index = r.open_index()
        if src_stripped in index:
            entry = index[src_stripped]
            if not isinstance(entry, ConflictedIndexEntry):
                obj = r.object_store[entry.sha]
                if isinstance(obj, Blob):
                    content = obj.data
                else:
                    raise ValueError(
                        f"Cannot {op_name}: source {src_stripped.decode('utf-8', errors='replace')} not found"
                    )
            else:
                raise ValueError(
                    f"Cannot {op_name}: source {src_stripped.decode('utf-8', errors='replace')} is conflicted"
                )
        else:
            raise ValueError(
                f"Cannot {op_name}: source {src_stripped.decode('utf-8', errors='replace')} not found"
            )

    # If there are hunks, return content as lines for further processing
    if patch.hunks:
        return content.splitlines(keepends=True), False

    # No hunks - pure rename/copy
    if check:
        return None, True

    # Write to destination
    if not cached:
        os.makedirs(os.path.dirname(dst_fs_path), exist_ok=True)
        with open(dst_fs_path, "wb") as f:
            f.write(content)
        if patch.new_mode is not None:
            os.chmod(dst_fs_path, patch.new_mode)

    # Update index
    index = r.open_index()
    blob = Blob.from_string(content)
    r.object_store.add_object(blob)

    if not cached and os.path.exists(dst_fs_path):
        st = os.stat(dst_fs_path)
        entry = index_entry_from_stat(st, blob.id, 0)
    else:
        entry = IndexEntry(
            ctime=(0, 0),
            mtime=(0, 0),
            dev=0,
            ino=0,
            mode=patch.new_mode or 0o100644,
            uid=0,
            gid=0,
            size=len(content),
            sha=blob.id,
            flags=0,
        )

    index[dst_stripped] = entry

    # For renames, remove the old file
    if is_rename:
        if not cached and os.path.exists(src_fs_path):
            os.remove(src_fs_path)
        if src_stripped in index:
            del index[src_stripped]

    index.write()
    return None, True


def apply_patches(
    r: "Repo",
    patches: list[FilePatch],
    cached: bool = False,
    reverse: bool = False,
    check: bool = False,
    strip: int = 1,
    three_way: bool = False,
) -> None:
    """Apply a list of file patches to a repository.

    Args:
        r: Repository object
        patches: List of FilePatch objects to apply
        cached: Apply patch to index only, not working tree
        reverse: Apply patch in reverse
        check: Only check if patch can be applied, don't apply
        strip: Number of leading path components to strip (default: 1)
        three_way: Fall back to 3-way merge if patch does not apply cleanly

    Raises:
        ValueError: If patch cannot be applied
    """
    from .index import ConflictedIndexEntry, IndexEntry, index_entry_from_stat

    for patch in patches:
        # Determine the file path
        # For renames/copies without hunks, old_path/new_path may be None
        # Use local variables to avoid mutating the patch object
        old_path = patch.old_path
        new_path = patch.new_path

        if new_path is None and old_path is None:
            if patch.rename_to is not None:
                # Use rename_to for the target path
                new_path = patch.rename_to
                old_path = patch.rename_from
            elif patch.copy_to is not None:
                # Use copy_to for the target path
                new_path = patch.copy_to
                old_path = patch.copy_from
            else:
                raise ValueError("Patch has no file path")

        # Choose path based on operation
        file_path: bytes
        if new_path is None:
            # Deletion
            if old_path is None:
                raise ValueError("Patch has no file path")
            file_path = old_path
        elif old_path is None:
            # Addition
            file_path = new_path
        else:
            # Modification (use new path)
            file_path = new_path

        # Strip path components
        if strip > 0:
            parts = file_path.split(b"/")
            if len(parts) > strip:
                file_path = b"/".join(parts[strip:])

        # Convert to filesystem path
        tree_path = file_path
        fs_path = os.path.join(
            r.path.encode("utf-8") if isinstance(r.path, str) else r.path, file_path
        )

        # Handle renames and copies
        original_lines: list[bytes] | None = None
        if patch.rename_from is not None and patch.rename_to is not None:
            original_lines, should_continue = _apply_rename_or_copy(
                r,
                patch.rename_from,
                patch.rename_to,
                strip,
                patch,
                is_rename=True,
                cached=cached,
                check=check,
            )
            if should_continue:
                continue
        elif patch.copy_from is not None and patch.copy_to is not None:
            original_lines, should_continue = _apply_rename_or_copy(
                r,
                patch.copy_from,
                patch.copy_to,
                strip,
                patch,
                is_rename=False,
                cached=cached,
                check=check,
            )
            if should_continue:
                continue

        # Handle binary patches
        if patch.binary:
            if patch.binary_new is not None:
                # Decode binary patch
                try:
                    binary_content = git_base85_decode(patch.binary_new)
                except (ValueError, KeyError) as e:
                    raise ValueError(f"Failed to decode binary patch: {e}")

                if check:
                    # Just checking, don't actually apply
                    continue

                # Write binary file
                if not cached:
                    os.makedirs(os.path.dirname(fs_path), exist_ok=True)
                    with open(fs_path, "wb") as f:
                        f.write(binary_content)
                    if patch.new_mode is not None:
                        os.chmod(fs_path, patch.new_mode)

                # Update index
                index = r.open_index()
                blob = Blob.from_string(binary_content)
                r.object_store.add_object(blob)

                if not cached and os.path.exists(fs_path):
                    st = os.stat(fs_path)
                    entry = index_entry_from_stat(st, blob.id, 0)
                else:
                    entry = IndexEntry(
                        ctime=(0, 0),
                        mtime=(0, 0),
                        dev=0,
                        ino=0,
                        mode=patch.new_mode or 0o100644,
                        uid=0,
                        gid=0,
                        size=len(binary_content),
                        sha=blob.id,
                        flags=0,
                    )

                index[tree_path] = entry
                index.write()
                continue
            else:
                # Old-style "Binary files differ" message without actual patch data
                raise NotImplementedError(
                    "Binary patch detected but no patch data provided (use git diff --binary)"
                )

        # Read original file content (unless already loaded from rename/copy)
        if original_lines is None:
            if patch.old_path is None:
                # New file
                original_lines = []
            else:
                if os.path.exists(fs_path):
                    with open(fs_path, "rb") as f:
                        content = f.read()
                    original_lines = content.splitlines(keepends=True)
                else:
                    # File doesn't exist - check if it's in the index
                    try:
                        index = r.open_index()
                        if tree_path in index:
                            index_entry: IndexEntry | ConflictedIndexEntry = index[
                                tree_path
                            ]
                            if not isinstance(index_entry, ConflictedIndexEntry):
                                obj = r.object_store[index_entry.sha]
                                if isinstance(obj, Blob):
                                    original_lines = obj.data.splitlines(keepends=True)
                                else:
                                    original_lines = []
                            else:
                                original_lines = []
                        else:
                            original_lines = []
                    except (KeyError, FileNotFoundError):
                        original_lines = []

        # Reverse patch if requested
        if reverse:
            # Swap old and new in hunks
            for hunk in patch.hunks:
                hunk.old_start, hunk.new_start = hunk.new_start, hunk.old_start
                hunk.old_count, hunk.new_count = hunk.new_count, hunk.old_count
                # Swap +/- prefixes
                reversed_lines = []
                for line in hunk.lines:
                    if line.startswith(b"+"):
                        reversed_lines.append(b"-" + line[1:])
                    elif line.startswith(b"-"):
                        reversed_lines.append(b"+" + line[1:])
                    else:
                        reversed_lines.append(line)
                hunk.lines = reversed_lines

        # Apply the patch
        assert original_lines is not None
        result = apply_patch_hunks(patch, original_lines)

        if result is None and three_way:
            # Try 3-way merge fallback
            from .merge import merge_blobs

            # Reconstruct base version from the patch
            # Base is what you get by taking only the old lines from hunks
            base_lines = []
            theirs_lines = []

            for hunk in patch.hunks:
                for line in hunk.lines:
                    if line.startswith(b"\\"):
                        # Skip "\ No newline at end of file" markers
                        continue
                    elif line.startswith(b" "):
                        # Context line - in both base and theirs
                        content = line[1:]
                        if not content.endswith(b"\n"):
                            content += b"\n"
                        base_lines.append(content)
                        theirs_lines.append(content)
                    elif line.startswith(b"-"):
                        # Deletion - only in base
                        content = line[1:]
                        if not content.endswith(b"\n"):
                            content += b"\n"
                        base_lines.append(content)
                    elif line.startswith(b"+"):
                        # Addition - only in theirs
                        content = line[1:]
                        if not content.endswith(b"\n"):
                            content += b"\n"
                        theirs_lines.append(content)

            # Create blobs for merging
            base_content = b"".join(base_lines)
            ours_content = b"".join(original_lines)
            theirs_content = b"".join(theirs_lines)

            base_blob = Blob.from_string(base_content) if base_content else None
            ours_blob = Blob.from_string(ours_content) if ours_content else None
            theirs_blob = Blob.from_string(theirs_content)

            # Perform 3-way merge
            merged_content, _had_conflicts = merge_blobs(
                base_blob, ours_blob, theirs_blob, path=tree_path
            )

            result = merged_content.splitlines(keepends=True)

            # Note: if _had_conflicts is True, the result contains conflict markers
            # Git would exit with error code, but we continue processing
        elif result is None:
            raise PatchApplicationFailure(
                f"Patch does not apply to {file_path.decode('utf-8', errors='replace')}"
            )

        if check:
            # Just checking, don't actually apply
            continue

        # Write result
        result_content = b"".join(result)

        if patch.new_path is None:
            # File deletion
            if not cached and os.path.exists(fs_path):
                os.remove(fs_path)
            # Remove from index
            index = r.open_index()
            if tree_path in index:
                del index[tree_path]
                index.write()
        else:
            # File addition or modification
            if not cached:
                # Write to working tree
                os.makedirs(os.path.dirname(fs_path), exist_ok=True)
                with open(fs_path, "wb") as f:
                    f.write(result_content)

                # Update file mode if specified
                if patch.new_mode is not None:
                    os.chmod(fs_path, patch.new_mode)

            # Update index
            index = r.open_index()
            blob = Blob.from_string(result_content)
            r.object_store.add_object(blob)

            # Get file stat for index entry
            if not cached and os.path.exists(fs_path):
                st = os.stat(fs_path)
                entry = index_entry_from_stat(st, blob.id, 0)
            else:
                # Create a minimal index entry for cached-only changes
                entry = IndexEntry(
                    ctime=(0, 0),
                    mtime=(0, 0),
                    dev=0,
                    ino=0,
                    mode=patch.new_mode or 0o100644,
                    uid=0,
                    gid=0,
                    size=len(result_content),
                    sha=blob.id,
                    flags=0,
                )

            index[tree_path] = entry

            # Handle cleanup for renames with hunks
            if patch.rename_from is not None and patch.rename_to is not None:
                # Remove old file after successful rename
                old_rename_path = patch.rename_from
                if strip > 0:
                    old_parts = old_rename_path.split(b"/")
                    if len(old_parts) > strip:
                        old_rename_path = b"/".join(old_parts[strip:])

                old_fs_path = os.path.join(
                    r.path.encode("utf-8") if isinstance(r.path, str) else r.path,
                    old_rename_path,
                )

                if not cached and os.path.exists(old_fs_path):
                    os.remove(old_fs_path)
                if old_rename_path in index:
                    del index[old_rename_path]

            index.write()


def mailinfo(
    msg: email.message.Message | BinaryIO | TextIO,
    keep_subject: bool = False,
    keep_non_patch: bool = False,
    encoding: str | None = None,
    scissors: bool = False,
    message_id: bool = False,
) -> MailinfoResult:
    """Extract patch information from an email message.

    This function parses an email message and extracts commit metadata
    (author, email, subject) and separates the commit message from the
    patch content, similar to git mailinfo.

    Args:
        msg: Email message (email.message.Message object) or file handle to read from
        keep_subject: If True, keep subject intact without munging (-k)
        keep_non_patch: If True, only strip [PATCH] from brackets (-b)
        encoding: Character encoding to use (default: detect from message)
        scissors: If True, remove everything before scissors line
        message_id: If True, include Message-ID in commit message (-m)

    Returns:
        MailinfoResult with parsed information

    Raises:
        ValueError: If message is malformed or missing required fields
    """
    # Parse message if given a file handle
    parsed_msg: email.message.Message
    if not isinstance(msg, email.message.Message):
        if hasattr(msg, "read"):
            content = msg.read()
            if isinstance(content, bytes):
                bparser = email.parser.BytesParser()
                parsed_msg = bparser.parsebytes(content)
            else:
                sparser = email.parser.Parser()
                parsed_msg = sparser.parsestr(content)
        else:
            raise ValueError("msg must be an email.message.Message or file-like object")
    else:
        parsed_msg = msg

    # Detect encoding from message if not specified
    if encoding is None:
        encoding = parsed_msg.get_content_charset() or "utf-8"

    # Extract author information
    from_header = parsed_msg.get("From", "")
    if not from_header:
        raise ValueError("Email message missing 'From' header")

    # Parse "Name <email>" format
    author_name, author_email = email.utils.parseaddr(from_header)
    if not author_email:
        raise ValueError(
            f"Could not parse email address from 'From' header: {from_header}"
        )

    # Extract date
    date_header = parsed_msg.get("Date")
    author_date = date_header if date_header else None

    # Extract and process subject
    subject = parsed_msg.get("Subject", "")
    if not subject:
        subject = "(no subject)"

    # Convert Header object to string if needed
    subject = str(subject)

    # Remove newlines from subject
    subject = subject.replace("\n", " ").replace("\r", " ")
    subject = _munge_subject(subject, keep_subject, keep_non_patch)

    # Extract Message-ID if requested
    msg_id = None
    if message_id:
        msg_id = parsed_msg.get("Message-ID")

    # Get message body
    body = parsed_msg.get_payload(decode=True)
    if body is None:
        body = b""
    elif isinstance(body, str):
        body = body.encode(encoding)
    elif not isinstance(body, bytes):
        # Handle multipart or other types
        body = str(body).encode(encoding)

    # Split into lines
    lines = body.splitlines(keepends=True)

    # Handle scissors
    scissors_idx = None
    if scissors:
        scissors_idx = _find_scissors_line(lines)
        if scissors_idx is not None:
            # Remove everything up to and including scissors line
            lines = lines[scissors_idx + 1 :]

    # Separate commit message from patch
    # Look for the "---" separator that indicates start of diffstat/patch
    message_lines: list[bytes] = []
    patch_lines: list[bytes] = []
    in_patch = False

    for line in lines:
        if not in_patch and line == b"---\n":
            in_patch = True
            patch_lines.append(line)
        elif in_patch:
            # Stop at signature marker "-- "
            if line == b"-- \n":
                break
            patch_lines.append(line)
        else:
            message_lines.append(line)

    # Build commit message
    commit_message = b"".join(message_lines).decode(encoding, errors="replace")

    # Clean up commit message
    commit_message = commit_message.strip()

    # Append Message-ID if requested
    if message_id and msg_id:
        if commit_message:
            commit_message += "\n\n"
        commit_message += f"Message-ID: {msg_id}"

    # Build patch content
    patch_content = b"".join(patch_lines).decode(encoding, errors="replace")

    return MailinfoResult(
        author_name=author_name,
        author_email=author_email,
        author_date=author_date,
        subject=subject,
        message=commit_message,
        patch=patch_content,
        message_id=msg_id,
    )
