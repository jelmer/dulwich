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
    "FilePatch",
    "MailinfoResult",
    "PatchApplicationFailure",
    "PatchHunk",
    "apply_patch_hunks",
    "apply_patches",
    "commit_patch_id",
    "gen_diff_header",
    "get_summary",
    "git_am_patch_split",
    "git_base85_decode",
    "is_binary",
    "mailinfo",
    "parse_patch_message",
    "parse_unified_diff",
    "patch_filename",
    "patch_id",
    "shortid",
    "unified_diff",
    "unified_diff_with_algorithm",
    "write_blob_diff",
    "write_commit_diff",
    "write_commit_patch",
    "write_object_diff",
    "write_tree_diff",
]

import base64
import email.message
import email.parser
import email.utils
import os
import re
import stat
import time
import zlib
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
    from .config import Config
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


def _sanitize_subject_for_filename(text: str, max_length: int = 52) -> str:
    """Sanitize a string for safe use as part of a filename.

    Matches git's ``format_sanitized_subject`` behavior:

    - Only ``[A-Za-z0-9._]`` are kept; other characters become ``-``
      (collapsed across runs).
    - Consecutive ``.`` are collapsed to a single ``.``.
    - The result is truncated to ``max_length`` characters.
    - Trailing ``.`` and ``-`` are stripped.

    Args:
      text: Input string (typically a commit subject line).
      max_length: Maximum length of the returned string.

    Returns: Sanitized string safe to embed in a filename.
    """
    result: list[str] = []
    # 2 = initial, 1 = saw a non-title char, 0 = saw a title char
    space = 2
    i = 0
    text_len = len(text)
    while i < text_len:
        c = text[i]
        if ("A" <= c <= "Z") or ("a" <= c <= "z") or ("0" <= c <= "9") or c in "._":
            if space == 1:
                result.append("-")
            space = 0
            result.append(c)
            if c == ".":
                while i + 1 < text_len and text[i + 1] == ".":
                    i += 1
        else:
            space |= 1
        i += 1
        if len(result) >= max_length:
            break

    return "".join(result)[:max_length].rstrip(".-")


def get_summary(commit: "Commit") -> str:
    """Determine the summary line for use in a filename.

    Sanitizes the commit subject so it is safe to use as a filename
    component, matching git's ``format_sanitized_subject`` behavior:
    characters outside ``[A-Za-z0-9._]`` are replaced with ``-`` (with
    runs collapsed) and consecutive ``.`` are collapsed. The result is
    also length-limited to prevent overly long filenames.

    Args:
      commit: Commit
    Returns: Sanitized summary string suitable for use as a filename
      component.
    """
    decoded = commit.message.decode(errors="replace")
    lines = decoded.splitlines()
    if not lines:
        return ""
    return _sanitize_subject_for_filename(lines[0])


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


def write_commit_diff(
    f: IO[bytes],
    store: "BaseObjectStore",
    commit: "Commit",
    diff_binary: bool = False,
    diff_algorithm: str | None = None,
) -> None:
    """Write the diff a commit introduces against its first parent.

    Args:
      f: File-like object to write to.
      store: Object store to read from.
      commit: Commit whose changes to diff. Root commits (with no parents)
        are diffed against the empty tree.
      diff_binary: Whether to diff files even if they are considered binary
        files by is_binary().
      diff_algorithm: Algorithm to use for diffing ("myers" or "patience").
    """
    if commit.parents:
        parent = store[commit.parents[0]]
        assert isinstance(parent, Commit)
        parent_tree: ObjectID | None = parent.tree
    else:
        parent_tree = None
    write_tree_diff(
        f,
        store,
        parent_tree,
        commit.tree,
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

    diff_output = BytesIO()
    write_commit_diff(diff_output, store, commit)

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
    # The perforation alternatives are written so no two unbounded ``-+``
    # quantifiers are separated by an optional/empty subpattern. The earlier
    # form ``(?:>?\s*-+\s*)?(?:8<|>8)?\s*-+\s*`` let a single run of dashes be
    # split between two ``-+`` groups, so a long non-matching line of dashes
    # backtracked quadratically (ReDoS) when fed through ``mailinfo`` /
    # ``git am --scissors`` on an untrusted mailbox. Here a marker is the
    # mandatory pivot between the two dash runs, and the marker-less case uses a
    # bounded ``(?:\s+-+)?`` second run, keeping the match linear.
    scissors_pattern = re.compile(
        rb"^>?\s*-+(?:\s+-+)?\s*$"
        rb"|^>?\s*(?:-+\s*)?(?:8<|>8)\s*-+\s*$"
        rb"|^(?:>?\s*-+\s*)(?:cut here|scissors)(?:\s*-+)?$",
        re.IGNORECASE,
    )

    for i, line in enumerate(lines):
        if scissors_pattern.match(line.strip()):
            return i

    return None


def git_base85_decode(data: bytes) -> bytes:
    """Decode Git's base85-encoded binary data.

    Each line starts with a byte giving the decoded length of that line
    (``A``-``Z`` for 1-26, ``a``-``z`` for 27-52), followed by groups of five
    base85 characters that each encode four bytes. Git's alphabet is the
    RFC 1924 one used by ``base64.b85decode``.

    Args:
        data: Base85-encoded data as bytes (may contain multiple lines)

    Returns:
        Decoded binary data

    Raises:
        ValueError: If the data is invalid
    """
    result = bytearray()
    for line in data.splitlines():
        if not line:
            continue
        length_byte = line[0]
        if ord("A") <= length_byte <= ord("Z"):
            length = length_byte - ord("A") + 1
        elif ord("a") <= length_byte <= ord("z"):
            length = length_byte - ord("a") + 27
        else:
            raise ValueError(f"Invalid base85 line length byte: {line[:1]!r}")
        encoded = line[1:]
        if len(encoded) != (length + 3) // 4 * 5:
            raise ValueError(
                f"Base85 line has {len(encoded)} characters, "
                f"expected {(length + 3) // 4 * 5} for {length} bytes"
            )
        result.extend(base64.b85decode(encoded)[:length])
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
        binary_old: Reverse binary data for binary patches (base85 encoded)
        binary_new: Forward binary data for binary patches (base85 encoded)
        binary_old_delta: True if binary_old is a delta against the new
            content rather than a literal copy of the old content
        binary_new_delta: True if binary_new is a delta against the old
            content rather than a literal copy of the new content
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
    binary_old_delta: bool = False
    binary_new_delta: bool = False


_C_STYLE_ESCAPES = {
    ord("a"): b"\a",
    ord("b"): b"\b",
    ord("t"): b"\t",
    ord("n"): b"\n",
    ord("v"): b"\v",
    ord("f"): b"\f",
    ord("r"): b"\r",
    ord('"'): b'"',
    ord("\\"): b"\\",
}


def _unquote_c_style(text: bytes) -> tuple[bytes, bytes]:
    """Unquote a C-style quoted name at the start of ``text``.

    Git quotes names containing special or non-ASCII characters this way.

    Returns:
      Tuple with the unquoted name and the remainder of ``text`` after the
      closing quote.

    Raises:
      ValueError: If the quoted name is malformed
    """
    if not text.startswith(b'"'):
        raise ValueError(f"Name is not quoted: {text!r}")
    result = bytearray()
    i = 1
    while i < len(text):
        c = text[i]
        if c == ord('"'):
            return bytes(result), text[i + 1 :]
        if c != ord("\\"):
            result.append(c)
            i += 1
            continue
        escaped = text[i + 1 : i + 2]
        if escaped and escaped[0] in _C_STYLE_ESCAPES:
            result += _C_STYLE_ESCAPES[escaped[0]]
            i += 2
        elif re.fullmatch(rb"[0-3][0-7][0-7]", text[i + 1 : i + 4]):
            result.append(int(text[i + 1 : i + 4], 8))
            i += 4
        else:
            raise ValueError(f"Invalid escape in quoted name: {text!r}")
    raise ValueError(f"Unterminated quoted name: {text!r}")


def _unquote_name(name: bytes) -> bytes:
    """Unquote a name from a patch header if git quoted it."""
    if not name.startswith(b'"'):
        return name
    unquoted, rest = _unquote_c_style(name)
    if rest:
        raise ValueError(f"Trailing data after quoted name: {name!r}")
    return unquoted


def _parse_file_line_path(path: bytes) -> bytes:
    """Parse the path from a ``---`` or ``+++`` line, dropping any timestamp."""
    if not path.startswith(b'"'):
        return path.split(b"\t")[0]
    unquoted, rest = _unquote_c_style(path)
    if rest and not rest.startswith(b"\t"):
        raise ValueError(f"Trailing data after quoted name: {path!r}")
    return unquoted


def _parse_git_diff_header_paths(line: bytes) -> tuple[bytes | None, bytes | None]:
    """Extract the old and new paths from a ``diff --git`` line.

    Unquoted names are only separable when they are the same apart from their
    prefixes (as for anything but a rename or copy), in which case the line is
    split in the middle. Returns ``(None, None)`` if the names differ.
    """
    names = line[len(b"diff --git ") :]
    if names.startswith(b'"'):
        old, rest = _unquote_c_style(names)
        if not rest.startswith(b" "):
            return None, None
        new = _unquote_name(rest[1:])
    else:
        half = len(names) // 2
        if len(names) % 2 != 1 or names[half : half + 1] != b" ":
            return None, None
        old, new = names[:half], names[half + 1 :]
    if old.partition(b"/")[2] != new.partition(b"/")[2]:
        return None, None
    return old, new


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
            binary_old_delta = False
            binary_new_delta = False
            header_old_path, header_new_path = _parse_git_diff_header_paths(line)

            # Parse extended headers
            i += 1
            while i < len(lines):
                line = lines[i]

                if line.startswith(b"old file mode "):
                    old_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"new file mode "):
                    new_mode = int(line.split()[-1], 8)
                    header_old_path = None
                    i += 1
                elif line.startswith(b"deleted file mode "):
                    old_mode = int(line.split()[-1], 8)
                    header_new_path = None
                    i += 1
                elif line.startswith(b"new mode "):
                    new_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"old mode "):
                    old_mode = int(line.split()[-1], 8)
                    i += 1
                elif line.startswith(b"rename from "):
                    rename_from = _unquote_name(line[12:].strip())
                    i += 1
                elif line.startswith(b"rename to "):
                    rename_to = _unquote_name(line[10:].strip())
                    i += 1
                elif line.startswith(b"copy from "):
                    copy_from = _unquote_name(line[10:].strip())
                    i += 1
                elif line.startswith(b"copy to "):
                    copy_to = _unquote_name(line[8:].strip())
                    i += 1
                elif line.startswith(b"similarity index "):
                    # Just skip similarity index for now
                    i += 1
                elif line.startswith(b"dissimilarity index "):
                    # Just skip dissimilarity index for now
                    i += 1
                elif line.startswith(b"index "):
                    # "index <old>..<new> <mode>" gives the mode when the
                    # patch doesn't change it.
                    fields = line.split()
                    if len(fields) == 3:
                        mode = int(fields[2], 8)
                        if old_mode is None:
                            old_mode = mode
                        if new_mode is None:
                            new_mode = mode
                    i += 1
                elif line.startswith(b"--- "):
                    # Parse old file path
                    path = _parse_file_line_path(line[4:])
                    if path != b"/dev/null":
                        old_path = path
                    i += 1
                elif line.startswith(b"+++ "):
                    # Parse new file path
                    path = _parse_file_line_path(line[4:])
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
                    # The forward data comes first, followed by the reverse
                    # data. Each is a "literal" or "delta" line, then base85
                    # lines up to a blank line.
                    blocks: list[tuple[bool, bytes]] = []
                    while (
                        i < len(lines)
                        and len(blocks) < 2
                        and lines[i].startswith((b"literal ", b"delta "))
                    ):
                        is_delta = lines[i].startswith(b"delta ")
                        i += 1
                        block_start = i
                        while i < len(lines) and lines[i]:
                            i += 1
                        blocks.append(
                            (
                                is_delta,
                                b"".join(data + b"\n" for data in lines[block_start:i]),
                            )
                        )
                        i += 1
                    if not blocks:
                        raise ValueError("GIT binary patch without data")
                    binary_new_delta, binary_new = blocks[0]
                    if len(blocks) > 1:
                        binary_old_delta, binary_old = blocks[1]
                    break
                else:
                    # Leave the line (e.g. the next "diff --git") to the
                    # hunk parser below.
                    break

            if old_path is None and new_path is None:
                # Binary patches and those without content changes (mode
                # changes, empty files) have no ---/+++ lines.
                old_path = header_old_path
                new_path = header_new_path

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
                    binary_old_delta=binary_old_delta,
                    binary_new_delta=binary_new_delta,
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

        previous = b""
        for line in hunk.lines:
            if line.startswith(b"\\"):
                # "\ No newline at end of file" applies to the line before it
                if previous in (b" ", b"-"):
                    old_content[-1] = old_content[-1].removesuffix(b"\n")
                if previous in (b" ", b"+"):
                    new_content[-1] = new_content[-1].removesuffix(b"\n")
                continue
            previous = line[:1]
            if line.startswith(b" "):
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


def _ensure_within_repo(repo_path: bytes, fs_path: bytes, rel_path: bytes) -> None:
    """Reject patch target paths that resolve outside the working tree.

    Patch headers are untrusted (e.g. ``git am`` of a mailbox), so a name such
    as ``../../etc/cron.d/x`` or an absolute path must not be written through
    ``os.path.join``. Mirrors git's refusal of paths outside the work tree.
    """
    repo_root = os.path.realpath(repo_path)
    resolved = os.path.realpath(fs_path)
    if resolved != repo_root and not resolved.startswith(
        repo_root + os.fsencode(os.sep)
    ):
        raise ValueError(f"patch affects file outside repository: {rel_path!r}")


def _validate_patch_target(r: "Repo", repo_path: bytes, tree_path: bytes) -> bytes:
    """Validate a patch target path and return its filesystem path.

    ``_ensure_within_repo`` alone only refuses paths that escape the work tree;
    a target like ``.git/hooks/pre-commit`` stays inside it and would otherwise
    be written (and later executed as a hook). On top of that containment check,
    apply the same name and symlink checks git uses for checkout, so patch
    application cannot reach the control directory. Mirrors
    ``porcelain._checked_worktree_path``.

    Returns:
      The filesystem path under ``repo_path``, as bytes.
    """
    from .index import (
        InvalidPathError,
        get_path_element_validator,
        validate_path,
        verify_leading_dirs,
    )

    fs_path = os.path.join(repo_path, tree_path)
    _ensure_within_repo(repo_path, fs_path, tree_path)
    validator = get_path_element_validator(r.get_config_stack())
    if not validate_path(tree_path, validator):
        raise ValueError(f"refusing to write unsafe path: {tree_path!r}")
    try:
        verify_leading_dirs(tree_path, [], repo_path)
    except InvalidPathError:
        raise ValueError(f"refusing to write through symlink: {tree_path!r}")
    return fs_path


def _read_patch_target(fs_path: bytes) -> tuple[bytes, int] | None:
    """Read the content and mode of a patch target in the work tree.

    A symlink is read with ``readlink`` rather than followed, since its
    content in git is the link target.

    Returns:
      Tuple with content and mode, or None if the path does not exist
    """
    from .index import cleanup_mode

    try:
        st = os.lstat(fs_path)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(st.st_mode):
        return os.readlink(fs_path), stat.S_IFLNK
    with open(fs_path, "rb") as f:
        return f.read(), cleanup_mode(st.st_mode)


def _check_patch_target_type(
    tree_path: bytes, disk_mode: int, expected_mode: int, config: "Config"
) -> None:
    """Refuse to apply a patch for a symlink to a file or vice versa.

    ``verify_leading_dirs`` only checks the leading directories. A tracked
    symlink such as ``trap -> .git/hooks/pre-commit`` resolves inside the work
    tree, so writing a regular file patch to it would follow the link and land
    in the control directory. git refuses such patches with "wrong type".
    """
    if stat.S_ISLNK(disk_mode) == stat.S_ISLNK(expected_mode):
        return
    if (
        stat.S_ISLNK(expected_mode)
        and not stat.S_ISLNK(disk_mode)
        and not config.get_boolean(b"core", b"symlinks", True)
    ):
        # With core.symlinks=false links are checked out as plain files.
        return
    raise ValueError(f"wrong type for patch target: {tree_path!r}")


def _write_patch_target(
    fs_path: bytes, content: bytes, mode: int, config: "Config"
) -> os.stat_result:
    """Write a patch result to the work tree without following symlinks.

    Returns:
      The ``lstat`` result of the written path
    """
    from .index import build_file_from_blob, get_symlink_fn

    os.makedirs(os.path.dirname(fs_path), exist_ok=True)
    return build_file_from_blob(
        Blob.from_string(content), mode, fs_path, symlink_fn=get_symlink_fn(config)
    )


def _refuse_existing_target(fs_path: bytes, tree_path: bytes) -> None:
    """Refuse to create a file over an existing path, like git does."""
    if os.path.lexists(fs_path):
        raise ValueError(f"{tree_path!r} already exists in working directory")


def _load_patch_target(
    r: "Repo",
    fs_path: bytes,
    tree_path: bytes,
    expected_mode: int | None,
    config: "Config",
) -> tuple[bytes, int] | None:
    """Load the current content and mode of an existing patch target.

    Reads the work tree, falling back to the index if the path is missing
    there. The mode from the patch, if any, must match the type on disk.

    Returns:
      Tuple with content and mode, or None if the path can't be found
    """
    from .index import ConflictedIndexEntry, IndexEntry

    def index_entry() -> IndexEntry | None:
        try:
            entry = r.open_index(config=config)[tree_path]
        except (FileNotFoundError, KeyError):
            return None
        if isinstance(entry, ConflictedIndexEntry):
            return None
        return entry

    current = _read_patch_target(fs_path)
    if current is None:
        entry = index_entry()
        if entry is None:
            return None
        obj = r.object_store[entry.sha]
        if not isinstance(obj, Blob):
            return None
        return obj.data, entry.mode
    content, mode = current
    if expected_mode is not None:
        _check_patch_target_type(tree_path, mode, expected_mode, config)
        return content, expected_mode
    if not stat.S_ISLNK(mode) and not config.get_boolean(b"core", b"symlinks", True):
        # A link checked out as a plain file is only recognizable as such
        # from the index.
        entry = index_entry()
        if entry is not None and stat.S_ISLNK(entry.mode):
            return content, entry.mode
    return content, mode


def _apply_rename_or_copy(
    r: "Repo",
    src_path: bytes,
    dst_path: bytes,
    strip: int,
    patch: FilePatch,
    is_rename: bool,
    cached: bool,
    check: bool,
    config: "Config",
) -> tuple[list[bytes] | None, int, bool]:
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
        config: Repository configuration

    Returns:
        A tuple of (``original_lines``, ``old_mode``, ``should_continue``) where:
        - ``original_lines``: Content lines if hunks need to be applied, None otherwise
        - ``old_mode``: Mode of the source
        - ``should_continue``: True to skip to next patch, False to continue processing
    """
    from .index import IndexEntry, index_entry_from_stat

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
    src_fs_path = _validate_patch_target(r, repo_path_bytes, src_stripped)
    dst_fs_path = _validate_patch_target(r, repo_path_bytes, dst_stripped)
    if not cached:
        _refuse_existing_target(dst_fs_path, dst_stripped)

    # Read content from source file
    op_name = "rename" if is_rename else "copy"
    current = _load_patch_target(r, src_fs_path, src_stripped, patch.old_mode, config)
    if current is None:
        raise ValueError(
            f"Cannot {op_name}: source {src_stripped.decode('utf-8', errors='replace')} not found"
        )
    content, old_mode = current

    # If the content changes too, return it for further processing
    if patch.hunks or patch.binary:
        return content.splitlines(keepends=True), old_mode, False

    # No hunks - pure rename/copy
    if check:
        return None, old_mode, True

    new_mode = patch.new_mode or old_mode
    index = r.open_index(config=config)
    blob = Blob.from_string(content)
    r.object_store.add_object(blob)

    if not cached:
        st = _write_patch_target(dst_fs_path, content, new_mode, config)
        entry = index_entry_from_stat(st, blob.id, mode=new_mode)
    else:
        entry = IndexEntry(
            ctime=(0, 0),
            mtime=(0, 0),
            dev=0,
            ino=0,
            mode=new_mode,
            uid=0,
            gid=0,
            size=len(content),
            sha=blob.id,
            flags=0,
        )

    index[dst_stripped] = entry

    # For renames, remove the old file
    if is_rename:
        if not cached and os.path.lexists(src_fs_path):
            os.remove(src_fs_path)
        if src_stripped in index:
            del index[src_stripped]

    index.write()
    return None, old_mode, True


def _apply_binary_patch(patch: FilePatch, original: bytes, reverse: bool) -> bytes:
    """Compute the new content of a file from a ``GIT binary patch``.

    Args:
        patch: Binary FilePatch
        original: Current content of the file
        reverse: Apply the reverse data instead of the forward data

    Returns:
        The patched content

    Raises:
        ValueError: If the patch data is invalid or doesn't apply
    """
    from .errors import ApplyDeltaError
    from .pack import apply_delta

    if reverse:
        data, is_delta = patch.binary_old, patch.binary_old_delta
    else:
        data, is_delta = patch.binary_new, patch.binary_new_delta
    if data is None:
        # "Binary files differ" message without actual patch data
        raise NotImplementedError(
            "Binary patch detected but no patch data provided (use git diff --binary)"
        )
    try:
        decoded = zlib.decompress(git_base85_decode(data))
    except (ValueError, zlib.error) as e:
        raise ValueError(f"Failed to decode binary patch: {e}")
    if not is_delta:
        return decoded
    try:
        return b"".join(apply_delta(original, decoded))
    except ApplyDeltaError as e:
        raise ValueError(f"Binary delta does not apply: {e}")


def apply_patches(
    r: "Repo",
    patches: list[FilePatch],
    cached: bool = False,
    reverse: bool = False,
    check: bool = False,
    strip: int = 1,
    three_way: bool = False,
    *,
    config: "Config | None" = None,
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
        config: Repository configuration. If None, falls back to
            ``r.get_config_stack()``.

    Raises:
        ValueError: If patch cannot be applied
    """
    from .index import (
        IndexEntry,
        index_entry_from_stat,
    )

    if config is None:
        config = r.get_config_stack()

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
        repo_path_bytes = r.path.encode("utf-8") if isinstance(r.path, str) else r.path
        fs_path = _validate_patch_target(r, repo_path_bytes, file_path)

        # Handle renames and copies
        original_lines: list[bytes] | None = None
        old_mode: int | None = None
        if patch.rename_from is not None and patch.rename_to is not None:
            original_lines, old_mode, should_continue = _apply_rename_or_copy(
                r,
                patch.rename_from,
                patch.rename_to,
                strip,
                patch,
                is_rename=True,
                cached=cached,
                check=check,
                config=config,
            )
            if should_continue:
                continue
        elif patch.copy_from is not None and patch.copy_to is not None:
            original_lines, old_mode, should_continue = _apply_rename_or_copy(
                r,
                patch.copy_from,
                patch.copy_to,
                strip,
                patch,
                is_rename=False,
                cached=cached,
                check=check,
                config=config,
            )
            if should_continue:
                continue

        # Read original file content (unless already loaded from rename/copy)
        if original_lines is None:
            if old_path is None:
                # New file
                if not cached:
                    _refuse_existing_target(fs_path, tree_path)
                original_lines = []
            else:
                current = _load_patch_target(
                    r, fs_path, tree_path, patch.old_mode, config
                )
                if current is None:
                    original_lines = []
                else:
                    content, old_mode = current
                    original_lines = content.splitlines(keepends=True)
        new_mode = patch.new_mode or old_mode or patch.old_mode or 0o100644

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
        result: list[bytes] | None
        # Deletions don't need the binary data, so treat those like text.
        if patch.binary and new_path is not None:
            result = _apply_binary_patch(
                patch, b"".join(original_lines), reverse
            ).splitlines(keepends=True)
        else:
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

        if new_path is None:
            # File deletion
            if not cached and os.path.lexists(fs_path):
                os.remove(fs_path)
            # Remove from index
            index = r.open_index(config=config)
            if tree_path in index:
                del index[tree_path]
                index.write()
        else:
            # File addition or modification
            index = r.open_index(config=config)
            blob = Blob.from_string(result_content)
            r.object_store.add_object(blob)

            if not cached:
                st = _write_patch_target(fs_path, result_content, new_mode, config)
                entry = index_entry_from_stat(st, blob.id, mode=new_mode)
            else:
                # Create a minimal index entry for cached-only changes
                entry = IndexEntry(
                    ctime=(0, 0),
                    mtime=(0, 0),
                    dev=0,
                    ino=0,
                    mode=new_mode,
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

                if not cached and os.path.lexists(old_fs_path):
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
