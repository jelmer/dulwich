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

import email.parser
import time
from collections.abc import Generator, Sequence
from difflib import SequenceMatcher
from typing import (
    IO,
    TYPE_CHECKING,
    BinaryIO,
    Optional,
    TextIO,
    Union,
)

if TYPE_CHECKING:
    import email.message

    from .object_store import BaseObjectStore

from .objects import S_ISGITLINK, Blob, Commit

FIRST_FEW_BYTES = 8000

DEFAULT_DIFF_ALGORITHM = "myers"


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
    contents: Union[str, bytes],
    progress: tuple[int, int],
    version: Optional[str] = None,
    encoding: Optional[str] = None,
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
    algorithm: Optional[str] = None,
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


def shortid(hexsha: Optional[bytes]) -> bytes:
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


def patch_filename(p: Optional[bytes], root: bytes) -> bytes:
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
    old_file: tuple[Optional[bytes], Optional[int], Optional[bytes]],
    new_file: tuple[Optional[bytes], Optional[int], Optional[bytes]],
    diff_binary: bool = False,
    diff_algorithm: Optional[str] = None,
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

    def content(mode: Optional[int], hexsha: Optional[bytes]) -> Blob:
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
    paths: tuple[Optional[bytes], Optional[bytes]],
    modes: tuple[Optional[int], Optional[int]],
    shas: tuple[Optional[bytes], Optional[bytes]],
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
    old_file: tuple[Optional[bytes], Optional[int], Optional["Blob"]],
    new_file: tuple[Optional[bytes], Optional[int], Optional["Blob"]],
    diff_algorithm: Optional[str] = None,
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

    def lines(blob: Optional["Blob"]) -> list[bytes]:
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
    old_tree: Optional[bytes],
    new_tree: Optional[bytes],
    diff_binary: bool = False,
    diff_algorithm: Optional[str] = None,
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
    f: Union[TextIO, BinaryIO], encoding: Optional[str] = None
) -> tuple["Commit", bytes, Optional[bytes]]:
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
    msg: "email.message.Message", encoding: Optional[str] = None
) -> tuple["Commit", bytes, Optional[bytes]]:
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


def commit_patch_id(store: "BaseObjectStore", commit_id: bytes) -> bytes:
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
