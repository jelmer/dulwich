# range_diff.py -- Compare two ranges of commits
# Copyright (C) 2026 Jelmer Vernooĳ <jelmer@jelmer.uk>
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

"""Compare two ranges of commits, like ``git range-diff``.

``git range-diff`` compares two ranges of commits (for example two versions
of a patch series or a branch before and after a rebase) and shows how the
commits and their diffs evolved.

The implementation follows git's approach: for each commit in both ranges a
patch (diff against its first parent) is computed.  Commits from the two
ranges are then matched up by finding the assignment that minimizes the total
size of the "diff of diffs" between matched patches, with unmatched commits
charged a cost derived from the size of their own patch.  The result is
rendered as a list of correspondences with a diff of the patches for commits
that changed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from types import ModuleType
from typing import TYPE_CHECKING

from .objects import Commit, Tag
from .objectspec import parse_commit
from .patch import shortid, unified_diff, write_commit_diff

if TYPE_CHECKING:
    from .object_store import BaseObjectStore
    from .objects import ObjectID
    from .repo import BaseRepo

    # A revision that can be resolved to a commit: a textual revision or
    # object id, or an already-parsed Commit or Tag.
    Committish = str | bytes | Commit | Tag

# The default "creation factor" used by git: a commit is only considered a
# candidate match for another commit if the cost of pairing them is below this
# percentage of the cost of leaving them both unmatched.  See git's
# Documentation/git-range-diff.txt and range-diff.c.
DEFAULT_CREATION_FACTOR = 60


def _import_munkres() -> ModuleType:
    """Import the optional ``munkres`` dependency.

    Raises:
      ImportError: If the ``munkres`` package is not installed.
    """
    try:
        import munkres
    except ImportError as exc:
        raise ImportError(
            "the munkres module is required for range-diff. "
            "Install it with: pip install 'dulwich[range_diff]'"
        ) from exc
    return munkres  # type: ignore[no-any-return,unused-ignore]


@dataclass
class RangeDiffEntry:
    """A single correspondence in a range-diff.

    Attributes:
      old_idx: 1-based index of the commit in the first range, or None if the
        commit only exists in the second range.
      new_idx: 1-based index of the commit in the second range, or None if the
        commit only exists in the first range.
      old_commit: Commit object from the first range, or None.
      new_commit: Commit object from the second range, or None.
      status: One of ``=`` (identical patch), ``!`` (different patch),
        ``>`` (only in second range) or ``<`` (only in first range).
      diff: Lines of the diff between the two patches (the "diff of diffs"),
        only populated when status is ``!``.
    """

    old_idx: int | None
    new_idx: int | None
    old_commit: Commit | None
    new_commit: Commit | None
    status: str
    diff: list[bytes]


def commit_patch(
    store: BaseObjectStore, commit: Commit, *, diff_algorithm: str | None = None
) -> bytes:
    """Compute the patch (diff against the first parent) for a commit.

    Args:
      store: Object store to read objects from.
      commit: Commit to compute the patch for.
      diff_algorithm: Diff algorithm to use ("myers" or "patience").

    Returns:
      The diff of the commit's tree against its first parent's tree (or the
      empty tree for a root commit) as bytes.
    """
    out = BytesIO()
    write_commit_diff(out, store, commit, diff_algorithm=diff_algorithm)
    return _normalize_patch(out.getvalue())


def _normalize_patch(patch: bytes) -> bytes:
    """Normalize a patch for range-diff comparison.

    This mirrors git's ``read_patches``: volatile parts of the patch that do
    not describe the change itself are removed so that two commits with the
    same logical change compare equal.  In particular the ``index`` line
    (which contains blob hashes) is dropped and ``@@`` hunk headers have their
    line numbers stripped, keeping only the file name as context.  Keeping the
    file name lets the diff-of-diffs show which file a change belongs to.

    Args:
      patch: Raw patch as produced by :func:`write_tree_diff`.

    Returns:
      The normalized patch.
    """
    out: list[bytes] = []
    current_filename = b""
    for line in patch.splitlines(keepends=True):
        stripped = line.rstrip(b"\n")
        if stripped.startswith(b"index "):
            continue
        if stripped.startswith(b"+++ "):
            # Track the post-image file name for hunk headers; "b/<path>".
            name = stripped[4:]
            if name.startswith(b"b/"):
                name = name[2:]
            current_filename = name
            out.append(line)
            continue
        if stripped.startswith(b"@@ "):
            # Drop the volatile line numbers, keep any function context and
            # prefix it with the current file name like git does.
            rest = stripped[3:]
            end = rest.find(b"@@")
            tail = rest[end + 2 :] if end != -1 else b""
            header = b"@@"
            if current_filename:
                header += b" " + current_filename + b":"
            header += tail
            out.append(header + b"\n")
            continue
        out.append(line)
    return b"".join(out)


def _patch_diffsize(patch: bytes) -> int:
    """Return the number of diff content lines in a (normalized) patch.

    This mirrors git's ``util->diffsize``, which counts only the hunk and
    body lines of a patch -- the ``diff --git``, ``---`` and ``+++`` headers
    do not contribute.  It is used to price leaving a commit unmatched.
    """
    count = 0
    for line in patch.splitlines():
        if line.startswith((b"diff --git ", b"--- ", b"+++ ")):
            continue
        count += 1
    return count


def _inner_diff(a: bytes, b: bytes) -> list[bytes]:
    """Render the "diff of diffs" between two patches.

    This follows git's presentation: the ``---``/``+++`` file headers are
    dropped, and each outer ``@@`` hunk header has its (meaningless) line
    numbers replaced by the section it falls in -- the most recent normalized
    ``@@ <file>:`` line of the patches.  The remaining lines are the content
    of the two patches prefixed with the outer diff marker (space, ``-`` or
    ``+``), so the reader sees how one patch turned into the other.

    The number of lines returned is also git's ``diffsize`` cost used to match
    commits up; see :func:`_diff_size`.

    Args:
      a: First (normalized) patch.
      b: Second (normalized) patch.

    Returns:
      A list of byte strings (each terminated with a newline).
    """
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)

    # Map each line index of patch ``a`` to the section header (normalized
    # "@@ <file>:" line) it belongs to, so hunk headers can be annotated.
    section_for_line: list[bytes] = []
    current_section = b""
    for line in a_lines:
        section_for_line.append(current_section)
        if line.startswith(b"@@"):
            current_section = line.rstrip(b"\n")

    result: list[bytes] = []
    for line in unified_diff(a_lines, b_lines, n=3):
        if line.startswith((b"--- ", b"+++ ")):
            continue
        if line.startswith(b"@@ "):
            # Replace the line numbers with the section this hunk falls in.
            match = re.match(rb"^@@ -(\d+)", line)
            section = b""
            if match and section_for_line:
                start = int(match.group(1))
                idx = min(max(start - 1, 0), len(section_for_line) - 1)
                section = section_for_line[idx]
            if section:
                result.append(b"@@ " + section[2:].lstrip() + b"\n")
            else:
                result.append(b"@@\n")
            continue
        if not line.endswith(b"\n"):
            line += b"\n"
        result.append(line)
    return result


def _diff_size(a: bytes, b: bytes) -> int:
    """Return git's ``diffsize`` cost of the diff between two patches.

    This is the number of lines in the rendered diff-of-diffs (see
    :func:`_inner_diff`), which git uses to decide how similar two commits
    are when matching the two ranges up.
    """
    return len(_inner_diff(a, b))


def _solve_assignment(cost: list[list[int]]) -> list[int]:
    """Solve a minimum cost assignment problem.

    Uses the Hungarian (Kuhn-Munkres) algorithm from the ``munkres`` package.

    Args:
      cost: A list of ``n`` rows, each a list of ``m`` costs.

    Returns:
      A list of length ``n`` where element ``i`` is the column assigned to
      row ``i`` (in ``range(m)``), or ``-1`` if the row was not assigned a
      column.

    Raises:
      ImportError: If the ``munkres`` package is not installed.
    """
    munkres = _import_munkres()
    n = len(cost)
    if n == 0:
        return []
    # munkres mutates its input, so hand it a copy.
    pairs = munkres.Munkres().compute([row[:] for row in cost])
    result = [-1] * n
    for row, col in pairs:
        result[row] = col
    return result


def range_diff_commits(
    store: BaseObjectStore,
    old_commits: list[Commit],
    new_commits: list[Commit],
    *,
    creation_factor: int = DEFAULT_CREATION_FACTOR,
    diff_algorithm: str | None = None,
) -> list[RangeDiffEntry]:
    """Compute the range-diff between two lists of commits.

    Args:
      store: Object store to read objects from.
      old_commits: Commits in the first range, oldest first.
      new_commits: Commits in the second range, oldest first.
      creation_factor: Percentage used to decide whether two commits are
        similar enough to be considered a match (see git's
        ``--creation-factor``).
      diff_algorithm: Diff algorithm to use ("myers" or "patience").

    Returns:
      A list of :class:`RangeDiffEntry` objects in display order (second-range
      order, with deleted commits inserted after their predecessors).

    Raises:
      ImportError: If the ``munkres`` package is not installed.
    """
    # Fail fast before computing any patches if the dependency is missing.
    _import_munkres()
    old_patches = [
        commit_patch(store, c, diff_algorithm=diff_algorithm) for c in old_commits
    ]
    new_patches = [
        commit_patch(store, c, diff_algorithm=diff_algorithm) for c in new_commits
    ]

    a = len(old_commits)
    b = len(new_commits)

    # old_to_new[i] = j if old commit i matches new commit j, else None.
    old_to_new: list[int | None] = [None] * a
    new_to_old: dict[int, int] = {}

    # First, match commits whose patches are byte-for-byte identical, like
    # git's find_exact_matches().  This keeps obviously unchanged commits
    # paired even when other commits would otherwise compete for them.
    new_by_patch: dict[bytes, list[int]] = {}
    for j, patch in enumerate(new_patches):
        new_by_patch.setdefault(patch, []).append(j)
    for i, patch in enumerate(old_patches):
        candidates = new_by_patch.get(patch)
        while candidates:
            j = candidates.pop(0)
            if j not in new_to_old:
                old_to_new[i] = j
                new_to_old[j] = i
                break

    # Then assign the remaining commits by minimizing the total diff-of-diffs
    # size, following git's get_correspondences().
    if a and b:
        # Cost of leaving a commit unmatched: its own patch size scaled by the
        # creation factor (a dummy slot in git's get_correspondences()).  An
        # already exact-matched commit must keep its match, so its dummy slot
        # is forbidden (cost_max).
        cost_max = 1 + 2 * sum(_patch_diffsize(p) for p in old_patches + new_patches)
        old_drop = [
            cost_max
            if old_to_new[i] is not None
            else 2 * _patch_diffsize(p) * creation_factor // 100
            for i, p in enumerate(old_patches)
        ]
        new_drop = [
            cost_max
            if j in new_to_old
            else 2 * _patch_diffsize(p) * creation_factor // 100
            for j, p in enumerate(new_patches)
        ]
        n = a + b

        def pair_cost(i: int, j: int) -> int:
            # Costs are scaled by 2 so a real pairing can be made one unit
            # cheaper than its scaled value, breaking ties in favor of
            # matching: "2*dos - 1" beats two dummies "2*(dropi + dropj)"
            # whenever dos <= dropi + dropj, as git does.
            if old_to_new[i] == j:
                return 0
            if old_to_new[i] is None and j not in new_to_old:
                return 2 * _diff_size(old_patches[i], new_patches[j]) - 1
            return cost_max

        # The cost matrix is square (a + b): the lower-right quadrant is the
        # commit-to-commit costs padded with free dummy-to-dummy slots.
        cost = [[0] * n for _ in range(n)]
        for i in range(a):
            for j in range(b):
                cost[i][j] = pair_cost(i, j)
            for j in range(b, n):
                cost[i][j] = old_drop[i]
        for j in range(b):
            for i in range(a, n):
                cost[i][j] = new_drop[j]

        assignment = _solve_assignment(cost)
        for i in range(a):
            j = assignment[i]
            if 0 <= j < b and old_to_new[i] is None and j not in new_to_old:
                old_to_new[i] = j
                new_to_old[j] = i

    entries: list[RangeDiffEntry] = []

    def emit(oi: int | None, ni: int | None, status: str, diff: list[bytes]) -> None:
        entries.append(
            RangeDiffEntry(
                old_idx=None if oi is None else oi + 1,
                new_idx=None if ni is None else ni + 1,
                old_commit=None if oi is None else old_commits[oi],
                new_commit=None if ni is None else new_commits[ni],
                status=status,
                diff=diff,
            )
        )

    # Interleave the two ranges in the same order as git's output(): walk both
    # lists with two cursors, emitting unmatched old commits, then unmatched
    # new commits, then the matched pair, repeating until both are exhausted.
    shown_old: set[int] = set()
    i = 0
    j = 0
    while i < a or j < b:
        # Skip old commits that were already shown as part of a pair.
        while i < a and i in shown_old:
            i += 1

        # An unmatched old commit whose predecessors have all been shown.
        if i < a and old_to_new[i] is None:
            emit(i, None, "<", [])
            i += 1
            continue

        # Any run of unmatched new commits.
        while j < b and j not in new_to_old:
            emit(None, j, ">", [])
            j += 1

        # A matched pair (the new commit at j is matched to some old commit).
        if j < b:
            oi = new_to_old[j]
            if old_patches[oi] == new_patches[j]:
                emit(oi, j, "=", [])
            else:
                emit(oi, j, "!", _inner_diff(old_patches[oi], new_patches[j]))
            shown_old.add(oi)
            j += 1

    return entries


def _commit_list(repo: BaseRepo, base: ObjectID, tip: ObjectID) -> list[Commit]:
    """Return commits reachable from ``tip`` but not ``base``, oldest first."""
    walker = repo.get_walker(include=[tip], exclude=[base])
    return [entry.commit for entry in walker][::-1]


def format_range_diff(entries: list[RangeDiffEntry]) -> list[bytes]:
    """Render range-diff entries to a list of output lines (with newlines).

    Args:
      entries: Entries as produced by :func:`range_diff_commits`.

    Returns:
      A list of byte strings, each terminated with a newline.
    """
    old_width = max(len(str(sum(e.old_idx is not None for e in entries))), 1)
    new_width = max(len(str(sum(e.new_idx is not None for e in entries))), 1)

    def fmt_idx(idx: int | None, width: int) -> str:
        return ("-" if idx is None else str(idx)).rjust(width)

    def short(commit: Commit | None) -> str:
        return "-" * 7 if commit is None else shortid(commit.id).decode("ascii")

    def subject(commit: Commit) -> str:
        lines = commit.message.decode("utf-8", errors="replace").splitlines()
        return lines[0] if lines else ""

    lines: list[bytes] = []
    for entry in entries:
        commit = entry.new_commit or entry.old_commit
        assert commit is not None
        header = (
            f"{fmt_idx(entry.old_idx, old_width)}:  {short(entry.old_commit)} "
            f"{entry.status} "
            f"{fmt_idx(entry.new_idx, new_width)}:  {short(entry.new_commit)} "
            f"{subject(commit)}"
        )
        lines.append(header.encode("utf-8") + b"\n")
        # _inner_diff already terminates each line with a newline.
        lines.extend(b"    " + diff_line for diff_line in entry.diff)
    return lines


def range_diff(
    repo: BaseRepo,
    old_base: Committish,
    old_tip: Committish,
    new_base: Committish,
    new_tip: Committish,
    *,
    creation_factor: int = DEFAULT_CREATION_FACTOR,
    diff_algorithm: str | None = None,
) -> list[RangeDiffEntry]:
    """Compute the range-diff between two commit ranges.

    The four endpoints may each be given as a textual revision or object id,
    or as a Commit or Tag object.

    Args:
      repo: Repository to read from.
      old_base: Base (exclusive) of the first range.
      old_tip: Tip (inclusive) of the first range.
      new_base: Base (exclusive) of the second range.
      new_tip: Tip (inclusive) of the second range.
      creation_factor: Percentage used to decide whether two commits match.
      diff_algorithm: Diff algorithm to use ("myers" or "patience").

    Returns:
      A list of :class:`RangeDiffEntry` objects in display order.
    """
    old_base_id = parse_commit(repo, old_base).id
    old_tip_id = parse_commit(repo, old_tip).id
    new_base_id = parse_commit(repo, new_base).id
    new_tip_id = parse_commit(repo, new_tip).id
    old_commits = _commit_list(repo, old_base_id, old_tip_id)
    new_commits = _commit_list(repo, new_base_id, new_tip_id)
    return range_diff_commits(
        repo.object_store,
        old_commits,
        new_commits,
        creation_factor=creation_factor,
        diff_algorithm=diff_algorithm,
    )
