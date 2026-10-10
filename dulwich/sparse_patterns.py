# sparse_patterns.py -- Sparse checkout pattern handling.
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Sparse checkout pattern handling."""

__all__ = [
    "BlobNotFoundError",
    "SparseCheckoutConflictError",
    "apply_included_paths",
    "compute_included_paths_cone",
    "compute_included_paths_full",
    "determine_included_paths",
    "match_sparse_patterns",
    "parse_sparse_patterns",
    "sparse_path_matcher",
]

import functools
import os
from collections.abc import Callable, Sequence, Set
from typing import TYPE_CHECKING

from .file import ensure_dir_exists
from .ignore import Pattern
from .index import (
    Index,
    IndexEntry,
    _remove_empty_parents,
    build_file_from_blob,
    get_path_element_validator,
    get_symlink_fn,
    verify_leading_dirs,
    verify_tree_path,
)
from .objects import Blob
from .repo import Repo

if TYPE_CHECKING:
    from .config import Config


class SparseCheckoutConflictError(Exception):
    """Raised when local modifications would be overwritten by a sparse checkout operation."""


class BlobNotFoundError(Exception):
    """Raised when a requested blob is not found in the repository's object store."""


def determine_included_paths(
    index: Index, lines: Sequence[str], cone: bool, *, ignorecase: bool = False
) -> set[str]:
    """Determine which paths in the index should be included based on either a full-pattern match or a cone-mode approach.

    Args:
      index: An Index object containing the repository's index.
      lines: A list of pattern lines (strings) from sparse-checkout config.
      cone: A bool indicating cone mode.
      ignorecase: Whether to match paths case-insensitively, as with
        ``core.ignorecase``.

    Returns:
      A set of included path strings.
    """
    if cone:
        return compute_included_paths_cone(index, lines, ignorecase=ignorecase)
    else:
        return compute_included_paths_full(index, lines, ignorecase=ignorecase)


def compute_included_paths_full(
    index: Index, lines: Sequence[str], *, ignorecase: bool = False
) -> set[str]:
    """Use .gitignore-style parsing and matching to determine included paths.

    Each file path in the index is tested against the parsed sparse patterns.
    If it matches the final (most recently applied) positive pattern, it is included.

    Args:
      index: An Index object containing the repository's index.
      lines: A list of pattern lines (strings) from sparse-checkout config.
      ignorecase: Whether to match paths case-insensitively.

    Returns:
      A set of included path strings.
    """
    matcher = sparse_path_matcher(lines, cone=False, ignorecase=ignorecase)
    return {path for path in (p.decode("utf-8") for p in index) if matcher(path)}


def compute_included_paths_cone(
    index: Index, lines: Sequence[str], *, ignorecase: bool = False
) -> set[str]:
    """Implement a simplified 'cone' approach for sparse-checkout.

    By default, this can include top-level files, exclude all subdirectories,
    and re-include specified directories. The logic is simpler than Git's
    built-in cone mode (recursive vs parent) and is essentially an
    implementation of the recursive cone mode.

    Args:
      index: An Index object containing the repository's index.
      lines: A list of pattern lines (strings), typically including entries like
        "/*", "!/*/", or "/mydir/".
      ignorecase: Whether to compare directory names case-insensitively.

    Returns:
      A set of included path strings.
    """
    matcher = sparse_path_matcher(lines, cone=True, ignorecase=ignorecase)
    return {path for path in (p.decode("utf-8") for p in index) if matcher(path)}


def sparse_path_matcher(
    lines: Sequence[str], cone: bool, *, ignorecase: bool = False
) -> Callable[[str], bool]:
    """Build a predicate that tells whether a file path is in the sparse checkout.

    Args:
      lines: A list of pattern lines (strings) from sparse-checkout config.
      cone: A bool indicating cone mode.
      ignorecase: Whether to match paths case-insensitively, as with
        ``core.ignorecase``.

    Returns:
      A function taking a slash-separated file path and returning True if it
      should be present in the working tree.
    """
    if not cone:
        parsed = parse_sparse_patterns(lines)
        return lambda path: match_sparse_patterns(
            path, parsed, path_is_dir=False, ignorecase=ignorecase
        )

    include_top_level = False
    exclude_subdirs = False
    reinclude_dirs = set()

    for pat in lines:
        if pat == "/*":
            include_top_level = True
        elif pat == "!/*/":
            exclude_subdirs = True
        elif pat.startswith("/"):
            # strip leading '/' and trailing '/'
            d = pat.strip("/")
            if d:
                reinclude_dirs.add(_ascii_lower(d) if ignorecase else d)

    def matches_cone(path: str) -> bool:
        # Check if this is top-level (no slash) or which top_dir it belongs to
        if "/" not in path:
            return include_top_level
        if exclude_subdirs:
            # subdirs are excluded unless they appear in reinclude_dirs
            top_dir = path.split("/", 1)[0]
            if ignorecase:
                top_dir = _ascii_lower(top_dir)
            return top_dir in reinclude_dirs
        # if we never set exclude_subdirs, we might include everything by default
        # or handle partial subdir logic. For now, let's assume everything is included
        return True

    return matches_cone


def apply_included_paths(
    repo: Repo,
    included_paths: Set[str],
    force: bool = False,
    *,
    config: "Config | None" = None,
) -> None:
    """Apply the sparse-checkout inclusion set to the index and working tree.

    This function updates skip-worktree bits in the index based on whether each
    path is included or not. It then adds or removes files in the working tree
    accordingly. If ``force=False``, files that have local modifications
    will cause an error instead of being removed.

    Args:
      repo: A path to the repository or a Repo object.
      included_paths: A set of paths (strings) that should remain included.
      force: Whether to forcibly remove locally modified files (default False).
      config: Repository configuration. If None, falls back to
        ``repo.get_config_stack()``.

    Returns:
      None

    Raises:
      InvalidPathError: If an index path is unsafe to write, e.g. because it
        contains ``..`` or a leading directory is a symlink.
    """
    if config is None:
        config = repo.get_config_stack()
    index = repo.open_index(config=config)
    normalizer = repo.get_blob_normalizer(config=config)
    honor_filemode = config.get_boolean(b"core", b"filemode", os.name != "nt")
    validate_path_element = get_path_element_validator(config)
    symlink_fn = get_symlink_fn(config)
    repo_path = os.fsencode(repo.path)

    def local_modifications_exist(full_path: str, index_entry: IndexEntry) -> bool:
        if not os.path.exists(full_path):
            return False
        with open(full_path, "rb") as f:
            disk_data = f.read()
        try:
            blob_obj = repo.object_store[index_entry.sha]
        except KeyError:
            return True
        disk_blob = Blob.from_string(disk_data)
        norm_blob = normalizer.checkin_normalize(disk_blob, full_path.encode("utf-8"))
        norm_data = norm_blob.data
        if not isinstance(blob_obj, Blob):
            return True
        return bool(norm_data != blob_obj.data)

    # 1) Update skip-worktree bits

    # The index may have been populated from an untrusted tree, so check
    # every path before changing anything.
    safe_prefix: list[bytes] = []
    for path_bytes, entry in list(index.items()):
        verify_tree_path(path_bytes, validate_path_element, safe_prefix, repo_path)
        if not isinstance(entry, IndexEntry):
            continue  # Skip conflicted entries
        path_str = path_bytes.decode("utf-8")
        if path_str in included_paths:
            entry.set_skip_worktree(False)
        else:
            entry.set_skip_worktree(True)
        index[path_bytes] = entry
    index.write()

    # 2) Reflect changes in the working tree
    safe_prefix = []
    for path_bytes, entry in list(index.items()):
        if not isinstance(entry, IndexEntry):
            continue  # Skip conflicted entries
        # Check again, as an earlier entry may have been checked out as a
        # symlink.
        verify_leading_dirs(path_bytes, safe_prefix, repo_path)
        full_path = os.path.join(repo.path, path_bytes.decode("utf-8"))

        if entry.skip_worktree:
            # Excluded => remove if safe
            if os.path.exists(full_path):
                if not force and local_modifications_exist(full_path, entry):
                    raise SparseCheckoutConflictError(
                        f"Local modifications in {full_path} would be overwritten "
                        "by sparse checkout. Use force=True to override."
                    )
                try:
                    os.remove(full_path)
                except IsADirectoryError:
                    pass
                except FileNotFoundError:
                    pass
                except PermissionError:
                    if not force:
                        raise
                else:
                    _remove_empty_parents(os.fsencode(full_path), repo_path)
        else:
            # Included => materialize if missing
            if not os.path.exists(full_path):
                try:
                    blob = repo.object_store[entry.sha]
                except KeyError:
                    raise BlobNotFoundError(
                        f"Blob {entry.sha.hex()} not found for {path_bytes.decode('utf-8')}."
                    )
                ensure_dir_exists(os.path.dirname(full_path))
                # Apply checkout normalization if normalizer is available
                if normalizer and isinstance(blob, Blob):
                    blob = normalizer.checkout_normalize(blob, path_bytes)

                if isinstance(blob, Blob):
                    build_file_from_blob(
                        blob,
                        entry.mode,
                        os.fsencode(full_path),
                        honor_filemode=honor_filemode,
                        symlink_fn=symlink_fn,
                    )


def parse_sparse_patterns(lines: Sequence[str]) -> list[tuple[str, bool, bool, bool]]:
    """Parse pattern lines from a sparse-checkout file (.git/info/sparse-checkout).

    This simplified parser:
      1. Strips comments (#...) and empty lines.
      2. Returns a list of (pattern, is_negation, is_dir_only, anchored) tuples.

    These lines are similar to .gitignore patterns but are used for sparse-checkout
    logic. This function strips comments and blank lines, identifies negation,
    anchoring, and directory-only markers, and returns data suitable for matching.

    Example:
      ``line = "*.txt" -> ("*.txt", False, False, False)`` not negated/dir/anchored
      ``line = "/*.txt" -> ("*.txt", False, False, True)`` anchored, not negated/dir
      ``line = "!/*.txt" -> ("*.txt", False, False, True)`` anchored/negated, not dir
      ``line = "!/mydir/" -> ("mydir", True, True, True)`` anchored/negated/dir

    Args:
      lines: A list of raw lines (strings) from the sparse-checkout file.

    Returns:
      A list of tuples (pattern, negation, dir_only, anchored), representing
      the essential details needed to perform matching.
    """
    results = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue  # ignore comments and blank lines

        negation = line.startswith("!")
        if negation:
            line = line[1:]  # remove leading '!'

        anchored = line.startswith("/")
        if anchored:
            line = line[1:]  # remove leading '/'

        # If pattern ends with '/', we consider it directory-only (like "docs/").
        dir_only = line.endswith("/")
        if dir_only:
            line = line[:-1]

        results.append((line, negation, dir_only, anchored))
    return results


def match_sparse_patterns(
    path_str: str,
    parsed_patterns: Sequence[tuple[str, bool, bool, bool]],
    path_is_dir: bool = False,
    *,
    ignorecase: bool = False,
) -> bool:
    """Check whether a path is included based on .gitignore-style patterns.

    Patterns use the same syntax and matching rules as .gitignore, but a
    matching positive pattern includes the path and a negated ('!') one
    excludes it. The last pattern matching the path decides; if none does,
    the nearest parent directory matched by a pattern decides, and paths
    matching nothing are excluded.

    Args:
      path_str: The path (string) to test.
      parsed_patterns: A list of (pattern, negation, dir_only, anchored) tuples
        as returned by parse_sparse_patterns.
      path_is_dir: Whether to treat the path as a directory (default False).
      ignorecase: Whether to match case-insensitively, as with
        ``core.ignorecase``.

    Returns:
      True if the path is included, False otherwise.
    """
    patterns = [
        _compile_sparse_pattern(*parsed, ignorecase) for parsed in parsed_patterns
    ]
    path = path_str.encode("utf-8")
    is_dir = path_is_dir
    # The last pattern matching the path itself decides; if none does, the
    # nearest parent directory that a pattern matches decides.
    while True:
        for pattern in reversed(patterns):
            if pattern.matches(path, is_dir):
                # A positive (non-negated) pattern includes the path.
                return pattern.is_exclude
        if b"/" not in path:
            return False
        path = path.rsplit(b"/", 1)[0]
        is_dir = True


@functools.lru_cache(maxsize=256)
def _compile_sparse_pattern(
    pattern: str, negation: bool, dir_only: bool, anchored: bool, ignorecase: bool
) -> Pattern:
    line = (
        ("!" if negation else "")
        + ("/" if anchored else "")
        + pattern
        + ("/" if dir_only else "")
    )
    return Pattern(line.encode("utf-8"), ignorecase)


def _ascii_lower(name: str) -> str:
    # Only ASCII letters are case-folded.
    return name.encode("utf-8").lower().decode("utf-8")
