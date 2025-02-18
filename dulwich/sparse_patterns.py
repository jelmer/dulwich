# sparse_patterns.py -- Sparse checkout pattern handling.
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Sparse checkout pattern handling."""

import fnmatch
import os
import re

from .file import ensure_dir_exists
from .repo import Repo


def determine_included_paths(repo, lines, cone):
    """Dispatches to either a full-pattern match or a cone-mode approach,
    returning a set of paths (strings) that should be included.
    """
    if cone:
        return compute_included_paths_cone(repo, lines)
    else:
        return compute_included_paths_full(repo, lines)


def compute_included_paths_full(repo, lines):
    """Uses the existing .gitignore-style parsing and matching.
    Every path in the index is tested against these patterns,
    and included if it matches a final 'positive' pattern.
    """
    parsed = parse_sparse_patterns(lines)
    index = repo.open_index()
    included = set()
    for path_bytes, entry in index.items():
        path_str = path_bytes.decode("utf-8")
        # For .gitignore logic, match_gitignore_patterns returns True if 'included'
        if match_gitignore_patterns(path_str, parsed, path_is_dir=False):
            included.add(path_str)
    return included


def compute_included_paths_cone(repo, lines):
    """A simplified "cone" approach:
      - '/*' => include top-level files
      - '!/*/' => exclude all subdirectories by default
      - '!/some/dir/' => re-include directory "some/dir" (and everything under it)
    We look for these special lines and then decide which index paths are included.
    Real Git uses more sophisticated logic pairing "recursive" vs. "parent" patterns,
    but this demonstrates the basic concept.
    """
    include_top_level = False
    exclude_subdirs = False
    reinclude_dirs = set()

    for pat in lines:
        if pat == "/*":
            include_top_level = True
        elif pat == "!/*/":
            exclude_subdirs = True
        elif pat.startswith("!/"):
            # strip leading '!/' and trailing '/'
            d = pat[2:].rstrip("/")
            if d:
                reinclude_dirs.add(d)

    index = repo.open_index()
    included = set()

    for path_bytes, entry in index.items():
        path_str = path_bytes.decode("utf-8")

        # Check if this is top-level (no slash) or which top_dir it belongs to
        if "/" not in path_str:
            # top-level file
            if include_top_level:
                included.add(path_str)
            continue

        top_dir = path_str.split("/", 1)[0]
        if exclude_subdirs:
            # subdirs are excluded unless they appear in reinclude_dirs
            if top_dir in reinclude_dirs:
                included.add(path_str)
        else:
            # if we never set exclude_subdirs, we might include everything by default
            # or handle partial subdir logic. For now, let's assume everything is included
            included.add(path_str)

    return included


def apply_included_paths(repo, included_paths, force=False):
    """Given a set of included paths, update skip-worktree bits in the index
    and apply the changes to the working tree. If force=False, do not remove
    locally modified files from excluded sets.
    """
    index = repo.open_index()
    normalizer = repo.get_blob_normalizer()

    def local_modifications_exist(full_path, index_entry):
        if not os.path.exists(full_path):
            return False
        try:
            with open(full_path, "rb") as f:
                disk_data = f.read()
        except OSError:
            return True
        try:
            blob = repo.object_store[index_entry.sha]
        except KeyError:
            return True
        norm_data = normalizer.checkin_normalize(disk_data, full_path)
        return norm_data != blob.data

    # 1) Update skip-worktree bits
    for path_bytes, entry in list(index.items()):
        path_str = path_bytes.decode("utf-8")
        if path_str in included_paths:
            entry.set_skip_worktree(False)
        else:
            entry.set_skip_worktree(True)
        index[path_bytes] = entry
    index.write()

    # 2) Reflect changes in the working tree
    for path_bytes, entry in list(index.items()):
        full_path = os.path.join(repo.path, path_bytes.decode("utf-8"))

        if entry.skip_worktree:
            # Excluded => remove if safe
            if os.path.exists(full_path):
                if not force and local_modifications_exist(full_path, entry):
                    raise CheckoutError(
                        f"Local modifications in {full_path} would be overwritten "
                        "by sparse checkout. Use force=True to override."
                    )
                try:
                    os.remove(full_path)
                except IsADirectoryError:
                    pass
                except FileNotFoundError:
                    pass
        else:
            # Included => materialize if missing
            if not os.path.exists(full_path):
                try:
                    blob = repo.object_store[entry.sha]
                except KeyError:
                    raise Error(f"Blob {entry.sha} not found for {path_bytes}.")
                ensure_dir_exists(os.path.dirname(full_path))
                with open(full_path, "wb") as f:
                    f.write(blob.data)


class ParsedPattern:
    """Represents one gitignore-style pattern (for sparse checkout).

    We store:
      - is_neg: whether the pattern is negative (leading '!')
      - is_dir_only: whether it ends with '/'
      - anchored: whether it starts with '/'
      - regex: a compiled regex object to match against a path
    """

    __slots__ = (
        "anchored",
        "is_dir_only",
        "is_neg",
        "regex",
    )

    def __init__(self, is_neg, is_dir_only, anchored, regex):
        self.is_neg = is_neg
        self.is_dir_only = is_dir_only
        self.anchored = anchored
        self.regex = regex


def fnmatch_to_regex(pat):
    """Translate a wildcard pattern (with * ? **) into a Python regex.
    This is somewhat like 'fnmatch.translate', but we also handle '**' for multi-dir.
    We'll keep slash as a special char that *never* matches with single '*'.
    """
    i = 0
    res = []
    while i < len(pat):
        c = pat[i]
        if c == "*":
            # check if next char is also '*'
            if (i + 1) < len(pat) and pat[i + 1] == "*":
                # this is a '**'
                # In Git, '**' means match zero or more directories.
                # We'll translate that to '(.+)?' across path segments, but we have to allow
                # skipping over multiple subdirs, so let's do something like:
                # '((?:[^/]+/)*)?' => zero or more <non-slash>+<slash> groups
                res.append(
                    "(?:.*)"
                )  # a simpler approach: match anything including slashes
                i += 2
            else:
                # single '*'
                # match any number of non-slash chars
                res.append("[^/]*")
                i += 1
        elif c == "?":
            # match exactly one non-slash character
            res.append("[^/]")
            i += 1
        else:
            # escape regex special chars, except slash we keep as slash
            if c in ".^$+{}[]()|\\":
                res.append("\\")
            res.append(c)
            i += 1
    # We'll match entire string if the 'line' is to match a full path component
    # but in gitignore, partial component match is allowed. We'll handle boundary logic later.
    return "^" + "".join(res) + "$"


def parse_sparse_patterns(lines):
    """Parse the lines from .git/info/sparse-checkout (or a .gitignore-like file)
    into a structure we can use for match_gitignore_patterns.

    This simplified parser:
      1. Strips comments (#...) and empty lines.
      2. Returns a list of (pattern, is_negation, is_dir_only, anchored) tuples.

    Example:
      line = "/*.txt"
        -> ("/.txt", False, False, True)
      line = "!/docs/"
        -> ("/docs/", True, True, True)
      line = "mydir/"
        -> ("mydir/", False, True, False)  # not anchored since no leading "/"
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

        # If pattern ends with '/', we consider it directory-only
        # (like "docs/"). Real Git might treat it slightly differently,
        # but we'll simplify and mark it as "dir_only" if it ends in "/".
        dir_only = False
        if line.endswith("/"):
            dir_only = True
            line = line[:-1]

        results.append((line, negation, dir_only, anchored))
    return results


def match_gitignore_patterns(path_str, parsed_patterns, path_is_dir=False):
    """Determine whether path_str is "included" based on .gitignore-style logic.
    This is a simplified approach that:
      1. Iterates over patterns in order.
      2. If a pattern matches, we set the "include" state depending on negation.
      3. Later matches override earlier ones.

    In a .gitignore sense, lines that do not start with '!' are "ignore" patterns,
    lines that start with '!' are "unignore" (re-include). But in sparse checkout,
    it's effectively reversed: a non-negation line is "include," negation is "exclude."
    However, many flows still rely on the same final logic: the last matching pattern
    decides "excluded" vs. "included."

    We'll interpret "include" as returning True, "exclude" as returning False.

    Because real Git uses more specialized code in cone mode, this is primarily used
    in full-pattern (non-cone) mode. If you want to replicate Git exactly, you'll need
    to handle anchored patterns, wildcards, directory restrictions, etc. precisely.

    Arguments:
      - path_str: "docs/readme.md"
      - parsed_patterns: list of (pattern, negation, dir_only, anchored)
      - path_is_dir: if True, treat path_str as a directory (only relevant if we handle "dir_only")

    Returns:
      True if the final pattern that matches path_str indicates it is included.
      False otherwise.
    """
    # Start by assuming "excluded" (like a .gitignore starts by including everything
    # until matched, but for sparse-checkout we often treat unmatched as "excluded").
    # We will flip if we match an "include" pattern.
    is_included = False

    for pattern, negation, dir_only, anchored in parsed_patterns:
        # If dir_only is True and path_is_dir is False, we skip matching
        # real Git might treat "docs/" pattern as matching "docs/readme.md" indirectly,
        # but let's keep it strictly directory-only for demonstration.
        if dir_only and not path_is_dir:
            # e.g. "docs/" won't match "docs/readme.md" in this naive approach
            continue

        # If anchored is True, pattern should match from the start of path_str.
        # If not anchored, we can match anywhere.
        if anchored:
            # We match from the beginning. For example, pattern = "docs"
            # path_str = "docs/readme.md" -> start is "docs"
            # We'll just do a prefix check or prefix + slash check
            # Or you can do a partial fnmatch. We'll do a manual approach:
            if pattern == "":
                # Means it was just "/", which can happen if line was "/"
                # That might represent top-level only?
                # We'll skip for simplicity or treat it as a special case.
                continue
            elif path_str == pattern:
                matched = True
            elif path_str.startswith(pattern + "/"):
                matched = True
            else:
                matched = False
        else:
            # Not anchored: we can do a simple wildcard match or a substring match.
            # For simplicity, let's use Python's fnmatch:
            matched = fnmatch.fnmatch(path_str, pattern)

        if matched:
            # If negation is True, that means 'exclude'. If negation is False, 'include'.
            # But note that in real .gitignore, negation is "unignore." For sparse-checkout,
            # it can be reversed. We'll keep it direct for demonstration:
            is_included = not negation
            # The last matching pattern overrides, so we continue checking until the end.

    return is_included


def parse_gitignore_line(line):
    """Parse a single line from a .gitignore/.git/info/sparse-checkout file
    into a ParsedPattern that handles:
      - ! negation
      - / anchor
      - trailing / for directory-only
      - wildcard expansion (*, ?, **)
    Returns: ParsedPattern or None if line is blank/comment.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None  # ignore empty or comment lines

    # 1) Check negation
    is_neg = False
    if line.startswith("!"):
        is_neg = True
        line = line[1:]  # remove '!'
        line = line.lstrip()  # in case there's a space, not typical but let's be safe

    # 2) Check anchored
    anchored = False
    if line.startswith("/"):
        anchored = True
        line = line[1:]  # remove leading slash

    # 3) Check if directory-only (trailing slash)
    is_dir_only = False
    if line.endswith("/"):
        is_dir_only = True
        line = line[:-1]  # remove trailing slash

    # 4) Convert the wildcard expression to a regex
    #    We'll do a step-by-step approach:
    #    - Escape regex special chars
    #    - Re-introduce our wildcards (*, ?, **)
    #    - Handle anchored vs non-anchored
    #    - If is_dir_only, ensure we match a directory (we'll check for a path
    #      that either equals or continues with a slash).
    regex_str = fnmatch_to_regex(line)

    # For anchored patterns, we want to match from the start of path (after rewriting
    # it to a canonical slash-based path). For non-anchored, we allow match at any
    # slash boundary. We'll implement a simplified approach:
    #    anchored => pattern must match from start
    #    non-anchored => we allow '.*' or '(^|/)somepattern($|/)'
    # Git is more subtle, but let's approximate:

    # We'll embed this in a bigger pattern that tries to find a match on a path
    # boundary. For directory-only, we might require a match that extends to end or slash.
    # We'll refine in the matching function instead.

    # We'll store the raw transformed pattern but actual anchoring logic is handled
    # in the final matching step.

    pat = ParsedPattern(
        is_neg=is_neg,
        is_dir_only=is_dir_only,
        anchored=anchored,
        regex=re.compile(regex_str),
    )
    return pat
