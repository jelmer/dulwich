# test_sparse_patterns.py -- Sparse checkout (full and cone mode) pattern handling
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


"""Tests for dulwich.sparse_patterns."""

import os
import shutil
import tempfile
import time

from dulwich.index import IndexEntry
from dulwich.repo import Repo
from dulwich.sparse_patterns import (
    BlobNotFoundError,
    SparseCheckoutConflictError,
    apply_included_paths,
    compute_included_paths_cone,
    compute_included_paths_full,
    determine_included_paths,
    match_gitignore_patterns,
    parse_sparse_patterns,
)

from . import TestCase


class ParseSparsePatternsTests(TestCase):
    """Test parse_sparse_patterns function."""

    def test_empty_and_comment_lines(self):
        lines = [
            "",
            "# comment here",
            "   ",
            "# another comment",
        ]
        parsed = parse_sparse_patterns(lines)
        self.assertEqual(parsed, [])

    def test_simple_patterns(self):
        lines = [
            "*.py",
            "!*.md",
            "/docs/",
            "!/docs/images/",
        ]
        parsed = parse_sparse_patterns(lines)
        self.assertEqual(len(parsed), 4)

        self.assertEqual(parsed[0], ("*.py", False, False, False))  # include *.py
        self.assertEqual(parsed[1], ("*.md", True, False, False))  # exclude *.md
        self.assertEqual(parsed[2], ("docs", False, True, True))  # anchored, dir_only
        self.assertEqual(parsed[3], ("docs/images", True, True, True))

    def test_trailing_slash_dir(self):
        lines = [
            "src/",
        ]
        parsed = parse_sparse_patterns(lines)
        # "src/" => (pattern="src", negation=False, dir_only=True, anchored=False)
        self.assertEqual(parsed, [("src", False, True, False)])

    def test_negation_anchor(self):
        lines = [
            "!/foo.txt",
        ]
        parsed = parse_sparse_patterns(lines)
        # => (pattern="foo.txt", negation=True, dir_only=False, anchored=True)
        self.assertEqual(parsed, [("foo.txt", True, False, True)])


class MatchGitignorePatternsTests(TestCase):
    """Test the match_gitignore_patterns function."""

    def test_no_patterns_returns_excluded(self):
        """If no patterns are provided, by default we treat the path as excluded."""
        self.assertFalse(match_gitignore_patterns("anyfile.py", []))

    def test_last_match_wins(self):
        """Checks that the last pattern to match determines included vs excluded."""
        parsed = parse_sparse_patterns(
            [
                "*.py",  # include
                "!foo.py",  # exclude
            ]
        )
        # "foo.py" matches first pattern => included
        # then matches second pattern => excluded
        self.assertFalse(match_gitignore_patterns("foo.py", parsed))

    def test_dir_only(self):
        """A pattern with a trailing slash should only match directories and subdirectories."""
        parsed = parse_sparse_patterns(["docs/"])
        # Because we set path_is_dir=False, it won't match
        self.assertTrue(
            match_gitignore_patterns("docs/readme.md", parsed, path_is_dir=False)
        )
        self.assertTrue(match_gitignore_patterns("docs", parsed, path_is_dir=True))
        # Even if the path name is "docs", if it's a file, won't match:
        self.assertFalse(match_gitignore_patterns("docs", parsed, path_is_dir=False))

    def test_anchored(self):
        """Anchored patterns match from the start of the path only."""
        parsed = parse_sparse_patterns(["/foo"])
        self.assertTrue(match_gitignore_patterns("foo", parsed))
        # But "some/foo" doesn't match because anchored requires start
        self.assertFalse(match_gitignore_patterns("some/foo", parsed))

    def test_unanchored_uses_fnmatch(self):
        parsed = parse_sparse_patterns(["foo"])
        self.assertTrue(match_gitignore_patterns("some/foo", parsed))
        self.assertFalse(match_gitignore_patterns("some/bar", parsed))


class ComputeIncludedPathsFullTests(TestCase):
    """Test compute_included_paths_full using a real ephemeral repo index."""

    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)
        self.repo = Repo.init(self.temp_dir)

    def _add_file_to_index(self, relpath, content=b"test"):
        full = os.path.join(self.temp_dir, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(content)
        # Stage in the index
        self.repo.stage([relpath])

    def test_basic_inclusion_exclusion(self):
        """Given patterns, check correct set of included paths."""
        self._add_file_to_index("foo.py", b"print(1)")
        self._add_file_to_index("bar.md", b"markdown")
        self._add_file_to_index("docs/readme", b"# docs")

        lines = [
            "*.py",  # include all .py
            "!bar.*",  # exclude bar.md
            "docs/",  # include docs dir
        ]
        included = compute_included_paths_full(self.repo, lines)
        self.assertEqual(included, {"foo.py", "docs/readme"})


class ComputeIncludedPathsConeTests(TestCase):
    """Test compute_included_paths_cone with ephemeral repo to see included vs excluded."""

    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)
        self.repo = Repo.init(self.temp_dir)

    def _add_file_to_index(self, relpath, content=b"test"):
        full = os.path.join(self.temp_dir, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(content)
        self.repo.stage([relpath])

    def test_cone_mode_patterns(self):
        """Simpler pattern handling in cone mode.

        Lines in 'cone' style typically look like:
          - /*     -> include top-level
          - !/*/   -> exclude all subdirs
          - /docs/ -> reinclude 'docs' directory
        """
        self._add_file_to_index("topfile", b"hi")
        self._add_file_to_index("docs/readme.md", b"stuff")
        self._add_file_to_index("lib/code.py", b"stuff")

        lines = [
            "/*",
            "!/*/",
            "/docs/",
        ]
        included = compute_included_paths_cone(self.repo, lines)
        # top-level => includes 'topfile'
        # subdirs => excluded, except docs/
        self.assertEqual(included, {"topfile", "docs/readme.md"})

    def test_no_exclude_subdirs(self):
        """If lines never specify '!/*/', we include everything by default."""
        self._add_file_to_index("topfile", b"hi")
        self._add_file_to_index("docs/readme.md", b"stuff")
        self._add_file_to_index("lib/code.py", b"stuff")

        lines = [
            "/*",  # top-level
            "/docs/",  # re-include docs?
        ]
        included = compute_included_paths_cone(self.repo, lines)
        # Because exclude_subdirs was never set, everything is included:
        self.assertEqual(
            included,
            {"topfile", "docs/readme.md", "lib/code.py"},
        )


class DetermineIncludedPathsTests(TestCase):
    """Test the top-level determine_included_paths function."""

    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)
        self.repo = Repo.init(self.temp_dir)

    def _add_file_to_index(self, relpath):
        path = os.path.join(self.temp_dir, relpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(b"data")
        self.repo.stage([relpath])

    def test_full_mode(self):
        self._add_file_to_index("foo.py")
        self._add_file_to_index("bar.md")

        lines = ["*.py", "!bar.*"]
        included = determine_included_paths(self.repo, lines, cone=False)
        self.assertEqual(included, {"foo.py"})

    def test_cone_mode(self):
        self._add_file_to_index("topfile")
        self._add_file_to_index("subdir/anotherfile")

        lines = ["/*", "!/*/"]
        included = determine_included_paths(self.repo, lines, cone=True)
        self.assertEqual(included, {"topfile"})


class ApplyIncludedPathsTests(TestCase):
    """Integration tests for apply_included_paths, verifying skip-worktree bits and file removal."""

    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)
        self.repo = Repo.init(self.temp_dir)
        # For testing local_modifications_exist logic, we'll need the normalizer
        # plus some real content in the object store.

    def _commit_blob(self, relpath, content=b"hello"):
        """Create a blob object in object_store, stage an index entry for it."""
        full = os.path.join(self.temp_dir, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(content)
        self.repo.stage([relpath])
        # Actually commit so the object is in the store
        self.repo.do_commit(message=b"Commit " + relpath.encode())

    def test_set_skip_worktree_bits(self):
        """If a path is not in included_paths, skip_worktree bit is set."""
        self._commit_blob("keep.py", b"print('keep')")
        self._commit_blob("exclude.md", b"# exclude")

        included = {"keep.py"}
        apply_included_paths(self.repo, included_paths=included, force=False)

        idx = self.repo.open_index()
        self.assertIn(b"keep.py", idx)
        self.assertFalse(idx[b"keep.py"].skip_worktree)

        self.assertIn(b"exclude.md", idx)
        self.assertTrue(idx[b"exclude.md"].skip_worktree)

        # Also check that the exclude.md file was removed from the working tree
        exclude_path = os.path.join(self.temp_dir, "exclude.md")
        self.assertFalse(os.path.exists(exclude_path))

    def test_conflict_with_local_modifications_no_force(self):
        """If local modifications exist for an excluded path, raise SparseCheckoutConflictError."""
        self._commit_blob("foo.txt", b"original")

        # Modify foo.txt on disk
        with open(os.path.join(self.temp_dir, "foo.txt"), "ab") as f:
            f.write(b" local changes")

        with self.assertRaises(SparseCheckoutConflictError):
            apply_included_paths(self.repo, included_paths=set(), force=False)

    def test_conflict_with_local_modifications_forced_removal(self):
        """With force=True, we remove local modifications and skip_worktree the file."""
        self._commit_blob("foo.txt", b"original")
        with open(os.path.join(self.temp_dir, "foo.txt"), "ab") as f:
            f.write(b" local changes")

        # This time, pass force=True => file is removed
        apply_included_paths(self.repo, included_paths=set(), force=True)

        # Check skip-worktree in index
        idx = self.repo.open_index()
        self.assertTrue(idx[b"foo.txt"].skip_worktree)
        # Working tree file removed
        self.assertFalse(os.path.exists(os.path.join(self.temp_dir, "foo.txt")))

    def test_materialize_included_file_if_missing(self):
        """If a path is included but missing from disk, we restore it from the blob in the store."""
        self._commit_blob("restored.txt", b"some content")
        # Manually remove the file from the working tree
        os.remove(os.path.join(self.temp_dir, "restored.txt"))

        apply_included_paths(self.repo, included_paths={"restored.txt"}, force=False)
        # Should have re-created "restored.txt" from the blob
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, "restored.txt")))
        with open(os.path.join(self.temp_dir, "restored.txt"), "rb") as f:
            self.assertEqual(f.read(), b"some content")

    def test_blob_not_found_raises(self):
        """If the object store is missing the blob for an included path, raise BlobNotFoundError."""
        # We'll create an entry in the index that references a nonexistent sha
        idx = self.repo.open_index()
        fake_sha = b"ab" * 20
        e = IndexEntry(
            ctime=(int(time.time()), 0),  # ctime (s, ns)
            mtime=(int(time.time()), 0),  # mtime (s, ns)
            dev=0,  # dev
            ino=0,  # ino
            mode=0o100644,  # mode
            uid=0,  # uid
            gid=0,  # gid
            size=0,  # size
            sha=fake_sha,  # sha
            flags=0,  # flags
            extended_flags=0,
        )
        e.set_skip_worktree(False)
        e.sha = fake_sha
        idx[(b"missing_file")] = e
        idx.write()

        with self.assertRaises(BlobNotFoundError):
            apply_included_paths(
                self.repo, included_paths={"missing_file"}, force=False
            )
