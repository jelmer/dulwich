# test_sparse_patterns.py -- Sparse checkout (full and cone mode) pattern handling
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


"""Tests for dulwich.sparse_patterns."""

import os
import shutil
import tempfile
import time

from dulwich.index import IndexEntry
from dulwich.objects import Blob
from dulwich.repo import Repo
from dulwich.sparse_patterns import (
    BlobNotFoundError,
    SparseCheckoutConflictError,
    apply_included_paths,
    compute_included_paths_cone,
    compute_included_paths_full,
    determine_included_paths,
    match_sparse_patterns,
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

    def test_sparse_pattern_combos(self):
        lines = [
            "*.py",  # Python files anywhere
            "!*.md",  # markdown files anywhere
            "/docs/",  # root docs dir
            "!/docs/images/",  # no root docs/images subdir
            "src/",  # src dir anywhere
            "/*.toml",  # root TOML files
            "!/*.bak",  # no root backup files
            "!data/",  # no data dirs anywhere
        ]
        parsed = parse_sparse_patterns(lines)
        self.assertEqual(len(parsed), 8)

        # Returns a 4-tuple of: (pattern, negation, dir_only, anchored)
        self.assertEqual(parsed[0], ("*.py", False, False, False))  # _,_,_
        self.assertEqual(parsed[1], ("*.md", True, False, False))  # N,_,_
        self.assertEqual(parsed[2], ("docs", False, True, True))  # _,D,A
        self.assertEqual(parsed[3], ("docs/images", True, True, True))  # N,D,A
        self.assertEqual(parsed[4], ("src", False, True, False))  # _,D,_
        self.assertEqual(parsed[5], ("*.toml", False, False, True))  # _,_,A
        self.assertEqual(parsed[6], ("*.bak", True, False, True))  # N,_,A
        self.assertEqual(parsed[7], ("data", True, True, False))  # N,D,_


class MatchSparsePatternsTests(TestCase):
    """Test the match_sparse_patterns function."""

    # def match_sparse_patterns(path_str, parsed_patterns, path_is_dir=False):
    def test_no_patterns_returns_excluded(self):
        """If no patterns are provided, by default we treat the path as excluded."""
        self.assertFalse(match_sparse_patterns("foo.py", [], path_is_dir=False))
        self.assertFalse(match_sparse_patterns("A/", [], path_is_dir=True))
        self.assertFalse(match_sparse_patterns("A/B/", [], path_is_dir=True))
        self.assertFalse(match_sparse_patterns("A/B/bar.md", [], path_is_dir=False))

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
        self.assertFalse(match_sparse_patterns("foo.py", parsed))
        self.assertFalse(match_sparse_patterns("A/foo.py", parsed))
        self.assertFalse(match_sparse_patterns("A/B/foo.py", parsed))
        self.assertTrue(match_sparse_patterns("bar.py", parsed))
        self.assertTrue(match_sparse_patterns("A/bar.py", parsed))
        self.assertTrue(match_sparse_patterns("A/B/bar.py", parsed))
        self.assertFalse(match_sparse_patterns("bar.md", parsed))
        self.assertFalse(match_sparse_patterns("A/bar.md", parsed))
        self.assertFalse(match_sparse_patterns("A/B", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns("A/B", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns(".cache", parsed, path_is_dir=True))

    def test_dir_only(self):
        """A pattern with a trailing slash should only match directories and subdirectories."""
        parsed = parse_sparse_patterns(["docs/"])
        # The directory pattern is not rooted, so can be at any level
        self.assertTrue(match_sparse_patterns("docs", parsed, path_is_dir=True))
        self.assertTrue(match_sparse_patterns("A/docs", parsed, path_is_dir=True))
        self.assertTrue(match_sparse_patterns("A/B/docs", parsed, path_is_dir=True))
        # Even if the path name is "docs", if it's a file, won't match:
        self.assertFalse(match_sparse_patterns("docs", parsed, path_is_dir=False))
        self.assertFalse(match_sparse_patterns("A/docs", parsed, path_is_dir=False))
        self.assertFalse(match_sparse_patterns("A/B/docs", parsed, path_is_dir=False))
        # Subfiles and subdirs of the included dir should match
        self.assertTrue(match_sparse_patterns("docs/x.md", parsed))
        self.assertTrue(match_sparse_patterns("docs/A/x.md", parsed))
        self.assertTrue(match_sparse_patterns("docs/A/B/x.md", parsed))
        self.assertTrue(match_sparse_patterns("docs/A", parsed, path_is_dir=True))
        self.assertTrue(match_sparse_patterns("docs/A/B", parsed, path_is_dir=True))
        self.assertTrue(match_sparse_patterns("docs", parsed, path_is_dir=True))

    def test_anchored(self):
        """Anchored patterns match from the start of the path only."""
        parsed = parse_sparse_patterns(["/foo"])  # Can be file or dir, must be at root
        self.assertTrue(match_sparse_patterns("foo", parsed))
        self.assertTrue(match_sparse_patterns("foo", parsed, path_is_dir=True))
        # But "some/foo" doesn't match because anchored requires start
        self.assertFalse(match_sparse_patterns("A/foo", parsed))
        self.assertFalse(match_sparse_patterns("A/foo", parsed, path_is_dir=True))

    def test_unanchored(self):
        parsed = parse_sparse_patterns(["foo"])
        self.assertTrue(match_sparse_patterns("foo", parsed))
        self.assertTrue(match_sparse_patterns("foo", parsed, path_is_dir=True))
        # But "some/foo" doesn't match because anchored requires start
        self.assertTrue(match_sparse_patterns("A/foo", parsed))
        self.assertTrue(match_sparse_patterns("A/foo", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns("bar", parsed))
        self.assertFalse(match_sparse_patterns("A/bar", parsed))

    def test_anchored_empty_pattern(self):
        """Test handling of empty pattern with anchoring (e.g., '/')."""
        # `/` should be recursive match of all files
        parsed = parse_sparse_patterns(["/"])
        self.assertEqual(parsed, [("", False, False, True)])  # anchored
        self.assertTrue(match_sparse_patterns("", parsed, path_is_dir=True))
        self.assertTrue(match_sparse_patterns("A", parsed, path_is_dir=True))
        self.assertTrue(match_sparse_patterns("A/B", parsed, path_is_dir=True))
        self.assertTrue(match_sparse_patterns("foo", parsed))
        self.assertTrue(match_sparse_patterns("A/foo", parsed))
        self.assertTrue(match_sparse_patterns("A/B/foo", parsed))

    def test_anchored_dir_only(self):
        """Test anchored directory-only patterns."""
        parsed = parse_sparse_patterns(["/docs/"])
        self.assertTrue(match_sparse_patterns("docs", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns("docs", parsed))  # file named docs
        self.assertFalse(match_sparse_patterns("A", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns("A/B", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns("A/docs", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns("A/docs", parsed))
        self.assertFalse(match_sparse_patterns("A/B/docs", parsed))
        self.assertFalse(match_sparse_patterns("A/B/docs", parsed, path_is_dir=True))

    def test_anchored_subpath(self):
        """Test anchored subpath pattern matching."""
        parsed = parse_sparse_patterns(["/A/B"])
        # TODO: should this also match the dir "A" (positively?)
        # self.assertTrue(match_sparse_patterns("A", parsed, path_is_dir=True))
        # self.assertFalse(match_sparse_patterns("A", parsed, path_is_dir=False))
        # Test exact match (both as file and dir, not dir-only pattern)
        self.assertTrue(match_sparse_patterns("A/B", parsed))
        self.assertTrue(match_sparse_patterns("A/B", parsed, path_is_dir=True))
        # Test subdirectory path (file and dir)
        self.assertTrue(match_sparse_patterns("A/B/file.txt", parsed))
        self.assertTrue(match_sparse_patterns("A/B/C", parsed, path_is_dir=True))
        # Test non-matching path
        self.assertFalse(match_sparse_patterns("X", parsed, path_is_dir=True))
        self.assertFalse(match_sparse_patterns("X/Y", parsed, path_is_dir=True))


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
        self.repo.get_worktree().stage([relpath])

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
        included = compute_included_paths_full(self.repo.open_index(), lines)
        self.assertEqual(included, {"foo.py", "docs/readme"})

    def test_full_with_utf8_paths(self):
        """Test that UTF-8 encoded paths are handled correctly."""
        self._add_file_to_index("unicode/文件.txt", b"unicode content")
        self._add_file_to_index("unicode/другой.md", b"more unicode")

        # Include all text files
        lines = ["*.txt"]
        included = compute_included_paths_full(self.repo.open_index(), lines)
        self.assertEqual(included, {"unicode/文件.txt"})


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
        self.repo.get_worktree().stage([relpath])

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
        included = compute_included_paths_cone(self.repo.open_index(), lines)
        # top-level => includes 'topfile'
        # subdirs => excluded, except docs/
        self.assertEqual(included, {"topfile", "docs/readme.md"})

    def test_cone_mode_with_empty_pattern(self):
        """Test cone mode with an empty reinclude directory."""
        self._add_file_to_index("topfile", b"hi")
        self._add_file_to_index("docs/readme.md", b"stuff")

        # Include an empty pattern that should be skipped
        lines = [
            "/*",
            "!/*/",
            "/",  # This empty pattern should be skipped
        ]
        included = compute_included_paths_cone(self.repo.open_index(), lines)
        # Only topfile should be included since the empty pattern is skipped
        self.assertEqual(included, {"topfile"})

    def test_no_exclude_subdirs(self):
        """If lines never specify '!/*/', we include everything by default."""
        self._add_file_to_index("topfile", b"hi")
        self._add_file_to_index("docs/readme.md", b"stuff")
        self._add_file_to_index("lib/code.py", b"stuff")

        lines = [
            "/*",  # top-level
            "/docs/",  # re-include docs?
        ]
        included = compute_included_paths_cone(self.repo.open_index(), lines)
        # Because exclude_subdirs was never set, everything is included:
        self.assertEqual(
            included,
            {"topfile", "docs/readme.md", "lib/code.py"},
        )

    def test_only_reinclude_dirs(self):
        """Test cone mode when only reinclude directories are specified."""
        self._add_file_to_index("topfile", b"hi")
        self._add_file_to_index("docs/readme.md", b"stuff")
        self._add_file_to_index("lib/code.py", b"stuff")

        # Only specify reinclude_dirs, need to explicitly exclude subdirs
        lines = ["!/*/", "/docs/"]
        included = compute_included_paths_cone(self.repo.open_index(), lines)
        # Only docs/* should be included, not topfile or lib/*
        self.assertEqual(included, {"docs/readme.md"})

    def test_exclude_subdirs_no_toplevel(self):
        """Test with exclude_subdirs but without toplevel files."""
        self._add_file_to_index("topfile", b"hi")
        self._add_file_to_index("docs/readme.md", b"stuff")
        self._add_file_to_index("lib/code.py", b"stuff")

        # Only exclude subdirs and reinclude docs
        lines = ["!/*/", "/docs/"]
        included = compute_included_paths_cone(self.repo.open_index(), lines)
        # Only docs/* should be included since we didn't include top level
        self.assertEqual(included, {"docs/readme.md"})


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
        self.repo.get_worktree().stage([relpath])

    def test_full_mode(self):
        self._add_file_to_index("foo.py")
        self._add_file_to_index("bar.md")

        lines = ["*.py", "!bar.*"]
        index = self.repo.open_index()
        included = determine_included_paths(index, lines, cone=False)
        self.assertEqual(included, {"foo.py"})

    def test_cone_mode(self):
        self._add_file_to_index("topfile")
        self._add_file_to_index("subdir/anotherfile")

        lines = ["/*", "!/*/"]
        index = self.repo.open_index()
        included = determine_included_paths(index, lines, cone=True)
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
        self.repo.get_worktree().stage([relpath])
        # Actually commit so the object is in the store
        self.repo.get_worktree().commit(
            message=b"Commit " + relpath.encode(),
        )

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

    def test_directory_removal(self):
        """Test handling of directories when removing excluded files."""
        # Create a directory with a file
        dir_path = os.path.join(self.temp_dir, "dir")
        os.makedirs(dir_path, exist_ok=True)
        self._commit_blob("dir/file.txt", b"content")

        # Make sure it exists before we proceed
        self.assertTrue(os.path.exists(os.path.join(dir_path, "file.txt")))

        # Exclude everything
        apply_included_paths(self.repo, included_paths=set(), force=True)

        # The file should be removed, but the directory might remain
        self.assertFalse(os.path.exists(os.path.join(dir_path, "file.txt")))

        # Test when file is actually a directory - should hit the IsADirectoryError case
        another_dir_path = os.path.join(self.temp_dir, "another_dir")
        os.makedirs(another_dir_path, exist_ok=True)
        self._commit_blob("another_dir/subfile.txt", b"content")

        # Create a path with the same name as the file but make it a dir to trigger IsADirectoryError
        subfile_dir_path = os.path.join(another_dir_path, "subfile.txt")
        if os.path.exists(subfile_dir_path):
            # Remove any existing file first
            os.remove(subfile_dir_path)
        os.makedirs(subfile_dir_path, exist_ok=True)

        # Attempt to apply sparse checkout, should trigger IsADirectoryError but not fail
        apply_included_paths(self.repo, included_paths=set(), force=True)

    def test_handling_removed_files(self):
        """Test that files already removed from disk are handled correctly during exclusion."""
        self._commit_blob("test_file.txt", b"test content")
        # Remove the file manually
        os.remove(os.path.join(self.temp_dir, "test_file.txt"))

        # Should not raise any errors when excluding this file
        apply_included_paths(self.repo, included_paths=set(), force=True)

        # Verify skip-worktree bit is set in index
        idx = self.repo.open_index()
        self.assertTrue(idx[b"test_file.txt"].skip_worktree)

    def test_local_modifications_ioerror(self):
        """Test handling of PermissionError/OSError when checking for local modifications."""
        import sys

        self._commit_blob("special_file.txt", b"content")
        file_path = os.path.join(self.temp_dir, "special_file.txt")

        # On Windows, chmod with 0 doesn't make files unreadable the same way
        # Skip this test on Windows as the permission model is different
        if sys.platform == "win32":
            self.skipTest("File permissions work differently on Windows")

        # Make the file unreadable on Unix-like systems
        os.chmod(file_path, 0)

        # Add a cleanup that checks if file exists first
        def safe_chmod_cleanup():
            if os.path.exists(file_path):
                try:
                    os.chmod(file_path, 0o644)
                except (FileNotFoundError, PermissionError):
                    pass

        self.addCleanup(safe_chmod_cleanup)

        # Should raise PermissionError with unreadable file and force=False
        with self.assertRaises((PermissionError, OSError)):
            apply_included_paths(self.repo, included_paths=set(), force=False)

        # With force=True, should remove the file anyway
        apply_included_paths(self.repo, included_paths=set(), force=True)

        # Verify file is gone and skip-worktree bit is set
        self.assertFalse(os.path.exists(file_path))
        idx = self.repo.open_index()
        self.assertTrue(idx[b"special_file.txt"].skip_worktree)

    def test_checkout_normalization_applied(self):
        """Test that checkout normalization is applied when materializing files during sparse checkout."""

        # Create a simple filter that converts content to uppercase
        class UppercaseFilter:
            def smudge(self, input_bytes, path=b""):
                return input_bytes.upper()

            def clean(self, input_bytes):
                return input_bytes.lower()

            def cleanup(self):
                pass

            def reuse(self, config, filter_name):
                return False

        # Create .gitattributes file
        gitattributes_path = os.path.join(self.temp_dir, ".gitattributes")
        with open(gitattributes_path, "w") as f:
            f.write("*.txt filter=uppercase\n")

        # Add and commit .gitattributes
        self.repo.get_worktree().stage([b".gitattributes"])
        self.repo.get_worktree().commit(
            b"Add gitattributes", committer=b"Test <test@example.com>"
        )

        # Initialize the filter context and register the filter
        _ = self.repo.get_blob_normalizer()

        # Register the filter with the cached filter context
        uppercase_filter = UppercaseFilter()
        self.repo.filter_context.filter_registry.register_driver(
            "uppercase", uppercase_filter
        )

        # Commit a file with lowercase content
        self._commit_blob("test.txt", b"hello world")

        # Remove the file from working tree to force materialization
        os.remove(os.path.join(self.temp_dir, "test.txt"))

        # Apply sparse checkout - this will call get_blob_normalizer() internally
        # which will use the cached filter_context with our registered filter
        apply_included_paths(self.repo, included_paths={"test.txt"}, force=False)

        # Verify file was materialized with uppercase content (checkout normalization applied)
        with open(os.path.join(self.temp_dir, "test.txt"), "rb") as f:
            content = f.read()
            self.assertEqual(content, b"HELLO WORLD")

    def test_checkout_normalization_with_lf_to_crlf(self):
        """Test that line ending normalization is applied during sparse checkout."""
        # Commit a file with LF line endings
        self._commit_blob("unix_file.txt", b"line1\nline2\nline3\n")

        # Remove the file from working tree
        os.remove(os.path.join(self.temp_dir, "unix_file.txt"))

        # Create a normalizer that converts LF to CRLF on checkout
        class CRLFNormalizer:
            def checkin_normalize(self, data, path):
                # For checkin, return unchanged
                return data

            def checkout_normalize(self, blob, path):
                if isinstance(blob, Blob):
                    # Convert LF to CRLF
                    new_blob = Blob()
                    new_blob.data = blob.data.replace(b"\n", b"\r\n")
                    return new_blob
                return blob

        # Monkey patch the repo to use our normalizer
        original_get_blob_normalizer = self.repo.get_blob_normalizer
        self.repo.get_blob_normalizer = lambda: CRLFNormalizer()

        # Apply sparse checkout
        apply_included_paths(self.repo, included_paths={"unix_file.txt"}, force=False)

        # Verify file was materialized with CRLF line endings
        with open(os.path.join(self.temp_dir, "unix_file.txt"), "rb") as f:
            content = f.read()
            self.assertEqual(content, b"line1\r\nline2\r\nline3\r\n")

        # Restore original method
        self.repo.get_blob_normalizer = original_get_blob_normalizer

    def test_checkout_normalization_not_applied_without_normalizer(self):
        """Test that when normalizer returns original blob, no transformation occurs."""
        # Commit a file with specific content
        original_content = b"original content\nwith newlines\n"
        self._commit_blob("no_norm.txt", original_content)

        # Remove the file from working tree
        os.remove(os.path.join(self.temp_dir, "no_norm.txt"))

        # Create a normalizer that returns blob unchanged
        class NoOpNormalizer:
            def checkin_normalize(self, data, path):
                return data

            def checkout_normalize(self, blob, path):
                # Return the blob unchanged
                return blob

        # Monkey patch the repo to use our no-op normalizer
        original_get_blob_normalizer = self.repo.get_blob_normalizer
        self.repo.get_blob_normalizer = lambda: NoOpNormalizer()

        # Apply sparse checkout
        apply_included_paths(self.repo, included_paths={"no_norm.txt"}, force=False)

        # Verify file was materialized with original content (no normalization)
        with open(os.path.join(self.temp_dir, "no_norm.txt"), "rb") as f:
            content = f.read()
            self.assertEqual(content, original_content)

        # Restore original method
        self.repo.get_blob_normalizer = original_get_blob_normalizer
