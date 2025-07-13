# test_filters.py -- Tests for filters
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for filters."""

import os
import tempfile
import unittest

from dulwich import porcelain
from dulwich.filters import FilterError
from dulwich.repo import Repo

from . import TestCase


class GitAttributesFilterIntegrationTests(TestCase):
    """Test gitattributes integration with filter drivers."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup_test_dir)
        self.repo = Repo.init(self.test_dir)

    def _cleanup_test_dir(self) -> None:
        """Clean up test directory."""
        import shutil

        shutil.rmtree(self.test_dir)

    def test_gitattributes_text_filter(self) -> None:
        """Test that text attribute triggers line ending conversion."""
        # Configure autocrlf first
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        config.write_to_path()

        # Create .gitattributes with text attribute
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.txt text\n")
            f.write(b"*.bin -text\n")

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])
        porcelain.commit(self.repo, message=b"Add gitattributes")

        # Create text file with CRLF
        text_file = os.path.join(self.test_dir, "test.txt")
        with open(text_file, "wb") as f:
            f.write(b"line1\r\nline2\r\n")

        # Create binary file with CRLF
        bin_file = os.path.join(self.test_dir, "test.bin")
        with open(bin_file, "wb") as f:
            f.write(b"binary\r\ndata\r\n")

        # Add files
        porcelain.add(self.repo, paths=["test.txt", "test.bin"])

        # Check that text file was normalized
        index = self.repo.open_index()
        text_entry = index[b"test.txt"]
        text_blob = self.repo.object_store[text_entry.sha]
        self.assertEqual(text_blob.data, b"line1\nline2\n")

        # Check that binary file was not normalized
        bin_entry = index[b"test.bin"]
        bin_blob = self.repo.object_store[bin_entry.sha]
        self.assertEqual(bin_blob.data, b"binary\r\ndata\r\n")

    @unittest.skip("Custom process filters require external commands")
    def test_gitattributes_custom_filter(self) -> None:
        """Test custom filter specified in gitattributes."""
        # Create .gitattributes with custom filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.secret filter=redact\n")

        # Configure custom filter (use tr command for testing)
        config = self.repo.get_config()
        # This filter replaces all digits with X
        config.set((b"filter", b"redact"), b"clean", b"tr '0-9' 'X'")
        config.write_to_path()

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create file with sensitive content
        secret_file = os.path.join(self.test_dir, "password.secret")
        with open(secret_file, "wb") as f:
            f.write(b"password123\ntoken456\n")

        # Add file
        porcelain.add(self.repo, paths=["password.secret"])

        # Check that content was filtered
        index = self.repo.open_index()
        entry = index[b"password.secret"]
        blob = self.repo.object_store[entry.sha]
        self.assertEqual(blob.data, b"passwordXXX\ntokenXXX\n")

    def test_gitattributes_from_tree(self) -> None:
        """Test that gitattributes from tree are used when no working tree exists."""
        # Create .gitattributes with text attribute
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.txt text\n")

        # Add and commit .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])
        porcelain.commit(self.repo, message=b"Add gitattributes")

        # Remove .gitattributes from working tree
        os.remove(gitattributes_path)

        # Get gitattributes - should still work from tree
        gitattributes = self.repo.get_gitattributes()
        attrs = gitattributes.match_path(b"test.txt")
        self.assertEqual(attrs.get(b"text"), True)

    def test_gitattributes_info_attributes(self) -> None:
        """Test that .git/info/attributes is read."""
        # Create info/attributes
        info_dir = os.path.join(self.repo.controldir(), "info")
        if not os.path.exists(info_dir):
            os.makedirs(info_dir)
        info_attrs_path = os.path.join(info_dir, "attributes")
        with open(info_attrs_path, "wb") as f:
            f.write(b"*.log text\n")

        # Get gitattributes
        gitattributes = self.repo.get_gitattributes()
        attrs = gitattributes.match_path(b"debug.log")
        self.assertEqual(attrs.get(b"text"), True)

    @unittest.skip("Custom process filters require external commands")
    def test_filter_precedence(self) -> None:
        """Test that filter attribute takes precedence over text attribute."""
        # Create .gitattributes with both text and filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.txt text filter=custom\n")

        # Configure autocrlf and custom filter
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        # This filter converts to uppercase
        config.set((b"filter", b"custom"), b"clean", b"tr '[:lower:]' '[:upper:]'")
        config.write_to_path()

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create text file with lowercase and CRLF
        text_file = os.path.join(self.test_dir, "test.txt")
        with open(text_file, "wb") as f:
            f.write(b"hello\r\nworld\r\n")

        # Add file
        porcelain.add(self.repo, paths=["test.txt"])

        # Check that custom filter was applied (not just line ending conversion)
        index = self.repo.open_index()
        entry = index[b"test.txt"]
        blob = self.repo.object_store[entry.sha]
        # Should be uppercase with LF endings
        self.assertEqual(blob.data, b"HELLO\nWORLD\n")

    def test_blob_normalizer_integration(self) -> None:
        """Test that get_blob_normalizer returns a FilterBlobNormalizer."""
        normalizer = self.repo.get_blob_normalizer()

        # Check it's the right type
        from dulwich.filters import FilterBlobNormalizer

        self.assertIsInstance(normalizer, FilterBlobNormalizer)

        # Check it has access to gitattributes
        self.assertIsNotNone(normalizer.gitattributes)
        self.assertIsNotNone(normalizer.filter_registry)

    def test_required_filter_missing(self) -> None:
        """Test that missing required filter raises an error."""
        # Create .gitattributes with required filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.secret filter=required_filter\n")

        # Configure filter as required but without commands
        config = self.repo.get_config()
        config.set((b"filter", b"required_filter"), b"required", b"true")
        config.write_to_path()

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create file that would use the filter
        secret_file = os.path.join(self.test_dir, "test.secret")
        with open(secret_file, "wb") as f:
            f.write(b"test content\n")

        # Adding file should raise error due to missing required filter
        with self.assertRaises(FilterError) as cm:
            porcelain.add(self.repo, paths=["test.secret"])
        self.assertIn(
            "Required filter 'required_filter' is not available", str(cm.exception)
        )

    def test_required_filter_clean_command_fails(self) -> None:
        """Test that required filter failure during clean raises an error."""
        # Create .gitattributes with required filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.secret filter=failing_filter\n")

        # Configure filter as required with failing command
        config = self.repo.get_config()
        config.set(
            (b"filter", b"failing_filter"), b"clean", b"false"
        )  # false command always fails
        config.set((b"filter", b"failing_filter"), b"required", b"true")
        config.write_to_path()

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create file that would use the filter
        secret_file = os.path.join(self.test_dir, "test.secret")
        with open(secret_file, "wb") as f:
            f.write(b"test content\n")

        # Adding file should raise error due to failing required filter
        with self.assertRaises(FilterError) as cm:
            porcelain.add(self.repo, paths=["test.secret"])
        self.assertIn("Required clean filter failed", str(cm.exception))

    def test_required_filter_success(self) -> None:
        """Test that required filter works when properly configured."""
        # Create .gitattributes with required filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.secret filter=working_filter\n")

        # Configure filter as required with working command
        config = self.repo.get_config()
        config.set(
            (b"filter", b"working_filter"), b"clean", b"tr 'a-z' 'A-Z'"
        )  # uppercase
        config.set((b"filter", b"working_filter"), b"required", b"true")
        config.write_to_path()

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create file that would use the filter
        secret_file = os.path.join(self.test_dir, "test.secret")
        with open(secret_file, "wb") as f:
            f.write(b"hello world\n")

        # Adding file should work and apply filter
        porcelain.add(self.repo, paths=["test.secret"])

        # Check that content was filtered
        index = self.repo.open_index()
        entry = index[b"test.secret"]
        blob = self.repo.object_store[entry.sha]
        self.assertEqual(blob.data, b"HELLO WORLD\n")

    def test_optional_filter_failure_fallback(self) -> None:
        """Test that optional filter failure falls back to original data."""
        # Create .gitattributes with optional filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.txt filter=optional_filter\n")

        # Configure filter as optional (required=false) with failing command
        config = self.repo.get_config()
        config.set(
            (b"filter", b"optional_filter"), b"clean", b"false"
        )  # false command always fails
        config.set((b"filter", b"optional_filter"), b"required", b"false")
        config.write_to_path()

        # Add .gitattributes
        porcelain.add(self.repo, paths=[".gitattributes"])

        # Create file that would use the filter
        test_file = os.path.join(self.test_dir, "test.txt")
        with open(test_file, "wb") as f:
            f.write(b"test content\n")

        # Adding file should work and fallback to original content
        porcelain.add(self.repo, paths=["test.txt"])

        # Check that original content was preserved
        index = self.repo.open_index()
        entry = index[b"test.txt"]
        blob = self.repo.object_store[entry.sha]
        self.assertEqual(blob.data, b"test content\n")
