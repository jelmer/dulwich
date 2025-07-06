# test_attrs.py -- tests for gitattributes
# Copyright (C) 2019-2020 Collabora Ltd
# Copyright (C) 2019-2020 Andrej Shadura <andrew.shadura@collabora.co.uk>
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

"""Tests for gitattributes parsing and matching."""

import os
import tempfile
from io import BytesIO

from dulwich.attrs import (
    GitAttributes,
    Pattern,
    _parse_attr,
    match_path,
    parse_git_attributes,
    parse_gitattributes_file,
    read_gitattributes,
)

from . import TestCase


class ParseAttrTests(TestCase):
    """Test the _parse_attr function."""

    def test_parse_set_attr(self):
        """Test parsing a set attribute."""
        name, value = _parse_attr(b"text")
        self.assertEqual(name, b"text")
        self.assertEqual(value, True)

    def test_parse_unset_attr(self):
        """Test parsing an unset attribute."""
        name, value = _parse_attr(b"-text")
        self.assertEqual(name, b"text")
        self.assertEqual(value, False)

    def test_parse_unspecified_attr(self):
        """Test parsing an unspecified attribute."""
        name, value = _parse_attr(b"!text")
        self.assertEqual(name, b"text")
        self.assertEqual(value, None)

    def test_parse_value_attr(self):
        """Test parsing an attribute with a value."""
        name, value = _parse_attr(b"diff=python")
        self.assertEqual(name, b"diff")
        self.assertEqual(value, b"python")

    def test_parse_value_with_equals(self):
        """Test parsing an attribute value containing equals."""
        name, value = _parse_attr(b"filter=foo=bar")
        self.assertEqual(name, b"filter")
        self.assertEqual(value, b"foo=bar")


class ParseGitAttributesTests(TestCase):
    """Test the parse_git_attributes function."""

    def test_parse_empty(self):
        """Test parsing empty file."""
        attrs = list(parse_git_attributes(BytesIO(b"")))
        self.assertEqual(attrs, [])

    def test_parse_comments(self):
        """Test parsing file with comments."""
        content = b"""# This is a comment
# Another comment
"""
        attrs = list(parse_git_attributes(BytesIO(content)))
        self.assertEqual(attrs, [])

    def test_parse_single_pattern(self):
        """Test parsing single pattern."""
        content = b"*.txt text"
        attrs = list(parse_git_attributes(BytesIO(content)))
        self.assertEqual(len(attrs), 1)
        pattern, attributes = attrs[0]
        self.assertEqual(pattern, b"*.txt")
        self.assertEqual(attributes, {b"text": True})

    def test_parse_multiple_attributes(self):
        """Test parsing pattern with multiple attributes."""
        content = b"*.jpg -text -diff binary"
        attrs = list(parse_git_attributes(BytesIO(content)))
        self.assertEqual(len(attrs), 1)
        pattern, attributes = attrs[0]
        self.assertEqual(pattern, b"*.jpg")
        self.assertEqual(attributes, {b"text": False, b"diff": False, b"binary": True})

    def test_parse_attributes_with_values(self):
        """Test parsing attributes with values."""
        content = b"*.c filter=indent diff=cpp text"
        attrs = list(parse_git_attributes(BytesIO(content)))
        self.assertEqual(len(attrs), 1)
        pattern, attributes = attrs[0]
        self.assertEqual(pattern, b"*.c")
        self.assertEqual(
            attributes, {b"filter": b"indent", b"diff": b"cpp", b"text": True}
        )

    def test_parse_multiple_patterns(self):
        """Test parsing multiple patterns."""
        content = b"""*.txt text
*.jpg -text binary
*.py diff=python
"""
        attrs = list(parse_git_attributes(BytesIO(content)))
        self.assertEqual(len(attrs), 3)

        # First pattern
        pattern, attributes = attrs[0]
        self.assertEqual(pattern, b"*.txt")
        self.assertEqual(attributes, {b"text": True})

        # Second pattern
        pattern, attributes = attrs[1]
        self.assertEqual(pattern, b"*.jpg")
        self.assertEqual(attributes, {b"text": False, b"binary": True})

        # Third pattern
        pattern, attributes = attrs[2]
        self.assertEqual(pattern, b"*.py")
        self.assertEqual(attributes, {b"diff": b"python"})

    def test_parse_git_lfs_example(self):
        """Test parsing Git LFS example from docstring."""
        content = b"""*.tar.* filter=lfs diff=lfs merge=lfs -text

# store signatures in Git
*.tar.*.asc -filter -diff merge=binary -text

# store .dsc verbatim
*.dsc -filter !diff merge=binary !text
"""
        attrs = list(parse_git_attributes(BytesIO(content)))
        self.assertEqual(len(attrs), 3)

        # LFS pattern
        pattern, attributes = attrs[0]
        self.assertEqual(pattern, b"*.tar.*")
        self.assertEqual(
            attributes,
            {b"filter": b"lfs", b"diff": b"lfs", b"merge": b"lfs", b"text": False},
        )

        # Signatures pattern
        pattern, attributes = attrs[1]
        self.assertEqual(pattern, b"*.tar.*.asc")
        self.assertEqual(
            attributes,
            {b"filter": False, b"diff": False, b"merge": b"binary", b"text": False},
        )

        # .dsc pattern
        pattern, attributes = attrs[2]
        self.assertEqual(pattern, b"*.dsc")
        self.assertEqual(
            attributes,
            {b"filter": False, b"diff": None, b"merge": b"binary", b"text": None},
        )


class PatternTests(TestCase):
    """Test the Pattern class."""

    def test_exact_match(self):
        """Test exact filename matching without path."""
        pattern = Pattern(b"README.txt")
        self.assertTrue(pattern.match(b"README.txt"))
        self.assertFalse(pattern.match(b"readme.txt"))
        # Patterns without slashes match at any level
        self.assertTrue(pattern.match(b"src/README.txt"))

    def test_wildcard_extension(self):
        """Test wildcard extension matching."""
        pattern = Pattern(b"*.txt")
        self.assertTrue(pattern.match(b"file.txt"))
        self.assertTrue(pattern.match(b"README.txt"))
        self.assertTrue(pattern.match(b"src/doc.txt"))
        self.assertFalse(pattern.match(b"file.txt.bak"))
        self.assertFalse(pattern.match(b"file.md"))

    def test_wildcard_in_name(self):
        """Test wildcard in filename."""
        pattern = Pattern(b"test_*.py")
        self.assertTrue(pattern.match(b"test_foo.py"))
        self.assertTrue(pattern.match(b"test_bar.py"))
        self.assertTrue(pattern.match(b"src/test_baz.py"))
        self.assertFalse(pattern.match(b"test.py"))
        self.assertFalse(pattern.match(b"tests.py"))

    def test_question_mark(self):
        """Test question mark matching."""
        pattern = Pattern(b"file?.txt")
        self.assertTrue(pattern.match(b"file1.txt"))
        self.assertTrue(pattern.match(b"fileA.txt"))
        self.assertFalse(pattern.match(b"file.txt"))
        self.assertFalse(pattern.match(b"file10.txt"))

    def test_character_class(self):
        """Test character class matching."""
        pattern = Pattern(b"file[0-9].txt")
        self.assertTrue(pattern.match(b"file0.txt"))
        self.assertTrue(pattern.match(b"file5.txt"))
        self.assertTrue(pattern.match(b"file9.txt"))
        self.assertFalse(pattern.match(b"fileA.txt"))
        self.assertFalse(pattern.match(b"file10.txt"))

    def test_negated_character_class(self):
        """Test negated character class."""
        pattern = Pattern(b"file[!0-9].txt")
        self.assertTrue(pattern.match(b"fileA.txt"))
        self.assertTrue(pattern.match(b"file_.txt"))
        self.assertFalse(pattern.match(b"file0.txt"))
        self.assertFalse(pattern.match(b"file5.txt"))

    def test_directory_pattern(self):
        """Test pattern with directory."""
        pattern = Pattern(b"src/*.py")
        self.assertTrue(pattern.match(b"src/foo.py"))
        self.assertTrue(pattern.match(b"src/bar.py"))
        self.assertFalse(pattern.match(b"foo.py"))
        self.assertFalse(pattern.match(b"src/sub/foo.py"))
        self.assertFalse(pattern.match(b"other/foo.py"))

    def test_double_asterisk(self):
        """Test double asterisk matching."""
        pattern = Pattern(b"**/foo.txt")
        self.assertTrue(pattern.match(b"foo.txt"))
        self.assertTrue(pattern.match(b"src/foo.txt"))
        self.assertTrue(pattern.match(b"src/sub/foo.txt"))
        self.assertTrue(pattern.match(b"a/b/c/foo.txt"))

    def test_double_asterisk_middle(self):
        """Test double asterisk in middle."""
        pattern = Pattern(b"src/**/foo.txt")
        self.assertTrue(pattern.match(b"src/foo.txt"))
        self.assertTrue(pattern.match(b"src/sub/foo.txt"))
        self.assertTrue(pattern.match(b"src/a/b/foo.txt"))
        self.assertFalse(pattern.match(b"foo.txt"))
        self.assertFalse(pattern.match(b"other/foo.txt"))

    def test_leading_slash(self):
        """Test pattern with leading slash."""
        pattern = Pattern(b"/README.txt")
        self.assertTrue(pattern.match(b"README.txt"))
        self.assertTrue(pattern.match(b"/README.txt"))
        self.assertFalse(pattern.match(b"src/README.txt"))


class MatchPathTests(TestCase):
    """Test the match_path function."""

    def test_no_matches(self):
        """Test when no patterns match."""
        patterns = [
            (Pattern(b"*.txt"), {b"text": True}),
            (Pattern(b"*.jpg"), {b"binary": True}),
        ]
        attrs = match_path(patterns, b"file.py")
        self.assertEqual(attrs, {})

    def test_single_match(self):
        """Test single pattern match."""
        patterns = [
            (Pattern(b"*.txt"), {b"text": True}),
            (Pattern(b"*.jpg"), {b"binary": True}),
        ]
        attrs = match_path(patterns, b"README.txt")
        self.assertEqual(attrs, {b"text": True})

    def test_multiple_matches_override(self):
        """Test that later patterns override earlier ones."""
        patterns = [
            (Pattern(b"*"), {b"text": True}),
            (Pattern(b"*.jpg"), {b"text": False, b"binary": True}),
        ]
        attrs = match_path(patterns, b"image.jpg")
        self.assertEqual(attrs, {b"text": False, b"binary": True})

    def test_unspecified_removes_attribute(self):
        """Test that unspecified (None) removes attributes."""
        patterns = [
            (Pattern(b"*"), {b"text": True, b"diff": True}),
            (Pattern(b"*.bin"), {b"text": None, b"binary": True}),
        ]
        attrs = match_path(patterns, b"file.bin")
        self.assertEqual(attrs, {b"diff": True, b"binary": True})
        # 'text' should be removed
        self.assertNotIn(b"text", attrs)


class FileOperationsTests(TestCase):
    """Test file operations."""

    def test_parse_gitattributes_file(self):
        """Test parsing a gitattributes file."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"*.txt text\n")
            f.write(b"*.jpg -text binary\n")
            temp_path = f.name

        self.addCleanup(os.unlink, temp_path)

        patterns = parse_gitattributes_file(temp_path)
        self.assertEqual(len(patterns), 2)

        # Check first pattern
        pattern, attrs = patterns[0]
        self.assertEqual(pattern.pattern, b"*.txt")
        self.assertEqual(attrs, {b"text": True})

        # Check second pattern
        pattern, attrs = patterns[1]
        self.assertEqual(pattern.pattern, b"*.jpg")
        self.assertEqual(attrs, {b"text": False, b"binary": True})

    def test_read_gitattributes(self):
        """Test reading gitattributes from a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create .gitattributes file
            attrs_path = os.path.join(tmpdir, ".gitattributes")
            with open(attrs_path, "wb") as f:
                f.write(b"*.py diff=python\n")

            patterns = read_gitattributes(tmpdir)
            self.assertEqual(len(patterns), 1)

            pattern, attrs = patterns[0]
            self.assertEqual(pattern.pattern, b"*.py")
            self.assertEqual(attrs, {b"diff": b"python"})

    def test_read_gitattributes_missing(self):
        """Test reading gitattributes when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            patterns = read_gitattributes(tmpdir)
            self.assertEqual(patterns, [])


class GitAttributesTests(TestCase):
    """Test the GitAttributes class."""

    def test_empty_gitattributes(self):
        """Test GitAttributes with no patterns."""
        ga = GitAttributes()
        attrs = ga.match_path(b"file.txt")
        self.assertEqual(attrs, {})
        self.assertEqual(len(ga), 0)

    def test_gitattributes_with_patterns(self):
        """Test GitAttributes with patterns."""
        patterns = [
            (Pattern(b"*.txt"), {b"text": True}),
            (Pattern(b"*.jpg"), {b"binary": True, b"text": False}),
        ]
        ga = GitAttributes(patterns)

        # Test matching .txt file
        attrs = ga.match_path(b"README.txt")
        self.assertEqual(attrs, {b"text": True})

        # Test matching .jpg file
        attrs = ga.match_path(b"image.jpg")
        self.assertEqual(attrs, {b"binary": True, b"text": False})

        # Test non-matching file
        attrs = ga.match_path(b"script.py")
        self.assertEqual(attrs, {})

        self.assertEqual(len(ga), 2)

    def test_add_patterns(self):
        """Test adding patterns to GitAttributes."""
        ga = GitAttributes()
        self.assertEqual(len(ga), 0)

        # Add patterns
        ga.add_patterns(
            [
                (Pattern(b"*.py"), {b"diff": b"python"}),
                (Pattern(b"*.md"), {b"text": True}),
            ]
        )

        self.assertEqual(len(ga), 2)
        attrs = ga.match_path(b"test.py")
        self.assertEqual(attrs, {b"diff": b"python"})

    def test_iteration(self):
        """Test iterating over patterns."""
        patterns = [
            (Pattern(b"*.txt"), {b"text": True}),
            (Pattern(b"*.jpg"), {b"binary": True}),
        ]
        ga = GitAttributes(patterns)

        collected = list(ga)
        self.assertEqual(len(collected), 2)
        self.assertEqual(collected[0][0].pattern, b"*.txt")
        self.assertEqual(collected[1][0].pattern, b"*.jpg")

    def test_from_file(self):
        """Test creating GitAttributes from file."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(b"*.txt text\n")
            f.write(b"*.bin -text binary\n")
            temp_path = f.name

        self.addCleanup(os.unlink, temp_path)

        ga = GitAttributes.from_file(temp_path)
        self.assertEqual(len(ga), 2)

        attrs = ga.match_path(b"file.txt")
        self.assertEqual(attrs, {b"text": True})

        attrs = ga.match_path(b"file.bin")
        self.assertEqual(attrs, {b"text": False, b"binary": True})

    def test_from_path(self):
        """Test creating GitAttributes from directory path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create .gitattributes file
            attrs_path = os.path.join(tmpdir, ".gitattributes")
            with open(attrs_path, "wb") as f:
                f.write(b"*.py diff=python\n")
                f.write(b"*.rs diff=rust\n")

            ga = GitAttributes.from_path(tmpdir)
            self.assertEqual(len(ga), 2)

            attrs = ga.match_path(b"main.py")
            self.assertEqual(attrs, {b"diff": b"python"})

            attrs = ga.match_path(b"lib.rs")
            self.assertEqual(attrs, {b"diff": b"rust"})

    def test_set_attribute(self):
        """Test setting attributes."""
        ga = GitAttributes()

        # Set attribute for a new pattern
        ga.set_attribute(b"*.txt", b"text", True)
        attrs = ga.match_path(b"file.txt")
        self.assertEqual(attrs, {b"text": True})

        # Update existing pattern
        ga.set_attribute(b"*.txt", b"diff", b"plain")
        attrs = ga.match_path(b"file.txt")
        self.assertEqual(attrs, {b"text": True, b"diff": b"plain"})

        # Unset attribute
        ga.set_attribute(b"*.txt", b"binary", False)
        attrs = ga.match_path(b"file.txt")
        self.assertEqual(attrs, {b"text": True, b"diff": b"plain", b"binary": False})

        # Remove attribute (unspecified)
        ga.set_attribute(b"*.txt", b"text", None)
        attrs = ga.match_path(b"file.txt")
        self.assertEqual(attrs, {b"diff": b"plain", b"binary": False})

    def test_remove_pattern(self):
        """Test removing patterns."""
        patterns = [
            (Pattern(b"*.txt"), {b"text": True}),
            (Pattern(b"*.jpg"), {b"binary": True}),
            (Pattern(b"*.py"), {b"diff": b"python"}),
        ]
        ga = GitAttributes(patterns)
        self.assertEqual(len(ga), 3)

        # Remove middle pattern
        ga.remove_pattern(b"*.jpg")
        self.assertEqual(len(ga), 2)

        # Check remaining patterns
        attrs = ga.match_path(b"file.txt")
        self.assertEqual(attrs, {b"text": True})

        attrs = ga.match_path(b"image.jpg")
        self.assertEqual(attrs, {})  # No match anymore

        attrs = ga.match_path(b"script.py")
        self.assertEqual(attrs, {b"diff": b"python"})

    def test_to_bytes_empty(self):
        """Test converting empty GitAttributes to bytes."""
        ga = GitAttributes()
        content = ga.to_bytes()
        self.assertEqual(content, b"")

    def test_to_bytes_single_pattern(self):
        """Test converting single pattern to bytes."""
        ga = GitAttributes()
        ga.set_attribute(b"*.txt", b"text", True)
        content = ga.to_bytes()
        self.assertEqual(content, b"*.txt text\n")

    def test_to_bytes_multiple_attributes(self):
        """Test converting pattern with multiple attributes to bytes."""
        ga = GitAttributes()
        ga.set_attribute(b"*.jpg", b"text", False)
        ga.set_attribute(b"*.jpg", b"diff", False)
        ga.set_attribute(b"*.jpg", b"binary", True)
        content = ga.to_bytes()
        # Attributes should be sorted
        self.assertEqual(content, b"*.jpg binary -diff -text\n")

    def test_to_bytes_multiple_patterns(self):
        """Test converting multiple patterns to bytes."""
        ga = GitAttributes()
        ga.set_attribute(b"*.txt", b"text", True)
        ga.set_attribute(b"*.jpg", b"binary", True)
        ga.set_attribute(b"*.jpg", b"text", False)
        content = ga.to_bytes()
        expected = b"*.txt text\n*.jpg binary -text\n"
        self.assertEqual(content, expected)

    def test_to_bytes_with_values(self):
        """Test converting attributes with values to bytes."""
        ga = GitAttributes()
        ga.set_attribute(b"*.c", b"filter", b"indent")
        ga.set_attribute(b"*.c", b"diff", b"cpp")
        ga.set_attribute(b"*.c", b"text", True)
        content = ga.to_bytes()
        # Attributes should be sorted
        self.assertEqual(content, b"*.c diff=cpp filter=indent text\n")

    def test_to_bytes_unspecified(self):
        """Test converting unspecified attributes to bytes."""
        ga = GitAttributes()
        ga.set_attribute(b"*.bin", b"text", None)
        ga.set_attribute(b"*.bin", b"diff", None)
        content = ga.to_bytes()
        # Unspecified attributes use !
        self.assertEqual(content, b"*.bin !diff !text\n")

    def test_write_to_file(self):
        """Test writing GitAttributes to file."""
        ga = GitAttributes()
        ga.set_attribute(b"*.txt", b"text", True)
        ga.set_attribute(b"*.jpg", b"binary", True)
        ga.set_attribute(b"*.jpg", b"text", False)

        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            ga.write_to_file(temp_path)

            # Read back the file
            with open(temp_path, "rb") as f:
                content = f.read()

            expected = b"*.txt text\n*.jpg binary -text\n"
            self.assertEqual(content, expected)
        finally:
            os.unlink(temp_path)

    def test_write_to_file_string_path(self):
        """Test writing GitAttributes to file with string path."""
        ga = GitAttributes()
        ga.set_attribute(b"*.py", b"diff", b"python")

        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            # Pass string path instead of bytes
            ga.write_to_file(temp_path)

            # Read back the file
            with open(temp_path, "rb") as f:
                content = f.read()

            self.assertEqual(content, b"*.py diff=python\n")
        finally:
            os.unlink(temp_path)

    def test_roundtrip_gitattributes(self):
        """Test reading and writing gitattributes preserves content."""
        original_content = b"""*.txt text
*.jpg -text binary
*.c filter=indent diff=cpp
*.bin !text !diff
"""

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            f.write(original_content)
            temp_path = f.name

        try:
            # Read the file
            ga = GitAttributes.from_file(temp_path)

            # Write to a new file
            with tempfile.NamedTemporaryFile(delete=False) as f2:
                temp_path2 = f2.name

            ga.write_to_file(temp_path2)

            # The content should be equivalent (though order might differ for attributes)
            ga2 = GitAttributes.from_file(temp_path2)

            # Compare patterns
            patterns1 = list(ga)
            patterns2 = list(ga2)
            self.assertEqual(len(patterns1), len(patterns2))

            for (p1, attrs1), (p2, attrs2) in zip(patterns1, patterns2):
                self.assertEqual(p1.pattern, p2.pattern)
                self.assertEqual(attrs1, attrs2)

            os.unlink(temp_path2)
        finally:
            os.unlink(temp_path)
