# test_merge_drivers.py -- Tests for merge driver support
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
#
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

"""Tests for merge driver support."""

import sys
import unittest
from typing import Optional

from dulwich.attrs import GitAttributes, Pattern
from dulwich.config import ConfigDict
from dulwich.merge import merge_blobs
from dulwich.merge_drivers import (
    MergeDriverRegistry,
    ProcessMergeDriver,
    get_merge_driver_registry,
)
from dulwich.objects import Blob


class _TestMergeDriver:
    """Test merge driver implementation."""

    def __init__(self, name: str = "test"):
        self.name = name
        self.called = False
        self.last_args = None

    def merge(
        self,
        ancestor: bytes,
        ours: bytes,
        theirs: bytes,
        path: Optional[str] = None,
        marker_size: int = 7,
    ) -> tuple[bytes, bool]:
        """Test merge implementation."""
        self.called = True
        self.last_args = {
            "ancestor": ancestor,
            "ours": ours,
            "theirs": theirs,
            "path": path,
            "marker_size": marker_size,
        }

        # Simple test merge: combine all three versions
        result = b"TEST MERGE OUTPUT\n"
        result += b"Ancestor: " + ancestor + b"\n"
        result += b"Ours: " + ours + b"\n"
        result += b"Theirs: " + theirs + b"\n"

        # Return success if all three are different
        success = ancestor != ours and ancestor != theirs
        return result, success


class MergeDriverRegistryTests(unittest.TestCase):
    """Tests for MergeDriverRegistry."""

    def test_register_driver(self):
        """Test registering a merge driver."""
        registry = MergeDriverRegistry()
        driver = _TestMergeDriver("mydriver")

        registry.register_driver("mydriver", driver)
        retrieved = registry.get_driver("mydriver")

        self.assertIs(retrieved, driver)

    def test_register_factory(self):
        """Test registering a merge driver factory."""
        registry = MergeDriverRegistry()

        def create_driver():
            return _TestMergeDriver("factory_driver")

        registry.register_factory("factory", create_driver)
        driver = registry.get_driver("factory")

        self.assertIsInstance(driver, _TestMergeDriver)
        self.assertEqual(driver.name, "factory_driver")

        # Second call should return the same instance
        driver2 = registry.get_driver("factory")
        self.assertIs(driver2, driver)

    def test_get_nonexistent_driver(self):
        """Test getting a non-existent driver returns None."""
        registry = MergeDriverRegistry()
        driver = registry.get_driver("nonexistent")
        self.assertIsNone(driver)

    def test_create_from_config(self):
        """Test creating a merge driver from configuration."""
        config = ConfigDict()
        config.set((b"merge", b"xmlmerge"), b"driver", b"xmlmerge %O %A %B")

        registry = MergeDriverRegistry(config)
        driver = registry.get_driver("xmlmerge")

        self.assertIsInstance(driver, ProcessMergeDriver)
        self.assertEqual(driver.name, "xmlmerge")
        self.assertEqual(driver.command, "xmlmerge %O %A %B")


class ProcessMergeDriverTests(unittest.TestCase):
    """Tests for ProcessMergeDriver."""

    def test_merge_with_echo(self):
        """Test merge driver using echo command."""
        # Use a simple echo command that writes to the output file
        command = "echo merged > %A"
        driver = ProcessMergeDriver(command, "echo_driver")

        ancestor = b"ancestor content"
        ours = b"our content"
        theirs = b"their content"

        result, success = driver.merge(ancestor, ours, theirs, "test.txt", 7)

        # Expect different line endings on Windows vs Unix
        if sys.platform == "win32":
            expected = b"merged \r\n"
        else:
            expected = b"merged\n"
        self.assertEqual(result, expected)
        self.assertTrue(success)  # echo returns 0

    def test_merge_with_cat(self):
        """Test merge driver using cat command."""
        # Cat all three files together
        command = "cat %O %B >> %A"
        driver = ProcessMergeDriver(command, "cat_driver")

        ancestor = b"ancestor\n"
        ours = b"ours\n"
        theirs = b"theirs\n"

        result, success = driver.merge(ancestor, ours, theirs)

        self.assertEqual(result, b"ours\nancestor\ntheirs\n")
        self.assertTrue(success)

    def test_merge_with_failure(self):
        """Test merge driver that fails."""
        # Use false command which always returns 1
        command = "false"
        driver = ProcessMergeDriver(command, "fail_driver")

        result, success = driver.merge(b"a", b"b", b"c")

        # Should return original content on failure
        self.assertEqual(result, b"b")
        self.assertFalse(success)

    def test_merge_with_markers(self):
        """Test merge driver with conflict marker size."""
        # Echo the marker size
        command = "echo marker size: %L > %A"
        driver = ProcessMergeDriver(command, "marker_driver")

        result, success = driver.merge(b"a", b"b", b"c", marker_size=15)

        # Expect different line endings on Windows vs Unix
        if sys.platform == "win32":
            expected = b"marker size: 15 \r\n"
        else:
            expected = b"marker size: 15\n"
        self.assertEqual(result, expected)
        self.assertTrue(success)

    def test_merge_with_path(self):
        """Test merge driver with file path."""
        # Echo the path
        command = "echo path: %P > %A"
        driver = ProcessMergeDriver(command, "path_driver")

        result, success = driver.merge(b"a", b"b", b"c", path="dir/file.xml")

        # Expect different line endings on Windows vs Unix
        if sys.platform == "win32":
            expected = b"path: dir/file.xml \r\n"
        else:
            expected = b"path: dir/file.xml\n"
        self.assertEqual(result, expected)
        self.assertTrue(success)


class MergeBlobsWithDriversTests(unittest.TestCase):
    """Tests for merge_blobs with merge drivers."""

    def setUp(self):
        """Set up test fixtures."""
        # Reset global registry
        global _merge_driver_registry
        from dulwich import merge_drivers

        merge_drivers._merge_driver_registry = None

    def test_merge_blobs_without_driver(self):
        """Test merge_blobs without any merge driver."""
        base = Blob.from_string(b"base\ncontent\n")
        ours = Blob.from_string(b"base\nour change\n")
        theirs = Blob.from_string(b"base\ntheir change\n")

        result, has_conflicts = merge_blobs(base, ours, theirs)

        # Should use default merge and have conflicts
        self.assertTrue(has_conflicts)
        self.assertIn(b"<<<<<<< ours", result)
        self.assertIn(b">>>>>>> theirs", result)

    def test_merge_blobs_with_text_driver(self):
        """Test merge_blobs with 'text' merge driver (default)."""
        base = Blob.from_string(b"base\ncontent\n")
        ours = Blob.from_string(b"base\nour change\n")
        theirs = Blob.from_string(b"base\ntheir change\n")

        # Set up gitattributes
        patterns = [(Pattern(b"*.txt"), {b"merge": b"text"})]
        gitattributes = GitAttributes(patterns)

        result, has_conflicts = merge_blobs(
            base, ours, theirs, b"file.txt", gitattributes
        )

        # Should use default merge (text is the default)
        self.assertTrue(has_conflicts)
        self.assertIn(b"<<<<<<< ours", result)

    def test_merge_blobs_with_custom_driver(self):
        """Test merge_blobs with custom merge driver."""
        # Register a test driver
        registry = get_merge_driver_registry()
        test_driver = _TestMergeDriver("custom")
        registry.register_driver("custom", test_driver)

        base = Blob.from_string(b"base content")
        ours = Blob.from_string(b"our content")
        theirs = Blob.from_string(b"their content")

        # Set up gitattributes
        patterns = [(Pattern(b"*.xml"), {b"merge": b"custom"})]
        gitattributes = GitAttributes(patterns)

        result, has_conflicts = merge_blobs(
            base, ours, theirs, b"file.xml", gitattributes
        )

        # Check that our test driver was called
        self.assertTrue(test_driver.called)
        self.assertEqual(test_driver.last_args["ancestor"], b"base content")
        self.assertEqual(test_driver.last_args["ours"], b"our content")
        self.assertEqual(test_driver.last_args["theirs"], b"their content")
        self.assertEqual(test_driver.last_args["path"], "file.xml")

        # Check result
        self.assertIn(b"TEST MERGE OUTPUT", result)
        self.assertFalse(
            has_conflicts
        )  # Our test driver returns success=True when all differ, so had_conflicts=False

    def test_merge_blobs_with_process_driver(self):
        """Test merge_blobs with process-based merge driver."""
        # Set up config with merge driver
        config = ConfigDict()
        config.set((b"merge", b"union"), b"driver", b"echo process merge worked > %A")

        base = Blob.from_string(b"base")
        ours = Blob.from_string(b"ours")
        theirs = Blob.from_string(b"theirs")

        # Set up gitattributes
        patterns = [(Pattern(b"*.list"), {b"merge": b"union"})]
        gitattributes = GitAttributes(patterns)

        result, has_conflicts = merge_blobs(
            base, ours, theirs, b"file.list", gitattributes, config
        )

        # Check that the process driver was executed
        # Expect different line endings on Windows vs Unix
        if sys.platform == "win32":
            expected = b"process merge worked \r\n"
        else:
            expected = b"process merge worked\n"
        self.assertEqual(result, expected)
        self.assertFalse(has_conflicts)  # echo returns 0

    def test_merge_blobs_driver_not_found(self):
        """Test merge_blobs when specified driver is not found."""
        base = Blob.from_string(b"base")
        ours = Blob.from_string(b"ours")
        theirs = Blob.from_string(b"theirs")

        # Set up gitattributes with non-existent driver
        patterns = [(Pattern(b"*.dat"), {b"merge": b"nonexistent"})]
        gitattributes = GitAttributes(patterns)

        result, has_conflicts = merge_blobs(
            base, ours, theirs, b"file.dat", gitattributes
        )

        # Should fall back to default merge
        self.assertTrue(has_conflicts)
        self.assertIn(b"<<<<<<< ours", result)


class GlobalRegistryTests(unittest.TestCase):
    """Tests for global merge driver registry."""

    def setUp(self):
        """Reset global registry before each test."""
        global _merge_driver_registry
        from dulwich import merge_drivers

        merge_drivers._merge_driver_registry = None

    def test_get_merge_driver_registry_singleton(self):
        """Test that get_merge_driver_registry returns singleton."""
        registry1 = get_merge_driver_registry()
        registry2 = get_merge_driver_registry()

        self.assertIs(registry1, registry2)

    def test_get_merge_driver_registry_with_config(self):
        """Test updating config on existing registry."""
        # Get registry without config
        registry = get_merge_driver_registry()
        self.assertIsNone(registry._config)

        # Get with config
        config = ConfigDict()
        registry2 = get_merge_driver_registry(config)

        self.assertIs(registry2, registry)
        self.assertIsNotNone(registry._config)


if __name__ == "__main__":
    unittest.main()
