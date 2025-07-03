# test_filters.py -- tests for filter drivers
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for filter drivers support."""

import sys
from unittest import skipIf

from dulwich.config import ConfigDict
from dulwich.filters import (
    FilterBlobNormalizer,
    FilterRegistry,
    ProcessFilterDriver,
    get_filter_for_path,
)
from dulwich.objects import Blob

from . import TestCase


class ProcessFilterDriverTests(TestCase):
    @skipIf(sys.platform == "win32", "Unix shell commands")
    def test_clean_filter(self) -> None:
        """Test clean filter with external command."""
        # Use a simple command that converts to uppercase
        driver = ProcessFilterDriver(clean_cmd="tr '[:lower:]' '[:upper:]'")
        result = driver.clean(b"hello world")
        self.assertEqual(result, b"HELLO WORLD")

    @skipIf(sys.platform == "win32", "Unix shell commands")
    def test_smudge_filter(self) -> None:
        """Test smudge filter with external command."""
        # Use a simple command that converts to lowercase
        driver = ProcessFilterDriver(smudge_cmd="tr '[:upper:]' '[:lower:]'")
        result = driver.smudge(b"HELLO WORLD")
        self.assertEqual(result, b"hello world")

    def test_no_filters(self) -> None:
        """Test driver with no filters configured."""
        driver = ProcessFilterDriver()
        data = b"test data"
        self.assertEqual(driver.clean(data), data)
        self.assertEqual(driver.smudge(data), data)

    @skipIf(sys.platform == "win32", "Unix shell commands")
    def test_failing_filter(self) -> None:
        """Test that failing filter propagates the error."""
        import subprocess

        # Use a command that will fail
        driver = ProcessFilterDriver(clean_cmd="false")
        data = b"test data"
        # Should raise CalledProcessError
        with self.assertRaises(subprocess.CalledProcessError):
            driver.clean(data)

        # Test smudge filter too
        driver = ProcessFilterDriver(smudge_cmd="false")
        with self.assertRaises(subprocess.CalledProcessError):
            driver.smudge(data)


class FilterRegistryTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = ConfigDict()
        self.registry = FilterRegistry(self.config)

    def test_register_and_get_driver(self) -> None:
        """Test registering and retrieving a driver."""
        driver = ProcessFilterDriver(clean_cmd="cat")
        self.registry.register_driver("test", driver)

        retrieved = self.registry.get_driver("test")
        self.assertIs(retrieved, driver)

    def test_get_nonexistent_driver(self) -> None:
        """Test getting a non-existent driver."""
        result = self.registry.get_driver("nonexistent")
        self.assertIsNone(result)

    def test_register_factory(self) -> None:
        """Test registering a driver factory."""
        created_driver = ProcessFilterDriver(clean_cmd="cat")

        def factory(registry):
            return created_driver

        self.registry.register_factory("test", factory)

        # Getting driver should invoke factory
        retrieved = self.registry.get_driver("test")
        self.assertIs(retrieved, created_driver)

        # Second get should return cached instance
        retrieved2 = self.registry.get_driver("test")
        self.assertIs(retrieved2, created_driver)

    def test_create_from_config(self) -> None:
        """Test creating driver from config."""
        # Set up config using the proper Config interface
        self.config.set(("filter", "test"), "clean", b"cat")
        self.config.set(("filter", "test"), "smudge", b"tac")

        # Get driver (should be created from config)
        driver = self.registry.get_driver("test")
        self.assertIsNotNone(driver)
        self.assertIsInstance(driver, ProcessFilterDriver)
        self.assertEqual(driver.clean_cmd, "cat")
        self.assertEqual(driver.smudge_cmd, "tac")

    def test_builtin_lfs_factory(self) -> None:
        """Test that LFS filter is available as a built-in."""
        from dulwich.lfs import LFSFilterDriver

        # Should be able to get LFS filter without explicit registration
        driver = self.registry.get_driver("lfs")
        self.assertIsNotNone(driver)
        self.assertIsInstance(driver, LFSFilterDriver)


class GetFilterForPathTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.registry = FilterRegistry()
        self.driver = ProcessFilterDriver(clean_cmd="cat")
        self.registry.register_driver("test", self.driver)

    def test_get_filter_for_path(self) -> None:
        """Test getting filter for a path with filter attribute."""
        gitattributes = {
            b"*.txt": {b"filter": b"test"},
        }

        result = get_filter_for_path(b"file.txt", gitattributes, self.registry)
        self.assertIs(result, self.driver)

    def test_no_filter_attribute(self) -> None:
        """Test path with no filter attribute."""
        gitattributes = {
            b"*.txt": {b"text": b"auto"},
        }

        result = get_filter_for_path(b"file.txt", gitattributes, self.registry)
        self.assertIsNone(result)

    def test_no_matching_pattern(self) -> None:
        """Test path with no matching pattern."""
        gitattributes = {
            b"*.jpg": {b"filter": b"test"},
        }

        result = get_filter_for_path(b"file.txt", gitattributes, self.registry)
        self.assertIsNone(result)

    def test_filter_not_registered(self) -> None:
        """Test path with filter that's not registered."""
        gitattributes = {
            b"*.txt": {b"filter": b"nonexistent"},
        }

        result = get_filter_for_path(b"file.txt", gitattributes, self.registry)
        self.assertIsNone(result)


class FilterBlobNormalizerTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.config = ConfigDict()
        self.registry = FilterRegistry(self.config)
        self.gitattributes = {}
        self.normalizer = FilterBlobNormalizer(
            self.config, self.gitattributes, self.registry
        )

    def test_no_filter(self) -> None:
        """Test normalizer with no filter defined."""
        blob = Blob()
        blob.data = b"test content"

        # Both checkin and checkout should return blob unchanged
        result = self.normalizer.checkin_normalize(blob, b"file.txt")
        self.assertIs(result, blob)

        result = self.normalizer.checkout_normalize(blob, b"file.txt")
        self.assertIs(result, blob)

    def test_with_filter(self) -> None:
        """Test normalizer with a filter defined."""

        # Create a simple filter that converts to uppercase on clean
        # and lowercase on smudge
        class TestFilter:
            def clean(self, data):
                return data.upper()

            def smudge(self, data):
                return data.lower()

        # Register the filter and set it in gitattributes
        self.registry.register_driver("test", TestFilter())
        self.gitattributes[b"*.txt"] = {b"filter": b"test"}

        blob = Blob()
        blob.data = b"Test Content"

        # Checkin should uppercase
        result = self.normalizer.checkin_normalize(blob, b"file.txt")
        self.assertEqual(result.data, b"TEST CONTENT")
        self.assertIsNot(result, blob)  # Should be a new blob

        # Checkout should lowercase
        result = self.normalizer.checkout_normalize(blob, b"file.txt")
        self.assertEqual(result.data, b"test content")
        self.assertIsNot(result, blob)  # Should be a new blob

    def test_filter_returns_same_data(self) -> None:
        """Test that normalizer returns same blob if filter doesn't change data."""

        # Create a filter that returns data unchanged
        class NoOpFilter:
            def clean(self, data):
                return data

            def smudge(self, data):
                return data

        self.registry.register_driver("noop", NoOpFilter())
        self.gitattributes[b"*.txt"] = {b"filter": b"noop"}

        blob = Blob()
        blob.data = b"unchanged content"

        # Both operations should return the same blob instance
        result = self.normalizer.checkin_normalize(blob, b"file.txt")
        self.assertIs(result, blob)

        result = self.normalizer.checkout_normalize(blob, b"file.txt")
        self.assertIs(result, blob)
