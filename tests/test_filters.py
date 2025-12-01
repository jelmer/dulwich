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
import shutil
import sys
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from dulwich import porcelain
from dulwich.filters import (
    FilterContext,
    FilterError,
    FilterRegistry,
    ProcessFilterDriver,
)
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

    def test_gitattributes_custom_filter(self) -> None:
        """Test custom filter specified in gitattributes."""
        # Create a Python script that acts as our filter
        import sys

        filter_script = os.path.join(self.test_dir, "redact_filter.py")
        with open(filter_script, "w") as f:
            f.write(
                """#!/usr/bin/env python3
import sys
data = sys.stdin.buffer.read()
# Replace all digits with X
result = bytearray()
for b in data:
    if chr(b).isdigit():
        result.append(ord('X'))
    else:
        result.append(b)
sys.stdout.buffer.write(result)
"""
            )
        os.chmod(filter_script, 0o755)

        # Create .gitattributes with custom filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.secret filter=redact\n")

        # Configure custom filter (use Python script for testing)
        config = self.repo.get_config()
        # This filter replaces all digits with X
        config.set(
            (b"filter", b"redact"),
            b"clean",
            f"{sys.executable} {filter_script}".encode(),
        )
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

    def test_filter_precedence(self) -> None:
        """Test that filter attribute takes precedence over text attribute."""
        # Create a Python script that converts to uppercase
        import sys

        filter_script = os.path.join(self.test_dir, "uppercase_filter.py")
        with open(filter_script, "w") as f:
            f.write(
                """#!/usr/bin/env python3
import sys
data = sys.stdin.buffer.read()
# Convert bytes to string, uppercase, then back to bytes
result = data.decode('utf-8', errors='replace').upper().encode('utf-8')
sys.stdout.buffer.write(result)
"""
            )
        os.chmod(filter_script, 0o755)

        # Create .gitattributes with both text and filter
        gitattributes_path = os.path.join(self.test_dir, ".gitattributes")
        with open(gitattributes_path, "wb") as f:
            f.write(b"*.txt text filter=custom\n")

        # Configure autocrlf and custom filter
        config = self.repo.get_config()
        config.set((b"core",), b"autocrlf", b"true")
        # This filter converts to uppercase
        config.set(
            (b"filter", b"custom"),
            b"clean",
            f"{sys.executable} {filter_script}".encode(),
        )
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


class ProcessFilterDriverTests(TestCase):
    """Tests for ProcessFilterDriver with real process filter."""

    def setUp(self):
        super().setUp()
        # Create a temporary test filter process dynamically
        self.test_filter_path = self._create_test_filter()

    def tearDown(self):
        # Clean up the test filter
        if hasattr(self, "test_filter_path") and os.path.exists(self.test_filter_path):
            os.unlink(self.test_filter_path)
        super().tearDown()

    def _create_test_filter(self):
        """Create a simple test filter process that works on all platforms."""
        import tempfile

        # Create filter script that uppercases on clean, lowercases on smudge
        filter_script = """import sys
import os

# Simple filter that doesn't use any external dependencies
def read_exact(n):
    data = b""
    while len(data) < n:
        chunk = sys.stdin.buffer.read(n - len(data))
        if not chunk:
            break
        data += chunk
    return data

def write_pkt(data):
    if data is None:
        sys.stdout.buffer.write(b"0000")
    else:
        length = len(data) + 4
        sys.stdout.buffer.write(("{:04x}".format(length)).encode())
        sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

def read_pkt():
    size_bytes = read_exact(4)
    if not size_bytes:
        return None
    size = int(size_bytes.decode(), 16)
    if size == 0:
        return None
    return read_exact(size - 4)

# Handshake
client_hello = read_pkt()
version = read_pkt()
flush = read_pkt()

write_pkt(b"git-filter-server")
write_pkt(b"version=2")
write_pkt(None)

# Read and echo capabilities
caps = []
while True:
    cap = read_pkt()
    if cap is None:
        break
    caps.append(cap)

for cap in caps:
    write_pkt(cap)
write_pkt(None)

# Process commands
while True:
    headers = {}
    while True:
        line = read_pkt()
        if line is None:
            break
        if b"=" in line:
            k, v = line.split(b"=", 1)
            headers[k.decode()] = v.decode()
    
    if not headers:
        break
    
    # Read data
    data_chunks = []
    while True:
        chunk = read_pkt()
        if chunk is None:
            break
        data_chunks.append(chunk)
    
    data = b"".join(data_chunks)
    
    # Process (uppercase for clean, lowercase for smudge)
    if headers.get("command") == "clean":
        result = data.upper()
    elif headers.get("command") == "smudge":
        result = data.lower()
    else:
        result = data
    
    # Send response
    write_pkt(b"status=success")
    write_pkt(None)

    # Send result
    chunk_size = 65516
    for i in range(0, len(result), chunk_size):
        write_pkt(result[i:i+chunk_size])
    write_pkt(None)

    # Send final headers (empty list to keep status=success)
    write_pkt(None)
"""

        # Create temporary file
        fd, path = tempfile.mkstemp(suffix=".py", prefix="test_filter_")
        try:
            os.write(fd, filter_script.encode())
            os.close(fd)

            # Make executable on Unix-like systems
            if os.name != "nt":  # Not Windows
                os.chmod(path, 0o755)

            return path
        except:
            if os.path.exists(path):
                os.unlink(path)
            raise

    def test_process_filter_clean_operation(self):
        """Test clean operation using real process filter."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        test_data = b"hello world"
        result = driver.clean(test_data)

        # Our test filter uppercases on clean
        self.assertEqual(result, b"HELLO WORLD")

    def test_process_filter_smudge_operation(self):
        """Test smudge operation using real process filter."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        test_data = b"HELLO WORLD"
        result = driver.smudge(test_data, b"test.txt")

        # Our test filter lowercases on smudge
        self.assertEqual(result, b"hello world")

    def test_process_filter_large_data(self):
        """Test process filter with data larger than single pkt-line."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        # Create data larger than max pkt-line payload (65516 bytes)
        test_data = b"a" * 70000
        result = driver.clean(test_data)

        # Should be uppercased
        self.assertEqual(result, b"A" * 70000)

    def test_fallback_to_individual_commands(self):
        """Test fallback when process filter fails."""
        driver = ProcessFilterDriver(
            clean_cmd="tr '[:lower:]' '[:upper:]'",  # Shell command to uppercase
            process_cmd="/nonexistent/command",  # This should fail
            required=False,
        )

        test_data = b"hello world\n"
        result = driver.clean(test_data)

        # Should fallback to tr command and uppercase
        self.assertEqual(result, b"HELLO WORLD\n")

    def test_process_reuse(self):
        """Test that process is reused across multiple operations."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        # First operation
        result1 = driver.clean(b"test1")
        self.assertEqual(result1, b"TEST1")

        # Second operation should reuse the same process
        result2 = driver.clean(b"test2")
        self.assertEqual(result2, b"TEST2")

        # Process should still be alive
        self.assertIsNotNone(driver._process)
        self.assertIsNone(driver._process.poll())  # None means still running

    def test_error_handling_invalid_command(self):
        """Test error handling with invalid filter command."""
        driver = ProcessFilterDriver(process_cmd="/nonexistent/command", required=True)

        with self.assertRaises(FilterError) as cm:
            driver.clean(b"test data")

        self.assertIn("Failed to start process filter", str(cm.exception))


class FilterContextTests(TestCase):
    """Tests for FilterContext class."""

    def test_filter_context_caches_long_running_drivers(self):
        """Test that FilterContext caches only long-running drivers."""

        # Create real filter drivers
        class UppercaseFilter:
            def clean(self, data):
                return data.upper()

            def smudge(self, data, path=b""):
                return data.lower()

            def cleanup(self):
                pass

            def reuse(self, config, filter_name):
                # Pretend it's a long-running filter that should be cached
                return True

        class IdentityFilter:
            def clean(self, data):
                return data

            def smudge(self, data, path=b""):
                return data

            def cleanup(self):
                pass

            def reuse(self, config, filter_name):
                # Lightweight filter, don't cache
                return False

        # Create registry and context
        # Need to provide a config for caching to work
        from dulwich.config import ConfigDict

        config = ConfigDict()
        # Add some dummy config to make it truthy (use proper format)
        config.set((b"filter", b"uppercase"), b"clean", b"dummy")
        registry = FilterRegistry(config=config)
        context = FilterContext(registry)

        # Register drivers
        long_running = UppercaseFilter()
        stateless = IdentityFilter()
        registry.register_driver("uppercase", long_running)
        registry.register_driver("identity", stateless)

        # Get drivers through context
        driver1 = context.get_driver("uppercase")
        driver2 = context.get_driver("uppercase")

        # Long-running driver should be cached
        self.assertIs(driver1, driver2)
        self.assertIs(driver1, long_running)

        # Get stateless driver
        stateless1 = context.get_driver("identity")
        stateless2 = context.get_driver("identity")

        # Stateless driver comes from registry but isn't cached in context
        self.assertIs(stateless1, stateless)
        self.assertIs(stateless2, stateless)
        self.assertNotIn("identity", context._active_drivers)
        self.assertIn("uppercase", context._active_drivers)

    def test_filter_context_cleanup(self):
        """Test that FilterContext properly cleans up resources."""
        cleanup_called = []

        class TrackableFilter:
            def __init__(self, name):
                self.name = name

            def clean(self, data):
                return data

            def smudge(self, data, path=b""):
                return data

            def cleanup(self):
                cleanup_called.append(self.name)

            def is_long_running(self):
                return True

        # Create registry and context
        registry = FilterRegistry()
        context = FilterContext(registry)

        # Register and use drivers
        filter1 = TrackableFilter("filter1")
        filter2 = TrackableFilter("filter2")
        filter3 = TrackableFilter("filter3")
        registry.register_driver("filter1", filter1)
        registry.register_driver("filter2", filter2)
        registry.register_driver("filter3", filter3)

        # Get only some drivers to cache them
        context.get_driver("filter1")
        context.get_driver("filter2")
        # Don't get filter3

        # Close context
        context.close()

        # Verify cleanup was called for all drivers (context closes registry too)
        self.assertEqual(set(cleanup_called), {"filter1", "filter2", "filter3"})

    def test_filter_context_get_driver_returns_none_for_missing(self):
        """Test that get_driver returns None for non-existent drivers."""
        registry = FilterRegistry()
        context = FilterContext(registry)

        result = context.get_driver("nonexistent")
        self.assertIsNone(result)

    def test_filter_context_with_real_process_filter(self):
        """Test FilterContext with real ProcessFilterDriver instances."""
        # Use existing test filter from ProcessFilterDriverTests
        test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, test_dir)

        # Create a simple test filter that just passes data through
        filter_script = _PASSTHROUGH_FILTER_SCRIPT
        filter_path = os.path.join(test_dir, "simple_filter.py")
        with open(filter_path, "w") as f:
            f.write(filter_script)

        # Create ProcessFilterDriver instances
        # One with process_cmd (long-running)
        process_driver = ProcessFilterDriver(
            process_cmd=None,  # Don't use actual process to avoid complexity
            clean_cmd=f"{sys.executable} {filter_path}",
            smudge_cmd=f"{sys.executable} {filter_path}",
        )

        # Register in context
        from dulwich.config import ConfigDict

        config = ConfigDict()
        # Add some dummy config to make it truthy (use proper format)
        config.set(
            (b"filter", b"process"),
            b"clean",
            f"{sys.executable} {filter_path}".encode(),
        )
        config.set(
            (b"filter", b"process"),
            b"smudge",
            f"{sys.executable} {filter_path}".encode(),
        )
        registry = FilterRegistry(config=config)
        context = FilterContext(registry)
        registry.register_driver("process", process_driver)

        # Get driver - should not be cached since it's not long-running
        driver1 = context.get_driver("process")
        self.assertIsNotNone(driver1)
        # Check that it's not a long-running process (no process_cmd)
        self.assertIsNone(driver1.process_cmd)
        self.assertNotIn("process", context._active_drivers)

        # Test with a long-running driver that should be cached
        # Create a mock driver that always wants to be reused
        class CacheableProcessDriver:
            def __init__(self):
                self.process_cmd = "dummy"
                self.clean_cmd = None
                self.smudge_cmd = None
                self.required = False

            def clean(self, data):
                return data

            def smudge(self, data, path=b""):
                return data

            def cleanup(self):
                pass

            def reuse(self, config, filter_name):
                # This driver always wants to be cached (simulates a long-running process)
                return True

        cacheable_driver = CacheableProcessDriver()
        registry.register_driver("long_process", cacheable_driver)

        driver2 = context.get_driver("long_process")
        # Check that it has a process_cmd (long-running)
        self.assertIsNotNone(driver2.process_cmd)
        self.assertIn("long_process", context._active_drivers)

        context.close()

    def test_filter_context_closes_registry(self):
        """Test that closing FilterContext also closes the registry."""
        # Track if registry.close() is called
        registry_closed = []

        class TrackingRegistry(FilterRegistry):
            def close(self):
                registry_closed.append(True)
                super().close()

        registry = TrackingRegistry()
        context = FilterContext(registry)

        # Close context should also close registry
        context.close()
        self.assertTrue(registry_closed)


class ProcessFilterProtocolTests(TestCase):
    """Tests for ProcessFilterDriver protocol compliance."""

    def setUp(self):
        super().setUp()
        # Create a spec-compliant test filter process dynamically
        self.test_filter_path = self._create_spec_compliant_filter()

    def tearDown(self):
        # Clean up the test filter
        if hasattr(self, "test_filter_path") and os.path.exists(self.test_filter_path):
            os.unlink(self.test_filter_path)
        super().tearDown()

    def _create_spec_compliant_filter(self):
        """Create a spec-compliant test filter that works on all platforms."""
        import tempfile

        # This filter strictly follows Git spec - no newlines in packets
        filter_script = """import sys

def read_exact(n):
    data = b""
    while len(data) < n:
        chunk = sys.stdin.buffer.read(n - len(data))
        if not chunk:
            break
        data += chunk
    return data

def write_pkt(data):
    if data is None:
        sys.stdout.buffer.write(b"0000")
    else:
        length = len(data) + 4
        sys.stdout.buffer.write(("{:04x}".format(length)).encode())
        sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

def read_pkt():
    size_bytes = read_exact(4)
    if not size_bytes:
        return None
    size = int(size_bytes.decode(), 16)
    if size == 0:
        return None
    return read_exact(size - 4)

# Handshake - exact format, no newlines
client_hello = read_pkt()
version = read_pkt()
flush = read_pkt()

if client_hello != b"git-filter-client":
    sys.exit(1)
if version != b"version=2":
    sys.exit(1)

write_pkt(b"git-filter-server")  # No newline
write_pkt(b"version=2")  # No newline
write_pkt(None)

# Read and echo capabilities
caps = []
while True:
    cap = read_pkt()
    if cap is None:
        break
    caps.append(cap)

for cap in caps:
    if cap in [b"capability=clean", b"capability=smudge"]:
        write_pkt(cap)
write_pkt(None)

# Process commands
while True:
    headers = {}
    while True:
        line = read_pkt()
        if line is None:
            break
        if b"=" in line:
            k, v = line.split(b"=", 1)
            headers[k.decode()] = v.decode()
    
    if not headers:
        break
    
    # Read data
    data_chunks = []
    while True:
        chunk = read_pkt()
        if chunk is None:
            break
        data_chunks.append(chunk)
    
    data = b"".join(data_chunks)
    
    # Process
    if headers.get("command") == "clean":
        result = data.upper()
    elif headers.get("command") == "smudge":
        result = data.lower()
    else:
        result = data
    
    # Send response
    write_pkt(b"status=success")
    write_pkt(None)

    # Send result
    chunk_size = 65516
    for i in range(0, len(result), chunk_size):
        write_pkt(result[i:i+chunk_size])
    write_pkt(None)

    # Send final headers (empty list to keep status=success)
    write_pkt(None)
"""

        fd, path = tempfile.mkstemp(suffix=".py", prefix="test_filter_spec_")
        try:
            os.write(fd, filter_script.encode())
            os.close(fd)

            if os.name != "nt":  # Not Windows
                os.chmod(path, 0o755)

            return path
        except:
            if os.path.exists(path):
                os.unlink(path)
            raise

    def test_protocol_handshake_exact_format(self):
        """Test that handshake uses exact format without newlines."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}",
            required=True,  # Require success to test protocol compliance
        )

        # This should work with exact protocol format
        test_data = b"hello world"
        result = driver.clean(test_data)

        # Our test filter uppercases on clean
        self.assertEqual(result, b"HELLO WORLD")

    def test_capability_negotiation_exact_format(self):
        """Test that capabilities are sent and received in exact format."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=True
        )

        # Force capability negotiation by using both clean and smudge
        clean_result = driver.clean(b"test")
        smudge_result = driver.smudge(b"TEST", b"test.txt")

        self.assertEqual(clean_result, b"TEST")
        self.assertEqual(smudge_result, b"test")

    def test_binary_data_handling(self):
        """Test handling of binary data through the protocol."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        # Binary data with null bytes, high bytes, etc.
        binary_data = bytes(range(256))

        result = driver.clean(binary_data)
        # Should handle binary data without crashing
        self.assertIsInstance(result, bytes)
        # Our test filter uppercases bytes directly, which works for binary data
        # The fix ensures headers are kept as bytes, so binary content doesn't cause decode errors

    def test_binary_data_with_invalid_utf8_sequences(self):
        """Test handling of binary data with invalid UTF-8 sequences.

        Regression test for https://github.com/jelmer/dulwich/issues/2023
        where binary files (like .ogg, .jpg) caused UTF-8 decode errors.
        """
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        # Create binary data with the specific byte that caused the issue (0xe5 at position 14)
        # plus other invalid UTF-8 sequences
        binary_data = b"some header \xe5\xff\xfe binary data"

        result = driver.clean(binary_data)
        # Should handle binary data without UTF-8 decode errors
        self.assertIsInstance(result, bytes)
        # The filter should process it successfully
        self.assertEqual(result, binary_data.upper())

    def test_large_file_chunking(self):
        """Test proper chunking of large files."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=True
        )

        # Create data larger than max pkt-line payload (65516 bytes)
        large_data = b"a" * 100000
        result = driver.clean(large_data)

        # Should be properly processed (uppercased)
        expected = b"A" * 100000
        self.assertEqual(result, expected)

    def test_empty_file_handling(self):
        """Test handling of empty files."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=True
        )

        result = driver.clean(b"")
        self.assertEqual(result, b"")

    def test_special_characters_in_pathname(self):
        """Test paths with special characters are handled correctly."""
        import sys

        # Test various special characters in paths
        special_paths = [
            b"file with spaces.txt",
            b"path/with/slashes.txt",
            b"file=with=equals.txt",
            b"file\nwith\nnewlines.txt",
            b"filew&with&ampersand.txt",
        ]

        test_data = b"test data"

        with create_passthrough_filter() as passthrough_filter_path:
            for process_cmd, smudge_cmd in [
                (f"{sys.executable} {self.test_filter_path}", None),
                (None, f"{sys.executable} {passthrough_filter_path} %f"),
            ]:
                driver = ProcessFilterDriver(
                    process_cmd=process_cmd,
                    smudge_cmd=smudge_cmd,
                    required=True,
                )
                for path in special_paths:
                    with self.subTest(
                        process_cmd=process_cmd, smudge_cmd=smudge_cmd, path=path
                    ):
                        result = driver.smudge(test_data, path)
                        self.assertEqual(result, b"test data")

    def test_process_crash_recovery(self):
        """Test that process is properly restarted after crash."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        # First operation
        result = driver.clean(b"test1")
        self.assertEqual(result, b"TEST1")

        # Kill the process
        if driver._process:
            driver._process.kill()
            driver._process.wait()
        driver.cleanup()

        # Should restart and work again
        result = driver.clean(b"test2")
        self.assertEqual(result, b"TEST2")

    def test_malformed_process_response_handling(self):
        """Test handling of malformed responses from process."""
        # Create a filter that sends malformed responses
        malformed_filter = """#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from dulwich.protocol import Protocol

protocol = Protocol(
    lambda n: sys.stdin.buffer.read(n),
    lambda d: sys.stdout.buffer.write(d) or len(d)
)

# Read handshake
protocol.read_pkt_line()
protocol.read_pkt_line()
protocol.read_pkt_line()

# Send invalid handshake
protocol.write_pkt_line(b"invalid-welcome")
protocol.write_pkt_line(b"version=2")
protocol.write_pkt_line(None)
"""

        import tempfile

        fd, script_path = tempfile.mkstemp(suffix=".py")
        try:
            os.write(fd, malformed_filter.encode())
            os.close(fd)
            os.chmod(script_path, 0o755)

            driver = ProcessFilterDriver(
                process_cmd=f"python3 {script_path}",
                clean_cmd="cat",  # Fallback
                required=False,
            )

            # Should fallback to clean_cmd when process fails
            result = driver.clean(b"test data")
            self.assertEqual(result, b"test data")

        finally:
            os.unlink(script_path)

    def test_concurrent_filter_operations(self):
        """Test that concurrent operations work correctly."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=True
        )

        results = []
        errors = []

        def worker(data):
            try:
                result = driver.clean(data)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Start 5 concurrent operations
        threads = []
        test_data = [f"test{i}".encode() for i in range(5)]

        for data in test_data:
            t = threading.Thread(target=worker, args=(data,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should have no errors
        self.assertEqual(len(errors), 0, f"Errors: {errors}")
        self.assertEqual(len(results), 5)

        # All results should be uppercase versions
        expected = [data.upper() for data in test_data]
        self.assertEqual(sorted(results), sorted(expected))

    def test_process_resource_cleanup(self):
        """Test that process resources are properly cleaned up."""
        import sys

        driver = ProcessFilterDriver(
            process_cmd=f"{sys.executable} {self.test_filter_path}", required=False
        )

        # Use the driver
        result = driver.clean(b"test")
        self.assertEqual(result, b"TEST")

        # Process should be running
        self.assertIsNotNone(driver._process)
        self.assertIsNone(driver._process.poll())  # None means still running

        # Remember the old process to check it was terminated
        old_process = driver._process

        # Manually clean up (simulates __del__)
        driver.cleanup()

        # Process reference should be cleared
        self.assertIsNone(driver._process)
        self.assertIsNone(driver._protocol)

        # Old process should be terminated
        self.assertIsNotNone(old_process.poll())  # Not None means terminated

    def test_required_filter_error_propagation(self):
        """Test that errors are properly propagated when filter is required."""
        driver = ProcessFilterDriver(
            process_cmd="/definitely/nonexistent/command", required=True
        )

        with self.assertRaises(FilterError) as cm:
            driver.clean(b"test data")

        self.assertIn("Failed to start process filter", str(cm.exception))

    def test_two_phase_response_protocol(self):
        """Test filter protocol with two-phase response (initial + final headers).

        This test verifies that the filter correctly handles the Git LFS protocol
        where filters send:
        1. Initial headers with status
        2. Content data
        3. Final headers with status

        This is the format used by git-lfs and documented in the Git filter protocol.
        """
        import sys
        import tempfile

        # Create a filter that follows the two-phase protocol
        filter_script = """import sys

def read_exact(n):
    data = b""
    while len(data) < n:
        chunk = sys.stdin.buffer.read(n - len(data))
        if not chunk:
            break
        data += chunk
    return data

def write_pkt(data):
    if data is None:
        sys.stdout.buffer.write(b"0000")
    else:
        length = len(data) + 4
        sys.stdout.buffer.write(("{:04x}".format(length)).encode())
        sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

def read_pkt():
    size_bytes = read_exact(4)
    if not size_bytes:
        return None
    size = int(size_bytes.decode(), 16)
    if size == 0:
        return None
    return read_exact(size - 4)

# Handshake
client_hello = read_pkt()
version = read_pkt()
flush = read_pkt()

write_pkt(b"git-filter-server")
write_pkt(b"version=2")
write_pkt(None)

# Read and echo capabilities
caps = []
while True:
    cap = read_pkt()
    if cap is None:
        break
    caps.append(cap)

for cap in caps:
    write_pkt(cap)
write_pkt(None)

# Process commands
while True:
    headers = {}
    while True:
        line = read_pkt()
        if line is None:
            break
        if b"=" in line:
            k, v = line.split(b"=", 1)
            headers[k.decode()] = v.decode()

    if not headers:
        break

    # Read data
    data_chunks = []
    while True:
        chunk = read_pkt()
        if chunk is None:
            break
        data_chunks.append(chunk)

    data = b"".join(data_chunks)

    # Process
    if headers.get("command") == "clean":
        result = data.upper()
    elif headers.get("command") == "smudge":
        result = data.lower()
    else:
        result = data

    # TWO-PHASE RESPONSE: Send initial headers
    write_pkt(b"status=success")
    write_pkt(None)

    # Send result data
    chunk_size = 65516
    for i in range(0, len(result), chunk_size):
        write_pkt(result[i:i+chunk_size])
    write_pkt(None)

    # TWO-PHASE RESPONSE: Send final headers (empty list to keep status=success)
    write_pkt(None)
"""

        fd, filter_path = tempfile.mkstemp(
            suffix=".py", prefix="test_filter_two_phase_"
        )
        try:
            os.write(fd, filter_script.encode())
            os.close(fd)

            if os.name != "nt":
                os.chmod(filter_path, 0o755)

            driver = ProcessFilterDriver(
                process_cmd=f"{sys.executable} {filter_path}", required=True
            )

            # Test clean operation
            test_data = b"hello world"
            result = driver.clean(test_data)
            self.assertEqual(result, b"HELLO WORLD")

            # Test smudge operation
            result = driver.smudge(b"HELLO WORLD", b"test.txt")
            self.assertEqual(result, b"hello world")

            driver.cleanup()

        finally:
            if os.path.exists(filter_path):
                os.unlink(filter_path)

    def test_two_phase_response_with_status_messages(self):
        """Test filter that sends status messages in final headers.

        Some filters (like git-lfs) may send progress or status messages
        in the final headers. This test verifies that we can handle those.
        """
        import sys
        import tempfile

        # Create a filter that sends extra status info in final headers
        filter_script = """import sys

def read_exact(n):
    data = b""
    while len(data) < n:
        chunk = sys.stdin.buffer.read(n - len(data))
        if not chunk:
            break
        data += chunk
    return data

def write_pkt(data):
    if data is None:
        sys.stdout.buffer.write(b"0000")
    else:
        length = len(data) + 4
        sys.stdout.buffer.write(("{:04x}".format(length)).encode())
        sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

def read_pkt():
    size_bytes = read_exact(4)
    if not size_bytes:
        return None
    size = int(size_bytes.decode(), 16)
    if size == 0:
        return None
    return read_exact(size - 4)

# Handshake
client_hello = read_pkt()
version = read_pkt()
flush = read_pkt()

write_pkt(b"git-filter-server")
write_pkt(b"version=2")
write_pkt(None)

# Read and echo capabilities
caps = []
while True:
    cap = read_pkt()
    if cap is None:
        break
    caps.append(cap)

for cap in caps:
    write_pkt(cap)
write_pkt(None)

# Process commands
while True:
    headers = {}
    while True:
        line = read_pkt()
        if line is None:
            break
        if b"=" in line:
            k, v = line.split(b"=", 1)
            headers[k.decode()] = v.decode()

    if not headers:
        break

    # Read data
    data_chunks = []
    while True:
        chunk = read_pkt()
        if chunk is None:
            break
        data_chunks.append(chunk)

    data = b"".join(data_chunks)

    # Process
    result = data.upper()

    # Send initial headers
    write_pkt(b"status=success")
    write_pkt(None)

    # Send result data
    chunk_size = 65516
    for i in range(0, len(result), chunk_size):
        write_pkt(result[i:i+chunk_size])
    write_pkt(None)

    # Send final headers with progress messages (like git-lfs does)
    write_pkt(b"status=success")
    write_pkt(None)
"""

        fd, filter_path = tempfile.mkstemp(suffix=".py", prefix="test_filter_status_")
        try:
            os.write(fd, filter_script.encode())
            os.close(fd)

            if os.name != "nt":
                os.chmod(filter_path, 0o755)

            driver = ProcessFilterDriver(
                process_cmd=f"{sys.executable} {filter_path}", required=True
            )

            # Test clean operation with status messages
            test_data = b"test data with status"
            result = driver.clean(test_data)
            self.assertEqual(result, b"TEST DATA WITH STATUS")

            driver.cleanup()

        finally:
            if os.path.exists(filter_path):
                os.unlink(filter_path)

    def test_two_phase_response_with_final_error(self):
        """Test filter that reports error in final headers.

        The Git protocol allows filters to report success initially,
        then report an error in the final headers. This test ensures
        we handle that correctly.
        """
        import sys
        import tempfile

        # Create a filter that sends error in final headers
        filter_script = """import sys

def read_exact(n):
    data = b""
    while len(data) < n:
        chunk = sys.stdin.buffer.read(n - len(data))
        if not chunk:
            break
        data += chunk
    return data

def write_pkt(data):
    if data is None:
        sys.stdout.buffer.write(b"0000")
    else:
        length = len(data) + 4
        sys.stdout.buffer.write(("{:04x}".format(length)).encode())
        sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

def read_pkt():
    size_bytes = read_exact(4)
    if not size_bytes:
        return None
    size = int(size_bytes.decode(), 16)
    if size == 0:
        return None
    return read_exact(size - 4)

# Handshake
client_hello = read_pkt()
version = read_pkt()
flush = read_pkt()

write_pkt(b"git-filter-server")
write_pkt(b"version=2")
write_pkt(None)

# Read and echo capabilities
caps = []
while True:
    cap = read_pkt()
    if cap is None:
        break
    caps.append(cap)

for cap in caps:
    write_pkt(cap)
write_pkt(None)

# Process commands
while True:
    headers = {}
    while True:
        line = read_pkt()
        if line is None:
            break
        if b"=" in line:
            k, v = line.split(b"=", 1)
            headers[k.decode()] = v.decode()

    if not headers:
        break

    # Read data
    data_chunks = []
    while True:
        chunk = read_pkt()
        if chunk is None:
            break
        data_chunks.append(chunk)

    data = b"".join(data_chunks)

    # Send initial headers with success
    write_pkt(b"status=success")
    write_pkt(None)

    # Send partial result
    write_pkt(b"PARTIAL")
    write_pkt(None)

    # Send final headers with error (simulating processing failure)
    write_pkt(b"status=error")
    write_pkt(None)
"""

        fd, filter_path = tempfile.mkstemp(suffix=".py", prefix="test_filter_error_")
        try:
            os.write(fd, filter_script.encode())
            os.close(fd)

            if os.name != "nt":
                os.chmod(filter_path, 0o755)

            driver = ProcessFilterDriver(
                process_cmd=f"{sys.executable} {filter_path}", required=True
            )

            # Should raise FilterError due to final status being error
            with self.assertRaises(FilterError) as cm:
                driver.clean(b"test data")

            self.assertIn("final status: error", str(cm.exception))

            driver.cleanup()

        finally:
            if os.path.exists(filter_path):
                os.unlink(filter_path)


_PASSTHROUGH_FILTER_SCRIPT = """import sys
while True:
    line = sys.stdin.buffer.read()
    if not line:
        break
    sys.stdout.buffer.write(line)
    sys.stdout.buffer.flush()
"""


@contextmanager
def create_passthrough_filter() -> Iterator[str]:
    filter_script = _PASSTHROUGH_FILTER_SCRIPT
    with tempfile.NamedTemporaryFile(
        suffix=".py", delete=False, prefix="test_filter_passthrough_"
    ) as f:
        f.write(filter_script.encode())
        path = f.name

    try:
        if os.name != "nt":  # Not Windows
            os.chmod(path, 0o755)
        yield path
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
