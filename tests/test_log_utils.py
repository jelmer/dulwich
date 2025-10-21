# test_log_utils.py -- Tests for log_utils.py
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.log_utils."""

import logging
import os
import tempfile

from dulwich.log_utils import (
    _DULWICH_LOGGER,
    _NULL_HANDLER,
    _get_trace_target,
    _NullHandler,
    _should_trace,
    default_logging_config,
    getLogger,
    remove_null_handler,
)

from . import TestCase


class LogUtilsTests(TestCase):
    """Tests for log_utils."""

    def setUp(self) -> None:
        super().setUp()
        # Save original handler configuration
        self.original_handlers = list(_DULWICH_LOGGER.handlers)
        # Save original GIT_TRACE value
        self.original_git_trace = os.environ.get("GIT_TRACE")

    def tearDown(self) -> None:
        # Restore original handler configuration
        _DULWICH_LOGGER.handlers = self.original_handlers
        # Restore original GIT_TRACE value
        if self.original_git_trace is None:
            os.environ.pop("GIT_TRACE", None)
        else:
            os.environ["GIT_TRACE"] = self.original_git_trace
        super().tearDown()

    def _set_git_trace(self, value: str | None) -> None:
        """Helper to set GIT_TRACE environment variable."""
        if value is None:
            os.environ.pop("GIT_TRACE", None)
        else:
            os.environ["GIT_TRACE"] = value

    def test_null_handler(self) -> None:
        """Test the _NullHandler class."""
        handler = _NullHandler()
        # Create a test record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test_log_utils.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        # Should not raise any exceptions
        handler.emit(record)

    def test_get_logger(self) -> None:
        """Test the getLogger function."""
        # Should return a logger instance
        logger = getLogger("dulwich.test")
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "dulwich.test")

    def test_remove_null_handler(self) -> None:
        """Test removing the null handler."""
        # Make sure _NULL_HANDLER is in the handlers
        if _NULL_HANDLER not in _DULWICH_LOGGER.handlers:
            _DULWICH_LOGGER.addHandler(_NULL_HANDLER)

        # Remove the null handler
        remove_null_handler()

        # Check that it was removed
        self.assertNotIn(_NULL_HANDLER, _DULWICH_LOGGER.handlers)

    def test_default_logging_config(self) -> None:
        """Test the default logging configuration."""
        # Apply default config
        default_logging_config()

        # Check that the null handler was removed
        self.assertNotIn(_NULL_HANDLER, _DULWICH_LOGGER.handlers)

        # Check that the root logger has a handler
        root_logger = logging.getLogger()
        self.assertTrue(root_logger.handlers)

        # Reset the root logger to not affect other tests
        root_logger.handlers = []

    def test_should_trace_disabled(self) -> None:
        """Test _should_trace with tracing disabled."""
        # Test with unset environment variable
        self._set_git_trace(None)
        self.assertFalse(_should_trace())

        # Test with empty string
        self._set_git_trace("")
        self.assertFalse(_should_trace())

        # Test with "0"
        self._set_git_trace("0")
        self.assertFalse(_should_trace())

        # Test with "false" (case insensitive)
        self._set_git_trace("false")
        self.assertFalse(_should_trace())

        self._set_git_trace("FALSE")
        self.assertFalse(_should_trace())

    def test_should_trace_enabled(self) -> None:
        """Test _should_trace with tracing enabled."""
        self._set_git_trace("1")
        self.assertTrue(_should_trace())

        self._set_git_trace("/tmp/trace.log")
        self.assertTrue(_should_trace())

    def test_get_trace_target_disabled(self) -> None:
        """Test _get_trace_target with tracing disabled."""
        # Test with unset environment variable
        self._set_git_trace(None)
        self.assertIsNone(_get_trace_target())

        # Test with empty string
        self._set_git_trace("")
        self.assertIsNone(_get_trace_target())

        # Test with "0"
        self._set_git_trace("0")
        self.assertIsNone(_get_trace_target())

        # Test with "false"
        self._set_git_trace("false")
        self.assertIsNone(_get_trace_target())

    def test_get_trace_target_stderr(self) -> None:
        """Test _get_trace_target with stderr output."""
        # Test with "1"
        self._set_git_trace("1")
        self.assertEqual(_get_trace_target(), 2)

        # Test with "2"
        self._set_git_trace("2")
        self.assertEqual(_get_trace_target(), 2)

        # Test with "true" (case insensitive)
        self._set_git_trace("true")
        self.assertEqual(_get_trace_target(), 2)

        self._set_git_trace("TRUE")
        self.assertEqual(_get_trace_target(), 2)

    def test_get_trace_target_file_descriptor(self) -> None:
        """Test _get_trace_target with file descriptor."""
        for fd in range(3, 10):
            self._set_git_trace(str(fd))
            self.assertEqual(_get_trace_target(), fd)

        # Test out of range values
        self._set_git_trace("10")
        self.assertIsNone(_get_trace_target())

        self._set_git_trace("2")
        self.assertEqual(_get_trace_target(), 2)  # Special case: stderr

    def test_get_trace_target_file(self) -> None:
        """Test _get_trace_target with file path."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            trace_file = f.name

        try:
            self._set_git_trace(trace_file)
            self.assertEqual(_get_trace_target(), trace_file)
        finally:
            if os.path.exists(trace_file):
                os.unlink(trace_file)

    def test_get_trace_target_directory(self) -> None:
        """Test _get_trace_target with directory path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._set_git_trace(tmpdir)
            self.assertEqual(_get_trace_target(), tmpdir)

    def test_get_trace_target_other_values(self) -> None:
        """Test _get_trace_target with other values."""
        # Any non-absolute path should be treated as disabled
        self._set_git_trace("relative/path")
        self.assertIsNone(_get_trace_target())

    def test_default_logging_config_with_trace(self) -> None:
        """Test default_logging_config with GIT_TRACE enabled."""
        # Save current root logger state
        root_logger = logging.getLogger()
        original_level = root_logger.level
        original_handlers = list(root_logger.handlers)

        # Clean up after test
        def cleanup() -> None:
            root_logger.handlers = original_handlers
            root_logger.level = original_level

        self.addCleanup(cleanup)

        # Reset root logger to ensure clean state
        root_logger.handlers = []
        root_logger.level = logging.WARNING

        self._set_git_trace("1")
        default_logging_config()

        # Check that the null handler was removed
        self.assertNotIn(_NULL_HANDLER, _DULWICH_LOGGER.handlers)

        # Check that the root logger has a handler
        self.assertTrue(root_logger.handlers)

        # Check that the level is DEBUG when tracing is enabled
        self.assertEqual(root_logger.level, logging.DEBUG)
