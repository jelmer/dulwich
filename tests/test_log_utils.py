# test_log_utils.py -- Tests for log_utils.py
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.log_utils."""

import logging

from dulwich.log_utils import (
    _DULWICH_LOGGER,
    _NULL_HANDLER,
    _NullHandler,
    default_logging_config,
    getLogger,
    remove_null_handler,
)

from . import TestCase


class LogUtilsTests(TestCase):
    """Tests for log_utils."""

    def setUp(self):
        super().setUp()
        # Save original handler configuration
        self.original_handlers = list(_DULWICH_LOGGER.handlers)

    def tearDown(self):
        # Restore original handler configuration
        _DULWICH_LOGGER.handlers = self.original_handlers
        super().tearDown()

    def test_null_handler(self):
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

    def test_get_logger(self):
        """Test the getLogger function."""
        # Should return a logger instance
        logger = getLogger("dulwich.test")
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "dulwich.test")

    def test_remove_null_handler(self):
        """Test removing the null handler."""
        # Make sure _NULL_HANDLER is in the handlers
        if _NULL_HANDLER not in _DULWICH_LOGGER.handlers:
            _DULWICH_LOGGER.addHandler(_NULL_HANDLER)

        # Remove the null handler
        remove_null_handler()

        # Check that it was removed
        self.assertNotIn(_NULL_HANDLER, _DULWICH_LOGGER.handlers)

    def test_default_logging_config(self):
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
