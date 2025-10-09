# log_utils.py -- Logging utilities for Dulwich
# Copyright (C) 2010 Google, Inc.
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

"""Logging utilities for Dulwich.

Any module that uses logging needs to do compile-time initialization to set up
the logging environment. Since Dulwich is also used as a library, clients may
not want to see any logging output. In that case, we need to use a special
handler to suppress spurious warnings like "No handlers could be found for
logger dulwich.foo".

For details on the _NullHandler approach, see:
http://docs.python.org/library/logging.html#configuring-logging-for-a-library

For many modules, the only function from the logging module they need is
getLogger; this module exports that function for convenience. If a calling
module needs something else, it can import the standard logging module
directly.
"""

import logging
import os
import sys
from typing import Optional, Union

getLogger = logging.getLogger


class _NullHandler(logging.Handler):
    """No-op logging handler to avoid unexpected logging warnings."""

    def emit(self, record: logging.LogRecord) -> None:
        pass


_NULL_HANDLER = _NullHandler()
_DULWICH_LOGGER = getLogger("dulwich")
_DULWICH_LOGGER.addHandler(_NULL_HANDLER)


def _should_trace() -> bool:
    """Check if GIT_TRACE is enabled.

    Returns True if tracing should be enabled, False otherwise.
    """
    trace_value = os.environ.get("GIT_TRACE", "")
    if not trace_value or trace_value.lower() in ("0", "false"):
        return False
    return True


def _get_trace_target() -> Optional[Union[str, int]]:
    """Get the trace target from GIT_TRACE environment variable.

    Returns:
        - None if tracing is disabled
        - 2 for stderr output (values "1", "2", "true")
        - int (3-9) for file descriptor
        - str for file path (absolute paths or directories)
    """
    trace_value = os.environ.get("GIT_TRACE", "")

    if not trace_value or trace_value.lower() in ("0", "false"):
        return None

    if trace_value.lower() in ("1", "2", "true"):
        return 2  # stderr

    # Check if it's a file descriptor (integer 3-9)
    try:
        fd = int(trace_value)
        if 3 <= fd <= 9:
            return fd
    except ValueError:
        pass

    # If it's an absolute path, return it as a string
    if os.path.isabs(trace_value):
        return trace_value

    # For any other value, treat it as disabled
    return None


def _configure_logging_from_trace() -> bool:
    """Configure logging based on GIT_TRACE environment variable.

    Returns True if trace configuration was successful, False otherwise.
    """
    trace_target = _get_trace_target()
    if trace_target is None:
        return False

    trace_format = "%(asctime)s %(name)s %(levelname)s: %(message)s"

    if trace_target == 2:
        # stderr
        logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, format=trace_format)
        return True

    if isinstance(trace_target, int):
        # File descriptor
        try:
            stream = os.fdopen(trace_target, "w", buffering=1)
            logging.basicConfig(level=logging.DEBUG, stream=stream, format=trace_format)
            return True
        except OSError as e:
            sys.stderr.write(
                f"Warning: Failed to open GIT_TRACE fd {trace_target}: {e}\n"
            )
            return False

    # File path
    try:
        if os.path.isdir(trace_target):
            # For directories, create a file per process
            filename = os.path.join(trace_target, f"trace.{os.getpid()}")
        else:
            filename = trace_target

        logging.basicConfig(
            level=logging.DEBUG, filename=filename, filemode="a", format=trace_format
        )
        return True
    except OSError as e:
        sys.stderr.write(
            f"Warning: Failed to open GIT_TRACE file {trace_target}: {e}\n"
        )
        return False


def default_logging_config() -> None:
    """Set up the default Dulwich loggers.

    Respects the GIT_TRACE environment variable for trace output:
    - If GIT_TRACE is set to "1", "2", or "true", trace to stderr
    - If GIT_TRACE is set to an integer 3-9, trace to that file descriptor
    - If GIT_TRACE is set to an absolute path, trace to that file
    - If the path is a directory, trace to files in that directory (per process)
    - Otherwise, use default stderr output
    """
    remove_null_handler()

    # Try to configure from GIT_TRACE, fall back to default if it fails
    if not _configure_logging_from_trace():
        logging.basicConfig(
            level=logging.INFO,
            stream=sys.stderr,
            format="%(asctime)s %(levelname)s: %(message)s",
        )


def remove_null_handler() -> None:
    """Remove the null handler from the Dulwich loggers.

    If a caller wants to set up logging using something other than
    default_logging_config, calling this function first is a minor optimization
    to avoid the overhead of using the _NullHandler.
    """
    _DULWICH_LOGGER.removeHandler(_NULL_HANDLER)
