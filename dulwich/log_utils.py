# log_utils.py -- Logging utilities for Dulwich
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor,
# Boston, MA  02110-1301, USA.

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
module needs something else, it can import the standard logging module directly.
"""

import logging
import sys

getLogger = logging.getLogger


class _NullHandler(logging.Handler):
    """No-op logging handler to avoid unexpected logging warnings."""

    def emit(self, record):
        pass


_NULL_HANDLER = _NullHandler()
_DULWICH_LOGGER = getLogger('dulwich')
_DULWICH_LOGGER.addHandler(_NULL_HANDLER)


def default_logging_config():
    """Set up the default Dulwich loggers."""
    remove_null_handler()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format='%(asctime)s %(levelname)s: %(message)s')


def remove_null_handler():
    """Remove the null handler from the Dulwich loggers.

    If a caller wants to set up logging using something other than
    default_logging_config, calling this function first is a minor optimization
    to avoid the overhead of using the _NullHandler.
    """
    _DULWICH_LOGGER.removeHandler(_NULL_HANDLER)
