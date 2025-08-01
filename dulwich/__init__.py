# __init__.py -- The git module of dulwich
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# Copyright (C) 2008 Jelmer Vernooij <jelmer@jelmer.uk>
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


"""Python implementation of the Git file formats and protocols."""

import sys
from typing import Callable, Optional, TypeVar

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

__version__ = (0, 24, 1)

__all__ = ["__version__", "replace_me"]

P = ParamSpec("P")
R = TypeVar("R")

try:
    from dissolve import replace_me
except ImportError:
    # if dissolve is not installed, then just provide a basic implementation
    # of its replace_me decorator
    def replace_me(
        since: Optional[str] = None, remove_in: Optional[str] = None
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            import functools
            import warnings

            m = f"{func.__name__} is deprecated"
            if since is not None and remove_in is not None:
                m += f" since {since} and will be removed in {remove_in}"
            elif since is not None:
                m += f" since {since}"
            elif remove_in is not None:
                m += f" and will be removed in {remove_in}"
            else:
                m += " and will be removed in a future version"

            @functools.wraps(func)
            def _wrapped_func(*args: P.args, **kwargs: P.kwargs) -> R:
                warnings.warn(
                    m,
                    DeprecationWarning,
                    stacklevel=2,
                )
                return func(*args, **kwargs)

            return _wrapped_func

        return decorator
