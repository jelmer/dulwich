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

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

__version__ = (0, 25, 0)

__all__ = ["__version__", "replace_me"]

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])

try:
    from dissolve import replace_me as replace_me
except ImportError:
    # if dissolve is not installed, then just provide a basic implementation
    # of its replace_me decorator
    def replace_me(
        since: tuple[int, ...] | str | None = None,
        remove_in: tuple[int, ...] | str | None = None,
    ) -> Callable[[F], F]:
        """Decorator to mark functions as deprecated.

        Args:
            since: Version when the function was deprecated
            remove_in: Version when the function will be removed

        Returns:
            Decorator function
        """

        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            import functools
            import warnings

            m = f"{func.__name__} is deprecated"
            since_str = str(since) if since is not None else None
            remove_in_str = str(remove_in) if remove_in is not None else None

            if since_str is not None and remove_in_str is not None:
                m += f" since {since_str} and will be removed in {remove_in_str}"
            elif since_str is not None:
                m += f" since {since_str}"
            elif remove_in_str is not None:
                m += f" and will be removed in {remove_in_str}"
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

        return decorator  # type: ignore[return-value]
