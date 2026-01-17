# test___init__.py -- tests for __init__.py
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

"""Tests for dulwich __init__ module."""

import sys
import warnings
from unittest import mock

from . import TestCase


class ReplaceMeDecoratorTests(TestCase):
    """Tests for the replace_me decorator fallback implementation."""

    def test_replace_me_with_since_only(self) -> None:
        """Test replace_me decorator with only 'since' parameter."""
        # Mock dissolve to not be available
        with mock.patch.dict(sys.modules, {"dissolve": None}):
            # Need to reimport to get the fallback implementation
            import importlib

            import dulwich

            importlib.reload(dulwich)

            @dulwich.replace_me(since=(0, 1, 0))
            def deprecated_func():
                return "result"

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = deprecated_func()

                self.assertEqual("result", result)
                self.assertEqual(1, len(w))
                self.assertTrue(issubclass(w[0].category, DeprecationWarning))
                self.assertEqual(
                    "deprecated_func is deprecated since (0, 1, 0)",
                    str(w[0].message),
                )

    def test_replace_me_with_remove_in_only(self) -> None:
        """Test replace_me decorator with only 'remove_in' parameter."""
        with mock.patch.dict(sys.modules, {"dissolve": None}):
            import importlib

            import dulwich

            importlib.reload(dulwich)

            @dulwich.replace_me(remove_in=(2, 0, 0))
            def deprecated_func():
                return "result"

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = deprecated_func()

                self.assertEqual("result", result)
                self.assertEqual(1, len(w))
                self.assertTrue(issubclass(w[0].category, DeprecationWarning))
                self.assertEqual(
                    "deprecated_func is deprecated and will be removed in (2, 0, 0)",
                    str(w[0].message),
                )

    def test_replace_me_with_neither_parameter(self) -> None:
        """Test replace_me decorator with neither 'since' nor 'remove_in'."""
        with mock.patch.dict(sys.modules, {"dissolve": None}):
            import importlib

            import dulwich

            importlib.reload(dulwich)

            @dulwich.replace_me()
            def deprecated_func():
                return "result"

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = deprecated_func()

                self.assertEqual("result", result)
                self.assertEqual(1, len(w))
                self.assertTrue(issubclass(w[0].category, DeprecationWarning))
                self.assertEqual(
                    "deprecated_func is deprecated and will be removed in a future version",
                    str(w[0].message),
                )

    def test_replace_me_with_both_parameters(self) -> None:
        """Test replace_me decorator with both 'since' and 'remove_in'."""
        with mock.patch.dict(sys.modules, {"dissolve": None}):
            import importlib

            import dulwich

            importlib.reload(dulwich)

            @dulwich.replace_me(since=(0, 1, 0), remove_in=(2, 0, 0))
            def deprecated_func():
                return "result"

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = deprecated_func()

                self.assertEqual("result", result)
                self.assertEqual(1, len(w))
                self.assertTrue(issubclass(w[0].category, DeprecationWarning))
                self.assertEqual(
                    "deprecated_func is deprecated since (0, 1, 0) and will be removed in (2, 0, 0)",
                    str(w[0].message),
                )
