# test_objects.py -- property tests for objects.py
# Copyright (C) 2026 The Dulwich contributors
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

"""Property tests for Git objects."""

import os
from unittest import SkipTest

from dulwich.objects import format_timezone, parse_timezone
from tests import TestCase

try:
    from hypothesis import given, settings
    from hypothesis import strategies as st
except ImportError:
    HYPOTHESIS_AVAILABLE = False
else:
    HYPOTHESIS_AVAILABLE = True


if HYPOTHESIS_AVAILABLE:
    settings.register_profile(
        "deterministic", max_examples=50, deadline=None, derandomize=True
    )
    settings.register_profile("ci", max_examples=50, deadline=None, derandomize=True)
    settings.register_profile(
        "local-deep", max_examples=1000, deadline=None, derandomize=True
    )
    settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "deterministic"))

    @st.composite
    def timezone_infos(draw) -> tuple[int, bool]:
        """Generate timezone offsets and compatible negative-marker flags."""
        offset_minutes = draw(st.integers(min_value=-(14 * 60), max_value=14 * 60))
        offset = offset_minutes * 60
        unnecessary_negative_timezone = draw(st.booleans()) if offset >= 0 else False
        return offset, unnecessary_negative_timezone


class ObjectPropertyTests(TestCase):
    """Property tests for object helpers."""

    if not HYPOTHESIS_AVAILABLE:

        def test_hypothesis_available(self) -> None:
            """Skip these tests when Hypothesis is unavailable."""
            raise SkipTest("hypothesis is not available")

    else:

        @given(timezone_infos())
        def test_timezone_format_parse_roundtrip(
            self, timezone_info: tuple[int, bool]
        ) -> None:
            """Check that formatted timezones parse back to their inputs."""
            self.assertEqual(
                timezone_info,
                parse_timezone(format_timezone(*timezone_info)),
            )
