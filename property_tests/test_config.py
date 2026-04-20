# test_config.py -- property tests for config.py
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

"""Property tests for reading and writing configuration files."""

import os
from io import BytesIO
from unittest import SkipTest

from dulwich.config import ConfigFile
from tests import TestCase

try:
    from hypothesis import given, settings
    from hypothesis import strategies as st
except ImportError:
    HYPOTHESIS_AVAILABLE = False
else:
    HYPOTHESIS_AVAILABLE = True


EXPECTED_PARSE_ERRORS = (
    "without section",
    "invalid variable name",
    "expected trailing ]",
    "invalid section name",
    "Invalid subsection",
    "escape character",
    "missing end quote",
)


if HYPOTHESIS_AVAILABLE:
    settings.register_profile(
        "deterministic", max_examples=50, deadline=None, derandomize=True
    )
    settings.register_profile("ci", max_examples=50, deadline=None, derandomize=True)
    settings.register_profile(
        "local-deep", max_examples=1000, deadline=None, derandomize=True
    )
    settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "deterministic"))

    section_names = st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-",
        min_size=1,
        max_size=12,
    ).map(lambda value: value.encode("ascii"))

    subsection_names = st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._/",
        min_size=1,
        max_size=16,
    ).map(lambda value: value.encode("ascii"))

    variable_names = st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-",
        min_size=1,
        max_size=12,
    ).map(lambda value: value.encode("ascii"))

    values = (
        st.text(
            alphabet=(
                "abcdefghijklmnopqrstuvwxyz"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "0123456789"
                ' -_./:#\t\n\\"'
            ),
            max_size=32,
        )
        .map(lambda value: value.encode("ascii"))
        .filter(lambda value: not value.endswith((b" ", b"\t")))
    )

    sections = st.tuples(
        section_names,
        st.one_of(st.none(), subsection_names),
        st.dictionaries(variable_names, values, min_size=1, max_size=6),
    )

    configs = st.lists(sections, min_size=0, max_size=6)


def _config_from_sections(
    sections: list[tuple[bytes, bytes | None, dict[bytes, bytes]]],
) -> ConfigFile:
    config = ConfigFile()
    for section_name, subsection_name, variables in sections:
        section = (
            (section_name,)
            if subsection_name is None
            else (section_name, subsection_name)
        )
        for key, value in variables.items():
            config.set(section, key, value)
    return config


class ConfigFilePropertyTests(TestCase):
    """Property tests for ConfigFile."""

    if not HYPOTHESIS_AVAILABLE:

        def test_hypothesis_available(self) -> None:
            """Skip these tests when Hypothesis is unavailable."""
            raise SkipTest("hypothesis is not available")

    else:

        @given(st.binary(max_size=512))
        def test_binary_input_only_raises_expected_parse_errors(
            self, data: bytes
        ) -> None:
            """Check that arbitrary bytes only raise expected parse errors."""
            try:
                ConfigFile.from_file(BytesIO(data))
            except ValueError as exc:
                self.assertTrue(
                    any(message in str(exc) for message in EXPECTED_PARSE_ERRORS),
                    str(exc),
                )

        @given(configs)
        def test_write_roundtrip_preserves_effective_config(
            self,
            generated_sections: list[tuple[bytes, bytes | None, dict[bytes, bytes]]],
        ) -> None:
            """Check that writing and reading preserves generated configs."""
            config = _config_from_sections(generated_sections)

            output = BytesIO()
            config.write_to_file(output)

            reparsed = ConfigFile.from_file(BytesIO(output.getvalue()))
            self.assertEqual(config, reparsed)
