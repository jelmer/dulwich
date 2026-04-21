# test_pack.py -- property tests for pack.py
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

"""Property tests for pack helpers."""

import os
from collections.abc import Iterable

from hypothesis import example, given, settings
from hypothesis import strategies as st

from dulwich.errors import ApplyDeltaError
from dulwich.pack import _create_delta_py, _delta_encode_size, apply_delta, create_delta
from tests import TestCase


def _delta_to_bytes(delta: bytes | Iterable[bytes]) -> bytes:
    if isinstance(delta, bytes):
        return delta
    return b"".join(delta)


settings.register_profile(
    "deterministic", max_examples=50, deadline=None, derandomize=True
)
settings.register_profile("ci", max_examples=50, deadline=None, derandomize=True)
settings.register_profile(
    "local-deep", max_examples=1000, deadline=None, derandomize=True
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "deterministic"))

byte_strings = st.binary(max_size=256)


@st.composite
def delta_pairs(draw) -> tuple[bytes, bytes]:
    """Generate byte pairs with some shared content for delta copies."""
    prefix = draw(st.binary(max_size=128))
    suffix = draw(st.binary(max_size=128))
    base_middle = draw(st.binary(max_size=128))
    target_middle = draw(st.binary(max_size=128))
    return prefix + base_middle + suffix, prefix + target_middle + suffix


@st.composite
def bounded_delta_inputs(draw) -> tuple[bytes, bytes]:
    """Generate arbitrary delta op streams with bounded output sizes."""
    base = draw(byte_strings)
    dest_size = draw(st.integers(min_value=0, max_value=512))
    ops = draw(st.binary(max_size=256))
    delta = _delta_encode_size(len(base)) + _delta_encode_size(dest_size) + ops
    return base, delta


byte_pairs = st.one_of(st.tuples(byte_strings, byte_strings), delta_pairs())


class PackPropertyTests(TestCase):
    """Property tests for pack helpers."""

    @given(byte_pairs)
    @example((b"", b""))
    @example((b"", b"Z" * 8192))
    @example((b"Z" * 8192, b"Z" * 8192))
    @example((b"Z" * 70000 + b"a", b"Z" * 70000 + b"b"))
    def test_create_delta_roundtrip(self, pair: tuple[bytes, bytes]) -> None:
        """Check that generated deltas apply back to the target."""
        base, target = pair
        delta = _delta_to_bytes(create_delta(base, target))
        self.assertEqual(target, b"".join(apply_delta(base, delta)))

    @given(byte_pairs)
    @example((b"", b""))
    @example((b"", b"Z" * 8192))
    @example((b"Z" * 8192, b"Z" * 8192))
    @example((b"Z" * 70000 + b"a", b"Z" * 70000 + b"b"))
    def test_create_delta_py_roundtrip(self, pair: tuple[bytes, bytes]) -> None:
        """Check that pure Python generated deltas apply to the target."""
        base, target = pair
        delta = _delta_to_bytes(_create_delta_py(base, target))
        self.assertEqual(target, b"".join(apply_delta(base, delta)))

    @given(bounded_delta_inputs())
    @example((b"", b"\x00\x01\x01"))
    def test_apply_delta_only_raises_apply_delta_error(
        self, base_and_delta: tuple[bytes, bytes]
    ) -> None:
        """Check that malformed deltas use the delta error type."""
        base, delta = base_and_delta
        try:
            apply_delta(base, delta)
        except ApplyDeltaError:
            pass
