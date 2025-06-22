# test_pack.py -- Compatibility tests for git packs.
# Copyright (C) 2010 Google, Inc.
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

"""Compatibility tests for git packs."""

import binascii
import os
import re
import shutil
import tempfile
from typing import NoReturn

from dulwich.file import GitFile
from dulwich.objects import Blob
from dulwich.pack import (
    PackData,
    PackIndex3,
    load_pack_index,
    write_pack,
    write_pack_index_v3,
)

from .. import SkipTest
from ..test_pack import PackTests, a_sha, pack1_sha
from .utils import require_git_version, rmtree_ro, run_git_or_fail

_NON_DELTA_RE = re.compile(b"non delta: (?P<non_delta>\\d+) objects")


def _git_verify_pack_object_list(output):
    pack_shas = set()
    for line in output.splitlines():
        sha = line[:40]
        try:
            binascii.unhexlify(sha)
        except (TypeError, binascii.Error):
            continue  # non-sha line
        pack_shas.add(sha)
    return pack_shas


class TestPack(PackTests):
    """Compatibility tests for reading and writing pack files."""

    def setUp(self) -> None:
        require_git_version((1, 5, 0))
        super().setUp()
        self._tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._tempdir)

    def test_copy(self) -> None:
        with self.get_pack(pack1_sha) as origpack:
            self.assertSucceeds(origpack.index.check)
            pack_path = os.path.join(self._tempdir, "Elch")
            write_pack(pack_path, origpack.pack_tuples())
            output = run_git_or_fail(["verify-pack", "-v", pack_path])
            orig_shas = {o.id for o in origpack.iterobjects()}
            self.assertEqual(orig_shas, _git_verify_pack_object_list(output))

    def test_deltas_work(self) -> None:
        with self.get_pack(pack1_sha) as orig_pack:
            orig_blob = orig_pack[a_sha]
            new_blob = Blob()
            new_blob.data = orig_blob.data + b"x"
            all_to_pack = [(o, None) for o in orig_pack.iterobjects()] + [
                (new_blob, None)
            ]
        pack_path = os.path.join(self._tempdir, "pack_with_deltas")
        write_pack(pack_path, all_to_pack, deltify=True)
        output = run_git_or_fail(["verify-pack", "-v", pack_path])
        self.assertEqual(
            {x[0].id for x in all_to_pack},
            _git_verify_pack_object_list(output),
        )
        # We specifically made a new blob that should be a delta
        # against the blob a_sha, so make sure we really got only 3
        # non-delta objects:
        got_non_delta = int(_NON_DELTA_RE.search(output).group("non_delta"))
        self.assertEqual(
            3,
            got_non_delta,
            f"Expected 3 non-delta objects, got {got_non_delta}",
        )

    def test_delta_medium_object(self) -> None:
        # This tests an object set that will have a copy operation
        # 2**20 in size.
        with self.get_pack(pack1_sha) as orig_pack:
            orig_blob = orig_pack[a_sha]
            new_blob = Blob()
            new_blob.data = orig_blob.data + (b"x" * 2**20)
            new_blob_2 = Blob()
            new_blob_2.data = new_blob.data + b"y"
            all_to_pack = [
                *list(orig_pack.pack_tuples()),
                (new_blob, None),
                (new_blob_2, None),
            ]
            pack_path = os.path.join(self._tempdir, "pack_with_deltas")
            write_pack(pack_path, all_to_pack, deltify=True)
        output = run_git_or_fail(["verify-pack", "-v", pack_path])
        self.assertEqual(
            {x[0].id for x in all_to_pack},
            _git_verify_pack_object_list(output),
        )
        # We specifically made a new blob that should be a delta
        # against the blob a_sha, so make sure we really got only 3
        # non-delta objects:
        got_non_delta = int(_NON_DELTA_RE.search(output).group("non_delta"))
        self.assertEqual(
            3,
            got_non_delta,
            f"Expected 3 non-delta objects, got {got_non_delta}",
        )
        # We expect one object to have a delta chain length of two
        # (new_blob_2), so let's verify that actually happens:
        self.assertIn(b"chain length = 2", output)

    # This test is SUPER slow: over 80 seconds on a 2012-era
    # laptop. This is because SequenceMatcher is worst-case quadratic
    # on the input size. It's impractical to produce deltas for
    # objects this large, but it's still worth doing the right thing
    # when it happens.
    def test_delta_large_object(self) -> NoReturn:
        # This tests an object set that will have a copy operation
        # 2**25 in size. This is a copy large enough that it requires
        # two copy operations in git's binary delta format.
        raise SkipTest("skipping slow, large test")
        with self.get_pack(pack1_sha) as orig_pack:
            new_blob = Blob()
            new_blob.data = "big blob" + ("x" * 2**25)
            new_blob_2 = Blob()
            new_blob_2.data = new_blob.data + "y"
            all_to_pack = [
                *list(orig_pack.pack_tuples()),
                (new_blob, None),
                (new_blob_2, None),
            ]
            pack_path = os.path.join(self._tempdir, "pack_with_deltas")
            write_pack(pack_path, all_to_pack, deltify=True)
        output = run_git_or_fail(["verify-pack", "-v", pack_path])
        self.assertEqual(
            {x[0].id for x in all_to_pack},
            _git_verify_pack_object_list(output),
        )
        # We specifically made a new blob that should be a delta
        # against the blob a_sha, so make sure we really got only 4
        # non-delta objects:
        got_non_delta = int(_NON_DELTA_RE.search(output).group("non_delta"))
        self.assertEqual(
            4,
            got_non_delta,
            f"Expected 4 non-delta objects, got {got_non_delta}",
        )


class TestPackIndexCompat(PackTests):
    """Compatibility tests for pack index formats."""

    def setUp(self) -> None:
        require_git_version((1, 5, 0))
        super().setUp()
        self._tempdir = tempfile.mkdtemp()
        self.addCleanup(rmtree_ro, self._tempdir)

    def test_dulwich_create_index_git_readable(self) -> None:
        """Test that git can read pack indexes created by dulwich."""
        # Create a simple pack with objects
        blob = Blob()
        blob.data = b"Test blob"

        pack_path = os.path.join(self._tempdir, "test_pack")
        entries = [(blob, None)]
        write_pack(pack_path, entries)

        # Load the pack and create v2 index (most compatible)
        pack_data = PackData(pack_path + ".pack")
        try:
            pack_data.create_index(pack_path + ".idx", version=2)
        finally:
            pack_data.close()

        # Verify git can read it
        output = run_git_or_fail(["verify-pack", "-v", pack_path + ".pack"])
        self.assertIn(blob.id.decode("ascii"), output.decode("ascii"))

    def test_dulwich_read_git_index(self) -> None:
        """Test that dulwich can read pack indexes created by git."""
        # Create a simple pack with objects
        blob = Blob()
        blob.data = b"Test blob for git"

        pack_path = os.path.join(self._tempdir, "git_pack")
        entries = [(blob, None)]
        write_pack(pack_path, entries)

        # Create index with git
        run_git_or_fail(["index-pack", pack_path + ".pack"])

        # Load with dulwich
        idx = load_pack_index(pack_path + ".idx")

        # Verify it works
        self.assertIn(blob.id, idx)
        self.assertEqual(len(idx), 1)

    def test_index_format_v3_sha256_future(self) -> None:
        """Test that v3 index format is ready for SHA-256 support."""
        # This test verifies the v3 implementation structure is ready
        # for SHA-256, even though SHA-256 itself is not yet implemented

        # Create a dummy v3 index to test the format
        entries = [(b"a" * 20, 100, 1234)]  # SHA-1 for now

        v3_path = os.path.join(self._tempdir, "v3_test.idx")
        with GitFile(v3_path, "wb") as f:
            write_pack_index_v3(f, entries, b"x" * 20, hash_algorithm=1)

        # Load and verify structure
        idx = load_pack_index(v3_path)
        self.assertIsInstance(idx, PackIndex3)
        self.assertEqual(idx.version, 3)
        self.assertEqual(idx.hash_algorithm, 1)  # SHA-1
        self.assertEqual(idx.hash_size, 20)

        # Verify SHA-256 would raise NotImplementedError
        with self.assertRaises(NotImplementedError):
            with GitFile(v3_path + ".sha256", "wb") as f:
                write_pack_index_v3(f, entries, b"x" * 32, hash_algorithm=2)
