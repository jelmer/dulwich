# test_pack.py -- Compatibility tests for git packs.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

"""Compatibility tests for git packs."""


import binascii
import os
import re
import shutil
import tempfile

from dulwich.pack import (
    write_pack,
    )
from dulwich.objects import (
    Blob,
    )
from dulwich.tests.test_pack import (
    a_sha,
    pack1_sha,
    PackTests,
    )
from dulwich.tests.compat.utils import (
    require_git_version,
    run_git_or_fail,
    )

_NON_DELTA_RE = re.compile('non delta: (?P<non_delta>\d+) objects')

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

    def setUp(self):
        require_git_version((1, 5, 0))
        super(TestPack, self).setUp()
        self._tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._tempdir)

    def test_copy(self):
        with self.get_pack(pack1_sha) as origpack:
            self.assertSucceeds(origpack.index.check)
            pack_path = os.path.join(self._tempdir, "Elch")
            write_pack(pack_path, origpack.pack_tuples())
            output = run_git_or_fail(['verify-pack', '-v', pack_path])
            orig_shas = set(o.id for o in origpack.iterobjects())
            self.assertEqual(orig_shas, _git_verify_pack_object_list(output))

    def test_deltas_work(self):
        orig_pack = self.get_pack(pack1_sha)
        orig_blob = orig_pack[a_sha]
        new_blob = Blob()
        new_blob.data = orig_blob.data + 'x'
        all_to_pack = list(orig_pack.pack_tuples()) + [(new_blob, None)]
        pack_path = os.path.join(self._tempdir, "pack_with_deltas")
        write_pack(pack_path, all_to_pack)
        output = run_git_or_fail(['verify-pack', '-v', pack_path])
        self.assertEqual(set(x[0].id for x in all_to_pack),
                         _git_verify_pack_object_list(output))
        # We specifically made a new blob that should be a delta
        # against the blob a_sha, so make sure we really got only 3
        # non-delta objects:
        got_non_delta = int(_NON_DELTA_RE.search(output).group('non_delta'))
        self.assertEqual(
            3, got_non_delta,
            'Expected 3 non-delta objects, got %d' % got_non_delta)
