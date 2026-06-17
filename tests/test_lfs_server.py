# test_lfs_server.py -- Tests for the LFS server
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for the LFS server."""

import http.client
import os
import shutil
import tempfile
import threading

from dulwich.lfs_server import run_lfs_server

from . import TestCase


class LFSServerOidValidationTests(TestCase):
    """Tests that the server rejects object ids that escape the store."""

    def setUp(self) -> None:
        super().setUp()
        self.base = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.base)
        lfs_dir = os.path.join(self.base, "lfs")
        self.server, self.url = run_lfs_server(lfs_dir=lfs_dir)
        thread = threading.Thread(target=self.server.serve_forever)
        thread.daemon = True
        thread.start()

        def stop() -> None:
            self.server.shutdown()
            thread.join()
            self.server.server_close()

        self.addCleanup(stop)
        host, port = self.server.server_address[:2]
        self.host = host
        self.port = port

    def _get(self, path: str) -> tuple[int, bytes]:
        # Use a raw connection so the request path is sent verbatim and not
        # normalized/percent-encoded by a higher level client.
        conn = http.client.HTTPConnection(self.host, self.port)
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()

    def test_valid_object_is_served(self) -> None:
        oid = self.server.lfs_store.write_object([b"hello"])
        status, body = self._get(f"/objects/{oid}")
        self.assertEqual(200, status)
        self.assertEqual(b"hello", body)

    def test_traversing_oid_is_rejected(self) -> None:
        # ``_sha_path`` joins objects/<oid[:2]>/<oid[2:4]>/<oid>, so an oid of
        # ".." for the first two components climbs out of the store. Plant a
        # file where ``....`` would resolve and confirm it is not served.
        secret = os.path.join(self.base, "....")
        with open(secret, "wb") as f:
            f.write(b"secret")
        status, body = self._get("/objects/....")
        self.assertEqual(404, status)
        self.assertNotIn(b"secret", body)

    def test_backslash_oid_is_rejected(self) -> None:
        # On Windows the backslash is a path separator, so this would traverse;
        # it must be refused on every platform.
        status, _ = self._get("/objects/..\\..\\..\\secret")
        self.assertEqual(404, status)

    def test_non_hex_oid_is_rejected(self) -> None:
        status, _ = self._get("/objects/" + "z" * 64)
        self.assertEqual(404, status)
