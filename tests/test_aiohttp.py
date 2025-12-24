# test_aiohttp.py -- Tests for the aiohttp HTTP server
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

"""Tests for the aiohttp Git HTTP server."""

try:
    from aiohttp.test_utils import AioHTTPTestCase

    aiohttp_missing = False
except ImportError:
    from unittest import TestCase as AioHTTPTestCase  # type: ignore

    aiohttp_missing = True

from dulwich.objects import Blob
from dulwich.repo import MemoryRepo
from dulwich.tests.utils import make_object

from . import skipIf

if not aiohttp_missing:
    from dulwich.aiohttp.server import create_repo_app


@skipIf(aiohttp_missing, "aiohttp not available")
class AiohttpAppTestCase(AioHTTPTestCase):  # type: ignore
    """Test the aiohttp application."""

    def setUp(self):
        super().setUp()
        self.repo = MemoryRepo.init_bare([], {})
        self.blob = make_object(Blob, data=b"blob contents")
        self.repo.object_store.add_object(self.blob)
        self.repo.refs[b"refs/heads/master"] = self.blob.id

    def get_app(self):
        return create_repo_app(self.repo)

    async def test_get_info_refs_dumb(self):
        """Test GET /info/refs without service parameter (dumb protocol)."""
        resp = await self.client.request("GET", "/info/refs")
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content_type, "text/plain")
        text = await resp.text()
        self.assertIn(self.blob.id.decode("ascii"), text)
        self.assertIn("refs/heads/master", text)

    async def test_get_info_refs_smart(self):
        """Test GET /info/refs?service=git-upload-pack (smart protocol)."""
        resp = await self.client.request("GET", "/info/refs?service=git-upload-pack")
        self.assertEqual(resp.status, 200)
        self.assertIn("git-upload-pack", resp.content_type)

    async def test_post_upload_pack(self):
        """Test POST /git-upload-pack."""
        # Simple test that the endpoint exists and accepts POST
        resp = await self.client.request(
            "POST",
            "/git-upload-pack",
            data=b"0000",
            headers={"Content-Type": "application/x-git-upload-pack-request"},
        )
        # Should respond with 200 even for invalid/minimal input
        self.assertEqual(resp.status, 200)
        self.assertIn("git-upload-pack", resp.content_type)


@skipIf(aiohttp_missing, "aiohttp not available")
class AiohttpDumbAppTestCase(AioHTTPTestCase):  # type: ignore
    """Test the aiohttp application with dumb protocol."""

    def setUp(self):
        super().setUp()
        self.repo = MemoryRepo.init_bare([], {})
        self.repo._put_named_file("HEAD", b"ref: refs/heads/master\n")
        self.blob = make_object(Blob, data=b"blob contents")
        self.repo.object_store.add_object(self.blob)
        self.repo.refs[b"refs/heads/master"] = self.blob.id

    def get_app(self):
        return create_repo_app(self.repo, dumb=True)

    async def test_get_head(self):
        """Test GET /HEAD."""
        resp = await self.client.request("GET", "/HEAD")
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content_type, "text/plain")
        text = await resp.text()
        self.assertEqual(text, "ref: refs/heads/master\n")

    async def test_get_info_packs(self):
        """Test GET /objects/info/packs."""
        resp = await self.client.request("GET", "/objects/info/packs")
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content_type, "text/plain")

    async def test_get_loose_object(self):
        """Test GET /objects/{dir}/{file} for loose objects."""
        sha = self.blob.id.decode("ascii")
        dir_part = sha[:2]
        file_part = sha[2:]
        resp = await self.client.request("GET", f"/objects/{dir_part}/{file_part}")
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.content_type, "application/x-git-loose-object")
        body = await resp.read()
        self.assertEqual(body, self.blob.as_legacy_object())

    async def test_get_loose_object_not_found(self):
        """Test GET /objects/{dir}/{file} for non-existent object."""
        resp = await self.client.request("GET", "/objects/ab/cdef" + "0" * 36)
        self.assertEqual(resp.status, 404)
