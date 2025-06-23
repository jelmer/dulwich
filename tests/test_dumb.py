# test_dumb.py -- Tests for dumb HTTP git repositories
# Copyright (C) 2025 Dulwich contributors
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

"""Tests for dumb HTTP git repositories."""

import zlib
from unittest import TestCase
from unittest.mock import Mock

from dulwich.dumb import DumbHTTPObjectStore, DumbRemoteHTTPRepo
from dulwich.errors import NotGitRepository
from dulwich.objects import Blob, Commit, Tag, Tree, sha_to_hex


class MockResponse:
    def __init__(self, status=200, content=b"", headers=None):
        self.status = status
        self.content = content
        self.headers = headers or {}
        self.closed = False

    def close(self):
        self.closed = True


class DumbHTTPObjectStoreTests(TestCase):
    """Tests for DumbHTTPObjectStore."""

    def setUp(self):
        self.base_url = "https://example.com/repo.git/"
        self.responses = {}
        self.store = DumbHTTPObjectStore(self.base_url, self._mock_http_request)

    def _mock_http_request(self, url, headers):
        """Mock HTTP request function."""
        if url in self.responses:
            resp_data = self.responses[url]
            resp = MockResponse(
                resp_data.get("status", 200), resp_data.get("content", b"")
            )
            # Create a mock read function that behaves like urllib3's read
            content = resp.content
            offset = [0]  # Use list to make it mutable in closure

            def read_func(size=None):
                if offset[0] >= len(content):
                    return b""
                if size is None:
                    result = content[offset[0] :]
                    offset[0] = len(content)
                else:
                    result = content[offset[0] : offset[0] + size]
                    offset[0] += size
                return result

            return resp, read_func
        else:
            resp = MockResponse(404)
            return resp, lambda size: b""

    def _add_response(self, path, content, status=200):
        """Add a mock response for a given path."""
        url = self.base_url + path
        self.responses[url] = {"status": status, "content": content}

    def _make_object(self, obj):
        """Create compressed git object data."""
        type_name = {
            Blob.type_num: b"blob",
            Tree.type_num: b"tree",
            Commit.type_num: b"commit",
            Tag.type_num: b"tag",
        }[obj.type_num]

        content = obj.as_raw_string()
        header = type_name + b" " + str(len(content)).encode() + b"\x00"
        return zlib.compress(header + content)

    def test_fetch_loose_object_blob(self):
        # Create a blob object
        blob = Blob()
        blob.data = b"Hello, world!"
        hex_sha = blob.id

        # Add mock response
        path = f"objects/{hex_sha[:2].decode('ascii')}/{hex_sha[2:].decode('ascii')}"
        self._add_response(path, self._make_object(blob))

        # Fetch the object
        type_num, content = self.store._fetch_loose_object(blob.id)
        self.assertEqual(Blob.type_num, type_num)
        self.assertEqual(b"Hello, world!", content)

    def test_fetch_loose_object_not_found(self):
        hex_sha = b"1" * 40
        self.assertRaises(KeyError, self.store._fetch_loose_object, hex_sha)

    def test_fetch_loose_object_invalid_format(self):
        sha = b"1" * 20
        hex_sha = sha_to_hex(sha)
        path = f"objects/{hex_sha[:2]}/{hex_sha[2:]}"

        # Add invalid compressed data
        self._add_response(path, b"invalid data")

        self.assertRaises(Exception, self.store._fetch_loose_object, sha)

    def test_load_packs_empty(self):
        # No packs file
        self.store._load_packs()
        self.assertEqual([], self.store._packs)

    def test_load_packs_with_entries(self):
        packs_content = b"""P pack-1234567890abcdef1234567890abcdef12345678.pack
P pack-abcdef1234567890abcdef1234567890abcdef12.pack
"""
        self._add_response("objects/info/packs", packs_content)

        self.store._load_packs()
        self.assertEqual(2, len(self.store._packs))
        self.assertEqual(
            "pack-1234567890abcdef1234567890abcdef12345678", self.store._packs[0][0]
        )
        self.assertEqual(
            "pack-abcdef1234567890abcdef1234567890abcdef12", self.store._packs[1][0]
        )

    def test_get_raw_from_cache(self):
        sha = b"1" * 40
        self.store._cached_objects[sha] = (Blob.type_num, b"cached content")

        type_num, content = self.store.get_raw(sha)
        self.assertEqual(Blob.type_num, type_num)
        self.assertEqual(b"cached content", content)

    def test_contains_loose(self):
        # Create a blob object
        blob = Blob()
        blob.data = b"Test blob"
        hex_sha = blob.id

        # Add mock response
        path = f"objects/{hex_sha[:2].decode('ascii')}/{hex_sha[2:].decode('ascii')}"
        self._add_response(path, self._make_object(blob))

        self.assertTrue(self.store.contains_loose(hex_sha))
        self.assertFalse(self.store.contains_loose(b"0" * 40))

    def test_add_object_not_implemented(self):
        blob = Blob()
        blob.data = b"test"
        self.assertRaises(NotImplementedError, self.store.add_object, blob)

    def test_add_objects_not_implemented(self):
        self.assertRaises(NotImplementedError, self.store.add_objects, [])


class DumbRemoteHTTPRepoTests(TestCase):
    """Tests for DumbRemoteHTTPRepo."""

    def setUp(self):
        self.base_url = "https://example.com/repo.git/"
        self.responses = {}
        self.repo = DumbRemoteHTTPRepo(self.base_url, self._mock_http_request)

    def _mock_http_request(self, url, headers):
        """Mock HTTP request function."""
        if url in self.responses:
            resp_data = self.responses[url]
            resp = MockResponse(
                resp_data.get("status", 200), resp_data.get("content", b"")
            )
            # Create a mock read function that behaves like urllib3's read
            content = resp.content
            offset = [0]  # Use list to make it mutable in closure

            def read_func(size=None):
                if offset[0] >= len(content):
                    return b""
                if size is None:
                    result = content[offset[0] :]
                    offset[0] = len(content)
                else:
                    result = content[offset[0] : offset[0] + size]
                    offset[0] += size
                return result

            return resp, read_func
        else:
            resp = MockResponse(404)
            return resp, lambda size: b""

    def _add_response(self, path, content, status=200):
        """Add a mock response for a given path."""
        url = self.base_url + path
        self.responses[url] = {"status": status, "content": content}

    def test_get_refs(self):
        refs_content = b"""0123456789abcdef0123456789abcdef01234567\trefs/heads/master
abcdef0123456789abcdef0123456789abcdef01\trefs/heads/develop
fedcba9876543210fedcba9876543210fedcba98\trefs/tags/v1.0
"""
        self._add_response("info/refs", refs_content)

        refs = self.repo.get_refs()
        self.assertEqual(3, len(refs))
        self.assertEqual(
            b"0123456789abcdef0123456789abcdef01234567",
            refs[b"refs/heads/master"],
        )
        self.assertEqual(
            b"abcdef0123456789abcdef0123456789abcdef01",
            refs[b"refs/heads/develop"],
        )
        self.assertEqual(
            b"fedcba9876543210fedcba9876543210fedcba98",
            refs[b"refs/tags/v1.0"],
        )

    def test_get_refs_not_found(self):
        self.assertRaises(NotGitRepository, self.repo.get_refs)

    def test_get_peeled(self):
        refs_content = b"0123456789abcdef0123456789abcdef01234567\trefs/heads/master\n"
        self._add_response("info/refs", refs_content)

        # For dumb HTTP, peeled just returns the ref value
        peeled = self.repo.get_peeled(b"refs/heads/master")
        self.assertEqual(b"0123456789abcdef0123456789abcdef01234567", peeled)

    def test_fetch_pack_data_no_wants(self):
        refs_content = b"0123456789abcdef0123456789abcdef01234567\trefs/heads/master\n"
        self._add_response("info/refs", refs_content)

        graph_walker = Mock()

        def determine_wants(refs):
            return []

        result = list(self.repo.fetch_pack_data(graph_walker, determine_wants))
        self.assertEqual([], result)

    def test_fetch_pack_data_with_blob(self):
        # Set up refs
        refs_content = b"0123456789abcdef0123456789abcdef01234567\trefs/heads/master\n"
        self._add_response("info/refs", refs_content)

        # Create a simple blob object
        blob = Blob()
        blob.data = b"Test content"
        blob_sha = blob.id
        # Add blob response
        self.repo._object_store._cached_objects[blob_sha] = (
            Blob.type_num,
            blob.as_raw_string(),
        )

        # Mock graph walker
        graph_walker = Mock()
        graph_walker.ack.return_value = []  # No existing objects

        def determine_wants(refs):
            return [blob_sha]

        result = list(self.repo.fetch_pack_data(graph_walker, determine_wants))
        self.assertEqual(1, len(result))
        self.assertEqual(Blob.type_num, result[0].pack_type_num)
        self.assertEqual([blob.as_raw_string()], result[0].obj_chunks)

    def test_object_store_property(self):
        self.assertIsInstance(self.repo.object_store, DumbHTTPObjectStore)
        self.assertEqual(self.base_url, self.repo.object_store.base_url)
