# dumb.py -- Support for dumb HTTP(S) git repositories
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

"""Support for dumb HTTP(S) git repositories."""

import os
import tempfile
from collections.abc import Iterator
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin
import zlib

from .errors import NotGitRepository, ObjectFormatException
from .object_store import BaseObjectStore
from .objects import (
    ZERO_SHA,
    ObjectID,
    ShaFile,
    hex_to_sha,
    sha_to_hex,
    Blob,
    Commit,
    Tag,
    Tree,
)
from .pack import Pack, PackIndex, UnpackedObject, load_pack_index_file
from .refs import Ref, read_info_refs, split_peeled_refs
from .repo import BaseRepo


class DumbHTTPObjectStore(BaseObjectStore):
    """Object store implementation that fetches objects over dumb HTTP."""

    def __init__(self, base_url: str, http_request_func):
        """Initialize a DumbHTTPObjectStore.

        Args:
          base_url: Base URL of the remote repository (e.g. "https://example.com/repo.git/")
          http_request_func: Function to make HTTP requests, should accept (url, headers)
                           and return (response, read_func)
        """
        self.base_url = base_url.rstrip("/") + "/"
        self._http_request = http_request_func
        self._packs: Optional[list[tuple[str, Optional[PackIndex]]]] = None
        self._cached_objects: dict[bytes, tuple[int, bytes]] = {}
        self._temp_pack_dir = None

    def _ensure_temp_pack_dir(self):
        """Ensure we have a temporary directory for storing pack files."""
        if self._temp_pack_dir is None:
            self._temp_pack_dir = tempfile.mkdtemp(prefix="dulwich-dumb-")

    def _fetch_url(self, path: str) -> bytes:
        """Fetch content from a URL path relative to base_url.

        Args:
          path: Path relative to base URL
        Returns:
          Content as bytes
        Raises:
          IOError: If the URL cannot be fetched
        """
        url = urljoin(self.base_url, path)
        resp, read = self._http_request(url, {})
        try:
            if resp.status == 404:
                raise OSError(f"Not found: {url}")
            elif resp.status != 200:
                raise OSError(f"HTTP error {resp.status}: {url}")

            # Read all content
            chunks = []
            while True:
                chunk = read(4096)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            resp.close()

    def _fetch_loose_object(self, sha: bytes) -> tuple[int, bytes]:
        """Fetch a loose object by SHA.

        Args:
          sha: SHA1 of the object
        Returns:
          Tuple of (type_num, content)

        Raises:
          KeyError: If object not found
        """
        hex_sha = sha_to_hex(sha).decode("ascii")
        path = f"objects/{hex_sha[:2]}/{hex_sha[2:]}"

        try:
            compressed = self._fetch_url(path)
        except OSError:
            raise KeyError(sha)

        # Decompress and parse the object
        decompressed = zlib.decompress(compressed)

        # Parse header
        header_end = decompressed.find(b"\x00")
        if header_end == -1:
            raise ObjectFormatException("Invalid object header")

        header = decompressed[:header_end]
        content = decompressed[header_end + 1 :]

        parts = header.split(b" ", 1)
        if len(parts) != 2:
            raise ObjectFormatException("Invalid object header")

        obj_type = parts[0]
        obj_size = int(parts[1])

        if len(content) != obj_size:
            raise ObjectFormatException("Object size mismatch")

        # Convert type name to type number
        type_map = {
            b"blob": Blob.type_num,
            b"tree": Tree.type_num,
            b"commit": Commit.type_num,
            b"tag": Tag.type_num,
        }

        if obj_type not in type_map:
            raise ObjectFormatException(f"Unknown object type: {obj_type!r}")

        return type_map[obj_type], content

    def _load_packs(self):
        """Load the list of available packs from the remote."""
        if self._packs is not None:
            return

        self._packs = []
        try:
            packs_data = self._fetch_url("objects/info/packs")
        except OSError:
            # No packs file, repository might only have loose objects
            return

        for line in packs_data.strip().split(b"\n"):
            if line.startswith(b"P "):
                pack_name = line[2:].decode("utf-8")
                # Extract just the pack name without path
                if "/" in pack_name:
                    pack_name = pack_name.split("/")[-1]
                if pack_name.endswith(".pack"):
                    pack_name = pack_name[:-5]  # Remove .pack extension
                self._packs.append((pack_name, None))

    def _get_pack_index(self, pack_name: str) -> PackIndex:
        """Get or fetch a pack index.

        Args:
          pack_name: Name of the pack (without .idx extension)

        Returns:
          PackIndex object
        """
        # Find the pack in our list
        for i, (name, idx) in enumerate(self._packs or []):
            if name == pack_name:
                if idx is None:
                    # Fetch and cache the index
                    idx_data = self._fetch_url(f"objects/pack/{pack_name}.idx")

                    idx = load_pack_index_file("<http>", BytesIO(idx_data))
                    if self._packs is not None:
                        self._packs[i] = (name, idx)
                return idx
        raise KeyError(f"Pack not found: {pack_name}")

    def _fetch_from_pack(self, sha: bytes) -> tuple[int, bytes]:
        """Try to fetch an object from pack files.

        Args:
          sha: SHA1 of the object
        Returns:
          Tuple of (type_num, content)

        Raises:
          KeyError: If object not found in any pack
        """
        self._load_packs()

        for pack_name, idx in self._packs or []:
            if idx is None:
                idx = self._get_pack_index(pack_name)

            try:
                # Check if object is in this pack
                idx.object_offset(sha)
            except KeyError:
                continue

            # We found the object, now we need to fetch the pack data
            # For efficiency, we could fetch just the needed portion, but for
            # simplicity we'll fetch the whole pack and cache it
            self._ensure_temp_pack_dir()
            if self._temp_pack_dir is None:
                raise RuntimeError("Temp pack directory not initialized")
            pack_path = os.path.join(self._temp_pack_dir, f"{pack_name}.pack")

            if not os.path.exists(pack_path):
                # Download the pack file
                pack_data = self._fetch_url(f"objects/pack/{pack_name}.pack")
                with open(pack_path, "wb") as f:
                    f.write(pack_data)

            # Open the pack and get the object
            pack = Pack(pack_path[:-5])  # Remove .pack extension
            try:
                return pack.get_raw(sha)
            finally:
                pack.close()

        raise KeyError(sha)

    def get_raw(self, sha: bytes) -> tuple[int, bytes]:
        """Obtain the raw text for an object.

        Args:
          sha: SHA1 of the object
        Returns:
          Tuple with numeric type and object contents
        """
        # Check cache first
        if sha in self._cached_objects:
            return self._cached_objects[sha]

        # Try loose object first
        try:
            result = self._fetch_loose_object(sha)
            self._cached_objects[sha] = result
            return result
        except KeyError:
            pass

        # Try packs
        result = self._fetch_from_pack(sha)
        self._cached_objects[sha] = result
        return result

    def contains_loose(self, sha: bytes) -> bool:
        """Check if a particular object is present by SHA1 and is loose."""
        try:
            self._fetch_loose_object(sha)
            return True
        except KeyError:
            return False

    def __contains__(self, sha: bytes) -> bool:
        """Check if a particular object is present by SHA1."""
        if sha in self._cached_objects:
            return True

        # Try loose object
        try:
            self._fetch_loose_object(sha)
            return True
        except KeyError:
            pass

        # Try packs
        try:
            self._fetch_from_pack(sha)
            return True
        except KeyError:
            return False

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over all SHAs in the store.

        Note: This is inefficient for dumb HTTP as it requires
        downloading all pack indices.
        """
        seen = set()

        # We can't efficiently list loose objects over dumb HTTP
        # So we only iterate pack objects
        self._load_packs()

        for pack_name, idx in self._packs or []:
            if idx is None:
                idx = self._get_pack_index(pack_name)

            for sha in idx:
                if sha not in seen:
                    seen.add(sha)
                    yield sha

    @property
    def packs(self):
        """Iterable of pack objects.

        Note: Returns empty list as we don't have actual Pack objects.
        """
        return []

    def add_object(self, obj) -> None:
        """Add a single object to this object store."""
        raise NotImplementedError("Cannot add objects to dumb HTTP repository")

    def add_objects(self, objects, progress=None) -> None:
        """Add a set of objects to this object store."""
        raise NotImplementedError("Cannot add objects to dumb HTTP repository")

    def __del__(self):
        """Clean up temporary directory on deletion."""
        if self._temp_pack_dir and os.path.exists(self._temp_pack_dir):
            import shutil

            shutil.rmtree(self._temp_pack_dir, ignore_errors=True)


class DumbRemoteRepo(BaseRepo):
    """Repository implementation for dumb HTTP remotes."""

    def __init__(self, base_url: str, http_request_func):
        """Initialize a DumbRemoteRepo.

        Args:
          base_url: Base URL of the remote repository
          http_request_func: Function to make HTTP requests
        """
        self.base_url = base_url.rstrip("/") + "/"
        self._http_request = http_request_func
        self._refs: Optional[dict[Ref, ObjectID]] = None
        self._peeled: Optional[dict[Ref, ObjectID]] = None
        self._object_store = DumbHTTPObjectStore(base_url, http_request_func)

    @property
    def object_store(self):
        """ObjectStore for this repository."""
        return self._object_store

    def _fetch_url(self, path: str) -> bytes:
        """Fetch content from a URL path relative to base_url."""
        url = urljoin(self.base_url, path)
        resp, read = self._http_request(url, {})
        try:
            if resp.status == 404:
                raise OSError(f"Not found: {url}")
            elif resp.status != 200:
                raise OSError(f"HTTP error {resp.status}: {url}")

            chunks = []
            while True:
                chunk = read(4096)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            resp.close()

    def get_refs(self) -> dict[Ref, ObjectID]:
        """Get dictionary with all refs."""
        if self._refs is None:
            # Fetch info/refs
            try:
                refs_data = self._fetch_url("info/refs")
            except OSError:
                raise NotGitRepository(f"Cannot read refs from {self.base_url}")

            refs_hex = read_info_refs(BytesIO(refs_data))
            # Convert hex SHAs to binary
            refs = {}
            for ref, hex_sha in refs_hex.items():
                refs[ref] = hex_to_sha(hex_sha)
            self._refs, self._peeled = split_peeled_refs(refs)

        return dict(self._refs)

    def get_peeled(self, ref: Ref) -> ObjectID:
        """Get the peeled value of a ref."""
        # For dumb HTTP, we don't have peeled refs readily available
        # We would need to fetch and parse tag objects
        sha = self.get_refs().get(ref, None)
        return sha if sha is not None else ZERO_SHA

    def fetch_pack_data(self, graph_walker, determine_wants, progress=None, depth=None):
        """Fetch pack data from the remote.

        This is the main method for fetching objects from a dumb HTTP remote.
        Since dumb HTTP doesn't support negotiation, we need to download
        all objects reachable from the wanted refs that we don't have locally.

        Args:
          graph_walker: GraphWalker instance that can tell us which commits we have
          determine_wants: Function that returns list of wanted SHAs
          progress: Optional progress callback
          depth: Depth for shallow clones (not fully supported)

        Returns:
          Iterator of UnpackedObject instances
        """

        refs = self.get_refs()
        wants = determine_wants(refs)

        if not wants:
            return

        # For dumb HTTP, we can't negotiate, so we need to fetch all objects
        # reachable from wants that we don't already have
        to_fetch = set(wants)
        seen = set()

        while to_fetch:
            sha = to_fetch.pop()
            if sha in seen:
                continue
            seen.add(sha)

            # Check if we already have this object
            haves = list(graph_walker.ack(sha))
            if haves:
                continue

            # Fetch the object
            try:
                type_num, content = self._object_store.get_raw(sha)
                unpacked = UnpackedObject(type_num, sha=sha)
                unpacked.obj_type_num = type_num
                unpacked.obj_chunks = [content]
                yield unpacked

                # If it's a commit or tag, we need to fetch its references
                obj = ShaFile.from_raw_string(type_num, content)

                if hasattr(obj, "tree"):  # Commit
                    to_fetch.add(obj.tree)
                    for parent in obj.parents:
                        to_fetch.add(parent)
                elif hasattr(obj, "object"):  # Tag
                    to_fetch.add(obj.object[1])
                elif hasattr(obj, "items"):  # Tree
                    for _, _, item_sha in obj.items():
                        to_fetch.add(item_sha)

            except KeyError:
                # Object not found, skip it
                pass

            if progress:
                progress(f"Fetching objects: {len(seen)} done")
