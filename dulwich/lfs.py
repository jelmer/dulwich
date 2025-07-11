# lfs.py -- Implementation of the LFS
# Copyright (C) 2020 Jelmer Vernooij
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

import hashlib
import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, BinaryIO, Optional
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from .repo import Repo


@dataclass
class LFSAction:
    """LFS action structure."""

    href: str
    header: Optional[dict[str, str]] = None
    expires_at: Optional[str] = None


@dataclass
class LFSErrorInfo:
    """LFS error structure."""

    code: int
    message: str


@dataclass
class LFSBatchObject:
    """LFS batch object structure."""

    oid: str
    size: int
    authenticated: Optional[bool] = None
    actions: Optional[dict[str, LFSAction]] = None
    error: Optional[LFSErrorInfo] = None


@dataclass
class LFSBatchResponse:
    """LFS batch response structure."""

    transfer: str
    objects: list[LFSBatchObject]
    hash_algo: Optional[str] = None


class LFSStore:
    """Stores objects on disk, indexed by SHA256."""

    def __init__(self, path: str) -> None:
        self.path = path

    @classmethod
    def create(cls, lfs_dir: str) -> "LFSStore":
        if not os.path.isdir(lfs_dir):
            os.mkdir(lfs_dir)
        tmp_dir = os.path.join(lfs_dir, "tmp")
        if not os.path.isdir(tmp_dir):
            os.mkdir(tmp_dir)
        objects_dir = os.path.join(lfs_dir, "objects")
        if not os.path.isdir(objects_dir):
            os.mkdir(objects_dir)
        return cls(lfs_dir)

    @classmethod
    def from_repo(cls, repo: "Repo", create: bool = False) -> "LFSStore":
        lfs_dir = os.path.join(repo.controldir(), "lfs")
        if create:
            return cls.create(lfs_dir)
        return cls(lfs_dir)

    def _sha_path(self, sha: str) -> str:
        return os.path.join(self.path, "objects", sha[0:2], sha[2:4], sha)

    def open_object(self, sha: str) -> BinaryIO:
        """Open an object by sha."""
        try:
            return open(self._sha_path(sha), "rb")
        except FileNotFoundError as exc:
            raise KeyError(sha) from exc

    def write_object(self, chunks: Iterable[bytes]) -> str:
        """Write an object.

        Returns: object SHA
        """
        sha = hashlib.sha256()
        tmpdir = os.path.join(self.path, "tmp")
        with tempfile.NamedTemporaryFile(dir=tmpdir, mode="wb", delete=False) as f:
            for chunk in chunks:
                sha.update(chunk)
                f.write(chunk)
            f.flush()
            tmppath = f.name
        path = self._sha_path(sha.hexdigest())
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        os.rename(tmppath, path)
        return sha.hexdigest()


class LFSPointer:
    """Represents an LFS pointer file."""

    def __init__(self, oid: str, size: int) -> None:
        self.oid = oid
        self.size = size

    @classmethod
    def from_bytes(cls, data: bytes) -> Optional["LFSPointer"]:
        """Parse LFS pointer from bytes.

        Returns None if data is not a valid LFS pointer.
        """
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return None

        # LFS pointer files have a specific format
        lines = text.strip().split("\n")
        if len(lines) < 3:
            return None

        # Must start with version
        if not lines[0].startswith("version https://git-lfs.github.com/spec/v1"):
            return None

        oid = None
        size = None

        for line in lines[1:]:
            if line.startswith("oid sha256:"):
                oid = line[11:].strip()
            elif line.startswith("size "):
                try:
                    size = int(line[5:].strip())
                    # Size must be non-negative
                    if size < 0:
                        return None
                except ValueError:
                    return None

        if oid is None or size is None:
            return None

        return cls(oid, size)

    def to_bytes(self) -> bytes:
        """Convert LFS pointer to bytes."""
        return (
            f"version https://git-lfs.github.com/spec/v1\n"
            f"oid sha256:{self.oid}\n"
            f"size {self.size}\n"
        ).encode()

    def is_valid_oid(self) -> bool:
        """Check if the OID is valid SHA256."""
        if len(self.oid) != 64:
            return False
        try:
            int(self.oid, 16)
            return True
        except ValueError:
            return False


class LFSFilterDriver:
    """LFS filter driver implementation."""

    def __init__(self, lfs_store: "LFSStore") -> None:
        self.lfs_store = lfs_store

    def clean(self, data: bytes) -> bytes:
        """Convert file content to LFS pointer (clean filter)."""
        # Check if data is already an LFS pointer
        pointer = LFSPointer.from_bytes(data)
        if pointer is not None:
            return data

        # Store the file content in LFS
        sha = self.lfs_store.write_object([data])

        # Create and return LFS pointer
        pointer = LFSPointer(sha, len(data))
        return pointer.to_bytes()

    def smudge(self, data: bytes) -> bytes:
        """Convert LFS pointer to file content (smudge filter)."""
        # Try to parse as LFS pointer
        pointer = LFSPointer.from_bytes(data)
        if pointer is None:
            # Not an LFS pointer, return as-is
            return data

        # Validate the pointer
        if not pointer.is_valid_oid():
            return data

        try:
            # Read the actual content from LFS store
            with self.lfs_store.open_object(pointer.oid) as f:
                return f.read()
        except KeyError:
            # Object not found in LFS store, return pointer as-is
            # This matches Git LFS behavior when object is missing
            return data


class LFSClient:
    """LFS client for network operations."""

    def __init__(self, url: str, auth: Optional[tuple[str, str]] = None) -> None:
        """Initialize LFS client.

        Args:
            url: LFS server URL
            auth: Optional (username, password) tuple for authentication
        """
        self.url = url.rstrip("/")
        self.auth = auth

    def _make_request(
        self,
        method: str,
        path: str,
        data: Optional[bytes] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> bytes:
        """Make an HTTP request to the LFS server."""
        url = urljoin(self.url, path)
        req_headers = {
            "Accept": "application/vnd.git-lfs+json",
            "Content-Type": "application/vnd.git-lfs+json",
        }
        if headers:
            req_headers.update(headers)

        req = Request(url, data=data, headers=req_headers, method=method)

        if self.auth:
            import base64

            auth_str = f"{self.auth[0]}:{self.auth[1]}"
            b64_auth = base64.b64encode(auth_str.encode()).decode("ascii")
            req.add_header("Authorization", f"Basic {b64_auth}")

        try:
            with urlopen(req) as response:
                return response.read()
        except HTTPError as e:
            error_body = e.read().decode("utf-8", errors="ignore")
            raise LFSError(f"LFS server error {e.code}: {error_body}")

    def batch(
        self,
        operation: str,
        objects: list[dict[str, str | int]],
        ref: Optional[str] = None,
    ) -> LFSBatchResponse:
        """Perform batch operation to get transfer URLs.

        Args:
            operation: "download" or "upload"
            objects: List of {"oid": str, "size": int} dicts
            ref: Optional ref name

        Returns:
            Batch response from server
        """
        data: dict[
            str, str | list[str] | list[dict[str, str | int]] | dict[str, str]
        ] = {
            "operation": operation,
            "transfers": ["basic"],
            "objects": objects,
        }
        if ref:
            data["ref"] = {"name": ref}

        response = self._make_request(
            "POST", "/objects/batch", json.dumps(data).encode("utf-8")
        )
        response_data = json.loads(response)
        return self._parse_batch_response(response_data)

    def _parse_batch_response(self, data: dict) -> LFSBatchResponse:
        """Parse JSON response into LFSBatchResponse dataclass."""
        objects = []
        for obj_data in data.get("objects", []):
            actions = None
            if "actions" in obj_data:
                actions = {}
                for action_name, action_data in obj_data["actions"].items():
                    actions[action_name] = LFSAction(
                        href=action_data["href"],
                        header=action_data.get("header"),
                        expires_at=action_data.get("expires_at"),
                    )

            error = None
            if "error" in obj_data:
                error = LFSErrorInfo(
                    code=obj_data["error"]["code"], message=obj_data["error"]["message"]
                )

            batch_obj = LFSBatchObject(
                oid=obj_data["oid"],
                size=obj_data["size"],
                authenticated=obj_data.get("authenticated"),
                actions=actions,
                error=error,
            )
            objects.append(batch_obj)

        return LFSBatchResponse(
            transfer=data.get("transfer", "basic"),
            objects=objects,
            hash_algo=data.get("hash_algo"),
        )

    def download(self, oid: str, size: int, ref: Optional[str] = None) -> bytes:
        """Download an LFS object.

        Args:
            oid: Object ID (SHA256)
            size: Expected size
            ref: Optional ref name

        Returns:
            Object content
        """
        # Get download URL via batch API
        batch_resp = self.batch("download", [{"oid": oid, "size": size}], ref)

        if not batch_resp.objects:
            raise LFSError(f"No objects returned for {oid}")

        obj = batch_resp.objects[0]
        if obj.error:
            raise LFSError(f"Server error for {oid}: {obj.error.message}")

        if not obj.actions or "download" not in obj.actions:
            raise LFSError(f"No download actions for {oid}")

        download_action = obj.actions["download"]
        download_url = download_action.href

        # Download the object
        req = Request(download_url)
        if download_action.header:
            for name, value in download_action.header.items():
                req.add_header(name, value)

        with urlopen(req) as response:
            content = response.read()

        # Verify size
        if len(content) != size:
            raise LFSError(f"Downloaded size {len(content)} != expected {size}")

        # Verify SHA256
        actual_oid = hashlib.sha256(content).hexdigest()
        if actual_oid != oid:
            raise LFSError(f"Downloaded OID {actual_oid} != expected {oid}")

        return content

    def upload(
        self, oid: str, size: int, content: bytes, ref: Optional[str] = None
    ) -> None:
        """Upload an LFS object.

        Args:
            oid: Object ID (SHA256)
            size: Object size
            content: Object content
            ref: Optional ref name
        """
        # Get upload URL via batch API
        batch_resp = self.batch("upload", [{"oid": oid, "size": size}], ref)

        if not batch_resp.objects:
            raise LFSError(f"No objects returned for {oid}")

        obj = batch_resp.objects[0]
        if obj.error:
            raise LFSError(f"Server error for {oid}: {obj.error.message}")

        # If no actions, object already exists
        if not obj.actions:
            return

        if "upload" not in obj.actions:
            raise LFSError(f"No upload action for {oid}")

        upload_action = obj.actions["upload"]
        upload_url = upload_action.href

        # Upload the object
        req = Request(upload_url, data=content, method="PUT")
        if upload_action.header:
            for name, value in upload_action.header.items():
                req.add_header(name, value)

        with urlopen(req) as response:
            if response.status >= 400:
                raise LFSError(f"Upload failed with status {response.status}")

        # Verify if needed
        if obj.actions and "verify" in obj.actions:
            verify_action = obj.actions["verify"]
            verify_data = json.dumps({"oid": oid, "size": size}).encode("utf-8")

            req = Request(verify_action.href, data=verify_data, method="POST")
            req.add_header("Content-Type", "application/vnd.git-lfs+json")
            if verify_action.header:
                for name, value in verify_action.header.items():
                    req.add_header(name, value)

            with urlopen(req) as response:
                if response.status >= 400:
                    raise LFSError(f"Verification failed with status {response.status}")


class LFSError(Exception):
    """LFS-specific error."""
