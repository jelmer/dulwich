# lfs.py -- Implementation of the LFS
# Copyright (C) 2020 Jelmer Vernooij
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

"""Git Large File Storage (LFS) support.

This module provides support for Git LFS, which is a Git extension for
versioning large files. It replaces large files with text pointers inside Git,
while storing the file contents on a remote server.

Key components:
- LFS pointer file parsing and creation
- LFS object storage and retrieval
- HTTP client for LFS server communication
- Integration with dulwich repositories
"""

import hashlib
import json
import logging
import os
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, BinaryIO, Optional, Union
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import urllib3

    from .config import Config
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
        """Initialize LFSStore."""
        self.path = path

    @classmethod
    def create(cls, lfs_dir: str) -> "LFSStore":
        """Create a new LFS store."""
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
        """Create LFS store from repository."""
        lfs_dir = os.path.join(repo.controldir(), "lfs")
        if create:
            return cls.create(lfs_dir)
        return cls(lfs_dir)

    @classmethod
    def from_controldir(cls, controldir: str, create: bool = False) -> "LFSStore":
        """Create LFS store from control directory."""
        lfs_dir = os.path.join(controldir, "lfs")
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
        # First pass: compute SHA256 and collect data
        sha = hashlib.sha256()
        data_chunks = []
        for chunk in chunks:
            sha.update(chunk)
            data_chunks.append(chunk)

        sha_hex = sha.hexdigest()
        path = self._sha_path(sha_hex)

        # If object already exists, no need to write
        if os.path.exists(path):
            return sha_hex

        # Object doesn't exist, write it
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        tmpdir = os.path.join(self.path, "tmp")
        with tempfile.NamedTemporaryFile(dir=tmpdir, mode="wb", delete=False) as f:
            for chunk in data_chunks:
                f.write(chunk)
            f.flush()
            tmppath = f.name

        # Handle concurrent writes - if file already exists, just remove temp file
        if os.path.exists(path):
            os.remove(tmppath)
        else:
            os.rename(tmppath, path)
        return sha_hex


class LFSPointer:
    """Represents an LFS pointer file."""

    def __init__(self, oid: str, size: int) -> None:
        """Initialize LFSPointer."""
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

    def __init__(
        self, lfs_store: "LFSStore", config: Optional["Config"] = None
    ) -> None:
        """Initialize LFSFilterDriver."""
        self.lfs_store = lfs_store
        self.config = config

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

    def smudge(self, data: bytes, path: bytes = b"") -> bytes:
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
            # Object not found in LFS store, try to download it
            try:
                content = self._download_object(pointer)
                return content
            except LFSError as e:
                # Download failed, fall back to returning pointer
                logger.warning("LFS object download failed for %s: %s", pointer.oid, e)

                # Return pointer as-is when object is missing and download failed
                return data

    def _download_object(self, pointer: LFSPointer) -> bytes:
        """Download an LFS object from the server.

        Args:
            pointer: LFS pointer containing OID and size

        Returns:
            Downloaded content

        Raises:
            LFSError: If download fails for any reason
        """
        if self.config is None:
            raise LFSError("No configuration available for LFS download")

        # Create LFS client and download
        client = LFSClient.from_config(self.config)
        if client is None:
            raise LFSError("No LFS client available from configuration")
        content = client.download(pointer.oid, pointer.size)

        # Store the downloaded content in local LFS store
        stored_oid = self.lfs_store.write_object([content])

        # Verify the stored OID matches what we expected
        if stored_oid != pointer.oid:
            raise LFSError(
                f"Downloaded OID mismatch: expected {pointer.oid}, got {stored_oid}"
            )

        return content

    def cleanup(self) -> None:
        """Clean up any resources held by this filter driver."""
        # LFSFilterDriver doesn't hold any resources that need cleanup

    def reuse(self, config: Optional["Config"], filter_name: str) -> bool:
        """Check if this filter driver should be reused with the given configuration."""
        # LFSFilterDriver is stateless and lightweight, no need to cache
        return False


def _get_lfs_user_agent(config: Optional["Config"]) -> str:
    """Get User-Agent string for LFS requests, respecting git config."""
    try:
        if config:
            # Use configured user agent verbatim if set
            return config.get(b"http", b"useragent").decode()
    except KeyError:
        pass

    # Default LFS user agent (similar to git-lfs format)
    from . import __version__

    version_str = ".".join([str(x) for x in __version__])
    return f"git-lfs/dulwich/{version_str}"


def _is_valid_lfs_url(url: str) -> bool:
    """Check if a URL is valid for LFS.

    Git LFS supports http://, https://, and file:// URLs.

    Args:
        url: URL to validate

    Returns:
        True if URL is a valid LFS URL, False otherwise
    """
    parsed = urlparse(url)

    # Must have a scheme
    if not parsed.scheme:
        return False

    # Only support http, https, and file schemes
    if parsed.scheme not in ("http", "https", "file"):
        return False

    # http/https require a hostname
    if parsed.scheme in ("http", "https"):
        return bool(parsed.netloc)

    # file:// URLs must have a path (netloc is typically empty)
    if parsed.scheme == "file":
        return bool(parsed.path)

    return False


class LFSClient:
    """Base class for LFS client operations."""

    def __init__(self, url: str, config: Optional["Config"] = None) -> None:
        """Initialize LFS client.

        Args:
            url: LFS server URL (http://, https://, or file://)
            config: Optional git config for authentication/proxy settings
        """
        self._base_url = url.rstrip("/") + "/"  # Ensure trailing slash for urljoin
        self.config = config

    @property
    def url(self) -> str:
        """Get the LFS server URL without trailing slash."""
        return self._base_url.rstrip("/")

    def download(self, oid: str, size: int, ref: Optional[str] = None) -> bytes:
        """Download an LFS object.

        Args:
            oid: Object ID (SHA256)
            size: Expected size
            ref: Optional ref name

        Returns:
            Object content
        """
        raise NotImplementedError

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
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: "Config") -> Optional["LFSClient"]:
        """Create LFS client from git config.

        Returns the appropriate subclass (HTTPLFSClient or FileLFSClient)
        based on the URL scheme.
        """
        # Try to get LFS URL from config first
        try:
            url = config.get((b"lfs",), b"url").decode()
        except KeyError:
            pass
        else:
            # Validate explicitly configured URL - raise error if invalid
            if not _is_valid_lfs_url(url):
                raise ValueError(
                    f"Invalid lfs.url in config: {url!r}. "
                    "URL must be an absolute URL with scheme http://, https://, or file://."
                )

            # Return appropriate client based on scheme
            parsed = urlparse(url)
            if parsed.scheme in ("http", "https"):
                return HTTPLFSClient(url, config)
            elif parsed.scheme == "file":
                return FileLFSClient(url, config)
            else:
                # This shouldn't happen if _is_valid_lfs_url works correctly
                raise ValueError(f"Unsupported LFS URL scheme: {parsed.scheme}")

        # Fall back to deriving from remote URL (same as git-lfs)
        try:
            remote_url = config.get((b"remote", b"origin"), b"url").decode()
        except KeyError:
            pass
        else:
            # Convert SSH URLs to HTTPS if needed
            if remote_url.startswith("git@"):
                # Convert git@host:user/repo.git to https://host/user/repo.git
                if ":" in remote_url and "/" in remote_url:
                    host_and_path = remote_url[4:]  # Remove "git@"
                    if ":" in host_and_path:
                        host, path = host_and_path.split(":", 1)
                        remote_url = f"https://{host}/{path}"

            # Ensure URL ends with .git for consistent LFS endpoint
            if not remote_url.endswith(".git"):
                remote_url = f"{remote_url}.git"

            # Standard LFS endpoint is remote_url + "/info/lfs"
            lfs_url = f"{remote_url}/info/lfs"

            # Return None if derived URL is invalid (LFS is optional)
            if not _is_valid_lfs_url(lfs_url):
                return None

            # Derived URLs are always http/https
            return HTTPLFSClient(lfs_url, config)

        return None


class HTTPLFSClient(LFSClient):
    """LFS client for HTTP/HTTPS operations."""

    def __init__(self, url: str, config: Optional["Config"] = None) -> None:
        """Initialize HTTP LFS client.

        Args:
            url: LFS server URL (http:// or https://)
            config: Optional git config for authentication/proxy settings
        """
        super().__init__(url, config)
        self._pool_manager: Optional[urllib3.PoolManager] = None

    def _get_pool_manager(self) -> "urllib3.PoolManager":
        """Get urllib3 pool manager with git config applied."""
        if self._pool_manager is None:
            from dulwich.client import default_urllib3_manager

            self._pool_manager = default_urllib3_manager(self.config)
        return self._pool_manager

    def _make_request(
        self,
        method: str,
        path: str,
        data: Optional[bytes] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> bytes:
        """Make an HTTP request to the LFS server."""
        url = urljoin(self._base_url, path)
        req_headers = {
            "Accept": "application/vnd.git-lfs+json",
            "Content-Type": "application/vnd.git-lfs+json",
            "User-Agent": _get_lfs_user_agent(self.config),
        }
        if headers:
            req_headers.update(headers)

        # Use urllib3 pool manager with git config applied
        pool_manager = self._get_pool_manager()
        response = pool_manager.request(method, url, headers=req_headers, body=data)
        if response.status >= 400:
            raise ValueError(
                f"HTTP {response.status}: {response.data.decode('utf-8', errors='ignore')}"
            )
        return response.data

    def batch(
        self,
        operation: str,
        objects: list[dict[str, Union[str, int]]],
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
            str, Union[str, list[str], list[dict[str, Union[str, int]]], dict[str, str]]
        ] = {
            "operation": operation,
            "transfers": ["basic"],
            "objects": objects,
        }
        if ref:
            data["ref"] = {"name": ref}

        response = self._make_request(
            "POST", "objects/batch", json.dumps(data).encode("utf-8")
        )
        if not response:
            raise ValueError("Empty response from LFS server")
        response_data = json.loads(response)
        return self._parse_batch_response(response_data)

    def _parse_batch_response(self, data: Mapping[str, Any]) -> LFSBatchResponse:
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

        # Download the object using urllib3 with git config
        download_headers = {"User-Agent": _get_lfs_user_agent(self.config)}
        if download_action.header:
            download_headers.update(download_action.header)

        pool_manager = self._get_pool_manager()
        response = pool_manager.request("GET", download_url, headers=download_headers)
        content = response.data

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


class FileLFSClient(LFSClient):
    """LFS client for file:// URLs that accesses local filesystem."""

    def __init__(self, url: str, config: Optional["Config"] = None) -> None:
        """Initialize File LFS client.

        Args:
            url: LFS server URL (file://)
            config: Optional git config (unused for file:// URLs)
        """
        super().__init__(url, config)

        # Convert file:// URL to filesystem path
        from urllib.request import url2pathname

        parsed = urlparse(url)
        if parsed.scheme != "file":
            raise ValueError(f"FileLFSClient requires file:// URL, got {url!r}")

        # url2pathname handles the conversion properly across platforms
        path = url2pathname(parsed.path)
        self._local_store = LFSStore(path)

    def download(self, oid: str, size: int, ref: Optional[str] = None) -> bytes:
        """Download an LFS object from local filesystem.

        Args:
            oid: Object ID (SHA256)
            size: Expected size
            ref: Optional ref name (ignored for file:// URLs)

        Returns:
            Object content

        Raises:
            LFSError: If object not found or size mismatch
        """
        try:
            with self._local_store.open_object(oid) as f:
                content = f.read()
        except KeyError as exc:
            raise LFSError(f"Object not found: {oid}") from exc

        # Verify size
        if len(content) != size:
            raise LFSError(f"Size mismatch: expected {size}, got {len(content)}")

        # Verify SHA256
        actual_oid = hashlib.sha256(content).hexdigest()
        if actual_oid != oid:
            raise LFSError(f"OID mismatch: expected {oid}, got {actual_oid}")

        return content

    def upload(
        self, oid: str, size: int, content: bytes, ref: Optional[str] = None
    ) -> None:
        """Upload an LFS object to local filesystem.

        Args:
            oid: Object ID (SHA256)
            size: Object size
            content: Object content
            ref: Optional ref name (ignored for file:// URLs)

        Raises:
            LFSError: If size or OID mismatch
        """
        # Verify size
        if len(content) != size:
            raise LFSError(f"Size mismatch: expected {size}, got {len(content)}")

        # Verify SHA256
        actual_oid = hashlib.sha256(content).hexdigest()
        if actual_oid != oid:
            raise LFSError(f"OID mismatch: expected {oid}, got {actual_oid}")

        # Store the object
        stored_oid = self._local_store.write_object([content])
        if stored_oid != oid:
            raise LFSError(f"Storage OID mismatch: expected {oid}, got {stored_oid}")


class LFSError(Exception):
    """LFS-specific error."""
