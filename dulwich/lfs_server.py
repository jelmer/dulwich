# lfs_server.py -- Simple Git LFS server implementation
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

"""Simple Git LFS server implementation for testing."""

import hashlib
import json
import tempfile
import typing
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from .lfs import LFSStore


class LFSRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for LFS operations."""

    server: "LFSServer"  # Type annotation for the server attribute

    def send_json_response(
        self, status_code: int, data: Mapping[str, typing.Any]
    ) -> None:
        """Send a JSON response."""
        response = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/vnd.git-lfs+json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def do_POST(self) -> None:
        """Handle POST requests."""
        if self.path == "/objects/batch":
            self.handle_batch()
        elif self.path.startswith("/objects/") and self.path.endswith("/verify"):
            self.handle_verify()
        else:
            self.send_error(404, "Not Found")

    def do_PUT(self) -> None:
        """Handle PUT requests (uploads)."""
        if self.path.startswith("/objects/"):
            self.handle_upload()
        else:
            self.send_error(404, "Not Found")

    def do_GET(self) -> None:
        """Handle GET requests (downloads)."""
        if self.path.startswith("/objects/"):
            self.handle_download()
        else:
            self.send_error(404, "Not Found")

    def handle_batch(self) -> None:
        """Handle batch API requests."""
        content_length = int(self.headers["Content-Length"])
        request_data = self.rfile.read(content_length)

        try:
            batch_request = json.loads(request_data)
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        operation = batch_request.get("operation")
        objects = batch_request.get("objects", [])

        if operation not in ["download", "upload"]:
            self.send_error(400, "Invalid operation")
            return

        response_objects = []

        for obj in objects:
            oid = obj.get("oid")
            size = obj.get("size")

            if not oid or size is None:
                response_objects.append(
                    {
                        "oid": oid,
                        "size": size,
                        "error": {"code": 400, "message": "Missing oid or size"},
                    }
                )
                continue

            response_obj = {
                "oid": oid,
                "size": size,
            }

            if operation == "download":
                # Check if object exists
                if self._object_exists(oid):
                    response_obj["actions"] = {
                        "download": {
                            "href": f"http://{self.headers['Host']}/objects/{oid}",
                            "header": {"Accept": "application/octet-stream"},
                        }
                    }
                else:
                    response_obj["error"] = {"code": 404, "message": "Object not found"}
            else:  # upload
                response_obj["actions"] = {
                    "upload": {
                        "href": f"http://{self.headers['Host']}/objects/{oid}",
                        "header": {"Content-Type": "application/octet-stream"},
                    },
                    "verify": {
                        "href": f"http://{self.headers['Host']}/objects/{oid}/verify"
                    },
                }

            response_objects.append(response_obj)

        self.send_json_response(200, {"objects": response_objects})

    def handle_download(self) -> None:
        """Handle object download requests."""
        # Extract OID from path
        path_parts = self.path.strip("/").split("/")
        if len(path_parts) != 2:
            self.send_error(404, "Not Found")
            return

        oid = path_parts[1]

        try:
            with self.server.lfs_store.open_object(oid) as f:
                content = f.read()

            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except KeyError:
            self.send_error(404, "Object not found")

    def handle_upload(self) -> None:
        """Handle object upload requests."""
        # Extract OID from path
        path_parts = self.path.strip("/").split("/")
        if len(path_parts) != 2:
            self.send_error(404, "Not Found")
            return

        oid = path_parts[1]
        content_length = int(self.headers["Content-Length"])

        # Read content in chunks
        chunks = []
        remaining = content_length
        while remaining > 0:
            chunk_size = min(8192, remaining)
            chunk = self.rfile.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)

        # Calculate SHA256
        content = b"".join(chunks)
        calculated_oid = hashlib.sha256(content).hexdigest()

        # Verify OID matches
        if calculated_oid != oid:
            self.send_error(400, f"OID mismatch: expected {oid}, got {calculated_oid}")
            return

        # Check if object already exists
        if not self._object_exists(oid):
            # Store the object only if it doesn't exist
            self.server.lfs_store.write_object(chunks)

        self.send_response(200)
        self.end_headers()

    def handle_verify(self) -> None:
        """Handle object verification requests."""
        # Extract OID from path
        path_parts = self.path.strip("/").split("/")
        if len(path_parts) != 3 or path_parts[2] != "verify":
            self.send_error(404, "Not Found")
            return

        oid = path_parts[1]
        content_length = int(self.headers.get("Content-Length", 0))

        if content_length > 0:
            request_data = self.rfile.read(content_length)
            try:
                verify_request = json.loads(request_data)
                # Optionally validate size
                if "size" in verify_request:
                    # Could verify size matches stored object
                    pass
            except json.JSONDecodeError:
                pass

        # Check if object exists
        if self._object_exists(oid):
            self.send_response(200)
            self.end_headers()
        else:
            self.send_error(404, "Object not found")

    def _object_exists(self, oid: str) -> bool:
        """Check if an object exists in the store."""
        try:
            # Try to open the object - if it exists, close it immediately
            with self.server.lfs_store.open_object(oid):
                return True
        except KeyError:
            return False

    def log_message(self, format: str, *args: object) -> None:
        """Override to suppress request logging during tests."""
        if self.server.log_requests:
            super().log_message(format, *args)


class LFSServer(HTTPServer):
    """Simple LFS server for testing."""

    def __init__(
        self,
        server_address: tuple[str, int],
        lfs_store: LFSStore,
        log_requests: bool = False,
    ) -> None:
        """Initialize LFSServer.

        Args:
          server_address: Tuple of (host, port) to bind to
          lfs_store: LFS store instance to use
          log_requests: Whether to log incoming requests
        """
        super().__init__(server_address, LFSRequestHandler)
        self.lfs_store = lfs_store
        self.log_requests = log_requests


def run_lfs_server(
    host: str = "localhost",
    port: int = 0,
    lfs_dir: Optional[str] = None,
    log_requests: bool = False,
) -> tuple[LFSServer, str]:
    """Run an LFS server.

    Args:
        host: Host to bind to
        port: Port to bind to (0 for random)
        lfs_dir: Directory for LFS storage (temp dir if None)
        log_requests: Whether to log HTTP requests

    Returns:
        Tuple of (server, url) where url is the base URL for the server
    """
    if lfs_dir is None:
        lfs_dir = tempfile.mkdtemp()

    lfs_store = LFSStore.create(lfs_dir)
    server = LFSServer((host, port), lfs_store, log_requests)

    # Get the actual port if we used 0
    actual_port = server.server_address[1]
    url = f"http://{host}:{actual_port}"

    return server, url
