# swift.py -- Repo implementation atop OpenStack SWIFT
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Fabien Boucher <fabien.boucher@enovance.com>
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

"""Repo implementation atop OpenStack SWIFT."""

__all__ = [
    "PackInfoMissingObjectFinder",
    "SwiftConnector",
    "SwiftException",
    "SwiftInfoRefsContainer",
    "SwiftObjectStore",
    "SwiftPack",
    "SwiftPackData",
    "SwiftPackReader",
    "SwiftRepo",
    "SwiftSystemBackend",
    "cmd_daemon",
    "cmd_init",
    "load_conf",
    "load_pack_info",
    "main",
    "pack_info_create",
    "swift_load_pack_index",
]

# TODO: Refactor to share more code with dulwich/repo.py.
# TODO(fbo): Second attempt to _send() must be notified via real log
# TODO(fbo): More logs for operations

import json
import logging
import os
import posixpath
import stat
import sys
import tempfile
import urllib.parse as urlparse
import zlib
from collections.abc import Callable, Iterator, Mapping
from configparser import ConfigParser
from io import BytesIO
from typing import TYPE_CHECKING, Any, BinaryIO, cast

if TYPE_CHECKING:
    from dulwich.object_format import ObjectFormat

from geventhttpclient import HTTPClient

from ..file import _GitFile
from ..lru_cache import LRUSizeCache
from ..object_store import INFODIR, PACKDIR, PackBasedObjectStore
from ..objects import S_ISGITLINK, Blob, Commit, ObjectID, Tag, Tree
from ..pack import (
    ObjectContainer,
    Pack,
    PackData,
    PackIndex,
    PackIndexer,
    PackStreamCopier,
    _compute_object_size,
    compute_file_sha,
    iter_sha1,
    load_pack_index_file,
    read_pack_header,
    unpack_object,
    write_pack_header,
    write_pack_index_v2,
    write_pack_object,
)
from ..protocol import TCP_GIT_PORT, split_peeled_refs, write_info_refs
from ..refs import HEADREF, Ref, RefsContainer, read_info_refs
from ..repo import OBJECTDIR, BaseRepo
from ..server import Backend, BackendRepo, TCPGitServer
from .greenthreads import GreenThreadsMissingObjectFinder

"""
# Configuration file sample
[swift]
# Authentication URL (Keystone or Swift)
auth_url = http://127.0.0.1:5000/v2.0
# Authentication version to use
auth_ver = 2
# The tenant and username separated by a semicolon
username = admin;admin
# The user password
password = pass
# The Object storage region to use (auth v2) (Default RegionOne)
region_name = RegionOne
# The Object storage endpoint URL to use (auth v2) (Default internalURL)
endpoint_type = internalURL
# Concurrency to use for parallel tasks (Default 10)
concurrency = 10
# Size of the HTTP pool (Default 10)
http_pool_length = 10
# Timeout delay for HTTP connections (Default 20)
http_timeout = 20
# Chunk size to read from pack (Bytes) (Default 12228)
chunk_length = 12228
# Cache size (MBytes) (Default 20)
cache_length = 20
"""


class PackInfoMissingObjectFinder(GreenThreadsMissingObjectFinder):
    """Find missing objects required for pack generation."""

    def next(self) -> tuple[bytes, int, bytes | None] | None:
        """Get the next missing object.

        Returns:
          Tuple of (sha, pack_type_num, name) or None if no more objects
        """
        while True:
            if not self.objects_to_send:
                return None
            (sha, name, leaf, _) = self.objects_to_send.pop()
            if sha not in self.sha_done:
                break
        if not leaf:
            try:
                obj = self.object_store[sha]
                if isinstance(obj, Commit):
                    self.add_todo([(obj.tree, b"", None, False)])
                elif isinstance(obj, Tree):
                    tree_items = [
                        (
                            item.sha,
                            item.path
                            if isinstance(item.path, bytes)
                            else item.path.encode("utf-8")
                            if item.path is not None
                            else b"",
                            None,
                            False,
                        )
                        for item in obj.items()
                        if item.sha is not None
                    ]
                    self.add_todo(tree_items)
                elif isinstance(obj, Tag):
                    self.add_todo([(obj.object[1], None, None, False)])
                if sha in self._tagged:
                    self.add_todo([(self._tagged[sha], None, None, True)])
            except KeyError:
                pass
        self.sha_done.add(sha)
        self.progress(f"counting objects: {len(self.sha_done)}\r".encode())
        return (
            sha,
            0,
            name if isinstance(name, bytes) else name.encode("utf-8") if name else None,
        )


def load_conf(path: str | None = None, file: str | None = None) -> ConfigParser:
    """Load configuration in global var CONF.

    Args:
      path: The path to the configuration file
      file: If provided read instead the file like object
    """
    conf = ConfigParser()
    if file:
        conf.read_file(file, path)
    else:
        confpath = None
        if not path:
            try:
                confpath = os.environ["DULWICH_SWIFT_CFG"]
            except KeyError as exc:
                raise Exception("You need to specify a configuration file") from exc
        else:
            confpath = path
        if not os.path.isfile(confpath):
            raise Exception(f"Unable to read configuration file {confpath}")
        conf.read(confpath)
    return conf


def swift_load_pack_index(
    scon: "SwiftConnector", filename: str, object_format: ObjectFormat
) -> "PackIndex":
    """Read a pack index file from Swift.

    Args:
      scon: a `SwiftConnector` instance
      filename: Path to the index file objectise
      object_format: Object format for this pack
    Returns: a `PackIndexer` instance
    """
    f = scon.get_object(filename)
    if f is None:
        raise Exception(f"Could not retrieve index file {filename}")
    if isinstance(f, bytes):
        f = BytesIO(f)
    return load_pack_index_file(filename, f, object_format)


def pack_info_create(pack_data: "PackData", pack_index: "PackIndex") -> bytes:
    """Create pack info file contents.

    Args:
      pack_data: The pack data object
      pack_index: The pack index object

    Returns:
      Compressed JSON bytes containing pack information
    """
    pack = Pack.from_objects(pack_data, pack_index)
    info: dict[bytes, Any] = {}
    for obj in pack.iterobjects():
        # Commit
        if obj.type_num == Commit.type_num:
            commit_obj = obj
            assert isinstance(commit_obj, Commit)
            info[obj.id] = (obj.type_num, commit_obj.parents, commit_obj.tree)
        # Tree
        elif obj.type_num == Tree.type_num:
            tree_obj = obj
            assert isinstance(tree_obj, Tree)
            shas = [
                (s, n, not stat.S_ISDIR(m))
                for n, m, s in tree_obj.items()
                if m is not None and not S_ISGITLINK(m)
            ]
            info[obj.id] = (obj.type_num, shas)
        # Blob
        elif obj.type_num == Blob.type_num:
            info[obj.id] = (obj.type_num,)
        # Tag
        elif obj.type_num == Tag.type_num:
            tag_obj = obj
            assert isinstance(tag_obj, Tag)
            info[obj.id] = (obj.type_num, tag_obj.object[1])
    return zlib.compress(json.dumps(info).encode("utf-8"))


def load_pack_info(
    filename: str,
    scon: "SwiftConnector | None" = None,
    file: BinaryIO | None = None,
) -> dict[str, Any] | None:
    """Load pack info from Swift or file.

    Args:
      filename: The pack info filename
      scon: Optional Swift connector to use for loading
      file: Optional file object to read from instead

    Returns:
      Dictionary containing pack information or None if not found
    """
    if not file:
        if scon is None:
            return None
        obj = scon.get_object(filename)
        if obj is None:
            return None
        if isinstance(obj, bytes):
            return cast(dict[str, Any], json.loads(zlib.decompress(obj)))
        else:
            f: BinaryIO = obj
    else:
        f = file
    try:
        return cast(dict[str, Any], json.loads(zlib.decompress(f.read())))
    finally:
        if hasattr(f, "close"):
            f.close()


class SwiftException(Exception):
    """Exception raised for Swift-related errors."""


class SwiftConnector:
    """A Connector to swift that manage authentication and errors catching."""

    def __init__(self, root: str, conf: ConfigParser) -> None:
        """Initialize a SwiftConnector.

        Args:
          root: The swift container that will act as Git bare repository
          conf: A ConfigParser Object
        """
        self.conf = conf
        self.auth_ver = self.conf.get("swift", "auth_ver")
        if self.auth_ver not in ["1", "2"]:
            raise NotImplementedError("Wrong authentication version use either 1 or 2")
        self.auth_url = self.conf.get("swift", "auth_url")
        self.user = self.conf.get("swift", "username")
        self.password = self.conf.get("swift", "password")
        self.concurrency = self.conf.getint("swift", "concurrency") or 10
        self.http_timeout = self.conf.getint("swift", "http_timeout") or 20
        self.http_pool_length = self.conf.getint("swift", "http_pool_length") or 10
        self.region_name = self.conf.get("swift", "region_name") or "RegionOne"
        self.endpoint_type = self.conf.get("swift", "endpoint_type") or "internalURL"
        self.cache_length = self.conf.getint("swift", "cache_length") or 20
        self.chunk_length = self.conf.getint("swift", "chunk_length") or 12228
        self.root = root
        block_size = 1024 * 12  # 12KB
        if self.auth_ver == "1":
            self.storage_url, self.token = self.swift_auth_v1()
        else:
            self.storage_url, self.token = self.swift_auth_v2()

        token_header = {"X-Auth-Token": str(self.token)}
        self.httpclient = HTTPClient.from_url(
            str(self.storage_url),
            concurrency=self.http_pool_length,
            block_size=block_size,
            connection_timeout=self.http_timeout,
            network_timeout=self.http_timeout,
            headers=token_header,
        )
        self.base_path = str(
            posixpath.join(urlparse.urlparse(self.storage_url).path, self.root)
        )

    def swift_auth_v1(self) -> tuple[str, str]:
        """Authenticate with Swift using v1 authentication.

        Returns:
          Tuple of (storage_url, auth_token)

        Raises:
          SwiftException: If authentication fails
        """
        self.user = self.user.replace(";", ":")
        auth_httpclient = HTTPClient.from_url(
            self.auth_url,
            connection_timeout=self.http_timeout,
            network_timeout=self.http_timeout,
        )
        headers = {"X-Auth-User": self.user, "X-Auth-Key": self.password}
        path = urlparse.urlparse(self.auth_url).path

        ret = auth_httpclient.request("GET", path, headers=headers)

        # Should do something with redirections (301 in my case)

        if ret.status_code < 200 or ret.status_code >= 300:
            raise SwiftException(
                "AUTH v1.0 request failed on "
                + f"{self.auth_url} with error code {ret.status_code} ({ret.items()!s})"
            )
        storage_url = ret["X-Storage-Url"]
        token = ret["X-Auth-Token"]
        return storage_url, token

    def swift_auth_v2(self) -> tuple[str, str]:
        """Authenticate with Swift using v2 authentication.

        Returns:
          Tuple of (storage_url, auth_token)

        Raises:
          SwiftException: If authentication fails
        """
        self.tenant, self.user = self.user.split(";")
        auth_dict = {}
        auth_dict["auth"] = {
            "passwordCredentials": {
                "username": self.user,
                "password": self.password,
            },
            "tenantName": self.tenant,
        }
        auth_json = json.dumps(auth_dict)
        headers = {"Content-Type": "application/json"}
        auth_httpclient = HTTPClient.from_url(
            self.auth_url,
            connection_timeout=self.http_timeout,
            network_timeout=self.http_timeout,
        )
        path = urlparse.urlparse(self.auth_url).path
        if not path.endswith("tokens"):
            path = posixpath.join(path, "tokens")
        ret = auth_httpclient.request("POST", path, body=auth_json, headers=headers)

        if ret.status_code < 200 or ret.status_code >= 300:
            raise SwiftException(
                "AUTH v2.0 request failed on "
                + f"{str(auth_httpclient.get_base_url()) + path} with error code {ret.status_code} ({ret.items()!s})"
            )
        auth_ret_json = json.loads(ret.read())
        token = auth_ret_json["access"]["token"]["id"]
        catalogs = auth_ret_json["access"]["serviceCatalog"]
        object_store = next(
            o_store for o_store in catalogs if o_store["type"] == "object-store"
        )
        endpoints = object_store["endpoints"]
        endpoint = next(
            endp for endp in endpoints if endp["region"] == self.region_name
        )
        return endpoint[self.endpoint_type], token

    def test_root_exists(self) -> bool | None:
        """Check that Swift container exist.

        Returns: True if exist or None it not
        """
        ret = self.httpclient.request("HEAD", self.base_path)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException(
                f"HEAD request failed with error code {ret.status_code}"
            )
        return True

    def create_root(self) -> None:
        """Create the Swift container.

        Raises:
          SwiftException: if unable to create
        """
        if not self.test_root_exists():
            ret = self.httpclient.request("PUT", self.base_path)
            if ret.status_code < 200 or ret.status_code > 300:
                raise SwiftException(
                    f"PUT request failed with error code {ret.status_code}"
                )

    def get_container_objects(self) -> list[dict[str, Any]] | None:
        """Retrieve objects list in a container.

        Returns: A list of dict that describe objects
                 or None if container does not exist
        """
        qs = "?format=json"
        path = self.base_path + qs
        ret = self.httpclient.request("GET", path)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException(
                f"GET request failed with error code {ret.status_code}"
            )
        content = ret.read()
        return cast(list[dict[str, Any]], json.loads(content))

    def get_object_stat(self, name: str) -> dict[str, Any] | None:
        """Retrieve object stat.

        Args:
          name: The object name
        Returns:
          A dict that describe the object or None if object does not exist
        """
        path = self.base_path + "/" + name
        ret = self.httpclient.request("HEAD", path)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException(
                f"HEAD request failed with error code {ret.status_code}"
            )
        resp_headers = {}
        for header, value in ret.items():
            resp_headers[header.lower()] = value
        return resp_headers

    def put_object(self, name: str, content: BinaryIO) -> None:
        """Put an object.

        Args:
          name: The object name
          content: A file object
        Raises:
          SwiftException: if unable to create
        """
        content.seek(0)
        data = content.read()
        path = self.base_path + "/" + name
        headers = {"Content-Length": str(len(data))}

        def _send() -> object:
            ret = self.httpclient.request("PUT", path, body=data, headers=headers)
            return ret

        try:
            # Sometime got Broken Pipe - Dirty workaround
            ret = _send()
        except (BrokenPipeError, ConnectionError):
            # Second attempt work
            ret = _send()

        if ret.status_code < 200 or ret.status_code > 300:  # type: ignore
            raise SwiftException(
                f"PUT request failed with error code {ret.status_code}"  # type: ignore
            )

    def get_object(self, name: str, range: str | None = None) -> bytes | BytesIO | None:
        """Retrieve an object.

        Args:
          name: The object name
          range: A string range like "0-10" to
                 retrieve specified bytes in object content
        Returns:
          A file like instance or bytestring if range is specified
        """
        headers = {}
        if range:
            headers["Range"] = f"bytes={range}"
        path = self.base_path + "/" + name
        ret = self.httpclient.request("GET", path, headers=headers)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException(
                f"GET request failed with error code {ret.status_code}"
            )
        content = cast(bytes, ret.read())

        if range:
            return content
        return BytesIO(content)

    def del_object(self, name: str) -> None:
        """Delete an object.

        Args:
          name: The object name
        Raises:
          SwiftException: if unable to delete
        """
        path = self.base_path + "/" + name
        ret = self.httpclient.request("DELETE", path)
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException(
                f"DELETE request failed with error code {ret.status_code}"
            )

    def del_root(self) -> None:
        """Delete the root container by removing container content.

        Raises:
          SwiftException: if unable to delete
        """
        objects = self.get_container_objects()
        if objects:
            for obj in objects:
                self.del_object(obj["name"])
        ret = self.httpclient.request("DELETE", self.base_path)
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException(
                f"DELETE request failed with error code {ret.status_code}"
            )


class SwiftPackReader:
    """A SwiftPackReader that mimic read and sync method.

    The reader allows to read a specified amount of bytes from
    a given offset of a Swift object. A read offset is kept internally.
    The reader will read from Swift a specified amount of data to complete
    its internal buffer. chunk_length specify the amount of data
    to read from Swift.
    """

    def __init__(self, scon: SwiftConnector, filename: str, pack_length: int) -> None:
        """Initialize a SwiftPackReader.

        Args:
          scon: a `SwiftConnector` instance
          filename: the pack filename
          pack_length: The size of the pack object
        """
        self.scon = scon
        self.filename = filename
        self.pack_length = pack_length
        self.offset = 0
        self.base_offset = 0
        self.buff = b""
        self.buff_length = self.scon.chunk_length

    def _read(self, more: bool = False) -> None:
        if more:
            self.buff_length = self.buff_length * 2
        offset = self.base_offset
        r = min(self.base_offset + self.buff_length, self.pack_length)
        ret = self.scon.get_object(self.filename, range=f"{offset}-{r}")
        if ret is None:
            self.buff = b""
        elif isinstance(ret, bytes):
            self.buff = ret
        else:
            self.buff = ret.read()

    def read(self, length: int) -> bytes:
        """Read a specified amount of Bytes form the pack object.

        Args:
          length: amount of bytes to read
        Returns:
          a bytestring
        """
        end = self.offset + length
        if self.base_offset + end > self.pack_length:
            data = self.buff[self.offset :]
            self.offset = end
            return data
        if end > len(self.buff):
            # Need to read more from swift
            self._read(more=True)
            return self.read(length)
        data = self.buff[self.offset : end]
        self.offset = end
        return data

    def seek(self, offset: int) -> None:
        """Seek to a specified offset.

        Args:
          offset: the offset to seek to
        """
        self.base_offset = offset
        self._read()
        self.offset = 0

    def read_checksum(self) -> bytes:
        """Read the checksum from the pack.

        Returns: the checksum bytestring
        """
        ret = self.scon.get_object(self.filename, range="-20")
        if ret is None:
            return b""
        elif isinstance(ret, bytes):
            return ret
        else:
            return ret.read()


class SwiftPackData(PackData):
    """The data contained in a packfile.

    We use the SwiftPackReader to read bytes from packs stored in Swift
    using the Range header feature of Swift.
    """

    def __init__(
        self,
        scon: SwiftConnector,
        filename: str | os.PathLike[str],
        object_format: "ObjectFormat | None" = None,
    ) -> None:
        """Initialize a SwiftPackReader.

        Args:
          scon: a `SwiftConnector` instance
          filename: the pack filename
          object_format: Object format for this pack
        """
        from dulwich.object_format import DEFAULT_OBJECT_FORMAT

        if object_format is None:
            import warnings

            warnings.warn(
                "SwiftPackData() should be called with object_format parameter",
                DeprecationWarning,
                stacklevel=2,
            )
            object_format = DEFAULT_OBJECT_FORMAT
        self.object_format = object_format
        self.scon = scon
        self._filename = filename
        self._header_size = 12
        headers = self.scon.get_object_stat(str(self._filename))
        if headers is None:
            raise Exception(f"Could not get stats for {self._filename}")
        self.pack_length = int(headers["content-length"])
        pack_reader = SwiftPackReader(self.scon, str(self._filename), self.pack_length)
        (_version, self._num_objects) = read_pack_header(pack_reader.read)
        self._offset_cache = LRUSizeCache(
            1024 * 1024 * self.scon.cache_length,
            compute_size=_compute_object_size,
        )
        self.pack = None

    def get_object_at(
        self, offset: int
    ) -> tuple[int, tuple[bytes | int, list[bytes]] | list[bytes]]:
        """Get the object at a specific offset in the pack.

        Args:
          offset: The offset in the pack file

        Returns:
          Tuple of (pack_type_num, object_data)
        """
        if offset in self._offset_cache:
            return self._offset_cache[offset]
        assert offset >= self._header_size
        pack_reader = SwiftPackReader(self.scon, str(self._filename), self.pack_length)
        pack_reader.seek(offset)
        unpacked, _ = unpack_object(pack_reader.read, self.object_format.hash_func)
        obj_data = unpacked._obj()
        return (unpacked.pack_type_num, obj_data)

    def get_stored_checksum(self) -> bytes:
        """Get the stored checksum for this pack.

        Returns:
          The pack checksum as bytes
        """
        pack_reader = SwiftPackReader(self.scon, str(self._filename), self.pack_length)
        return pack_reader.read_checksum()

    def close(self) -> None:
        """Close the pack data (no-op for Swift)."""


class SwiftPack(Pack):
    """A Git pack object.

    Same implementation as pack.Pack except that _idx_load and
    _data_load are bounded to Swift version of load_pack_index and
    PackData.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize SwiftPack.

        Args:
          *args: Arguments to pass to parent class
          **kwargs: Keyword arguments, must include 'scon' (SwiftConnector)
        """
        self.scon: SwiftConnector = kwargs["scon"]  # type: ignore
        del kwargs["scon"]
        super().__init__(*args, **kwargs)  # type: ignore
        self._pack_info_path = self._basename + ".info"
        self._pack_info: dict[str, Any] | None = None
        self._pack_info_load: Callable[[], dict[str, Any] | None] = (
            lambda: load_pack_info(self._pack_info_path, self.scon)
        )
        self._idx_load = lambda: swift_load_pack_index(
            self.scon, self._idx_path, self.object_format
        )
        self._data_load = lambda: SwiftPackData(self.scon, self._data_path)

    @property
    def pack_info(self) -> dict[str, Any] | None:
        """The pack data object being used."""
        if self._pack_info is None:
            self._pack_info = self._pack_info_load()
        return self._pack_info


class SwiftObjectStore(PackBasedObjectStore):
    """A Swift Object Store.

    Allow to manage a bare Git repository from Openstack Swift.
    This object store only supports pack files and not loose objects.
    """

    def __init__(self, scon: SwiftConnector) -> None:
        """Open a Swift object store.

        Args:
          scon: A `SwiftConnector` instance
        """
        super().__init__()
        self.scon = scon
        self.root = self.scon.root
        self.pack_dir = posixpath.join(OBJECTDIR, PACKDIR)
        self._alternates = None

    def _update_pack_cache(self) -> list[Any]:
        objects = self.scon.get_container_objects()
        if objects is None:
            return []
        pack_files = [
            o["name"].replace(".pack", "")
            for o in objects
            if o["name"].endswith(".pack")
        ]
        ret = []
        for basename in pack_files:
            pack = SwiftPack(basename, object_format=self.object_format, scon=self.scon)
            self._pack_cache[basename] = pack
            ret.append(pack)
        return ret

    def _iter_loose_objects(self) -> Iterator[Any]:
        """Loose objects are not supported by this repository."""
        return iter([])

    def pack_info_get(self, sha: ObjectID) -> tuple[Any, ...] | None:
        """Get pack info for a specific SHA.

        Args:
          sha: The SHA to look up

        Returns:
          Pack info tuple or None if not found
        """
        for pack in self.packs:
            if sha in pack:
                if hasattr(pack, "pack_info"):
                    pack_info = pack.pack_info
                    if pack_info is not None:
                        return cast(tuple[Any, ...] | None, pack_info.get(sha))
        return None

    def _collect_ancestors(
        self, heads: list[Any], common: set[Any] | None = None
    ) -> tuple[set[Any], set[Any]]:
        if common is None:
            common = set()

        def _find_parents(commit: ObjectID) -> list[Any]:
            for pack in self.packs:
                if commit in pack:
                    try:
                        if hasattr(pack, "pack_info"):
                            pack_info = pack.pack_info
                            if pack_info is not None:
                                return cast(list[Any], pack_info[commit][1])
                    except KeyError:
                        # Seems to have no parents
                        return []
            return []

        bases = set()
        commits = set()
        queue = []
        queue.extend(heads)
        while queue:
            e = queue.pop(0)
            if e in common:
                bases.add(e)
            elif e not in commits:
                commits.add(e)
                parents = _find_parents(e)
                queue.extend(parents)
        return (commits, bases)

    def add_pack(self) -> tuple[BytesIO, Callable[[], None], Callable[[], None]]:
        """Add a new pack to this object store.

        Returns: Fileobject to write to and a commit function to
            call when the pack is finished.
        """
        f = BytesIO()

        def commit() -> "SwiftPack | None":
            """Commit the pack to Swift storage.

            Returns:
              The created SwiftPack or None if empty
            """
            f.seek(0)
            from typing import cast

            from ..file import _GitFile

            pack = PackData(
                file=cast(_GitFile, f), filename="", object_format=self.object_format
            )
            entries = pack.sorted_entries()
            if entries:
                basename = posixpath.join(
                    self.pack_dir,
                    f"pack-{iter_sha1(entry[0] for entry in entries).decode('ascii')}",
                )
                index = BytesIO()
                write_pack_index_v2(index, entries, pack.get_stored_checksum())
                self.scon.put_object(basename + ".pack", f)
                f.close()
                self.scon.put_object(basename + ".idx", index)
                index.close()
                final_pack = SwiftPack(
                    basename, object_format=self.object_format, scon=self.scon
                )
                final_pack.check_length_and_checksum()
                self._add_cached_pack(basename, final_pack)
                return final_pack
            else:
                return None

        def abort() -> None:
            """Abort the pack operation (no-op)."""

        def commit_wrapper() -> None:
            """Wrapper that discards the return value."""
            commit()

        return f, commit_wrapper, abort

    def add_object(self, obj: object) -> None:
        """Add a single object to the store.

        Args:
          obj: The object to add
        """
        self.add_objects(
            [
                (obj, None),  # type: ignore
            ]
        )

    def _pack_cache_stale(self) -> bool:
        return False

    def _get_loose_object(self, sha: bytes) -> None:
        return None

    def add_thin_pack(
        self, read_all: Callable[[int], bytes], read_some: Callable[[int], bytes]
    ) -> "SwiftPack":
        """Read a thin pack.

        Read it from a stream and complete it in a temporary file.
        Then the pack and the corresponding index file are uploaded to Swift.
        """
        fd, path = tempfile.mkstemp(prefix="tmp_pack_")
        f = os.fdopen(fd, "w+b")
        try:
            pack_data = PackData(
                file=cast(_GitFile, f), filename=path, object_format=self.object_format
            )
            indexer = PackIndexer(
                cast(BinaryIO, pack_data._file),
                self.object_format.hash_func,
                resolve_ext_ref=None,
            )
            copier = PackStreamCopier(
                self.object_format.hash_func, read_all, read_some, f, delta_iter=None
            )
            copier.verify()
            return self._complete_thin_pack(f, path, copier, indexer)
        finally:
            f.close()
            os.unlink(path)

    def _complete_thin_pack(
        self, f: BinaryIO, path: str, copier: object, indexer: object
    ) -> "SwiftPack":
        entries = list(indexer)  # type: ignore

        # Update the header with the new number of objects.
        f.seek(0)
        write_pack_header(f, len(entries) + len(indexer.ext_refs()))  # type: ignore

        # Must flush before reading (http://bugs.python.org/issue3207)
        f.flush()

        # Rescan the rest of the pack, computing the SHA with the new header.
        new_sha = compute_file_sha(
            f,
            hash_func=self.object_format.hash_func,
            end_ofs=-self.object_format.oid_length,
        )

        # Must reposition before writing (http://bugs.python.org/issue3207)
        f.seek(0, os.SEEK_CUR)

        # Complete the pack.
        for ext_sha in indexer.ext_refs():  # type: ignore
            assert len(ext_sha) in (20, 32)  # SHA-1 or SHA-256
            type_num, data = self.get_raw(ext_sha)
            offset = f.tell()
            crc32 = write_pack_object(f, type_num, data, sha=new_sha)  # type: ignore
            entries.append((ext_sha, offset, crc32))
        pack_sha = new_sha.digest()
        f.write(pack_sha)
        f.flush()

        # Move the pack in.
        entries.sort()
        pack_base_name = posixpath.join(
            self.pack_dir,
            "pack-" + os.fsdecode(iter_sha1(e[0] for e in entries)),
        )
        self.scon.put_object(pack_base_name + ".pack", f)

        # Write the index.
        filename = pack_base_name + ".idx"
        index_file = BytesIO()
        write_pack_index_v2(index_file, entries, pack_sha)
        self.scon.put_object(filename, index_file)

        # Write pack info.
        f.seek(0)
        pack_data = PackData(
            filename="", file=cast(_GitFile, f), object_format=self.object_format
        )
        index_file.seek(0)
        pack_index = load_pack_index_file("", index_file, self.object_format)
        serialized_pack_info = pack_info_create(pack_data, pack_index)
        f.close()
        index_file.close()
        pack_info_file = BytesIO(serialized_pack_info)
        filename = pack_base_name + ".info"
        self.scon.put_object(filename, pack_info_file)
        pack_info_file.close()

        # Add the pack to the store and return it.
        final_pack = SwiftPack(
            pack_base_name, object_format=self.object_format, scon=self.scon
        )
        final_pack.check_length_and_checksum()
        self._add_cached_pack(pack_base_name, final_pack)
        return final_pack


class SwiftInfoRefsContainer(RefsContainer):
    """Manage references in info/refs object."""

    def __init__(self, scon: SwiftConnector, store: object) -> None:
        """Initialize SwiftInfoRefsContainer.

        Args:
          scon: Swift connector instance
          store: Object store instance
        """
        self.scon = scon
        self.filename = "info/refs"
        self.store = store
        f = self.scon.get_object(self.filename)
        if not f:
            f = BytesIO(b"")
        elif isinstance(f, bytes):
            f = BytesIO(f)

        # Initialize refs from info/refs file
        self._refs: dict[Ref, ObjectID] = {}
        self._peeled: dict[Ref, ObjectID] = {}
        refs = read_info_refs(f)
        (self._refs, self._peeled) = split_peeled_refs(refs)

    def _load_check_ref(
        self, name: Ref, old_ref: ObjectID | None
    ) -> dict[Ref, ObjectID] | bool:
        self._check_refname(name)
        obj = self.scon.get_object(self.filename)
        if not obj:
            return {}
        if isinstance(obj, bytes):
            f = BytesIO(obj)
        else:
            f = obj
        refs = read_info_refs(f)
        (refs, _peeled) = split_peeled_refs(refs)
        if old_ref is not None:
            if refs[name] != old_ref:
                return False
        return refs

    def _write_refs(self, refs: Mapping[Ref, ObjectID]) -> None:
        f = BytesIO()
        f.writelines(write_info_refs(refs, cast("ObjectContainer", self.store)))
        self.scon.put_object(self.filename, f)

    def set_if_equals(
        self,
        name: Ref,
        old_ref: ObjectID | None,
        new_ref: ObjectID,
        committer: bytes | None = None,
        timestamp: float | None = None,
        timezone: int | None = None,
        message: bytes | None = None,
    ) -> bool:
        """Set a refname to new_ref only if it currently equals old_ref."""
        if name == HEADREF:
            return True
        refs = self._load_check_ref(name, old_ref)
        if not isinstance(refs, dict):
            return False
        refs[name] = new_ref
        self._write_refs(refs)
        self._refs[name] = new_ref
        return True

    def remove_if_equals(
        self,
        name: Ref,
        old_ref: ObjectID | None,
        committer: object = None,
        timestamp: object = None,
        timezone: object = None,
        message: object = None,
    ) -> bool:
        """Remove a refname only if it currently equals old_ref."""
        if name == HEADREF:
            return True
        refs = self._load_check_ref(name, old_ref)
        if not isinstance(refs, dict):
            return False
        del refs[name]
        self._write_refs(refs)
        del self._refs[name]
        return True

    def read_loose_ref(self, name: Ref) -> bytes | None:
        """Read a loose reference."""
        return self._refs.get(name, None)

    def get_packed_refs(self) -> dict[Ref, ObjectID]:
        """Get packed references."""
        return {}

    def get_peeled(self, name: Ref) -> ObjectID | None:
        """Get peeled version of a reference."""
        try:
            return self._peeled[name]
        except KeyError:
            ref_value = self._refs.get(name)
            # Only return if it's an ObjectID (not a symref)
            if isinstance(ref_value, bytes) and len(ref_value) == 40:
                return ObjectID(ref_value)
            return None

    def allkeys(self) -> set[Ref]:
        """Get all reference names.

        Returns:
          Set of reference names as Ref
        """
        try:
            self._refs[HEADREF] = self._refs[Ref(b"refs/heads/master")]
        except KeyError:
            pass
        return set(self._refs.keys())


class SwiftRepo(BaseRepo):
    """A Git repository backed by Swift object storage."""

    def __init__(self, root: str, conf: ConfigParser) -> None:
        """Init a Git bare Repository on top of a Swift container.

        References are managed in info/refs objects by
        `SwiftInfoRefsContainer`. The root attribute is the Swift
        container that contain the Git bare repository.

        Args:
          root: The container which contains the bare repo
          conf: A ConfigParser object
        """
        self.root = root.lstrip("/")
        self.conf = conf
        self.scon = SwiftConnector(self.root, self.conf)
        objects = self.scon.get_container_objects()
        if not objects:
            raise Exception(f"There is not any GIT repo here : {self.root}")
        object_names = [o["name"].split("/")[0] for o in objects]
        if OBJECTDIR not in object_names:
            raise Exception(f"This repository ({self.root}) is not bare.")
        self.bare = True
        self._controldir = self.root
        object_store = SwiftObjectStore(self.scon)
        refs = SwiftInfoRefsContainer(self.scon, object_store)
        BaseRepo.__init__(self, object_store, refs)

    def _determine_file_mode(self) -> bool:
        """Probe the file-system to determine whether permissions can be trusted.

        Returns: True if permissions can be trusted, False otherwise.
        """
        return False

    def _put_named_file(self, filename: str, contents: bytes) -> None:
        """Put an object in a Swift container.

        Args:
          filename: the path to the object to put on Swift
          contents: the content as bytestring
        """
        with BytesIO() as f:
            f.write(contents)
            self.scon.put_object(filename, f)

    @classmethod
    def init_bare(cls, scon: SwiftConnector, conf: ConfigParser) -> "SwiftRepo":
        """Create a new bare repository.

        Args:
          scon: a `SwiftConnector` instance
          conf: a ConfigParser object
        Returns:
          a `SwiftRepo` instance
        """
        scon.create_root()
        for obj in [
            posixpath.join(OBJECTDIR, PACKDIR),
            posixpath.join(INFODIR, "refs"),
        ]:
            scon.put_object(obj, BytesIO(b""))
        ret = cls(scon.root, conf)
        ret._init_files(True)
        return ret


class SwiftSystemBackend(Backend):
    """Backend for serving Git repositories from Swift."""

    def __init__(self, logger: "logging.Logger", conf: ConfigParser) -> None:
        """Initialize SwiftSystemBackend.

        Args:
          logger: Logger instance
          conf: Configuration parser instance
        """
        self.conf = conf
        self.logger = logger

    def open_repository(self, path: str) -> "BackendRepo":
        """Open a repository at the given path.

        Args:
          path: Path to the repository in Swift

        Returns:
          SwiftRepo instance
        """
        self.logger.info("opening repository at %s", path)
        return cast("BackendRepo", SwiftRepo(path, self.conf))


def cmd_daemon(args: list[str]) -> None:
    """Start a TCP git server for Swift repositories.

    Args:
      args: Command line arguments
    """
    import optparse

    parser = optparse.OptionParser()
    parser.add_option(
        "-l",
        "--listen_address",
        dest="listen_address",
        default="127.0.0.1",
        help="Binding IP address.",
    )
    parser.add_option(
        "-p",
        "--port",
        dest="port",
        type=int,
        default=TCP_GIT_PORT,
        help="Binding TCP port.",
    )
    parser.add_option(
        "-c",
        "--swift_config",
        dest="swift_config",
        default="",
        help="Path to the configuration file for Swift backend.",
    )
    options, args = parser.parse_args(args)

    try:
        import gevent
        import geventhttpclient  # noqa: F401
    except ImportError:
        print(
            "gevent and geventhttpclient libraries are mandatory "
            " for use the Swift backend."
        )
        sys.exit(1)
    import gevent.monkey

    gevent.monkey.patch_socket()
    from dulwich import log_utils

    logger = log_utils.getLogger(__name__)
    conf = load_conf(options.swift_config)
    backend = SwiftSystemBackend(logger, conf)

    log_utils.default_logging_config()
    server = TCPGitServer(backend, options.listen_address, port=options.port)
    server.serve_forever()


def cmd_init(args: list[str]) -> None:
    """Initialize a new Git repository in Swift.

    Args:
      args: Command line arguments
    """
    import optparse

    parser = optparse.OptionParser()
    parser.add_option(
        "-c",
        "--swift_config",
        dest="swift_config",
        default="",
        help="Path to the configuration file for Swift backend.",
    )
    options, args = parser.parse_args(args)

    conf = load_conf(options.swift_config)
    if args == []:
        parser.error("missing repository name")
    repo = args[0]
    scon = SwiftConnector(repo, conf)
    SwiftRepo.init_bare(scon, conf)


def main(argv: list[str] = sys.argv) -> None:
    """Main entry point for Swift Git command line interface.

    Args:
      argv: Command line arguments
    """
    commands = {
        "init": cmd_init,
        "daemon": cmd_daemon,
    }

    if len(argv) < 2:
        print("Usage: {} <{}> [OPTIONS...]".format(argv[0], "|".join(commands.keys())))
        sys.exit(1)

    cmd = argv[1]
    if cmd not in commands:
        print(f"No such subcommand: {cmd}")
        sys.exit(1)
    commands[cmd](argv[2:])


if __name__ == "__main__":
    main()
