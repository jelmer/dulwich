# client.py -- Implementation of the client side git protocols
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Client side support for the Git protocol.

The Dulwich client supports the following capabilities:

 * thin-pack
 * multi_ack_detailed
 * multi_ack
 * side-band-64k
 * ofs-delta
 * quiet
 * report-status
 * delete-refs
 * shallow

Known capabilities that are not supported:

 * no-progress
 * include-tag
"""

import copy
import functools
import logging
import os
import select
import socket
import subprocess
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence, Set
from contextlib import closing
from io import BufferedReader, BytesIO
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Union,
)
from urllib.parse import ParseResult, urljoin, urlparse, urlunparse, urlunsplit
from urllib.parse import quote as urlquote

if TYPE_CHECKING:
    import urllib3

import dulwich

if TYPE_CHECKING:
    from typing import Protocol as TypingProtocol

    from .pack import UnpackedObject

    class HTTPResponse(TypingProtocol):
        """Protocol for HTTP response objects."""

        redirect_location: Optional[str]
        content_type: Optional[str]

        def close(self) -> None:
            """Close the response."""
            ...

    class GeneratePackDataFunc(TypingProtocol):
        """Protocol for generate_pack_data functions."""

        def __call__(
            self,
            have: Set[bytes],
            want: Set[bytes],
            *,
            ofs_delta: bool = False,
            progress: Optional[Callable[[bytes], None]] = None,
        ) -> tuple[int, Iterator[UnpackedObject]]:
            """Generate pack data for the given have and want sets."""
            ...

    class DetermineWantsFunc(TypingProtocol):
        """Protocol for determine_wants functions."""

        def __call__(
            self,
            refs: Mapping[bytes, bytes],
            depth: Optional[int] = None,
        ) -> list[bytes]:
            """Determine the objects to fetch from the given refs."""
            ...


from .bundle import Bundle
from .config import Config, apply_instead_of, get_xdg_config_home_path
from .credentials import match_partial_url, match_urls
from .errors import GitProtocolError, HangupException, NotGitRepository, SendPackError
from .object_store import GraphWalker
from .pack import (
    PACK_SPOOL_FILE_MAX_SIZE,
    PackChunkGenerator,
    PackData,
    write_pack_from_container,
)
from .protocol import (
    _RBUFSIZE,
    CAPABILITIES_REF,
    CAPABILITY_AGENT,
    CAPABILITY_DELETE_REFS,
    CAPABILITY_FETCH,
    CAPABILITY_FILTER,
    CAPABILITY_INCLUDE_TAG,
    CAPABILITY_MULTI_ACK,
    CAPABILITY_MULTI_ACK_DETAILED,
    CAPABILITY_OFS_DELTA,
    CAPABILITY_QUIET,
    CAPABILITY_REPORT_STATUS,
    CAPABILITY_SHALLOW,
    CAPABILITY_SIDE_BAND_64K,
    CAPABILITY_SYMREF,
    CAPABILITY_THIN_PACK,
    COMMAND_DEEPEN,
    COMMAND_DEEPEN_NOT,
    COMMAND_DEEPEN_SINCE,
    COMMAND_DONE,
    COMMAND_HAVE,
    COMMAND_SHALLOW,
    COMMAND_UNSHALLOW,
    COMMAND_WANT,
    DEFAULT_GIT_PROTOCOL_VERSION_FETCH,
    DEFAULT_GIT_PROTOCOL_VERSION_SEND,
    GIT_PROTOCOL_VERSIONS,
    KNOWN_RECEIVE_CAPABILITIES,
    KNOWN_UPLOAD_CAPABILITIES,
    SIDE_BAND_CHANNEL_DATA,
    SIDE_BAND_CHANNEL_FATAL,
    SIDE_BAND_CHANNEL_PROGRESS,
    TCP_GIT_PORT,
    ZERO_SHA,
    PktLineParser,
    Protocol,
    agent_string,
    capability_agent,
    extract_capabilities,
    extract_capability_names,
    parse_capability,
    pkt_line,
    pkt_seq,
)
from .refs import (
    PEELED_TAG_SUFFIX,
    SYMREF,
    Ref,
    _import_remote_refs,
    _set_default_branch,
    _set_head,
    _set_origin_head,
    filter_ref_prefix,
    read_info_refs,
    split_peeled_refs,
)
from .repo import BaseRepo, Repo

# Default ref prefix, used if none is specified.
# GitHub defaults to just sending HEAD if no ref-prefix is
# specified, so explicitly request all refs to match
# behaviour with v1 when no ref-prefix is specified.
DEFAULT_REF_PREFIX = [b"HEAD", b"refs/"]

ObjectID = bytes


logger = logging.getLogger(__name__)


class InvalidWants(Exception):
    """Invalid wants."""

    def __init__(self, wants: Set[bytes]) -> None:
        """Initialize InvalidWants exception.

        Args:
            wants: List of invalid wants
        """
        Exception.__init__(
            self, f"requested wants not in server provided refs: {wants!r}"
        )


class HTTPUnauthorized(Exception):
    """Raised when authentication fails."""

    def __init__(self, www_authenticate: Optional[str], url: str) -> None:
        """Initialize HTTPUnauthorized exception.

        Args:
            www_authenticate: WWW-Authenticate header value
            url: URL that requires authentication
        """
        Exception.__init__(self, "No valid credentials provided")
        self.www_authenticate = www_authenticate
        self.url = url


def _to_optional_dict(refs: Mapping[bytes, bytes]) -> dict[bytes, Optional[bytes]]:
    """Convert a dict[bytes, bytes] to dict[bytes, Optional[bytes]].

    This is needed for compatibility with result types that expect Optional values.
    """
    return {k: v for k, v in refs.items()}


class HTTPProxyUnauthorized(Exception):
    """Raised when proxy authentication fails."""

    def __init__(self, proxy_authenticate: Optional[str], url: str) -> None:
        """Initialize HTTPProxyUnauthorized exception.

        Args:
            proxy_authenticate: Proxy-Authenticate header value
            url: URL that requires proxy authentication
        """
        Exception.__init__(self, "No valid proxy credentials provided")
        self.proxy_authenticate = proxy_authenticate
        self.url = url


def _fileno_can_read(fileno: int) -> bool:
    """Check if a file descriptor is readable."""
    return len(select.select([fileno], [], [], 0)[0]) > 0


def _win32_peek_avail(handle: int) -> int:
    """Wrapper around PeekNamedPipe to check how many bytes are available."""
    from ctypes import (  # type: ignore[attr-defined,unused-ignore]
        byref,
        windll,
        wintypes,
    )

    c_avail = wintypes.DWORD()
    c_message = wintypes.DWORD()
    success = windll.kernel32.PeekNamedPipe(
        handle, None, 0, None, byref(c_avail), byref(c_message)
    )
    if not success:
        from ctypes import GetLastError  # type: ignore[attr-defined,unused-ignore]

        raise OSError(GetLastError())
    return c_avail.value


COMMON_CAPABILITIES = [CAPABILITY_OFS_DELTA, CAPABILITY_SIDE_BAND_64K]
UPLOAD_CAPABILITIES = [
    CAPABILITY_THIN_PACK,
    CAPABILITY_MULTI_ACK,
    CAPABILITY_MULTI_ACK_DETAILED,
    CAPABILITY_SHALLOW,
    *COMMON_CAPABILITIES,
]
RECEIVE_CAPABILITIES = [
    CAPABILITY_REPORT_STATUS,
    CAPABILITY_DELETE_REFS,
    *COMMON_CAPABILITIES,
]


class ReportStatusParser:
    """Handle status as reported by servers with 'report-status' capability."""

    def __init__(self) -> None:
        """Initialize ReportStatusParser."""
        self._done = False
        self._pack_status: Optional[bytes] = None
        self._ref_statuses: list[bytes] = []

    def check(self) -> Iterator[tuple[bytes, Optional[str]]]:
        """Check if there were any errors and, if so, raise exceptions.

        Raises:
          SendPackError: Raised when the server could not unpack
        Returns:
          iterator over refs
        """
        if self._pack_status not in (b"unpack ok", None):
            raise SendPackError(self._pack_status)
        for status in self._ref_statuses:
            try:
                status, rest = status.split(b" ", 1)
            except ValueError:
                # malformed response, move on to the next one
                continue
            if status == b"ng":
                ref, error = rest.split(b" ", 1)
                yield ref, error.decode("utf-8")
            elif status == b"ok":
                yield rest, None
            else:
                raise GitProtocolError(f"invalid ref status {status!r}")

    def handle_packet(self, pkt: Optional[bytes]) -> None:
        """Handle a packet.

        Raises:
          GitProtocolError: Raised when packets are received after a flush
          packet.
        """
        if self._done:
            raise GitProtocolError("received more data after status report")
        if pkt is None:
            self._done = True
            return
        if self._pack_status is None:
            self._pack_status = pkt.strip()
        else:
            ref_status = pkt.strip()
            self._ref_statuses.append(ref_status)


def negotiate_protocol_version(proto: Protocol) -> int:
    """Negotiate protocol version with the server."""
    pkt = proto.read_pkt_line()
    if pkt is not None and pkt.strip() == b"version 2":
        return 2
    proto.unread_pkt_line(pkt)
    return 0


def read_server_capabilities(pkt_seq: Iterable[bytes]) -> set[bytes]:
    """Read server capabilities from packet sequence."""
    server_capabilities = []
    for pkt in pkt_seq:
        server_capabilities.append(pkt)
    return set(server_capabilities)


def read_pkt_refs_v2(
    pkt_seq: Iterable[bytes],
) -> tuple[dict[bytes, Optional[bytes]], dict[bytes, bytes], dict[bytes, bytes]]:
    """Read references using protocol version 2."""
    refs: dict[bytes, Optional[bytes]] = {}
    symrefs = {}
    peeled = {}
    # Receive refs from server
    for pkt in pkt_seq:
        parts = pkt.rstrip(b"\n").split(b" ")
        sha: Optional[bytes] = parts[0]
        if sha == b"unborn":
            sha = None
        ref = parts[1]
        for part in parts[2:]:
            if part.startswith(b"peeled:"):
                peeled[ref] = part[7:]
            elif part.startswith(b"symref-target:"):
                symrefs[ref] = part[14:]
            else:
                logging.warning("unknown part in pkt-ref: %s", part)
        refs[ref] = sha

    return refs, symrefs, peeled


def read_pkt_refs_v1(
    pkt_seq: Iterable[bytes],
) -> tuple[dict[bytes, bytes], set[bytes]]:
    """Read references using protocol version 1."""
    server_capabilities = None
    refs: dict[bytes, bytes] = {}
    # Receive refs from server
    for pkt in pkt_seq:
        (sha, ref) = pkt.rstrip(b"\n").split(None, 1)
        if sha == b"ERR":
            raise GitProtocolError(ref.decode("utf-8", "replace"))
        if server_capabilities is None:
            (ref, server_capabilities) = extract_capabilities(ref)
        refs[ref] = sha

    if len(refs) == 0:
        return {}, set()
    if refs == {CAPABILITIES_REF: ZERO_SHA}:
        refs = {}
    assert server_capabilities is not None
    return refs, set(server_capabilities)


class _DeprecatedDictProxy:
    """Base class for result objects that provide deprecated dict-like interface."""

    refs: dict[bytes, Optional[bytes]]  # To be overridden by subclasses

    _FORWARDED_ATTRS: ClassVar[set[str]] = {
        "clear",
        "copy",
        "fromkeys",
        "get",
        "items",
        "keys",
        "pop",
        "popitem",
        "setdefault",
        "update",
        "values",
        "viewitems",
        "viewkeys",
        "viewvalues",
    }

    def _warn_deprecated(self) -> None:
        import warnings

        warnings.warn(
            f"Use {self.__class__.__name__}.refs instead.",
            DeprecationWarning,
            stacklevel=3,
        )

    def __contains__(self, name: bytes) -> bool:
        self._warn_deprecated()
        return name in self.refs

    def __getitem__(self, name: bytes) -> Optional[bytes]:
        self._warn_deprecated()
        return self.refs[name]

    def __len__(self) -> int:
        self._warn_deprecated()
        return len(self.refs)

    def __iter__(self) -> Iterator[bytes]:
        self._warn_deprecated()
        return iter(self.refs)

    def __getattribute__(self, name: str) -> object:
        # Avoid infinite recursion by checking against class variable directly
        if name != "_FORWARDED_ATTRS" and name in type(self)._FORWARDED_ATTRS:
            self._warn_deprecated()
            # Direct attribute access to avoid recursion
            refs = object.__getattribute__(self, "refs")
            return getattr(refs, name)
        return super().__getattribute__(name)


class FetchPackResult(_DeprecatedDictProxy):
    """Result of a fetch-pack operation.

    Attributes:
      refs: Dictionary with all remote refs
      symrefs: Dictionary with remote symrefs
      agent: User agent string
    """

    symrefs: dict[bytes, bytes]
    agent: Optional[bytes]

    def __init__(
        self,
        refs: dict[bytes, Optional[bytes]],
        symrefs: dict[bytes, bytes],
        agent: Optional[bytes],
        new_shallow: Optional[set[bytes]] = None,
        new_unshallow: Optional[set[bytes]] = None,
    ) -> None:
        """Initialize FetchPackResult.

        Args:
            refs: Dictionary with all remote refs
            symrefs: Dictionary with remote symrefs
            agent: User agent string
            new_shallow: New shallow commits
            new_unshallow: New unshallow commits
        """
        self.refs = refs
        self.symrefs = symrefs
        self.agent = agent
        self.new_shallow = new_shallow
        self.new_unshallow = new_unshallow

    def __eq__(self, other: object) -> bool:
        """Check equality with another object."""
        if isinstance(other, dict):
            self._warn_deprecated()
            return self.refs == other
        if not isinstance(other, FetchPackResult):
            return False
        return (
            self.refs == other.refs
            and self.symrefs == other.symrefs
            and self.agent == other.agent
        )

    def __repr__(self) -> str:
        """Return string representation of FetchPackResult."""
        return f"{self.__class__.__name__}({self.refs!r}, {self.symrefs!r}, {self.agent!r})"


class LsRemoteResult(_DeprecatedDictProxy):
    """Result of a ls-remote operation.

    Attributes:
      refs: Dictionary with all remote refs
      symrefs: Dictionary with remote symrefs
    """

    symrefs: dict[bytes, bytes]

    def __init__(
        self, refs: dict[bytes, Optional[bytes]], symrefs: dict[bytes, bytes]
    ) -> None:
        """Initialize LsRemoteResult.

        Args:
            refs: Dictionary with all remote refs
            symrefs: Dictionary with remote symrefs
        """
        self.refs = refs
        self.symrefs = symrefs

    def _warn_deprecated(self) -> None:
        import warnings

        warnings.warn(
            "Treating LsRemoteResult as a dictionary is deprecated. "
            "Use result.refs instead.",
            DeprecationWarning,
            stacklevel=3,
        )

    def __eq__(self, other: object) -> bool:
        """Check equality with another object."""
        if isinstance(other, dict):
            self._warn_deprecated()
            return self.refs == other
        if not isinstance(other, LsRemoteResult):
            return False
        return self.refs == other.refs and self.symrefs == other.symrefs

    def __repr__(self) -> str:
        """Return string representation of LsRemoteResult."""
        return f"{self.__class__.__name__}({self.refs!r}, {self.symrefs!r})"


class SendPackResult(_DeprecatedDictProxy):
    """Result of a upload-pack operation.

    Attributes:
      refs: Dictionary with all remote refs
      agent: User agent string
      ref_status: Optional dictionary mapping ref name to error message (if it
        failed to update), or None if it was updated successfully
    """

    def __init__(
        self,
        refs: dict[bytes, Optional[bytes]],
        agent: Optional[bytes] = None,
        ref_status: Optional[dict[bytes, Optional[str]]] = None,
    ) -> None:
        """Initialize SendPackResult.

        Args:
            refs: Dictionary with all remote refs
            agent: User agent string
            ref_status: Optional dictionary mapping ref name to error message
        """
        self.refs = refs
        self.agent = agent
        self.ref_status = ref_status

    def __eq__(self, other: object) -> bool:
        """Check equality with another object."""
        if isinstance(other, dict):
            self._warn_deprecated()
            return self.refs == other
        if not isinstance(other, SendPackResult):
            return False
        return self.refs == other.refs and self.agent == other.agent

    def __repr__(self) -> str:
        """Return string representation of SendPackResult."""
        return f"{self.__class__.__name__}({self.refs!r}, {self.agent!r})"


def _read_shallow_updates(pkt_seq: Iterable[bytes]) -> tuple[set[bytes], set[bytes]]:
    new_shallow = set()
    new_unshallow = set()
    for pkt in pkt_seq:
        if pkt == b"shallow-info\n":  # Git-protocol v2
            continue
        try:
            cmd, sha = pkt.split(b" ", 1)
        except ValueError:
            raise GitProtocolError(f"unknown command {pkt!r}")
        if cmd == COMMAND_SHALLOW:
            new_shallow.add(sha.strip())
        elif cmd == COMMAND_UNSHALLOW:
            new_unshallow.add(sha.strip())
        else:
            raise GitProtocolError(f"unknown command {pkt!r}")
    return (new_shallow, new_unshallow)


class _v1ReceivePackHeader:
    def __init__(
        self,
        capabilities: Sequence[bytes],
        old_refs: Mapping[bytes, bytes],
        new_refs: Mapping[bytes, bytes],
    ) -> None:
        self.want: set[bytes] = set()
        self.have: set[bytes] = set()
        self._it = self._handle_receive_pack_head(capabilities, old_refs, new_refs)
        self.sent_capabilities = False

    def __iter__(self) -> Iterator[Optional[bytes]]:
        return self._it

    def _handle_receive_pack_head(
        self,
        capabilities: Sequence[bytes],
        old_refs: Mapping[bytes, bytes],
        new_refs: Mapping[bytes, bytes],
    ) -> Iterator[Optional[bytes]]:
        """Handle the head of a 'git-receive-pack' request.

        Args:
          capabilities: List of negotiated capabilities
          old_refs: Old refs, as received from the server
          new_refs: Refs to change

        Returns:
          (have, want) tuple
        """
        self.have = {x for x in old_refs.values() if not x == ZERO_SHA}

        for refname in new_refs:
            if not isinstance(refname, bytes):
                raise TypeError(f"refname is not a bytestring: {refname!r}")
            old_sha1 = old_refs.get(refname, ZERO_SHA)
            if not isinstance(old_sha1, bytes):
                raise TypeError(
                    f"old sha1 for {refname!r} is not a bytestring: {old_sha1!r}"
                )
            new_sha1 = new_refs.get(refname, ZERO_SHA)
            if not isinstance(new_sha1, bytes):
                raise TypeError(
                    f"old sha1 for {refname!r} is not a bytestring {new_sha1!r}"
                )

            if old_sha1 != new_sha1:
                logger.debug(
                    "Sending updated ref %r: %r -> %r", refname, old_sha1, new_sha1
                )
                if self.sent_capabilities:
                    yield old_sha1 + b" " + new_sha1 + b" " + refname
                else:
                    yield (
                        old_sha1
                        + b" "
                        + new_sha1
                        + b" "
                        + refname
                        + b"\0"
                        + b" ".join(sorted(capabilities))
                    )
                    self.sent_capabilities = True
            if new_sha1 not in self.have and new_sha1 != ZERO_SHA:
                self.want.add(new_sha1)
        yield None


def _read_side_band64k_data(pkt_seq: Iterable[bytes]) -> Iterator[tuple[int, bytes]]:
    """Read per-channel data.

    This requires the side-band-64k capability.

    Args:
      pkt_seq: Sequence of packets to read
    """
    for pkt in pkt_seq:
        channel = ord(pkt[:1])
        yield channel, pkt[1:]


def find_capability(
    capabilities: Iterable[bytes], key: bytes, value: Optional[bytes]
) -> Optional[bytes]:
    """Find a capability with a specific key and value."""
    for capability in capabilities:
        k, v = parse_capability(capability)
        if k != key:
            continue
        if value and v and value not in v.split(b" "):
            continue
        return capability
    return None


def _handle_upload_pack_head(
    proto: Protocol,
    capabilities: Iterable[bytes],
    graph_walker: GraphWalker,
    wants: list[bytes],
    can_read: Optional[Callable[[], bool]],
    depth: Optional[int],
    protocol_version: Optional[int],
    shallow_since: Optional[str] = None,
    shallow_exclude: Optional[list[str]] = None,
) -> tuple[Optional[set[bytes]], Optional[set[bytes]]]:
    """Handle the head of a 'git-upload-pack' request.

    Args:
      proto: Protocol object to read from
      capabilities: List of negotiated capabilities
      graph_walker: GraphWalker instance to call .ack() on
      wants: List of commits to fetch
      can_read: function that returns a boolean that indicates
    whether there is extra graph data to read on proto
      depth: Depth for request
      protocol_version: Neogiated Git protocol version.
      shallow_since: Deepen the history to include commits after this date
      shallow_exclude: Deepen the history to exclude commits reachable from these refs
    """
    new_shallow: Optional[set[bytes]]
    new_unshallow: Optional[set[bytes]]
    assert isinstance(wants, list) and isinstance(wants[0], bytes)
    wantcmd = COMMAND_WANT + b" " + wants[0]
    if protocol_version is None:
        protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_SEND
    if protocol_version != 2:
        wantcmd += b" " + b" ".join(sorted(capabilities))
    wantcmd += b"\n"
    proto.write_pkt_line(wantcmd)
    for want in wants[1:]:
        proto.write_pkt_line(COMMAND_WANT + b" " + want + b"\n")
    if (
        depth not in (0, None)
        or shallow_since is not None
        or shallow_exclude
        or (hasattr(graph_walker, "shallow") and graph_walker.shallow)
    ):
        if protocol_version == 2:
            if not find_capability(capabilities, CAPABILITY_FETCH, CAPABILITY_SHALLOW):
                raise GitProtocolError(
                    "server does not support shallow capability required for depth"
                )
        elif CAPABILITY_SHALLOW not in capabilities:
            raise GitProtocolError(
                "server does not support shallow capability required for depth"
            )
        if hasattr(graph_walker, "shallow"):
            for sha in graph_walker.shallow:
                proto.write_pkt_line(COMMAND_SHALLOW + b" " + sha + b"\n")
        if depth is not None:
            proto.write_pkt_line(
                COMMAND_DEEPEN + b" " + str(depth).encode("ascii") + b"\n"
            )
        if shallow_since is not None:
            proto.write_pkt_line(
                COMMAND_DEEPEN_SINCE + b" " + shallow_since.encode("ascii") + b"\n"
            )
        if shallow_exclude:
            for ref in shallow_exclude:
                proto.write_pkt_line(
                    COMMAND_DEEPEN_NOT + b" " + ref.encode("ascii") + b"\n"
                )
    if protocol_version != 2:
        proto.write_pkt_line(None)

    have = next(graph_walker)
    while have:
        proto.write_pkt_line(COMMAND_HAVE + b" " + have + b"\n")
        if can_read is not None and can_read():
            pkt = proto.read_pkt_line()
            assert pkt is not None
            parts = pkt.rstrip(b"\n").split(b" ")
            if parts[0] == b"ACK":
                graph_walker.ack(parts[1])
                if parts[2] in (b"continue", b"common"):
                    pass
                elif parts[2] == b"ready":
                    break
                else:
                    raise AssertionError(
                        f"{parts[2]!r} not in ('continue', 'ready', 'common)"
                    )
        have = next(graph_walker)
    proto.write_pkt_line(COMMAND_DONE + b"\n")
    if protocol_version == 2:
        proto.write_pkt_line(None)

    if depth not in (0, None) or shallow_since is not None or shallow_exclude:
        if can_read is not None:
            (new_shallow, new_unshallow) = _read_shallow_updates(proto.read_pkt_seq())
        else:
            new_shallow = None
            new_unshallow = None
    else:
        new_shallow = new_unshallow = set()

    return (new_shallow, new_unshallow)


def _handle_upload_pack_tail(
    proto: "Protocol",
    capabilities: Set[bytes],
    graph_walker: "GraphWalker",
    pack_data: Callable[[bytes], int],
    progress: Optional[Callable[[bytes], None]] = None,
    rbufsize: int = _RBUFSIZE,
    protocol_version: int = 0,
) -> None:
    """Handle the tail of a 'git-upload-pack' request.

    Args:
      proto: Protocol object to read from
      capabilities: List of negotiated capabilities
      graph_walker: GraphWalker instance to call .ack() on
      pack_data: Function to call with pack data
      progress: Optional progress reporting function
      rbufsize: Read buffer size
      protocol_version: Neogiated Git protocol version.
    """
    pkt = proto.read_pkt_line()
    while pkt:
        parts = pkt.rstrip(b"\n").split(b" ")
        if protocol_version == 2 and parts[0] != b"packfile":
            break
        else:
            if parts[0] == b"ACK":
                graph_walker.ack(parts[1])
            if parts[0] == b"NAK":
                graph_walker.nak()
            if len(parts) < 3 or parts[2] not in (
                b"ready",
                b"continue",
                b"common",
            ):
                break
        pkt = proto.read_pkt_line()
    if CAPABILITY_SIDE_BAND_64K in capabilities or protocol_version == 2:
        if progress is None:
            # Just ignore progress data

            def progress(x: bytes) -> None:
                pass

        for chan, data in _read_side_band64k_data(proto.read_pkt_seq()):
            if chan == SIDE_BAND_CHANNEL_DATA:
                pack_data(data)
            elif chan == SIDE_BAND_CHANNEL_PROGRESS:
                progress(data)
            else:
                raise AssertionError(f"Invalid sideband channel {chan}")
    else:
        while True:
            data = proto.read(rbufsize)
            if data == b"":
                break
            pack_data(data)


def _extract_symrefs_and_agent(
    capabilities: Iterable[bytes],
) -> tuple[dict[bytes, bytes], Optional[bytes]]:
    """Extract symrefs and agent from capabilities.

    Args:
     capabilities: List of capabilities
    Returns:
     (symrefs, agent) tuple
    """
    symrefs = {}
    agent = None
    for capability in capabilities:
        k, v = parse_capability(capability)
        if k == CAPABILITY_SYMREF:
            assert v is not None
            (src, dst) = v.split(b":", 1)
            symrefs[src] = dst
        if k == CAPABILITY_AGENT:
            agent = v
    return (symrefs, agent)


# TODO(durin42): this doesn't correctly degrade if the server doesn't
# support some capabilities. This should work properly with servers
# that don't support multi_ack.
class GitClient:
    """Git smart server client."""

    def __init__(
        self,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Create a new GitClient instance.

        Args:
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport
            activity.
          quiet: Whether to suppress output
          include_tags: send annotated tags when sending the objects they point
            to
        """
        self._report_activity = report_activity
        self._report_status_parser: Optional[ReportStatusParser] = None
        self._fetch_capabilities = set(UPLOAD_CAPABILITIES)
        self._fetch_capabilities.add(capability_agent())
        self._send_capabilities = set(RECEIVE_CAPABILITIES)
        self._send_capabilities.add(capability_agent())
        if quiet:
            self._send_capabilities.add(CAPABILITY_QUIET)
        if not thin_packs:
            self._fetch_capabilities.remove(CAPABILITY_THIN_PACK)
        if include_tags:
            self._fetch_capabilities.add(CAPABILITY_INCLUDE_TAG)
        self.protocol_version = 0  # will be overridden later

    def get_url(self, path: str) -> str:
        """Retrieves full url to given path.

        Args:
          path: Repository path (as string)

        Returns:
          Url to path (as string)

        """
        raise NotImplementedError(self.get_url)

    @classmethod
    def from_parsedurl(
        cls,
        parsedurl: ParseResult,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        dumb: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "GitClient":
        """Create an instance of this client from a urlparse.parsed object.

        Args:
          parsedurl: Result of urlparse()
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
          dumb: Whether to use dumb HTTP transport (only for HTTP)
          username: Optional username for authentication (only for HTTP)
          password: Optional password for authentication (only for HTTP)
          config: Optional configuration object

        Returns:
          A `GitClient` object
        """
        raise NotImplementedError(cls.from_parsedurl)

    def send_pack(
        self,
        path: bytes,
        update_refs: Callable[[dict[bytes, bytes]], dict[bytes, bytes]],
        generate_pack_data: "GeneratePackDataFunc",
        progress: Optional[Callable[[bytes], None]] = None,
    ) -> SendPackResult:
        """Upload a pack to a remote repository.

        Args:
          path: Repository path (as bytestring)
          update_refs: Function to determine changes to remote refs. Receive
            dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
          generate_pack_data: Function that can return a tuple
            with number of objects and list of pack data to include
          progress: Optional progress function

        Returns:
          SendPackResult object

        Raises:
          SendPackError: if server rejects the pack data

        """
        raise NotImplementedError(self.send_pack)

    def clone(
        self,
        path: str,
        target_path: str,
        mkdir: bool = True,
        bare: bool = False,
        origin: Optional[str] = "origin",
        checkout: Optional[bool] = None,
        branch: Optional[str] = None,
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
    ) -> Repo:
        """Clone a repository."""
        if mkdir:
            os.mkdir(target_path)

        try:
            target = None
            if not bare:
                target = Repo.init(target_path)
                if checkout is None:
                    checkout = True
            else:
                if checkout:
                    raise ValueError("checkout and bare are incompatible")
                target = Repo.init_bare(target_path)

            # TODO(jelmer): abstract method for get_location?
            if isinstance(self, (LocalGitClient, SubprocessGitClient)):
                encoded_path = path.encode("utf-8")
            else:
                encoded_path = self.get_url(path).encode("utf-8")

            assert target is not None
            if origin is not None:
                target_config = target.get_config()
                target_config.set(
                    (b"remote", origin.encode("utf-8")), b"url", encoded_path
                )
                target_config.set(
                    (b"remote", origin.encode("utf-8")),
                    b"fetch",
                    b"+refs/heads/*:refs/remotes/" + origin.encode("utf-8") + b"/*",
                )
                target_config.write_to_path()

            ref_message = b"clone: from " + encoded_path
            result = self.fetch(
                path.encode("utf-8"),
                target,
                progress=progress,
                depth=depth,
                ref_prefix=ref_prefix,
                filter_spec=filter_spec,
                protocol_version=protocol_version,
            )
            if origin is not None:
                _import_remote_refs(
                    target.refs, origin, result.refs, message=ref_message
                )

            origin_head = result.symrefs.get(b"HEAD")
            origin_sha = result.refs.get(b"HEAD")
            if origin is None or (origin_sha and not origin_head):
                # set detached HEAD
                if origin_sha is not None:
                    target.refs[b"HEAD"] = origin_sha
                    head = origin_sha
                else:
                    head = None
            else:
                _set_origin_head(target.refs, origin.encode("utf-8"), origin_head)
                head_ref = _set_default_branch(
                    target.refs,
                    origin.encode("utf-8"),
                    origin_head,
                    branch.encode("utf-8") if branch is not None else None,
                    ref_message,
                )

                # Update target head
                if head_ref:
                    head = _set_head(target.refs, head_ref, ref_message)
                else:
                    head = None

            if checkout and head is not None:
                target.get_worktree().reset_index()
        except BaseException:
            if target is not None:
                target.close()
            if mkdir:
                import shutil

                shutil.rmtree(target_path)
            raise
        return target

    def fetch(
        self,
        path: bytes,
        target: BaseRepo,
        determine_wants: Optional["DetermineWantsFunc"] = None,
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Fetch into a target repository.

        Args:
          path: Path to fetch from (as bytestring)
          target: Target repository to fetch into
          determine_wants: Optional function to determine what refs to fetch.
            Receives dictionary of name->sha, should return
            list of shas to fetch. Defaults to all shas.
          progress: Optional progress function
          depth: Depth to fetch at
          ref_prefix: List of prefixes of desired references, as a list of
            bytestrings. Filtering is done by the server if supported, and
            client side otherwise.
          filter_spec: A git-rev-list-style object filter spec, as bytestring.
            Only used if the server supports the Git protocol-v2 'filter'
            feature, and ignored otherwise.
          protocol_version: Desired Git protocol version. By default the highest
            mutually supported protocol version will be used.
          shallow_since: Deepen the history to include commits after this date
          shallow_exclude: Deepen the history to exclude commits reachable from these refs

        Returns:
          Dictionary with all remote refs (not just those fetched)

        """
        if determine_wants is None:
            determine_wants = target.object_store.determine_wants_all
        if CAPABILITY_THIN_PACK in self._fetch_capabilities:
            from tempfile import SpooledTemporaryFile

            f: IO[bytes] = SpooledTemporaryFile(
                max_size=PACK_SPOOL_FILE_MAX_SIZE,
                prefix="incoming-",
                dir=getattr(target.object_store, "path", None),
            )

            def commit() -> None:
                if f.tell():
                    f.seek(0)
                    target.object_store.add_thin_pack(f.read, None, progress=progress)  # type: ignore
                f.close()

            def abort() -> None:
                f.close()

        else:
            f, commit, abort = target.object_store.add_pack()
        try:
            result = self.fetch_pack(
                path,
                determine_wants,
                target.get_graph_walker(),
                f.write,
                progress=progress,
                depth=depth,
                ref_prefix=ref_prefix,
                filter_spec=filter_spec,
                protocol_version=protocol_version,
                shallow_since=shallow_since,
                shallow_exclude=shallow_exclude,
            )
        except BaseException:
            abort()
            raise
        else:
            commit()
        target.update_shallow(result.new_shallow, result.new_unshallow)
        return result

    def fetch_pack(
        self,
        path: bytes,
        determine_wants: "DetermineWantsFunc",
        graph_walker: GraphWalker,
        pack_data: Callable[[bytes], int],
        *,
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Retrieve a pack from a git smart server.

        Args:
          path: Remote path to fetch from
          determine_wants: Function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch.
          graph_walker: Object with next() and ack().
          pack_data: Callback called for each bit of data in the pack
          progress: Callback for progress reports (strings)
          depth: Shallow fetch depth
          ref_prefix: List of prefixes of desired references, as a list of
            bytestrings. Filtering is done by the server if supported, and
            client side otherwise.
          filter_spec: A git-rev-list-style object filter spec, as bytestring.
            Only used if the server supports the Git protocol-v2 'filter'
            feature, and ignored otherwise.
          protocol_version: Desired Git protocol version. By default the highest
            mutually supported protocol version will be used.
          shallow_since: Deepen the history to include commits after this date
          shallow_exclude: Deepen the history to exclude commits reachable from these refs

        Returns:
          FetchPackResult object

        """
        raise NotImplementedError(self.fetch_pack)

    def get_refs(
        self,
        path: bytes,
        protocol_version: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
    ) -> LsRemoteResult:
        """Retrieve the current refs from a git smart server.

        Args:
          path: Path to the repo to fetch from. (as bytestring)
          protocol_version: Desired Git protocol version.
          ref_prefix: Prefix filter for refs.

        Returns:
          LsRemoteResult object with refs and symrefs
        """
        raise NotImplementedError(self.get_refs)

    @staticmethod
    def _should_send_pack(new_refs: Mapping[bytes, bytes]) -> bool:
        # The packfile MUST NOT be sent if the only command used is delete.
        return any(sha != ZERO_SHA for sha in new_refs.values())

    def _negotiate_receive_pack_capabilities(
        self, server_capabilities: set[bytes]
    ) -> tuple[set[bytes], Optional[bytes]]:
        negotiated_capabilities = self._send_capabilities & server_capabilities
        (_symrefs, agent) = _extract_symrefs_and_agent(server_capabilities)
        (extract_capability_names(server_capabilities) - KNOWN_RECEIVE_CAPABILITIES)
        # TODO(jelmer): warn about unknown capabilities
        return (negotiated_capabilities, agent)

    def _handle_receive_pack_tail(
        self,
        proto: Protocol,
        capabilities: Set[bytes],
        progress: Optional[Callable[[bytes], None]] = None,
    ) -> Optional[dict[bytes, Optional[str]]]:
        """Handle the tail of a 'git-receive-pack' request.

        Args:
          proto: Protocol object to read from
          capabilities: List of negotiated capabilities
          progress: Optional progress reporting function

        Returns:
          dict mapping ref name to:
            error message if the ref failed to update
            None if it was updated successfully
        """
        if CAPABILITY_SIDE_BAND_64K in capabilities or self.protocol_version == 2:
            if progress is None:

                def progress(x: bytes) -> None:
                    pass

            if CAPABILITY_REPORT_STATUS in capabilities:
                assert self._report_status_parser is not None
                pktline_parser = PktLineParser(self._report_status_parser.handle_packet)
            for chan, data in _read_side_band64k_data(proto.read_pkt_seq()):
                if chan == SIDE_BAND_CHANNEL_DATA:
                    if CAPABILITY_REPORT_STATUS in capabilities:
                        pktline_parser.parse(data)
                elif chan == SIDE_BAND_CHANNEL_PROGRESS:
                    progress(data)
                else:
                    raise AssertionError(f"Invalid sideband channel {chan}")
        else:
            if CAPABILITY_REPORT_STATUS in capabilities:
                assert self._report_status_parser
                for pkt in proto.read_pkt_seq():
                    self._report_status_parser.handle_packet(pkt)
        if self._report_status_parser is not None:
            return dict(self._report_status_parser.check())

        return None

    def _negotiate_upload_pack_capabilities(
        self, server_capabilities: set[bytes]
    ) -> tuple[set[bytes], dict[bytes, bytes], Optional[bytes]]:
        (extract_capability_names(server_capabilities) - KNOWN_UPLOAD_CAPABILITIES)
        # TODO(jelmer): warn about unknown capabilities
        fetch_capa = None
        for capability in server_capabilities:
            k, v = parse_capability(capability)
            if self.protocol_version == 2 and k == CAPABILITY_FETCH:
                fetch_capa = CAPABILITY_FETCH
                fetch_features = []
                assert v is not None
                v_list = v.strip().split(b" ")
                if b"shallow" in v_list:
                    fetch_features.append(CAPABILITY_SHALLOW)
                if b"filter" in v_list:
                    fetch_features.append(CAPABILITY_FILTER)
                for i in range(len(fetch_features)):
                    if i == 0:
                        fetch_capa += b"="
                    else:
                        fetch_capa += b" "
                    fetch_capa += fetch_features[i]

        (symrefs, agent) = _extract_symrefs_and_agent(server_capabilities)

        negotiated_capabilities = self._fetch_capabilities & server_capabilities
        if fetch_capa:
            negotiated_capabilities.add(fetch_capa)
        return (negotiated_capabilities, symrefs, agent)

    def archive(
        self,
        path: bytes,
        committish: bytes,
        write_data: Callable[[bytes], None],
        progress: Optional[Callable[[bytes], None]] = None,
        write_error: Optional[Callable[[bytes], None]] = None,
        format: Optional[bytes] = None,
        subdirs: Optional[Sequence[bytes]] = None,
        prefix: Optional[bytes] = None,
    ) -> None:
        """Retrieve an archive of the specified tree."""
        raise NotImplementedError(self.archive)

    @staticmethod
    def _warn_filter_objects() -> None:
        import warnings

        warnings.warn(
            "object filtering not recognized by server, ignoring",
            UserWarning,
        )


def check_wants(wants: Set[bytes], refs: Mapping[bytes, bytes]) -> None:
    """Check that a set of wants is valid.

    Args:
      wants: Set of object SHAs to fetch
      refs: Refs dictionary to check against
    """
    missing = set(wants) - {
        v for (k, v) in refs.items() if not k.endswith(PEELED_TAG_SUFFIX)
    }
    if missing:
        raise InvalidWants(missing)


def _remote_error_from_stderr(stderr: Optional[IO[bytes]]) -> Exception:
    if stderr is None:
        return HangupException()
    lines = [line.rstrip(b"\n") for line in stderr.readlines()]
    for line in lines:
        if line.startswith(b"ERROR: "):
            return GitProtocolError(line[len(b"ERROR: ") :].decode("utf-8", "replace"))
    return HangupException(lines)


class TraditionalGitClient(GitClient):
    """Traditional Git client."""

    DEFAULT_ENCODING = "utf-8"

    def __init__(
        self,
        path_encoding: str = DEFAULT_ENCODING,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Initialize a TraditionalGitClient.

        Args:
            path_encoding: Encoding for paths (default: utf-8)
            thin_packs: Whether or not thin packs should be retrieved
            report_activity: Optional callback for reporting transport activity
            quiet: Whether to suppress progress output
            include_tags: Whether to include tags
        """
        self._remote_path_encoding = path_encoding
        super().__init__(
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )

    def _connect(
        self,
        cmd: bytes,
        path: Union[str, bytes],
        protocol_version: Optional[int] = None,
    ) -> tuple[Protocol, Callable[[], bool], Optional[IO[bytes]]]:
        """Create a connection to the server.

        This method is abstract - concrete implementations should
        implement their own variant which connects to the server and
        returns an initialized Protocol object with the service ready
        for use and a can_read function which may be used to see if
        reads would block.

        Args:
          cmd: The git service name to which we should connect.
          path: The path we should pass to the service. (as bytestirng)
          protocol_version: Desired Git protocol version. By default the highest
            mutually supported protocol version will be used.
        """
        raise NotImplementedError

    def send_pack(
        self,
        path: bytes,
        update_refs: Callable[[dict[bytes, bytes]], dict[bytes, bytes]],
        generate_pack_data: "GeneratePackDataFunc",
        progress: Optional[Callable[[bytes], None]] = None,
    ) -> SendPackResult:
        """Upload a pack to a remote repository.

        Args:
          path: Repository path (as bytestring)
          update_refs: Function to determine changes to remote refs.
            Receive dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
          generate_pack_data: Function that can return a tuple with
            number of objects and pack data to upload.
          progress: Optional callback called with progress updates

        Returns:
          SendPackResult

        Raises:
          SendPackError: if server rejects the pack data

        """
        self.protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_SEND
        proto, _unused_can_read, stderr = self._connect(b"receive-pack", path)
        with proto:
            try:
                old_refs, server_capabilities = read_pkt_refs_v1(proto.read_pkt_seq())
            except HangupException as exc:
                raise _remote_error_from_stderr(stderr) from exc
            (
                negotiated_capabilities,
                agent,
            ) = self._negotiate_receive_pack_capabilities(server_capabilities)
            if CAPABILITY_REPORT_STATUS in negotiated_capabilities:
                self._report_status_parser = ReportStatusParser()
            report_status_parser = self._report_status_parser

            try:
                new_refs = orig_new_refs = update_refs(old_refs)
            except BaseException:
                proto.write_pkt_line(None)
                raise

            if set(new_refs.items()).issubset(set(old_refs.items())):
                proto.write_pkt_line(None)
                # Convert new_refs to match SendPackResult expected type
                return SendPackResult(
                    _to_optional_dict(new_refs), agent=agent, ref_status={}
                )

            if CAPABILITY_DELETE_REFS not in server_capabilities:
                # Server does not support deletions. Fail later.
                new_refs = dict(orig_new_refs)
                for ref, sha in orig_new_refs.items():
                    if sha == ZERO_SHA:
                        if CAPABILITY_REPORT_STATUS in negotiated_capabilities:
                            assert report_status_parser is not None
                            report_status_parser._ref_statuses.append(
                                b"ng " + ref + b" remote does not support deleting refs"
                            )
                        del new_refs[ref]

            if new_refs is None:
                proto.write_pkt_line(None)
                return SendPackResult(old_refs, agent=agent, ref_status={})

            if len(new_refs) == 0 and orig_new_refs:
                # NOOP - Original new refs filtered out by policy
                proto.write_pkt_line(None)
                if report_status_parser is not None:
                    ref_status = dict(report_status_parser.check())
                else:
                    ref_status = None
                # Convert to Optional type for SendPackResult
                return SendPackResult(
                    _to_optional_dict(old_refs), agent=agent, ref_status=ref_status
                )

            header_handler = _v1ReceivePackHeader(
                list(negotiated_capabilities),
                old_refs,
                new_refs,
            )

            for pkt in header_handler:
                proto.write_pkt_line(pkt)

            pack_data_count, pack_data = generate_pack_data(
                header_handler.have,
                header_handler.want,
                ofs_delta=(CAPABILITY_OFS_DELTA in negotiated_capabilities),
                progress=progress,
            )

            if self._should_send_pack(new_refs):
                for chunk in PackChunkGenerator(pack_data_count, pack_data):
                    proto.write(chunk)

            ref_status = self._handle_receive_pack_tail(
                proto, negotiated_capabilities, progress
            )
            refs_with_optional_2: dict[bytes, Optional[bytes]] = {
                k: v for k, v in new_refs.items()
            }
            return SendPackResult(
                refs_with_optional_2, agent=agent, ref_status=ref_status
            )

    def fetch_pack(
        self,
        path: bytes,
        determine_wants: "DetermineWantsFunc",
        graph_walker: GraphWalker,
        pack_data: Callable[[bytes], int],
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Retrieve a pack from a git smart server.

        Args:
          path: Remote path to fetch from
          determine_wants: Function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch.
          graph_walker: Object with next() and ack().
          pack_data: Callback called for each bit of data in the pack
          progress: Callback for progress reports (strings)
          depth: Shallow fetch depth
          ref_prefix: List of prefixes of desired references, as a list of
            bytestrings. Filtering is done by the server if supported, and
            client side otherwise.
          filter_spec: A git-rev-list-style object filter spec, as bytestring.
            Only used if the server supports the Git protocol-v2 'filter'
            feature, and ignored otherwise.
          protocol_version: Desired Git protocol version. By default the highest
            mutually supported protocol version will be used.
          shallow_since: Deepen the history to include commits after this date
          shallow_exclude: Deepen the history to exclude commits reachable from these refs

        Returns:
          FetchPackResult object

        """
        if (
            protocol_version is not None
            and protocol_version not in GIT_PROTOCOL_VERSIONS
        ):
            raise ValueError(f"unknown Git protocol version {protocol_version}")
        proto, can_read, stderr = self._connect(b"upload-pack", path, protocol_version)
        server_protocol_version = negotiate_protocol_version(proto)
        if server_protocol_version not in GIT_PROTOCOL_VERSIONS:
            raise ValueError(
                f"unknown Git protocol version {server_protocol_version} used by server"
            )
        if protocol_version and server_protocol_version > protocol_version:
            raise ValueError(
                f"bad Git protocol version {server_protocol_version} used by server"
            )
        self.protocol_version = server_protocol_version
        with proto:
            # refs may have None values in v2 but not in v1
            refs: dict[bytes, Optional[bytes]]
            if self.protocol_version == 2:
                try:
                    server_capabilities = read_server_capabilities(proto.read_pkt_seq())
                except HangupException as exc:
                    raise _remote_error_from_stderr(stderr) from exc
                (
                    negotiated_capabilities,
                    symrefs,
                    agent,
                ) = self._negotiate_upload_pack_capabilities(server_capabilities)

                proto.write_pkt_line(b"command=ls-refs\n")
                proto.write(b"0001")  # delim-pkt
                proto.write_pkt_line(b"symrefs")
                proto.write_pkt_line(b"peel")
                if ref_prefix is None:
                    ref_prefix = DEFAULT_REF_PREFIX
                for prefix in ref_prefix:
                    proto.write_pkt_line(b"ref-prefix " + prefix)
                proto.write_pkt_line(None)
                refs, symrefs, _peeled = read_pkt_refs_v2(proto.read_pkt_seq())
            else:
                try:
                    refs_v1, server_capabilities = read_pkt_refs_v1(
                        proto.read_pkt_seq()
                    )
                    # v1 refs never have None values, but we need Optional type for compatibility
                    refs = _to_optional_dict(refs_v1)
                except HangupException as exc:
                    raise _remote_error_from_stderr(stderr) from exc
                (
                    negotiated_capabilities,
                    symrefs,
                    agent,
                ) = self._negotiate_upload_pack_capabilities(server_capabilities)

                if ref_prefix is not None:
                    refs = filter_ref_prefix(refs, ref_prefix)

            if refs is None:
                proto.write_pkt_line(None)
                return FetchPackResult(refs, symrefs, agent)

            try:
                # Filter out None values (shouldn't be any in v1 protocol)
                refs_no_none = {k: v for k, v in refs.items() if v is not None}
                # Handle both old and new style determine_wants
                try:
                    wants = determine_wants(refs_no_none, depth)
                except TypeError:
                    # Old-style determine_wants that doesn't accept depth
                    wants = determine_wants(refs_no_none)
            except BaseException:
                proto.write_pkt_line(None)
                raise
            if wants is not None:
                wants = [cid for cid in wants if cid != ZERO_SHA]
            if not wants:
                proto.write_pkt_line(None)
                return FetchPackResult(refs, symrefs, agent)
            if self.protocol_version == 2:
                proto.write_pkt_line(b"command=fetch\n")
                proto.write(b"0001")  # delim-pkt
                if CAPABILITY_THIN_PACK in self._fetch_capabilities:
                    proto.write(pkt_line(b"thin-pack\n"))
                if (
                    find_capability(
                        list(negotiated_capabilities),
                        CAPABILITY_FETCH,
                        CAPABILITY_FILTER,
                    )
                    and filter_spec
                ):
                    proto.write(pkt_line(b"filter %s\n" % filter_spec))
                elif filter_spec:
                    self._warn_filter_objects()
            elif filter_spec:
                self._warn_filter_objects()
            (new_shallow, new_unshallow) = _handle_upload_pack_head(
                proto,
                list(negotiated_capabilities),
                graph_walker,
                wants,
                can_read,
                depth=depth,
                protocol_version=self.protocol_version,
                shallow_since=shallow_since,
                shallow_exclude=shallow_exclude,
            )
            _handle_upload_pack_tail(
                proto,
                negotiated_capabilities,
                graph_walker,
                pack_data,
                progress,
                protocol_version=self.protocol_version,
            )
            return FetchPackResult(refs, symrefs, agent, new_shallow, new_unshallow)

    def get_refs(
        self,
        path: bytes,
        protocol_version: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
    ) -> LsRemoteResult:
        """Retrieve the current refs from a git smart server."""
        # stock `git ls-remote` uses upload-pack
        if (
            protocol_version is not None
            and protocol_version not in GIT_PROTOCOL_VERSIONS
        ):
            raise ValueError(f"unknown Git protocol version {protocol_version}")
        proto, _, stderr = self._connect(b"upload-pack", path, protocol_version)
        server_protocol_version = negotiate_protocol_version(proto)
        if server_protocol_version not in GIT_PROTOCOL_VERSIONS:
            raise ValueError(
                f"unknown Git protocol version {server_protocol_version} used by server"
            )
        if protocol_version and server_protocol_version > protocol_version:
            raise ValueError(
                f"bad Git protocol version {server_protocol_version} used by server"
            )
        self.protocol_version = server_protocol_version
        if self.protocol_version == 2:
            server_capabilities = read_server_capabilities(proto.read_pkt_seq())
            proto.write_pkt_line(b"command=ls-refs\n")
            proto.write(b"0001")  # delim-pkt
            proto.write_pkt_line(b"symrefs")
            proto.write_pkt_line(b"peel")
            if ref_prefix is None:
                ref_prefix = DEFAULT_REF_PREFIX
            for prefix in ref_prefix:
                proto.write_pkt_line(b"ref-prefix " + prefix)
            proto.write_pkt_line(None)
            with proto:
                try:
                    refs, symrefs, peeled = read_pkt_refs_v2(proto.read_pkt_seq())
                except HangupException as exc:
                    raise _remote_error_from_stderr(stderr) from exc
                proto.write_pkt_line(None)
                for refname, refvalue in peeled.items():
                    refs[refname + PEELED_TAG_SUFFIX] = refvalue
                return LsRemoteResult(refs, symrefs)
        else:
            with proto:
                try:
                    refs_v1, server_capabilities = read_pkt_refs_v1(
                        proto.read_pkt_seq()
                    )
                    # v1 refs never have None values, but we need Optional type for compatibility
                    refs = _to_optional_dict(refs_v1)
                except HangupException as exc:
                    raise _remote_error_from_stderr(stderr) from exc
                proto.write_pkt_line(None)
                (symrefs, _agent) = _extract_symrefs_and_agent(server_capabilities)
                if ref_prefix is not None:
                    refs = filter_ref_prefix(refs, ref_prefix)
                return LsRemoteResult(refs, symrefs)

    def archive(
        self,
        path: bytes,
        committish: bytes,
        write_data: Callable[[bytes], None],
        progress: Optional[Callable[[bytes], None]] = None,
        write_error: Optional[Callable[[bytes], None]] = None,
        format: Optional[bytes] = None,
        subdirs: Optional[Sequence[bytes]] = None,
        prefix: Optional[bytes] = None,
    ) -> None:
        """Request an archive of a specific commit.

        Args:
            path: Repository path
            committish: Commit ID or ref to archive
            write_data: Function to write archive data
            progress: Optional progress callback
            write_error: Optional error callback
            format: Optional archive format
            subdirs: Optional subdirectories to include
            prefix: Optional prefix for archived files
        """
        proto, _can_read, stderr = self._connect(b"upload-archive", path)
        with proto:
            if format is not None:
                proto.write_pkt_line(b"argument --format=" + format)
            proto.write_pkt_line(b"argument " + committish)
            if subdirs is not None:
                for subdir in subdirs:
                    proto.write_pkt_line(b"argument " + subdir)
            if prefix is not None:
                proto.write_pkt_line(b"argument --prefix=" + prefix)
            proto.write_pkt_line(None)
            try:
                pkt = proto.read_pkt_line()
            except HangupException as exc:
                raise _remote_error_from_stderr(stderr) from exc
            if pkt == b"NACK\n" or pkt == b"NACK":
                return
            elif pkt == b"ACK\n" or pkt == b"ACK":
                pass
            elif pkt and pkt.startswith(b"ERR "):
                raise GitProtocolError(pkt[4:].rstrip(b"\n").decode("utf-8", "replace"))
            else:
                raise AssertionError(f"invalid response {pkt!r}")
            ret = proto.read_pkt_line()
            if ret is not None:
                raise AssertionError("expected pkt tail")
            for chan, data in _read_side_band64k_data(proto.read_pkt_seq()):
                if chan == SIDE_BAND_CHANNEL_DATA:
                    write_data(data)
                elif chan == SIDE_BAND_CHANNEL_PROGRESS:
                    if progress is not None:
                        progress(data)
                elif chan == SIDE_BAND_CHANNEL_FATAL:
                    if write_error is not None:
                        write_error(data)
                else:
                    raise AssertionError(f"Invalid sideband channel {chan}")


class TCPGitClient(TraditionalGitClient):
    """A Git Client that works over TCP directly (i.e. git://)."""

    def __init__(
        self,
        host: str,
        port: Optional[int] = None,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Initialize a TCPGitClient.

        Args:
          host: Hostname or IP address to connect to
          port: Port number (defaults to TCP_GIT_PORT)
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
        """
        if port is None:
            port = TCP_GIT_PORT
        self._host = host
        self._port = port
        super().__init__(
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )

    @classmethod
    def from_parsedurl(
        cls,
        parsedurl: ParseResult,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        dumb: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "TCPGitClient":
        """Create an instance of TCPGitClient from a parsed URL.

        Args:
          parsedurl: Result of urlparse()
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
          dumb: Whether to use dumb protocol (not used for TCPGitClient)
          username: Username for authentication (not used for TCPGitClient)
          password: Password for authentication (not used for TCPGitClient)
          config: Configuration object (not used for TCPGitClient)

        Returns:
          A TCPGitClient instance
        """
        assert parsedurl.hostname is not None
        return cls(
            parsedurl.hostname,
            port=parsedurl.port,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )

    def get_url(self, path: str) -> str:
        r"""Get the URL for a TCP git connection.

        Args:
          path: Repository path

        Returns:
          ``git://`` URL for the path
        """
        # IPv6 addresses contain colons and need to be wrapped in brackets
        if ":" in self._host:
            netloc = f"[{self._host}]"
        else:
            netloc = self._host

        if self._port is not None and self._port != TCP_GIT_PORT:
            netloc += f":{self._port}"
        return urlunsplit(("git", netloc, path, "", ""))

    def _connect(
        self,
        cmd: bytes,
        path: Union[str, bytes],
        protocol_version: Optional[int] = None,
    ) -> tuple[Protocol, Callable[[], bool], Optional[IO[bytes]]]:
        if not isinstance(cmd, bytes):
            raise TypeError(cmd)
        if not isinstance(path, bytes):
            path = path.encode(self._remote_path_encoding)
        sockaddrs = socket.getaddrinfo(
            self._host, self._port, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        s = None
        err = OSError(f"no address found for {self._host}")
        for family, socktype, protof, canonname, sockaddr in sockaddrs:
            s = socket.socket(family, socktype, protof)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                s.connect(sockaddr)
                break
            except OSError as e:
                err = e
                if s is not None:
                    s.close()
                s = None
        if s is None:
            raise err
        # -1 means system default buffering
        rfile = s.makefile("rb", -1)
        # 0 means unbuffered
        wfile = s.makefile("wb", 0)

        def close() -> None:
            rfile.close()
            wfile.close()
            s.close()

        proto = Protocol(
            rfile.read,
            wfile.write,
            close,
            report_activity=self._report_activity,
        )
        if path.startswith(b"/~"):
            path = path[1:]
        if cmd == b"upload-pack":
            if protocol_version is None:
                self.protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_FETCH
            else:
                self.protocol_version = protocol_version
        else:
            self.protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_SEND

        if cmd == b"upload-pack" and self.protocol_version == 2:
            # Git protocol version advertisement is hidden behind two NUL bytes
            # for compatibility with older Git server implementations, which
            # would crash if something other than a "host=" header was found
            # after the first NUL byte.
            version_str = b"\0\0version=%d\0" % self.protocol_version
        else:
            version_str = b""
        # TODO(jelmer): Alternative to ascii?
        proto.send_cmd(
            b"git-" + cmd, path, b"host=" + self._host.encode("ascii") + version_str
        )
        return proto, lambda: _fileno_can_read(s.fileno()), None


class SubprocessWrapper:
    """A socket-like object that talks to a subprocess via pipes."""

    def __init__(self, proc: subprocess.Popen[bytes]) -> None:
        """Initialize a SubprocessWrapper.

        Args:
          proc: Subprocess.Popen instance to wrap
        """
        self.proc = proc
        assert proc.stdout is not None
        assert proc.stdin is not None
        self.read = BufferedReader(proc.stdout).read  # type: ignore[type-var]
        self.write = proc.stdin.write

    @property
    def stderr(self) -> Optional[IO[bytes]]:
        """Return the stderr stream of the subprocess."""
        return self.proc.stderr

    def can_read(self) -> bool:
        """Check if there is data available to read.

        Returns: True if data is available, False otherwise
        """
        if sys.platform == "win32":
            from msvcrt import get_osfhandle

            assert self.proc.stdout is not None
            handle = get_osfhandle(self.proc.stdout.fileno())
            return _win32_peek_avail(handle) != 0
        else:
            assert self.proc.stdout is not None
            return _fileno_can_read(self.proc.stdout.fileno())

    def close(self, timeout: Optional[int] = 60) -> None:
        """Close the subprocess and wait for it to terminate.

        Args:
          timeout: Maximum time to wait for subprocess to terminate (seconds)

        Raises:
          GitProtocolError: If subprocess doesn't terminate within timeout
        """
        if self.proc.stdin:
            self.proc.stdin.close()
        if self.proc.stdout:
            self.proc.stdout.close()
        if self.proc.stderr:
            self.proc.stderr.close()
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as e:
            self.proc.kill()
            self.proc.wait()
            raise GitProtocolError(
                f"Git subprocess did not terminate within {timeout} seconds; killed it."
            ) from e


def find_git_command() -> list[str]:
    """Find command to run for system Git (usually C Git)."""
    if sys.platform == "win32":  # support .exe, .bat and .cmd
        try:  # to avoid overhead
            import pywintypes
            import win32api
        except ImportError:  # run through cmd.exe with some overhead
            return ["cmd", "/c", "git"]
        else:
            try:
                _status, git = win32api.FindExecutable("git")
                return [git]
            except pywintypes.error:
                return ["cmd", "/c", "git"]
    else:
        return ["git"]


class SubprocessGitClient(TraditionalGitClient):
    """Git client that talks to a server using a subprocess."""

    @classmethod
    def from_parsedurl(
        cls,
        parsedurl: ParseResult,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        dumb: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "SubprocessGitClient":
        """Create an instance of SubprocessGitClient from a parsed URL.

        Args:
          parsedurl: Result of urlparse()
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
          dumb: Whether to use dumb protocol (not used for SubprocessGitClient)
          username: Username for authentication (not used for SubprocessGitClient)
          password: Password for authentication (not used for SubprocessGitClient)
          config: Configuration object (not used for SubprocessGitClient)

        Returns:
          A SubprocessGitClient instance
        """
        return cls(
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )

    git_command: Optional[str] = None

    def _connect(
        self,
        service: bytes,
        path: Union[bytes, str],
        protocol_version: Optional[int] = None,
    ) -> tuple[Protocol, Callable[[], bool], Optional[IO[bytes]]]:
        if not isinstance(service, bytes):
            raise TypeError(service)
        if isinstance(path, bytes):
            path = path.decode(self._remote_path_encoding)
        if self.git_command is None:
            git_command = find_git_command()
        argv = [*git_command, service.decode("ascii"), path]
        p = subprocess.Popen(
            argv,
            bufsize=0,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        pw = SubprocessWrapper(p)
        return (
            Protocol(
                pw.read,
                pw.write,
                pw.close,
                report_activity=self._report_activity,
            ),
            pw.can_read,
            p.stderr,
        )


class LocalGitClient(GitClient):
    """Git Client that just uses a local on-disk repository."""

    def __init__(
        self,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        config: Optional[Config] = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Create a new LocalGitClient instance.

        Args:
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport
            activity.
          config: Optional configuration object
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
        """
        self._report_activity = report_activity
        self._quiet = quiet
        self._include_tags = include_tags
        # Ignore the thin_packs argument

    def get_url(self, path: str) -> str:
        """Get the URL for a local file path.

        Args:
          path: Local file path

        Returns:
          file:// URL for the path
        """
        return urlunsplit(("file", "", path, "", ""))

    @classmethod
    def from_parsedurl(
        cls,
        parsedurl: ParseResult,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        dumb: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "LocalGitClient":
        """Create an instance of LocalGitClient from a parsed URL.

        Args:
          parsedurl: Result of urlparse()
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
          dumb: Whether to use dumb protocol (not used for LocalGitClient)
          username: Username for authentication (not used for LocalGitClient)
          password: Password for authentication (not used for LocalGitClient)
          config: Optional configuration object

        Returns:
          A LocalGitClient instance
        """
        return cls(
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
            config=config,
        )

    @classmethod
    def _open_repo(cls, path: Union[str, bytes]) -> "closing[Repo]":
        """Open a local repository.

        Args:
          path: Repository path (as bytes or str)

        Returns:
          Repo instance wrapped in a closing context manager
        """
        if not isinstance(path, str):
            path = os.fsdecode(path)
        return closing(Repo(path))

    def send_pack(
        self,
        path: Union[str, bytes],
        update_refs: Callable[[dict[bytes, bytes]], dict[bytes, bytes]],
        generate_pack_data: "GeneratePackDataFunc",
        progress: Optional[Callable[[bytes], None]] = None,
    ) -> SendPackResult:
        """Upload a pack to a local on-disk repository.

        Args:
          path: Repository path (as bytestring)
          update_refs: Function to determine changes to remote refs.
            Receive dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
            with number of items and pack data to upload.
          generate_pack_data: Function that generates pack data given
            have and want object sets
          progress: Optional progress function

        Returns:
          SendPackResult

        Raises:
          SendPackError: if server rejects the pack data

        """
        if not progress:

            def progress(x: bytes) -> None:
                pass

        with self._open_repo(path) as target:
            old_refs = target.get_refs()
            new_refs = update_refs(dict(old_refs))

            have = [sha1 for sha1 in old_refs.values() if sha1 != ZERO_SHA]
            want = []
            for refname, new_sha1 in new_refs.items():
                if (
                    new_sha1 not in have
                    and new_sha1 not in want
                    and new_sha1 != ZERO_SHA
                ):
                    want.append(new_sha1)

            if not want and set(new_refs.items()).issubset(set(old_refs.items())):
                return SendPackResult(_to_optional_dict(new_refs), ref_status={})

            target.object_store.add_pack_data(
                *generate_pack_data(
                    set(have), set(want), ofs_delta=True, progress=progress
                )
            )

            ref_status: dict[bytes, Optional[str]] = {}

            for refname, new_sha1 in new_refs.items():
                old_sha1 = old_refs.get(refname, ZERO_SHA)
                if new_sha1 != ZERO_SHA:
                    if not target.refs.set_if_equals(refname, old_sha1, new_sha1):
                        msg = f"unable to set {refname!r} to {new_sha1!r}"
                        progress(msg.encode())
                        ref_status[refname] = msg
                else:
                    if not target.refs.remove_if_equals(refname, old_sha1):
                        progress(f"unable to remove {refname!r}".encode())
                        ref_status[refname] = "unable to remove"

        return SendPackResult(_to_optional_dict(new_refs), ref_status=ref_status)

    def fetch(
        self,
        path: bytes,
        target: BaseRepo,
        determine_wants: Optional["DetermineWantsFunc"] = None,
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[bytes]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Fetch into a target repository.

        Args:
          path: Path to fetch from (as bytestring)
          target: Target repository to fetch into
          determine_wants: Optional function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch. Defaults to all shas.
          progress: Optional progress function
          depth: Shallow fetch depth
          ref_prefix: List of prefixes of desired references, as a list of
            bytestrings. Filtering is done by the server if supported, and
            client side otherwise.
          filter_spec: A git-rev-list-style object filter spec, as bytestring.
            Only used if the server supports the Git protocol-v2 'filter'
            feature, and ignored otherwise.
          protocol_version: Optional Git protocol version
          shallow_since: Deepen the history to include commits after this date
          shallow_exclude: Deepen the history to exclude commits reachable from these refs

        Returns:
          FetchPackResult object

        """
        with self._open_repo(path) as r:
            refs = r.fetch(
                target,
                determine_wants=determine_wants,
                progress=progress,
                depth=depth,
            )
            return FetchPackResult(
                _to_optional_dict(refs), r.refs.get_symrefs(), agent_string()
            )

    def fetch_pack(
        self,
        path: Union[str, bytes],
        determine_wants: "DetermineWantsFunc",
        graph_walker: GraphWalker,
        pack_data: Callable[[bytes], int],
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Retrieve a pack from a local on-disk repository.

        Args:
          path: Remote path to fetch from
          determine_wants: Function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch.
          graph_walker: Object with next() and ack().
          pack_data: Callback called for each bit of data in the pack
          progress: Callback for progress reports (strings)
          depth: Shallow fetch depth
          ref_prefix: List of prefixes of desired references, as a list of
            bytestrings. Filtering is done by the server if supported, and
            client side otherwise.
          filter_spec: A git-rev-list-style object filter spec, as bytestring.
            Only used if the server supports the Git protocol-v2 'filter'
            feature, and ignored otherwise.
          protocol_version: Optional Git protocol version
          shallow_since: Deepen the history to include commits after this date
          shallow_exclude: Deepen the history to exclude commits reachable from these refs

        Returns:
          FetchPackResult object

        """
        with self._open_repo(path) as r:
            missing_objects = r.find_missing_objects(
                determine_wants, graph_walker, progress=progress, depth=depth
            )
            if missing_objects is None:
                other_haves = set()
                object_ids = []
            else:
                other_haves = missing_objects.get_remote_has()
                object_ids = list(missing_objects)
            symrefs = r.refs.get_symrefs()
            agent = agent_string()

            # Did the process short-circuit (e.g. in a stateless RPC call)?
            # Note that the client still expects a 0-object pack in most cases.
            if object_ids is None:
                return FetchPackResult(None, symrefs, agent)
            write_pack_from_container(
                pack_data,  # type: ignore[arg-type]
                r.object_store,
                object_ids,
                other_haves=other_haves,
            )
            # Convert refs to Optional type for FetchPackResult
            return FetchPackResult(_to_optional_dict(r.get_refs()), symrefs, agent)

    def get_refs(
        self,
        path: Union[str, bytes],
        protocol_version: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
    ) -> LsRemoteResult:
        """Retrieve the current refs from a local on-disk repository."""
        with self._open_repo(path) as target:
            refs_dict = target.get_refs()
            refs = _to_optional_dict(refs_dict)
            # Extract symrefs from the local repository
            symrefs: dict[bytes, bytes] = {}
            for ref in refs:
                try:
                    # Check if this ref is symbolic by reading it directly
                    ref_value = target.refs.read_ref(ref)
                    if ref_value and ref_value.startswith(SYMREF):
                        # Extract the target from the symref
                        symrefs[ref] = ref_value[len(SYMREF) :]
                except (KeyError, ValueError):
                    # Not a symbolic ref or error reading it
                    pass
            return LsRemoteResult(refs, symrefs)


class BundleClient(GitClient):
    """Git Client that reads from a bundle file."""

    def __init__(
        self,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        config: Optional[Config] = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Create a new BundleClient instance.

        Args:
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport
            activity.
          config: Optional configuration object
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
        """
        self._report_activity = report_activity
        self._quiet = quiet
        self._include_tags = include_tags

    def get_url(self, path: str) -> str:
        """Get the URL for a bundle file path.

        Args:
          path: Bundle file path

        Returns:
          The path unchanged (bundle files use local paths)
        """
        return path

    @classmethod
    def from_parsedurl(
        cls,
        parsedurl: ParseResult,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        dumb: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> "BundleClient":
        """Create an instance of BundleClient from a parsed URL.

        Args:
          parsedurl: Result of urlparse()
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
          dumb: Whether to use dumb protocol (not used for BundleClient)
          username: Username for authentication (not used for BundleClient)
          password: Password for authentication (not used for BundleClient)
          config: Configuration object (not used for BundleClient)

        Returns:
          A BundleClient instance
        """
        return cls(
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )

    @classmethod
    def _is_bundle_file(cls, path: str) -> bool:
        """Check if a file is a git bundle by reading the first line."""
        try:
            with open(path, "rb") as f:
                first_line = f.readline()
                return first_line in (b"# v2 git bundle\n", b"# v3 git bundle\n")
        except OSError:
            return False

    @classmethod
    def _open_bundle(cls, path: Union[str, bytes]) -> "Bundle":
        """Open and parse a bundle file.

        Args:
          path: Path to the bundle file (bytes or str)

        Returns:
          Bundle object with parsed metadata

        Raises:
          AssertionError: If bundle format is unsupported
        """
        if not isinstance(path, str):
            path = os.fsdecode(path)
        # Read bundle metadata without PackData to avoid file handle issues
        with open(path, "rb") as f:
            from dulwich.bundle import Bundle

            version = None
            firstline = f.readline()
            if firstline == b"# v2 git bundle\n":
                version = 2
            elif firstline == b"# v3 git bundle\n":
                version = 3
            else:
                raise AssertionError(f"unsupported bundle format header: {firstline!r}")

            capabilities = {}
            prerequisites = []
            references = {}
            line = f.readline()

            if version >= 3:
                while line.startswith(b"@"):
                    line = line[1:].rstrip(b"\n")
                    try:
                        key, value_bytes = line.split(b"=", 1)
                        value = value_bytes.decode("utf-8")
                    except ValueError:
                        key = line
                        value = None
                    capabilities[key.decode("utf-8")] = value
                    line = f.readline()

            while line.startswith(b"-"):
                (obj_id, comment) = line[1:].rstrip(b"\n").split(b" ", 1)
                prerequisites.append((obj_id, comment))
                line = f.readline()

            while line != b"\n":
                (obj_id, ref) = line.rstrip(b"\n").split(b" ", 1)
                references[ref] = obj_id
                line = f.readline()

            # Don't read PackData here, we'll do it later
            bundle = Bundle()
            bundle.version = version
            bundle.capabilities = capabilities
            bundle.prerequisites = prerequisites
            bundle.references = references
            bundle.pack_data = None  # Will be read on demand

            return bundle

    @staticmethod
    def _skip_to_pack_data(f: IO[bytes], version: int) -> None:
        """Skip to the pack data section in a bundle file.

        Args:
          f: File object positioned at the beginning of the bundle
          version: Bundle format version (2 or 3)

        Raises:
          AssertionError: If bundle header is invalid
        """
        # Skip header
        header = f.readline()
        if header not in (b"# v2 git bundle\n", b"# v3 git bundle\n"):
            raise AssertionError(f"Invalid bundle header: {header!r}")

        line = f.readline()

        # Skip capabilities (v3 only)
        if version >= 3:
            while line.startswith(b"@"):
                line = f.readline()

        # Skip prerequisites
        while line.startswith(b"-"):
            line = f.readline()

        # Skip references
        while line != b"\n":
            line = f.readline()

        # Now at pack data

    def send_pack(
        self,
        path: Union[str, bytes],
        update_refs: Callable[[dict[bytes, bytes]], dict[bytes, bytes]],
        generate_pack_data: "GeneratePackDataFunc",
        progress: Optional[Callable[[bytes], None]] = None,
    ) -> SendPackResult:
        """Upload is not supported for bundle files."""
        raise NotImplementedError("Bundle files are read-only")

    def fetch(
        self,
        path: bytes,
        target: BaseRepo,
        determine_wants: Optional["DetermineWantsFunc"] = None,
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Fetch into a target repository from a bundle file."""
        bundle = self._open_bundle(path)

        # Get references from bundle
        refs = dict(bundle.references)

        # Determine what we want to fetch
        if determine_wants is None:
            _ = list(refs.values())
        else:
            _ = determine_wants(refs, None)

        # Add pack data to target repository
        # Need to reopen the file for pack data access
        with open(path, "rb") as pack_file:
            # Skip to pack data section
            assert bundle.version is not None
            BundleClient._skip_to_pack_data(pack_file, bundle.version)
            # Read pack data into memory to avoid file positioning issues
            pack_bytes = pack_file.read()

        # Create PackData from in-memory bytes
        from io import BytesIO

        pack_io = BytesIO(pack_bytes)
        pack_data = PackData.from_file(pack_io)
        target.object_store.add_pack_data(len(pack_data), pack_data.iter_unpacked())

        # Apply ref filtering if specified
        if ref_prefix:
            filtered_refs = {}
            for ref_name, ref_value in refs.items():
                for prefix in ref_prefix:
                    if ref_name.startswith(prefix):
                        filtered_refs[ref_name] = ref_value
                        break
            refs = filtered_refs

        return FetchPackResult(_to_optional_dict(refs), {}, agent_string())

    def fetch_pack(
        self,
        path: Union[str, bytes],
        determine_wants: "DetermineWantsFunc",
        graph_walker: GraphWalker,
        pack_data: Callable[[bytes], int],
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Retrieve a pack from a bundle file."""
        bundle = self._open_bundle(path)

        # Get references from bundle
        refs = dict(bundle.references)

        # Determine what we want to fetch
        try:
            _ = determine_wants(refs, depth)
        except TypeError:
            # Old-style determine_wants that doesn't accept depth
            _ = determine_wants(refs)

        # Write pack data to the callback
        # Need to reopen the file for pack data access
        with open(path, "rb") as pack_file:
            # Skip to pack data section
            assert bundle.version is not None
            BundleClient._skip_to_pack_data(pack_file, bundle.version)
            # Read pack data and write it to the callback
            pack_bytes = pack_file.read()
            pack_data(pack_bytes)

        # Apply ref filtering if specified
        if ref_prefix:
            filtered_refs = {}
            for ref_name, ref_value in refs.items():
                for prefix in ref_prefix:
                    if ref_name.startswith(prefix):
                        filtered_refs[ref_name] = ref_value
                        break
            refs = filtered_refs

        return FetchPackResult(_to_optional_dict(refs), {}, agent_string())

    def get_refs(
        self,
        path: Union[str, bytes],
        protocol_version: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
    ) -> LsRemoteResult:
        """Retrieve the current refs from a bundle file."""
        bundle = self._open_bundle(path)

        refs = dict(bundle.references)

        # Apply ref filtering if specified
        if ref_prefix:
            filtered_refs = {}
            for ref_name, ref_value in refs.items():
                for prefix in ref_prefix:
                    if ref_name.startswith(prefix):
                        filtered_refs[ref_name] = ref_value
                        break
            refs = filtered_refs

        # Bundle refs are always concrete (never None), but LsRemoteResult expects Optional
        return LsRemoteResult(_to_optional_dict(refs), {})


# What Git client to use for local access
default_local_git_client_cls = LocalGitClient


class SSHVendor:
    """A client side SSH implementation."""

    def run_command(
        self,
        host: str,
        command: bytes,
        username: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        key_filename: Optional[str] = None,
        ssh_command: Optional[str] = None,
        protocol_version: Optional[int] = None,
    ) -> SubprocessWrapper:
        """Connect to an SSH server.

        Run a command remotely and return a file-like object for interaction
        with the remote command.

        Args:
          host: Host name
          command: Command to run (as argv array)
          username: Optional ame of user to log in as
          port: Optional SSH port to use
          password: Optional ssh password for login or private key
          key_filename: Optional path to private keyfile
          ssh_command: Optional SSH command
          protocol_version: Desired Git protocol version. By default the highest
            mutually supported protocol version will be used.
        """
        raise NotImplementedError(self.run_command)


class StrangeHostname(Exception):
    """Refusing to connect to strange SSH hostname."""

    def __init__(self, hostname: str) -> None:
        """Initialize StrangeHostname exception.

        Args:
            hostname: The strange hostname that was rejected
        """
        super().__init__(hostname)


class SubprocessSSHVendor(SSHVendor):
    """SSH vendor that shells out to the local 'ssh' command."""

    def run_command(
        self,
        host: str,
        command: bytes,
        username: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        key_filename: Optional[str] = None,
        ssh_command: Optional[str] = None,
        protocol_version: Optional[int] = None,
    ) -> SubprocessWrapper:
        """Run a git command over SSH.

        Args:
            host: SSH host to connect to
            command: Git command to run
            username: Optional username
            port: Optional port number
            password: Optional password (not supported)
            key_filename: Optional SSH key file
            ssh_command: Optional custom SSH command
            protocol_version: Optional Git protocol version

        Returns:
            Tuple of (subprocess.Popen, Protocol, stderr_stream)
        """
        if password is not None:
            raise NotImplementedError(
                "Setting password not supported by SubprocessSSHVendor."
            )

        if ssh_command:
            import shlex

            args = [*shlex.split(ssh_command, posix=sys.platform != "win32"), "-x"]
        else:
            args = ["ssh", "-x"]

        if port:
            args.extend(["-p", str(port)])

        if key_filename:
            args.extend(["-i", str(key_filename)])

        if protocol_version is None:
            protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_FETCH
        if protocol_version > 0:
            args.extend(["-o", f"SetEnv GIT_PROTOCOL=version={protocol_version}"])

        if username:
            host = f"{username}@{host}"
        if host.startswith("-"):
            raise StrangeHostname(hostname=host)
        args.append(host)

        proc = subprocess.Popen(
            [*args, command],
            bufsize=0,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return SubprocessWrapper(proc)


class PLinkSSHVendor(SSHVendor):
    """SSH vendor that shells out to the local 'plink' command."""

    def run_command(
        self,
        host: str,
        command: bytes,
        username: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        key_filename: Optional[str] = None,
        ssh_command: Optional[str] = None,
        protocol_version: Optional[int] = None,
    ) -> SubprocessWrapper:
        """Run a git command over SSH using PLink.

        Args:
            host: SSH host to connect to
            command: Git command to run
            username: Optional username
            port: Optional port number
            password: Optional password
            key_filename: Optional SSH key file
            ssh_command: Optional custom SSH command
            protocol_version: Optional Git protocol version

        Returns:
            Tuple of (subprocess.Popen, Protocol, stderr_stream)
        """
        if ssh_command:
            import shlex

            args = [*shlex.split(ssh_command, posix=sys.platform != "win32"), "-ssh"]
        elif sys.platform == "win32":
            args = ["plink.exe", "-ssh"]
        else:
            args = ["plink", "-ssh"]

        if password is not None:
            import warnings

            warnings.warn(
                "Invoking PLink with a password exposes the password in the "
                "process list."
            )
            args.extend(["-pw", str(password)])

        if port:
            args.extend(["-P", str(port)])

        if key_filename:
            args.extend(["-i", str(key_filename)])

        if username:
            host = f"{username}@{host}"
        if host.startswith("-"):
            raise StrangeHostname(hostname=host)
        args.append(host)

        # plink.exe does not provide a way to pass environment variables
        # via the command line. The best we can do is set an environment
        # variable and hope that plink will pass it to the server. If this
        # does not work then the server should behave as if we had requested
        # protocol version 0.
        env = copy.deepcopy(os.environ)
        if protocol_version is None:
            protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_FETCH
        if protocol_version > 0:
            env["GIT_PROTOCOL"] = f"version={protocol_version}"

        proc = subprocess.Popen(
            [*args, command],
            bufsize=0,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        return SubprocessWrapper(proc)


# Can be overridden by users
get_ssh_vendor: Callable[[], SSHVendor] = SubprocessSSHVendor


class SSHGitClient(TraditionalGitClient):
    """Git client that connects over SSH."""

    def __init__(
        self,
        host: str,
        port: Optional[int] = None,
        username: Optional[str] = None,
        vendor: Optional[SSHVendor] = None,
        config: Optional[Config] = None,
        password: Optional[str] = None,
        key_filename: Optional[str] = None,
        ssh_command: Optional[str] = None,
        path_encoding: str = TraditionalGitClient.DEFAULT_ENCODING,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Initialize SSHGitClient.

        Args:
            host: SSH hostname
            port: Optional SSH port
            username: Optional username
            vendor: Optional SSH vendor
            config: Optional configuration
            password: Optional password
            key_filename: Optional SSH key file
            ssh_command: Optional custom SSH command
            path_encoding: Encoding for paths (default: utf-8)
            thin_packs: Whether or not thin packs should be retrieved
            report_activity: Optional callback for reporting transport activity
            quiet: Whether to suppress output
            include_tags: Send annotated tags when sending the objects they point to
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_filename = key_filename
        # Priority: ssh_command parameter, then env vars, then core.sshCommand config
        if ssh_command:
            self.ssh_command = ssh_command
        else:
            # Check environment variables first
            env_ssh_command = os.environ.get("GIT_SSH_COMMAND")
            if env_ssh_command:
                self.ssh_command = env_ssh_command
            else:
                env_ssh = os.environ.get("GIT_SSH")
                if env_ssh:
                    self.ssh_command = env_ssh
                else:
                    # Fall back to config if no environment variable set
                    if config is not None:
                        try:
                            config_ssh_command = config.get((b"core",), b"sshCommand")
                            self.ssh_command = (
                                config_ssh_command.decode()
                                if config_ssh_command
                                else "ssh"
                            )
                        except KeyError:
                            self.ssh_command = "ssh"
                    else:
                        self.ssh_command = "ssh"

        super().__init__(
            path_encoding=path_encoding,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )
        self.alternative_paths: dict[bytes, bytes] = {}
        if vendor is not None:
            self.ssh_vendor = vendor
        else:
            self.ssh_vendor = get_ssh_vendor()

    def get_url(self, path: str) -> str:
        """Get the SSH URL for a path."""
        netloc = self.host
        if self.port is not None:
            netloc += f":{self.port}"

        if self.username is not None:
            netloc = urlquote(self.username, "@/:") + "@" + netloc

        return urlunsplit(("ssh", netloc, path, "", ""))

    @classmethod
    def from_parsedurl(
        cls,
        parsedurl: ParseResult,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        dumb: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[Config] = None,
        path_encoding: str = TraditionalGitClient.DEFAULT_ENCODING,
        vendor: Optional[SSHVendor] = None,
    ) -> "SSHGitClient":
        """Create an SSHGitClient from a parsed URL.

        Args:
          parsedurl: Result of urlparse()
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
          dumb: Whether to use dumb protocol (not used for SSHGitClient)
          username: SSH username
          password: SSH password
          config: Configuration object
          path_encoding: Encoding for paths
          vendor: SSH implementation to use

        Returns:
          An SSHGitClient instance
        """
        if parsedurl.hostname is None:
            raise ValueError("SSH URL must have a hostname")
        return cls(
            host=parsedurl.hostname,
            port=parsedurl.port,
            username=username or parsedurl.username,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
            path_encoding=path_encoding,
            vendor=vendor,
            config=config,
            password=password,
        )

    def _get_cmd_path(self, cmd: bytes) -> bytes:
        cmd = self.alternative_paths.get(cmd, b"git-" + cmd)
        assert isinstance(cmd, bytes)
        return cmd

    def _connect(
        self,
        cmd: bytes,
        path: Union[str, bytes],
        protocol_version: Optional[int] = None,
    ) -> tuple[Protocol, Callable[[], bool], Optional[IO[bytes]]]:
        if not isinstance(cmd, bytes):
            raise TypeError(cmd)
        if isinstance(path, bytes):
            path = path.decode(self._remote_path_encoding)
        if path.startswith("/~"):
            path = path[1:]
        argv = (
            self._get_cmd_path(cmd)
            + b" '"
            + path.encode(self._remote_path_encoding)
            + b"'"
        )
        kwargs = {}
        if self.password is not None:
            kwargs["password"] = self.password
        if self.key_filename is not None:
            kwargs["key_filename"] = self.key_filename
        # GIT_SSH_COMMAND takes precedence over GIT_SSH
        if self.ssh_command is not None:
            kwargs["ssh_command"] = self.ssh_command
        con = self.ssh_vendor.run_command(
            self.host,
            argv,
            port=self.port,
            username=self.username,
            protocol_version=protocol_version,
            **kwargs,
        )
        return (
            Protocol(
                con.read,
                con.write,
                con.close,
                report_activity=self._report_activity,
            ),
            con.can_read,
            getattr(con, "stderr", None),
        )


def default_user_agent_string() -> str:
    """Return the default user agent string for Dulwich."""
    # Start user agent with "git/", because GitHub requires this. :-( See
    # https://github.com/jelmer/dulwich/issues/562 for details.
    return "git/dulwich/{}".format(".".join([str(x) for x in dulwich.__version__]))


def _urlmatch_http_sections(
    config: Config, url: Optional[str]
) -> Iterator[tuple[bytes, ...]]:
    """Yield http config sections matching the given URL, ordered by specificity.

    Yields sections from least specific to most specific, so callers can
    apply settings in order with more specific settings overriding less specific ones.

    Args:
      config: Git configuration object
      url: URL to match against config sections (if None, only yields global http section)

    Yields:
      Config section tuples that match the URL, ordered by specificity
    """
    encoding = getattr(config, "encoding", None) or sys.getdefaultencoding()
    parsed_url = urlparse(url) if url else None

    # Collect all matching sections with their specificity
    # (specificity is based on URL path length - longer = more specific)
    matching_sections: list[tuple[int, tuple[bytes, ...]]] = []

    for config_section in config.sections():
        if config_section[0] != b"http":
            continue

        if len(config_section) < 2:
            # Global http section (least specific)
            matching_sections.append((0, config_section))
        elif parsed_url is not None:
            # URL-specific http section - only match if we have a URL
            config_url = config_section[1].decode(encoding)
            parsed_config_url = urlparse(config_url)

            is_match = False
            if parsed_config_url.scheme and parsed_config_url.netloc:
                is_match = match_urls(parsed_url, parsed_config_url)
            else:
                is_match = match_partial_url(parsed_url, config_url)

            if is_match:
                # Calculate specificity based on URL path length
                specificity = len(parsed_config_url.path.rstrip("/"))
                matching_sections.append((specificity, config_section))

    # Sort by specificity (least specific first)
    matching_sections.sort(key=lambda x: x[0])

    for _, section in matching_sections:
        yield section


def default_urllib3_manager(
    config: Optional[Config],
    pool_manager_cls: Optional[type] = None,
    proxy_manager_cls: Optional[type] = None,
    base_url: Optional[str] = None,
    timeout: Optional[float] = None,
    cert_reqs: Optional[str] = None,
) -> Union["urllib3.ProxyManager", "urllib3.PoolManager"]:
    """Return urllib3 connection pool manager.

    Honour detected proxy configurations.

    Args:
      config: `dulwich.config.ConfigDict` instance with Git configuration.
      pool_manager_cls: Pool manager class to use
      proxy_manager_cls: Proxy manager class to use
      base_url: Base URL for proxy bypass checks
      timeout: Timeout for HTTP requests in seconds
      cert_reqs: SSL certificate requirements (e.g. "CERT_REQUIRED", "CERT_NONE")

    Returns:
      Either pool_manager_cls (defaults to `urllib3.ProxyManager`) instance for
      proxy configurations, proxy_manager_cls
      (defaults to `urllib3.PoolManager`) instance otherwise

    """
    proxy_server: Optional[str] = None
    user_agent: Optional[str] = None
    ca_certs: Optional[str] = None
    ssl_verify: Optional[bool] = None

    if proxy_server is None:
        for proxyname in ("https_proxy", "http_proxy", "all_proxy"):
            proxy_server = os.environ.get(proxyname)
            if proxy_server:
                break

    if proxy_server:
        if check_for_proxy_bypass(base_url):
            proxy_server = None

    if config is not None:
        # Iterate through all matching http sections from least to most specific
        # More specific settings will override less specific ones
        for section in _urlmatch_http_sections(config, base_url):
            if proxy_server is None:
                try:
                    proxy_server_bytes = config.get(section, b"proxy")
                except KeyError:
                    pass
                else:
                    if proxy_server_bytes is not None:
                        proxy_server = proxy_server_bytes.decode("utf-8")

            try:
                user_agent_bytes = config.get(section, b"useragent")
            except KeyError:
                pass
            else:
                if user_agent_bytes is not None:
                    user_agent = user_agent_bytes.decode("utf-8")

            try:
                ssl_verify_value = config.get_boolean(section, b"sslVerify")
            except KeyError:
                pass
            else:
                if ssl_verify_value is not None:
                    ssl_verify = ssl_verify_value

            try:
                ca_certs_bytes = config.get(section, b"sslCAInfo")
            except KeyError:
                pass
            else:
                if ca_certs_bytes is not None:
                    ca_certs = ca_certs_bytes.decode("utf-8")

            if timeout is None:
                try:
                    timeout_bytes = config.get(section, b"timeout")
                except KeyError:
                    pass
                else:
                    if timeout_bytes is not None:
                        timeout = float(timeout_bytes.decode("utf-8"))

        # Default ssl_verify to True if not set
        if ssl_verify is None:
            ssl_verify = True

    if user_agent is None:
        user_agent = default_user_agent_string()

    headers = {"User-agent": user_agent}

    # Check for extra headers in config with URL matching
    if config is not None:
        # Apply extra headers from least specific to most specific
        for section in _urlmatch_http_sections(config, base_url):
            try:
                extra_headers = config.get_multivar(section, b"extraHeader")
            except KeyError:
                continue

            for extra_header in extra_headers:
                if not extra_header:
                    logger.warning("Ignoring empty http.extraHeader value")
                    continue
                if b": " not in extra_header:
                    logger.warning(
                        "Ignoring invalid http.extraHeader value %r (missing ': ' separator)",
                        extra_header,
                    )
                    continue
                # Parse the header (format: "Header-Name: value")
                header_name, header_value = extra_header.split(b": ", 1)
                try:
                    headers[header_name.decode("utf-8")] = header_value.decode("utf-8")
                except UnicodeDecodeError as e:
                    logger.warning(
                        "Ignoring http.extraHeader with invalid UTF-8: %s", e
                    )

    kwargs: dict[str, Union[str, float, None]] = {
        "ca_certs": ca_certs,
    }

    # Add timeout if specified
    if timeout is not None:
        kwargs["timeout"] = timeout

    # Handle cert_reqs - allow override from parameter
    if cert_reqs is not None:
        kwargs["cert_reqs"] = cert_reqs
    elif ssl_verify is True:
        kwargs["cert_reqs"] = "CERT_REQUIRED"
    elif ssl_verify is False:
        kwargs["cert_reqs"] = "CERT_NONE"
    else:
        # Default to SSL verification
        kwargs["cert_reqs"] = "CERT_REQUIRED"

    import urllib3

    manager: Union[urllib3.ProxyManager, urllib3.PoolManager]
    if proxy_server is not None:
        if proxy_manager_cls is None:
            proxy_manager_cls = urllib3.ProxyManager
        if not isinstance(proxy_server, str):
            proxy_server = proxy_server.decode()
        proxy_server_url = urlparse(proxy_server)
        if proxy_server_url.username is not None:
            proxy_headers = urllib3.make_headers(
                proxy_basic_auth=f"{proxy_server_url.username}:{proxy_server_url.password or ''}"
            )
        else:
            proxy_headers = {}
        manager = proxy_manager_cls(
            proxy_server, proxy_headers=proxy_headers, headers=headers, **kwargs
        )
    else:
        if pool_manager_cls is None:
            pool_manager_cls = urllib3.PoolManager
        manager = pool_manager_cls(headers=headers, **kwargs)

    return manager


def check_for_proxy_bypass(base_url: Optional[str]) -> bool:
    """Check if proxy should be bypassed for the given URL."""
    # Check if a proxy bypass is defined with the no_proxy environment variable
    if base_url:  # only check if base_url is provided
        no_proxy_str = os.environ.get("no_proxy")
        if no_proxy_str:
            # implementation based on curl behavior: https://curl.se/libcurl/c/CURLOPT_NOPROXY.html
            # get hostname of provided parsed url
            parsed_url = urlparse(base_url)
            hostname = parsed_url.hostname

            if hostname:
                import ipaddress

                # check if hostname is an ip address
                try:
                    hostname_ip = ipaddress.ip_address(hostname)
                except ValueError:
                    hostname_ip = None

                no_proxy_values = no_proxy_str.split(",")
                for no_proxy_value in no_proxy_values:
                    no_proxy_value = no_proxy_value.strip()
                    if no_proxy_value:
                        no_proxy_value = no_proxy_value.lower()
                        no_proxy_value = no_proxy_value.lstrip(
                            "."
                        )  # ignore leading dots

                        if hostname_ip:
                            # check if no_proxy_value is a ip network
                            try:
                                no_proxy_value_network = ipaddress.ip_network(
                                    no_proxy_value, strict=False
                                )
                            except ValueError:
                                no_proxy_value_network = None
                            if no_proxy_value_network:
                                # if hostname is a ip address and no_proxy_value is a ip network -> check if ip address is part of network
                                if hostname_ip in no_proxy_value_network:
                                    return True

                        if no_proxy_value == "*":
                            # '*' is special case for always bypass proxy
                            return True
                        if hostname == no_proxy_value:
                            return True
                        no_proxy_value = (
                            "." + no_proxy_value
                        )  # add a dot to only match complete domains
                        if hostname.endswith(no_proxy_value):
                            return True
    return False


class AbstractHttpGitClient(GitClient):
    """Abstract base class for HTTP Git Clients.

    This is agonistic of the actual HTTP implementation.

    Subclasses should provide an implementation of the
    _http_request method.
    """

    def __init__(
        self,
        base_url: str,
        dumb: bool = False,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """Initialize AbstractHttpGitClient."""
        self._base_url = base_url.rstrip("/") + "/"
        self._username = username
        self._password = password
        # Track original URL with credentials (set by from_parsedurl when credentials come from URL)
        self._url_with_auth: Optional[str] = None
        self.dumb = dumb
        GitClient.__init__(
            self,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )

    def _http_request(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        data: Optional[Union[bytes, Iterator[bytes]]] = None,
        raise_for_status: bool = True,
    ) -> tuple["HTTPResponse", Callable[[int], bytes]]:
        """Perform HTTP request.

        Args:
          url: Request URL.
          headers: Optional custom headers to override defaults.
          data: Request data.
          raise_for_status: Whether to raise an exception for HTTP errors.

        Returns:
          Tuple (response, read), where response is an urllib3
          response object with additional content_type and
          redirect_location properties, and read is a consumable read
          method for the response data.

        Raises:
          GitProtocolError
        """
        raise NotImplementedError(self._http_request)

    def _discover_references(
        self,
        service: bytes,
        base_url: str,
        protocol_version: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
    ) -> tuple[
        dict[Ref, Optional[ObjectID]],
        set[bytes],
        str,
        dict[Ref, Ref],
        dict[Ref, ObjectID],
    ]:
        if (
            protocol_version is not None
            and protocol_version not in GIT_PROTOCOL_VERSIONS
        ):
            raise ValueError(f"unknown Git protocol version {protocol_version}")
        assert base_url[-1] == "/"
        tail = "info/refs"
        headers = {"Accept": "*/*"}
        if self.dumb is not True:
            tail += "?service={}".format(service.decode("ascii"))
            # Enable protocol v2 only when fetching, not when pushing.
            # Git does not yet implement push over protocol v2, and as of
            # git version 2.37.3 git-http-backend's behaviour is erratic if
            # we try: It responds with a Git-protocol-v1-style ref listing
            # which lacks the "001f# service=git-receive-pack" marker.
            if service == b"git-upload-pack":
                if protocol_version is None:
                    self.protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_FETCH
                else:
                    self.protocol_version = protocol_version
                if self.protocol_version == 2:
                    headers["Git-Protocol"] = "version=2"
            else:
                self.protocol_version = DEFAULT_GIT_PROTOCOL_VERSION_SEND
        url = urljoin(base_url, tail)
        resp, read = self._http_request(url, headers)

        if resp.redirect_location:
            # Something changed (redirect!), so let's update the base URL
            if not resp.redirect_location.endswith(tail):
                raise GitProtocolError(
                    f"Redirected from URL {url} to URL {resp.redirect_location} without {tail}"
                )
            base_url = urljoin(url, resp.redirect_location[: -len(tail)])

        try:
            self.dumb = resp.content_type is None or not resp.content_type.startswith(
                "application/x-git-"
            )
            if not self.dumb:

                def begin_protocol_v2(
                    proto: Protocol,
                ) -> tuple[set[bytes], Any, Callable[[int], bytes], Protocol]:
                    nonlocal ref_prefix
                    server_capabilities = read_server_capabilities(proto.read_pkt_seq())
                    if ref_prefix is None:
                        ref_prefix = DEFAULT_REF_PREFIX

                    pkts = [
                        b"symrefs",
                        b"peel",
                    ]
                    for prefix in ref_prefix:
                        pkts.append(b"ref-prefix " + prefix)

                    body = b"".join(
                        [pkt_line(b"command=ls-refs\n"), b"0001", pkt_seq(*pkts)]
                    )

                    resp, read = self._smart_request(
                        service.decode("ascii"), base_url, body
                    )
                    proto = Protocol(read, lambda data: None)
                    return server_capabilities, resp, read, proto

                proto = Protocol(read, lambda data: None)
                server_protocol_version = negotiate_protocol_version(proto)
                if server_protocol_version not in GIT_PROTOCOL_VERSIONS:
                    raise ValueError(
                        f"unknown Git protocol version {server_protocol_version} used by server"
                    )
                if protocol_version and server_protocol_version > protocol_version:
                    raise ValueError(
                        f"bad Git protocol version {server_protocol_version} used by server"
                    )
                self.protocol_version = server_protocol_version
                if self.protocol_version == 2:
                    server_capabilities, resp, read, proto = begin_protocol_v2(proto)
                    (refs, symrefs, peeled) = read_pkt_refs_v2(proto.read_pkt_seq())
                    return refs, server_capabilities, base_url, symrefs, peeled

                else:
                    try:
                        [pkt] = list(proto.read_pkt_seq())
                    except ValueError as exc:
                        raise GitProtocolError(
                            "unexpected number of packets received"
                        ) from exc
                    if pkt.rstrip(b"\n") != (b"# service=" + service):
                        raise GitProtocolError(
                            f"unexpected first line {pkt!r} from smart server"
                        )
                    # Github sends "version 2" after sending the service name.
                    # Try to negotiate protocol version 2 again.
                    server_protocol_version = negotiate_protocol_version(proto)
                    if server_protocol_version not in GIT_PROTOCOL_VERSIONS:
                        raise ValueError(
                            f"unknown Git protocol version {server_protocol_version} used by server"
                        )
                    if protocol_version and server_protocol_version > protocol_version:
                        raise ValueError(
                            f"bad Git protocol version {server_protocol_version} used by server"
                        )
                    self.protocol_version = server_protocol_version
                    if self.protocol_version == 2:
                        server_capabilities, resp, read, proto = begin_protocol_v2(
                            proto
                        )
                        (refs, symrefs, peeled) = read_pkt_refs_v2(proto.read_pkt_seq())
                    else:
                        (
                            refs_v1,
                            server_capabilities,
                        ) = read_pkt_refs_v1(proto.read_pkt_seq())
                        # Convert v1 refs to Optional type
                        refs = _to_optional_dict(refs_v1)
                        (refs, peeled) = split_peeled_refs(refs)
                        (symrefs, _agent) = _extract_symrefs_and_agent(
                            server_capabilities
                        )
                        if ref_prefix is not None:
                            refs = filter_ref_prefix(refs, ref_prefix)
                    return refs, server_capabilities, base_url, symrefs, peeled
            else:
                self.protocol_version = 0  # dumb servers only support protocol v0
                # Read all the response data
                data = b""
                while True:
                    chunk = read(4096)
                    if not chunk:
                        break
                    data += chunk
                from typing import Optional, cast

                info_refs = read_info_refs(BytesIO(data))
                (refs, peeled) = split_peeled_refs(
                    cast(dict[bytes, Optional[bytes]], info_refs)
                )
                if ref_prefix is not None:
                    refs = filter_ref_prefix(refs, ref_prefix)
                return refs, set(), base_url, {}, peeled
        finally:
            resp.close()

    def _smart_request(
        self, service: str, url: str, data: Union[bytes, Iterator[bytes]]
    ) -> tuple["HTTPResponse", Callable[[int], bytes]]:
        """Send a 'smart' HTTP request.

        This is a simple wrapper around _http_request that sets
        a couple of extra headers.
        """
        assert url[-1] == "/"
        url = urljoin(url, service)
        result_content_type = f"application/x-{service}-result"
        headers = {
            "Content-Type": f"application/x-{service}-request",
            "Accept": result_content_type,
        }
        if self.protocol_version == 2:
            headers["Git-Protocol"] = "version=2"
        if isinstance(data, bytes):
            headers["Content-Length"] = str(len(data))
        resp, read = self._http_request(url, headers, data)
        if (
            not resp.content_type
            or resp.content_type.split(";")[0] != result_content_type
        ):
            raise GitProtocolError(
                f"Invalid content-type from server: {resp.content_type}"
            )
        return resp, read

    def send_pack(
        self,
        path: Union[str, bytes],
        update_refs: Callable[[dict[bytes, bytes]], dict[bytes, bytes]],
        generate_pack_data: "GeneratePackDataFunc",
        progress: Optional[Callable[[bytes], None]] = None,
    ) -> SendPackResult:
        """Upload a pack to a remote repository.

        Args:
          path: Repository path (as bytestring or string)
          update_refs: Function to determine changes to remote refs.
            Receives dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
          generate_pack_data: Function that can return a tuple
            with number of elements and pack data to upload.
          progress: Optional progress function

        Returns:
          SendPackResult

        Raises:
          SendPackError: if server rejects the pack data

        """
        url = self._get_url(path)
        old_refs, server_capabilities, url, _symrefs, _peeled = (
            self._discover_references(b"git-receive-pack", url)
        )
        (
            negotiated_capabilities,
            agent,
        ) = self._negotiate_receive_pack_capabilities(server_capabilities)
        negotiated_capabilities.add(capability_agent())

        if CAPABILITY_REPORT_STATUS in negotiated_capabilities:
            self._report_status_parser = ReportStatusParser()

        # Assert that old_refs has no None values
        assert all(v is not None for v in old_refs.values()), (
            "old_refs should not contain None values"
        )
        old_refs_typed: dict[bytes, bytes] = old_refs  # type: ignore[assignment]
        new_refs = update_refs(dict(old_refs_typed))
        if new_refs is None:
            # Determine wants function is aborting the push.
            # Convert to Optional type for SendPackResult
            old_refs_optional: dict[bytes, Optional[bytes]] = old_refs
            return SendPackResult(old_refs_optional, agent=agent, ref_status={})
        if set(new_refs.items()).issubset(set(old_refs_typed.items())):
            # Convert to Optional type for SendPackResult
            return SendPackResult(
                _to_optional_dict(new_refs), agent=agent, ref_status={}
            )
        if self.dumb:
            raise NotImplementedError(self.fetch_pack)

        def body_generator() -> Iterator[bytes]:
            header_handler = _v1ReceivePackHeader(
                list(negotiated_capabilities), old_refs_typed, new_refs
            )
            for pkt in header_handler:
                yield pkt_line(pkt)
            pack_data_count, pack_data = generate_pack_data(
                header_handler.have,
                header_handler.want,
                ofs_delta=(CAPABILITY_OFS_DELTA in negotiated_capabilities),
                progress=progress,
            )
            if self._should_send_pack(new_refs):
                yield from PackChunkGenerator(pack_data_count, pack_data)

        resp, read = self._smart_request("git-receive-pack", url, data=body_generator())
        try:
            resp_proto = Protocol(read, lambda data: None)
            ref_status = self._handle_receive_pack_tail(
                resp_proto, negotiated_capabilities, progress
            )
            # Convert to Optional type for SendPackResult
            return SendPackResult(
                _to_optional_dict(new_refs), agent=agent, ref_status=ref_status
            )
        finally:
            resp.close()

    def fetch_pack(
        self,
        path: Union[str, bytes],
        determine_wants: "DetermineWantsFunc",
        graph_walker: GraphWalker,
        pack_data: Callable[[bytes], int],
        progress: Optional[Callable[[bytes], None]] = None,
        depth: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
        filter_spec: Optional[bytes] = None,
        protocol_version: Optional[int] = None,
        shallow_since: Optional[str] = None,
        shallow_exclude: Optional[list[str]] = None,
    ) -> FetchPackResult:
        """Retrieve a pack from a git smart server.

        Args:
          path: Path to fetch from
          determine_wants: Callback that returns list of commits to fetch
          graph_walker: Object with next() and ack().
          pack_data: Callback called for each bit of data in the pack
          progress: Callback for progress reports (strings)
          depth: Depth for request
          ref_prefix: List of prefixes of desired references, as a list of
            bytestrings. Filtering is done by the server if supported, and
            client side otherwise.
          filter_spec: A git-rev-list-style object filter spec, as bytestring.
            Only used if the server supports the Git protocol-v2 'filter'
            feature, and ignored otherwise.
          protocol_version: Desired Git protocol version. By default the highest
            mutually supported protocol version will be used.
          shallow_since: Deepen the history to include commits after this date
          shallow_exclude: Deepen the history to exclude commits reachable from these refs

        Returns:
          FetchPackResult object

        """
        url = self._get_url(path)
        refs, server_capabilities, url, symrefs, _peeled = self._discover_references(
            b"git-upload-pack",
            url,
            protocol_version=protocol_version,
            ref_prefix=ref_prefix,
        )
        (
            negotiated_capabilities,
            capa_symrefs,
            agent,
        ) = self._negotiate_upload_pack_capabilities(server_capabilities)
        if not symrefs and capa_symrefs:
            symrefs = capa_symrefs
        # Filter out None values from refs for determine_wants
        refs_filtered = {k: v for k, v in refs.items() if v is not None}
        if depth is not None:
            wants = determine_wants(refs_filtered, depth=depth)
        else:
            wants = determine_wants(refs_filtered)
        if wants is not None:
            wants = [cid for cid in wants if cid != ZERO_SHA]
        if not wants and not self.dumb:
            return FetchPackResult(refs, symrefs, agent)
        elif self.dumb:
            # Use dumb HTTP protocol
            from .dumb import DumbRemoteHTTPRepo

            # Pass http_request function
            dumb_repo = DumbRemoteHTTPRepo(
                url, functools.partial(self._http_request, raise_for_status=False)
            )

            # Fetch pack data from dumb remote
            pack_data_list = list(
                dumb_repo.fetch_pack_data(
                    lambda refs, depth: wants,
                    graph_walker,
                    progress=progress,
                    depth=depth,
                )
            )

            symrefs[b"HEAD"] = dumb_repo.get_head()

            # Write pack data
            if pack_data_list:
                from .pack import write_pack_data

                # Wrap pack_data to match expected signature
                def write_fn(data: bytes) -> None:
                    pack_data(data)

                # Write pack data directly using the unpacked objects
                write_pack_data(
                    write_fn,
                    iter(pack_data_list),
                    num_records=len(pack_data_list),
                    progress=progress,
                )

            return FetchPackResult(refs, symrefs, agent)
        req_data = BytesIO()
        req_proto = Protocol(None, req_data.write)  # type: ignore
        (new_shallow, new_unshallow) = _handle_upload_pack_head(
            req_proto,
            negotiated_capabilities,
            graph_walker,
            wants,
            can_read=None,
            depth=depth,
            protocol_version=self.protocol_version,
            shallow_since=shallow_since,
            shallow_exclude=shallow_exclude,
        )
        if self.protocol_version == 2:
            data = pkt_line(b"command=fetch\n") + b"0001"
            if CAPABILITY_THIN_PACK in self._fetch_capabilities:
                data += pkt_line(b"thin-pack\n")
            if (
                find_capability(
                    negotiated_capabilities, CAPABILITY_FETCH, CAPABILITY_FILTER
                )
                and filter_spec
            ):
                data += pkt_line(b"filter %s\n" % filter_spec)
            elif filter_spec:
                self._warn_filter_objects()
            data += req_data.getvalue()
        else:
            if filter_spec:
                self._warn_filter_objects()
            data = req_data.getvalue()
        resp, read = self._smart_request("git-upload-pack", url, data)
        try:
            resp_proto = Protocol(read, None)  # type: ignore
            if new_shallow is None and new_unshallow is None:
                (new_shallow, new_unshallow) = _read_shallow_updates(
                    resp_proto.read_pkt_seq()
                )
            _handle_upload_pack_tail(
                resp_proto,
                negotiated_capabilities,
                graph_walker,
                pack_data,
                progress,
                protocol_version=self.protocol_version,
            )
            return FetchPackResult(refs, symrefs, agent, new_shallow, new_unshallow)
        finally:
            resp.close()

    def get_refs(
        self,
        path: Union[str, bytes],
        protocol_version: Optional[int] = None,
        ref_prefix: Optional[Sequence[Ref]] = None,
    ) -> LsRemoteResult:
        """Retrieve the current refs from a git smart server."""
        url = self._get_url(path)
        refs, _, _, symrefs, peeled = self._discover_references(
            b"git-upload-pack",
            url,
            protocol_version=protocol_version,
            ref_prefix=ref_prefix,
        )
        for refname, refvalue in peeled.items():
            refs[refname + PEELED_TAG_SUFFIX] = refvalue
        return LsRemoteResult(refs, symrefs)

    def get_url(self, path: str) -> str:
        """Get the HTTP URL for a path."""
        url = self._get_url(path).rstrip("/")

        # Include credentials in the URL only if they came from a URL (not passed explicitly)
        # This preserves credentials that were in the original URL for git config storage
        if self._url_with_auth is not None:
            from urllib.parse import quote, urlparse, urlunparse

            assert self._username is not None
            parsed = urlparse(url)
            # Construct netloc with credentials
            if self._password is not None:
                netloc = f"{quote(self._username, safe='')}:{quote(self._password, safe='')}@{parsed.hostname}"
            else:
                netloc = f"{quote(self._username, safe='')}@{parsed.hostname}"

            if parsed.port:
                netloc += f":{parsed.port}"

            # Reconstruct URL with credentials
            url = urlunparse(
                (
                    parsed.scheme,
                    netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                )
            )

        return url

    def _get_url(self, path: Union[str, bytes]) -> str:
        path_str = path if isinstance(path, str) else path.decode("utf-8")
        return urljoin(self._base_url, path_str).rstrip("/") + "/"

    @classmethod
    def from_parsedurl(
        cls,
        parsedurl: ParseResult,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
        dumb: bool = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        config: Optional[Config] = None,
        pool_manager: Optional["urllib3.PoolManager"] = None,
    ) -> "AbstractHttpGitClient":
        """Create an AbstractHttpGitClient from a parsed URL.

        Args:
          parsedurl: Result of urlparse()
          thin_packs: Whether or not thin packs should be retrieved
          report_activity: Optional callback for reporting transport activity
          quiet: Whether to suppress progress output
          include_tags: Whether to include tags
          dumb: Whether to use dumb HTTP transport
          username: Optional username for authentication
          password: Optional password for authentication
          config: Configuration object
          pool_manager: Optional urllib3 PoolManager for HTTP(S) connections

        Returns:
          An AbstractHttpGitClient instance
        """
        # Extract credentials from URL if present
        # ParseResult.username and .password are URL-encoded, need to unquote them
        from urllib.parse import unquote

        url_username = unquote(parsedurl.username) if parsedurl.username else None
        url_password = unquote(parsedurl.password) if parsedurl.password else None

        # Explicit parameters take precedence over URL credentials
        final_username = username if username is not None else url_username
        final_password = password if password is not None else url_password

        # Remove credentials from URL for base_url
        hostname = parsedurl.hostname or ""
        base_parsed = parsedurl._replace(netloc=hostname)
        if parsedurl.port:
            base_parsed = base_parsed._replace(netloc=f"{hostname}:{parsedurl.port}")

        # Pass credentials to constructor if it's a subclass that supports them
        if cls is Urllib3HttpGitClient:
            client = cls(  # type: ignore[call-arg]
                urlunparse(base_parsed),
                dumb=dumb,
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
                username=final_username,
                password=final_password,
                config=config,
                pool_manager=pool_manager,
            )
        else:
            # Base class now supports credentials in constructor
            client = cls(
                urlunparse(base_parsed),
                dumb=dumb,
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
                username=final_username,
                password=final_password,
            )

        # Mark that credentials came from URL (not passed explicitly) if URL had credentials
        if url_username is not None or url_password is not None:
            client._url_with_auth = urlunparse(parsedurl)

        return client

    def __repr__(self) -> str:
        """Return string representation of this client."""
        return f"{type(self).__name__}({self._base_url!r}, dumb={self.dumb!r})"


def _wrap_urllib3_exceptions(
    func: Callable[..., bytes],
) -> Callable[..., bytes]:
    from urllib3.exceptions import ProtocolError

    def wrapper(*args: object, **kwargs: object) -> bytes:
        try:
            return func(*args, **kwargs)
        except ProtocolError as error:
            raise GitProtocolError(str(error)) from error

    return wrapper


class Urllib3HttpGitClient(AbstractHttpGitClient):
    """Git client that uses urllib3 for HTTP(S) connections."""

    def __init__(
        self,
        base_url: str,
        dumb: Optional[bool] = None,
        pool_manager: Optional["urllib3.PoolManager"] = None,
        config: Optional[Config] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: Optional[float] = None,
        extra_headers: Optional[dict[str, str]] = None,
        thin_packs: bool = True,
        report_activity: Optional[Callable[[int, str], None]] = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Initialize Urllib3HttpGitClient."""
        self._timeout = timeout
        self._extra_headers = extra_headers or {}

        if pool_manager is None:
            self.pool_manager = default_urllib3_manager(
                config, base_url=base_url, timeout=timeout
            )
        else:
            self.pool_manager = pool_manager

        if username is not None:
            # No escaping needed: ":" is not allowed in username:
            # https://tools.ietf.org/html/rfc2617#section-2
            credentials = f"{username}:{password or ''}"
            import urllib3.util

            basic_auth = urllib3.util.make_headers(basic_auth=credentials)
            self.pool_manager.headers.update(basic_auth)  # type: ignore

        self.config = config

        super().__init__(
            base_url=base_url,
            dumb=dumb if dumb is not None else False,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
            username=username,
            password=password,
        )

    def _get_url(self, path: Union[str, bytes]) -> str:
        if not isinstance(path, str):
            # urllib3.util.url._encode_invalid_chars() converts the path back
            # to bytes using the utf-8 codec.
            path = path.decode("utf-8")
        return urljoin(self._base_url, path).rstrip("/") + "/"

    def _http_request(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        data: Optional[Union[bytes, Iterator[bytes]]] = None,
        raise_for_status: bool = True,
    ) -> tuple["HTTPResponse", Callable[[int], bytes]]:
        import urllib3.exceptions

        req_headers = dict(self.pool_manager.headers)
        if headers is not None:
            req_headers.update(headers)
        req_headers["Pragma"] = "no-cache"

        try:
            request_kwargs = {
                "headers": req_headers,
                "preload_content": False,
            }
            if self._timeout is not None:
                request_kwargs["timeout"] = self._timeout

            if data is None:
                resp = self.pool_manager.request("GET", url, **request_kwargs)  # type: ignore[arg-type]
            else:
                request_kwargs["body"] = data
                resp = self.pool_manager.request("POST", url, **request_kwargs)  # type: ignore[arg-type]
        except urllib3.exceptions.HTTPError as e:
            raise GitProtocolError(str(e)) from e

        if raise_for_status:
            if resp.status == 404:
                raise NotGitRepository
            if resp.status == 401:
                raise HTTPUnauthorized(resp.headers.get("WWW-Authenticate"), url)
            if resp.status == 407:
                raise HTTPProxyUnauthorized(resp.headers.get("Proxy-Authenticate"), url)
            if resp.status != 200:
                raise GitProtocolError(f"unexpected http resp {resp.status} for {url}")

        resp.content_type = resp.headers.get("Content-Type")  # type: ignore[attr-defined]
        # Check if geturl() is available (urllib3 version >= 1.23)
        try:
            resp_url = resp.geturl()
        except AttributeError:
            # get_redirect_location() is available for urllib3 >= 1.1
            resp.redirect_location = resp.get_redirect_location()  # type: ignore[attr-defined]
        else:
            resp.redirect_location = resp_url if resp_url != url else ""  # type: ignore[attr-defined]
        return resp, _wrap_urllib3_exceptions(resp.read)  # type: ignore[return-value]


HttpGitClient = Urllib3HttpGitClient


def _win32_url_to_path(parsed: ParseResult) -> str:
    """Convert a file: URL to a path.

    https://datatracker.ietf.org/doc/html/rfc8089
    """
    assert parsed.scheme == "file"

    _, netloc, path, _, _, _ = parsed

    if netloc == "localhost" or not netloc:
        netloc = ""
    elif (
        netloc
        and len(netloc) >= 2
        and netloc[0].isalpha()
        and netloc[1:2] in (":", ":/")
    ):
        # file://C:/foo.bar/baz or file://C://foo.bar//baz
        netloc = netloc[:2]
    else:
        raise NotImplementedError("Non-local file URLs are not supported")

    from nturl2path import url2pathname

    return url2pathname(netloc + path)


def get_transport_and_path_from_url(
    url: str,
    config: Optional[Config] = None,
    operation: Optional[str] = None,
    thin_packs: bool = True,
    report_activity: Optional[Callable[[int, str], None]] = None,
    quiet: bool = False,
    include_tags: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
    key_filename: Optional[str] = None,
    ssh_command: Optional[str] = None,
    pool_manager: Optional["urllib3.PoolManager"] = None,
) -> tuple[GitClient, str]:
    """Obtain a git client from a URL.

    Args:
      url: URL to open (a unicode string)
      config: Optional config object
      operation: Kind of operation that'll be performed; "pull" or "push"
      thin_packs: Whether or not thin packs should be retrieved
      report_activity: Optional callback for reporting transport activity
      quiet: Whether to suppress output
      include_tags: Send annotated tags when sending the objects they point to
      username: Optional username for authentication
      password: Optional password for authentication
      key_filename: Optional SSH key file
      ssh_command: Optional custom SSH command
      pool_manager: Optional urllib3 PoolManager for HTTP(S) connections

    Returns:
      Tuple with client instance and relative path.

    """
    if config is not None:
        url = apply_instead_of(config, url, push=(operation == "push"))

    return _get_transport_and_path_from_url(
        url,
        config=config,
        operation=operation,
        thin_packs=thin_packs,
        report_activity=report_activity,
        quiet=quiet,
        include_tags=include_tags,
        username=username,
        password=password,
        key_filename=key_filename,
        ssh_command=ssh_command,
        pool_manager=pool_manager,
    )


def _get_transport_and_path_from_url(
    url: str,
    config: Optional[Config],
    operation: Optional[str],
    thin_packs: bool = True,
    report_activity: Optional[Callable[[int, str], None]] = None,
    quiet: bool = False,
    include_tags: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
    key_filename: Optional[str] = None,
    ssh_command: Optional[str] = None,
    pool_manager: Optional["urllib3.PoolManager"] = None,
) -> tuple[GitClient, str]:
    parsed = urlparse(url)
    if parsed.scheme == "git":
        return (
            TCPGitClient.from_parsedurl(
                parsed,
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
            ),
            parsed.path,
        )
    elif parsed.scheme in ("git+ssh", "ssh"):
        return SSHGitClient.from_parsedurl(
            parsed,
            config=config,
            username=username,
            password=password,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        ), parsed.path
    elif parsed.scheme in ("http", "https"):
        return (
            HttpGitClient.from_parsedurl(
                parsed,
                config=config,
                username=username,
                password=password,
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
                pool_manager=pool_manager,
            ),
            parsed.path,
        )
    elif parsed.scheme == "file":
        if sys.platform == "win32" or os.name == "nt":
            return default_local_git_client_cls(
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
            ), _win32_url_to_path(parsed)
        return (
            default_local_git_client_cls.from_parsedurl(
                parsed,
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
            ),
            parsed.path,
        )

    raise ValueError(f"unknown scheme '{parsed.scheme}'")


def parse_rsync_url(location: str) -> tuple[Optional[str], str, str]:
    """Parse a rsync-style URL."""
    if ":" in location and "@" not in location:
        # SSH with no user@, zero or one leading slash.
        (host, path) = location.split(":", 1)
        user = None
    elif ":" in location:
        # SSH with user@host:foo.
        user_host, path = location.split(":", 1)
        if "@" in user_host:
            user, host = user_host.rsplit("@", 1)
        else:
            user = None
            host = user_host
    else:
        raise ValueError("not a valid rsync-style URL")
    return (user, host, path)


def get_transport_and_path(
    location: str,
    config: Optional[Config] = None,
    operation: Optional[str] = None,
    thin_packs: bool = True,
    report_activity: Optional[Callable[[int, str], None]] = None,
    quiet: bool = False,
    include_tags: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
    key_filename: Optional[str] = None,
    ssh_command: Optional[str] = None,
    pool_manager: Optional["urllib3.PoolManager"] = None,
) -> tuple[GitClient, str]:
    """Obtain a git client from a URL.

    Args:
      location: URL or path (a string)
      config: Optional config object
      operation: Kind of operation that'll be performed; "pull" or "push"
      thin_packs: Whether or not thin packs should be retrieved
      report_activity: Optional callback for reporting transport activity
      quiet: Whether to suppress output
      include_tags: Send annotated tags when sending the objects they point to
      username: Optional username for authentication
      password: Optional password for authentication
      key_filename: Optional SSH key file
      ssh_command: Optional custom SSH command
      pool_manager: Optional urllib3 PoolManager for HTTP(S) connections

    Returns:
      Tuple with client instance and relative path.

    """
    if config is not None:
        location = apply_instead_of(config, location, push=(operation == "push"))

    # First, try to parse it as a URL
    try:
        return _get_transport_and_path_from_url(
            location,
            config=config,
            operation=operation,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
            username=username,
            password=password,
            key_filename=key_filename,
            ssh_command=ssh_command,
            pool_manager=pool_manager,
        )
    except ValueError:
        pass

    if sys.platform == "win32" and location[0].isalpha() and location[1:3] == ":\\":
        # Windows local path - but check if it's a bundle file first
        if BundleClient._is_bundle_file(location):
            return BundleClient(
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
            ), location
        return default_local_git_client_cls(
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        ), location

    try:
        (rsync_username, hostname, path) = parse_rsync_url(location)
    except ValueError:
        # Check if it's a bundle file before assuming it's a local path
        if BundleClient._is_bundle_file(location):
            return BundleClient(
                thin_packs=thin_packs,
                report_activity=report_activity,
                quiet=quiet,
                include_tags=include_tags,
            ), location
        # Otherwise, assume it's a local path.
        return default_local_git_client_cls(
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        ), location
    else:
        return SSHGitClient(
            hostname,
            username=rsync_username or username,
            config=config,
            password=password,
            key_filename=key_filename,
            ssh_command=ssh_command,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        ), path


DEFAULT_GIT_CREDENTIALS_PATHS = [
    os.path.expanduser("~/.git-credentials"),
    get_xdg_config_home_path("git", "credentials"),
]


def get_credentials_from_store(
    scheme: str,
    hostname: str,
    username: Optional[str] = None,
    fnames: list[str] = DEFAULT_GIT_CREDENTIALS_PATHS,
) -> Iterator[tuple[str, str]]:
    """Read credentials from a Git credential store."""
    for fname in fnames:
        try:
            with open(fname, "rb") as f:
                for line in f:
                    line_str = line.strip().decode("utf-8")
                    parsed_line = urlparse(line_str)
                    if (
                        parsed_line.scheme == scheme
                        and parsed_line.hostname == hostname
                        and (username is None or parsed_line.username == username)
                    ):
                        if parsed_line.username and parsed_line.password:
                            yield parsed_line.username, parsed_line.password
        except FileNotFoundError:
            # If the file doesn't exist, try the next one.
            continue
