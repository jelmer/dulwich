# client.py -- Implementation of the client side git protocols
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@jelmer.uk>
#
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

Known capabilities that are not supported:

 * shallow
 * no-progress
 * include-tag
"""

from contextlib import closing
from io import BytesIO, BufferedReader
import select
import socket
import subprocess
import sys

try:
    from urllib import quote as urlquote
    from urllib import unquote as urlunquote
except ImportError:
    from urllib.parse import quote as urlquote
    from urllib.parse import unquote as urlunquote

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

import dulwich
from dulwich.errors import (
    GitProtocolError,
    NotGitRepository,
    SendPackError,
    UpdateRefsError,
    )
from dulwich.protocol import (
    HangupException,
    _RBUFSIZE,
    agent_string,
    capability_agent,
    extract_capability_names,
    CAPABILITY_AGENT,
    CAPABILITY_DELETE_REFS,
    CAPABILITY_MULTI_ACK,
    CAPABILITY_MULTI_ACK_DETAILED,
    CAPABILITY_OFS_DELTA,
    CAPABILITY_QUIET,
    CAPABILITY_REPORT_STATUS,
    CAPABILITY_SHALLOW,
    CAPABILITY_SYMREF,
    CAPABILITY_SIDE_BAND_64K,
    CAPABILITY_THIN_PACK,
    CAPABILITIES_REF,
    KNOWN_RECEIVE_CAPABILITIES,
    KNOWN_UPLOAD_CAPABILITIES,
    COMMAND_DEEPEN,
    COMMAND_SHALLOW,
    COMMAND_UNSHALLOW,
    COMMAND_DONE,
    COMMAND_HAVE,
    COMMAND_WANT,
    SIDE_BAND_CHANNEL_DATA,
    SIDE_BAND_CHANNEL_PROGRESS,
    SIDE_BAND_CHANNEL_FATAL,
    PktLineParser,
    Protocol,
    ProtocolFile,
    TCP_GIT_PORT,
    ZERO_SHA,
    extract_capabilities,
    parse_capability,
    )
from dulwich.pack import (
    write_pack_data,
    write_pack_objects,
    )
from dulwich.refs import (
    read_info_refs,
    ANNOTATED_TAG_SUFFIX,
    )


class InvalidWants(Exception):
    """Invalid wants."""

    def __init__(self, wants):
        Exception.__init__(
            self,
            "requested wants not in server provided refs: %r" % wants)


def _fileno_can_read(fileno):
    """Check if a file descriptor is readable."""
    return len(select.select([fileno], [], [], 0)[0]) > 0


def _win32_peek_avail(handle):
    """Wrapper around PeekNamedPipe to check how many bytes are available."""
    from ctypes import byref, wintypes, windll
    c_avail = wintypes.DWORD()
    c_message = wintypes.DWORD()
    success = windll.kernel32.PeekNamedPipe(
        handle, None, 0, None, byref(c_avail),
        byref(c_message))
    if not success:
        raise OSError(wintypes.GetLastError())
    return c_avail.value


COMMON_CAPABILITIES = [CAPABILITY_OFS_DELTA, CAPABILITY_SIDE_BAND_64K]
UPLOAD_CAPABILITIES = ([CAPABILITY_THIN_PACK, CAPABILITY_MULTI_ACK,
                        CAPABILITY_MULTI_ACK_DETAILED, CAPABILITY_SHALLOW]
                       + COMMON_CAPABILITIES)
RECEIVE_CAPABILITIES = [CAPABILITY_REPORT_STATUS] + COMMON_CAPABILITIES


class ReportStatusParser(object):
    """Handle status as reported by servers with 'report-status' capability.
    """

    def __init__(self):
        self._done = False
        self._pack_status = None
        self._ref_status_ok = True
        self._ref_statuses = []

    def check(self):
        """Check if there were any errors and, if so, raise exceptions.

        :raise SendPackError: Raised when the server could not unpack
        :raise UpdateRefsError: Raised when refs could not be updated
        """
        if self._pack_status not in (b'unpack ok', None):
            raise SendPackError(self._pack_status)
        if not self._ref_status_ok:
            ref_status = {}
            ok = set()
            for status in self._ref_statuses:
                if b' ' not in status:
                    # malformed response, move on to the next one
                    continue
                status, ref = status.split(b' ', 1)

                if status == b'ng':
                    if b' ' in ref:
                        ref, status = ref.split(b' ', 1)
                else:
                    ok.add(ref)
                ref_status[ref] = status
            # TODO(jelmer): don't assume encoding of refs is ascii.
            raise UpdateRefsError(', '.join([
                refname.decode('ascii') for refname in ref_status
                if refname not in ok]) +
                ' failed to update', ref_status=ref_status)

    def handle_packet(self, pkt):
        """Handle a packet.

        :raise GitProtocolError: Raised when packets are received after a
            flush packet.
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
            if not ref_status.startswith(b'ok '):
                self._ref_status_ok = False


def read_pkt_refs(proto):
    server_capabilities = None
    refs = {}
    # Receive refs from server
    for pkt in proto.read_pkt_seq():
        (sha, ref) = pkt.rstrip(b'\n').split(None, 1)
        if sha == b'ERR':
            raise GitProtocolError(ref.decode('utf-8', 'replace'))
        if server_capabilities is None:
            (ref, server_capabilities) = extract_capabilities(ref)
        refs[ref] = sha

    if len(refs) == 0:
        return {}, set([])
    if refs == {CAPABILITIES_REF: ZERO_SHA}:
        refs = {}
    return refs, set(server_capabilities)


class FetchPackResult(object):
    """Result of a fetch-pack operation.

    :var refs: Dictionary with all remote refs
    :var symrefs: Dictionary with remote symrefs
    :var agent: User agent string
    """

    _FORWARDED_ATTRS = [
            'clear', 'copy', 'fromkeys', 'get', 'has_key', 'items',
            'iteritems', 'iterkeys', 'itervalues', 'keys', 'pop', 'popitem',
            'setdefault', 'update', 'values', 'viewitems', 'viewkeys',
            'viewvalues']

    def __init__(self, refs, symrefs, agent, new_shallow=None,
                 new_unshallow=None):
        self.refs = refs
        self.symrefs = symrefs
        self.agent = agent
        self.new_shallow = new_shallow
        self.new_unshallow = new_unshallow

    def _warn_deprecated(self):
        import warnings
        warnings.warn(
            "Use FetchPackResult.refs instead.",
            DeprecationWarning, stacklevel=3)

    def __eq__(self, other):
        if isinstance(other, dict):
            self._warn_deprecated()
            return (self.refs == other)
        return (self.refs == other.refs and
                self.symrefs == other.symrefs and
                self.agent == other.agent)

    def __contains__(self, name):
        self._warn_deprecated()
        return name in self.refs

    def __getitem__(self, name):
        self._warn_deprecated()
        return self.refs[name]

    def __len__(self):
        self._warn_deprecated()
        return len(self.refs)

    def __iter__(self):
        self._warn_deprecated()
        return iter(self.refs)

    def __getattribute__(self, name):
        if name in type(self)._FORWARDED_ATTRS:
            self._warn_deprecated()
            return getattr(self.refs, name)
        return super(FetchPackResult, self).__getattribute__(name)

    def __repr__(self):
        return "%s(%r, %r, %r)" % (
                self.__class__.__name__, self.refs, self.symrefs, self.agent)


def _read_shallow_updates(proto):
    new_shallow = set()
    new_unshallow = set()
    for pkt in proto.read_pkt_seq():
        cmd, sha = pkt.split(b' ', 1)
        if cmd == COMMAND_SHALLOW:
            new_shallow.add(sha.strip())
        elif cmd == COMMAND_UNSHALLOW:
            new_unshallow.add(sha.strip())
        else:
            raise GitProtocolError('unknown command %s' % pkt)
    return (new_shallow, new_unshallow)


# TODO(durin42): this doesn't correctly degrade if the server doesn't
# support some capabilities. This should work properly with servers
# that don't support multi_ack.
class GitClient(object):
    """Git smart server client.

    """

    def __init__(self, thin_packs=True, report_activity=None, quiet=False):
        """Create a new GitClient instance.

        :param thin_packs: Whether or not thin packs should be retrieved
        :param report_activity: Optional callback for reporting transport
            activity.
        """
        self._report_activity = report_activity
        self._report_status_parser = None
        self._fetch_capabilities = set(UPLOAD_CAPABILITIES)
        self._fetch_capabilities.add(capability_agent())
        self._send_capabilities = set(RECEIVE_CAPABILITIES)
        self._send_capabilities.add(capability_agent())
        if quiet:
            self._send_capabilities.add(CAPABILITY_QUIET)
        if not thin_packs:
            self._fetch_capabilities.remove(CAPABILITY_THIN_PACK)

    def get_url(self, path):
        """Retrieves full url to given path.

        :param path: Repository path (as string)
        :return: Url to path (as string)
        """
        raise NotImplementedError(self.get_url)

    @classmethod
    def from_parsedurl(cls, parsedurl, **kwargs):
        """Create an instance of this client from a urlparse.parsed object.

        :param parsedurl: Result of urlparse.urlparse()
        :return: A `GitClient` object
        """
        raise NotImplementedError(cls.from_parsedurl)

    def send_pack(self, path, update_refs, generate_pack_data,
                  progress=None):
        """Upload a pack to a remote repository.

        :param path: Repository path (as bytestring)
        :param update_refs: Function to determine changes to remote refs.
            Receive dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
        :param generate_pack_data: Function that can return a tuple
            with number of objects and list of pack data to include
        :param progress: Optional progress function

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        :return: new_refs dictionary containing the changes that were made
            {refname: new_ref}, including deleted refs.
        """
        raise NotImplementedError(self.send_pack)

    def fetch(self, path, target, determine_wants=None, progress=None,
              depth=None):
        """Fetch into a target repository.

        :param path: Path to fetch from (as bytestring)
        :param target: Target repository to fetch into
        :param determine_wants: Optional function to determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch. Defaults to all shas.
        :param progress: Optional progress function
        :param depth: Depth to fetch at
        :return: Dictionary with all remote refs (not just those fetched)
        """
        if determine_wants is None:
            determine_wants = target.object_store.determine_wants_all
        if CAPABILITY_THIN_PACK in self._fetch_capabilities:
            # TODO(jelmer): Avoid reading entire file into memory and
            # only processing it after the whole file has been fetched.
            f = BytesIO()

            def commit():
                if f.tell():
                    f.seek(0)
                    target.object_store.add_thin_pack(f.read, None)

            def abort():
                pass
        else:
            f, commit, abort = target.object_store.add_pack()
        try:
            result = self.fetch_pack(
                path, determine_wants, target.get_graph_walker(), f.write,
                progress=progress, depth=depth)
        except BaseException:
            abort()
            raise
        else:
            commit()
        target.update_shallow(result.new_shallow, result.new_unshallow)
        return result

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress=None, depth=None):
        """Retrieve a pack from a git smart server.

        :param path: Remote path to fetch from
        :param determine_wants: Function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch.
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        :param depth: Shallow fetch depth
        :return: FetchPackResult object
        """
        raise NotImplementedError(self.fetch_pack)

    def get_refs(self, path):
        """Retrieve the current refs from a git smart server.

        :param path: Path to the repo to fetch from. (as bytestring)
        """
        raise NotImplementedError(self.get_refs)

    def _parse_status_report(self, proto):
        unpack = proto.read_pkt_line().strip()
        if unpack != b'unpack ok':
            st = True
            # flush remaining error data
            while st is not None:
                st = proto.read_pkt_line()
            raise SendPackError(unpack)
        statuses = []
        errs = False
        ref_status = proto.read_pkt_line()
        while ref_status:
            ref_status = ref_status.strip()
            statuses.append(ref_status)
            if not ref_status.startswith(b'ok '):
                errs = True
            ref_status = proto.read_pkt_line()

        if errs:
            ref_status = {}
            ok = set()
            for status in statuses:
                if b' ' not in status:
                    # malformed response, move on to the next one
                    continue
                status, ref = status.split(b' ', 1)

                if status == b'ng':
                    if b' ' in ref:
                        ref, status = ref.split(b' ', 1)
                else:
                    ok.add(ref)
                ref_status[ref] = status
            raise UpdateRefsError(', '.join([
                refname for refname in ref_status if refname not in ok]) +
                b' failed to update', ref_status=ref_status)

    def _read_side_band64k_data(self, proto, channel_callbacks):
        """Read per-channel data.

        This requires the side-band-64k capability.

        :param proto: Protocol object to read from
        :param channel_callbacks: Dictionary mapping channels to packet
            handlers to use. None for a callback discards channel data.
        """
        for pkt in proto.read_pkt_seq():
            channel = ord(pkt[:1])
            pkt = pkt[1:]
            try:
                cb = channel_callbacks[channel]
            except KeyError:
                raise AssertionError('Invalid sideband channel %d' % channel)
            else:
                if cb is not None:
                    cb(pkt)

    def _handle_receive_pack_head(self, proto, capabilities, old_refs,
                                  new_refs):
        """Handle the head of a 'git-receive-pack' request.

        :param proto: Protocol object to read from
        :param capabilities: List of negotiated capabilities
        :param old_refs: Old refs, as received from the server
        :param new_refs: Refs to change
        :return: (have, want) tuple
        """
        want = []
        have = [x for x in old_refs.values() if not x == ZERO_SHA]
        sent_capabilities = False

        for refname in new_refs:
            if not isinstance(refname, bytes):
                raise TypeError('refname is not a bytestring: %r' % refname)
            old_sha1 = old_refs.get(refname, ZERO_SHA)
            if not isinstance(old_sha1, bytes):
                raise TypeError('old sha1 for %s is not a bytestring: %r' %
                                (refname, old_sha1))
            new_sha1 = new_refs.get(refname, ZERO_SHA)
            if not isinstance(new_sha1, bytes):
                raise TypeError('old sha1 for %s is not a bytestring %r' %
                                (refname, new_sha1))

            if old_sha1 != new_sha1:
                if sent_capabilities:
                    proto.write_pkt_line(old_sha1 + b' ' + new_sha1 + b' ' +
                                         refname)
                else:
                    proto.write_pkt_line(
                        old_sha1 + b' ' + new_sha1 + b' ' + refname + b'\0' +
                        b' '.join(capabilities))
                    sent_capabilities = True
            if new_sha1 not in have and new_sha1 != ZERO_SHA:
                want.append(new_sha1)
        proto.write_pkt_line(None)
        return (have, want)

    def _negotiate_receive_pack_capabilities(self, server_capabilities):
        negotiated_capabilities = (
            self._send_capabilities & server_capabilities)
        unknown_capabilities = (  # noqa: F841
            extract_capability_names(server_capabilities) -
            KNOWN_RECEIVE_CAPABILITIES)
        # TODO(jelmer): warn about unknown capabilities
        return negotiated_capabilities

    def _handle_receive_pack_tail(self, proto, capabilities, progress=None):
        """Handle the tail of a 'git-receive-pack' request.

        :param proto: Protocol object to read from
        :param capabilities: List of negotiated capabilities
        :param progress: Optional progress reporting function
        """
        if CAPABILITY_SIDE_BAND_64K in capabilities:
            if progress is None:
                def progress(x):
                    pass
            channel_callbacks = {2: progress}
            if CAPABILITY_REPORT_STATUS in capabilities:
                channel_callbacks[1] = PktLineParser(
                    self._report_status_parser.handle_packet).parse
            self._read_side_band64k_data(proto, channel_callbacks)
        else:
            if CAPABILITY_REPORT_STATUS in capabilities:
                for pkt in proto.read_pkt_seq():
                    self._report_status_parser.handle_packet(pkt)
        if self._report_status_parser is not None:
            self._report_status_parser.check()

    def _negotiate_upload_pack_capabilities(self, server_capabilities):
        unknown_capabilities = (  # noqa: F841
            extract_capability_names(server_capabilities) -
            KNOWN_UPLOAD_CAPABILITIES)
        # TODO(jelmer): warn about unknown capabilities
        symrefs = {}
        agent = None
        for capability in server_capabilities:
            k, v = parse_capability(capability)
            if k == CAPABILITY_SYMREF:
                (src, dst) = v.split(b':', 1)
                symrefs[src] = dst
            if k == CAPABILITY_AGENT:
                agent = v

        negotiated_capabilities = (
            self._fetch_capabilities & server_capabilities)
        return (negotiated_capabilities, symrefs, agent)

    def _handle_upload_pack_head(self, proto, capabilities, graph_walker,
                                 wants, can_read, depth):
        """Handle the head of a 'git-upload-pack' request.

        :param proto: Protocol object to read from
        :param capabilities: List of negotiated capabilities
        :param graph_walker: GraphWalker instance to call .ack() on
        :param wants: List of commits to fetch
        :param can_read: function that returns a boolean that indicates
            whether there is extra graph data to read on proto
        :param depth: Depth for request
        """
        assert isinstance(wants, list) and isinstance(wants[0], bytes)
        proto.write_pkt_line(COMMAND_WANT + b' ' + wants[0] + b' ' +
                             b' '.join(capabilities) + b'\n')
        for want in wants[1:]:
            proto.write_pkt_line(COMMAND_WANT + b' ' + want + b'\n')
        if depth not in (0, None) or getattr(graph_walker, 'shallow', None):
            if CAPABILITY_SHALLOW not in capabilities:
                raise GitProtocolError(
                    "server does not support shallow capability required for "
                    "depth")
            for sha in graph_walker.shallow:
                proto.write_pkt_line(COMMAND_SHALLOW + b' ' + sha + b'\n')
            proto.write_pkt_line(COMMAND_DEEPEN + b' ' +
                                 str(depth).encode('ascii') + b'\n')
            proto.write_pkt_line(None)
            if can_read is not None:
                (new_shallow, new_unshallow) = _read_shallow_updates(proto)
            else:
                new_shallow = new_unshallow = None
        else:
            new_shallow = new_unshallow = set()
            proto.write_pkt_line(None)
        have = next(graph_walker)
        while have:
            proto.write_pkt_line(COMMAND_HAVE + b' ' + have + b'\n')
            if can_read is not None and can_read():
                pkt = proto.read_pkt_line()
                parts = pkt.rstrip(b'\n').split(b' ')
                if parts[0] == b'ACK':
                    graph_walker.ack(parts[1])
                    if parts[2] in (b'continue', b'common'):
                        pass
                    elif parts[2] == b'ready':
                        break
                    else:
                        raise AssertionError(
                            "%s not in ('continue', 'ready', 'common)" %
                            parts[2])
            have = next(graph_walker)
        proto.write_pkt_line(COMMAND_DONE + b'\n')
        return (new_shallow, new_unshallow)

    def _handle_upload_pack_tail(self, proto, capabilities, graph_walker,
                                 pack_data, progress=None, rbufsize=_RBUFSIZE):
        """Handle the tail of a 'git-upload-pack' request.

        :param proto: Protocol object to read from
        :param capabilities: List of negotiated capabilities
        :param graph_walker: GraphWalker instance to call .ack() on
        :param pack_data: Function to call with pack data
        :param progress: Optional progress reporting function
        :param rbufsize: Read buffer size
        """
        pkt = proto.read_pkt_line()
        while pkt:
            parts = pkt.rstrip(b'\n').split(b' ')
            if parts[0] == b'ACK':
                graph_walker.ack(parts[1])
            if len(parts) < 3 or parts[2] not in (
                    b'ready', b'continue', b'common'):
                break
            pkt = proto.read_pkt_line()
        if CAPABILITY_SIDE_BAND_64K in capabilities:
            if progress is None:
                # Just ignore progress data

                def progress(x):
                    pass
            self._read_side_band64k_data(proto, {
                SIDE_BAND_CHANNEL_DATA: pack_data,
                SIDE_BAND_CHANNEL_PROGRESS: progress}
            )
        else:
            while True:
                data = proto.read(rbufsize)
                if data == b"":
                    break
                pack_data(data)


def check_wants(wants, refs):
    """Check that a set of wants is valid.

    :param wants: Set of object SHAs to fetch
    :param refs: Refs dictionary to check against
    """
    missing = set(wants) - {
            v for (k, v) in refs.items()
            if not k.endswith(ANNOTATED_TAG_SUFFIX)}
    if missing:
        raise InvalidWants(missing)


def remote_error_from_stderr(stderr):
    """Return an appropriate exception based on stderr output. """
    if stderr is None:
        return HangupException()
    for l in stderr.readlines():
        if l.startswith(b'ERROR: '):
            return GitProtocolError(
                l[len(b'ERROR: '):].decode('utf-8', 'replace'))
        return GitProtocolError(l.decode('utf-8', 'replace'))
    return HangupException()


class TraditionalGitClient(GitClient):
    """Traditional Git client."""

    DEFAULT_ENCODING = 'utf-8'

    def __init__(self, path_encoding=DEFAULT_ENCODING, **kwargs):
        self._remote_path_encoding = path_encoding
        super(TraditionalGitClient, self).__init__(**kwargs)

    def _connect(self, cmd, path):
        """Create a connection to the server.

        This method is abstract - concrete implementations should
        implement their own variant which connects to the server and
        returns an initialized Protocol object with the service ready
        for use and a can_read function which may be used to see if
        reads would block.

        :param cmd: The git service name to which we should connect.
        :param path: The path we should pass to the service. (as bytestirng)
        """
        raise NotImplementedError()

    def send_pack(self, path, update_refs, generate_pack_data,
                  progress=None):
        """Upload a pack to a remote repository.

        :param path: Repository path (as bytestring)
        :param update_refs: Function to determine changes to remote refs.
            Receive dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
        :param generate_pack_data: Function that can return a tuple with
            number of objects and pack data to upload.
        :param progress: Optional callback called with progress updates

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        :return: new_refs dictionary containing the changes that were made
            {refname: new_ref}, including deleted refs.
        """
        proto, unused_can_read, stderr = self._connect(b'receive-pack', path)
        with proto:
            try:
                old_refs, server_capabilities = read_pkt_refs(proto)
            except HangupException:
                raise remote_error_from_stderr(stderr)
            negotiated_capabilities = \
                self._negotiate_receive_pack_capabilities(server_capabilities)
            if CAPABILITY_REPORT_STATUS in negotiated_capabilities:
                self._report_status_parser = ReportStatusParser()
            report_status_parser = self._report_status_parser

            try:
                new_refs = orig_new_refs = update_refs(dict(old_refs))
            except BaseException:
                proto.write_pkt_line(None)
                raise

            if CAPABILITY_DELETE_REFS not in server_capabilities:
                # Server does not support deletions. Fail later.
                new_refs = dict(orig_new_refs)
                for ref, sha in orig_new_refs.items():
                    if sha == ZERO_SHA:
                        if CAPABILITY_REPORT_STATUS in negotiated_capabilities:
                            report_status_parser._ref_statuses.append(
                                b'ng ' + sha +
                                b' remote does not support deleting refs')
                            report_status_parser._ref_status_ok = False
                        del new_refs[ref]

            if new_refs is None:
                proto.write_pkt_line(None)
                return old_refs

            if len(new_refs) == 0 and len(orig_new_refs):
                # NOOP - Original new refs filtered out by policy
                proto.write_pkt_line(None)
                if report_status_parser is not None:
                    report_status_parser.check()
                return old_refs

            (have, want) = self._handle_receive_pack_head(
                proto, negotiated_capabilities, old_refs, new_refs)
            if (not want and
                    set(new_refs.items()).issubset(set(old_refs.items()))):
                return new_refs
            pack_data_count, pack_data = generate_pack_data(
                have, want,
                ofs_delta=(CAPABILITY_OFS_DELTA in negotiated_capabilities))

            dowrite = bool(pack_data_count)
            dowrite = dowrite or any(old_refs.get(ref) != sha
                                     for (ref, sha) in new_refs.items()
                                     if sha != ZERO_SHA)
            if dowrite:
                write_pack_data(proto.write_file(), pack_data_count, pack_data)

            self._handle_receive_pack_tail(
                proto, negotiated_capabilities, progress)
            return new_refs

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress=None, depth=None):
        """Retrieve a pack from a git smart server.

        :param path: Remote path to fetch from
        :param determine_wants: Function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch.
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        :param depth: Shallow fetch depth
        :return: FetchPackResult object
        """
        proto, can_read, stderr = self._connect(b'upload-pack', path)
        with proto:
            try:
                refs, server_capabilities = read_pkt_refs(proto)
            except HangupException:
                raise remote_error_from_stderr(stderr)
            negotiated_capabilities, symrefs, agent = (
                    self._negotiate_upload_pack_capabilities(
                            server_capabilities))

            if refs is None:
                proto.write_pkt_line(None)
                return FetchPackResult(refs, symrefs, agent)

            try:
                wants = determine_wants(refs)
            except BaseException:
                proto.write_pkt_line(None)
                raise
            if wants is not None:
                wants = [cid for cid in wants if cid != ZERO_SHA]
            if not wants:
                proto.write_pkt_line(None)
                return FetchPackResult(refs, symrefs, agent)
            check_wants(wants, refs)
            (new_shallow, new_unshallow) = self._handle_upload_pack_head(
                proto, negotiated_capabilities, graph_walker, wants, can_read,
                depth=depth)
            self._handle_upload_pack_tail(
                proto, negotiated_capabilities, graph_walker, pack_data,
                progress)
            return FetchPackResult(
                    refs, symrefs, agent, new_shallow, new_unshallow)

    def get_refs(self, path):
        """Retrieve the current refs from a git smart server."""
        # stock `git ls-remote` uses upload-pack
        proto, _, stderr = self._connect(b'upload-pack', path)
        with proto:
            try:
                refs, _ = read_pkt_refs(proto)
            except HangupException:
                raise remote_error_from_stderr(stderr)
            proto.write_pkt_line(None)
            return refs

    def archive(self, path, committish, write_data, progress=None,
                write_error=None, format=None, subdirs=None, prefix=None):
        proto, can_read, stderr = self._connect(b'upload-archive', path)
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
            except HangupException:
                raise remote_error_from_stderr(stderr)
            if pkt == b"NACK\n":
                return
            elif pkt == b"ACK\n":
                pass
            elif pkt.startswith(b"ERR "):
                raise GitProtocolError(
                        pkt[4:].rstrip(b"\n").decode('utf-8', 'replace'))
            else:
                raise AssertionError("invalid response %r" % pkt)
            ret = proto.read_pkt_line()
            if ret is not None:
                raise AssertionError("expected pkt tail")
            self._read_side_band64k_data(proto, {
                SIDE_BAND_CHANNEL_DATA: write_data,
                SIDE_BAND_CHANNEL_PROGRESS: progress,
                SIDE_BAND_CHANNEL_FATAL: write_error})


class TCPGitClient(TraditionalGitClient):
    """A Git Client that works over TCP directly (i.e. git://)."""

    def __init__(self, host, port=None, **kwargs):
        if port is None:
            port = TCP_GIT_PORT
        self._host = host
        self._port = port
        super(TCPGitClient, self).__init__(**kwargs)

    @classmethod
    def from_parsedurl(cls, parsedurl, **kwargs):
        return cls(parsedurl.hostname, port=parsedurl.port, **kwargs)

    def get_url(self, path):
        netloc = self._host
        if self._port is not None and self._port != TCP_GIT_PORT:
            netloc += ":%d" % self._port
        return urlparse.urlunsplit(("git", netloc, path, '', ''))

    def _connect(self, cmd, path):
        if not isinstance(cmd, bytes):
            raise TypeError(cmd)
        if not isinstance(path, bytes):
            path = path.encode(self._remote_path_encoding)
        sockaddrs = socket.getaddrinfo(
            self._host, self._port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        s = None
        err = socket.error("no address found for %s" % self._host)
        for (family, socktype, proto, canonname, sockaddr) in sockaddrs:
            s = socket.socket(family, socktype, proto)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                s.connect(sockaddr)
                break
            except socket.error as e:
                err = e
                if s is not None:
                    s.close()
                s = None
        if s is None:
            raise err
        # -1 means system default buffering
        rfile = s.makefile('rb', -1)
        # 0 means unbuffered
        wfile = s.makefile('wb', 0)

        def close():
            rfile.close()
            wfile.close()
            s.close()

        proto = Protocol(rfile.read, wfile.write, close,
                         report_activity=self._report_activity)
        if path.startswith(b"/~"):
            path = path[1:]
        # TODO(jelmer): Alternative to ascii?
        proto.send_cmd(
            b'git-' + cmd, path, b'host=' + self._host.encode('ascii'))
        return proto, lambda: _fileno_can_read(s), None


class SubprocessWrapper(object):
    """A socket-like object that talks to a subprocess via pipes."""

    def __init__(self, proc):
        self.proc = proc
        if sys.version_info[0] == 2:
            self.read = proc.stdout.read
        else:
            self.read = BufferedReader(proc.stdout).read
        self.write = proc.stdin.write

    @property
    def stderr(self):
        return self.proc.stderr

    def can_read(self):
        if sys.platform == 'win32':
            from msvcrt import get_osfhandle
            handle = get_osfhandle(self.proc.stdout.fileno())
            return _win32_peek_avail(handle) != 0
        else:
            return _fileno_can_read(self.proc.stdout.fileno())

    def close(self):
        self.proc.stdin.close()
        self.proc.stdout.close()
        if self.proc.stderr:
            self.proc.stderr.close()
        self.proc.wait()


def find_git_command():
    """Find command to run for system Git (usually C Git).
    """
    if sys.platform == 'win32':  # support .exe, .bat and .cmd
        try:  # to avoid overhead
            import win32api
        except ImportError:  # run through cmd.exe with some overhead
            return ['cmd', '/c', 'git']
        else:
            status, git = win32api.FindExecutable('git')
            return [git]
    else:
        return ['git']


class SubprocessGitClient(TraditionalGitClient):
    """Git client that talks to a server using a subprocess."""

    @classmethod
    def from_parsedurl(cls, parsedurl, **kwargs):
        return cls(**kwargs)

    git_command = None

    def _connect(self, service, path):
        if not isinstance(service, bytes):
            raise TypeError(service)
        if isinstance(path, bytes):
            path = path.decode(self._remote_path_encoding)
        if self.git_command is None:
            git_command = find_git_command()
        argv = git_command + [service.decode('ascii'), path]
        p = subprocess.Popen(argv, bufsize=0, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        pw = SubprocessWrapper(p)
        return (Protocol(pw.read, pw.write, pw.close,
                         report_activity=self._report_activity),
                pw.can_read, p.stderr)


class LocalGitClient(GitClient):
    """Git Client that just uses a local Repo."""

    def __init__(self, thin_packs=True, report_activity=None, config=None):
        """Create a new LocalGitClient instance.

        :param thin_packs: Whether or not thin packs should be retrieved
        :param report_activity: Optional callback for reporting transport
            activity.
        """
        self._report_activity = report_activity
        # Ignore the thin_packs argument

    def get_url(self, path):
        return urlparse.urlunsplit(('file', '', path, '', ''))

    @classmethod
    def from_parsedurl(cls, parsedurl, **kwargs):
        return cls(**kwargs)

    @classmethod
    def _open_repo(cls, path):
        from dulwich.repo import Repo
        if not isinstance(path, str):
            path = path.decode(sys.getfilesystemencoding())
        return closing(Repo(path))

    def send_pack(self, path, update_refs, generate_pack_data,
                  progress=None):
        """Upload a pack to a remote repository.

        :param path: Repository path (as bytestring)
        :param update_refs: Function to determine changes to remote refs.
            Receive dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
        :param generate_pack_data: Function that can return a tuple
            with number of items and pack data to upload.
        :param progress: Optional progress function

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        :return: new_refs dictionary containing the changes that were made
            {refname: new_ref}, including deleted refs.
        """
        if not progress:
            def progress(x):
                pass

        with self._open_repo(path) as target:
            old_refs = target.get_refs()
            new_refs = update_refs(dict(old_refs))

            have = [sha1 for sha1 in old_refs.values() if sha1 != ZERO_SHA]
            want = []
            for refname, new_sha1 in new_refs.items():
                if (new_sha1 not in have and
                        new_sha1 not in want and
                        new_sha1 != ZERO_SHA):
                    want.append(new_sha1)

            if (not want and
                    set(new_refs.items()).issubset(set(old_refs.items()))):
                return new_refs

            target.object_store.add_pack_data(
                *generate_pack_data(have, want, ofs_delta=True))

            for refname, new_sha1 in new_refs.items():
                old_sha1 = old_refs.get(refname, ZERO_SHA)
                if new_sha1 != ZERO_SHA:
                    if not target.refs.set_if_equals(
                            refname, old_sha1, new_sha1):
                        progress('unable to set %s to %s' %
                                 (refname, new_sha1))
                else:
                    if not target.refs.remove_if_equals(refname, old_sha1):
                        progress('unable to remove %s' % refname)

        return new_refs

    def fetch(self, path, target, determine_wants=None, progress=None,
              depth=None):
        """Fetch into a target repository.

        :param path: Path to fetch from (as bytestring)
        :param target: Target repository to fetch into
        :param determine_wants: Optional function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch. Defaults to all shas.
        :param progress: Optional progress function
        :param depth: Shallow fetch depth
        :return: FetchPackResult object
        """
        with self._open_repo(path) as r:
            refs = r.fetch(target, determine_wants=determine_wants,
                           progress=progress, depth=depth)
            return FetchPackResult(refs, r.refs.get_symrefs(),
                                   agent_string())

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress=None, depth=None):
        """Retrieve a pack from a git smart server.

        :param path: Remote path to fetch from
        :param determine_wants: Function determine what refs
            to fetch. Receives dictionary of name->sha, should return
            list of shas to fetch.
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        :param depth: Shallow fetch depth
        :return: FetchPackResult object
        """
        with self._open_repo(path) as r:
            objects_iter = r.fetch_objects(
                determine_wants, graph_walker, progress=progress, depth=depth)
            symrefs = r.refs.get_symrefs()
            agent = agent_string()

            # Did the process short-circuit (e.g. in a stateless RPC call)?
            # Note that the client still expects a 0-object pack in most cases.
            if objects_iter is None:
                return FetchPackResult(None, symrefs, agent)
            protocol = ProtocolFile(None, pack_data)
            write_pack_objects(protocol, objects_iter)
            return FetchPackResult(r.get_refs(), symrefs, agent)

    def get_refs(self, path):
        """Retrieve the current refs from a git smart server."""

        with self._open_repo(path) as target:
            return target.get_refs()


# What Git client to use for local access
default_local_git_client_cls = LocalGitClient


class SSHVendor(object):
    """A client side SSH implementation."""

    def connect_ssh(self, host, command, username=None, port=None,
                    password=None, key_filename=None):
        # This function was deprecated in 0.9.1
        import warnings
        warnings.warn(
            "SSHVendor.connect_ssh has been renamed to SSHVendor.run_command",
            DeprecationWarning)
        return self.run_command(host, command, username=username, port=port,
                                password=password, key_filename=key_filename)

    def run_command(self, host, command, username=None, port=None,
                    password=None, key_filename=None):
        """Connect to an SSH server.

        Run a command remotely and return a file-like object for interaction
        with the remote command.

        :param host: Host name
        :param command: Command to run (as argv array)
        :param username: Optional ame of user to log in as
        :param port: Optional SSH port to use
        :param password: Optional ssh password for login or private key
        :param key_filename: Optional path to private keyfile
        """
        raise NotImplementedError(self.run_command)


class StrangeHostname(Exception):
    """Refusing to connect to strange SSH hostname."""

    def __init__(self, hostname):
        super(StrangeHostname, self).__init__(hostname)


class SubprocessSSHVendor(SSHVendor):
    """SSH vendor that shells out to the local 'ssh' command."""

    def run_command(self, host, command, username=None, port=None,
                    password=None, key_filename=None):

        if password is not None:
            raise NotImplementedError(
                "Setting password not supported by SubprocessSSHVendor.")

        args = ['ssh', '-x']

        if port:
            args.extend(['-p', str(port)])

        if key_filename:
            args.extend(['-i', str(key_filename)])

        if username:
            host = '%s@%s' % (username, host)
        if host.startswith('-'):
            raise StrangeHostname(hostname=host)
        args.append(host)

        proc = subprocess.Popen(args + [command], bufsize=0,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        return SubprocessWrapper(proc)


class PLinkSSHVendor(SSHVendor):
    """SSH vendor that shells out to the local 'plink' command."""

    def run_command(self, host, command, username=None, port=None,
                    password=None, key_filename=None):

        if sys.platform == 'win32':
            args = ['plink.exe', '-ssh']
        else:
            args = ['plink', '-ssh']

        if password is not None:
            import warnings
            warnings.warn(
                "Invoking PLink with a password exposes the password in the "
                "process list.")
            args.extend(['-pw', str(password)])

        if port:
            args.extend(['-P', str(port)])

        if key_filename:
            args.extend(['-i', str(key_filename)])

        if username:
            host = '%s@%s' % (username, host)
        if host.startswith('-'):
            raise StrangeHostname(hostname=host)
        args.append(host)

        proc = subprocess.Popen(args + [command], bufsize=0,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        return SubprocessWrapper(proc)


def ParamikoSSHVendor(**kwargs):
    import warnings
    warnings.warn(
        "ParamikoSSHVendor has been moved to dulwich.contrib.paramiko_vendor.",
        DeprecationWarning)
    from dulwich.contrib.paramiko_vendor import ParamikoSSHVendor
    return ParamikoSSHVendor(**kwargs)


# Can be overridden by users
get_ssh_vendor = SubprocessSSHVendor


class SSHGitClient(TraditionalGitClient):

    def __init__(self, host, port=None, username=None, vendor=None,
                 config=None, password=None, key_filename=None, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key_filename = key_filename
        super(SSHGitClient, self).__init__(**kwargs)
        self.alternative_paths = {}
        if vendor is not None:
            self.ssh_vendor = vendor
        else:
            self.ssh_vendor = get_ssh_vendor()

    def get_url(self, path):
        netloc = self.host
        if self.port is not None:
            netloc += ":%d" % self.port

        if self.username is not None:
            netloc = urlquote(self.username, '@/:') + "@" + netloc

        return urlparse.urlunsplit(('ssh', netloc, path, '', ''))

    @classmethod
    def from_parsedurl(cls, parsedurl, **kwargs):
        return cls(host=parsedurl.hostname, port=parsedurl.port,
                   username=parsedurl.username, **kwargs)

    def _get_cmd_path(self, cmd):
        cmd = self.alternative_paths.get(cmd, b'git-' + cmd)
        assert isinstance(cmd, bytes)
        return cmd

    def _connect(self, cmd, path):
        if not isinstance(cmd, bytes):
            raise TypeError(cmd)
        if isinstance(path, bytes):
            path = path.decode(self._remote_path_encoding)
        if path.startswith("/~"):
            path = path[1:]
        argv = (self._get_cmd_path(cmd).decode(self._remote_path_encoding) +
                " '" + path + "'")
        kwargs = {}
        if self.password is not None:
            kwargs['password'] = self.password
        if self.key_filename is not None:
            kwargs['key_filename'] = self.key_filename
        con = self.ssh_vendor.run_command(
            self.host, argv, port=self.port, username=self.username,
            **kwargs)
        return (Protocol(con.read, con.write, con.close,
                         report_activity=self._report_activity),
                con.can_read, getattr(con, 'stderr', None))


def default_user_agent_string():
    # Start user agent with "git/", because GitHub requires this. :-( See
    # https://github.com/jelmer/dulwich/issues/562 for details.
    return "git/dulwich/%s" % ".".join([str(x) for x in dulwich.__version__])


def default_urllib3_manager(config, **override_kwargs):
    """Return `urllib3` connection pool manager.

    Honour detected proxy configurations.

    :param config: `dulwich.config.ConfigDict` instance with Git configuration.
    :param kwargs: Additional arguments for urllib3.ProxyManager
    :return: `urllib3.ProxyManager` instance for proxy configurations,
        `urllib3.PoolManager` otherwise.
    """
    proxy_server = user_agent = None
    ca_certs = ssl_verify = None

    if config is not None:
        try:
            proxy_server = config.get(b"http", b"proxy")
        except KeyError:
            pass
        try:
            user_agent = config.get(b"http", b"useragent")
        except KeyError:
            pass

        # TODO(jelmer): Support per-host settings
        try:
            ssl_verify = config.get_boolean(b"http", b"sslVerify")
        except KeyError:
            ssl_verify = True

        try:
            ca_certs = config.get_boolean(b"http", b"sslCAInfo")
        except KeyError:
            ca_certs = None

    if user_agent is None:
        user_agent = default_user_agent_string()

    headers = {"User-agent": user_agent}

    kwargs = {}
    if ssl_verify is True:
        kwargs['cert_reqs'] = "CERT_REQUIRED"
    elif ssl_verify is False:
        kwargs['cert_reqs'] = 'CERT_NONE'
    else:
        # Default to SSL verification
        kwargs['cert_reqs'] = "CERT_REQUIRED"

    if ca_certs is not None:
        kwargs['ca_certs'] = ca_certs
    kwargs.update(override_kwargs)

    # Try really hard to find a SSL certificate path
    if 'ca_certs' not in kwargs and kwargs.get('cert_reqs') != 'CERT_NONE':
        try:
            import certifi
        except ImportError:
            pass
        else:
            kwargs['ca_certs'] = certifi.where()

    import urllib3

    if proxy_server is not None:
        # `urllib3` requires a `str` object in both Python 2 and 3, while
        # `ConfigDict` coerces entries to `bytes` on Python 3. Compensate.
        if not isinstance(proxy_server, str):
            proxy_server = proxy_server.decode()
        manager = urllib3.ProxyManager(proxy_server, headers=headers,
                                       **kwargs)
    else:
        manager = urllib3.PoolManager(headers=headers, **kwargs)

    return manager


class HttpGitClient(GitClient):

    def __init__(self, base_url, dumb=None, pool_manager=None, config=None,
                 username=None, password=None, **kwargs):
        self._base_url = base_url.rstrip("/") + "/"
        self._username = username
        self._password = password
        self.dumb = dumb

        if pool_manager is None:
            self.pool_manager = default_urllib3_manager(config)
        else:
            self.pool_manager = pool_manager

        if username is not None:
            # No escaping needed: ":" is not allowed in username:
            # https://tools.ietf.org/html/rfc2617#section-2
            credentials = "%s:%s" % (username, password)
            import urllib3.util
            basic_auth = urllib3.util.make_headers(basic_auth=credentials)
            self.pool_manager.headers.update(basic_auth)

        GitClient.__init__(self, **kwargs)

    def get_url(self, path):
        return self._get_url(path).rstrip("/")

    @classmethod
    def from_parsedurl(cls, parsedurl, **kwargs):
        password = parsedurl.password
        if password is not None:
            kwargs['password'] = urlunquote(password)
        username = parsedurl.username
        if username is not None:
            kwargs['username'] = urlunquote(username)
        netloc = parsedurl.hostname
        if parsedurl.port:
            netloc = "%s:%s" % (netloc, parsedurl.port)
        if parsedurl.username:
            netloc = "%s@%s" % (parsedurl.username, netloc)
        parsedurl = parsedurl._replace(netloc=netloc)
        return cls(urlparse.urlunparse(parsedurl), **kwargs)

    def __repr__(self):
        return "%s(%r, dumb=%r)" % (
            type(self).__name__, self._base_url, self.dumb)

    def _get_url(self, path):
        if not isinstance(path, str):
            # TODO(jelmer): this is unrelated to the local filesystem;
            # This is not necessarily the right encoding to decode the path
            # with.
            path = path.decode(sys.getfilesystemencoding())
        return urlparse.urljoin(self._base_url, path).rstrip("/") + "/"

    def _http_request(self, url, headers=None, data=None,
                      allow_compression=False):
        """Perform HTTP request.

        :param url: Request URL.
        :param headers: Optional custom headers to override defaults.
        :param data: Request data.
        :param allow_compression: Allow GZipped communication.
        :return: Tuple (`response`, `read`), where response is an `urllib3`
            response object with additional `content_type` and
            `redirect_location` properties, and `read` is a consumable read
            method for the response data.
        """
        req_headers = self.pool_manager.headers.copy()
        if headers is not None:
            req_headers.update(headers)
        req_headers["Pragma"] = "no-cache"
        if allow_compression:
            req_headers["Accept-Encoding"] = "gzip"
        else:
            req_headers["Accept-Encoding"] = "identity"

        if data is None:
            resp = self.pool_manager.request("GET", url, headers=req_headers)
        else:
            resp = self.pool_manager.request("POST", url, headers=req_headers,
                                             body=data)

        if resp.status == 404:
            raise NotGitRepository()
        elif resp.status != 200:
            raise GitProtocolError("unexpected http resp %d for %s" %
                                   (resp.status, url))

        # TODO: Optimization available by adding `preload_content=False` to the
        # request and just passing the `read` method on instead of going via
        # `BytesIO`, if we can guarantee that the entire response is consumed
        # before issuing the next to still allow for connection reuse from the
        # pool.
        read = BytesIO(resp.data).read

        resp.content_type = resp.getheader("Content-Type")
        resp.redirect_location = resp.get_redirect_location()

        return resp, read

    def _discover_references(self, service, base_url):
        assert base_url[-1] == "/"
        tail = "info/refs"
        headers = {"Accept": "*/*"}
        if self.dumb is not True:
            tail += "?service=%s" % service.decode('ascii')
        url = urlparse.urljoin(base_url, tail)
        resp, read = self._http_request(url, headers, allow_compression=True)

        if resp.redirect_location:
            # Something changed (redirect!), so let's update the base URL
            if not resp.redirect_location.endswith(tail):
                raise GitProtocolError(
                        "Redirected from URL %s to URL %s without %s" % (
                            url, resp.redirect_location, tail))
            base_url = resp.redirect_location[:-len(tail)]

        try:
            self.dumb = not resp.content_type.startswith("application/x-git-")
            if not self.dumb:
                proto = Protocol(read, None)
                # The first line should mention the service
                try:
                    [pkt] = list(proto.read_pkt_seq())
                except ValueError:
                    raise GitProtocolError(
                        "unexpected number of packets received")
                if pkt.rstrip(b'\n') != (b'# service=' + service):
                    raise GitProtocolError(
                        "unexpected first line %r from smart server" % pkt)
                return read_pkt_refs(proto) + (base_url, )
            else:
                return read_info_refs(resp), set(), base_url
        finally:
            resp.close()

    def _smart_request(self, service, url, data):
        assert url[-1] == "/"
        url = urlparse.urljoin(url, service)
        result_content_type = "application/x-%s-result" % service
        headers = {
            "Content-Type": "application/x-%s-request" % service,
            "Accept": result_content_type,
            "Content-Length": str(len(data)),
        }
        resp, read = self._http_request(url, headers, data)
        if resp.content_type != result_content_type:
            raise GitProtocolError("Invalid content-type from server: %s"
                                   % resp.content_type)
        return resp, read

    def send_pack(self, path, update_refs, generate_pack_data,
                  progress=None):
        """Upload a pack to a remote repository.

        :param path: Repository path (as bytestring)
        :param update_refs: Function to determine changes to remote refs.
            Receive dict with existing remote refs, returns dict with
            changed refs (name -> sha, where sha=ZERO_SHA for deletions)
        :param generate_pack_data: Function that can return a tuple
            with number of elements and pack data to upload.
        :param progress: Optional progress function

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        :return: new_refs dictionary containing the changes that were made
            {refname: new_ref}, including deleted refs.
        """
        url = self._get_url(path)
        old_refs, server_capabilities, url = self._discover_references(
            b"git-receive-pack", url)
        negotiated_capabilities = self._negotiate_receive_pack_capabilities(
                server_capabilities)
        negotiated_capabilities.add(capability_agent())

        if CAPABILITY_REPORT_STATUS in negotiated_capabilities:
            self._report_status_parser = ReportStatusParser()

        new_refs = update_refs(dict(old_refs))
        if new_refs is None:
            # Determine wants function is aborting the push.
            return old_refs
        if self.dumb:
            raise NotImplementedError(self.fetch_pack)
        req_data = BytesIO()
        req_proto = Protocol(None, req_data.write)
        (have, want) = self._handle_receive_pack_head(
            req_proto, negotiated_capabilities, old_refs, new_refs)
        if not want and set(new_refs.items()).issubset(set(old_refs.items())):
            return new_refs
        pack_data_count, pack_data = generate_pack_data(
                have, want,
                ofs_delta=(CAPABILITY_OFS_DELTA in negotiated_capabilities))
        if pack_data_count:
            write_pack_data(req_proto.write_file(), pack_data_count, pack_data)
        resp, read = self._smart_request("git-receive-pack", url,
                                         data=req_data.getvalue())
        try:
            resp_proto = Protocol(read, None)
            self._handle_receive_pack_tail(
                resp_proto, negotiated_capabilities, progress)
            return new_refs
        finally:
            resp.close()

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress=None, depth=None):
        """Retrieve a pack from a git smart server.

        :param determine_wants: Callback that returns list of commits to fetch
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        :param depth: Depth for request
        :return: FetchPackResult object
        """
        url = self._get_url(path)
        refs, server_capabilities, url = self._discover_references(
            b"git-upload-pack", url)
        negotiated_capabilities, symrefs, agent = (
                self._negotiate_upload_pack_capabilities(
                        server_capabilities))
        wants = determine_wants(refs)
        if wants is not None:
            wants = [cid for cid in wants if cid != ZERO_SHA]
        if not wants:
            return FetchPackResult(refs, symrefs, agent)
        if self.dumb:
            raise NotImplementedError(self.send_pack)
        check_wants(wants, refs)
        req_data = BytesIO()
        req_proto = Protocol(None, req_data.write)
        (new_shallow, new_unshallow) = self._handle_upload_pack_head(
                req_proto, negotiated_capabilities, graph_walker, wants,
                can_read=None, depth=depth)
        resp, read = self._smart_request(
            "git-upload-pack", url, data=req_data.getvalue())
        try:
            resp_proto = Protocol(read, None)
            if new_shallow is None and new_unshallow is None:
                (new_shallow, new_unshallow) = _read_shallow_updates(
                        resp_proto)
            self._handle_upload_pack_tail(
                resp_proto, negotiated_capabilities, graph_walker, pack_data,
                progress)
            return FetchPackResult(
                    refs, symrefs, agent, new_shallow, new_unshallow)
        finally:
            resp.close()

    def get_refs(self, path):
        """Retrieve the current refs from a git smart server."""
        url = self._get_url(path)
        refs, _, _ = self._discover_references(
            b"git-upload-pack", url)
        return refs


def get_transport_and_path_from_url(url, config=None, **kwargs):
    """Obtain a git client from a URL.

    :param url: URL to open (a unicode string)
    :param config: Optional config object
    :param thin_packs: Whether or not thin packs should be retrieved
    :param report_activity: Optional callback for reporting transport
        activity.
    :return: Tuple with client instance and relative path.
    """
    parsed = urlparse.urlparse(url)
    if parsed.scheme == 'git':
        return (TCPGitClient.from_parsedurl(parsed, **kwargs),
                parsed.path)
    elif parsed.scheme in ('git+ssh', 'ssh'):
        return SSHGitClient.from_parsedurl(parsed, **kwargs), parsed.path
    elif parsed.scheme in ('http', 'https'):
        return HttpGitClient.from_parsedurl(
            parsed, config=config, **kwargs), parsed.path
    elif parsed.scheme == 'file':
        return default_local_git_client_cls.from_parsedurl(
            parsed, **kwargs), parsed.path

    raise ValueError("unknown scheme '%s'" % parsed.scheme)


def parse_rsync_url(location):
    """Parse a rsync-style URL."""
    if ':' in location and '@' not in location:
        # SSH with no user@, zero or one leading slash.
        (host, path) = location.split(':', 1)
        user = None
    elif ':' in location:
        # SSH with user@host:foo.
        user_host, path = location.split(':', 1)
        if '@' in user_host:
            user, host = user_host.rsplit('@', 1)
        else:
            user = None
            host = user_host
    else:
        raise ValueError('not a valid rsync-style URL')
    return (user, host, path)


def get_transport_and_path(location, **kwargs):
    """Obtain a git client from a URL.

    :param location: URL or path (a string)
    :param config: Optional config object
    :param thin_packs: Whether or not thin packs should be retrieved
    :param report_activity: Optional callback for reporting transport
        activity.
    :return: Tuple with client instance and relative path.
    """
    # First, try to parse it as a URL
    try:
        return get_transport_and_path_from_url(location, **kwargs)
    except ValueError:
        pass

    if (sys.platform == 'win32' and
            location[0].isalpha() and location[1:3] == ':\\'):
        # Windows local path
        return default_local_git_client_cls(**kwargs), location

    try:
        (username, hostname, path) = parse_rsync_url(location)
    except ValueError:
        # Otherwise, assume it's a local path.
        return default_local_git_client_cls(**kwargs), location
    else:
        return SSHGitClient(hostname, username=username, **kwargs), path
