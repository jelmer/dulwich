# client.py -- Implementation of the server side git protocols
# Copyright (C) 2008-2009 Jelmer Vernooij <jelmer@samba.org>
# Copyright (C) 2008 John Carr
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

"""Client side support for the Git protocol.

The Dulwich client supports the following capabilities:

 * thin-pack
 * multi_ack_detailed
 * multi_ack
 * side-band-64k
 * ofs-delta
 * report-status
 * delete-refs

Known capabilities that are not supported:

 * shallow
 * no-progress
 * include-tag
"""

__docformat__ = 'restructuredText'

from cStringIO import StringIO
import select
import socket
import subprocess
import urllib2
import urlparse

from dulwich.errors import (
    GitProtocolError,
    NotGitRepository,
    SendPackError,
    UpdateRefsError,
    )
from dulwich.protocol import (
    _RBUFSIZE,
    PktLineParser,
    Protocol,
    TCP_GIT_PORT,
    ZERO_SHA,
    extract_capabilities,
    )
from dulwich.pack import (
    write_pack_objects,
    )


# Python 2.6.6 included these in urlparse.uses_netloc upstream. Do
# monkeypatching to enable similar behaviour in earlier Pythons:
for scheme in ('git', 'git+ssh'):
    if scheme not in urlparse.uses_netloc:
        urlparse.uses_netloc.append(scheme)

def _fileno_can_read(fileno):
    """Check if a file descriptor is readable."""
    return len(select.select([fileno], [], [], 0)[0]) > 0

COMMON_CAPABILITIES = ['ofs-delta', 'side-band-64k']
FETCH_CAPABILITIES = ['multi_ack', 'multi_ack_detailed'] + COMMON_CAPABILITIES
SEND_CAPABILITIES = ['report-status'] + COMMON_CAPABILITIES


class ReportStatusParser(object):
    """Handle status as reported by servers with the 'report-status' capability.
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
        if self._pack_status not in ('unpack ok', None):
            raise SendPackError(self._pack_status)
        if not self._ref_status_ok:
            ref_status = {}
            ok = set()
            for status in self._ref_statuses:
                if ' ' not in status:
                    # malformed response, move on to the next one
                    continue
                status, ref = status.split(' ', 1)

                if status == 'ng':
                    if ' ' in ref:
                        ref, status = ref.split(' ', 1)
                else:
                    ok.add(ref)
                ref_status[ref] = status
            raise UpdateRefsError('%s failed to update' %
                                  ', '.join([ref for ref in ref_status
                                             if ref not in ok]),
                                  ref_status=ref_status)

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
            if not ref_status.startswith('ok '):
                self._ref_status_ok = False


# TODO(durin42): this doesn't correctly degrade if the server doesn't
# support some capabilities. This should work properly with servers
# that don't support multi_ack.
class GitClient(object):
    """Git smart server client.

    """

    def __init__(self, thin_packs=True, report_activity=None):
        """Create a new GitClient instance.

        :param thin_packs: Whether or not thin packs should be retrieved
        :param report_activity: Optional callback for reporting transport
            activity.
        """
        self._report_activity = report_activity
        self._fetch_capabilities = set(FETCH_CAPABILITIES)
        self._send_capabilities = set(SEND_CAPABILITIES)
        if thin_packs:
            self._fetch_capabilities.add('thin-pack')

    def _read_refs(self, proto):
        server_capabilities = None
        refs = {}
        # Receive refs from server
        for pkt in proto.read_pkt_seq():
            (sha, ref) = pkt.rstrip('\n').split(' ', 1)
            if sha == 'ERR':
                raise GitProtocolError(ref)
            if server_capabilities is None:
                (ref, server_capabilities) = extract_capabilities(ref)
            refs[ref] = sha
        return refs, set(server_capabilities)

    def send_pack(self, path, determine_wants, generate_pack_contents,
                  progress=None):
        """Upload a pack to a remote repository.

        :param path: Repository path
        :param generate_pack_contents: Function that can return a sequence of the
            shas of the objects to upload.
        :param progress: Optional progress function

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        """
        raise NotImplementedError(self.send_pack)

    def fetch(self, path, target, determine_wants=None, progress=None):
        """Fetch into a target repository.

        :param path: Path to fetch from
        :param target: Target repository to fetch into
        :param determine_wants: Optional function to determine what refs
            to fetch
        :param progress: Optional progress function
        :return: remote refs as dictionary
        """
        if determine_wants is None:
            determine_wants = target.object_store.determine_wants_all
        f, commit = target.object_store.add_pack()
        try:
            return self.fetch_pack(path, determine_wants,
                target.get_graph_walker(), f.write, progress)
        finally:
            commit()

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress=None):
        """Retrieve a pack from a git smart server.

        :param determine_wants: Callback that returns list of commits to fetch
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        """
        raise NotImplementedError(self.fetch_pack)

    def _parse_status_report(self, proto):
        unpack = proto.read_pkt_line().strip()
        if unpack != 'unpack ok':
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
            if not ref_status.startswith('ok '):
                errs = True
            ref_status = proto.read_pkt_line()

        if errs:
            ref_status = {}
            ok = set()
            for status in statuses:
                if ' ' not in status:
                    # malformed response, move on to the next one
                    continue
                status, ref = status.split(' ', 1)

                if status == 'ng':
                    if ' ' in ref:
                        ref, status = ref.split(' ', 1)
                else:
                    ok.add(ref)
                ref_status[ref] = status
            raise UpdateRefsError('%s failed to update' %
                                  ', '.join([ref for ref in ref_status
                                             if ref not in ok]),
                                  ref_status=ref_status)

    def _read_side_band64k_data(self, proto, channel_callbacks):
        """Read per-channel data.

        This requires the side-band-64k capability.

        :param proto: Protocol object to read from
        :param channel_callbacks: Dictionary mapping channels to packet
            handlers to use. None for a callback discards channel data.
        """
        for pkt in proto.read_pkt_seq():
            channel = ord(pkt[0])
            pkt = pkt[1:]
            try:
                cb = channel_callbacks[channel]
            except KeyError:
                raise AssertionError('Invalid sideband channel %d' % channel)
            else:
                if cb is not None:
                    cb(pkt)

    def _handle_receive_pack_head(self, proto, capabilities, old_refs, new_refs):
        """Handle the head of a 'git-receive-pack' request.

        :param proto: Protocol object to read from
        :param capabilities: List of negotiated capabilities
        :param old_refs: Old refs, as received from the server
        :param new_refs: New refs
        :return: (have, want) tuple
        """
        want = []
        have = [x for x in old_refs.values() if not x == ZERO_SHA]
        sent_capabilities = False
        for refname in set(new_refs.keys() + old_refs.keys()):
            old_sha1 = old_refs.get(refname, ZERO_SHA)
            new_sha1 = new_refs.get(refname, ZERO_SHA)
            if old_sha1 != new_sha1:
                if sent_capabilities:
                    proto.write_pkt_line('%s %s %s' % (old_sha1, new_sha1,
                                                            refname))
                else:
                    proto.write_pkt_line(
                      '%s %s %s\0%s' % (old_sha1, new_sha1, refname,
                                        ' '.join(capabilities)))
                    sent_capabilities = True
            if new_sha1 not in have and new_sha1 != ZERO_SHA:
                want.append(new_sha1)
        proto.write_pkt_line(None)
        return (have, want)

    def _handle_receive_pack_tail(self, proto, capabilities, progress=None):
        """Handle the tail of a 'git-receive-pack' request.

        :param proto: Protocol object to read from
        :param capabilities: List of negotiated capabilities
        :param progress: Optional progress reporting function
        """
        if 'report-status' in capabilities:
            report_status_parser = ReportStatusParser()
        else:
            report_status_parser = None
        if "side-band-64k" in capabilities:
            if progress is None:
                progress = lambda x: None
            channel_callbacks = { 2: progress }
            if 'report-status' in capabilities:
                channel_callbacks[1] = PktLineParser(
                    report_status_parser.handle_packet).parse
            self._read_side_band64k_data(proto, channel_callbacks)
        else:
            if 'report-status' in capabilities:
                for pkt in proto.read_pkt_seq():
                    report_status_parser.handle_packet(pkt)
        if report_status_parser is not None:
            report_status_parser.check()
        # wait for EOF before returning
        data = proto.read()
        if data:
            raise SendPackError('Unexpected response %r' % data)

    def _handle_upload_pack_head(self, proto, capabilities, graph_walker,
                                 wants, can_read):
        """Handle the head of a 'git-upload-pack' request.

        :param proto: Protocol object to read from
        :param capabilities: List of negotiated capabilities
        :param graph_walker: GraphWalker instance to call .ack() on
        :param wants: List of commits to fetch
        :param can_read: function that returns a boolean that indicates
            whether there is extra graph data to read on proto
        """
        assert isinstance(wants, list) and type(wants[0]) == str
        proto.write_pkt_line('want %s %s\n' % (
            wants[0], ' '.join(capabilities)))
        for want in wants[1:]:
            proto.write_pkt_line('want %s\n' % want)
        proto.write_pkt_line(None)
        have = graph_walker.next()
        while have:
            proto.write_pkt_line('have %s\n' % have)
            if can_read():
                pkt = proto.read_pkt_line()
                parts = pkt.rstrip('\n').split(' ')
                if parts[0] == 'ACK':
                    graph_walker.ack(parts[1])
                    if parts[2] in ('continue', 'common'):
                        pass
                    elif parts[2] == 'ready':
                        break
                    else:
                        raise AssertionError(
                            "%s not in ('continue', 'ready', 'common)" %
                            parts[2])
            have = graph_walker.next()
        proto.write_pkt_line('done\n')

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
            parts = pkt.rstrip('\n').split(' ')
            if parts[0] == 'ACK':
                graph_walker.ack(pkt.split(' ')[1])
            if len(parts) < 3 or parts[2] not in (
                    'ready', 'continue', 'common'):
                break
            pkt = proto.read_pkt_line()
        if "side-band-64k" in capabilities:
            if progress is None:
                # Just ignore progress data
                progress = lambda x: None
            self._read_side_band64k_data(proto, {1: pack_data, 2: progress})
            # wait for EOF before returning
            data = proto.read()
            if data:
                raise Exception('Unexpected response %r' % data)
        else:
            while True:
                data = self.read(rbufsize)
                if data == "":
                    break
                pack_data(data)


class TraditionalGitClient(GitClient):
    """Traditional Git client."""

    def _connect(self, cmd, path):
        """Create a connection to the server.

        This method is abstract - concrete implementations should
        implement their own variant which connects to the server and
        returns an initialized Protocol object with the service ready
        for use and a can_read function which may be used to see if
        reads would block.

        :param cmd: The git service name to which we should connect.
        :param path: The path we should pass to the service.
        """
        raise NotImplementedError()

    def send_pack(self, path, determine_wants, generate_pack_contents,
                  progress=None):
        """Upload a pack to a remote repository.

        :param path: Repository path
        :param generate_pack_contents: Function that can return a sequence of the
            shas of the objects to upload.
        :param progress: Optional callback called with progress updates

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        """
        proto, unused_can_read = self._connect('receive-pack', path)
        old_refs, server_capabilities = self._read_refs(proto)
        negotiated_capabilities = self._send_capabilities & server_capabilities
        try:
            new_refs = determine_wants(old_refs)
        except:
            proto.write_pkt_line(None)
            raise
        if new_refs is None:
            proto.write_pkt_line(None)
            return old_refs
        (have, want) = self._handle_receive_pack_head(proto,
            negotiated_capabilities, old_refs, new_refs)
        if not want and old_refs == new_refs:
            return new_refs
        objects = generate_pack_contents(have, want)
        if len(objects) > 0:
            entries, sha = write_pack_objects(proto.write_file(), objects)
        self._handle_receive_pack_tail(proto, negotiated_capabilities,
            progress)
        return new_refs

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress=None):
        """Retrieve a pack from a git smart server.

        :param determine_wants: Callback that returns list of commits to fetch
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        """
        proto, can_read = self._connect('upload-pack', path)
        refs, server_capabilities = self._read_refs(proto)
        negotiated_capabilities = self._fetch_capabilities & server_capabilities
        try:
            wants = determine_wants(refs)
        except:
            proto.write_pkt_line(None)
            raise
        if wants is not None:
            wants = [cid for cid in wants if cid != ZERO_SHA]
        if not wants:
            proto.write_pkt_line(None)
            return refs
        self._handle_upload_pack_head(proto, negotiated_capabilities,
            graph_walker, wants, can_read)
        self._handle_upload_pack_tail(proto, negotiated_capabilities,
            graph_walker, pack_data, progress)
        return refs

    def archive(self, path, committish, write_data, progress=None):
        proto, can_read = self._connect('upload-archive', path)
        proto.write_pkt_line("argument %s" % committish)
        proto.write_pkt_line(None)
        pkt = proto.read_pkt_line()
        if pkt == "NACK\n":
            return
        elif pkt == "ACK\n":
            pass
        elif pkt.startswith("ERR "):
            raise GitProtocolError(pkt[4:].rstrip("\n"))
        else:
            raise AssertionError("invalid response %r" % pkt)
        ret = proto.read_pkt_line()
        if ret is not None:
            raise AssertionError("expected pkt tail")
        self._read_side_band64k_data(proto, {1: write_data, 2: progress})


class TCPGitClient(TraditionalGitClient):
    """A Git Client that works over TCP directly (i.e. git://)."""

    def __init__(self, host, port=None, *args, **kwargs):
        if port is None:
            port = TCP_GIT_PORT
        self._host = host
        self._port = port
        TraditionalGitClient.__init__(self, *args, **kwargs)

    def _connect(self, cmd, path):
        sockaddrs = socket.getaddrinfo(self._host, self._port,
            socket.AF_UNSPEC, socket.SOCK_STREAM)
        s = None
        err = socket.error("no address found for %s" % self._host)
        for (family, socktype, proto, canonname, sockaddr) in sockaddrs:
            s = socket.socket(family, socktype, proto)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                s.connect(sockaddr)
                break
            except socket.error, err:
                if s is not None:
                    s.close()
                s = None
        if s is None:
            raise err
        # -1 means system default buffering
        rfile = s.makefile('rb', -1)
        # 0 means unbuffered
        wfile = s.makefile('wb', 0)
        proto = Protocol(rfile.read, wfile.write,
                         report_activity=self._report_activity)
        if path.startswith("/~"):
            path = path[1:]
        proto.send_cmd('git-%s' % cmd, path, 'host=%s' % self._host)
        return proto, lambda: _fileno_can_read(s)


class SubprocessWrapper(object):
    """A socket-like object that talks to a subprocess via pipes."""

    def __init__(self, proc):
        self.proc = proc
        self.read = proc.stdout.read
        self.write = proc.stdin.write

    def can_read(self):
        if subprocess.mswindows:
            from msvcrt import get_osfhandle
            from win32pipe import PeekNamedPipe
            handle = get_osfhandle(self.proc.stdout.fileno())
            return PeekNamedPipe(handle, 0)[2] != 0
        else:
            return _fileno_can_read(self.proc.stdout.fileno())

    def close(self):
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.proc.wait()


class SubprocessGitClient(TraditionalGitClient):
    """Git client that talks to a server using a subprocess."""

    def __init__(self, *args, **kwargs):
        self._connection = None
        self._stderr = None
        self._stderr = kwargs.get('stderr')
        if 'stderr' in kwargs:
            del kwargs['stderr']
        TraditionalGitClient.__init__(self, *args, **kwargs)

    def _connect(self, service, path):
        import subprocess
        argv = ['git', service, path]
        p = SubprocessWrapper(
            subprocess.Popen(argv, bufsize=0, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=self._stderr))
        return Protocol(p.read, p.write,
                        report_activity=self._report_activity), p.can_read


class SSHVendor(object):

    def connect_ssh(self, host, command, username=None, port=None):
        import subprocess
        #FIXME: This has no way to deal with passwords..
        args = ['ssh', '-x']
        if port is not None:
            args.extend(['-p', str(port)])
        if username is not None:
            host = '%s@%s' % (username, host)
        args.append(host)
        proc = subprocess.Popen(args + command,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        return SubprocessWrapper(proc)

# Can be overridden by users
get_ssh_vendor = SSHVendor


class SSHGitClient(TraditionalGitClient):

    def __init__(self, host, port=None, username=None, *args, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        TraditionalGitClient.__init__(self, *args, **kwargs)
        self.alternative_paths = {}

    def _get_cmd_path(self, cmd):
        return self.alternative_paths.get(cmd, 'git-%s' % cmd)

    def _connect(self, cmd, path):
        con = get_ssh_vendor().connect_ssh(
            self.host, ["%s '%s'" % (self._get_cmd_path(cmd), path)],
            port=self.port, username=self.username)
        return (Protocol(con.read, con.write, report_activity=self._report_activity),
                con.can_read)


class HttpGitClient(GitClient):

    def __init__(self, base_url, dumb=None, *args, **kwargs):
        self.base_url = base_url.rstrip("/") + "/"
        self.dumb = dumb
        GitClient.__init__(self, *args, **kwargs)

    def _get_url(self, path):
        return urlparse.urljoin(self.base_url, path).rstrip("/") + "/"

    def _perform(self, req):
        """Perform a HTTP request.

        This is provided so subclasses can provide their own version.

        :param req: urllib2.Request instance
        :return: matching response
        """
        return urllib2.urlopen(req)

    def _discover_references(self, service, url):
        assert url[-1] == "/"
        url = urlparse.urljoin(url, "info/refs")
        headers = {}
        if self.dumb != False:
            url += "?service=%s" % service
            headers["Content-Type"] = "application/x-%s-request" % service
        req = urllib2.Request(url, headers=headers)
        resp = self._perform(req)
        if resp.getcode() == 404:
            raise NotGitRepository()
        if resp.getcode() != 200:
            raise GitProtocolError("unexpected http response %d" %
                resp.getcode())
        self.dumb = (not resp.info().gettype().startswith("application/x-git-"))
        proto = Protocol(resp.read, None)
        if not self.dumb:
            # The first line should mention the service
            pkts = list(proto.read_pkt_seq())
            if pkts != [('# service=%s\n' % service)]:
                raise GitProtocolError(
                    "unexpected first line %r from smart server" % pkts)
        return self._read_refs(proto)

    def _smart_request(self, service, url, data):
        assert url[-1] == "/"
        url = urlparse.urljoin(url, service)
        req = urllib2.Request(url,
            headers={"Content-Type": "application/x-%s-request" % service},
            data=data)
        resp = self._perform(req)
        if resp.getcode() == 404:
            raise NotGitRepository()
        if resp.getcode() != 200:
            raise GitProtocolError("Invalid HTTP response from server: %d"
                % resp.getcode())
        if resp.info().gettype() != ("application/x-%s-result" % service):
            raise GitProtocolError("Invalid content-type from server: %s"
                % resp.info().gettype())
        return resp

    def send_pack(self, path, determine_wants, generate_pack_contents,
                  progress=None):
        """Upload a pack to a remote repository.

        :param path: Repository path
        :param generate_pack_contents: Function that can return a sequence of the
            shas of the objects to upload.
        :param progress: Optional progress function

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        """
        url = self._get_url(path)
        old_refs, server_capabilities = self._discover_references(
            "git-receive-pack", url)
        negotiated_capabilities = self._send_capabilities & server_capabilities
        new_refs = determine_wants(old_refs)
        if new_refs is None:
            return old_refs
        if self.dumb:
            raise NotImplementedError(self.fetch_pack)
        req_data = StringIO()
        req_proto = Protocol(None, req_data.write)
        (have, want) = self._handle_receive_pack_head(
            req_proto, negotiated_capabilities, old_refs, new_refs)
        if not want and old_refs == new_refs:
            return new_refs
        objects = generate_pack_contents(have, want)
        if len(objects) > 0:
            entries, sha = write_pack_objects(req_proto.write_file(), objects)
        resp = self._smart_request("git-receive-pack", url,
            data=req_data.getvalue())
        resp_proto = Protocol(resp.read, None)
        self._handle_receive_pack_tail(resp_proto, negotiated_capabilities,
            progress)
        return new_refs

    def fetch_pack(self, path, determine_wants, graph_walker, pack_data,
                   progress=None):
        """Retrieve a pack from a git smart server.

        :param determine_wants: Callback that returns list of commits to fetch
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        :return: Dictionary with the refs of the remote repository
        """
        url = self._get_url(path)
        refs, server_capabilities = self._discover_references(
            "git-upload-pack", url)
        negotiated_capabilities = server_capabilities
        wants = determine_wants(refs)
        if wants is not None:
            wants = [cid for cid in wants if cid != ZERO_SHA]
        if not wants:
            return refs
        if self.dumb:
            raise NotImplementedError(self.send_pack)
        req_data = StringIO()
        req_proto = Protocol(None, req_data.write)
        self._handle_upload_pack_head(req_proto,
            negotiated_capabilities, graph_walker, wants,
            lambda: False)
        resp = self._smart_request("git-upload-pack", url,
            data=req_data.getvalue())
        resp_proto = Protocol(resp.read, None)
        self._handle_upload_pack_tail(resp_proto, negotiated_capabilities,
            graph_walker, pack_data, progress)
        return refs


def get_transport_and_path(uri, **kwargs):
    """Obtain a git client from a URI or path.

    :param uri: URI or path
    :param thin_packs: Whether or not thin packs should be retrieved
    :param report_activity: Optional callback for reporting transport
        activity.
    :return: Tuple with client instance and relative path.
    """
    parsed = urlparse.urlparse(uri)
    if parsed.scheme == 'git':
        return (TCPGitClient(parsed.hostname, port=parsed.port, **kwargs),
                parsed.path)
    elif parsed.scheme == 'git+ssh':
        return SSHGitClient(parsed.hostname, port=parsed.port,
                            username=parsed.username, **kwargs), parsed.path
    elif parsed.scheme in ('http', 'https'):
        return HttpGitClient(urlparse.urlunparse(parsed)), parsed.path

    if parsed.scheme and not parsed.netloc:
        # SSH with no user@, zero or one leading slash.
        return SSHGitClient(parsed.scheme, **kwargs), parsed.path
    elif parsed.scheme:
        raise ValueError('Unknown git protocol scheme: %s' % parsed.scheme)
    elif '@' in parsed.path and ':' in parsed.path:
        # SSH with user@host:foo.
        user_host, path = parsed.path.split(':')
        user, host = user_host.rsplit('@')
        return SSHGitClient(host, username=user, **kwargs), path

    # Otherwise, assume it's a local path.
    return SubprocessGitClient(**kwargs), uri
