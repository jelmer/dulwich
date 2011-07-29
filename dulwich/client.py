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

"""Client side support for the Git protocol."""

__docformat__ = 'restructuredText'

import select
import socket
import subprocess
import urlparse

from dulwich.errors import (
    GitProtocolError,
    SendPackError,
    UpdateRefsError,
    )
from dulwich.protocol import (
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

COMMON_CAPABILITIES = ['ofs-delta']
FETCH_CAPABILITIES = ['multi_ack', 'side-band-64k'] + COMMON_CAPABILITIES
SEND_CAPABILITIES = ['report-status'] + COMMON_CAPABILITIES

# TODO(durin42): this doesn't correctly degrade if the server doesn't
# support some capabilities. This should work properly with servers
# that don't support side-band-64k and multi_ack.
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
        self._fetch_capabilities = list(FETCH_CAPABILITIES)
        self._send_capabilities = list(SEND_CAPABILITIES)
        if thin_packs:
            self._fetch_capabilities.append('thin-pack')

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
        return refs, server_capabilities

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


    # TODO(durin42): add side-band-64k capability support here and advertise it
    def send_pack(self, path, determine_wants, generate_pack_contents):
        """Upload a pack to a remote repository.

        :param path: Repository path
        :param generate_pack_contents: Function that can return a sequence of the
            shas of the objects to upload.

        :raises SendPackError: if server rejects the pack data
        :raises UpdateRefsError: if the server supports report-status
                                 and rejects ref updates
        """
        proto, unused_can_read = self._connect('receive-pack', path)
        old_refs, server_capabilities = self._read_refs(proto)
        if 'report-status' not in server_capabilities:
            self._send_capabilities.remove('report-status')
        new_refs = determine_wants(old_refs)
        if not new_refs:
            proto.write_pkt_line(None)
            return {}
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
                                        ' '.join(self._send_capabilities)))
                    sent_capabilities = True
            if new_sha1 not in have and new_sha1 != ZERO_SHA:
                want.append(new_sha1)
        proto.write_pkt_line(None)
        if not want:
            return new_refs
        objects = generate_pack_contents(have, want)
        entries, sha = write_pack_objects(proto.write_file(), objects)

        if 'report-status' in self._send_capabilities:
            self._parse_status_report(proto)
        # wait for EOF before returning
        data = proto.read()
        if data:
            raise SendPackError('Unexpected response %r' % data)
        return new_refs

    def fetch(self, path, target, determine_wants=None, progress=None):
        """Fetch into a target repository.

        :param path: Path to fetch from
        :param target: Target repository to fetch into
        :param determine_wants: Optional function to determine what refs
            to fetch
        :param progress: Optional progress function
        :return: remote refs
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
                   progress):
        """Retrieve a pack from a git smart server.

        :param determine_wants: Callback that returns list of commits to fetch
        :param graph_walker: Object with next() and ack().
        :param pack_data: Callback called for each bit of data in the pack
        :param progress: Callback for progress reports (strings)
        """
        proto, can_read = self._connect('upload-pack', path)
        (refs, server_capabilities) = self._read_refs(proto)
        wants = determine_wants(refs)
        if not wants:
            proto.write_pkt_line(None)
            return refs
        assert isinstance(wants, list) and type(wants[0]) == str
        proto.write_pkt_line('want %s %s\n' % (
            wants[0], ' '.join(self._fetch_capabilities)))
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
                    assert parts[2] == 'continue'
            have = graph_walker.next()
        proto.write_pkt_line('done\n')
        pkt = proto.read_pkt_line()
        while pkt:
            parts = pkt.rstrip('\n').split(' ')
            if parts[0] == 'ACK':
                graph_walker.ack(pkt.split(' ')[1])
            if len(parts) < 3 or parts[2] != 'continue':
                break
            pkt = proto.read_pkt_line()
        # TODO(durin42): this is broken if the server didn't support the
        # side-band-64k capability.
        for pkt in proto.read_pkt_seq():
            channel = ord(pkt[0])
            pkt = pkt[1:]
            if channel == 1:
                pack_data(pkt)
            elif channel == 2:
                if progress is not None:
                    progress(pkt)
            else:
                raise AssertionError('Invalid sideband channel %d' % channel)
        return refs


class TCPGitClient(GitClient):
    """A Git Client that works over TCP directly (i.e. git://)."""

    def __init__(self, host, port=None, *args, **kwargs):
        if port is None:
            port = TCP_GIT_PORT
        self._host = host
        self._port = port
        GitClient.__init__(self, *args, **kwargs)

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


class SubprocessGitClient(GitClient):
    """Git client that talks to a server using a subprocess."""

    def __init__(self, *args, **kwargs):
        self._connection = None
        GitClient.__init__(self, *args, **kwargs)

    def _connect(self, service, path):
        import subprocess
        argv = ['git', service, path]
        p = SubprocessWrapper(
            subprocess.Popen(argv, bufsize=0, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE))
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


class SSHGitClient(GitClient):

    def __init__(self, host, port=None, username=None, *args, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        GitClient.__init__(self, *args, **kwargs)
        self.alternative_paths = {}

    def _get_cmd_path(self, cmd):
        return self.alternative_paths.get(cmd, 'git-%s' % cmd)

    def _connect(self, cmd, path):
        con = get_ssh_vendor().connect_ssh(
            self.host, ["%s '%s'" % (self._get_cmd_path(cmd), path)],
            port=self.port, username=self.username)
        return (Protocol(con.read, con.write, report_activity=self._report_activity),
                con.can_read)


def get_transport_and_path(uri):
    """Obtain a git client from a URI or path.

    :param uri: URI or path
    :return: Tuple with client instance and relative path.
    """
    parsed = urlparse.urlparse(uri)
    if parsed.scheme == 'git':
        return TCPGitClient(parsed.hostname, port=parsed.port), parsed.path
    elif parsed.scheme == 'git+ssh':
        return SSHGitClient(parsed.hostname, port=parsed.port,
                            username=parsed.username), parsed.path

    if parsed.scheme and not parsed.netloc:
        # SSH with no user@, zero or one leading slash.
        return SSHGitClient(parsed.scheme), parsed.path
    elif parsed.scheme:
        raise ValueError('Unknown git protocol scheme: %s' % parsed.scheme)
    elif '@' in parsed.path and ':' in parsed.path:
        # SSH with user@host:foo.
        user_host, path = parsed.path.split(':')
        user, host = user_host.rsplit('@')
        return SSHGitClient(host, username=user), path

    # Otherwise, assume it's a local path.
    return SubprocessGitClient(), uri
