# server.py -- Implementation of the server side git protocols
# Copryight (C) 2008 John Carr <john.carr@unrouted.co.uk>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# or (at your option) any later version of the License.
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


"""Git smart network protocol server implementation.

For more detailed implementation on the network protocol, see the
Documentation/technical directory in the cgit distribution, and in particular:
    Documentation/technical/protocol-capabilities.txt
    Documentation/technical/pack-protocol.txt
"""


import collections
import SocketServer
import tempfile

from dulwich.errors import (
    ApplyDeltaError,
    ChecksumMismatch,
    GitProtocolError,
    )
from dulwich.objects import (
    hex_to_sha,
    )
from dulwich.protocol import (
    Protocol,
    ProtocolFile,
    TCP_GIT_PORT,
    extract_capabilities,
    extract_want_line_capabilities,
    SINGLE_ACK,
    MULTI_ACK,
    MULTI_ACK_DETAILED,
    ack_type,
    )
from dulwich.repo import (
    Repo,
    )
from dulwich.pack import (
    write_pack_data,
    )

class Backend(object):

    def get_refs(self):
        """
        Get all the refs in the repository

        :return: dict of name -> sha
        """
        raise NotImplementedError

    def apply_pack(self, refs, read):
        """ Import a set of changes into a repository and update the refs

        :param refs: list of tuple(name, sha)
        :param read: callback to read from the incoming pack
        """
        raise NotImplementedError

    def fetch_objects(self, determine_wants, graph_walker, progress):
        """
        Yield the objects required for a list of commits.

        :param progress: is a callback to send progress messages to the client
        """
        raise NotImplementedError


class GitBackend(Backend):

    def __init__(self, repo=None):
        if repo is None:
            repo = Repo(tmpfile.mkdtemp())
        self.repo = repo
        self.object_store = self.repo.object_store
        self.fetch_objects = self.repo.fetch_objects
        self.get_refs = self.repo.get_refs

    def apply_pack(self, refs, read):
        f, commit = self.repo.object_store.add_thin_pack()
        all_exceptions = (IOError, OSError, ChecksumMismatch, ApplyDeltaError)
        status = []
        unpack_error = None
        # TODO: more informative error messages than just the exception string
        try:
            # TODO: decode the pack as we stream to avoid blocking reads beyond
            # the end of data (when using HTTP/1.1 chunked encoding)
            while True:
                data = read(10240)
                if not data:
                    break
                f.write(data)
        except all_exceptions, e:
            unpack_error = str(e).replace('\n', '')
        try:
            commit()
        except all_exceptions, e:
            if not unpack_error:
                unpack_error = str(e).replace('\n', '')

        if unpack_error:
            status.append(('unpack', unpack_error))
        else:
            status.append(('unpack', 'ok'))

        for oldsha, sha, ref in refs:
            # TODO: check refname
            ref_error = None
            try:
                if ref == "0" * 40:
                    try:
                        del self.repo.refs[ref]
                    except all_exceptions:
                        ref_error = 'failed to delete'
                else:
                    try:
                        self.repo.refs[ref] = sha
                    except all_exceptions:
                        ref_error = 'failed to write'
            except KeyError, e:
                ref_error = 'bad ref'
            if ref_error:
                status.append((ref, ref_error))
            else:
                status.append((ref, 'ok'))


        print "pack applied"
        return status


class Handler(object):
    """Smart protocol command handler base class."""

    def __init__(self, backend, read, write):
        self.backend = backend
        self.proto = Protocol(read, write)
        self._client_capabilities = None

    def capability_line(self):
        return " ".join(self.capabilities())

    def capabilities(self):
        raise NotImplementedError(self.capabilities)

    def set_client_capabilities(self, caps):
        my_caps = self.capabilities()
        for cap in caps:
            if cap not in my_caps:
                raise GitProtocolError('Client asked for capability %s that '
                                       'was not advertised.' % cap)
        self._client_capabilities = caps

    def has_capability(self, cap):
        if self._client_capabilities is None:
            raise GitProtocolError('Server attempted to access capability %s '
                                   'before asking client' % cap)
        return cap in self._client_capabilities


class UploadPackHandler(Handler):
    """Protocol handler for uploading a pack to the server."""

    def __init__(self, backend, read, write,
                 stateless_rpc=False, advertise_refs=False):
        Handler.__init__(self, backend, read, write)
        self._graph_walker = None
        self.stateless_rpc = stateless_rpc
        self.advertise_refs = advertise_refs

    def capabilities(self):
        return ("multi_ack_detailed", "multi_ack", "side-band-64k", "thin-pack",
                "ofs-delta")

    def handle(self):

        progress = lambda x: self.proto.write_sideband(2, x)
        write = lambda x: self.proto.write_sideband(1, x)

        graph_walker = ProtocolGraphWalker(self)
        objects_iter = self.backend.fetch_objects(
          graph_walker.determine_wants, graph_walker, progress)

        # Do they want any objects?
        if len(objects_iter) == 0:
            return

        progress("dul-daemon says what\n")
        progress("counting objects: %d, done.\n" % len(objects_iter))
        write_pack_data(ProtocolFile(None, write), objects_iter, 
                        len(objects_iter))
        progress("how was that, then?\n")
        # we are done
        self.proto.write("0000")


class ProtocolGraphWalker(object):
    """A graph walker that knows the git protocol.

    As a graph walker, this class implements ack(), next(), and reset(). It also
    contains some base methods for interacting with the wire and walking the
    commit tree.

    The work of determining which acks to send is passed on to the
    implementation instance stored in _impl. The reason for this is that we do
    not know at object creation time what ack level the protocol requires. A
    call to set_ack_level() is required to set up the implementation, before any
    calls to next() or ack() are made.
    """
    def __init__(self, handler):
        self.handler = handler
        self.store = handler.backend.object_store
        self.proto = handler.proto
        self.stateless_rpc = handler.stateless_rpc
        self.advertise_refs = handler.advertise_refs
        self._wants = []
        self._cached = False
        self._cache = []
        self._cache_index = 0
        self._impl = None

    def determine_wants(self, heads):
        """Determine the wants for a set of heads.

        The given heads are advertised to the client, who then specifies which
        refs he wants using 'want' lines. This portion of the protocol is the
        same regardless of ack type, and in fact is used to set the ack type of
        the ProtocolGraphWalker.

        :param heads: a dict of refname->SHA1 to advertise
        :return: a list of SHA1s requested by the client
        """
        if not heads:
            raise GitProtocolError('No heads found')
        values = set(heads.itervalues())
        if self.advertise_refs or not self.stateless_rpc:
            for i, (ref, sha) in enumerate(heads.iteritems()):
                line = "%s %s" % (sha, ref)
                if not i:
                    line = "%s\x00%s" % (line, self.handler.capability_line())
                self.proto.write_pkt_line("%s\n" % line)
                # TODO: include peeled value of any tags

            # i'm done..
            self.proto.write_pkt_line(None)

            if self.advertise_refs:
                return []

        # Now client will sending want want want commands
        want = self.proto.read_pkt_line()
        if not want:
            return []
        line, caps = extract_want_line_capabilities(want)
        self.handler.set_client_capabilities(caps)
        self.set_ack_type(ack_type(caps))
        command, sha = self._split_proto_line(line)

        want_revs = []
        while command != None:
            if command != 'want':
                raise GitProtocolError(
                    'Protocol got unexpected command %s' % command)
            if sha not in values:
                raise GitProtocolError(
                    'Client wants invalid object %s' % sha)
            want_revs.append(sha)
            command, sha = self.read_proto_line()

        self.set_wants(want_revs)
        return want_revs

    def ack(self, have_ref):
        return self._impl.ack(have_ref)

    def reset(self):
        self._cached = True
        self._cache_index = 0

    def next(self):
        if not self._cached:
            if not self._impl and self.stateless_rpc:
                return None
            return self._impl.next()
        self._cache_index += 1
        if self._cache_index > len(self._cache):
            return None
        return self._cache[self._cache_index]

    def _split_proto_line(self, line):
        fields = line.rstrip('\n').split(' ', 1)
        if len(fields) == 1 and fields[0] == 'done':
            return ('done', None)
        elif len(fields) == 2 and fields[0] in ('want', 'have'):
            try:
                hex_to_sha(fields[1])
                return tuple(fields)
            except (TypeError, AssertionError), e:
                raise GitProtocolError(e)
        raise GitProtocolError('Received invalid line from client:\n%s' % line)

    def read_proto_line(self):
        """Read a line from the wire.

        :return: a tuple having one of the following forms:
            ('want', obj_id)
            ('have', obj_id)
            ('done', None)
            (None, None)  (for a flush-pkt)

        :raise GitProtocolError: if the line cannot be parsed into one of the
            possible return values.
        """
        line = self.proto.read_pkt_line()
        if not line:
            return (None, None)
        return self._split_proto_line(line)

    def send_ack(self, sha, ack_type=''):
        if ack_type:
            ack_type = ' %s' % ack_type
        self.proto.write_pkt_line('ACK %s%s\n' % (sha, ack_type))

    def send_nak(self):
        self.proto.write_pkt_line('NAK\n')

    def set_wants(self, wants):
        self._wants = wants

    def _is_satisfied(self, haves, want, earliest):
        """Check whether a want is satisfied by a set of haves.

        A want, typically a branch tip, is "satisfied" only if there exists a
        path back from that want to one of the haves.

        :param haves: A set of commits we know the client has.
        :param want: The want to check satisfaction for.
        :param earliest: A timestamp beyond which the search for haves will be
            terminated, presumably because we're searching too far down the
            wrong branch.
        """
        o = self.store[want]
        pending = collections.deque([o])
        while pending:
            commit = pending.popleft()
            if commit.id in haves:
                return True
            if not getattr(commit, 'get_parents', None):
                # non-commit wants are assumed to be satisfied
                continue
            for parent in commit.get_parents():
                parent_obj = self.store[parent]
                # TODO: handle parents with later commit times than children
                if parent_obj.commit_time >= earliest:
                    pending.append(parent_obj)
        return False

    def all_wants_satisfied(self, haves):
        """Check whether all the current wants are satisfied by a set of haves.

        :param haves: A set of commits we know the client has.
        :note: Wants are specified with set_wants rather than passed in since
            in the current interface they are determined outside this class.
        """
        haves = set(haves)
        earliest = min([self.store[h].commit_time for h in haves])
        for want in self._wants:
            if not self._is_satisfied(haves, want, earliest):
                return False
        return True

    def set_ack_type(self, ack_type):
        impl_classes = {
            MULTI_ACK: MultiAckGraphWalkerImpl,
            MULTI_ACK_DETAILED: MultiAckDetailedGraphWalkerImpl,
            SINGLE_ACK: SingleAckGraphWalkerImpl,
            }
        self._impl = impl_classes[ack_type](self)


class SingleAckGraphWalkerImpl(object):
    """Graph walker implementation that speaks the single-ack protocol."""

    def __init__(self, walker):
        self.walker = walker
        self._sent_ack = False

    def ack(self, have_ref):
        if not self._sent_ack:
            self.walker.send_ack(have_ref)
            self._sent_ack = True

    def next(self):
        command, sha = self.walker.read_proto_line()
        if command in (None, 'done'):
            if not self._sent_ack:
                self.walker.send_nak()
            return None
        elif command == 'have':
            return sha


class MultiAckGraphWalkerImpl(object):
    """Graph walker implementation that speaks the multi-ack protocol."""

    def __init__(self, walker):
        self.walker = walker
        self._found_base = False
        self._common = []

    def ack(self, have_ref):
        self._common.append(have_ref)
        if not self._found_base:
            self.walker.send_ack(have_ref, 'continue')
            if self.walker.all_wants_satisfied(self._common):
                self._found_base = True
        # else we blind ack within next

    def next(self):
        while True:
            command, sha = self.walker.read_proto_line()
            if command is None:
                self.walker.send_nak()
                # in multi-ack mode, a flush-pkt indicates the client wants to
                # flush but more have lines are still coming
                continue
            elif command == 'done':
                # don't nak unless no common commits were found, even if not
                # everything is satisfied
                if self._common:
                    self.walker.send_ack(self._common[-1])
                else:
                    self.walker.send_nak()
                return None
            elif command == 'have':
                if self._found_base:
                    # blind ack
                    self.walker.send_ack(sha, 'continue')
                return sha


class MultiAckDetailedGraphWalkerImpl(object):
    """Graph walker implementation speaking the multi-ack-detailed protocol."""

    def __init__(self, walker):
        self.walker = walker
        self._found_base = False
        self._common = []

    def ack(self, have_ref):
        self._common.append(have_ref)
        if not self._found_base:
            self.walker.send_ack(have_ref, 'common')
            if self.walker.all_wants_satisfied(self._common):
                self._found_base = True
                self.walker.send_ack(have_ref, 'ready')
        # else we blind ack within next

    def next(self):
        while True:
            command, sha = self.walker.read_proto_line()
            if command is None:
                self.walker.send_nak()
                if self.walker.stateless_rpc:
                    return None
                continue
            elif command == 'done':
                # don't nak unless no common commits were found, even if not
                # everything is satisfied
                if self._common:
                    self.walker.send_ack(self._common[-1])
                else:
                    self.walker.send_nak()
                return None
            elif command == 'have':
                if self._found_base:
                    # blind ack; can happen if the client has more requests
                    # inflight
                    self.walker.send_ack(sha, 'ready')
                return sha


class ReceivePackHandler(Handler):
    """Protocol handler for downloading a pack from the client."""

    def __init__(self, backend, read, write,
                 stateless_rpc=False, advertise_refs=False):
        Handler.__init__(self, backend, read, write)
        self.stateless_rpc = stateless_rpc
        self.advertise_refs = advertise_refs

    def __init__(self, backend, read, write,
                 stateless_rpc=False, advertise_refs=False):
        Handler.__init__(self, backend, read, write)
        self._stateless_rpc = stateless_rpc
        self._advertise_refs = advertise_refs

    def capabilities(self):
        return ("report-status", "delete-refs")

    def handle(self):
        refs = self.backend.get_refs().items()

        if self.advertise_refs or not self.stateless_rpc:
            if refs:
                self.proto.write_pkt_line(
                    "%s %s\x00%s\n" % (refs[0][1], refs[0][0],
                                       self.capability_line()))
                for i in range(1, len(refs)):
                    ref = refs[i]
                    self.proto.write_pkt_line("%s %s\n" % (ref[1], ref[0]))
            else:
                self.proto.write_pkt_line("0000000000000000000000000000000000000000 capabilities^{} %s" % self.capability_line())

            self.proto.write("0000")
            if self.advertise_refs:
                return

        client_refs = []
        ref = self.proto.read_pkt_line()

        # if ref is none then client doesnt want to send us anything..
        if ref is None:
            return

        ref, caps = extract_capabilities(ref)
        self.set_client_capabilities(caps)

        # client will now send us a list of (oldsha, newsha, ref)
        while ref:
            client_refs.append(ref.split())
            ref = self.proto.read_pkt_line()

        # backend can now deal with this refs and read a pack using self.read
        status = self.backend.apply_pack(client_refs, self.proto.read)

        # when we have read all the pack from the client, send a status report
        # if the client asked for it
        if self.has_capability('report-status'):
            for name, msg in status:
                if name == 'unpack':
                    self.proto.write_pkt_line('unpack %s\n' % msg)
                elif msg == 'ok':
                    self.proto.write_pkt_line('ok %s\n' % name)
                else:
                    self.proto.write_pkt_line('ng %s %s\n' % (name, msg))
            self.proto.write_pkt_line(None)


class TCPGitRequestHandler(SocketServer.StreamRequestHandler):

    def handle(self):
        proto = Protocol(self.rfile.read, self.wfile.write)
        command, args = proto.read_cmd()

        # switch case to handle the specific git command
        if command == 'git-upload-pack':
            cls = UploadPackHandler
        elif command == 'git-receive-pack':
            cls = ReceivePackHandler
        else:
            return

        h = cls(self.server.backend, self.rfile.read, self.wfile.write)
        h.handle()


class TCPGitServer(SocketServer.TCPServer):

    allow_reuse_address = True
    serve = SocketServer.TCPServer.serve_forever

    def __init__(self, backend, listen_addr, port=TCP_GIT_PORT):
        self.backend = backend
        SocketServer.TCPServer.__init__(self, (listen_addr, port), TCPGitRequestHandler)
