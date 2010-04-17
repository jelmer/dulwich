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
from cStringIO import StringIO
import os
import SocketServer

from dulwich.errors import (
    ApplyDeltaError,
    ChecksumMismatch,
    GitProtocolError,
    )
from dulwich.misc import (
    make_sha,
    )
from dulwich.objects import (
    hex_to_sha,
    sha_to_hex,
    )
from dulwich.pack import (
    read_pack_header,
    unpack_object,
    write_pack_data,
    )
from dulwich.protocol import (
    MULTI_ACK,
    MULTI_ACK_DETAILED,
    ProtocolFile,
    ReceivableProtocol,
    SINGLE_ACK,
    TCP_GIT_PORT,
    ZERO_SHA,
    extract_capabilities,
    extract_want_line_capabilities,
    ack_type,
    )


class Backend(object):
    """A backend for the Git smart server implementation."""

    def open_repository(self, path):
        """Open the repository at a path."""
        raise NotImplementedError(self.open_repository)


class BackendRepo(object):
    """Repository abstraction used by the Git server.
    
    Please note that the methods required here are a 
    subset of those provided by dulwich.repo.Repo.
    """

    object_store = None
    refs = None

    def get_refs(self):
        """
        Get all the refs in the repository

        :return: dict of name -> sha
        """
        raise NotImplementedError

    def get_peeled(self, name):
        """Return the cached peeled value of a ref, if available.

        :param name: Name of the ref to peel
        :return: The peeled value of the ref. If the ref is known not point to
            a tag, this will be the SHA the ref refers to. If no cached
            information about a tag is available, this method may return None,
            but it should attempt to peel the tag if possible.
        """
        return None

    def fetch_objects(self, determine_wants, graph_walker, progress,
                      get_tagged=None):
        """
        Yield the objects required for a list of commits.

        :param progress: is a callback to send progress messages to the client
        :param get_tagged: Function that returns a dict of pointed-to sha -> tag
            sha for including tags.
        """
        raise NotImplementedError


class PackStreamReader(object):
    """Class to read a pack stream.

    The pack is read from a ReceivableProtocol using read() or recv() as
    appropriate.
    """

    def __init__(self, read_all, read_some=None):
        self.read_all = read_all
        if read_some is None:
            self.read_some = read_all
        else:
            self.read_some = read_some
        self.sha = make_sha()
        self._rbuf = StringIO()
        # trailer is a deque to avoid memory allocation on small reads
        self._trailer = collections.deque()

    def _read(self, read, size):
        """Read up to size bytes using the given callback.

        As a side effect, update the verifier's hash (excluding the last 20
        bytes read) and write through to the output file.

        :param read: The read callback to read from.
        :param size: The maximum number of bytes to read; the particular
            behavior is callback-specific.
        """
        data = read(size)

        # maintain a trailer of the last 20 bytes we've read
        n = len(data)
        tn = len(self._trailer)
        if n >= 20:
            to_pop = tn
            to_add = 20
        else:
            to_pop = max(n + tn - 20, 0)
            to_add = n
        for _ in xrange(to_pop):
            self.sha.update(self._trailer.popleft())
        self._trailer.extend(data[-to_add:])

        # hash everything but the trailer
        self.sha.update(data[:-to_add])
        return data

    def _buf_len(self):
        buf = self._rbuf
        start = buf.tell()
        buf.seek(0, os.SEEK_END)
        end = buf.tell()
        buf.seek(start)
        return end - start

    def read(self, size):
        """Read, blocking until size bytes are read."""
        buf_len = self._buf_len()
        if buf_len >= size:
            return self._rbuf.read(size)
        buf_data = self._rbuf.read()
        self._rbuf = StringIO()
        return buf_data + self._read(self.read_all, size - buf_len)

    def recv(self, size):
        """Read up to size bytes, blocking until one byte is read."""
        buf_len = self._buf_len()
        if buf_len:
            data = self._rbuf.read(size)
            if size >= buf_len:
                self._rbuf = StringIO()
            return data
        return self._read(self.read_some, size)

    def read_objects(self):
        """Read the objects in this pack file.

        :raise AssertionError: if there is an error in the pack format.
        :raise ChecksumMismatch: if the checksum of the pack contents does not
            match the checksum in the pack trailer.
        :raise zlib.error: if an error occurred during zlib decompression.
        :raise IOError: if an error occurred writing to the output file.
        """
        pack_version, num_objects = read_pack_header(self.read)
        for i in xrange(num_objects):
            type, uncomp, comp_len, unused = unpack_object(self.read, self.recv)
            yield type, uncomp, comp_len

            # prepend any unused data to current read buffer
            buf = StringIO()
            buf.write(unused)
            buf.write(self._rbuf.read())
            buf.seek(0)
            self._rbuf = buf

        pack_sha = sha_to_hex(''.join([c for c in self._trailer]))
        calculated_sha = self.sha.hexdigest()
        if pack_sha != calculated_sha:
            raise ChecksumMismatch(pack_sha, calculated_sha)


class PackStreamCopier(PackStreamReader):
    """Class to verify a pack stream as it is being read.

    The pack is read from a ReceivableProtocol using read() or recv() as
    appropriate and written out to the given file-like object.
    """

    def __init__(self, read_all, read_some, outfile):
        super(PackStreamCopier, self).__init__(read_all, read_some)
        self.outfile = outfile

    def _read(self, read, size):
        data = super(PackStreamCopier, self)._read(read, size)
        self.outfile.write(data)
        return data

    def verify(self):
        """Verify a pack stream and write it to the output file.

        See PackStreamReader.iterobjects for a list of exceptions this may throw.
        """
        for _, _, _, _ in self.iterobjects():
            pass


class DictBackend(Backend):
    """Trivial backend that looks up Git repositories in a dictionary."""

    def __init__(self, repos):
        self.repos = repos

    def open_repository(self, path):
        # FIXME: What to do in case there is no repo ?
        return self.repos[path]


class Handler(object):
    """Smart protocol command handler base class."""

    def __init__(self, backend, proto):
        self.backend = backend
        self.proto = proto
        self._client_capabilities = None

    def capability_line(self):
        return " ".join(self.capabilities())

    def capabilities(self):
        raise NotImplementedError(self.capabilities)

    def innocuous_capabilities(self):
        return ("include-tag", "thin-pack", "no-progress", "ofs-delta")

    def required_capabilities(self):
        """Return a list of capabilities that we require the client to have."""
        return []

    def set_client_capabilities(self, caps):
        allowable_caps = set(self.innocuous_capabilities())
        allowable_caps.update(self.capabilities())
        for cap in caps:
            if cap not in allowable_caps:
                raise GitProtocolError('Client asked for capability %s that '
                                       'was not advertised.' % cap)
        for cap in self.required_capabilities():
            if cap not in caps:
                raise GitProtocolError('Client does not support required '
                                       'capability %s.' % cap)
        self._client_capabilities = set(caps)

    def has_capability(self, cap):
        if self._client_capabilities is None:
            raise GitProtocolError('Server attempted to access capability %s '
                                   'before asking client' % cap)
        return cap in self._client_capabilities


class UploadPackHandler(Handler):
    """Protocol handler for uploading a pack to the server."""

    def __init__(self, backend, args, proto,
                 stateless_rpc=False, advertise_refs=False):
        Handler.__init__(self, backend, proto)
        self.repo = backend.open_repository(args[0])
        self._graph_walker = None
        self.stateless_rpc = stateless_rpc
        self.advertise_refs = advertise_refs

    def capabilities(self):
        return ("multi_ack_detailed", "multi_ack", "side-band-64k", "thin-pack",
                "ofs-delta", "no-progress", "include-tag")

    def required_capabilities(self):
        return ("side-band-64k", "thin-pack", "ofs-delta")

    def progress(self, message):
        if self.has_capability("no-progress"):
            return
        self.proto.write_sideband(2, message)

    def get_tagged(self, refs=None, repo=None):
        """Get a dict of peeled values of tags to their original tag shas.

        :param refs: dict of refname -> sha of possible tags; defaults to all of
            the backend's refs.
        :param repo: optional Repo instance for getting peeled refs; defaults to
            the backend's repo, if available
        :return: dict of peeled_sha -> tag_sha, where tag_sha is the sha of a
            tag whose peeled value is peeled_sha.
        """
        if not self.has_capability("include-tag"):
            return {}
        if refs is None:
            refs = self.repo.get_refs()
        if repo is None:
            repo = getattr(self.repo, "repo", None)
            if repo is None:
                # Bail if we don't have a Repo available; this is ok since
                # clients must be able to handle if the server doesn't include
                # all relevant tags.
                # TODO: fix behavior when missing
                return {}
        tagged = {}
        for name, sha in refs.iteritems():
            peeled_sha = repo.get_peeled(name)
            if peeled_sha != sha:
                tagged[peeled_sha] = sha
        return tagged

    def handle(self):
        write = lambda x: self.proto.write_sideband(1, x)

        graph_walker = ProtocolGraphWalker(self, self.repo.object_store,
            self.repo.get_peeled)
        objects_iter = self.repo.fetch_objects(
          graph_walker.determine_wants, graph_walker, self.progress,
          get_tagged=self.get_tagged)

        # Do they want any objects?
        if len(objects_iter) == 0:
            return

        self.progress("dul-daemon says what\n")
        self.progress("counting objects: %d, done.\n" % len(objects_iter))
        write_pack_data(ProtocolFile(None, write), objects_iter, 
                        len(objects_iter))
        self.progress("how was that, then?\n")
        # we are done
        self.proto.write("0000")


class ProtocolGraphWalker(object):
    """A graph walker that knows the git protocol.

    As a graph walker, this class implements ack(), next(), and reset(). It
    also contains some base methods for interacting with the wire and walking
    the commit tree.

    The work of determining which acks to send is passed on to the
    implementation instance stored in _impl. The reason for this is that we do
    not know at object creation time what ack level the protocol requires. A
    call to set_ack_level() is required to set up the implementation, before any
    calls to next() or ack() are made.
    """
    def __init__(self, handler, object_store, get_peeled):
        self.handler = handler
        self.store = object_store
        self.get_peeled = get_peeled
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
                peeled_sha = self.get_peeled(ref)
                if peeled_sha != sha:
                    self.proto.write_pkt_line('%s %s^{}\n' %
                                              (peeled_sha, ref))

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
            if commit.type_name != "commit":
                # non-commit wants are assumed to be satisfied
                continue
            for parent in commit.parents:
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

    def __init__(self, backend, args, proto,
                 stateless_rpc=False, advertise_refs=False):
        Handler.__init__(self, backend, proto)
        self.repo = backend.open_repository(args[0])
        self.stateless_rpc = stateless_rpc
        self.advertise_refs = advertise_refs

    def capabilities(self):
        return ("report-status", "delete-refs")

    def _apply_pack(self, refs):
        f, commit = self.repo.object_store.add_thin_pack()
        all_exceptions = (IOError, OSError, ChecksumMismatch, ApplyDeltaError)
        status = []
        unpack_error = None
        # TODO: more informative error messages than just the exception string
        try:
            PackStreamCopier(self.proto.read, self.proto.recv, f).verify()
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
            ref_error = None
            try:
                if sha == ZERO_SHA:
                    if not self.has_capability('delete-refs'):
                        raise GitProtocolError(
                          'Attempted to delete refs without delete-refs '
                          'capability.')
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

        return status

    def handle(self):
        refs = self.repo.get_refs().items()

        if self.advertise_refs or not self.stateless_rpc:
            if refs:
                self.proto.write_pkt_line(
                  "%s %s\x00%s\n" % (refs[0][1], refs[0][0],
                                     self.capability_line()))
                for i in range(1, len(refs)):
                    ref = refs[i]
                    self.proto.write_pkt_line("%s %s\n" % (ref[1], ref[0]))
            else:
                self.proto.write_pkt_line("%s capabilities^{} %s" % (
                  ZERO_SHA, self.capability_line()))

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
        status = self._apply_pack(client_refs)

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
        proto = ReceivableProtocol(self.connection.recv, self.wfile.write)
        command, args = proto.read_cmd()

        # switch case to handle the specific git command
        if command == 'git-upload-pack':
            cls = UploadPackHandler
        elif command == 'git-receive-pack':
            cls = ReceivePackHandler
        else:
            return

        h = cls(self.server.backend, args, proto)
        h.handle()


class TCPGitServer(SocketServer.TCPServer):

    allow_reuse_address = True
    serve = SocketServer.TCPServer.serve_forever

    def __init__(self, backend, listen_addr, port=TCP_GIT_PORT):
        self.backend = backend
        SocketServer.TCPServer.__init__(self, (listen_addr, port), TCPGitRequestHandler)
