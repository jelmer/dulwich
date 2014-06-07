# server.py -- Implementation of the server side git protocols
# Copyright (C) 2008 John Carr <john.carr@unrouted.co.uk>
# Coprygith (C) 2011-2012 Jelmer Vernooij <jelmer@samba.org>
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

* Documentation/technical/protocol-capabilities.txt
* Documentation/technical/pack-protocol.txt

Currently supported capabilities:

 * include-tag
 * thin-pack
 * multi_ack_detailed
 * multi_ack
 * side-band-64k
 * ofs-delta
 * no-progress
 * report-status
 * delete-refs

Known capabilities that are not supported:
 * shallow (http://pad.lv/909524)
"""

import collections
import os
import socket
import SocketServer
import sys
import zlib

from dulwich.errors import (
    ApplyDeltaError,
    ChecksumMismatch,
    GitProtocolError,
    NotGitRepository,
    UnexpectedCommandError,
    ObjectFormatException,
    )
from dulwich import log_utils
from dulwich.objects import (
    hex_to_sha,
    Commit,
    )
from dulwich.pack import (
    write_pack_objects,
    )
from dulwich.protocol import (
    BufferedPktLineWriter,
    MULTI_ACK,
    MULTI_ACK_DETAILED,
    Protocol,
    ProtocolFile,
    ReceivableProtocol,
    SINGLE_ACK,
    TCP_GIT_PORT,
    ZERO_SHA,
    ack_type,
    extract_capabilities,
    extract_want_line_capabilities,
    )
from dulwich.refs import (
    write_info_refs,
    )
from dulwich.repo import (
    Repo,
    )


logger = log_utils.getLogger(__name__)


class Backend(object):
    """A backend for the Git smart server implementation."""

    def open_repository(self, path):
        """Open the repository at a path.

        :param path: Path to the repository
        :raise NotGitRepository: no git repository was found at path
        :return: Instance of BackendRepo
        """
        raise NotImplementedError(self.open_repository)


class BackendRepo(object):
    """Repository abstraction used by the Git server.

    The methods required here are a subset of those provided by
    dulwich.repo.Repo.
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


class DictBackend(Backend):
    """Trivial backend that looks up Git repositories in a dictionary."""

    def __init__(self, repos):
        self.repos = repos

    def open_repository(self, path):
        logger.debug('Opening repository at %s', path)
        try:
            return self.repos[path]
        except KeyError:
            raise NotGitRepository(
                "No git repository was found at %(path)s" % dict(path=path)
            )


class FileSystemBackend(Backend):
    """Simple backend that looks up Git repositories in the local file system."""

    def open_repository(self, path):
        logger.debug('opening repository at %s', path)
        return Repo(path)


class Handler(object):
    """Smart protocol command handler base class."""

    def __init__(self, backend, proto, http_req=None):
        self.backend = backend
        self.proto = proto
        self.http_req = http_req
        self._client_capabilities = None

    @classmethod
    def capability_line(cls):
        return " ".join(cls.capabilities())

    @classmethod
    def capabilities(cls):
        raise NotImplementedError(cls.capabilities)

    @classmethod
    def innocuous_capabilities(cls):
        return ("include-tag", "thin-pack", "no-progress", "ofs-delta")

    @classmethod
    def required_capabilities(cls):
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
        logger.info('Client capabilities: %s', caps)

    def has_capability(self, cap):
        if self._client_capabilities is None:
            raise GitProtocolError('Server attempted to access capability %s '
                                   'before asking client' % cap)
        return cap in self._client_capabilities


class UploadPackHandler(Handler):
    """Protocol handler for uploading a pack to the server."""

    def __init__(self, backend, args, proto, http_req=None,
                 advertise_refs=False):
        Handler.__init__(self, backend, proto, http_req=http_req)
        self.repo = backend.open_repository(args[0])
        self._graph_walker = None
        self.advertise_refs = advertise_refs

    @classmethod
    def capabilities(cls):
        return ("multi_ack_detailed", "multi_ack", "side-band-64k", "thin-pack",
                "ofs-delta", "no-progress", "include-tag", "shallow")

    @classmethod
    def required_capabilities(cls):
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

        # Did the process short-circuit (e.g. in a stateless RPC call)? Note
        # that the client still expects a 0-object pack in most cases.
        if len(objects_iter) == 0:
            return

        self.progress("dul-daemon says what\n")
        self.progress("counting objects: %d, done.\n" % len(objects_iter))
        write_pack_objects(ProtocolFile(None, write), objects_iter)
        self.progress("how was that, then?\n")
        # we are done
        self.proto.write("0000")


def _split_proto_line(line, allowed):
    """Split a line read from the wire.

    :param line: The line read from the wire.
    :param allowed: An iterable of command names that should be allowed.
        Command names not listed below as possible return values will be
        ignored.  If None, any commands from the possible return values are
        allowed.
    :return: a tuple having one of the following forms:
        ('want', obj_id)
        ('have', obj_id)
        ('done', None)
        (None, None)  (for a flush-pkt)

    :raise UnexpectedCommandError: if the line cannot be parsed into one of the
        allowed return values.
    """
    if not line:
        fields = [None]
    else:
        fields = line.rstrip('\n').split(' ', 1)
    command = fields[0]
    if allowed is not None and command not in allowed:
        raise UnexpectedCommandError(command)
    try:
        if len(fields) == 1 and command in ('done', None):
            return (command, None)
        elif len(fields) == 2:
            if command in ('want', 'have', 'shallow', 'unshallow'):
                hex_to_sha(fields[1])
                return tuple(fields)
            elif command == 'deepen':
                return command, int(fields[1])
    except (TypeError, AssertionError) as e:
        raise GitProtocolError(e)
    raise GitProtocolError('Received invalid line from client: %s' % line)


def _find_shallow(store, heads, depth):
    """Find shallow commits according to a given depth.

    :param store: An ObjectStore for looking up objects.
    :param heads: Iterable of head SHAs to start walking from.
    :param depth: The depth of ancestors to include.
    :return: A tuple of (shallow, not_shallow), sets of SHAs that should be
        considered shallow and unshallow according to the arguments. Note that
        these sets may overlap if a commit is reachable along multiple paths.
    """
    parents = {}
    def get_parents(sha):
        result = parents.get(sha, None)
        if not result:
            result = store[sha].parents
            parents[sha] = result
        return result

    todo = []  # stack of (sha, depth)
    for head_sha in heads:
        obj = store.peel_sha(head_sha)
        if isinstance(obj, Commit):
            todo.append((obj.id, 0))

    not_shallow = set()
    shallow = set()
    while todo:
        sha, cur_depth = todo.pop()
        if cur_depth < depth:
            not_shallow.add(sha)
            new_depth = cur_depth + 1
            todo.extend((p, new_depth) for p in get_parents(sha))
        else:
            shallow.add(sha)

    return shallow, not_shallow


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
        self.http_req = handler.http_req
        self.advertise_refs = handler.advertise_refs
        self._wants = []
        self.shallow = set()
        self.client_shallow = set()
        self.unshallow = set()
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

        If the client has the 'shallow' capability, this method also reads and
        responds to the 'shallow' and 'deepen' lines from the client. These are
        not part of the wants per se, but they set up necessary state for
        walking the graph. Additionally, later code depends on this method
        consuming everything up to the first 'have' line.

        :param heads: a dict of refname->SHA1 to advertise
        :return: a list of SHA1s requested by the client
        """
        values = set(heads.itervalues())
        if self.advertise_refs or not self.http_req:
            for i, (ref, sha) in enumerate(sorted(heads.iteritems())):
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
        allowed = ('want', 'shallow', 'deepen', None)
        command, sha = _split_proto_line(line, allowed)

        want_revs = []
        while command == 'want':
            if sha not in values:
                raise GitProtocolError(
                  'Client wants invalid object %s' % sha)
            want_revs.append(sha)
            command, sha = self.read_proto_line(allowed)

        self.set_wants(want_revs)
        if command in ('shallow', 'deepen'):
            self.unread_proto_line(command, sha)
            self._handle_shallow_request(want_revs)

        if self.http_req and self.proto.eof():
            # The client may close the socket at this point, expecting a
            # flush-pkt from the server. We might be ready to send a packfile at
            # this point, so we need to explicitly short-circuit in this case.
            return []

        return want_revs

    def unread_proto_line(self, command, value):
        self.proto.unread_pkt_line('%s %s' % (command, value))

    def ack(self, have_ref):
        return self._impl.ack(have_ref)

    def reset(self):
        self._cached = True
        self._cache_index = 0

    def next(self):
        if not self._cached:
            if not self._impl and self.http_req:
                return None
            return next(self._impl)
        self._cache_index += 1
        if self._cache_index > len(self._cache):
            return None
        return self._cache[self._cache_index]

    __next__ = next

    def read_proto_line(self, allowed):
        """Read a line from the wire.

        :param allowed: An iterable of command names that should be allowed.
        :return: A tuple of (command, value); see _split_proto_line.
        :raise UnexpectedCommandError: If an error occurred reading the line.
        """
        return _split_proto_line(self.proto.read_pkt_line(), allowed)

    def _handle_shallow_request(self, wants):
        while True:
            command, val = self.read_proto_line(('deepen', 'shallow'))
            if command == 'deepen':
                depth = val
                break
            self.client_shallow.add(val)
        self.read_proto_line((None,))  # consume client's flush-pkt

        shallow, not_shallow = _find_shallow(self.store, wants, depth)

        # Update self.shallow instead of reassigning it since we passed a
        # reference to it before this method was called.
        self.shallow.update(shallow - not_shallow)
        new_shallow = self.shallow - self.client_shallow
        unshallow = self.unshallow = not_shallow & self.client_shallow

        for sha in sorted(new_shallow):
            self.proto.write_pkt_line('shallow %s' % sha)
        for sha in sorted(unshallow):
            self.proto.write_pkt_line('unshallow %s' % sha)

        self.proto.write_pkt_line(None)

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


_GRAPH_WALKER_COMMANDS = ('have', 'done', None)


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
        command, sha = self.walker.read_proto_line(_GRAPH_WALKER_COMMANDS)
        if command in (None, 'done'):
            if not self._sent_ack:
                self.walker.send_nak()
            return None
        elif command == 'have':
            return sha

    __next__ = next


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
            command, sha = self.walker.read_proto_line(_GRAPH_WALKER_COMMANDS)
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

    __next__ = next


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
            command, sha = self.walker.read_proto_line(_GRAPH_WALKER_COMMANDS)
            if command is None:
                self.walker.send_nak()
                if self.walker.http_req:
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

    __next__ = next


class ReceivePackHandler(Handler):
    """Protocol handler for downloading a pack from the client."""

    def __init__(self, backend, args, proto, http_req=None,
                 advertise_refs=False):
        Handler.__init__(self, backend, proto, http_req=http_req)
        self.repo = backend.open_repository(args[0])
        self.advertise_refs = advertise_refs

    @classmethod
    def capabilities(cls):
        return ("report-status", "delete-refs", "side-band-64k")

    def _apply_pack(self, refs):
        all_exceptions = (IOError, OSError, ChecksumMismatch, ApplyDeltaError,
                          AssertionError, socket.error, zlib.error,
                          ObjectFormatException)
        status = []
        will_send_pack = False

        for command in refs:
            if command[1] != ZERO_SHA:
                will_send_pack = True

        if will_send_pack:
            # TODO: more informative error messages than just the exception string
            try:
                recv = getattr(self.proto, "recv", None)
                self.repo.object_store.add_thin_pack(self.proto.read, recv)
                status.append(('unpack', 'ok'))
            except all_exceptions as e:
                status.append(('unpack', str(e).replace('\n', '')))
                # The pack may still have been moved in, but it may contain broken
                # objects. We trust a later GC to clean it up.
        else:
            # The git protocol want to find a status entry related to unpack process
            # even if no pack data has been sent.
            status.append(('unpack', 'ok'))

        for oldsha, sha, ref in refs:
            ref_status = 'ok'
            try:
                if sha == ZERO_SHA:
                    if not 'delete-refs' in self.capabilities():
                        raise GitProtocolError(
                          'Attempted to delete refs without delete-refs '
                          'capability.')
                    try:
                        del self.repo.refs[ref]
                    except all_exceptions:
                        ref_status = 'failed to delete'
                else:
                    try:
                        self.repo.refs[ref] = sha
                    except all_exceptions:
                        ref_status = 'failed to write'
            except KeyError as e:
                ref_status = 'bad ref'
            status.append((ref, ref_status))

        return status

    def _report_status(self, status):
        if self.has_capability('side-band-64k'):
            writer = BufferedPktLineWriter(
              lambda d: self.proto.write_sideband(1, d))
            write = writer.write

            def flush():
                writer.flush()
                self.proto.write_pkt_line(None)
        else:
            write = self.proto.write_pkt_line
            flush = lambda: None

        for name, msg in status:
            if name == 'unpack':
                write('unpack %s\n' % msg)
            elif msg == 'ok':
                write('ok %s\n' % name)
            else:
                write('ng %s %s\n' % (name, msg))
        write(None)
        flush()

    def handle(self):
        refs = sorted(self.repo.get_refs().iteritems())

        if self.advertise_refs or not self.http_req:
            if refs:
                self.proto.write_pkt_line(
                  "%s %s\x00%s\n" % (refs[0][1], refs[0][0],
                                     self.capability_line()))
                for i in range(1, len(refs)):
                    ref = refs[i]
                    self.proto.write_pkt_line("%s %s\n" % (ref[1], ref[0]))
            else:
                self.proto.write_pkt_line("%s capabilities^{}\0%s" % (
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
            self._report_status(status)


# Default handler classes for git services.
DEFAULT_HANDLERS = {
  'git-upload-pack': UploadPackHandler,
  'git-receive-pack': ReceivePackHandler,
  }


class TCPGitRequestHandler(SocketServer.StreamRequestHandler):

    def __init__(self, handlers, *args, **kwargs):
        self.handlers = handlers
        SocketServer.StreamRequestHandler.__init__(self, *args, **kwargs)

    def handle(self):
        proto = ReceivableProtocol(self.connection.recv, self.wfile.write)
        command, args = proto.read_cmd()
        logger.info('Handling %s request, args=%s', command, args)

        cls = self.handlers.get(command, None)
        if not callable(cls):
            raise GitProtocolError('Invalid service %s' % command)
        h = cls(self.server.backend, args, proto)
        h.handle()


class TCPGitServer(SocketServer.TCPServer):

    allow_reuse_address = True
    serve = SocketServer.TCPServer.serve_forever

    def _make_handler(self, *args, **kwargs):
        return TCPGitRequestHandler(self.handlers, *args, **kwargs)

    def __init__(self, backend, listen_addr, port=TCP_GIT_PORT, handlers=None):
        self.handlers = dict(DEFAULT_HANDLERS)
        if handlers is not None:
            self.handlers.update(handlers)
        self.backend = backend
        logger.info('Listening for TCP connections on %s:%d', listen_addr, port)
        SocketServer.TCPServer.__init__(self, (listen_addr, port),
                                        self._make_handler)

    def verify_request(self, request, client_address):
        logger.info('Handling request from %s', client_address)
        return True

    def handle_error(self, request, client_address):
        logger.exception('Exception happened during processing of request '
                         'from %s', client_address)


def main(argv=sys.argv):
    """Entry point for starting a TCP git server."""
    import optparse
    parser = optparse.OptionParser()
    parser.add_option("-l", "--listen_address", dest="listen_address",
                      default="localhost",
                      help="Binding IP address.")
    parser.add_option("-p", "--port", dest="port", type=int,
                      default=TCP_GIT_PORT,
                      help="Binding TCP port.")
    options, args = parser.parse_args(argv)

    log_utils.default_logging_config()
    if len(argv) > 1:
        gitdir = args[1]
    else:
        gitdir = '.'
    from dulwich import porcelain
    porcelain.daemon(gitdir, address=options.listen_address,
                     port=options.port)


def serve_command(handler_cls, argv=sys.argv, backend=None, inf=sys.stdin,
                  outf=sys.stdout):
    """Serve a single command.

    This is mostly useful for the implementation of commands used by e.g. git+ssh.

    :param handler_cls: `Handler` class to use for the request
    :param argv: execv-style command-line arguments. Defaults to sys.argv.
    :param backend: `Backend` to use
    :param inf: File-like object to read from, defaults to standard input.
    :param outf: File-like object to write to, defaults to standard output.
    :return: Exit code for use with sys.exit. 0 on success, 1 on failure.
    """
    if backend is None:
        backend = FileSystemBackend()
    def send_fn(data):
        outf.write(data)
        outf.flush()
    proto = Protocol(inf.read, send_fn)
    handler = handler_cls(backend, argv[1:], proto)
    # FIXME: Catch exceptions and write a single-line summary to outf.
    handler.handle()
    return 0


def generate_info_refs(repo):
    """Generate an info refs file."""
    refs = repo.get_refs()
    return write_info_refs(refs, repo.object_store)


def generate_objects_info_packs(repo):
    """Generate an index for for packs."""
    for pack in repo.object_store.packs:
        yield 'P %s\n' % pack.data.filename


def update_server_info(repo):
    """Generate server info for dumb file access.

    This generates info/refs and objects/info/packs,
    similar to "git update-server-info".
    """
    repo._put_named_file(os.path.join('info', 'refs'),
        "".join(generate_info_refs(repo)))

    repo._put_named_file(os.path.join('objects', 'info', 'packs'),
        "".join(generate_objects_info_packs(repo)))


if __name__ == '__main__':
    main()
