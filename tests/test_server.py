# test_server.py -- Tests for the git server
# Copyright (C) 2010 Google, Inc.
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

"""Tests for the smart protocol server."""

import os
import shutil
import sys
import tempfile
from io import BytesIO

from dulwich.errors import (
    GitProtocolError,
    HangupException,
    NotGitRepository,
    UnexpectedCommandError,
)
from dulwich.object_store import MemoryObjectStore, find_shallow
from dulwich.objects import Tree
from dulwich.protocol import ZERO_SHA, format_capability_line
from dulwich.repo import MemoryRepo, Repo
from dulwich.server import (
    Backend,
    DictBackend,
    FileSystemBackend,
    MultiAckDetailedGraphWalkerImpl,
    MultiAckGraphWalkerImpl,
    PackHandler,
    ReceivePackHandler,
    SingleAckGraphWalkerImpl,
    UploadPackHandler,
    _ProtocolGraphWalker,
    _split_proto_line,
    serve_command,
    update_server_info,
)
from dulwich.tests.utils import make_commit, make_tag

from . import TestCase

ONE = b"1" * 40
TWO = b"2" * 40
THREE = b"3" * 40
FOUR = b"4" * 40
FIVE = b"5" * 40
SIX = b"6" * 40


class TestProto:
    def __init__(self) -> None:
        self._output: list[bytes] = []
        self._received: dict[int, list[bytes]] = {0: [], 1: [], 2: [], 3: []}

    def set_output(self, output_lines) -> None:
        self._output = output_lines

    def read_pkt_line(self):
        if self._output:
            data = self._output.pop(0)
            if data is not None:
                return data.rstrip() + b"\n"
            else:
                # flush-pkt ('0000').
                return None
        else:
            raise HangupException

    def write_sideband(self, band, data) -> None:
        self._received[band].append(data)

    def write_pkt_line(self, data) -> None:
        self._received[0].append(data)

    def get_received_line(self, band=0):
        lines = self._received[band]
        return lines.pop(0)


class TestGenericPackHandler(PackHandler):
    def __init__(self) -> None:
        PackHandler.__init__(self, Backend(), None)

    @classmethod
    def capabilities(cls):
        return [b"cap1", b"cap2", b"cap3"]

    @classmethod
    def required_capabilities(cls):
        return [b"cap2"]


class HandlerTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._handler = TestGenericPackHandler()

    def assertSucceeds(self, func, *args, **kwargs) -> None:
        try:
            func(*args, **kwargs)
        except GitProtocolError as e:
            self.fail(e)

    def test_capability_line(self) -> None:
        self.assertEqual(
            b" cap1 cap2 cap3",
            format_capability_line([b"cap1", b"cap2", b"cap3"]),
        )

    def test_set_client_capabilities(self) -> None:
        set_caps = self._handler.set_client_capabilities
        self.assertSucceeds(set_caps, [b"cap2"])
        self.assertSucceeds(set_caps, [b"cap1", b"cap2"])

        # different order
        self.assertSucceeds(set_caps, [b"cap3", b"cap1", b"cap2"])

        # error cases
        self.assertRaises(GitProtocolError, set_caps, [b"capxxx", b"cap2"])
        self.assertRaises(GitProtocolError, set_caps, [b"cap1", b"cap3"])

        # ignore innocuous but unknown capabilities
        self.assertRaises(GitProtocolError, set_caps, [b"cap2", b"ignoreme"])
        self.assertNotIn(b"ignoreme", self._handler.capabilities())
        self._handler.innocuous_capabilities = lambda: (b"ignoreme",)
        self.assertSucceeds(set_caps, [b"cap2", b"ignoreme"])

    def test_has_capability(self) -> None:
        self.assertRaises(GitProtocolError, self._handler.has_capability, b"cap")
        caps = self._handler.capabilities()
        self._handler.set_client_capabilities(caps)
        for cap in caps:
            self.assertTrue(self._handler.has_capability(cap))
        self.assertFalse(self._handler.has_capability(b"capxxx"))


class UploadPackHandlerTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.path)
        self.repo = Repo.init(self.path)
        self._repo = Repo.init_bare(self.path)
        backend = DictBackend({b"/": self._repo})
        self._handler = UploadPackHandler(
            backend, [b"/", b"host=lolcathost"], TestProto()
        )

    def test_progress(self) -> None:
        caps = self._handler.required_capabilities()
        self._handler.set_client_capabilities(caps)
        self._handler._start_pack_send_phase()
        self._handler.progress(b"first message")
        self._handler.progress(b"second message")
        self.assertEqual(b"first message", self._handler.proto.get_received_line(2))
        self.assertEqual(b"second message", self._handler.proto.get_received_line(2))
        self.assertRaises(IndexError, self._handler.proto.get_received_line, 2)

    def test_no_progress(self) -> None:
        caps = [*list(self._handler.required_capabilities()), b"no-progress"]
        self._handler.set_client_capabilities(caps)
        self._handler.progress(b"first message")
        self._handler.progress(b"second message")
        self.assertRaises(IndexError, self._handler.proto.get_received_line, 2)

    def test_get_tagged(self) -> None:
        refs = {
            b"refs/tags/tag1": ONE,
            b"refs/tags/tag2": TWO,
            b"refs/heads/master": FOUR,  # not a tag, no peeled value
        }
        # repo needs to peel this object
        self._repo.object_store.add_object(make_commit(id=FOUR))
        for name, sha in refs.items():
            self._repo.refs[name] = sha
        peeled = {
            b"refs/tags/tag1": b"1234" * 10,
            b"refs/tags/tag2": b"5678" * 10,
        }
        self._repo.refs._peeled_refs = peeled
        self._repo.refs.add_packed_refs(refs)

        caps = [*list(self._handler.required_capabilities()), b"include-tag"]
        self._handler.set_client_capabilities(caps)
        self.assertEqual(
            {b"1234" * 10: ONE, b"5678" * 10: TWO},
            self._handler.get_tagged(refs, repo=self._repo),
        )

        # non-include-tag case
        caps = self._handler.required_capabilities()
        self._handler.set_client_capabilities(caps)
        self.assertEqual({}, self._handler.get_tagged(refs, repo=self._repo))

    def test_nothing_to_do_but_wants(self) -> None:
        # Just the fact that the client claims to want an object is enough
        # for sending a pack. Even if there turns out to be nothing.
        refs = {b"refs/tags/tag1": ONE}
        tree = Tree()
        self._repo.object_store.add_object(tree)
        self._repo.object_store.add_object(make_commit(id=ONE, tree=tree))
        for name, sha in refs.items():
            self._repo.refs[name] = sha
        self._handler.proto.set_output(
            [
                b"want " + ONE + b" side-band-64k thin-pack ofs-delta",
                None,
                b"have " + ONE,
                b"done",
                None,
            ]
        )
        self._handler.handle()
        # The server should always send a pack, even if it's empty.
        self.assertTrue(self._handler.proto.get_received_line(1).startswith(b"PACK"))

    def test_nothing_to_do_no_wants(self) -> None:
        # Don't send a pack if the client didn't ask for anything.
        refs = {b"refs/tags/tag1": ONE}
        tree = Tree()
        self._repo.object_store.add_object(tree)
        self._repo.object_store.add_object(make_commit(id=ONE, tree=tree))
        for ref, sha in refs.items():
            self._repo.refs[ref] = sha
        self._handler.proto.set_output([None])
        self._handler.handle()
        # The server should not send a pack, since the client didn't ask for
        # anything.
        self.assertEqual([], self._handler.proto._received[1])


class FindShallowTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._store = MemoryObjectStore()

    def make_commit(self, **attrs):
        commit = make_commit(**attrs)
        self._store.add_object(commit)
        return commit

    def make_linear_commits(self, n, message=b""):
        commits = []
        parents = []
        for _ in range(n):
            commits.append(self.make_commit(parents=parents, message=message))
            parents = [commits[-1].id]
        return commits

    def assertSameElements(self, expected, actual) -> None:
        self.assertEqual(set(expected), set(actual))

    def test_linear(self) -> None:
        c1, c2, c3 = self.make_linear_commits(3)

        self.assertEqual(({c3.id}, set()), find_shallow(self._store, [c3.id], 1))
        self.assertEqual(
            ({c2.id}, {c3.id}),
            find_shallow(self._store, [c3.id], 2),
        )
        self.assertEqual(
            ({c1.id}, {c2.id, c3.id}),
            find_shallow(self._store, [c3.id], 3),
        )
        self.assertEqual(
            (set(), {c1.id, c2.id, c3.id}),
            find_shallow(self._store, [c3.id], 4),
        )

    def test_multiple_independent(self) -> None:
        a = self.make_linear_commits(2, message=b"a")
        b = self.make_linear_commits(2, message=b"b")
        c = self.make_linear_commits(2, message=b"c")
        heads = [a[1].id, b[1].id, c[1].id]

        self.assertEqual(
            ({a[0].id, b[0].id, c[0].id}, set(heads)),
            find_shallow(self._store, heads, 2),
        )

    def test_multiple_overlapping(self) -> None:
        # Create the following commit tree:
        # 1--2
        #  \
        #   3--4
        c1, c2 = self.make_linear_commits(2)
        c3 = self.make_commit(parents=[c1.id])
        c4 = self.make_commit(parents=[c3.id])

        # 1 is shallow along the path from 4, but not along the path from 2.
        self.assertEqual(
            ({c1.id}, {c1.id, c2.id, c3.id, c4.id}),
            find_shallow(self._store, [c2.id, c4.id], 3),
        )

    def test_merge(self) -> None:
        c1 = self.make_commit()
        c2 = self.make_commit()
        c3 = self.make_commit(parents=[c1.id, c2.id])

        self.assertEqual(
            ({c1.id, c2.id}, {c3.id}),
            find_shallow(self._store, [c3.id], 2),
        )

    def test_tag(self) -> None:
        c1, c2 = self.make_linear_commits(2)
        tag = make_tag(c2, name=b"tag")
        self._store.add_object(tag)

        self.assertEqual(
            ({c1.id}, {c2.id}),
            find_shallow(self._store, [tag.id], 2),
        )


class TestUploadPackHandler(UploadPackHandler):
    @classmethod
    def required_capabilities(self):
        return []


class ReceivePackHandlerTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._repo = MemoryRepo.init_bare([], {})
        backend = DictBackend({b"/": self._repo})
        self._handler = ReceivePackHandler(
            backend, [b"/", b"host=lolcathost"], TestProto()
        )

    def test_apply_pack_del_ref(self) -> None:
        refs = {b"refs/heads/master": TWO, b"refs/heads/fake-branch": ONE}
        self._repo.refs._update(refs)
        update_refs = [
            [ONE, ZERO_SHA, b"refs/heads/fake-branch"],
        ]
        self._handler.set_client_capabilities([b"delete-refs"])
        status = list(self._handler._apply_pack(update_refs))
        self.assertEqual(status[0][0], b"unpack")
        self.assertEqual(status[0][1], b"ok")
        self.assertEqual(status[1][0], b"refs/heads/fake-branch")
        self.assertEqual(status[1][1], b"ok")


class ProtocolGraphWalkerEmptyTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._repo = MemoryRepo.init_bare([], {})
        backend = DictBackend({b"/": self._repo})
        self._walker = _ProtocolGraphWalker(
            TestUploadPackHandler(backend, [b"/", b"host=lolcats"], TestProto()),
            self._repo.object_store,
            self._repo.get_peeled,
            self._repo.refs.get_symrefs,
        )

    def test_empty_repository(self) -> None:
        # The server should wait for a flush packet.
        self._walker.proto.set_output([])
        self.assertRaises(HangupException, self._walker.determine_wants, {})
        self.assertEqual(None, self._walker.proto.get_received_line())

        self._walker.proto.set_output([None])
        self.assertEqual([], self._walker.determine_wants({}))
        self.assertEqual(None, self._walker.proto.get_received_line())


class ProtocolGraphWalkerTestCase(TestCase):
    def setUp(self) -> None:
        super().setUp()
        # Create the following commit tree:
        #   3---5
        #  /
        # 1---2---4
        commits = [
            make_commit(id=ONE, parents=[], commit_time=111),
            make_commit(id=TWO, parents=[ONE], commit_time=222),
            make_commit(id=THREE, parents=[ONE], commit_time=333),
            make_commit(id=FOUR, parents=[TWO], commit_time=444),
            make_commit(id=FIVE, parents=[THREE], commit_time=555),
        ]
        self._repo = MemoryRepo.init_bare(commits, {})
        backend = DictBackend({b"/": self._repo})
        self._walker = _ProtocolGraphWalker(
            TestUploadPackHandler(backend, [b"/", b"host=lolcats"], TestProto()),
            self._repo.object_store,
            self._repo.get_peeled,
            self._repo.refs.get_symrefs,
        )

    def test_all_wants_satisfied_no_haves(self) -> None:
        self._walker.set_wants([ONE])
        self.assertFalse(self._walker.all_wants_satisfied([]))
        self._walker.set_wants([TWO])
        self.assertFalse(self._walker.all_wants_satisfied([]))
        self._walker.set_wants([THREE])
        self.assertFalse(self._walker.all_wants_satisfied([]))

    def test_all_wants_satisfied_have_root(self) -> None:
        self._walker.set_wants([ONE])
        self.assertTrue(self._walker.all_wants_satisfied([ONE]))
        self._walker.set_wants([TWO])
        self.assertTrue(self._walker.all_wants_satisfied([ONE]))
        self._walker.set_wants([THREE])
        self.assertTrue(self._walker.all_wants_satisfied([ONE]))

    def test_all_wants_satisfied_have_branch(self) -> None:
        self._walker.set_wants([TWO])
        self.assertTrue(self._walker.all_wants_satisfied([TWO]))
        # wrong branch
        self._walker.set_wants([THREE])
        self.assertFalse(self._walker.all_wants_satisfied([TWO]))

    def test_all_wants_satisfied(self) -> None:
        self._walker.set_wants([FOUR, FIVE])
        # trivial case: wants == haves
        self.assertTrue(self._walker.all_wants_satisfied([FOUR, FIVE]))
        # cases that require walking the commit tree
        self.assertTrue(self._walker.all_wants_satisfied([ONE]))
        self.assertFalse(self._walker.all_wants_satisfied([TWO]))
        self.assertFalse(self._walker.all_wants_satisfied([THREE]))
        self.assertTrue(self._walker.all_wants_satisfied([TWO, THREE]))

    def test_split_proto_line(self) -> None:
        allowed = (b"want", b"done", None)
        self.assertEqual(
            (b"want", ONE), _split_proto_line(b"want " + ONE + b"\n", allowed)
        )
        self.assertEqual(
            (b"want", TWO), _split_proto_line(b"want " + TWO + b"\n", allowed)
        )
        self.assertRaises(GitProtocolError, _split_proto_line, b"want xxxx\n", allowed)
        self.assertRaises(
            UnexpectedCommandError,
            _split_proto_line,
            b"have " + THREE + b"\n",
            allowed,
        )
        self.assertRaises(
            GitProtocolError,
            _split_proto_line,
            b"foo " + FOUR + b"\n",
            allowed,
        )
        self.assertRaises(GitProtocolError, _split_proto_line, b"bar", allowed)
        self.assertEqual((b"done", None), _split_proto_line(b"done\n", allowed))
        self.assertEqual((None, None), _split_proto_line(b"", allowed))

    def test_determine_wants(self) -> None:
        self._walker.proto.set_output([None])
        self.assertEqual([], self._walker.determine_wants({}))
        self.assertEqual(None, self._walker.proto.get_received_line())

        self._walker.proto.set_output(
            [
                b"want " + ONE + b" multi_ack",
                b"want " + TWO,
                None,
            ]
        )
        heads = {
            b"refs/heads/ref1": ONE,
            b"refs/heads/ref2": TWO,
            b"refs/heads/ref3": THREE,
        }
        self._repo.refs._update(heads)
        self.assertEqual([ONE, TWO], self._walker.determine_wants(heads))

        self._walker.advertise_refs = True
        self.assertEqual([], self._walker.determine_wants(heads))
        self._walker.advertise_refs = False

        self._walker.proto.set_output([b"want " + FOUR + b" multi_ack", None])
        self.assertRaises(GitProtocolError, self._walker.determine_wants, heads)

        self._walker.proto.set_output([None])
        self.assertEqual([], self._walker.determine_wants(heads))

        self._walker.proto.set_output([b"want " + ONE + b" multi_ack", b"foo", None])
        self.assertRaises(GitProtocolError, self._walker.determine_wants, heads)

        self._walker.proto.set_output([b"want " + FOUR + b" multi_ack", None])
        self.assertRaises(GitProtocolError, self._walker.determine_wants, heads)

    def test_determine_wants_advertisement(self) -> None:
        self._walker.proto.set_output([None])
        # advertise branch tips plus tag
        heads = {
            b"refs/heads/ref4": FOUR,
            b"refs/heads/ref5": FIVE,
            b"refs/heads/tag6": SIX,
        }
        self._repo.refs._update(heads)
        self._repo.refs._update_peeled(heads)
        self._repo.refs._update_peeled({b"refs/heads/tag6": FIVE})
        self._walker.determine_wants(heads)
        lines = []
        while True:
            line = self._walker.proto.get_received_line()
            if line is None:
                break
            # strip capabilities list if present
            if b"\x00" in line:
                line = line[: line.index(b"\x00")]
            lines.append(line.rstrip())

        self.assertEqual(
            [
                FOUR + b" refs/heads/ref4",
                FIVE + b" refs/heads/ref5",
                FIVE + b" refs/heads/tag6^{}",
                SIX + b" refs/heads/tag6",
            ],
            sorted(lines),
        )

        # ensure peeled tag was advertised immediately following tag
        for i, line in enumerate(lines):
            if line.endswith(b" refs/heads/tag6"):
                self.assertEqual(FIVE + b" refs/heads/tag6^{}", lines[i + 1])

    # TODO: test commit time cutoff

    def _handle_shallow_request(self, lines, heads) -> None:
        self._walker.proto.set_output([*lines, None])
        self._walker._handle_shallow_request(heads)

    def assertReceived(self, expected) -> None:
        self.assertEqual(
            expected, list(iter(self._walker.proto.get_received_line, None))
        )

    def test_handle_shallow_request_no_client_shallows(self) -> None:
        self._handle_shallow_request([b"deepen 2\n"], [FOUR, FIVE])
        self.assertEqual({TWO, THREE}, self._walker.shallow)
        self.assertReceived(
            [
                b"shallow " + TWO,
                b"shallow " + THREE,
            ]
        )

    def test_handle_shallow_request_no_new_shallows(self) -> None:
        lines = [
            b"shallow " + TWO + b"\n",
            b"shallow " + THREE + b"\n",
            b"deepen 2\n",
        ]
        self._handle_shallow_request(lines, [FOUR, FIVE])
        self.assertEqual({TWO, THREE}, self._walker.shallow)
        self.assertReceived([])

    def test_handle_shallow_request_unshallows(self) -> None:
        lines = [
            b"shallow " + TWO + b"\n",
            b"deepen 3\n",
        ]
        self._handle_shallow_request(lines, [FOUR, FIVE])
        self.assertEqual({ONE}, self._walker.shallow)
        self.assertReceived(
            [
                b"shallow " + ONE,
                b"unshallow " + TWO,
                # THREE is unshallow but was is not shallow in the client
            ]
        )


class TestProtocolGraphWalker:
    def __init__(self) -> None:
        self.acks: list[bytes] = []
        self.lines: list[bytes] = []
        self.wants_satisified = False
        self.stateless_rpc = None
        self.advertise_refs = False
        self._impl = None
        self.done_required = True
        self.done_received = False
        self._empty = False
        self.pack_sent = False

    def read_proto_line(self, allowed):
        command, sha = self.lines.pop(0)
        if allowed is not None:
            assert command in allowed
        return command, sha

    def send_ack(self, sha, ack_type=b"") -> None:
        self.acks.append((sha, ack_type))

    def send_nak(self) -> None:
        self.acks.append((None, b"nak"))

    def all_wants_satisfied(self, haves):
        if haves:
            return self.wants_satisified

    def pop_ack(self):
        if not self.acks:
            return None
        return self.acks.pop(0)

    def handle_done(self):
        if not self._impl:
            return
        # Whether or not PACK is sent after is determined by this, so
        # record this value.
        self.pack_sent = self._impl.handle_done(self.done_required, self.done_received)
        return self.pack_sent

    def notify_done(self) -> None:
        self.done_received = True


class AckGraphWalkerImplTestCase(TestCase):
    """Base setup and asserts for AckGraphWalker tests."""

    def setUp(self) -> None:
        super().setUp()
        self._walker = TestProtocolGraphWalker()
        self._walker.lines = [
            (b"have", TWO),
            (b"have", ONE),
            (b"have", THREE),
            (b"done", None),
        ]
        self._impl = self.impl_cls(self._walker)
        self._walker._impl = self._impl

    def assertNoAck(self) -> None:
        self.assertEqual(None, self._walker.pop_ack())

    def assertAcks(self, acks) -> None:
        for sha, ack_type in acks:
            self.assertEqual((sha, ack_type), self._walker.pop_ack())
        self.assertNoAck()

    def assertAck(self, sha, ack_type=b"") -> None:
        self.assertAcks([(sha, ack_type)])

    def assertNak(self) -> None:
        self.assertAck(None, b"nak")

    def assertNextEquals(self, sha) -> None:
        self.assertEqual(sha, next(self._impl))

    def assertNextEmpty(self) -> None:
        # This is necessary because of no-done - the assumption that it
        # it safe to immediately send out the final ACK is no longer
        # true but the test is still needed for it.  TestProtocolWalker
        # does implement the handle_done which will determine whether
        # the final confirmation can be sent.
        self.assertRaises(IndexError, next, self._impl)
        self._walker.handle_done()


class SingleAckGraphWalkerImplTestCase(AckGraphWalkerImplTestCase):
    impl_cls = SingleAckGraphWalkerImpl

    def test_single_ack(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE)

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNoAck()

    def test_single_ack_flush(self) -> None:
        # same as ack test but ends with a flush-pkt instead of done
        self._walker.lines[-1] = (None, None)

        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE)

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNoAck()

    def test_single_ack_nak(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertNak()

    def test_single_ack_nak_flush(self) -> None:
        # same as nak test but ends with a flush-pkt instead of done
        self._walker.lines[-1] = (None, None)

        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertNak()


class MultiAckGraphWalkerImplTestCase(AckGraphWalkerImplTestCase):
    impl_cls = MultiAckGraphWalkerImpl

    def test_multi_ack(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, b"continue")

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, b"continue")

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertAck(THREE)

    def test_multi_ack_partial(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, b"continue")

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertAck(ONE)

    def test_multi_ack_flush(self) -> None:
        self._walker.lines = [
            (b"have", TWO),
            (None, None),
            (b"have", ONE),
            (b"have", THREE),
            (b"done", None),
        ]
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNak()  # nak the flush-pkt

        self._impl.ack(ONE)
        self.assertAck(ONE, b"continue")

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, b"continue")

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertAck(THREE)

    def test_multi_ack_nak(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertNak()


class MultiAckDetailedGraphWalkerImplTestCase(AckGraphWalkerImplTestCase):
    impl_cls = MultiAckDetailedGraphWalkerImpl

    def test_multi_ack(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, b"common")

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, b"common")

        # done is read.
        self._walker.wants_satisified = True
        self.assertNextEquals(None)
        self._walker.lines.append((None, None))
        self.assertNextEmpty()
        self.assertAcks([(THREE, b"ready"), (None, b"nak"), (THREE, b"")])
        # PACK is sent
        self.assertTrue(self._walker.pack_sent)

    def test_multi_ack_nodone(self) -> None:
        self._walker.done_required = False
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, b"common")

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, b"common")

        # done is read.
        self._walker.wants_satisified = True
        self.assertNextEquals(None)
        self._walker.lines.append((None, None))
        self.assertNextEmpty()
        self.assertAcks([(THREE, b"ready"), (None, b"nak"), (THREE, b"")])
        # PACK is sent
        self.assertTrue(self._walker.pack_sent)

    def test_multi_ack_flush_end(self) -> None:
        # transmission ends with a flush-pkt without a done but no-done is
        # assumed.
        self._walker.lines[-1] = (None, None)
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, b"common")

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, b"common")

        # no done is read
        self._walker.wants_satisified = True
        self.assertNextEmpty()
        self.assertAcks([(THREE, b"ready"), (None, b"nak")])
        # PACK is NOT sent
        self.assertFalse(self._walker.pack_sent)

    def test_multi_ack_flush_end_nodone(self) -> None:
        # transmission ends with a flush-pkt without a done but no-done is
        # assumed.
        self._walker.lines[-1] = (None, None)
        self._walker.done_required = False
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, b"common")

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, b"common")

        # no done is read, but pretend it is (last 'ACK 'commit_id' '')
        self._walker.wants_satisified = True
        self.assertNextEmpty()
        self.assertAcks([(THREE, b"ready"), (None, b"nak"), (THREE, b"")])
        # PACK is sent
        self.assertTrue(self._walker.pack_sent)

    def test_multi_ack_partial(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, b"common")

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertAck(ONE)

    def test_multi_ack_flush(self) -> None:
        # same as ack test but contains a flush-pkt in the middle
        self._walker.lines = [
            (b"have", TWO),
            (None, None),
            (b"have", ONE),
            (b"have", THREE),
            (b"done", None),
            (None, None),
        ]
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNak()  # nak the flush-pkt

        self._impl.ack(ONE)
        self.assertAck(ONE, b"common")

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, b"common")

        self._walker.wants_satisified = True
        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertAcks([(THREE, b"ready"), (None, b"nak"), (THREE, b"")])

    def test_multi_ack_nak(self) -> None:
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        # Done is sent here.
        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertNak()
        self.assertNextEmpty()
        self.assertTrue(self._walker.pack_sent)

    def test_multi_ack_nak_nodone(self) -> None:
        self._walker.done_required = False
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        # Done is sent here.
        self.assertFalse(self._walker.pack_sent)
        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertTrue(self._walker.pack_sent)
        self.assertNak()
        self.assertNextEmpty()

    def test_multi_ack_nak_flush(self) -> None:
        # same as nak test but contains a flush-pkt in the middle
        self._walker.lines = [
            (b"have", TWO),
            (None, None),
            (b"have", ONE),
            (b"have", THREE),
            (b"done", None),
        ]
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNak()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNextEmpty()
        self.assertNak()

    def test_multi_ack_stateless(self) -> None:
        # transmission ends with a flush-pkt
        self._walker.lines[-1] = (None, None)
        self._walker.stateless_rpc = True

        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertFalse(self._walker.pack_sent)
        self.assertNextEquals(None)
        self.assertNak()

        self.assertNextEmpty()
        self.assertNoAck()
        self.assertFalse(self._walker.pack_sent)

    def test_multi_ack_stateless_nodone(self) -> None:
        self._walker.done_required = False
        # transmission ends with a flush-pkt
        self._walker.lines[-1] = (None, None)
        self._walker.stateless_rpc = True

        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertFalse(self._walker.pack_sent)
        self.assertNextEquals(None)
        self.assertNak()

        self.assertNextEmpty()
        self.assertNoAck()
        # PACK will still not be sent.
        self.assertFalse(self._walker.pack_sent)


class FileSystemBackendTests(TestCase):
    """Tests for FileSystemBackend."""

    def setUp(self) -> None:
        super().setUp()
        self.path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.path)
        self.repo = Repo.init(self.path)
        if sys.platform == "win32":
            self.backend = FileSystemBackend(self.path[0] + ":" + os.sep)
        else:
            self.backend = FileSystemBackend()

    def test_nonexistant(self) -> None:
        self.assertRaises(
            NotGitRepository,
            self.backend.open_repository,
            "/does/not/exist/unless/foo",
        )

    def test_absolute(self) -> None:
        repo = self.backend.open_repository(self.path)
        self.assertTrue(
            os.path.samefile(
                os.path.abspath(repo.path), os.path.abspath(self.repo.path)
            )
        )

    def test_child(self) -> None:
        self.assertRaises(
            NotGitRepository,
            self.backend.open_repository,
            os.path.join(self.path, "foo"),
        )

    def test_bad_repo_path(self) -> None:
        backend = FileSystemBackend()

        self.assertRaises(NotGitRepository, lambda: backend.open_repository("/ups"))

    def test_bytes_path(self) -> None:
        # Test that FileSystemBackend can handle bytes paths (issue #973)
        repo = self.backend.open_repository(self.path.encode("utf-8"))
        self.assertTrue(
            os.path.samefile(
                os.path.abspath(repo.path), os.path.abspath(self.repo.path)
            )
        )


class DictBackendTests(TestCase):
    """Tests for DictBackend."""

    def test_nonexistant(self) -> None:
        repo = MemoryRepo.init_bare([], {})
        backend = DictBackend({b"/": repo})
        self.assertRaises(
            NotGitRepository,
            backend.open_repository,
            "/does/not/exist/unless/foo",
        )

    def test_bad_repo_path(self) -> None:
        repo = MemoryRepo.init_bare([], {})
        backend = DictBackend({b"/": repo})

        self.assertRaises(NotGitRepository, lambda: backend.open_repository("/ups"))


class ServeCommandTests(TestCase):
    """Tests for serve_command."""

    def setUp(self) -> None:
        super().setUp()
        self.backend = DictBackend({})

    def serve_command(self, handler_cls, args, inf, outf):
        return serve_command(
            handler_cls,
            [b"test", *args],
            backend=self.backend,
            inf=inf,
            outf=outf,
        )

    def test_receive_pack(self) -> None:
        commit = make_commit(id=ONE, parents=[], commit_time=111)
        self.backend.repos[b"/"] = MemoryRepo.init_bare(
            [commit], {b"refs/heads/master": commit.id}
        )
        outf = BytesIO()
        exitcode = self.serve_command(
            ReceivePackHandler, [b"/"], BytesIO(b"0000"), outf
        )
        outlines = outf.getvalue().splitlines()
        self.assertEqual(2, len(outlines))
        self.assertEqual(
            b"1111111111111111111111111111111111111111 refs/heads/master",
            outlines[0][4:].split(b"\x00")[0],
        )
        self.assertEqual(b"0000", outlines[-1])
        self.assertEqual(0, exitcode)


class UpdateServerInfoTests(TestCase):
    """Tests for update_server_info."""

    def setUp(self) -> None:
        super().setUp()
        self.path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.path)
        self.repo = Repo.init(self.path)

    def test_empty(self) -> None:
        update_server_info(self.repo)
        with open(os.path.join(self.path, ".git", "info", "refs"), "rb") as f:
            self.assertEqual(b"", f.read())
        p = os.path.join(self.path, ".git", "objects", "info", "packs")
        with open(p, "rb") as f:
            self.assertEqual(b"", f.read())

    def test_simple(self) -> None:
        commit_id = self.repo.do_commit(
            message=b"foo",
            committer=b"Joe Example <joe@example.com>",
            ref=b"refs/heads/foo",
        )
        update_server_info(self.repo)
        with open(os.path.join(self.path, ".git", "info", "refs"), "rb") as f:
            self.assertEqual(f.read(), commit_id + b"\trefs/heads/foo\n")
        p = os.path.join(self.path, ".git", "objects", "info", "packs")
        with open(p, "rb") as f:
            self.assertEqual(f.read(), b"")
