# test_server.py -- Tests for the git server
# Copyright (C) 2010 Google, Inc.
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

"""Tests for the smart protocol server."""


from dulwich.errors import (
    GitProtocolError,
    )
from dulwich.server import (
    Backend,
    DictBackend,
    BackendRepo,
    Handler,
    MultiAckGraphWalkerImpl,
    MultiAckDetailedGraphWalkerImpl,
    ProtocolGraphWalker,
    SingleAckGraphWalkerImpl,
    UploadPackHandler,
    )
from dulwich.tests import TestCase



ONE = '1' * 40
TWO = '2' * 40
THREE = '3' * 40
FOUR = '4' * 40
FIVE = '5' * 40
SIX = '6' * 40


class TestProto(object):

    def __init__(self):
        self._output = []
        self._received = {0: [], 1: [], 2: [], 3: []}

    def set_output(self, output_lines):
        self._output = ['%s\n' % line.rstrip() for line in output_lines]

    def read_pkt_line(self):
        if self._output:
            return self._output.pop(0)
        else:
            return None

    def write_sideband(self, band, data):
        self._received[band].append(data)

    def write_pkt_line(self, data):
        if data is None:
            data = 'None'
        self._received[0].append(data)

    def get_received_line(self, band=0):
        lines = self._received[band]
        if lines:
            return lines.pop(0)
        else:
            return None


class HandlerTestCase(TestCase):

    def setUp(self):
        self._handler = Handler(Backend(), None)
        self._handler.capabilities = lambda: ('cap1', 'cap2', 'cap3')
        self._handler.required_capabilities = lambda: ('cap2',)

    def assertSucceeds(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except GitProtocolError, e:
            self.fail(e)

    def test_capability_line(self):
        self.assertEquals('cap1 cap2 cap3', self._handler.capability_line())

    def test_set_client_capabilities(self):
        set_caps = self._handler.set_client_capabilities
        self.assertSucceeds(set_caps, ['cap2'])
        self.assertSucceeds(set_caps, ['cap1', 'cap2'])

        # different order
        self.assertSucceeds(set_caps, ['cap3', 'cap1', 'cap2'])

        # error cases
        self.assertRaises(GitProtocolError, set_caps, ['capxxx', 'cap2'])
        self.assertRaises(GitProtocolError, set_caps, ['cap1', 'cap3'])

        # ignore innocuous but unknown capabilities
        self.assertRaises(GitProtocolError, set_caps, ['cap2', 'ignoreme'])
        self.assertFalse('ignoreme' in self._handler.capabilities())
        self._handler.innocuous_capabilities = lambda: ('ignoreme',)
        self.assertSucceeds(set_caps, ['cap2', 'ignoreme'])

    def test_has_capability(self):
        self.assertRaises(GitProtocolError, self._handler.has_capability, 'cap')
        caps = self._handler.capabilities()
        self._handler.set_client_capabilities(caps)
        for cap in caps:
            self.assertTrue(self._handler.has_capability(cap))
        self.assertFalse(self._handler.has_capability('capxxx'))


class UploadPackHandlerTestCase(TestCase):

    def setUp(self):
        self._backend = DictBackend({"/": BackendRepo()})
        self._handler = UploadPackHandler(self._backend,
                ["/", "host=lolcathost"], None, None)
        self._handler.proto = TestProto()

    def test_progress(self):
        caps = self._handler.required_capabilities()
        self._handler.set_client_capabilities(caps)
        self._handler.progress('first message')
        self._handler.progress('second message')
        self.assertEqual('first message',
                         self._handler.proto.get_received_line(2))
        self.assertEqual('second message',
                         self._handler.proto.get_received_line(2))
        self.assertEqual(None, self._handler.proto.get_received_line(2))

    def test_no_progress(self):
        caps = list(self._handler.required_capabilities()) + ['no-progress']
        self._handler.set_client_capabilities(caps)
        self._handler.progress('first message')
        self._handler.progress('second message')
        self.assertEqual(None, self._handler.proto.get_received_line(2))

    def test_get_tagged(self):
        refs = {
            'refs/tags/tag1': ONE,
            'refs/tags/tag2': TWO,
            'refs/heads/master': FOUR,  # not a tag, no peeled value
            }
        peeled = {
            'refs/tags/tag1': '1234',
            'refs/tags/tag2': '5678',
            }

        class TestRepo(object):
            def get_peeled(self, ref):
                return peeled.get(ref, refs[ref])

        caps = list(self._handler.required_capabilities()) + ['include-tag']
        self._handler.set_client_capabilities(caps)
        self.assertEquals({'1234': ONE, '5678': TWO},
                          self._handler.get_tagged(refs, repo=TestRepo()))

        # non-include-tag case
        caps = self._handler.required_capabilities()
        self._handler.set_client_capabilities(caps)
        self.assertEquals({}, self._handler.get_tagged(refs, repo=TestRepo()))


class TestCommit(object):

    def __init__(self, sha, parents, commit_time):
        self.id = sha
        self.parents = parents
        self.commit_time = commit_time
        self.type_name = "commit"

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._sha)


class TestRepo(object):
    def __init__(self):
        self.peeled = {}

    def get_peeled(self, name):
        return self.peeled[name]


class TestBackend(object):

    def __init__(self, repo, objects):
        self.repo = repo
        self.object_store = objects


class TestUploadPackHandler(Handler):

    def __init__(self, objects, proto):
        self.backend = TestBackend(TestRepo(), objects)
        self.proto = proto
        self.stateless_rpc = False
        self.advertise_refs = False

    def capabilities(self):
        return ('multi_ack',)


class ProtocolGraphWalkerTestCase(TestCase):

    def setUp(self):
        # Create the following commit tree:
        #   3---5
        #  /
        # 1---2---4
        self._objects = {
          ONE: TestCommit(ONE, [], 111),
          TWO: TestCommit(TWO, [ONE], 222),
          THREE: TestCommit(THREE, [ONE], 333),
          FOUR: TestCommit(FOUR, [TWO], 444),
          FIVE: TestCommit(FIVE, [THREE], 555),
          }

        self._walker = ProtocolGraphWalker(
            TestUploadPackHandler(self._objects, TestProto()),
            self._objects, None)

    def test_is_satisfied_no_haves(self):
        self.assertFalse(self._walker._is_satisfied([], ONE, 0))
        self.assertFalse(self._walker._is_satisfied([], TWO, 0))
        self.assertFalse(self._walker._is_satisfied([], THREE, 0))

    def test_is_satisfied_have_root(self):
        self.assertTrue(self._walker._is_satisfied([ONE], ONE, 0))
        self.assertTrue(self._walker._is_satisfied([ONE], TWO, 0))
        self.assertTrue(self._walker._is_satisfied([ONE], THREE, 0))

    def test_is_satisfied_have_branch(self):
        self.assertTrue(self._walker._is_satisfied([TWO], TWO, 0))
        # wrong branch
        self.assertFalse(self._walker._is_satisfied([TWO], THREE, 0))

    def test_all_wants_satisfied(self):
        self._walker.set_wants([FOUR, FIVE])
        # trivial case: wants == haves
        self.assertTrue(self._walker.all_wants_satisfied([FOUR, FIVE]))
        # cases that require walking the commit tree
        self.assertTrue(self._walker.all_wants_satisfied([ONE]))
        self.assertFalse(self._walker.all_wants_satisfied([TWO]))
        self.assertFalse(self._walker.all_wants_satisfied([THREE]))
        self.assertTrue(self._walker.all_wants_satisfied([TWO, THREE]))

    def test_read_proto_line(self):
        self._walker.proto.set_output([
          'want %s' % ONE,
          'want %s' % TWO,
          'have %s' % THREE,
          'foo %s' % FOUR,
          'bar',
          'done',
          ])
        self.assertEquals(('want', ONE), self._walker.read_proto_line())
        self.assertEquals(('want', TWO), self._walker.read_proto_line())
        self.assertEquals(('have', THREE), self._walker.read_proto_line())
        self.assertRaises(GitProtocolError, self._walker.read_proto_line)
        self.assertRaises(GitProtocolError, self._walker.read_proto_line)
        self.assertEquals(('done', None), self._walker.read_proto_line())
        self.assertEquals((None, None), self._walker.read_proto_line())

    def test_determine_wants(self):
        self.assertRaises(GitProtocolError, self._walker.determine_wants, {})

        self._walker.proto.set_output([
          'want %s multi_ack' % ONE,
          'want %s' % TWO,
          ])
        heads = {'ref1': ONE, 'ref2': TWO, 'ref3': THREE}
        self._walker.get_peeled = heads.get
        self.assertEquals([ONE, TWO], self._walker.determine_wants(heads))

        self._walker.proto.set_output(['want %s multi_ack' % FOUR])
        self.assertRaises(GitProtocolError, self._walker.determine_wants, heads)

        self._walker.proto.set_output([])
        self.assertEquals([], self._walker.determine_wants(heads))

        self._walker.proto.set_output(['want %s multi_ack' % ONE, 'foo'])
        self.assertRaises(GitProtocolError, self._walker.determine_wants, heads)

        self._walker.proto.set_output(['want %s multi_ack' % FOUR])
        self.assertRaises(GitProtocolError, self._walker.determine_wants, heads)

    def test_determine_wants_advertisement(self):
        self._walker.proto.set_output([])
        # advertise branch tips plus tag
        heads = {'ref4': FOUR, 'ref5': FIVE, 'tag6': SIX}
        peeled = {'ref4': FOUR, 'ref5': FIVE, 'tag6': FIVE}
        self._walker.get_peeled = peeled.get
        self._walker.determine_wants(heads)
        lines = []
        while True:
            line = self._walker.proto.get_received_line()
            if line == 'None':
                break
            # strip capabilities list if present
            if '\x00' in line:
                line = line[:line.index('\x00')]
            lines.append(line.rstrip())

        self.assertEquals([
          '%s ref4' % FOUR,
          '%s ref5' % FIVE,
          '%s tag6^{}' % FIVE,
          '%s tag6' % SIX,
          ], sorted(lines))

        # ensure peeled tag was advertised immediately following tag
        for i, line in enumerate(lines):
            if line.endswith(' tag6'):
                self.assertEquals('%s tag6^{}' % FIVE, lines[i+1])

    # TODO: test commit time cutoff


class TestProtocolGraphWalker(object):

    def __init__(self):
        self.acks = []
        self.lines = []
        self.done = False
        self.stateless_rpc = False
        self.advertise_refs = False

    def read_proto_line(self):
        return self.lines.pop(0)

    def send_ack(self, sha, ack_type=''):
        self.acks.append((sha, ack_type))

    def send_nak(self):
        self.acks.append((None, 'nak'))

    def all_wants_satisfied(self, haves):
        return self.done

    def pop_ack(self):
        if not self.acks:
            return None
        return self.acks.pop(0)


class AckGraphWalkerImplTestCase(TestCase):
    """Base setup and asserts for AckGraphWalker tests."""

    def setUp(self):
        self._walker = TestProtocolGraphWalker()
        self._walker.lines = [
          ('have', TWO),
          ('have', ONE),
          ('have', THREE),
          ('done', None),
          ]
        self._impl = self.impl_cls(self._walker)

    def assertNoAck(self):
        self.assertEquals(None, self._walker.pop_ack())

    def assertAcks(self, acks):
        for sha, ack_type in acks:
            self.assertEquals((sha, ack_type), self._walker.pop_ack())
        self.assertNoAck()

    def assertAck(self, sha, ack_type=''):
        self.assertAcks([(sha, ack_type)])

    def assertNak(self):
        self.assertAck(None, 'nak')

    def assertNextEquals(self, sha):
        self.assertEquals(sha, self._impl.next())


class SingleAckGraphWalkerImplTestCase(AckGraphWalkerImplTestCase):

    impl_cls = SingleAckGraphWalkerImpl

    def test_single_ack(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._walker.done = True
        self._impl.ack(ONE)
        self.assertAck(ONE)

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNoAck()

    def test_single_ack_flush(self):
        # same as ack test but ends with a flush-pkt instead of done
        self._walker.lines[-1] = (None, None)

        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._walker.done = True
        self._impl.ack(ONE)
        self.assertAck(ONE)

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNoAck()

    def test_single_ack_nak(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNak()

    def test_single_ack_nak_flush(self):
        # same as nak test but ends with a flush-pkt instead of done
        self._walker.lines[-1] = (None, None)

        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNak()


class MultiAckGraphWalkerImplTestCase(AckGraphWalkerImplTestCase):

    impl_cls = MultiAckGraphWalkerImpl

    def test_multi_ack(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._walker.done = True
        self._impl.ack(ONE)
        self.assertAck(ONE, 'continue')

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, 'continue')

        self.assertNextEquals(None)
        self.assertAck(THREE)

    def test_multi_ack_partial(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, 'continue')

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        # done, re-send ack of last common
        self.assertAck(ONE)

    def test_multi_ack_flush(self):
        self._walker.lines = [
          ('have', TWO),
          (None, None),
          ('have', ONE),
          ('have', THREE),
          ('done', None),
          ]
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNak()  # nak the flush-pkt

        self._walker.done = True
        self._impl.ack(ONE)
        self.assertAck(ONE, 'continue')

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, 'continue')

        self.assertNextEquals(None)
        self.assertAck(THREE)

    def test_multi_ack_nak(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNak()


class MultiAckDetailedGraphWalkerImplTestCase(AckGraphWalkerImplTestCase):

    impl_cls = MultiAckDetailedGraphWalkerImpl

    def test_multi_ack(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._walker.done = True
        self._impl.ack(ONE)
        self.assertAcks([(ONE, 'common'), (ONE, 'ready')])

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, 'ready')

        self.assertNextEquals(None)
        self.assertAck(THREE)

    def test_multi_ack_partial(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self._impl.ack(ONE)
        self.assertAck(ONE, 'common')

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        # done, re-send ack of last common
        self.assertAck(ONE)

    def test_multi_ack_flush(self):
        # same as ack test but contains a flush-pkt in the middle
        self._walker.lines = [
          ('have', TWO),
          (None, None),
          ('have', ONE),
          ('have', THREE),
          ('done', None),
          ]
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNak()  # nak the flush-pkt

        self._walker.done = True
        self._impl.ack(ONE)
        self.assertAcks([(ONE, 'common'), (ONE, 'ready')])

        self.assertNextEquals(THREE)
        self._impl.ack(THREE)
        self.assertAck(THREE, 'ready')

        self.assertNextEquals(None)
        self.assertAck(THREE)

    def test_multi_ack_nak(self):
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNak()

    def test_multi_ack_nak_flush(self):
        # same as nak test but contains a flush-pkt in the middle
        self._walker.lines = [
          ('have', TWO),
          (None, None),
          ('have', ONE),
          ('have', THREE),
          ('done', None),
          ]
        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNak()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNak()

    def test_multi_ack_stateless(self):
        # transmission ends with a flush-pkt
        self._walker.lines[-1] = (None, None)
        self._walker.stateless_rpc = True

        self.assertNextEquals(TWO)
        self.assertNoAck()

        self.assertNextEquals(ONE)
        self.assertNoAck()

        self.assertNextEquals(THREE)
        self.assertNoAck()

        self.assertNextEquals(None)
        self.assertNak()
