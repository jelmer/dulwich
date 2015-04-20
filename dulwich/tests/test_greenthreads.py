# test_greenthreads.py -- Unittests for eventlet.
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Fabien Boucher <fabien.boucher@enovance.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
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

import time

from dulwich.tests import (
    skipIf,
    TestCase,
    )
from dulwich.object_store import (
    MemoryObjectStore,
    MissingObjectFinder,
    )
from dulwich.objects import (
    Commit,
    Blob,
    Tree,
    parse_timezone,
    )

try:
    import gevent
    gevent_support = True
except ImportError:
    gevent_support = False

if gevent_support:
    from dulwich.greenthreads import (
        GreenThreadsObjectStoreIterator,
        GreenThreadsMissingObjectFinder,
    )

skipmsg = "Gevent library is not installed"

def create_commit(marker=None):
    blob = Blob.from_string('The blob content %s' % marker)
    tree = Tree()
    tree.add("thefile %s" % marker, 0o100644, blob.id)
    cmt = Commit()
    cmt.tree = tree.id
    cmt.author = cmt.committer = "John Doe <john@doe.net>"
    cmt.message = "%s" % marker
    tz = parse_timezone('-0200')[0]
    cmt.commit_time = cmt.author_time = int(time.time())
    cmt.commit_timezone = cmt.author_timezone = tz
    return cmt, tree, blob


def init_store(store, count=1):
    ret = []
    for i in range(0, count):
        objs = create_commit(marker=i)
        for obj in objs:
            ret.append(obj)
            store.add_object(obj)
    return ret


@skipIf(not gevent_support, skipmsg)
class TestGreenThreadsObjectStoreIterator(TestCase):

    def setUp(self):
        super(TestGreenThreadsObjectStoreIterator, self).setUp()
        self.store = MemoryObjectStore()
        self.cmt_amount = 10
        self.objs = init_store(self.store, self.cmt_amount)

    def test_len(self):
        wants = [sha.id for sha in self.objs if isinstance(sha, Commit)]
        finder = MissingObjectFinder(self.store, (), wants)
        iterator = GreenThreadsObjectStoreIterator(self.store,
                                               iter(finder.next, None),
                                               finder)
        # One commit refers one tree and one blob
        self.assertEqual(len(iterator), self.cmt_amount * 3)
        haves = wants[0:self.cmt_amount-1]
        finder = MissingObjectFinder(self.store, haves, wants)
        iterator = GreenThreadsObjectStoreIterator(self.store,
                                               iter(finder.next, None),
                                               finder)
        self.assertEqual(len(iterator), 3)

    def test_iter(self):
        wants = [sha.id for sha in self.objs if isinstance(sha, Commit)]
        finder = MissingObjectFinder(self.store, (), wants)
        iterator = GreenThreadsObjectStoreIterator(self.store,
                                               iter(finder.next, None),
                                               finder)
        objs = []
        for sha, path in iterator:
            self.assertIn(sha, self.objs)
            objs.append(sha)
        self.assertEqual(len(objs), len(self.objs))


@skipIf(not gevent_support, skipmsg)
class TestGreenThreadsMissingObjectFinder(TestCase):

    def setUp(self):
        super(TestGreenThreadsMissingObjectFinder, self).setUp()
        self.store = MemoryObjectStore()
        self.cmt_amount = 10
        self.objs = init_store(self.store, self.cmt_amount)

    def test_finder(self):
        wants = [sha.id for sha in self.objs if isinstance(sha, Commit)]
        finder = GreenThreadsMissingObjectFinder(self.store, (), wants)
        self.assertEqual(len(finder.sha_done), 0)
        self.assertEqual(len(finder.objects_to_send), self.cmt_amount)

        finder = GreenThreadsMissingObjectFinder(self.store,
                                             wants[0:self.cmt_amount/2],
                                             wants)
        # sha_done will contains commit id and sha of blob refered in tree
        self.assertEqual(len(finder.sha_done), (self.cmt_amount/2)*2)
        self.assertEqual(len(finder.objects_to_send), self.cmt_amount/2)
