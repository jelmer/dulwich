# test_grafts.py -- Tests for graftpoints
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

"""Tests for graftpoints."""

import os
import tempfile
import shutil

from dulwich.errors import ObjectFormatException
from dulwich.tests import TestCase
from dulwich.objects import (
    Tree,
    )
from dulwich.repo import (
    parse_graftpoints,
    serialize_graftpoints,
    MemoryRepo,
    Repo,
)


def makesha(digit):
    return (str(digit) * 40)[:40]


class GraftParserTests(TestCase):

    def assertParse(self, expected, graftpoints):
        self.assertEqual(expected, parse_graftpoints(iter(graftpoints)))

    def test_no_grafts(self):
        self.assertParse({}, [])

    def test_no_parents(self):
        self.assertParse({makesha(0): []}, [makesha(0)])

    def test_parents(self):
        self.assertParse({makesha(0): [makesha(1), makesha(2)]},
                         [' '.join([makesha(0), makesha(1), makesha(2)])])

    def test_multiple_hybrid(self):
        self.assertParse(
            {makesha(0): [],
             makesha(1): [makesha(2)],
             makesha(3): [makesha(4), makesha(5)]},
            [makesha(0),
             ' '.join([makesha(1), makesha(2)]),
             ' '.join([makesha(3), makesha(4), makesha(5)])])


class GraftSerializerTests(TestCase):

    def assertSerialize(self, expected, graftpoints):
        self.assertEqual(
            sorted(expected),
            sorted(serialize_graftpoints(graftpoints)))

    def test_no_grafts(self):
        self.assertSerialize('', {})

    def test_no_parents(self):
        self.assertSerialize(makesha(0), {makesha(0): []})

    def test_parents(self):
        self.assertSerialize(' '.join([makesha(0), makesha(1), makesha(2)]),
                             {makesha(0): [makesha(1), makesha(2)]})

    def test_multiple_hybrid(self):
        self.assertSerialize(
            '\n'.join([
                makesha(0),
                ' '.join([makesha(1), makesha(2)]),
                ' '.join([makesha(3), makesha(4), makesha(5)])]),
            {makesha(0): [],
             makesha(1): [makesha(2)],
             makesha(3): [makesha(4), makesha(5)]})


class GraftsInRepositoryBase(object):

    def tearDown(self):
        super(GraftsInRepositoryBase, self).tearDown()

    def get_repo_with_grafts(self, grafts):
        r = self._repo
        r._add_graftpoints(grafts)
        return r

    def test_no_grafts(self):
        r = self.get_repo_with_grafts({})

        shas = [e.commit.id for e in r.get_walker()]
        self.assertEqual(shas, self._shas[::-1])

    def test_no_parents_graft(self):
        r = self.get_repo_with_grafts({self._repo.head(): []})

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [r.head()])

    def test_existing_parent_graft(self):
        r = self.get_repo_with_grafts({self._shas[-1]: [self._shas[0]]})

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [self._shas[-1], self._shas[0]])

    def test_remove_graft(self):
        r = self.get_repo_with_grafts({self._repo.head(): []})
        r._remove_graftpoints([self._repo.head()])

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         self._shas[::-1])

    def test_object_store_fail_invalid_parents(self):
        r = self._repo

        self.assertRaises(
            ObjectFormatException,
            r._add_graftpoints,
            {self._shas[-1]: ['1']})


class GraftsInRepoTests(GraftsInRepositoryBase, TestCase):

    def setUp(self):
        super(GraftsInRepoTests, self).setUp()
        self._repo_dir = os.path.join(tempfile.mkdtemp())
        r = self._repo = Repo.init(self._repo_dir)
        self.addCleanup(shutil.rmtree, self._repo_dir)

        self._shas = []

        commit_kwargs = {
            'committer': 'Test Committer <test@nodomain.com>',
            'author': 'Test Author <test@nodomain.com>',
            'commit_timestamp': 12395,
            'commit_timezone': 0,
            'author_timestamp': 12395,
            'author_timezone': 0,
        }

        self._shas.append(r.do_commit(
            'empty commit', **commit_kwargs))
        self._shas.append(r.do_commit(
            'empty commit', **commit_kwargs))
        self._shas.append(r.do_commit(
            'empty commit', **commit_kwargs))

    def test_init_with_empty_info_grafts(self):
        r = self._repo
        r._put_named_file(os.path.join('info', 'grafts'), '')

        r = Repo(self._repo_dir)
        self.assertEqual({}, r._graftpoints)

    def test_init_with_info_grafts(self):
        r = self._repo
        r._put_named_file(
            os.path.join('info', 'grafts'),
            "%s %s" % (self._shas[-1], self._shas[0]))

        r = Repo(self._repo_dir)
        self.assertEqual({self._shas[-1]: [self._shas[0]]}, r._graftpoints)


class GraftsInMemoryRepoTests(GraftsInRepositoryBase, TestCase):

    def setUp(self):
        super(GraftsInMemoryRepoTests, self).setUp()
        r = self._repo = MemoryRepo()

        self._shas = []

        tree = Tree()

        commit_kwargs = {
            'committer': 'Test Committer <test@nodomain.com>',
            'author': 'Test Author <test@nodomain.com>',
            'commit_timestamp': 12395,
            'commit_timezone': 0,
            'author_timestamp': 12395,
            'author_timezone': 0,
            'tree': tree.id
        }

        self._shas.append(r.do_commit(
            'empty commit', **commit_kwargs))
        self._shas.append(r.do_commit(
            'empty commit', **commit_kwargs))
        self._shas.append(r.do_commit(
            'empty commit', **commit_kwargs))
