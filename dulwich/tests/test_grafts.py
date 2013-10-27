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
import warnings

from dulwich.errors import ObjectFormatException
from dulwich.tests import TestCase
from dulwich.objects import (
    Commit,
    GraftedCommit,
    Tree,
)
from dulwich.object_store import (
    MemoryObjectStore,
)
from dulwich.repo import (
    MemoryRepo,
    Repo,
)
from dulwich.tests.utils import (
    make_commit,
    setup_warning_catcher
)


default_committer = 'James Westby <jw+debian@jameswestby.net> 1174773719 +0000'


class GraftedCommitTests(TestCase):

    def setUp(self):
        self._gc = GraftedCommit(commit=self.make_commit())

    def make_commit(self):
        attrs = {'tree': 'd80c186a03f423a81b39df39dc87fd269736ca86',
                 'parents': ['ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                             '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6'],
                 'author': 'James Westby <jw+debian@jameswestby.net>',
                 'committer': 'James Westby <jw+debian@jameswestby.net>',
                 'commit_time': 1174773719,
                 'author_time': 1174773719,
                 'commit_timezone': 0,
                 'author_timezone': 0,
                 'message':  'Merge ../b\n'}
        return make_commit(**attrs)

    def test_graft_sha_equality(self):
        c = self.make_commit()
        gc = GraftedCommit(c, [])
        self.assertEqual(gc.sha, c.sha)

        gc = self._gc
        gc.fake_parents = []
        self.assertEqual(gc.sha, c.sha)

    def test_graft_inherited_from_commit(self):
        gc = self._gc
        self.assertTrue(isinstance(gc, Commit))
        self.assertTrue(issubclass(GraftedCommit, Commit))

    def test_graft_serialization(self):
        c = self.make_commit()
        gc = self._gc
        gc.fake_parents = []

        self.assertEqual(gc.as_raw_string(), c.as_raw_string())

    def test_graft_real_parents_property(self):
        gc = self._gc
        gc.real_parents = []
        self.assertEqual([], gc.real_parents)

    def test_graft_fake_parents_property(self):
        gc = self._gc
        gc.fake_parents = []
        self.assertEqual([], gc.fake_parents)

    def test_graft_parents_property_no_fake(self):
        gc = self._gc
        gc.parents = []
        self.assertEqual([], gc.parents)

    def test_graft_parents_property_existing_fake(self):
        gc = self._gc
        gc.fake_parents = ['4cffe90e0a41ad3f5190079d7c8f036bde29cbe6']
        self.assertEqual(['4cffe90e0a41ad3f5190079d7c8f036bde29cbe6'],
                         gc.fake_parents)

        gc.parents = []
        self.assertEqual([], gc.parents)
        self.assertEqual([], gc.fake_parents)

    def test_graft_no_parents(self):
        gc = self._gc
        gc.fake_parents = []

        self.assertEqual([], gc.parents)
        self.assertEqual(['ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                          '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6'],
                         gc.real_parents)

    def test_graft_fake_parent(self):
        gc = self._gc
        gc.fake_parents = ['0d89f20333fbb1d2f3a94da77f4981373d8f4310']

        self.assertEqual(
            ['0d89f20333fbb1d2f3a94da77f4981373d8f4310'],
            gc.parents)
        self.assertEqual(['ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                          '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6'],
                         gc.real_parents)

    def test_graft_check(self):
        gc = self._gc

        gc.fake_parents = ['0d89f20333fbb1d2f3a94da77f4981373d8f4310']
        gc.check()

        gc.fake_parents = ['asdfasdf']
        self.assertRaises(ObjectFormatException, gc.check)


class GraftsInObjectStore(TestCase):

    def setUp(self):
        store = self._store = MemoryObjectStore()

        commit = self.make_commit()
        store.add_object(commit)
        self._commit_sha = commit.id

    def make_commit(self, **kwargs):
        attrs = {'tree': 'd80c186a03f423a81b39df39dc87fd269736ca86',
                 'parents': ['ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                             '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6'],
                 'author': 'James Westby <jw+debian@jameswestby.net>',
                 'committer': 'James Westby <jw+debian@jameswestby.net>',
                 'commit_time': 1174773719,
                 'author_time': 1174773719,
                 'commit_timezone': 0,
                 'author_timezone': 0,
                 'message':  'Merge ../b\n'}
        attrs.update(kwargs)
        return make_commit(**attrs)

    def test_graft_property(self):
        store = self._store

        self.assertEqual({}, store.grafts)

        store.grafts = {self._commit_sha: []}

        self.assertEqual({self._commit_sha: []}, store.grafts)

    def test_getitem(self):
        store = self._store
        store.grafts = {self._commit_sha: []}
        gc = store[self._commit_sha]
        self.assertTrue(isinstance(gc, GraftedCommit))
        self.assertEqual([], gc.parents)


class GraftsInRepositoryBase(object):

    def tearDown(self):
        super(GraftsInRepositoryBase, self).tearDown()

    def test_no_grafts(self):
        r = self._repo
        r._put_named_file(os.path.join('info', 'grafts'), '')

        shas = [e.commit.id for e in r.get_walker()]
        self.assertEqual(shas, self._shas[::-1])

    def test_no_parents_graft(self):
        r = self._repo
        r._put_named_file(os.path.join('info', 'grafts'), r.head())

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [r.head()])

    def test_existing_parent_graft(self):
        r = self._repo
        r._put_named_file(
            os.path.join('info', 'grafts'),
            "%s %s" % (self._shas[-1], self._shas[0]))

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [self._shas[-1], self._shas[0]])

    def test_add_no_parents_graft(self):
        r = self._repo
        r.add_grafts({r.head(): []})

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [r.head()])

    def test_add_existing_parent_graft(self):
        r = self._repo
        r.add_grafts({self._shas[-1]: [self._shas[0]]})

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [self._shas[-1], self._shas[0]])

    def test_remove_graft(self):
        r = self._repo
        r.add_grafts({r.head(): []})
        r.remove_grafts([r.head()])

        self.assertEqual([e.commit.id for e in r.get_walker()],
                         self._shas[::-1])

    def test_seralize_grafts(self):
        r = self._repo
        r.add_grafts({self._shas[-1]: [self._shas[0]]})

        grafts = r.serialize_grafts()
        self.assertEqual(["%s %s" % (self._shas[-1], self._shas[0])],
                         grafts.splitlines())

    def test_object_store_skip_invalid_parents(self):
        r = self._repo

        warnings.simplefilter("always", UserWarning)
        warnings_list = setup_warning_catcher()

        r.add_grafts({self._shas[-1]: ['1' * 40]})

        self.assertEqual(len(warnings_list), 1)
        self.assertIsInstance(warnings_list[-1], UserWarning)

        shas = [e.commit.id for e in r.get_walker()]
        self.assertEqual(shas, self._shas[::-1])


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
        r._put_named_file(os.path.join('info', 'grafts'), r.head())

        r = Repo(self._repo_dir)


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
