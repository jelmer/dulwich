# test_repository.py -- tests for repository.py
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
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

"""Tests for the repository."""

from cStringIO import StringIO
import os
import shutil
import tempfile
import warnings

from dulwich import errors
from dulwich.file import (
    GitFile,
    )
from dulwich.object_store import (
    tree_lookup_path,
    )
from dulwich import objects
from dulwich.config import Config
from dulwich.repo import (
    check_ref_format,
    DictRefsContainer,
    InfoRefsContainer,
    Repo,
    MemoryRepo,
    read_packed_refs,
    read_packed_refs_with_peeled,
    write_packed_refs,
    _split_ref_line,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    open_repo,
    tear_down_repo,
    )

missing_sha = 'b91fa4d900e17e99b433218e988c4eb4a3e9a097'


class CreateRepositoryTests(TestCase):

    def assertFileContentsEqual(self, expected, repo, path):
        f = repo.get_named_file(path)
        if not f:
            self.assertEqual(expected, None)
        else:
            try:
                self.assertEqual(expected, f.read())
            finally:
                f.close()

    def _check_repo_contents(self, repo, expect_bare):
        self.assertEqual(expect_bare, repo.bare)
        self.assertFileContentsEqual('Unnamed repository', repo, 'description')
        self.assertFileContentsEqual('', repo, os.path.join('info', 'exclude'))
        self.assertFileContentsEqual(None, repo, 'nonexistent file')
        barestr = 'bare = %s' % str(expect_bare).lower()
        config_text = repo.get_named_file('config').read()
        self.assertTrue(barestr in config_text, "%r" % config_text)

    def test_create_disk_bare(self):
        tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp_dir)
        repo = Repo.init_bare(tmp_dir)
        self.assertEqual(tmp_dir, repo._controldir)
        self._check_repo_contents(repo, True)

    def test_create_disk_non_bare(self):
        tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp_dir)
        repo = Repo.init(tmp_dir)
        self.assertEqual(os.path.join(tmp_dir, '.git'), repo._controldir)
        self._check_repo_contents(repo, False)

    def test_create_memory(self):
        repo = MemoryRepo.init_bare([], {})
        self._check_repo_contents(repo, True)


class RepositoryTests(TestCase):

    def setUp(self):
        super(RepositoryTests, self).setUp()
        self._repo = None

    def tearDown(self):
        if self._repo is not None:
            tear_down_repo(self._repo)
        super(RepositoryTests, self).tearDown()

    def test_simple_props(self):
        r = self._repo = open_repo('a.git')
        self.assertEqual(r.controldir(), r.path)

    def test_ref(self):
        r = self._repo = open_repo('a.git')
        self.assertEqual(r.ref('refs/heads/master'),
                         'a90fa2d900a17e99b433217e988c4eb4a2e9a097')

    def test_setitem(self):
        r = self._repo = open_repo('a.git')
        r["refs/tags/foo"] = 'a90fa2d900a17e99b433217e988c4eb4a2e9a097'
        self.assertEqual('a90fa2d900a17e99b433217e988c4eb4a2e9a097',
                          r["refs/tags/foo"].id)

    def test_delitem(self):
        r = self._repo = open_repo('a.git')

        del r['refs/heads/master']
        self.assertRaises(KeyError, lambda: r['refs/heads/master'])

        del r['HEAD']
        self.assertRaises(KeyError, lambda: r['HEAD'])

        self.assertRaises(ValueError, r.__delitem__, 'notrefs/foo')

    def test_get_refs(self):
        r = self._repo = open_repo('a.git')
        self.assertEqual({
            'HEAD': 'a90fa2d900a17e99b433217e988c4eb4a2e9a097',
            'refs/heads/master': 'a90fa2d900a17e99b433217e988c4eb4a2e9a097',
            'refs/tags/mytag': '28237f4dc30d0d462658d6b937b08a0f0b6ef55a',
            'refs/tags/mytag-packed': 'b0931cadc54336e78a1d980420e3268903b57a50',
            }, r.get_refs())

    def test_head(self):
        r = self._repo = open_repo('a.git')
        self.assertEqual(r.head(), 'a90fa2d900a17e99b433217e988c4eb4a2e9a097')

    def test_get_object(self):
        r = self._repo = open_repo('a.git')
        obj = r.get_object(r.head())
        self.assertEqual(obj.type_name, 'commit')

    def test_get_object_non_existant(self):
        r = self._repo = open_repo('a.git')
        self.assertRaises(KeyError, r.get_object, missing_sha)

    def test_contains_object(self):
        r = self._repo = open_repo('a.git')
        self.assertTrue(r.head() in r)

    def test_contains_ref(self):
        r = self._repo = open_repo('a.git')
        self.assertTrue("HEAD" in r)

    def test_contains_missing(self):
        r = self._repo = open_repo('a.git')
        self.assertFalse("bar" in r)

    def test_commit(self):
        r = self._repo = open_repo('a.git')
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        obj = r.commit(r.head())
        self.assertEqual(obj.type_name, 'commit')

    def test_commit_not_commit(self):
        r = self._repo = open_repo('a.git')
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        self.assertRaises(errors.NotCommitError,
            r.commit, '4f2e6529203aa6d44b5af6e3292c837ceda003f9')

    def test_tree(self):
        r = self._repo = open_repo('a.git')
        commit = r[r.head()]
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        tree = r.tree(commit.tree)
        self.assertEqual(tree.type_name, 'tree')
        self.assertEqual(tree.sha().hexdigest(), commit.tree)

    def test_tree_not_tree(self):
        r = self._repo = open_repo('a.git')
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        self.assertRaises(errors.NotTreeError, r.tree, r.head())

    def test_tag(self):
        r = self._repo = open_repo('a.git')
        tag_sha = '28237f4dc30d0d462658d6b937b08a0f0b6ef55a'
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        tag = r.tag(tag_sha)
        self.assertEqual(tag.type_name, 'tag')
        self.assertEqual(tag.sha().hexdigest(), tag_sha)
        obj_class, obj_sha = tag.object
        self.assertEqual(obj_class, objects.Commit)
        self.assertEqual(obj_sha, r.head())

    def test_tag_not_tag(self):
        r = self._repo = open_repo('a.git')
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        self.assertRaises(errors.NotTagError, r.tag, r.head())

    def test_get_peeled(self):
        # unpacked ref
        r = self._repo = open_repo('a.git')
        tag_sha = '28237f4dc30d0d462658d6b937b08a0f0b6ef55a'
        self.assertNotEqual(r[tag_sha].sha().hexdigest(), r.head())
        self.assertEqual(r.get_peeled('refs/tags/mytag'), r.head())

        # packed ref with cached peeled value
        packed_tag_sha = 'b0931cadc54336e78a1d980420e3268903b57a50'
        parent_sha = r[r.head()].parents[0]
        self.assertNotEqual(r[packed_tag_sha].sha().hexdigest(), parent_sha)
        self.assertEqual(r.get_peeled('refs/tags/mytag-packed'), parent_sha)

        # TODO: add more corner cases to test repo

    def test_get_peeled_not_tag(self):
        r = self._repo = open_repo('a.git')
        self.assertEqual(r.get_peeled('HEAD'), r.head())

    def test_get_blob(self):
        r = self._repo = open_repo('a.git')
        commit = r[r.head()]
        tree = r[commit.tree]
        blob_sha = tree.items()[0][2]
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        blob = r.get_blob(blob_sha)
        self.assertEqual(blob.type_name, 'blob')
        self.assertEqual(blob.sha().hexdigest(), blob_sha)

    def test_get_blob_notblob(self):
        r = self._repo = open_repo('a.git')
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        self.assertRaises(errors.NotBlobError, r.get_blob, r.head())

    def test_get_walker(self):
        r = self._repo = open_repo('a.git')
        # include defaults to [r.head()]
        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [r.head(), '2a72d929692c41d8554c07f6301757ba18a65d91'])
        self.assertEqual(
            [e.commit.id for e in r.get_walker(['2a72d929692c41d8554c07f6301757ba18a65d91'])],
            ['2a72d929692c41d8554c07f6301757ba18a65d91'])

    def test_linear_history(self):
        r = self._repo = open_repo('a.git')
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        history = r.revision_history(r.head())
        shas = [c.sha().hexdigest() for c in history]
        self.assertEqual(shas, [r.head(),
                                '2a72d929692c41d8554c07f6301757ba18a65d91'])

    def test_clone(self):
        r = self._repo = open_repo('a.git')
        tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp_dir)
        t = r.clone(tmp_dir, mkdir=False)
        self.assertEqual({
            'HEAD': 'a90fa2d900a17e99b433217e988c4eb4a2e9a097',
            'refs/remotes/origin/master':
                'a90fa2d900a17e99b433217e988c4eb4a2e9a097',
            'refs/heads/master': 'a90fa2d900a17e99b433217e988c4eb4a2e9a097',
            'refs/tags/mytag': '28237f4dc30d0d462658d6b937b08a0f0b6ef55a',
            'refs/tags/mytag-packed':
                'b0931cadc54336e78a1d980420e3268903b57a50',
            }, t.refs.as_dict())
        shas = [e.commit.id for e in r.get_walker()]
        self.assertEqual(shas, [t.head(),
                         '2a72d929692c41d8554c07f6301757ba18a65d91'])

    def test_merge_history(self):
        r = self._repo = open_repo('simple_merge.git')
        shas = [e.commit.id for e in r.get_walker()]
        self.assertEqual(shas, ['5dac377bdded4c9aeb8dff595f0faeebcc8498cc',
                                'ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                                '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6',
                                '60dacdc733de308bb77bb76ce0fb0f9b44c9769e',
                                '0d89f20333fbb1d2f3a94da77f4981373d8f4310'])

    def test_revision_history_missing_commit(self):
        r = self._repo = open_repo('simple_merge.git')
        warnings.simplefilter("ignore", DeprecationWarning)
        self.addCleanup(warnings.resetwarnings)
        self.assertRaises(errors.MissingCommitError, r.revision_history,
                          missing_sha)

    def test_out_of_order_merge(self):
        """Test that revision history is ordered by date, not parent order."""
        r = self._repo = open_repo('ooo_merge.git')
        shas = [e.commit.id for e in r.get_walker()]
        self.assertEqual(shas, ['7601d7f6231db6a57f7bbb79ee52e4d462fd44d1',
                                'f507291b64138b875c28e03469025b1ea20bc614',
                                'fb5b0425c7ce46959bec94d54b9a157645e114f5',
                                'f9e39b120c68182a4ba35349f832d0e4e61f485c'])

    def test_get_tags_empty(self):
        r = self._repo = open_repo('ooo_merge.git')
        self.assertEqual({}, r.refs.as_dict('refs/tags'))

    def test_get_config(self):
        r = self._repo = open_repo('ooo_merge.git')
        self.assertIsInstance(r.get_config(), Config)

    def test_get_config_stack(self):
        r = self._repo = open_repo('ooo_merge.git')
        self.assertIsInstance(r.get_config_stack(), Config)

    def test_common_revisions(self):
        """
        This test demonstrates that ``find_common_revisions()`` actually returns
        common heads, not revisions; dulwich already uses
        ``find_common_revisions()`` in such a manner (see
        ``Repo.fetch_objects()``).
        """

        expected_shas = set(['60dacdc733de308bb77bb76ce0fb0f9b44c9769e'])

        # Source for objects.
        r_base = open_repo('simple_merge.git')

        # Re-create each-side of the merge in simple_merge.git.
        #
        # Since the trees and blobs are missing, the repository created is
        # corrupted, but we're only checking for commits for the purpose of this
        # test, so it's immaterial.
        r1_dir = tempfile.mkdtemp()
        r1_commits = ['ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd', # HEAD
                      '60dacdc733de308bb77bb76ce0fb0f9b44c9769e',
                      '0d89f20333fbb1d2f3a94da77f4981373d8f4310']

        r2_dir = tempfile.mkdtemp()
        r2_commits = ['4cffe90e0a41ad3f5190079d7c8f036bde29cbe6', # HEAD
                      '60dacdc733de308bb77bb76ce0fb0f9b44c9769e',
                      '0d89f20333fbb1d2f3a94da77f4981373d8f4310']

        try:
            r1 = Repo.init_bare(r1_dir)
            map(lambda c: r1.object_store.add_object(r_base.get_object(c)), \
                r1_commits)
            r1.refs['HEAD'] = r1_commits[0]

            r2 = Repo.init_bare(r2_dir)
            map(lambda c: r2.object_store.add_object(r_base.get_object(c)), \
                r2_commits)
            r2.refs['HEAD'] = r2_commits[0]

            # Finally, the 'real' testing!
            shas = r2.object_store.find_common_revisions(r1.get_graph_walker())
            self.assertEqual(set(shas), expected_shas)

            shas = r1.object_store.find_common_revisions(r2.get_graph_walker())
            self.assertEqual(set(shas), expected_shas)
        finally:
            shutil.rmtree(r1_dir)
            shutil.rmtree(r2_dir)


class BuildRepoTests(TestCase):
    """Tests that build on-disk repos from scratch.

    Repos live in a temp dir and are torn down after each test. They start with
    a single commit in master having single file named 'a'.
    """

    def setUp(self):
        super(BuildRepoTests, self).setUp()
        repo_dir = os.path.join(tempfile.mkdtemp(), 'test')
        os.makedirs(repo_dir)
        r = self._repo = Repo.init(repo_dir)
        self.assertFalse(r.bare)
        self.assertEqual('ref: refs/heads/master', r.refs.read_ref('HEAD'))
        self.assertRaises(KeyError, lambda: r.refs['refs/heads/master'])

        f = open(os.path.join(r.path, 'a'), 'wb')
        try:
            f.write('file contents')
        finally:
            f.close()
        r.stage(['a'])
        commit_sha = r.do_commit('msg',
                                 committer='Test Committer <test@nodomain.com>',
                                 author='Test Author <test@nodomain.com>',
                                 commit_timestamp=12345, commit_timezone=0,
                                 author_timestamp=12345, author_timezone=0)
        self.assertEqual([], r[commit_sha].parents)
        self._root_commit = commit_sha

    def tearDown(self):
        tear_down_repo(self._repo)
        super(BuildRepoTests, self).tearDown()

    def test_build_repo(self):
        r = self._repo
        self.assertEqual('ref: refs/heads/master', r.refs.read_ref('HEAD'))
        self.assertEqual(self._root_commit, r.refs['refs/heads/master'])
        expected_blob = objects.Blob.from_string('file contents')
        self.assertEqual(expected_blob.data, r[expected_blob.id].data)
        actual_commit = r[self._root_commit]
        self.assertEqual('msg', actual_commit.message)

    def test_commit_modified(self):
        r = self._repo
        f = open(os.path.join(r.path, 'a'), 'wb')
        try:
            f.write('new contents')
        finally:
            f.close()
        r.stage(['a'])
        commit_sha = r.do_commit('modified a',
                                 committer='Test Committer <test@nodomain.com>',
                                 author='Test Author <test@nodomain.com>',
                                 commit_timestamp=12395, commit_timezone=0,
                                 author_timestamp=12395, author_timezone=0)
        self.assertEqual([self._root_commit], r[commit_sha].parents)
        _, blob_id = tree_lookup_path(r.get_object, r[commit_sha].tree, 'a')
        self.assertEqual('new contents', r[blob_id].data)

    def test_commit_deleted(self):
        r = self._repo
        os.remove(os.path.join(r.path, 'a'))
        r.stage(['a'])
        commit_sha = r.do_commit('deleted a',
                                 committer='Test Committer <test@nodomain.com>',
                                 author='Test Author <test@nodomain.com>',
                                 commit_timestamp=12395, commit_timezone=0,
                                 author_timestamp=12395, author_timezone=0)
        self.assertEqual([self._root_commit], r[commit_sha].parents)
        self.assertEqual([], list(r.open_index()))
        tree = r[r[commit_sha].tree]
        self.assertEqual([], list(tree.iteritems()))

    def test_commit_encoding(self):
        r = self._repo
        commit_sha = r.do_commit('commit with strange character \xee',
             committer='Test Committer <test@nodomain.com>',
             author='Test Author <test@nodomain.com>',
             commit_timestamp=12395, commit_timezone=0,
             author_timestamp=12395, author_timezone=0,
             encoding="iso8859-1")
        self.assertEqual("iso8859-1", r[commit_sha].encoding)

    def test_commit_config_identity(self):
        # commit falls back to the users' identity if it wasn't specified
        r = self._repo
        c = r.get_config()
        c.set(("user", ), "name", "Jelmer")
        c.set(("user", ), "email", "jelmer@apache.org")
        c.write_to_path()
        commit_sha = r.do_commit('message')
        self.assertEqual(
            "Jelmer <jelmer@apache.org>",
            r[commit_sha].author)
        self.assertEqual(
            "Jelmer <jelmer@apache.org>",
            r[commit_sha].committer)

    def test_commit_fail_ref(self):
        r = self._repo

        def set_if_equals(name, old_ref, new_ref):
            return False
        r.refs.set_if_equals = set_if_equals

        def add_if_new(name, new_ref):
            self.fail('Unexpected call to add_if_new')
        r.refs.add_if_new = add_if_new

        old_shas = set(r.object_store)
        self.assertRaises(errors.CommitError, r.do_commit, 'failed commit',
                          committer='Test Committer <test@nodomain.com>',
                          author='Test Author <test@nodomain.com>',
                          commit_timestamp=12345, commit_timezone=0,
                          author_timestamp=12345, author_timezone=0)
        new_shas = set(r.object_store) - old_shas
        self.assertEqual(1, len(new_shas))
        # Check that the new commit (now garbage) was added.
        new_commit = r[new_shas.pop()]
        self.assertEqual(r[self._root_commit].tree, new_commit.tree)
        self.assertEqual('failed commit', new_commit.message)

    def test_commit_branch(self):
        r = self._repo

        commit_sha = r.do_commit('commit to branch',
             committer='Test Committer <test@nodomain.com>',
             author='Test Author <test@nodomain.com>',
             commit_timestamp=12395, commit_timezone=0,
             author_timestamp=12395, author_timezone=0,
             ref="refs/heads/new_branch")
        self.assertEqual(self._root_commit, r["HEAD"].id)
        self.assertEqual(commit_sha, r["refs/heads/new_branch"].id)
        self.assertEqual([], r[commit_sha].parents)
        self.assertTrue("refs/heads/new_branch" in r)

        new_branch_head = commit_sha

        commit_sha = r.do_commit('commit to branch 2',
             committer='Test Committer <test@nodomain.com>',
             author='Test Author <test@nodomain.com>',
             commit_timestamp=12395, commit_timezone=0,
             author_timestamp=12395, author_timezone=0,
             ref="refs/heads/new_branch")
        self.assertEqual(self._root_commit, r["HEAD"].id)
        self.assertEqual(commit_sha, r["refs/heads/new_branch"].id)
        self.assertEqual([new_branch_head], r[commit_sha].parents)

    def test_commit_merge_heads(self):
        r = self._repo
        merge_1 = r.do_commit('commit to branch 2',
             committer='Test Committer <test@nodomain.com>',
             author='Test Author <test@nodomain.com>',
             commit_timestamp=12395, commit_timezone=0,
             author_timestamp=12395, author_timezone=0,
             ref="refs/heads/new_branch")
        commit_sha = r.do_commit('commit with merge',
             committer='Test Committer <test@nodomain.com>',
             author='Test Author <test@nodomain.com>',
             commit_timestamp=12395, commit_timezone=0,
             author_timestamp=12395, author_timezone=0,
             merge_heads=[merge_1])
        self.assertEqual(
            [self._root_commit, merge_1],
            r[commit_sha].parents)

    def test_stage_deleted(self):
        r = self._repo
        os.remove(os.path.join(r.path, 'a'))
        r.stage(['a'])
        r.stage(['a'])  # double-stage a deleted path


class CheckRefFormatTests(TestCase):
    """Tests for the check_ref_format function.

    These are the same tests as in the git test suite.
    """

    def test_valid(self):
        self.assertTrue(check_ref_format('heads/foo'))
        self.assertTrue(check_ref_format('foo/bar/baz'))
        self.assertTrue(check_ref_format('refs///heads/foo'))
        self.assertTrue(check_ref_format('foo./bar'))
        self.assertTrue(check_ref_format('heads/foo@bar'))
        self.assertTrue(check_ref_format('heads/fix.lock.error'))

    def test_invalid(self):
        self.assertFalse(check_ref_format('foo'))
        self.assertFalse(check_ref_format('heads/foo/'))
        self.assertFalse(check_ref_format('./foo'))
        self.assertFalse(check_ref_format('.refs/foo'))
        self.assertFalse(check_ref_format('heads/foo..bar'))
        self.assertFalse(check_ref_format('heads/foo?bar'))
        self.assertFalse(check_ref_format('heads/foo.lock'))
        self.assertFalse(check_ref_format('heads/v@{ation'))
        self.assertFalse(check_ref_format('heads/foo\bar'))


ONES = "1" * 40
TWOS = "2" * 40
THREES = "3" * 40
FOURS = "4" * 40

class PackedRefsFileTests(TestCase):

    def test_split_ref_line_errors(self):
        self.assertRaises(errors.PackedRefsException, _split_ref_line,
                          'singlefield')
        self.assertRaises(errors.PackedRefsException, _split_ref_line,
                          'badsha name')
        self.assertRaises(errors.PackedRefsException, _split_ref_line,
                          '%s bad/../refname' % ONES)

    def test_read_without_peeled(self):
        f = StringIO('# comment\n%s ref/1\n%s ref/2' % (ONES, TWOS))
        self.assertEqual([(ONES, 'ref/1'), (TWOS, 'ref/2')],
                         list(read_packed_refs(f)))

    def test_read_without_peeled_errors(self):
        f = StringIO('%s ref/1\n^%s' % (ONES, TWOS))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

    def test_read_with_peeled(self):
        f = StringIO('%s ref/1\n%s ref/2\n^%s\n%s ref/4' % (
          ONES, TWOS, THREES, FOURS))
        self.assertEqual([
          (ONES, 'ref/1', None),
          (TWOS, 'ref/2', THREES),
          (FOURS, 'ref/4', None),
          ], list(read_packed_refs_with_peeled(f)))

    def test_read_with_peeled_errors(self):
        f = StringIO('^%s\n%s ref/1' % (TWOS, ONES))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

        f = StringIO('%s ref/1\n^%s\n^%s' % (ONES, TWOS, THREES))
        self.assertRaises(errors.PackedRefsException, list, read_packed_refs(f))

    def test_write_with_peeled(self):
        f = StringIO()
        write_packed_refs(f, {'ref/1': ONES, 'ref/2': TWOS},
                          {'ref/1': THREES})
        self.assertEqual(
          "# pack-refs with: peeled\n%s ref/1\n^%s\n%s ref/2\n" % (
          ONES, THREES, TWOS), f.getvalue())

    def test_write_without_peeled(self):
        f = StringIO()
        write_packed_refs(f, {'ref/1': ONES, 'ref/2': TWOS})
        self.assertEqual("%s ref/1\n%s ref/2\n" % (ONES, TWOS), f.getvalue())


# Dict of refs that we expect all RefsContainerTests subclasses to define.
_TEST_REFS = {
  'HEAD': '42d06bd4b77fed026b154d16493e5deab78f02ec',
  'refs/heads/master': '42d06bd4b77fed026b154d16493e5deab78f02ec',
  'refs/heads/packed': '42d06bd4b77fed026b154d16493e5deab78f02ec',
  'refs/tags/refs-0.1': 'df6800012397fb85c56e7418dd4eb9405dee075c',
  'refs/tags/refs-0.2': '3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8',
  }


class RefsContainerTests(object):

    def test_keys(self):
        actual_keys = set(self._refs.keys())
        self.assertEqual(set(self._refs.allkeys()), actual_keys)
        # ignore the symref loop if it exists
        actual_keys.discard('refs/heads/loop')
        self.assertEqual(set(_TEST_REFS.iterkeys()), actual_keys)

        actual_keys = self._refs.keys('refs/heads')
        actual_keys.discard('loop')
        self.assertEqual(['master', 'packed'], sorted(actual_keys))
        self.assertEqual(['refs-0.1', 'refs-0.2'],
                         sorted(self._refs.keys('refs/tags')))

    def test_as_dict(self):
        # refs/heads/loop does not show up even if it exists
        self.assertEqual(_TEST_REFS, self._refs.as_dict())

    def test_setitem(self):
        self._refs['refs/some/ref'] = '42d06bd4b77fed026b154d16493e5deab78f02ec'
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/some/ref'])
        self.assertRaises(errors.RefFormatError, self._refs.__setitem__,
                          'notrefs/foo', '42d06bd4b77fed026b154d16493e5deab78f02ec')

    def test_set_if_equals(self):
        nines = '9' * 40
        self.assertFalse(self._refs.set_if_equals('HEAD', 'c0ffee', nines))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['HEAD'])

        self.assertTrue(self._refs.set_if_equals(
          'HEAD', '42d06bd4b77fed026b154d16493e5deab78f02ec', nines))
        self.assertEqual(nines, self._refs['HEAD'])

        self.assertTrue(self._refs.set_if_equals('refs/heads/master', None,
                                                 nines))
        self.assertEqual(nines, self._refs['refs/heads/master'])

    def test_add_if_new(self):
        nines = '9' * 40
        self.assertFalse(self._refs.add_if_new('refs/heads/master', nines))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/master'])

        self.assertTrue(self._refs.add_if_new('refs/some/ref', nines))
        self.assertEqual(nines, self._refs['refs/some/ref'])

    def test_set_symbolic_ref(self):
        self._refs.set_symbolic_ref('refs/heads/symbolic', 'refs/heads/master')
        self.assertEqual('ref: refs/heads/master',
                         self._refs.read_loose_ref('refs/heads/symbolic'))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/symbolic'])

    def test_set_symbolic_ref_overwrite(self):
        nines = '9' * 40
        self.assertFalse('refs/heads/symbolic' in self._refs)
        self._refs['refs/heads/symbolic'] = nines
        self.assertEqual(nines, self._refs.read_loose_ref('refs/heads/symbolic'))
        self._refs.set_symbolic_ref('refs/heads/symbolic', 'refs/heads/master')
        self.assertEqual('ref: refs/heads/master',
                         self._refs.read_loose_ref('refs/heads/symbolic'))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/symbolic'])

    def test_check_refname(self):
        self._refs._check_refname('HEAD')
        self._refs._check_refname('refs/stash')
        self._refs._check_refname('refs/heads/foo')

        self.assertRaises(errors.RefFormatError, self._refs._check_refname,
                          'refs')
        self.assertRaises(errors.RefFormatError, self._refs._check_refname,
                          'notrefs/foo')

    def test_contains(self):
        self.assertTrue('refs/heads/master' in self._refs)
        self.assertFalse('refs/heads/bar' in self._refs)

    def test_delitem(self):
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                          self._refs['refs/heads/master'])
        del self._refs['refs/heads/master']
        self.assertRaises(KeyError, lambda: self._refs['refs/heads/master'])

    def test_remove_if_equals(self):
        self.assertFalse(self._refs.remove_if_equals('HEAD', 'c0ffee'))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['HEAD'])
        self.assertTrue(self._refs.remove_if_equals(
          'refs/tags/refs-0.2', '3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8'))
        self.assertFalse('refs/tags/refs-0.2' in self._refs)


class DictRefsContainerTests(RefsContainerTests, TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self._refs = DictRefsContainer(dict(_TEST_REFS))

    def test_invalid_refname(self):
        # FIXME: Move this test into RefsContainerTests, but requires
        # some way of injecting invalid refs.
        self._refs._refs["refs/stash"] = "00" * 20
        expected_refs = dict(_TEST_REFS)
        expected_refs["refs/stash"] = "00" * 20
        self.assertEqual(expected_refs, self._refs.as_dict())


class DiskRefsContainerTests(RefsContainerTests, TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self._repo = open_repo('refs.git')
        self._refs = self._repo.refs

    def tearDown(self):
        tear_down_repo(self._repo)
        TestCase.tearDown(self)

    def test_get_packed_refs(self):
        self.assertEqual({
          'refs/heads/packed': '42d06bd4b77fed026b154d16493e5deab78f02ec',
          'refs/tags/refs-0.1': 'df6800012397fb85c56e7418dd4eb9405dee075c',
          }, self._refs.get_packed_refs())

    def test_get_peeled_not_packed(self):
        # not packed
        self.assertEqual(None, self._refs.get_peeled('refs/tags/refs-0.2'))
        self.assertEqual('3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8',
                         self._refs['refs/tags/refs-0.2'])

        # packed, known not peelable
        self.assertEqual(self._refs['refs/heads/packed'],
                         self._refs.get_peeled('refs/heads/packed'))

        # packed, peeled
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs.get_peeled('refs/tags/refs-0.1'))

    def test_setitem(self):
        RefsContainerTests.test_setitem(self)
        f = open(os.path.join(self._refs.path, 'refs', 'some', 'ref'), 'rb')
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                          f.read()[:40])
        f.close()

    def test_setitem_symbolic(self):
        ones = '1' * 40
        self._refs['HEAD'] = ones
        self.assertEqual(ones, self._refs['HEAD'])

        # ensure HEAD was not modified
        f = open(os.path.join(self._refs.path, 'HEAD'), 'rb')
        self.assertEqual('ref: refs/heads/master', iter(f).next().rstrip('\n'))
        f.close()

        # ensure the symbolic link was written through
        f = open(os.path.join(self._refs.path, 'refs', 'heads', 'master'), 'rb')
        self.assertEqual(ones, f.read()[:40])
        f.close()

    def test_set_if_equals(self):
        RefsContainerTests.test_set_if_equals(self)

        # ensure symref was followed
        self.assertEqual('9' * 40, self._refs['refs/heads/master'])

        # ensure lockfile was deleted
        self.assertFalse(os.path.exists(
          os.path.join(self._refs.path, 'refs', 'heads', 'master.lock')))
        self.assertFalse(os.path.exists(
          os.path.join(self._refs.path, 'HEAD.lock')))

    def test_add_if_new_packed(self):
        # don't overwrite packed ref
        self.assertFalse(self._refs.add_if_new('refs/tags/refs-0.1', '9' * 40))
        self.assertEqual('df6800012397fb85c56e7418dd4eb9405dee075c',
                         self._refs['refs/tags/refs-0.1'])

    def test_add_if_new_symbolic(self):
        # Use an empty repo instead of the default.
        tear_down_repo(self._repo)
        repo_dir = os.path.join(tempfile.mkdtemp(), 'test')
        os.makedirs(repo_dir)
        self._repo = Repo.init(repo_dir)
        refs = self._repo.refs

        nines = '9' * 40
        self.assertEqual('ref: refs/heads/master', refs.read_ref('HEAD'))
        self.assertFalse('refs/heads/master' in refs)
        self.assertTrue(refs.add_if_new('HEAD', nines))
        self.assertEqual('ref: refs/heads/master', refs.read_ref('HEAD'))
        self.assertEqual(nines, refs['HEAD'])
        self.assertEqual(nines, refs['refs/heads/master'])
        self.assertFalse(refs.add_if_new('HEAD', '1' * 40))
        self.assertEqual(nines, refs['HEAD'])
        self.assertEqual(nines, refs['refs/heads/master'])

    def test_follow(self):
        self.assertEqual(
          ('refs/heads/master', '42d06bd4b77fed026b154d16493e5deab78f02ec'),
          self._refs._follow('HEAD'))
        self.assertEqual(
          ('refs/heads/master', '42d06bd4b77fed026b154d16493e5deab78f02ec'),
          self._refs._follow('refs/heads/master'))
        self.assertRaises(KeyError, self._refs._follow, 'refs/heads/loop')

    def test_delitem(self):
        RefsContainerTests.test_delitem(self)
        ref_file = os.path.join(self._refs.path, 'refs', 'heads', 'master')
        self.assertFalse(os.path.exists(ref_file))
        self.assertFalse('refs/heads/master' in self._refs.get_packed_refs())

    def test_delitem_symbolic(self):
        self.assertEqual('ref: refs/heads/master',
                          self._refs.read_loose_ref('HEAD'))
        del self._refs['HEAD']
        self.assertRaises(KeyError, lambda: self._refs['HEAD'])
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
                         self._refs['refs/heads/master'])
        self.assertFalse(os.path.exists(os.path.join(self._refs.path, 'HEAD')))

    def test_remove_if_equals_symref(self):
        # HEAD is a symref, so shouldn't equal its dereferenced value
        self.assertFalse(self._refs.remove_if_equals(
          'HEAD', '42d06bd4b77fed026b154d16493e5deab78f02ec'))
        self.assertTrue(self._refs.remove_if_equals(
          'refs/heads/master', '42d06bd4b77fed026b154d16493e5deab78f02ec'))
        self.assertRaises(KeyError, lambda: self._refs['refs/heads/master'])

        # HEAD is now a broken symref
        self.assertRaises(KeyError, lambda: self._refs['HEAD'])
        self.assertEqual('ref: refs/heads/master',
                          self._refs.read_loose_ref('HEAD'))

        self.assertFalse(os.path.exists(
            os.path.join(self._refs.path, 'refs', 'heads', 'master.lock')))
        self.assertFalse(os.path.exists(
            os.path.join(self._refs.path, 'HEAD.lock')))

    def test_remove_packed_without_peeled(self):
        refs_file = os.path.join(self._repo.path, 'packed-refs')
        f = GitFile(refs_file)
        refs_data = f.read()
        f.close()
        f = GitFile(refs_file, 'wb')
        f.write('\n'.join(l for l in refs_data.split('\n')
                          if not l or l[0] not in '#^'))
        f.close()
        self._repo = Repo(self._repo.path)
        refs = self._repo.refs
        self.assertTrue(refs.remove_if_equals(
          'refs/heads/packed', '42d06bd4b77fed026b154d16493e5deab78f02ec'))

    def test_remove_if_equals_packed(self):
        # test removing ref that is only packed
        self.assertEqual('df6800012397fb85c56e7418dd4eb9405dee075c',
                         self._refs['refs/tags/refs-0.1'])
        self.assertTrue(
          self._refs.remove_if_equals('refs/tags/refs-0.1',
          'df6800012397fb85c56e7418dd4eb9405dee075c'))
        self.assertRaises(KeyError, lambda: self._refs['refs/tags/refs-0.1'])

    def test_read_ref(self):
        self.assertEqual('ref: refs/heads/master', self._refs.read_ref("HEAD"))
        self.assertEqual('42d06bd4b77fed026b154d16493e5deab78f02ec',
            self._refs.read_ref("refs/heads/packed"))
        self.assertEqual(None,
            self._refs.read_ref("nonexistant"))


_TEST_REFS_SERIALIZED = (
'42d06bd4b77fed026b154d16493e5deab78f02ec\trefs/heads/master\n'
'42d06bd4b77fed026b154d16493e5deab78f02ec\trefs/heads/packed\n'
'df6800012397fb85c56e7418dd4eb9405dee075c\trefs/tags/refs-0.1\n'
'3ec9c43c84ff242e3ef4a9fc5bc111fd780a76a8\trefs/tags/refs-0.2\n')


class InfoRefsContainerTests(TestCase):

    def test_invalid_refname(self):
        text = _TEST_REFS_SERIALIZED + '00' * 20 + '\trefs/stash\n'
        refs = InfoRefsContainer(StringIO(text))
        expected_refs = dict(_TEST_REFS)
        del expected_refs['HEAD']
        expected_refs["refs/stash"] = "00" * 20
        self.assertEqual(expected_refs, refs.as_dict())

    def test_keys(self):
        refs = InfoRefsContainer(StringIO(_TEST_REFS_SERIALIZED))
        actual_keys = set(refs.keys())
        self.assertEqual(set(refs.allkeys()), actual_keys)
        # ignore the symref loop if it exists
        actual_keys.discard('refs/heads/loop')
        expected_refs = dict(_TEST_REFS)
        del expected_refs['HEAD']
        self.assertEqual(set(expected_refs.iterkeys()), actual_keys)

        actual_keys = refs.keys('refs/heads')
        actual_keys.discard('loop')
        self.assertEqual(['master', 'packed'], sorted(actual_keys))
        self.assertEqual(['refs-0.1', 'refs-0.2'],
                         sorted(refs.keys('refs/tags')))

    def test_as_dict(self):
        refs = InfoRefsContainer(StringIO(_TEST_REFS_SERIALIZED))
        # refs/heads/loop does not show up even if it exists
        expected_refs = dict(_TEST_REFS)
        del expected_refs['HEAD']
        self.assertEqual(expected_refs, refs.as_dict())

    def test_contains(self):
        refs = InfoRefsContainer(StringIO(_TEST_REFS_SERIALIZED))
        self.assertTrue('refs/heads/master' in refs)
        self.assertFalse('refs/heads/bar' in refs)

    def test_get_peeled(self):
        refs = InfoRefsContainer(StringIO(_TEST_REFS_SERIALIZED))
        # refs/heads/loop does not show up even if it exists
        self.assertEqual(
            _TEST_REFS['refs/heads/master'],
            refs.get_peeled('refs/heads/master'))
