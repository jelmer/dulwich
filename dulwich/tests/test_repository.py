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

import os
import stat
import shutil
import tempfile
import warnings

from dulwich import errors
from dulwich.object_store import (
    tree_lookup_path,
    )
from dulwich import objects
from dulwich.config import Config
from dulwich.repo import (
    Repo,
    MemoryRepo,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    open_repo,
    tear_down_repo,
    setup_warning_catcher,
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

    def test_setitem(self):
        r = self._repo = open_repo('a.git')
        r["refs/tags/foo"] = 'a90fa2d900a17e99b433217e988c4eb4a2e9a097'
        self.assertEqual('a90fa2d900a17e99b433217e988c4eb4a2e9a097',
                          r["refs/tags/foo"].id)

    def test_getitem_unicode(self):
        r = self._repo = open_repo('a.git')

        test_keys = [
            ('refs/heads/master', True),
            ('a90fa2d900a17e99b433217e988c4eb4a2e9a097', True),
            ('11' * 19 + '--', False),
        ]

        for k, contained in test_keys:
            self.assertEqual(k in r, contained)

        for k, _ in test_keys:
            self.assertRaisesRegexp(
                TypeError, "'name' must be bytestring, not unicode",
                r.__getitem__, unicode(k)
            )

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

    def test_get_no_description(self):
        r = self._repo = open_repo('a.git')
        self.assertIs(None, r.get_description())

    def test_get_description(self):
        r = self._repo = open_repo('a.git')
        f = open(os.path.join(r.path, 'description'), 'w')
        try:
            f.write("Some description")
        finally:
            f.close()
        self.assertEqual("Some description", r.get_description())

    def test_set_description(self):
        r = self._repo = open_repo('a.git')
        description = "Some description"
        r.set_description(description)
        self.assertEqual(description, r.get_description())

    def test_contains_missing(self):
        r = self._repo = open_repo('a.git')
        self.assertFalse("bar" in r)

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

    def test_get_walker(self):
        r = self._repo = open_repo('a.git')
        # include defaults to [r.head()]
        self.assertEqual([e.commit.id for e in r.get_walker()],
                         [r.head(), '2a72d929692c41d8554c07f6301757ba18a65d91'])
        self.assertEqual(
            [e.commit.id for e in r.get_walker(['2a72d929692c41d8554c07f6301757ba18a65d91'])],
            ['2a72d929692c41d8554c07f6301757ba18a65d91'])
        self.assertEqual(
            [e.commit.id for e in r.get_walker('2a72d929692c41d8554c07f6301757ba18a65d91')],
            ['2a72d929692c41d8554c07f6301757ba18a65d91'])

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

    def test_clone_no_head(self):
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_dir)
        repo_dir = os.path.join(os.path.dirname(__file__), 'data', 'repos')
        dest_dir = os.path.join(temp_dir, 'a.git')
        shutil.copytree(os.path.join(repo_dir, 'a.git'),
                        dest_dir, symlinks=True)
        r = Repo(dest_dir)
        del r.refs["refs/heads/master"]
        del r.refs["HEAD"]
        t = r.clone(os.path.join(temp_dir, 'b.git'), mkdir=True)
        self.assertEqual({
            'refs/tags/mytag': '28237f4dc30d0d462658d6b937b08a0f0b6ef55a',
            'refs/tags/mytag-packed':
                'b0931cadc54336e78a1d980420e3268903b57a50',
            }, t.refs.as_dict())

    def test_clone_empty(self):
        """Test clone() doesn't crash if HEAD points to a non-existing ref.

        This simulates cloning server-side bare repository either when it is
        still empty or if user renames master branch and pushes private repo
        to the server.
        Non-bare repo HEAD always points to an existing ref.
        """
        r = self._repo = open_repo('empty.git')
        tmp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp_dir)
        r.clone(tmp_dir, mkdir=False, bare=True)

    def test_merge_history(self):
        r = self._repo = open_repo('simple_merge.git')
        shas = [e.commit.id for e in r.get_walker()]
        self.assertEqual(shas, ['5dac377bdded4c9aeb8dff595f0faeebcc8498cc',
                                'ab64bbdcc51b170d21588e5c5d391ee5c0c96dfd',
                                '4cffe90e0a41ad3f5190079d7c8f036bde29cbe6',
                                '60dacdc733de308bb77bb76ce0fb0f9b44c9769e',
                                '0d89f20333fbb1d2f3a94da77f4981373d8f4310'])

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

    def test_submodule(self):
        temp_dir = tempfile.mkdtemp()
        repo_dir = os.path.join(os.path.dirname(__file__), 'data', 'repos')
        shutil.copytree(os.path.join(repo_dir, 'a.git'),
                        os.path.join(temp_dir, 'a.git'), symlinks=True)
        rel = os.path.relpath(os.path.join(repo_dir, 'submodule'), temp_dir)
        os.symlink(os.path.join(rel, 'dotgit'), os.path.join(temp_dir, '.git'))
        r = Repo(temp_dir)
        self.assertEqual(r.head(), 'a90fa2d900a17e99b433217e988c4eb4a2e9a097')

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
            for c in r1_commits:
                r1.object_store.add_object(r_base.get_object(c))
            r1.refs['HEAD'] = r1_commits[0]

            r2 = Repo.init_bare(r2_dir)
            for c in r2_commits:
                r2.object_store.add_object(r_base.get_object(c))
            r2.refs['HEAD'] = r2_commits[0]

            # Finally, the 'real' testing!
            shas = r2.object_store.find_common_revisions(r1.get_graph_walker())
            self.assertEqual(set(shas), expected_shas)

            shas = r1.object_store.find_common_revisions(r2.get_graph_walker())
            self.assertEqual(set(shas), expected_shas)
        finally:
            shutil.rmtree(r1_dir)
            shutil.rmtree(r2_dir)

    def test_shell_hook_pre_commit(self):
        if os.name != 'posix':
            self.skipTest('shell hook tests requires POSIX shell')

        pre_commit_fail = """#!/bin/sh
exit 1
"""

        pre_commit_success = """#!/bin/sh
exit 0
"""

        repo_dir = os.path.join(tempfile.mkdtemp())
        r = Repo.init(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)

        pre_commit = os.path.join(r.controldir(), 'hooks', 'pre-commit')

        f = open(pre_commit, 'wb')
        try:
            f.write(pre_commit_fail)
        finally:
            f.close()
        os.chmod(pre_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        self.assertRaises(errors.CommitError, r.do_commit, 'failed commit',
                          committer='Test Committer <test@nodomain.com>',
                          author='Test Author <test@nodomain.com>',
                          commit_timestamp=12345, commit_timezone=0,
                          author_timestamp=12345, author_timezone=0)

        f = open(pre_commit, 'wb')
        try:
            f.write(pre_commit_success)
        finally:
            f.close()
        os.chmod(pre_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        commit_sha = r.do_commit(
            'empty commit',
            committer='Test Committer <test@nodomain.com>',
            author='Test Author <test@nodomain.com>',
            commit_timestamp=12395, commit_timezone=0,
            author_timestamp=12395, author_timezone=0)
        self.assertEqual([], r[commit_sha].parents)

    def test_shell_hook_commit_msg(self):
        if os.name != 'posix':
            self.skipTest('shell hook tests requires POSIX shell')

        commit_msg_fail = """#!/bin/sh
exit 1
"""

        commit_msg_success = """#!/bin/sh
exit 0
"""

        repo_dir = os.path.join(tempfile.mkdtemp())
        r = Repo.init(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)

        commit_msg = os.path.join(r.controldir(), 'hooks', 'commit-msg')

        f = open(commit_msg, 'wb')
        try:
            f.write(commit_msg_fail)
        finally:
            f.close()
        os.chmod(commit_msg, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        self.assertRaises(errors.CommitError, r.do_commit, 'failed commit',
                          committer='Test Committer <test@nodomain.com>',
                          author='Test Author <test@nodomain.com>',
                          commit_timestamp=12345, commit_timezone=0,
                          author_timestamp=12345, author_timezone=0)

        f = open(commit_msg, 'wb')
        try:
            f.write(commit_msg_success)
        finally:
            f.close()
        os.chmod(commit_msg, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        commit_sha = r.do_commit(
            'empty commit',
            committer='Test Committer <test@nodomain.com>',
            author='Test Author <test@nodomain.com>',
            commit_timestamp=12395, commit_timezone=0,
            author_timestamp=12395, author_timezone=0)
        self.assertEqual([], r[commit_sha].parents)

    def test_shell_hook_post_commit(self):
        if os.name != 'posix':
            self.skipTest('shell hook tests requires POSIX shell')

        repo_dir = os.path.join(tempfile.mkdtemp())
        r = Repo.init(repo_dir)
        self.addCleanup(shutil.rmtree, repo_dir)

        (fd, path) = tempfile.mkstemp(dir=repo_dir)
        post_commit_msg = """#!/bin/sh
rm %(file)s
""" % {'file': path}

        root_sha = r.do_commit(
            'empty commit',
            committer='Test Committer <test@nodomain.com>',
            author='Test Author <test@nodomain.com>',
            commit_timestamp=12345, commit_timezone=0,
            author_timestamp=12345, author_timezone=0)
        self.assertEqual([], r[root_sha].parents)

        post_commit = os.path.join(r.controldir(), 'hooks', 'post-commit')

        f = open(post_commit, 'wb')
        try:
            f.write(post_commit_msg)
        finally:
            f.close()
        os.chmod(post_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        commit_sha = r.do_commit(
            'empty commit',
            committer='Test Committer <test@nodomain.com>',
            author='Test Author <test@nodomain.com>',
            commit_timestamp=12345, commit_timezone=0,
            author_timestamp=12345, author_timezone=0)
        self.assertEqual([root_sha], r[commit_sha].parents)

        self.assertFalse(os.path.exists(path))

        post_commit_msg_fail = """#!/bin/sh
exit 1
"""
        f = open(post_commit, 'wb')
        try:
            f.write(post_commit_msg_fail)
        finally:
            f.close()
        os.chmod(post_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        warnings.simplefilter("always", UserWarning)
        self.addCleanup(warnings.resetwarnings)
        warnings_list, restore_warnings = setup_warning_catcher()
        self.addCleanup(restore_warnings)

        commit_sha2 = r.do_commit(
            'empty commit',
            committer='Test Committer <test@nodomain.com>',
            author='Test Author <test@nodomain.com>',
            commit_timestamp=12345, commit_timezone=0,
            author_timestamp=12345, author_timezone=0)
        self.assertEqual(len(warnings_list), 1)
        self.assertIsInstance(warnings_list[-1], UserWarning)
        self.assertTrue("post-commit hook failed: " in str(warnings_list[-1]))
        self.assertEqual([commit_sha], r[commit_sha2].parents)


class BuildRepoTests(TestCase):
    """Tests that build on-disk repos from scratch.

    Repos live in a temp dir and are torn down after each test. They start with
    a single commit in master having single file named 'a'.
    """

    def setUp(self):
        super(BuildRepoTests, self).setUp()
        self._repo_dir = os.path.join(tempfile.mkdtemp(), 'test')
        os.makedirs(self._repo_dir)
        r = self._repo = Repo.init(self._repo_dir)
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
        os.symlink('a', os.path.join(self._repo_dir, 'b'))
        r.stage(['a', 'b'])
        commit_sha = r.do_commit('modified a',
                                 committer='Test Committer <test@nodomain.com>',
                                 author='Test Author <test@nodomain.com>',
                                 commit_timestamp=12395, commit_timezone=0,
                                 author_timestamp=12395, author_timezone=0)
        self.assertEqual([self._root_commit], r[commit_sha].parents)
        a_mode, a_id = tree_lookup_path(r.get_object, r[commit_sha].tree, 'a')
        self.assertEqual(stat.S_IFREG | 0o644, a_mode)
        self.assertEqual('new contents', r[a_id].data)
        b_mode, b_id = tree_lookup_path(r.get_object, r[commit_sha].tree, 'b')
        self.assertTrue(stat.S_ISLNK(b_mode))
        self.assertEqual('a', r[b_id].data)

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

    def test_commit_config_identity_in_memoryrepo(self):
        # commit falls back to the users' identity if it wasn't specified
        r = MemoryRepo.init_bare([], {})
        c = r.get_config()
        c.set(("user", ), "name", "Jelmer")
        c.set(("user", ), "email", "jelmer@apache.org")

        commit_sha = r.do_commit('message', tree=objects.Tree().id)
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

    def test_commit_dangling_commit(self):
        r = self._repo

        old_shas = set(r.object_store)
        old_refs = r.get_refs()
        commit_sha = r.do_commit('commit with no ref',
             committer='Test Committer <test@nodomain.com>',
             author='Test Author <test@nodomain.com>',
             commit_timestamp=12395, commit_timezone=0,
             author_timestamp=12395, author_timezone=0,
             ref=None)
        new_shas = set(r.object_store) - old_shas

        # New sha is added, but no new refs
        self.assertEqual(1, len(new_shas))
        new_commit = r[new_shas.pop()]
        self.assertEqual(r[self._root_commit].tree, new_commit.tree)
        self.assertEqual([], r[commit_sha].parents)
        self.assertEqual(old_refs, r.get_refs())

    def test_commit_dangling_commit_with_parents(self):
        r = self._repo

        old_shas = set(r.object_store)
        old_refs = r.get_refs()
        commit_sha = r.do_commit('commit with no ref',
             committer='Test Committer <test@nodomain.com>',
             author='Test Author <test@nodomain.com>',
             commit_timestamp=12395, commit_timezone=0,
             author_timestamp=12395, author_timezone=0,
             ref=None, merge_heads=[self._root_commit])
        new_shas = set(r.object_store) - old_shas

        # New sha is added, but no new refs
        self.assertEqual(1, len(new_shas))
        new_commit = r[new_shas.pop()]
        self.assertEqual(r[self._root_commit].tree, new_commit.tree)
        self.assertEqual([self._root_commit], r[commit_sha].parents)
        self.assertEqual(old_refs, r.get_refs())

    def test_stage_deleted(self):
        r = self._repo
        os.remove(os.path.join(r.path, 'a'))
        r.stage(['a'])
        r.stage(['a'])  # double-stage a deleted path

