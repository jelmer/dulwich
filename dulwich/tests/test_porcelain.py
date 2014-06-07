# test_porcelain.py -- porcelain tests
# Copyright (C) 2013 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version.
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

"""Tests for dulwich.porcelain."""

from io import BytesIO
import os
import shutil
import tarfile
import tempfile

from dulwich import porcelain
from dulwich.diff_tree import tree_changes
from dulwich.objects import (
    Blob,
    Tag,
    Tree,
    )
from dulwich.repo import Repo
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.compat.utils import require_git_version
from dulwich.tests.utils import (
    build_commit_graph,
    make_object,
    )


class PorcelainTestCase(TestCase):

    def setUp(self):
        super(TestCase, self).setUp()
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        self.repo = Repo.init(repo_dir)


class ArchiveTests(PorcelainTestCase):
    """Tests for the archive command."""

    def test_simple(self):
        # TODO(jelmer): Remove this once dulwich has its own implementation of archive.
        require_git_version((1, 5, 0))
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs["refs/heads/master"] = c3.id
        out = BytesIO()
        err = BytesIO()
        porcelain.archive(self.repo.path, "refs/heads/master", outstream=out,
            errstream=err)
        self.assertEqual("", err.getvalue())
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEqual([], tf.getnames())


class UpdateServerInfoTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["refs/heads/foo"] = c3.id
        porcelain.update_server_info(self.repo.path)
        self.assertTrue(os.path.exists(os.path.join(self.repo.controldir(),
            'info', 'refs')))


class CommitTests(PorcelainTestCase):

    def test_custom_author(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["refs/heads/foo"] = c3.id
        sha = porcelain.commit(self.repo.path, message="Some message",
                author="Joe <joe@example.com>", committer="Bob <bob@example.com>")
        self.assertTrue(isinstance(sha, str))
        self.assertEqual(len(sha), 40)


class CloneTests(PorcelainTestCase):

    def test_simple_local(self):
        f1_1 = make_object(Blob, data='f1')
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {1: [('f1', f1_1), ('f2', f1_1)],
                 2: [('f1', f1_1), ('f2', f1_1)],
                 3: [('f1', f1_1), ('f2', f1_1)], }

        c1, c2, c3 = build_commit_graph(self.repo.object_store,
                                        commit_spec, trees)
        self.repo.refs["refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        outstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path,
                            checkout=False, outstream=outstream)
        self.assertEqual(r.path, target_path)
        self.assertEqual(Repo(target_path).head(), c3.id)
        self.assertTrue('f1' not in os.listdir(target_path))
        self.assertTrue('f2' not in os.listdir(target_path))

    def test_simple_local_with_checkout(self):
        f1_1 = make_object(Blob, data='f1')
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {1: [('f1', f1_1), ('f2', f1_1)],
                 2: [('f1', f1_1), ('f2', f1_1)],
                 3: [('f1', f1_1), ('f2', f1_1)], }

        c1, c2, c3 = build_commit_graph(self.repo.object_store,
                                        commit_spec, trees)
        self.repo.refs["refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        outstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path,
                            checkout=True, outstream=outstream)
        self.assertEqual(r.path, target_path)
        self.assertEqual(Repo(target_path).head(), c3.id)
        self.assertTrue('f1' in os.listdir(target_path))
        self.assertTrue('f2' in os.listdir(target_path))

    def test_bare_local_with_checkout(self):
        f1_1 = make_object(Blob, data='f1')
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {1: [('f1', f1_1), ('f2', f1_1)],
                 2: [('f1', f1_1), ('f2', f1_1)],
                 3: [('f1', f1_1), ('f2', f1_1)], }

        c1, c2, c3 = build_commit_graph(self.repo.object_store,
                                        commit_spec, trees)
        self.repo.refs["refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        outstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path,
                            bare=True, outstream=outstream)
        self.assertEqual(r.path, target_path)
        self.assertEqual(Repo(target_path).head(), c3.id)
        self.assertFalse('f1' in os.listdir(target_path))
        self.assertFalse('f2' in os.listdir(target_path))

    def test_no_checkout_with_bare(self):
        f1_1 = make_object(Blob, data='f1')
        commit_spec = [[1]]
        trees = {1: [('f1', f1_1), ('f2', f1_1)]}

        (c1, ) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs["refs/heads/master"] = c1.id
        target_path = tempfile.mkdtemp()
        outstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        self.assertRaises(ValueError, porcelain.clone, self.repo.path,
            target_path, checkout=True, bare=True, outstream=outstream)


class InitTests(TestCase):

    def test_non_bare(self):
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        porcelain.init(repo_dir)

    def test_bare(self):
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        porcelain.init(repo_dir, bare=True)


class AddTests(PorcelainTestCase):

    def test_add_default_paths(self):

        # create a file for initial commit
        with open(os.path.join(self.repo.path, 'blah'), 'w') as f:
            f.write("\n")
        porcelain.add(repo=self.repo.path, paths=['blah'])
        porcelain.commit(repo=self.repo.path, message='test',
            author='test', committer='test')

        # Add a second test file and a file in a directory
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("\n")
        os.mkdir(os.path.join(self.repo.path, 'adir'))
        with open(os.path.join(self.repo.path, 'adir', 'afile'), 'w') as f:
            f.write("\n")
        porcelain.add(self.repo.path)

        # Check that foo was added and nothing in .git was modified
        index = self.repo.open_index()
        self.assertEqual(list(index), ['blah', 'foo', 'adir/afile'])

    def test_add_file(self):
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=["foo"])


class RemoveTests(PorcelainTestCase):

    def test_remove_file(self):
        f = open(os.path.join(self.repo.path, 'foo'), 'w')
        try:
            f.write("BAR")
        finally:
            f.close()
        porcelain.add(self.repo.path, paths=["foo"])
        porcelain.rm(self.repo.path, paths=["foo"])


class LogTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.log(self.repo.path, outstream=outstream)
        self.assertEqual(3, outstream.getvalue().count("-" * 50))

    def test_max_entries(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.log(self.repo.path, outstream=outstream, max_entries=1)
        self.assertEqual(1, outstream.getvalue().count("-" * 50))


class ShowTests(PorcelainTestCase):

    def test_nolist(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.show(self.repo.path, objects=c3.id, outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.show(self.repo.path, objects=[c3.id], outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))

    def test_blob(self):
        b = Blob.from_string("The Foo\n")
        self.repo.object_store.add_object(b)
        outstream = BytesIO()
        porcelain.show(self.repo.path, objects=[b.id], outstream=outstream)
        self.assertEqual(outstream.getvalue(), "The Foo\n")


class SymbolicRefTests(PorcelainTestCase):

    def test_set_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        self.assertRaises(ValueError, porcelain.symbolic_ref, self.repo.path, 'foobar')

    def test_set_force_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, 'force_foobar', force=True)

        #test if we actually changed the file
        with self.repo.get_named_file('HEAD') as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b'ref: refs/heads/force_foobar\n')

    def test_set_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, 'master')

    def test_set_symbolic_ref_other_than_master(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]], attrs=dict(refs='develop'))
        self.repo.refs["HEAD"] = c3.id
        self.repo.refs["refs/heads/develop"] = c3.id

        porcelain.symbolic_ref(self.repo.path, 'develop')

        #test if we actually changed the file
        with self.repo.get_named_file('HEAD') as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b'ref: refs/heads/develop\n')


class DiffTreeTests(PorcelainTestCase):

    def test_empty(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.diff_tree(self.repo.path, c2.tree, c3.tree, outstream=outstream)
        self.assertEqual(outstream.getvalue(), "")


class CommitTreeTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        b = Blob()
        b.data = "foo the bar"
        t = Tree()
        t.add("somename", 0o100644, b.id)
        self.repo.object_store.add_object(t)
        self.repo.object_store.add_object(b)
        sha = porcelain.commit_tree(
            self.repo.path, t.id, message="Withcommit.",
            author="Joe <joe@example.com>",
            committer="Jane <jane@example.com>")
        self.assertTrue(isinstance(sha, str))
        self.assertEqual(len(sha), 40)


class RevListTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        outstream = BytesIO()
        porcelain.rev_list(
            self.repo.path, [c3.id], outstream=outstream)
        self.assertEqual(
            "%s\n%s\n%s\n" % (c3.id, c2.id, c1.id),
            outstream.getvalue())


class TagTests(PorcelainTestCase):

    def test_annotated(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        porcelain.tag(self.repo.path, "tryme", 'foo <foo@bar.com>', 'bar',
                annotated=True)

        tags = self.repo.refs.as_dict("refs/tags")
        self.assertEqual(tags.keys(), ["tryme"])
        tag = self.repo['refs/tags/tryme']
        self.assertTrue(isinstance(tag, Tag))
        self.assertEqual("foo <foo@bar.com>", tag.tagger)
        self.assertEqual("bar", tag.message)

    def test_unannotated(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        porcelain.tag(self.repo.path, "tryme", annotated=False)

        tags = self.repo.refs.as_dict("refs/tags")
        self.assertEqual(tags.keys(), ["tryme"])
        self.repo['refs/tags/tryme']
        self.assertEqual(tags.values(), [self.repo.head()])


class ListTagsTests(PorcelainTestCase):

    def test_empty(self):
        tags = porcelain.list_tags(self.repo.path)
        self.assertEqual([], tags)

    def test_simple(self):
        self.repo.refs["refs/tags/foo"] = "aa" * 20
        self.repo.refs["refs/tags/bar/bla"] = "bb" * 20
        tags = porcelain.list_tags(self.repo.path)

        self.assertEqual(["bar/bla", "foo"], tags)


class ResetTests(PorcelainTestCase):

    def test_hard_head(self):
        f = open(os.path.join(self.repo.path, 'foo'), 'w')
        try:
            f.write("BAR")
        finally:
            f.close()
        porcelain.add(self.repo.path, paths=["foo"])
        porcelain.commit(self.repo.path, message="Some message",
                committer="Jane <jane@example.com>",
                author="John <john@example.com>")

        f = open(os.path.join(self.repo.path, 'foo'), 'w')
        try:
            f.write("OOH")
        finally:
            f.close()

        porcelain.reset(self.repo, "hard", "HEAD")

        index = self.repo.open_index()
        changes = list(tree_changes(self.repo,
                       index.commit(self.repo.object_store),
                       self.repo['HEAD'].tree))

        self.assertEqual([], changes)


class PushTests(PorcelainTestCase):

    def test_simple(self):
        """
        Basic test of porcelain push where self.repo is the remote.  First
        clone the remote, commit a file to the clone, then push the changes
        back to the remote.
        """
        outstream = BytesIO()
        errstream = BytesIO()

        porcelain.commit(repo=self.repo.path, message='init',
            author='', committer='')

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        porcelain.clone(self.repo.path, target=clone_path, outstream=outstream)

        # create a second file to be pushed back to origin
        handle, fullpath = tempfile.mkstemp(dir=clone_path)
        porcelain.add(repo=clone_path, paths=[os.path.basename(fullpath)])
        porcelain.commit(repo=clone_path, message='push',
            author='', committer='')

        # Setup a non-checked out branch in the remote
        refs_path = os.path.join('refs', 'heads', 'foo')
        self.repo[refs_path] = self.repo['HEAD']

        # Push to the remote
        porcelain.push(clone_path, self.repo.path, refs_path, outstream=outstream,
                errstream=errstream)

        # Check that the target and source
        r_clone = Repo(clone_path)

        # Get the change in the target repo corresponding to the add
        # this will be in the foo branch.
        change = list(tree_changes(self.repo, self.repo['HEAD'].tree,
                                   self.repo['refs/heads/foo'].tree))[0]

        self.assertEqual(r_clone['HEAD'].id, self.repo[refs_path].id)
        self.assertEqual(os.path.basename(fullpath), change.new.path)


class PullTests(PorcelainTestCase):

    def test_simple(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        filename = os.path.basename(fullpath)
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message='test',
                         author='test', committer='test')

        # Setup target repo
        target_path = tempfile.mkdtemp()
        porcelain.clone(self.repo.path, target=target_path, outstream=outstream)

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        filename = os.path.basename(fullpath)
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message='test2',
            author='test2', committer='test2')

        # Pull changes into the cloned repo
        porcelain.pull(target_path, self.repo.path, 'refs/heads/master',
            outstream=outstream, errstream=errstream)

        # Check the target repo for pushed changes
        r = Repo(target_path)
        self.assertEqual(r['HEAD'].id, self.repo['HEAD'].id)


class StatusTests(PorcelainTestCase):

    def test_status(self):
        """Integration test for `status` functionality."""

        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write('origstuff')

        porcelain.add(repo=self.repo.path, paths=['foo'])
        porcelain.commit(repo=self.repo.path, message='test status',
            author='', committer='')

        # modify access and modify time of path
        os.utime(fullpath, (0, 0))

        with open(fullpath, 'w') as f:
            f.write('stuff')

        # Make a dummy file and stage it
        filename_add = 'bar'
        fullpath = os.path.join(self.repo.path, filename_add)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename_add)

        results = porcelain.status(self.repo)

        self.assertEqual(results.staged['add'][0], filename_add)
        self.assertEqual(results.unstaged, ['foo'])

    def test_get_tree_changes_add(self):
        """Unit test for get_tree_changes add."""

        # Make a dummy file, stage
        filename = 'bar'
        with open(os.path.join(self.repo.path, filename), 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message='test status',
            author='', committer='')

        filename = 'foo'
        with open(os.path.join(self.repo.path, filename), 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['add'][0], filename)
        self.assertEqual(len(changes['add']), 1)
        self.assertEqual(len(changes['modify']), 0)
        self.assertEqual(len(changes['delete']), 0)

    def test_get_tree_changes_modify(self):
        """Unit test for get_tree_changes modify."""

        # Make a dummy file, stage, commit, modify
        filename = 'foo'
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message='test status',
            author='', committer='')
        with open(fullpath, 'w') as f:
            f.write('otherstuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['modify'][0], filename)
        self.assertEqual(len(changes['add']), 0)
        self.assertEqual(len(changes['modify']), 1)
        self.assertEqual(len(changes['delete']), 0)

    def test_get_tree_changes_delete(self):
        """Unit test for get_tree_changes delete."""

        # Make a dummy file, stage, commit, remove
        filename = 'foo'
        with open(os.path.join(self.repo.path, filename), 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message='test status',
            author='', committer='')
        porcelain.rm(repo=self.repo.path, paths=[filename])
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['delete'][0], filename)
        self.assertEqual(len(changes['add']), 0)
        self.assertEqual(len(changes['modify']), 0)
        self.assertEqual(len(changes['delete']), 1)


# TODO(jelmer): Add test for dulwich.porcelain.daemon
