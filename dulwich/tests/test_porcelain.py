# test_porcelain.py -- porcelain tests
# Copyright (C) 2013 Jelmer Vernooij <jelmer@samba.org>
#
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

"""Tests for dulwich.porcelain."""

from contextlib import closing
from io import BytesIO
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import os
import shutil
import tarfile
import tempfile
import time

from dulwich import porcelain
from dulwich.diff_tree import tree_changes
from dulwich.objects import (
    Blob,
    Tag,
    Tree,
    ZERO_SHA,
    )
from dulwich.repo import Repo
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    build_commit_graph,
    make_object,
    )


class PorcelainTestCase(TestCase):

    def setUp(self):
        super(PorcelainTestCase, self).setUp()
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        self.repo = Repo.init(repo_dir)

    def tearDown(self):
        super(PorcelainTestCase, self).tearDown()
        self.repo.close()


class ArchiveTests(PorcelainTestCase):
    """Tests for the archive command."""

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"refs/heads/master"] = c3.id
        out = BytesIO()
        err = BytesIO()
        porcelain.archive(self.repo.path, b"refs/heads/master", outstream=out,
            errstream=err)
        self.assertEqual(b"", err.getvalue())
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEqual([], tf.getnames())


class UpdateServerInfoTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"refs/heads/foo"] = c3.id
        porcelain.update_server_info(self.repo.path)
        self.assertTrue(os.path.exists(os.path.join(self.repo.controldir(),
            'info', 'refs')))


class CommitTests(PorcelainTestCase):

    def test_custom_author(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"refs/heads/foo"] = c3.id
        sha = porcelain.commit(self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>", committer=b"Bob <bob@example.com>")
        self.assertTrue(isinstance(sha, bytes))
        self.assertEqual(len(sha), 40)


class CloneTests(PorcelainTestCase):

    def test_simple_local(self):
        f1_1 = make_object(Blob, data=b'f1')
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {1: [(b'f1', f1_1), (b'f2', f1_1)],
                 2: [(b'f1', f1_1), (b'f2', f1_1)],
                 3: [(b'f1', f1_1), (b'f2', f1_1)], }

        c1, c2, c3 = build_commit_graph(self.repo.object_store,
                                        commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c3.id
        self.repo.refs[b"refs/tags/foo"] = c3.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path,
                            checkout=False, errstream=errstream)
        self.assertEqual(r.path, target_path)
        target_repo = Repo(target_path)
        self.assertEqual(target_repo.head(), c3.id)
        self.assertEqual(c3.id, target_repo.refs[b'refs/tags/foo'])
        self.assertTrue(b'f1' not in os.listdir(target_path))
        self.assertTrue(b'f2' not in os.listdir(target_path))

    def test_simple_local_with_checkout(self):
        f1_1 = make_object(Blob, data=b'f1')
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {1: [(b'f1', f1_1), (b'f2', f1_1)],
                 2: [(b'f1', f1_1), (b'f2', f1_1)],
                 3: [(b'f1', f1_1), (b'f2', f1_1)], }

        c1, c2, c3 = build_commit_graph(self.repo.object_store,
                                        commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        with closing(porcelain.clone(self.repo.path, target_path,
                                     checkout=True,
                                     errstream=errstream)) as r:
            self.assertEqual(r.path, target_path)
        with closing(Repo(target_path)) as r:
            self.assertEqual(r.head(), c3.id)
        self.assertTrue('f1' in os.listdir(target_path))
        self.assertTrue('f2' in os.listdir(target_path))

    def test_bare_local_with_checkout(self):
        f1_1 = make_object(Blob, data=b'f1')
        commit_spec = [[1], [2, 1], [3, 1, 2]]
        trees = {1: [(b'f1', f1_1), (b'f2', f1_1)],
                 2: [(b'f1', f1_1), (b'f2', f1_1)],
                 3: [(b'f1', f1_1), (b'f2', f1_1)], }

        c1, c2, c3 = build_commit_graph(self.repo.object_store,
                                        commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path,
                            bare=True, errstream=errstream)
        self.assertEqual(r.path, target_path)
        self.assertEqual(Repo(target_path).head(), c3.id)
        self.assertFalse(b'f1' in os.listdir(target_path))
        self.assertFalse(b'f2' in os.listdir(target_path))

    def test_no_checkout_with_bare(self):
        f1_1 = make_object(Blob, data=b'f1')
        commit_spec = [[1]]
        trees = {1: [(b'f1', f1_1), (b'f2', f1_1)]}

        (c1, ) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c1.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        self.assertRaises(ValueError, porcelain.clone, self.repo.path,
            target_path, checkout=True, bare=True, errstream=errstream)


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
        porcelain.commit(repo=self.repo.path, message=b'test',
            author=b'test', committer=b'test')

        # Add a second test file and a file in a directory
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("\n")
        os.mkdir(os.path.join(self.repo.path, 'adir'))
        with open(os.path.join(self.repo.path, 'adir', 'afile'), 'w') as f:
            f.write("\n")
        porcelain.add(self.repo.path)

        # Check that foo was added and nothing in .git was modified
        index = self.repo.open_index()
        self.assertEqual(sorted(index), [b'adir/afile', b'blah', b'foo'])

    def test_add_file(self):
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=["foo"])


class RemoveTests(PorcelainTestCase):

    def test_remove_file(self):
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=["foo"])
        porcelain.rm(self.repo.path, paths=["foo"])


class LogTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream)
        self.assertEqual(3, outstream.getvalue().count("-" * 50))

    def test_max_entries(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream, max_entries=1)
        self.assertEqual(1, outstream.getvalue().count("-" * 50))


class ShowTests(PorcelainTestCase):

    def test_nolist(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=c3.id, outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[c3.id], outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))

    def test_blob(self):
        b = Blob.from_string(b"The Foo\n")
        self.repo.object_store.add_object(b)
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[b.id], outstream=outstream)
        self.assertEqual(outstream.getvalue(), "The Foo\n")


class SymbolicRefTests(PorcelainTestCase):

    def test_set_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        self.assertRaises(ValueError, porcelain.symbolic_ref, self.repo.path, b'foobar')

    def test_set_force_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b'force_foobar', force=True)

        #test if we actually changed the file
        with self.repo.get_named_file('HEAD') as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b'ref: refs/heads/force_foobar\n')

    def test_set_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b'master')

    def test_set_symbolic_ref_other_than_master(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]], attrs=dict(refs='develop'))
        self.repo.refs[b"HEAD"] = c3.id
        self.repo.refs[b"refs/heads/develop"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b'develop')

        #test if we actually changed the file
        with self.repo.get_named_file('HEAD') as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b'ref: refs/heads/develop\n')


class DiffTreeTests(PorcelainTestCase):

    def test_empty(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.diff_tree(self.repo.path, c2.tree, c3.tree, outstream=outstream)
        self.assertEqual(outstream.getvalue(), b"")


class CommitTreeTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        b = Blob()
        b.data = b"foo the bar"
        t = Tree()
        t.add(b"somename", 0o100644, b.id)
        self.repo.object_store.add_object(t)
        self.repo.object_store.add_object(b)
        sha = porcelain.commit_tree(
            self.repo.path, t.id, message=b"Withcommit.",
            author=b"Joe <joe@example.com>",
            committer=b"Jane <jane@example.com>")
        self.assertTrue(isinstance(sha, bytes))
        self.assertEqual(len(sha), 40)


class RevListTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        outstream = BytesIO()
        porcelain.rev_list(
            self.repo.path, [c3.id], outstream=outstream)
        self.assertEqual(
            c3.id + b"\n" +
            c2.id + b"\n" +
            c1.id + b"\n",
            outstream.getvalue())


class TagCreateTests(PorcelainTestCase):

    def test_annotated(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.tag_create(self.repo.path, b"tryme", b'foo <foo@bar.com>',
                b'bar', annotated=True)

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        tag = self.repo[b'refs/tags/tryme']
        self.assertTrue(isinstance(tag, Tag))
        self.assertEqual(b"foo <foo@bar.com>", tag.tagger)
        self.assertEqual(b"bar", tag.message)
        self.assertLess(time.time() - tag.tag_time, 5)

    def test_unannotated(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.tag_create(self.repo.path, b"tryme", annotated=False)

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        self.repo[b'refs/tags/tryme']
        self.assertEqual(list(tags.values()), [self.repo.head()])


class TagListTests(PorcelainTestCase):

    def test_empty(self):
        tags = porcelain.tag_list(self.repo.path)
        self.assertEqual([], tags)

    def test_simple(self):
        self.repo.refs[b"refs/tags/foo"] = b"aa" * 20
        self.repo.refs[b"refs/tags/bar/bla"] = b"bb" * 20
        tags = porcelain.tag_list(self.repo.path)

        self.assertEqual([b"bar/bla", b"foo"], tags)


class TagDeleteTests(PorcelainTestCase):

    def test_simple(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.tag_create(self.repo, b'foo')
        self.assertTrue(b"foo" in porcelain.tag_list(self.repo))
        porcelain.tag_delete(self.repo, b'foo')
        self.assertFalse(b"foo" in porcelain.tag_list(self.repo))


class ResetTests(PorcelainTestCase):

    def test_hard_head(self):
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=["foo"])
        porcelain.commit(self.repo.path, message=b"Some message",
                committer=b"Jane <jane@example.com>",
                author=b"John <john@example.com>")

        with open(os.path.join(self.repo.path, 'foo'), 'wb') as f:
            f.write(b"OOH")

        porcelain.reset(self.repo, "hard", b"HEAD")

        index = self.repo.open_index()
        changes = list(tree_changes(self.repo,
                       index.commit(self.repo.object_store),
                       self.repo[b'HEAD'].tree))

        self.assertEqual([], changes)

    def test_hard_commit(self):
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=["foo"])
        sha = porcelain.commit(self.repo.path, message=b"Some message",
                committer=b"Jane <jane@example.com>",
                author=b"John <john@example.com>")

        with open(os.path.join(self.repo.path, 'foo'), 'wb') as f:
            f.write(b"BAZ")
        porcelain.add(self.repo.path, paths=["foo"])
        porcelain.commit(self.repo.path, message=b"Some other message",
                committer=b"Jane <jane@example.com>",
                author=b"John <john@example.com>")

        porcelain.reset(self.repo, "hard", sha)

        index = self.repo.open_index()
        changes = list(tree_changes(self.repo,
                       index.commit(self.repo.object_store),
                       self.repo[sha].tree))

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

        porcelain.commit(repo=self.repo.path, message=b'init',
            author=b'', committer=b'')

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(self.repo.path, target=clone_path,
            errstream=errstream)
        try:
            self.assertEqual(target_repo[b'HEAD'], self.repo[b'HEAD'])
        finally:
            target_repo.close()

        # create a second file to be pushed back to origin
        handle, fullpath = tempfile.mkstemp(dir=clone_path)
        os.close(handle)
        porcelain.add(repo=clone_path, paths=[os.path.basename(fullpath)])
        porcelain.commit(repo=clone_path, message=b'push',
            author=b'', committer=b'')

        # Setup a non-checked out branch in the remote
        refs_path = b"refs/heads/foo"
        new_id = self.repo[b'HEAD'].id
        self.assertNotEqual(new_id, ZERO_SHA)
        self.repo.refs[refs_path] = new_id

        # Push to the remote
        porcelain.push(clone_path, self.repo.path, b"HEAD:" + refs_path, outstream=outstream,
            errstream=errstream)

        # Check that the target and source
        with closing(Repo(clone_path)) as r_clone:
            self.assertEqual({
                b'HEAD': new_id,
                b'refs/heads/foo': r_clone[b'HEAD'].id,
                b'refs/heads/master': new_id,
                }, self.repo.get_refs())
            self.assertEqual(r_clone[b'HEAD'].id, self.repo.refs[refs_path])

            # Get the change in the target repo corresponding to the add
            # this will be in the foo branch.
            change = list(tree_changes(self.repo, self.repo[b'HEAD'].tree,
                                       self.repo[b'refs/heads/foo'].tree))[0]
            self.assertEqual(os.path.basename(fullpath),
                change.new.path.decode('ascii'))

    def test_delete(self):
        """Basic test of porcelain push, removing a branch.
        """
        outstream = BytesIO()
        errstream = BytesIO()

        porcelain.commit(repo=self.repo.path, message=b'init',
            author=b'', committer=b'')

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(self.repo.path, target=clone_path,
            errstream=errstream)
        target_repo.close()

        # Setup a non-checked out branch in the remote
        refs_path = b"refs/heads/foo"
        new_id = self.repo[b'HEAD'].id
        self.assertNotEqual(new_id, ZERO_SHA)
        self.repo.refs[refs_path] = new_id

        # Push to the remote
        porcelain.push(clone_path, self.repo.path, b":" + refs_path, outstream=outstream,
            errstream=errstream)

        self.assertEqual({
            b'HEAD': new_id,
            b'refs/heads/master': new_id,
            }, self.repo.get_refs())



class PullTests(PorcelainTestCase):

    def setUp(self):
        super(PullTests, self).setUp()
        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        filename = os.path.basename(fullpath)
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message=b'test',
                         author=b'test', committer=b'test')

        # Setup target repo
        self.target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.target_path)
        target_repo = porcelain.clone(self.repo.path, target=self.target_path,
                errstream=BytesIO())
        target_repo.close()

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        filename = os.path.basename(fullpath)
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message=b'test2',
            author=b'test2', committer=b'test2')

        self.assertTrue(b'refs/heads/master' in self.repo.refs)
        self.assertTrue(b'refs/heads/master' in target_repo.refs)

    def test_simple(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(self.target_path, self.repo.path, b'refs/heads/master',
            outstream=outstream, errstream=errstream)

        # Check the target repo for pushed changes
        with closing(Repo(self.target_path)) as r:
            self.assertEqual(r[b'HEAD'].id, self.repo[b'HEAD'].id)

    def test_no_refspec(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(self.target_path, self.repo.path, outstream=outstream,
                       errstream=errstream)

        # Check the target repo for pushed changes
        with closing(Repo(self.target_path)) as r:
            self.assertEqual(r[b'HEAD'].id, self.repo[b'HEAD'].id)


class StatusTests(PorcelainTestCase):

    def test_empty(self):
        results = porcelain.status(self.repo)
        self.assertEqual(
            {'add': [], 'delete': [], 'modify': []},
            results.staged)
        self.assertEqual([], results.unstaged)

    def test_status(self):
        """Integration test for `status` functionality."""

        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write('origstuff')

        porcelain.add(repo=self.repo.path, paths=['foo'])
        porcelain.commit(repo=self.repo.path, message=b'test status',
            author=b'', committer=b'')

        # modify access and modify time of path
        os.utime(fullpath, (0, 0))

        with open(fullpath, 'wb') as f:
            f.write(b'stuff')

        # Make a dummy file and stage it
        filename_add = 'bar'
        fullpath = os.path.join(self.repo.path, filename_add)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename_add)

        results = porcelain.status(self.repo)

        self.assertEqual(results.staged['add'][0], filename_add.encode('ascii'))
        self.assertEqual(results.unstaged, [b'foo'])

    def test_get_tree_changes_add(self):
        """Unit test for get_tree_changes add."""

        # Make a dummy file, stage
        filename = 'bar'
        with open(os.path.join(self.repo.path, filename), 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message=b'test status',
            author=b'', committer=b'')

        filename = 'foo'
        with open(os.path.join(self.repo.path, filename), 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['add'][0], filename.encode('ascii'))
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
        porcelain.commit(repo=self.repo.path, message=b'test status',
            author=b'', committer=b'')
        with open(fullpath, 'w') as f:
            f.write('otherstuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['modify'][0], filename.encode('ascii'))
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
        porcelain.commit(repo=self.repo.path, message=b'test status',
            author=b'', committer=b'')
        porcelain.rm(repo=self.repo.path, paths=[filename])
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['delete'][0], filename.encode('ascii'))
        self.assertEqual(len(changes['add']), 0)
        self.assertEqual(len(changes['modify']), 0)
        self.assertEqual(len(changes['delete']), 1)


# TODO(jelmer): Add test for dulwich.porcelain.daemon


class UploadPackTests(PorcelainTestCase):
    """Tests for upload_pack."""

    def test_upload_pack(self):
        outf = BytesIO()
        exitcode = porcelain.upload_pack(self.repo.path, BytesIO(b"0000"), outf)
        outlines = outf.getvalue().splitlines()
        self.assertEqual([b"0000"], outlines)
        self.assertEqual(0, exitcode)


class ReceivePackTests(PorcelainTestCase):
    """Tests for receive_pack."""

    def test_receive_pack(self):
        filename = 'foo'
        with open(os.path.join(self.repo.path, filename), 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=filename)
        self.repo.do_commit(message=b'test status',
            author=b'', committer=b'', author_timestamp=1402354300,
            commit_timestamp=1402354300, author_timezone=0, commit_timezone=0)
        outf = BytesIO()
        exitcode = porcelain.receive_pack(self.repo.path, BytesIO(b"0000"), outf)
        outlines = outf.getvalue().splitlines()
        self.assertEqual([
            b'00739e65bdcf4a22cdd4f3700604a275cd2aaf146b23 HEAD\x00 report-status '
            b'delete-refs quiet ofs-delta side-band-64k no-done',
            b'003f9e65bdcf4a22cdd4f3700604a275cd2aaf146b23 refs/heads/master',
            b'0000'], outlines)
        self.assertEqual(0, exitcode)


class BranchListTests(PorcelainTestCase):

    def test_standard(self):
        self.assertEqual(set([]), set(porcelain.branch_list(self.repo)))

    def test_new_branch(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b"foo")
        self.assertEqual(
            set([b"master", b"foo"]),
            set(porcelain.branch_list(self.repo)))


class BranchCreateTests(PorcelainTestCase):

    def test_branch_exists(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b"foo")
        self.assertRaises(KeyError, porcelain.branch_create, self.repo, b"foo")
        porcelain.branch_create(self.repo, b"foo", force=True)

    def test_new_branch(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b"foo")
        self.assertEqual(
            set([b"master", b"foo"]),
            set(porcelain.branch_list(self.repo)))


class BranchDeleteTests(PorcelainTestCase):

    def test_simple(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, b'foo')
        self.assertTrue(b"foo" in porcelain.branch_list(self.repo))
        porcelain.branch_delete(self.repo, b'foo')
        self.assertFalse(b"foo" in porcelain.branch_list(self.repo))


class FetchTests(PorcelainTestCase):

    def test_simple(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        filename = os.path.basename(fullpath)
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message=b'test',
                         author=b'test', committer=b'test')

        # Setup target repo
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target_repo = porcelain.clone(self.repo.path, target=target_path,
            errstream=errstream)

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        filename = os.path.basename(fullpath)
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.commit(repo=self.repo.path, message=b'test2',
            author=b'test2', committer=b'test2')

        self.assertFalse(self.repo[b'HEAD'].id in target_repo)
        target_repo.close()

        # Fetch changes into the cloned repo
        porcelain.fetch(target_path, self.repo.path, outstream=outstream,
            errstream=errstream)

        # Check the target repo for pushed changes
        with closing(Repo(target_path)) as r:
            self.assertTrue(self.repo[b'HEAD'].id in r)


class RepackTests(PorcelainTestCase):

    def test_empty(self):
        porcelain.repack(self.repo)

    def test_simple(self):
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        filename = os.path.basename(fullpath)
        porcelain.add(repo=self.repo.path, paths=filename)
        porcelain.repack(self.repo)


class LsTreeTests(PorcelainTestCase):

    def test_empty(self):
        porcelain.commit(repo=self.repo.path, message=b'test status',
            author=b'', committer=b'')

        f = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=f)
        self.assertEqual(f.getvalue(), "")

    def test_simple(self):
        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write('origstuff')

        porcelain.add(repo=self.repo.path, paths=['foo'])
        porcelain.commit(repo=self.repo.path, message=b'test status',
            author=b'', committer=b'')

        f = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=f)
        self.assertEqual(
                f.getvalue(),
                '100644 blob 8b82634d7eae019850bb883f06abf428c58bc9aa\tfoo\n')
