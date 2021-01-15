# test_porcelain.py -- porcelain tests
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

from io import BytesIO, StringIO
import os
import re
import shutil
import stat
import tarfile
import tempfile
import time

from dulwich import porcelain
from dulwich.diff_tree import tree_changes
from dulwich.errors import CommitError
from dulwich.objects import (
    Blob,
    Tag,
    Tree,
    ZERO_SHA,
    )
from dulwich.repo import (
    NoIndexPresent,
    Repo,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    build_commit_graph,
    make_commit,
    make_object,
    )


def flat_walk_dir(dir_to_walk):
    for dirpath, _, filenames in os.walk(dir_to_walk):
        rel_dirpath = os.path.relpath(dirpath, dir_to_walk)
        if not dirpath == dir_to_walk:
            yield rel_dirpath
        for filename in filenames:
            if dirpath == dir_to_walk:
                yield filename
            else:
                yield os.path.join(rel_dirpath, filename)


class PorcelainTestCase(TestCase):

    def setUp(self):
        super(PorcelainTestCase, self).setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, 'repo')
        self.repo = Repo.init(self.repo_path, mkdir=True)
        self.addCleanup(self.repo.close)


class ArchiveTests(PorcelainTestCase):
    """Tests for the archive command."""

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
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
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"refs/heads/foo"] = c3.id
        porcelain.update_server_info(self.repo.path)
        self.assertTrue(os.path.exists(
                os.path.join(self.repo.controldir(), 'info', 'refs')))


class CommitTests(PorcelainTestCase):

    def test_custom_author(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"refs/heads/foo"] = c3.id
        sha = porcelain.commit(
                self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>",
                committer=b"Bob <bob@example.com>")
        self.assertTrue(isinstance(sha, bytes))
        self.assertEqual(len(sha), 40)

    def test_unicode(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"refs/heads/foo"] = c3.id
        sha = porcelain.commit(
                self.repo.path, message="Some message",
                author="Joe <joe@example.com>",
                committer="Bob <bob@example.com>")
        self.assertTrue(isinstance(sha, bytes))
        self.assertEqual(len(sha), 40)

    def test_no_verify(self):
        if os.name != 'posix':
            self.skipTest('shell hook tests requires POSIX shell')
        self.assertTrue(os.path.exists('/bin/sh'))

        hooks_dir = os.path.join(self.repo.controldir(), "hooks")
        os.makedirs(hooks_dir, exist_ok=True)
        self.addCleanup(shutil.rmtree, hooks_dir)

        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])

        hook_fail = "#!/bin/sh\nexit 1"

        # hooks are executed in pre-commit, commit-msg order
        # test commit-msg failure first, then pre-commit failure, then
        # no_verify to skip both hooks
        commit_msg = os.path.join(hooks_dir, "commit-msg")
        with open(commit_msg, 'w') as f:
            f.write(hook_fail)
        os.chmod(commit_msg, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        with self.assertRaises(CommitError):
            porcelain.commit(
                    self.repo.path, message="Some message",
                    author="Joe <joe@example.com>",
                    committer="Bob <bob@example.com>")

        pre_commit = os.path.join(hooks_dir, "pre-commit")
        with open(pre_commit, 'w') as f:
            f.write(hook_fail)
        os.chmod(pre_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        with self.assertRaises(CommitError):
            porcelain.commit(
                    self.repo.path, message="Some message",
                    author="Joe <joe@example.com>",
                    committer="Bob <bob@example.com>")

        sha = porcelain.commit(
                self.repo.path, message="Some message",
                author="Joe <joe@example.com>",
                committer="Bob <bob@example.com>", no_verify=True)
        self.assertTrue(isinstance(sha, bytes))
        self.assertEqual(len(sha), 40)


class CleanTests(PorcelainTestCase):

    def put_files(self, tracked, ignored, untracked, empty_dirs):
        """Put the described files in the wd
        """
        all_files = tracked | ignored | untracked
        for file_path in all_files:
            abs_path = os.path.join(self.repo.path, file_path)
            # File may need to be written in a dir that doesn't exist yet, so
            # create the parent dir(s) as necessary
            parent_dir = os.path.dirname(abs_path)
            try:
                os.makedirs(parent_dir)
            except FileExistsError:
                pass
            with open(abs_path, 'w') as f:
                f.write('')

        with open(os.path.join(self.repo.path, '.gitignore'), 'w') as f:
            f.writelines(ignored)

        for dir_path in empty_dirs:
            os.mkdir(os.path.join(self.repo.path, 'empty_dir'))

        files_to_add = [os.path.join(self.repo.path, t) for t in tracked]
        porcelain.add(repo=self.repo.path, paths=files_to_add)
        porcelain.commit(repo=self.repo.path, message="init commit")

    def assert_wd(self, expected_paths):
        """Assert paths of files and dirs in wd are same as expected_paths
        """
        control_dir_rel = os.path.relpath(
            self.repo._controldir, self.repo.path)

        # normalize paths to simplify comparison across platforms
        found_paths = {
            os.path.normpath(p)
            for p in flat_walk_dir(self.repo.path)
            if not p.split(os.sep)[0] == control_dir_rel}
        norm_expected_paths = {os.path.normpath(p) for p in expected_paths}
        self.assertEqual(found_paths, norm_expected_paths)

    def test_from_root(self):
        self.put_files(
            tracked={
                'tracked_file',
                'tracked_dir/tracked_file',
                '.gitignore'},
            ignored={
                'ignored_file'},
            untracked={
                'untracked_file',
                'tracked_dir/untracked_dir/untracked_file',
                'untracked_dir/untracked_dir/untracked_file'},
            empty_dirs={
                'empty_dir'})

        porcelain.clean(repo=self.repo.path, target_dir=self.repo.path)

        self.assert_wd({
            'tracked_file',
            'tracked_dir/tracked_file',
            '.gitignore',
            'ignored_file',
            'tracked_dir'})

    def test_from_subdir(self):
        self.put_files(
            tracked={
                'tracked_file',
                'tracked_dir/tracked_file',
                '.gitignore'},
            ignored={
                'ignored_file'},
            untracked={
                'untracked_file',
                'tracked_dir/untracked_dir/untracked_file',
                'untracked_dir/untracked_dir/untracked_file'},
            empty_dirs={
                'empty_dir'})

        porcelain.clean(
            repo=self.repo,
            target_dir=os.path.join(self.repo.path, 'untracked_dir'))

        self.assert_wd({
            'tracked_file',
            'tracked_dir/tracked_file',
            '.gitignore',
            'ignored_file',
            'untracked_file',
            'tracked_dir/untracked_dir/untracked_file',
            'empty_dir',
            'untracked_dir',
            'tracked_dir',
            'tracked_dir/untracked_dir'})


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
        self.addCleanup(r.close)
        self.assertEqual(r.path, target_path)
        target_repo = Repo(target_path)
        self.assertEqual(0, len(target_repo.open_index()))
        self.assertEqual(c3.id, target_repo.refs[b'refs/tags/foo'])
        self.assertTrue(b'f1' not in os.listdir(target_path))
        self.assertTrue(b'f2' not in os.listdir(target_path))
        c = r.get_config()
        encoded_path = self.repo.path
        if not isinstance(encoded_path, bytes):
            encoded_path = encoded_path.encode('utf-8')
        self.assertEqual(encoded_path, c.get((b'remote', b'origin'), b'url'))
        self.assertEqual(
            b'+refs/heads/*:refs/remotes/origin/*',
            c.get((b'remote', b'origin'), b'fetch'))

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
        with porcelain.clone(self.repo.path, target_path,
                             checkout=True,
                             errstream=errstream) as r:
            self.assertEqual(r.path, target_path)
        with Repo(target_path) as r:
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
        with porcelain.clone(
                self.repo.path, target_path, bare=True,
                errstream=errstream) as r:
            self.assertEqual(r.path, target_path)
        with Repo(target_path) as r:
            r.head()
            self.assertRaises(NoIndexPresent, r.open_index)
        self.assertFalse(b'f1' in os.listdir(target_path))
        self.assertFalse(b'f2' in os.listdir(target_path))

    def test_no_checkout_with_bare(self):
        f1_1 = make_object(Blob, data=b'f1')
        commit_spec = [[1]]
        trees = {1: [(b'f1', f1_1), (b'f2', f1_1)]}

        (c1, ) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c1.id
        self.repo.refs[b"HEAD"] = c1.id
        target_path = tempfile.mkdtemp()
        errstream = BytesIO()
        self.addCleanup(shutil.rmtree, target_path)
        self.assertRaises(
            porcelain.Error, porcelain.clone, self.repo.path,
            target_path, checkout=True, bare=True, errstream=errstream)

    def test_no_head_no_checkout(self):
        f1_1 = make_object(Blob, data=b'f1')
        commit_spec = [[1]]
        trees = {1: [(b'f1', f1_1), (b'f2', f1_1)]}

        (c1, ) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c1.id
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        errstream = BytesIO()
        r = porcelain.clone(
            self.repo.path, target_path, checkout=True, errstream=errstream)
        r.close()

    def test_no_head_no_checkout_outstream_errstream_autofallback(self):
        f1_1 = make_object(Blob, data=b'f1')
        commit_spec = [[1]]
        trees = {1: [(b'f1', f1_1), (b'f2', f1_1)]}

        (c1, ) = build_commit_graph(self.repo.object_store, commit_spec, trees)
        self.repo.refs[b"refs/heads/master"] = c1.id
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        errstream = porcelain.NoneStream()
        r = porcelain.clone(
            self.repo.path, target_path, checkout=True, errstream=errstream)
        r.close()

    def test_source_broken(self):
        target_path = tempfile.mkdtemp()
        self.assertRaises(
            Exception, porcelain.clone, '/nonexistant/repo', target_path)
        self.assertFalse(os.path.exists(target_path))


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
        fullpath = os.path.join(self.repo.path, 'blah')
        with open(fullpath, 'w') as f:
            f.write("\n")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(repo=self.repo.path, message=b'test',
                         author=b'test <email>', committer=b'test <email>')

        # Add a second test file and a file in a directory
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("\n")
        os.mkdir(os.path.join(self.repo.path, 'adir'))
        with open(os.path.join(self.repo.path, 'adir', 'afile'), 'w') as f:
            f.write("\n")
        cwd = os.getcwd()
        try:
            os.chdir(self.repo.path)
            self.assertEqual(
                set(['foo', 'blah', 'adir', '.git']),
                set(os.listdir('.')))
            self.assertEqual(
                (['foo', os.path.join('adir', 'afile')], set()),
                porcelain.add(self.repo.path))
        finally:
            os.chdir(cwd)

        # Check that foo was added and nothing in .git was modified
        index = self.repo.open_index()
        self.assertEqual(sorted(index), [b'adir/afile', b'blah', b'foo'])

    def test_add_default_paths_subdir(self):
        os.mkdir(os.path.join(self.repo.path, 'foo'))
        with open(os.path.join(self.repo.path, 'blah'), 'w') as f:
            f.write("\n")
        with open(os.path.join(self.repo.path, 'foo', 'blie'), 'w') as f:
            f.write("\n")

        cwd = os.getcwd()
        try:
            os.chdir(os.path.join(self.repo.path, 'foo'))
            porcelain.add(repo=self.repo.path)
            porcelain.commit(repo=self.repo.path, message=b'test',
                             author=b'test <email>',
                             committer=b'test <email>')
        finally:
            os.chdir(cwd)

        index = self.repo.open_index()
        self.assertEqual(sorted(index), [b'foo/blie'])

    def test_add_file(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        self.assertIn(b"foo", self.repo.open_index())

    def test_add_ignored(self):
        with open(os.path.join(self.repo.path, '.gitignore'), 'w') as f:
            f.write("foo")
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("BAR")
        with open(os.path.join(self.repo.path, 'bar'), 'w') as f:
            f.write("BAR")
        (added, ignored) = porcelain.add(self.repo.path, paths=[
            os.path.join(self.repo.path, "foo"),
            os.path.join(self.repo.path, "bar")])
        self.assertIn(b"bar", self.repo.open_index())
        self.assertEqual(set(['bar']), set(added))
        self.assertEqual(set(['foo']), ignored)

    def test_add_file_absolute_path(self):
        # Absolute paths are (not yet) supported
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo, paths=[os.path.join(self.repo.path, "foo")])
        self.assertIn(b"foo", self.repo.open_index())

    def test_add_not_in_repo(self):
        with open(os.path.join(self.test_dir, 'foo'), 'w') as f:
            f.write("BAR")
        self.assertRaises(
            ValueError,
            porcelain.add, self.repo,
            paths=[os.path.join(self.test_dir, "foo")])
        self.assertRaises(
            (ValueError, FileNotFoundError),
            porcelain.add, self.repo,
            paths=["../foo"])
        self.assertEqual([], list(self.repo.open_index()))

    def test_add_file_clrf_conversion(self):
        # Set the right configuration to the repo
        c = self.repo.get_config()
        c.set("core", "autocrlf", "input")
        c.write_to_path()

        # Add a file with CRLF line-ending
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'wb') as f:
            f.write(b"line1\r\nline2")
        porcelain.add(self.repo.path, paths=[fullpath])

        # The line-endings should have been converted to LF
        index = self.repo.open_index()
        self.assertIn(b"foo", index)

        entry = index[b"foo"]
        blob = self.repo[entry.sha]
        self.assertEqual(blob.data, b"line1\nline2")


class RemoveTests(PorcelainTestCase):

    def test_remove_file(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        porcelain.commit(repo=self.repo, message=b'test',
                         author=b'test <email>',
                         committer=b'test <email>')
        self.assertTrue(os.path.exists(os.path.join(self.repo.path, 'foo')))
        cwd = os.getcwd()
        try:
            os.chdir(self.repo.path)
            porcelain.remove(self.repo.path, paths=["foo"])
        finally:
            os.chdir(cwd)
        self.assertFalse(os.path.exists(os.path.join(self.repo.path, 'foo')))

    def test_remove_file_staged(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        cwd = os.getcwd()
        try:
            os.chdir(self.repo.path)
            porcelain.add(self.repo.path, paths=[fullpath])
            self.assertRaises(Exception, porcelain.rm, self.repo.path,
                              paths=["foo"])
        finally:
            os.chdir(cwd)

    def test_remove_file_removed_on_disk(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        cwd = os.getcwd()
        try:
            os.chdir(self.repo.path)
            os.remove(fullpath)
            porcelain.remove(self.repo.path, paths=["foo"])
        finally:
            os.chdir(cwd)
        self.assertFalse(os.path.exists(os.path.join(self.repo.path, 'foo')))


class LogTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream)
        self.assertEqual(3, outstream.getvalue().count("-" * 50))

    def test_max_entries(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream, max_entries=1)
        self.assertEqual(1, outstream.getvalue().count("-" * 50))


class ShowTests(PorcelainTestCase):

    def test_nolist(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=c3.id, outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
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

    def test_commit_no_parent(self):
        a = Blob.from_string(b"The Foo\n")
        ta = Tree()
        ta.add(b"somename", 0o100644, a.id)
        ca = make_commit(tree=ta.id)
        self.repo.object_store.add_objects([(a, None), (ta, None), (ca, None)])
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[ca.id], outstream=outstream)
        self.assertMultiLineEqual(outstream.getvalue(), """\
--------------------------------------------------
commit: 344da06c1bb85901270b3e8875c988a027ec087d
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Test message.

diff --git a/somename b/somename
new file mode 100644
index 0000000..ea5c7bf
--- /dev/null
+++ b/somename
@@ -0,0 +1 @@
+The Foo
""")

    def test_tag(self):
        a = Blob.from_string(b"The Foo\n")
        ta = Tree()
        ta.add(b"somename", 0o100644, a.id)
        ca = make_commit(tree=ta.id)
        self.repo.object_store.add_objects([(a, None), (ta, None), (ca, None)])
        porcelain.tag_create(
            self.repo.path, b"tryme", b'foo <foo@bar.com>', b'bar',
            annotated=True, objectish=ca.id, tag_time=1552854211,
            tag_timezone=0)
        outstream = StringIO()
        porcelain.show(self.repo, objects=[b'refs/tags/tryme'],
                       outstream=outstream)
        self.maxDiff = None
        self.assertMultiLineEqual(outstream.getvalue(), """\
Tagger: foo <foo@bar.com>
Date:   Sun Mar 17 2019 20:23:31 +0000

bar

--------------------------------------------------
commit: 344da06c1bb85901270b3e8875c988a027ec087d
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Test message.

diff --git a/somename b/somename
new file mode 100644
index 0000000..ea5c7bf
--- /dev/null
+++ b/somename
@@ -0,0 +1 @@
+The Foo
""")

    def test_commit_with_change(self):
        a = Blob.from_string(b"The Foo\n")
        ta = Tree()
        ta.add(b"somename", 0o100644, a.id)
        ca = make_commit(tree=ta.id)
        b = Blob.from_string(b"The Bar\n")
        tb = Tree()
        tb.add(b"somename", 0o100644, b.id)
        cb = make_commit(tree=tb.id, parents=[ca.id])
        self.repo.object_store.add_objects(
            [(a, None), (b, None), (ta, None), (tb, None),
             (ca, None), (cb, None)])
        outstream = StringIO()
        porcelain.show(self.repo.path, objects=[cb.id], outstream=outstream)
        self.assertMultiLineEqual(outstream.getvalue(), """\
--------------------------------------------------
commit: 2c6b6c9cb72c130956657e1fdae58e5b103744fa
Author: Test Author <test@nodomain.com>
Committer: Test Committer <test@nodomain.com>
Date:   Fri Jan 01 2010 00:00:00 +0000

Test message.

diff --git a/somename b/somename
index ea5c7bf..fd38bcb 100644
--- a/somename
+++ b/somename
@@ -1 +1 @@
-The Foo
+The Bar
""")


class SymbolicRefTests(PorcelainTestCase):

    def test_set_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        self.assertRaises(
            porcelain.Error, porcelain.symbolic_ref, self.repo.path,
            b'foobar')

    def test_set_force_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b'force_foobar', force=True)

        # test if we actually changed the file
        with self.repo.get_named_file('HEAD') as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b'ref: refs/heads/force_foobar\n')

    def test_set_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b'master')

    def test_set_symbolic_ref_other_than_master(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]],
                attrs=dict(refs='develop'))
        self.repo.refs[b"HEAD"] = c3.id
        self.repo.refs[b"refs/heads/develop"] = c3.id

        porcelain.symbolic_ref(self.repo.path, b'develop')

        # test if we actually changed the file
        with self.repo.get_named_file('HEAD') as f:
            new_ref = f.read()
        self.assertEqual(new_ref, b'ref: refs/heads/develop\n')


class DiffTreeTests(PorcelainTestCase):

    def test_empty(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id
        outstream = BytesIO()
        porcelain.diff_tree(self.repo.path, c2.tree, c3.tree,
                            outstream=outstream)
        self.assertEqual(outstream.getvalue(), b"")


class CommitTreeTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
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
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
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
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
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
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.tag_create(self.repo.path, b"tryme", annotated=False)

        tags = self.repo.refs.as_dict(b"refs/tags")
        self.assertEqual(list(tags.keys()), [b"tryme"])
        self.repo[b'refs/tags/tryme']
        self.assertEqual(list(tags.values()), [self.repo.head()])

    def test_unannotated_unicode(self):
        c1, c2, c3 = build_commit_graph(
                self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs[b"HEAD"] = c3.id

        porcelain.tag_create(self.repo.path, "tryme", annotated=False)

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
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
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
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(self.repo.path, paths=[fullpath])
        sha = porcelain.commit(self.repo.path, message=b"Some message",
                               committer=b"Jane <jane@example.com>",
                               author=b"John <john@example.com>")

        with open(fullpath, 'wb') as f:
            f.write(b"BAZ")
        porcelain.add(self.repo.path, paths=[fullpath])
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
                         author=b'author <email>',
                         committer=b'committer <email>')

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
        porcelain.add(repo=clone_path, paths=[fullpath])
        porcelain.commit(repo=clone_path, message=b'push',
                         author=b'author <email>',
                         committer=b'committer <email>')

        # Setup a non-checked out branch in the remote
        refs_path = b"refs/heads/foo"
        new_id = self.repo[b'HEAD'].id
        self.assertNotEqual(new_id, ZERO_SHA)
        self.repo.refs[refs_path] = new_id

        # Push to the remote
        porcelain.push(clone_path, 'origin', b"HEAD:" + refs_path,
                       outstream=outstream, errstream=errstream)

        self.assertEqual(
            target_repo.refs[b'refs/remotes/origin/foo'],
            target_repo.refs[b'HEAD'])

        # Check that the target and source
        with Repo(clone_path) as r_clone:
            self.assertEqual({
                b'HEAD': new_id,
                b'refs/heads/foo': r_clone[b'HEAD'].id,
                b'refs/heads/master': new_id,
                }, self.repo.get_refs())
            self.assertEqual(r_clone[b'HEAD'].id, self.repo[refs_path].id)

            # Get the change in the target repo corresponding to the add
            # this will be in the foo branch.
            change = list(tree_changes(self.repo, self.repo[b'HEAD'].tree,
                                       self.repo[b'refs/heads/foo'].tree))[0]
            self.assertEqual(os.path.basename(fullpath),
                             change.new.path.decode('ascii'))

    def test_local_missing(self):
        """Pushing a new branch."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.init(clone_path)
        target_repo.close()

        self.assertRaises(
            porcelain.Error,
            porcelain.push, self.repo, clone_path,
            b"HEAD:refs/heads/master",
            outstream=outstream, errstream=errstream)

    def test_new(self):
        """Pushing a new branch."""
        outstream = BytesIO()
        errstream = BytesIO()

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.init(clone_path)
        target_repo.close()

        # create a second file to be pushed back to origin
        handle, fullpath = tempfile.mkstemp(dir=clone_path)
        os.close(handle)
        porcelain.add(repo=clone_path, paths=[fullpath])
        new_id = porcelain.commit(
            repo=self.repo, message=b'push',
            author=b'author <email>',
            committer=b'committer <email>')

        # Push to the remote
        porcelain.push(self.repo, clone_path, b"HEAD:refs/heads/master",
                       outstream=outstream, errstream=errstream)

        with Repo(clone_path) as r_clone:
            self.assertEqual({
                b'HEAD': new_id,
                b'refs/heads/master': new_id,
                }, r_clone.get_refs())

    def test_delete(self):
        """Basic test of porcelain push, removing a branch.
        """
        outstream = BytesIO()
        errstream = BytesIO()

        porcelain.commit(repo=self.repo.path, message=b'init',
                         author=b'author <email>',
                         committer=b'committer <email>')

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
        porcelain.push(clone_path, self.repo.path, b":" + refs_path,
                       outstream=outstream, errstream=errstream)

        self.assertEqual({
            b'HEAD': new_id,
            b'refs/heads/master': new_id,
            }, self.repo.get_refs())

    def test_diverged(self):
        outstream = BytesIO()
        errstream = BytesIO()

        porcelain.commit(repo=self.repo.path, message=b'init',
                         author=b'author <email>',
                         committer=b'committer <email>')

        # Setup target repo cloned from temp test repo
        clone_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, clone_path)
        target_repo = porcelain.clone(self.repo.path, target=clone_path,
                                      errstream=errstream)
        target_repo.close()

        remote_id = porcelain.commit(
            repo=self.repo.path, message=b'remote change',
            author=b'author <email>',
            committer=b'committer <email>')

        local_id = porcelain.commit(
            repo=clone_path, message=b'local change',
            author=b'author <email>',
            committer=b'committer <email>')

        outstream = BytesIO()
        errstream = BytesIO()

        # Push to the remote
        self.assertRaises(
            porcelain.DivergedBranches,
            porcelain.push, clone_path, self.repo.path, b'refs/heads/master',
            outstream=outstream, errstream=errstream)

        self.assertEqual({
            b'HEAD': remote_id,
            b'refs/heads/master': remote_id,
            }, self.repo.get_refs())

        self.assertEqual(b'', outstream.getvalue())
        self.assertEqual(b'', errstream.getvalue())

        outstream = BytesIO()
        errstream = BytesIO()

        # Push to the remote with --force
        porcelain.push(
            clone_path, self.repo.path, b'refs/heads/master',
            outstream=outstream, errstream=errstream, force=True)

        self.assertEqual({
            b'HEAD': local_id,
            b'refs/heads/master': local_id,
            }, self.repo.get_refs())

        self.assertEqual(b'', outstream.getvalue())
        self.assertTrue(
            re.match(b'Push to .* successful.\n', errstream.getvalue()))


class PullTests(PorcelainTestCase):

    def setUp(self):
        super(PullTests, self).setUp()
        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test',
                         author=b'test <email>',
                         committer=b'test <email>')

        # Setup target repo
        self.target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.target_path)
        target_repo = porcelain.clone(self.repo.path, target=self.target_path,
                                      errstream=BytesIO())
        target_repo.close()

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test2',
                         author=b'test2 <email>',
                         committer=b'test2 <email>')

        self.assertIn(b'refs/heads/master', self.repo.refs)
        self.assertIn(b'refs/heads/master', target_repo.refs)

    def test_simple(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(self.target_path, self.repo.path, b'refs/heads/master',
                       outstream=outstream, errstream=errstream)

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b'HEAD'].id, self.repo[b'HEAD'].id)

    def test_diverged(self):
        outstream = BytesIO()
        errstream = BytesIO()

        c3a = porcelain.commit(
            repo=self.target_path, message=b'test3a',
            author=b'test2 <email>',
            committer=b'test2 <email>')

        porcelain.commit(
            repo=self.repo.path, message=b'test3b',
            author=b'test2 <email>',
            committer=b'test2 <email>')

        # Pull changes into the cloned repo
        self.assertRaises(
            porcelain.DivergedBranches, porcelain.pull, self.target_path,
            self.repo.path, b'refs/heads/master', outstream=outstream,
            errstream=errstream)

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b'refs/heads/master'].id, c3a)

        self.assertRaises(
            NotImplementedError, porcelain.pull,
            self.target_path, self.repo.path,
            b'refs/heads/master', outstream=outstream, errstream=errstream,
            fast_forward=False)

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b'refs/heads/master'].id, c3a)

    def test_no_refspec(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(self.target_path, self.repo.path, outstream=outstream,
                       errstream=errstream)

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b'HEAD'].id, self.repo[b'HEAD'].id)

    def test_no_remote_location(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # Pull changes into the cloned repo
        porcelain.pull(self.target_path, refspecs=b'refs/heads/master',
                       outstream=outstream, errstream=errstream)

        # Check the target repo for pushed changes
        with Repo(self.target_path) as r:
            self.assertEqual(r[b'HEAD'].id, self.repo[b'HEAD'].id)


class StatusTests(PorcelainTestCase):

    def test_empty(self):
        results = porcelain.status(self.repo)
        self.assertEqual(
            {'add': [], 'delete': [], 'modify': []},
            results.staged)
        self.assertEqual([], results.unstaged)

    def test_status_base(self):
        """Integration test for `status` functionality."""

        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write('origstuff')

        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')

        # modify access and modify time of path
        os.utime(fullpath, (0, 0))

        with open(fullpath, 'wb') as f:
            f.write(b'stuff')

        # Make a dummy file and stage it
        filename_add = 'bar'
        fullpath = os.path.join(self.repo.path, filename_add)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=fullpath)

        results = porcelain.status(self.repo)

        self.assertEqual(results.staged['add'][0],
                         filename_add.encode('ascii'))
        self.assertEqual(results.unstaged, [b'foo'])

    def test_status_all(self):
        del_path = os.path.join(self.repo.path, 'foo')
        mod_path = os.path.join(self.repo.path, 'bar')
        add_path = os.path.join(self.repo.path, 'baz')
        us_path = os.path.join(self.repo.path, 'blye')
        ut_path = os.path.join(self.repo.path, 'blyat')
        with open(del_path, 'w') as f:
            f.write('origstuff')
        with open(mod_path, 'w') as f:
            f.write('origstuff')
        with open(us_path, 'w') as f:
            f.write('origstuff')
        porcelain.add(repo=self.repo.path, paths=[del_path, mod_path, us_path])
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')
        porcelain.remove(self.repo.path, [del_path])
        with open(add_path, 'w') as f:
            f.write('origstuff')
        with open(mod_path, 'w') as f:
            f.write('more_origstuff')
        with open(us_path, 'w') as f:
            f.write('more_origstuff')
        porcelain.add(repo=self.repo.path, paths=[add_path, mod_path])
        with open(us_path, 'w') as f:
            f.write('\norigstuff')
        with open(ut_path, 'w') as f:
            f.write('origstuff')
        results = porcelain.status(self.repo.path)
        self.assertDictEqual(
            {'add': [b'baz'], 'delete': [b'foo'], 'modify': [b'bar']},
            results.staged)
        self.assertListEqual(results.unstaged, [b'blye'])
        self.assertListEqual(results.untracked, ['blyat'])

    def test_status_crlf_mismatch(self):
        # First make a commit as if the file has been added on a Linux system
        # or with core.autocrlf=True
        file_path = os.path.join(self.repo.path, 'crlf')
        with open(file_path, 'wb') as f:
            f.write(b'line1\nline2')
        porcelain.add(repo=self.repo.path, paths=[file_path])
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')

        # Then update the file as if it was created by CGit on a Windows
        # system with core.autocrlf=true
        with open(file_path, 'wb') as f:
            f.write(b'line1\r\nline2')

        results = porcelain.status(self.repo)
        self.assertDictEqual(
            {'add': [], 'delete': [], 'modify': []},
            results.staged)
        self.assertListEqual(results.unstaged, [b'crlf'])
        self.assertListEqual(results.untracked, [])

    def test_status_crlf_convert(self):
        # First make a commit as if the file has been added on a Linux system
        # or with core.autocrlf=True
        file_path = os.path.join(self.repo.path, 'crlf')
        with open(file_path, 'wb') as f:
            f.write(b'line1\nline2')
        porcelain.add(repo=self.repo.path, paths=[file_path])
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')

        # Then update the file as if it was created by CGit on a Windows
        # system with core.autocrlf=true
        with open(file_path, 'wb') as f:
            f.write(b'line1\r\nline2')

        # TODO: It should be set automatically by looking at the configuration
        c = self.repo.get_config()
        c.set("core", "autocrlf", True)
        c.write_to_path()

        results = porcelain.status(self.repo)
        self.assertDictEqual(
            {'add': [], 'delete': [], 'modify': []},
            results.staged)
        self.assertListEqual(results.unstaged, [])
        self.assertListEqual(results.untracked, [])

    def test_get_tree_changes_add(self):
        """Unit test for get_tree_changes add."""

        # Make a dummy file, stage
        filename = 'bar'
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')

        filename = 'foo'
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=fullpath)
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
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')
        with open(fullpath, 'w') as f:
            f.write('otherstuff')
        porcelain.add(repo=self.repo.path, paths=fullpath)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['modify'][0], filename.encode('ascii'))
        self.assertEqual(len(changes['add']), 0)
        self.assertEqual(len(changes['modify']), 1)
        self.assertEqual(len(changes['delete']), 0)

    def test_get_tree_changes_delete(self):
        """Unit test for get_tree_changes delete."""

        # Make a dummy file, stage, commit, remove
        filename = 'foo'
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')
        cwd = os.getcwd()
        try:
            os.chdir(self.repo.path)
            porcelain.remove(repo=self.repo.path, paths=[filename])
        finally:
            os.chdir(cwd)
        changes = porcelain.get_tree_changes(self.repo.path)

        self.assertEqual(changes['delete'][0], filename.encode('ascii'))
        self.assertEqual(len(changes['add']), 0)
        self.assertEqual(len(changes['modify']), 0)
        self.assertEqual(len(changes['delete']), 1)

    def test_get_untracked_paths(self):
        with open(os.path.join(self.repo.path, '.gitignore'), 'w') as f:
            f.write('ignored\n')
        with open(os.path.join(self.repo.path, 'ignored'), 'w') as f:
            f.write('blah\n')
        with open(os.path.join(self.repo.path, 'notignored'), 'w') as f:
            f.write('blah\n')
        self.assertEqual(
            set(['ignored', 'notignored', '.gitignore']),
            set(porcelain.get_untracked_paths(self.repo.path, self.repo.path,
                                              self.repo.open_index())))
        self.assertEqual(set(['.gitignore', 'notignored']),
                         set(porcelain.status(self.repo).untracked))
        self.assertEqual(set(['.gitignore', 'notignored', 'ignored']),
                         set(porcelain.status(self.repo, ignored=True)
                             .untracked))

    def test_get_untracked_paths_nested(self):
        with open(os.path.join(self.repo.path, '.gitignore'), 'w') as f:
            f.write('nested/\n')
        with open(os.path.join(self.repo.path, 'notignored'), 'w') as f:
            f.write('blah\n')

        subrepo = Repo.init(os.path.join(self.repo.path, 'nested'), mkdir=True)
        with open(os.path.join(subrepo.path, 'ignored'), 'w') as f:
            f.write('bleep\n')
        with open(os.path.join(subrepo.path, 'with'), 'w') as f:
            f.write('bloop\n')
        with open(os.path.join(subrepo.path, 'manager'), 'w') as f:
            f.write('blop\n')

        self.assertEqual(
            set(['.gitignore', 'notignored']),
            set(porcelain.get_untracked_paths(self.repo.path, self.repo.path,
                                              self.repo.open_index())))
        self.assertEqual(
            set(['ignored', 'with', 'manager']),
            set(porcelain.get_untracked_paths(subrepo.path, subrepo.path,
                                              subrepo.open_index())))
        self.assertEqual(
            set([os.path.join('nested', 'ignored'),
                os.path.join('nested', 'with'),
                os.path.join('nested', 'manager')]),
            set(porcelain.get_untracked_paths(self.repo.path, subrepo.path,
                                              self.repo.open_index(),
                                              exclude_ignored=False)))
        self.assertEqual(
            set([]),
            set(porcelain.get_untracked_paths(self.repo.path, subrepo.path,
                                              self.repo.open_index(),
                                              exclude_ignored=True)))


# TODO(jelmer): Add test for dulwich.porcelain.daemon


class UploadPackTests(PorcelainTestCase):
    """Tests for upload_pack."""

    def test_upload_pack(self):
        outf = BytesIO()
        exitcode = porcelain.upload_pack(
                self.repo.path, BytesIO(b"0000"), outf)
        outlines = outf.getvalue().splitlines()
        self.assertEqual([b"0000"], outlines)
        self.assertEqual(0, exitcode)


class ReceivePackTests(PorcelainTestCase):
    """Tests for receive_pack."""

    def test_receive_pack(self):
        filename = 'foo'
        fullpath = os.path.join(self.repo.path, filename)
        with open(fullpath, 'w') as f:
            f.write('stuff')
        porcelain.add(repo=self.repo.path, paths=fullpath)
        self.repo.do_commit(message=b'test status',
                            author=b'author <email>',
                            committer=b'committer <email>',
                            author_timestamp=1402354300,
                            commit_timestamp=1402354300, author_timezone=0,
                            commit_timezone=0)
        outf = BytesIO()
        exitcode = porcelain.receive_pack(
                self.repo.path, BytesIO(b"0000"), outf)
        outlines = outf.getvalue().splitlines()
        self.assertEqual([
            b'0091319b56ce3aee2d489f759736a79cc552c9bb86d9 HEAD\x00 report-status '  # noqa: E501
            b'delete-refs quiet ofs-delta side-band-64k '
            b'no-done symref=HEAD:refs/heads/master',
            b'003f319b56ce3aee2d489f759736a79cc552c9bb86d9 refs/heads/master',
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
        self.assertRaises(
            porcelain.Error, porcelain.branch_create,
            self.repo, b"foo")
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

    def test_simple_unicode(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo[b"HEAD"] = c1.id
        porcelain.branch_create(self.repo, 'foo')
        self.assertTrue(b"foo" in porcelain.branch_list(self.repo))
        porcelain.branch_delete(self.repo, 'foo')
        self.assertFalse(b"foo" in porcelain.branch_list(self.repo))


class FetchTests(PorcelainTestCase):

    def test_simple(self):
        outstream = BytesIO()
        errstream = BytesIO()

        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test',
                         author=b'test <email>',
                         committer=b'test <email>')

        # Setup target repo
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target_repo = porcelain.clone(self.repo.path, target=target_path,
                                      errstream=errstream)

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test2',
                         author=b'test2 <email>',
                         committer=b'test2 <email>')

        self.assertFalse(self.repo[b'HEAD'].id in target_repo)
        target_repo.close()

        # Fetch changes into the cloned repo
        porcelain.fetch(target_path, 'origin',
                        outstream=outstream, errstream=errstream)

        # Assert that fetch updated the local image of the remote
        self.assert_correct_remote_refs(
            target_repo.get_refs(), self.repo.get_refs())

        # Check the target repo for pushed changes
        with Repo(target_path) as r:
            self.assertTrue(self.repo[b'HEAD'].id in r)

    def test_with_remote_name(self):
        remote_name = 'origin'
        outstream = BytesIO()
        errstream = BytesIO()

        # create a file for initial commit
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test',
                         author=b'test <email>',
                         committer=b'test <email>')

        # Setup target repo
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target_repo = porcelain.clone(self.repo.path, target=target_path,
                                      errstream=errstream)

        # Capture current refs
        target_refs = target_repo.get_refs()

        # create a second file to be pushed
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.commit(repo=self.repo.path, message=b'test2',
                         author=b'test2 <email>',
                         committer=b'test2 <email>')

        self.assertFalse(self.repo[b'HEAD'].id in target_repo)

        target_config = target_repo.get_config()
        target_config.set(
            (b'remote', remote_name.encode()), b'url', self.repo.path.encode())
        target_repo.close()

        # Fetch changes into the cloned repo
        porcelain.fetch(target_path, remote_name,
                        outstream=outstream, errstream=errstream)

        # Assert that fetch updated the local image of the remote
        self.assert_correct_remote_refs(
            target_repo.get_refs(), self.repo.get_refs())

        # Check the target repo for pushed changes, as well as updates
        # for the refs
        with Repo(target_path) as r:
            self.assertTrue(self.repo[b'HEAD'].id in r)
            self.assertNotEqual(self.repo.get_refs(), target_refs)

    def assert_correct_remote_refs(
            self, local_refs, remote_refs, remote_name=b'origin'):
        """Assert that known remote refs corresponds to actual remote refs."""
        local_ref_prefix = b'refs/heads'
        remote_ref_prefix = b'refs/remotes/' + remote_name

        locally_known_remote_refs = {
            k[len(remote_ref_prefix) + 1:]: v for k, v in local_refs.items()
            if k.startswith(remote_ref_prefix)}

        normalized_remote_refs = {
            k[len(local_ref_prefix) + 1:]: v for k, v in remote_refs.items()
            if k.startswith(local_ref_prefix)}

        self.assertEqual(locally_known_remote_refs, normalized_remote_refs)


class RepackTests(PorcelainTestCase):

    def test_empty(self):
        porcelain.repack(self.repo)

    def test_simple(self):
        handle, fullpath = tempfile.mkstemp(dir=self.repo.path)
        os.close(handle)
        porcelain.add(repo=self.repo.path, paths=fullpath)
        porcelain.repack(self.repo)


class LsTreeTests(PorcelainTestCase):

    def test_empty(self):
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')

        f = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=f)
        self.assertEqual(f.getvalue(), "")

    def test_simple(self):
        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write('origstuff')

        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')

        f = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=f)
        self.assertEqual(
                f.getvalue(),
                '100644 blob 8b82634d7eae019850bb883f06abf428c58bc9aa\tfoo\n')

    def test_recursive(self):
        # Create a directory then write a dummy file in it
        dirpath = os.path.join(self.repo.path, 'adir')
        filepath = os.path.join(dirpath, 'afile')
        os.mkdir(dirpath)
        with open(filepath, 'w') as f:
            f.write('origstuff')
        porcelain.add(repo=self.repo.path, paths=[filepath])
        porcelain.commit(repo=self.repo.path, message=b'test status',
                         author=b'author <email>',
                         committer=b'committer <email>')
        f = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=f)
        self.assertEqual(
                f.getvalue(),
                '40000 tree b145cc69a5e17693e24d8a7be0016ed8075de66d\tadir\n')
        f = StringIO()
        porcelain.ls_tree(self.repo, b"HEAD", outstream=f, recursive=True)
        self.assertEqual(
                f.getvalue(),
                '40000 tree b145cc69a5e17693e24d8a7be0016ed8075de66d\tadir\n'
                '100644 blob 8b82634d7eae019850bb883f06abf428c58bc9aa\tadir'
                '/afile\n')


class LsRemoteTests(PorcelainTestCase):

    def test_empty(self):
        self.assertEqual({}, porcelain.ls_remote(self.repo.path))

    def test_some(self):
        cid = porcelain.commit(repo=self.repo.path, message=b'test status',
                               author=b'author <email>',
                               committer=b'committer <email>')

        self.assertEqual({
            b'refs/heads/master': cid,
            b'HEAD': cid},
            porcelain.ls_remote(self.repo.path))


class LsFilesTests(PorcelainTestCase):

    def test_empty(self):
        self.assertEqual([], list(porcelain.ls_files(self.repo)))

    def test_simple(self):
        # Commit a dummy file then modify it
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write('origstuff')

        porcelain.add(repo=self.repo.path, paths=[fullpath])
        self.assertEqual([b'foo'], list(porcelain.ls_files(self.repo)))


class RemoteAddTests(PorcelainTestCase):

    def test_new(self):
        porcelain.remote_add(
            self.repo, 'jelmer', 'git://jelmer.uk/code/dulwich')
        c = self.repo.get_config()
        self.assertEqual(
            c.get((b'remote', b'jelmer'), b'url'),
            b'git://jelmer.uk/code/dulwich')

    def test_exists(self):
        porcelain.remote_add(
            self.repo, 'jelmer', 'git://jelmer.uk/code/dulwich')
        self.assertRaises(porcelain.RemoteExists, porcelain.remote_add,
                          self.repo, 'jelmer', 'git://jelmer.uk/code/dulwich')


class CheckIgnoreTests(PorcelainTestCase):

    def test_check_ignored(self):
        with open(os.path.join(self.repo.path, '.gitignore'), 'w') as f:
            f.write('foo')
        foo_path = os.path.join(self.repo.path, 'foo')
        with open(foo_path, 'w') as f:
            f.write('BAR')
        bar_path = os.path.join(self.repo.path, 'bar')
        with open(bar_path, 'w') as f:
            f.write('BAR')
        self.assertEqual(
            ['foo'],
            list(porcelain.check_ignore(self.repo, [foo_path])))
        self.assertEqual(
            [], list(porcelain.check_ignore(self.repo, [bar_path])))

    def test_check_added_abs(self):
        path = os.path.join(self.repo.path, 'foo')
        with open(path, 'w') as f:
            f.write('BAR')
        self.repo.stage(['foo'])
        with open(os.path.join(self.repo.path, '.gitignore'), 'w') as f:
            f.write('foo\n')
        self.assertEqual(
            [], list(porcelain.check_ignore(self.repo, [path])))
        self.assertEqual(
            ['foo'],
            list(porcelain.check_ignore(self.repo, [path], no_index=True)))

    def test_check_added_rel(self):
        with open(os.path.join(self.repo.path, 'foo'), 'w') as f:
            f.write('BAR')
        self.repo.stage(['foo'])
        with open(os.path.join(self.repo.path, '.gitignore'), 'w') as f:
            f.write('foo\n')
        cwd = os.getcwd()
        os.mkdir(os.path.join(self.repo.path, 'bar'))
        os.chdir(os.path.join(self.repo.path, 'bar'))
        try:
            self.assertEqual(
                list(porcelain.check_ignore(self.repo, ['../foo'])), [])
            self.assertEqual(['../foo'], list(
               porcelain.check_ignore(self.repo, ['../foo'], no_index=True)))
        finally:
            os.chdir(cwd)


class UpdateHeadTests(PorcelainTestCase):

    def test_set_to_branch(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, "blah")
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(b'ref: refs/heads/blah',
                         self.repo.refs.read_ref(b'HEAD'))

    def test_set_to_branch_detached(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, "blah", detached=True)
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(c1.id, self.repo.refs.read_ref(b'HEAD'))

    def test_set_to_commit_detached(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, c1.id, detached=True)
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(c1.id, self.repo.refs.read_ref(b'HEAD'))

    def test_set_new_branch(self):
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        self.repo.refs[b"refs/heads/blah"] = c1.id
        porcelain.update_head(self.repo, "blah", new_branch="bar")
        self.assertEqual(c1.id, self.repo.head())
        self.assertEqual(b'ref: refs/heads/bar',
                         self.repo.refs.read_ref(b'HEAD'))


class MailmapTests(PorcelainTestCase):

    def test_no_mailmap(self):
        self.assertEqual(
            b'Jelmer Vernooij <jelmer@samba.org>',
            porcelain.check_mailmap(
                self.repo, b'Jelmer Vernooij <jelmer@samba.org>'))

    def test_mailmap_lookup(self):
        with open(os.path.join(self.repo.path, '.mailmap'), 'wb') as f:
            f.write(b"""\
Jelmer Vernooij <jelmer@debian.org>
""")
        self.assertEqual(
            b'Jelmer Vernooij <jelmer@debian.org>',
            porcelain.check_mailmap(
                self.repo, b'Jelmer Vernooij <jelmer@samba.org>'))


class FsckTests(PorcelainTestCase):

    def test_none(self):
        self.assertEqual(
                [],
                list(porcelain.fsck(self.repo)))

    def test_git_dir(self):
        obj = Tree()
        a = Blob()
        a.data = b"foo"
        obj.add(b".git", 0o100644, a.id)
        self.repo.object_store.add_objects(
            [(a, None), (obj, None)])
        self.assertEqual(
                [(obj.id, 'invalid name .git')],
                [(sha, str(e)) for (sha, e) in porcelain.fsck(self.repo)])


class DescribeTests(PorcelainTestCase):

    def test_no_commits(self):
        self.assertRaises(KeyError, porcelain.describe, self.repo.path)

    def test_single_commit(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
                self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>",
                committer=b"Bob <bob@example.com>")
        self.assertEqual(
                'g{}'.format(sha[:7].decode('ascii')),
                porcelain.describe(self.repo.path))

    def test_tag(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
                self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>",
                committer=b"Bob <bob@example.com>")
        porcelain.tag_create(self.repo.path, b"tryme", b'foo <foo@bar.com>',
                             b'bar', annotated=True)
        self.assertEqual(
                "tryme",
                porcelain.describe(self.repo.path))

    def test_tag_and_commit(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
                self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>",
                committer=b"Bob <bob@example.com>")
        porcelain.tag_create(self.repo.path, b"tryme", b'foo <foo@bar.com>',
                             b'bar', annotated=True)
        with open(fullpath, 'w') as f:
            f.write("BAR2")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        sha = porcelain.commit(
                self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>",
                committer=b"Bob <bob@example.com>")
        self.assertEqual(
                'tryme-1-g{}'.format(sha[:7].decode('ascii')),
                porcelain.describe(self.repo.path))


class PathToTreeTests(PorcelainTestCase):

    def setUp(self):
        super(PathToTreeTests, self).setUp()
        self.fp = os.path.join(self.test_dir, 'bar')
        with open(self.fp, 'w') as f:
            f.write('something')
        oldcwd = os.getcwd()
        self.addCleanup(os.chdir, oldcwd)
        os.chdir(self.test_dir)

    def test_path_to_tree_path_base(self):
        self.assertEqual(
            b'bar', porcelain.path_to_tree_path(self.test_dir, self.fp))
        self.assertEqual(b'bar', porcelain.path_to_tree_path('.', './bar'))
        self.assertEqual(b'bar', porcelain.path_to_tree_path('.', 'bar'))
        cwd = os.getcwd()
        self.assertEqual(
            b'bar', porcelain.path_to_tree_path('.', os.path.join(cwd, 'bar')))
        self.assertEqual(b'bar', porcelain.path_to_tree_path(cwd, 'bar'))

    def test_path_to_tree_path_syntax(self):
        self.assertEqual(b'bar', porcelain.path_to_tree_path('.', './bar'))

    def test_path_to_tree_path_error(self):
        with self.assertRaises(ValueError):
            with tempfile.TemporaryDirectory() as od:
                porcelain.path_to_tree_path(od, self.fp)

    def test_path_to_tree_path_rel(self):
        cwd = os.getcwd()
        os.mkdir(os.path.join(self.repo.path, 'foo'))
        os.mkdir(os.path.join(self.repo.path, 'foo/bar'))
        try:
            os.chdir(os.path.join(self.repo.path, 'foo/bar'))
            with open('baz', 'w') as f:
                f.write('contents')
            self.assertEqual(b'bar/baz', porcelain.path_to_tree_path(
                '..', 'baz'))
            self.assertEqual(b'bar/baz', porcelain.path_to_tree_path(
                os.path.join(os.getcwd(), '..'),
                os.path.join(os.getcwd(), 'baz')))
            self.assertEqual(b'bar/baz', porcelain.path_to_tree_path(
                '..', os.path.join(os.getcwd(), 'baz')))
            self.assertEqual(b'bar/baz', porcelain.path_to_tree_path(
                os.path.join(os.getcwd(), '..'), 'baz'))
        finally:
            os.chdir(cwd)


class GetObjectByPathTests(PorcelainTestCase):

    def test_simple(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
                self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>",
                committer=b"Bob <bob@example.com>")
        self.assertEqual(
            b"BAR",
            porcelain.get_object_by_path(self.repo, 'foo').data)
        self.assertEqual(
            b"BAR",
            porcelain.get_object_by_path(self.repo, b'foo').data)

    def test_encoding(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        porcelain.commit(
                self.repo.path, message=b"Some message",
                author=b"Joe <joe@example.com>",
                committer=b"Bob <bob@example.com>",
                encoding=b"utf-8")
        self.assertEqual(
            b"BAR",
            porcelain.get_object_by_path(self.repo, 'foo').data)
        self.assertEqual(
            b"BAR",
            porcelain.get_object_by_path(self.repo, b'foo').data)

    def test_missing(self):
        self.assertRaises(
            KeyError,
            porcelain.get_object_by_path, self.repo, 'foo')


class WriteTreeTests(PorcelainTestCase):

    def test_simple(self):
        fullpath = os.path.join(self.repo.path, 'foo')
        with open(fullpath, 'w') as f:
            f.write("BAR")
        porcelain.add(repo=self.repo.path, paths=[fullpath])
        self.assertEqual(
            b'd2092c8a9f311f0311083bf8d177f2ca0ab5b241',
            porcelain.write_tree(self.repo))


class ActiveBranchTests(PorcelainTestCase):

    def test_simple(self):
        self.assertEqual(b'master', porcelain.active_branch(self.repo))
