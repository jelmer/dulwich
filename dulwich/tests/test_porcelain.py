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

from cStringIO import StringIO
import os
import shutil
import tarfile
import tempfile

from dulwich import porcelain
from dulwich.objects import (
    Blob,
    Tree,
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
        super(TestCase, self).setUp()
        repo_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, repo_dir)
        self.repo = Repo.init(repo_dir)


class ArchiveTests(PorcelainTestCase):
    """Tests for the archive command."""

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1], [3, 1, 2]])
        self.repo.refs["refs/heads/master"] = c3.id
        out = StringIO()
        err = StringIO()
        porcelain.archive(self.repo.path, "refs/heads/master", outstream=out,
            errstream=err)
        self.assertEquals("", err.getvalue())
        tf = tarfile.TarFile(fileobj=out)
        self.addCleanup(tf.close)
        self.assertEquals([], tf.getnames())


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
        self.assertTrue(type(sha) is str)
        self.assertEquals(len(sha), 40)


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
        outstream = StringIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path,
                            checkout=False, outstream=outstream)
        self.assertEquals(r.path, target_path)
        self.assertEquals(Repo(target_path).head(), c3.id)
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
        outstream = StringIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path,
                            checkout=True, outstream=outstream)
        self.assertEquals(r.path, target_path)
        self.assertEquals(Repo(target_path).head(), c3.id)
        self.assertTrue('f1' in os.listdir(target_path))
        self.assertTrue('f2' in os.listdir(target_path))


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

    def test_add_file(self):
        f = open(os.path.join(self.repo.path, 'foo'), 'w')
        try:
            f.write("BAR")
        finally:
            f.close()
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
        outstream = StringIO()
        porcelain.log(self.repo.path, outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))


class ShowTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id
        outstream = StringIO()
        porcelain.show(self.repo.path, committish=c3.id, outstream=outstream)
        self.assertTrue(outstream.getvalue().startswith("-" * 50))


class SymbolicRefTests(PorcelainTestCase):

    def test_set_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        outstream = StringIO()
        self.assertRaises(ValueError, porcelain.symbolic_ref, self.repo.path, 'foobar')

    def test_set_force_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        porcelain.symbolic_ref(self.repo.path, 'force_foobar', force=True)

        #test if we actually changed the file
        new_ref = self.repo.get_named_file('HEAD').read()
        self.assertEqual(new_ref, 'ref: refs/heads/force_foobar\n')

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
        new_ref = self.repo.get_named_file('HEAD').read()
        self.assertEqual(new_ref, 'ref: refs/heads/develop\n')


class DiffTreeTests(PorcelainTestCase):

    def test_empty(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id
        outstream = StringIO()
        porcelain.diff_tree(self.repo.path, c2.tree, c3.tree, outstream=outstream)
        self.assertEquals(outstream.getvalue(), "")


class CommitTreeTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        b = Blob()
        b.data = "foo the bar"
        t = Tree()
        t.add("somename", 0100644, b.id)
        self.repo.object_store.add_object(t)
        self.repo.object_store.add_object(b)
        sha = porcelain.commit_tree(
            self.repo.path, t.id, message="Withcommit.",
            author="Joe <joe@example.com>",
            committer="Jane <jane@example.com>")
        self.assertTrue(type(sha) is str)
        self.assertEquals(len(sha), 40)


class RevListTests(PorcelainTestCase):

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        outstream = StringIO()
        porcelain.rev_list(
            self.repo.path, [c3.id], outstream=outstream)
        self.assertEquals(
            "%s\n%s\n%s\n" % (c3.id, c2.id, c1.id),
            outstream.getvalue())
