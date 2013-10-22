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
from dulwich.repo import Repo
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    build_commit_graph,
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

    def test_simple(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["refs/heads/foo"] = c3.id
        sha = porcelain.commit(self.repo.path, message="Some message")
        self.assertTrue(type(sha) is str)
        self.assertEquals(len(sha), 40)

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
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["refs/heads/master"] = c3.id
        target_path = tempfile.mkdtemp()
        outstream = StringIO()
        self.addCleanup(shutil.rmtree, target_path)
        r = porcelain.clone(self.repo.path, target_path, outstream=outstream)
        self.assertEquals(r.path, target_path)
        self.assertEquals(Repo(target_path).head(), c3.id)


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
        porcelain.symbolic_ref(self.repo.path, 'foobar', errstream=outstream)
        err = 'fatal: ref `%s` is not a symbolic ref' % 'foobar'
        self.assertTrue(err in outstream.getvalue())

    def test_set_force_wrong_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        outstream = StringIO()
        porcelain.symbolic_ref(self.repo.path, 'force_foobar', force=True,
                               outstream=outstream)
        self.assertTrue('new symbolic ref' in outstream.getvalue())

        #test if we actually changed the file
        new_ref = self.repo.get_named_file('HEAD').read()
        self.assertEqual(new_ref, 'ref: refs/heads/force_foobar\n')

    def test_set_symbolic_ref(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]])
        self.repo.refs["HEAD"] = c3.id

        outstream = StringIO()
        porcelain.symbolic_ref(self.repo.path, 'master', outstream=outstream)
        self.assertTrue('new symbolic ref' in outstream.getvalue())

    def test_set_symbolic_ref_other_than_master(self):
        c1, c2, c3 = build_commit_graph(self.repo.object_store, [[1], [2, 1],
            [3, 1, 2]], attrs=dict(refs='develop'))
        self.repo.refs["HEAD"] = c3.id
        self.repo.refs["refs/heads/develop"] = c3.id

        outstream = StringIO()
        porcelain.symbolic_ref(self.repo.path, 'develop', outstream=outstream)
        self.assertTrue('new symbolic ref' in outstream.getvalue())

        #test if we actually changed the file
        new_ref = self.repo.get_named_file('HEAD').read()
        self.assertEqual(new_ref, 'ref: refs/heads/develop\n')
