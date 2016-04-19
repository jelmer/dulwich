# test_fastexport.py -- Fast export/import functionality
# Copyright (C) 2010 Jelmer Vernooij <jelmer@samba.org>
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

from io import BytesIO
import stat


from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    Commit,
    Tree,
    )
from dulwich.repo import (
    MemoryRepo,
    )
from dulwich.tests import (
    SkipTest,
    TestCase,
    )
from dulwich.tests.utils import (
    build_commit_graph,
    )


class GitFastExporterTests(TestCase):
    """Tests for the GitFastExporter tests."""

    def setUp(self):
        super(GitFastExporterTests, self).setUp()
        self.store = MemoryObjectStore()
        self.stream = BytesIO()
        try:
            from dulwich.fastexport import GitFastExporter
        except ImportError:
            raise SkipTest("python-fastimport not available")
        self.fastexporter = GitFastExporter(self.stream, self.store)

    def test_emit_blob(self):
        b = Blob()
        b.data = b"fooBAR"
        self.fastexporter.emit_blob(b)
        self.assertEqual(b'blob\nmark :1\ndata 6\nfooBAR\n',
            self.stream.getvalue())

    def test_emit_commit(self):
        b = Blob()
        b.data = b"FOO"
        t = Tree()
        t.add(b"foo", stat.S_IFREG | 0o644, b.id)
        c = Commit()
        c.committer = c.author = b"Jelmer <jelmer@host>"
        c.author_time = c.commit_time = 1271345553
        c.author_timezone = c.commit_timezone = 0
        c.message = b"msg"
        c.tree = t.id
        self.store.add_objects([(b, None), (t, None), (c, None)])
        self.fastexporter.emit_commit(c, b"refs/heads/master")
        self.assertEqual(b"""blob
mark :1
data 3
FOO
commit refs/heads/master
mark :2
author Jelmer <jelmer@host> 1271345553 +0000
committer Jelmer <jelmer@host> 1271345553 +0000
data 3
msg
M 644 :1 foo
""", self.stream.getvalue())


class GitImportProcessorTests(TestCase):
    """Tests for the GitImportProcessor tests."""

    def setUp(self):
        super(GitImportProcessorTests, self).setUp()
        self.repo = MemoryRepo()
        try:
            from dulwich.fastexport import GitImportProcessor
        except ImportError:
            raise SkipTest("python-fastimport not available")
        self.processor = GitImportProcessor(self.repo)

    def test_reset_handler(self):
        from fastimport import commands
        [c1] = build_commit_graph(self.repo.object_store, [[1]])
        cmd = commands.ResetCommand(b"refs/heads/foo", c1.id)
        self.processor.reset_handler(cmd)
        self.assertEqual(c1.id, self.repo.get_refs()[b"refs/heads/foo"])

    def test_commit_handler(self):
        from fastimport import commands
        cmd = commands.CommitCommand(b"refs/heads/foo",  b"mrkr",
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            b"FOO", None, [], [])
        self.processor.commit_handler(cmd)
        commit = self.repo[self.processor.last_commit]
        self.assertEqual(b"Jelmer <jelmer@samba.org>", commit.author)
        self.assertEqual(b"Jelmer <jelmer@samba.org>", commit.committer)
        self.assertEqual(b"FOO", commit.message)
        self.assertEqual([], commit.parents)
        self.assertEqual(432432432.0, commit.commit_time)
        self.assertEqual(432432432.0, commit.author_time)
        self.assertEqual(3600, commit.commit_timezone)
        self.assertEqual(3600, commit.author_timezone)
        self.assertEqual(commit, self.repo[b"refs/heads/foo"])

    def test_import_stream(self):
        markers = self.processor.import_stream(BytesIO(b"""blob
mark :1
data 11
text for a

commit refs/heads/master
mark :2
committer Joe Foo <joe@foo.com> 1288287382 +0000
data 20
<The commit message>
M 100644 :1 a

"""))
        self.assertEqual(2, len(markers))
        self.assertTrue(isinstance(self.repo[markers[b"1"]], Blob))
        self.assertTrue(isinstance(self.repo[markers[b"2"]], Commit))

    def test_file_add(self):
        from fastimport import commands
        cmd = commands.BlobCommand(b"23", b"data")
        self.processor.blob_handler(cmd)
        cmd = commands.CommitCommand(b"refs/heads/foo", b"mrkr",
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            b"FOO", None, [], [commands.FileModifyCommand(b"path", 0o100644, b":23", None)])
        self.processor.commit_handler(cmd)
        commit = self.repo[self.processor.last_commit]
        self.assertEqual([
            (b'path', 0o100644, b'6320cd248dd8aeaab759d5871f8781b5c0505172')],
            self.repo[commit.tree].items())

    def simple_commit(self):
        from fastimport import commands
        cmd = commands.BlobCommand(b"23", b"data")
        self.processor.blob_handler(cmd)
        cmd = commands.CommitCommand(b"refs/heads/foo", b"mrkr",
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            b"FOO", None, [], [commands.FileModifyCommand(b"path", 0o100644, b":23", None)])
        self.processor.commit_handler(cmd)
        commit = self.repo[self.processor.last_commit]
        return commit

    def make_file_commit(self, file_cmds):
        """Create a trivial commit with the specified file commands.

        :param file_cmds: File commands to run.
        :return: The created commit object
        """
        from fastimport import commands
        cmd = commands.CommitCommand(b"refs/heads/foo", b"mrkr",
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            (b"Jelmer", b"jelmer@samba.org", 432432432.0, 3600),
            b"FOO", None, [], file_cmds)
        self.processor.commit_handler(cmd)
        return self.repo[self.processor.last_commit]

    def test_file_copy(self):
        from fastimport import commands
        self.simple_commit()
        commit = self.make_file_commit([commands.FileCopyCommand(b"path", b"new_path")])
        self.assertEqual([
            (b'new_path', 0o100644, b'6320cd248dd8aeaab759d5871f8781b5c0505172'),
            (b'path', 0o100644, b'6320cd248dd8aeaab759d5871f8781b5c0505172'),
            ], self.repo[commit.tree].items())

    def test_file_move(self):
        from fastimport import commands
        self.simple_commit()
        commit = self.make_file_commit([commands.FileRenameCommand(b"path", b"new_path")])
        self.assertEqual([
            (b'new_path', 0o100644, b'6320cd248dd8aeaab759d5871f8781b5c0505172'),
            ], self.repo[commit.tree].items())

    def test_file_delete(self):
        from fastimport import commands
        self.simple_commit()
        commit = self.make_file_commit([commands.FileDeleteCommand(b"path")])
        self.assertEqual([], self.repo[commit.tree].items())

    def test_file_deleteall(self):
        from fastimport import commands
        self.simple_commit()
        commit = self.make_file_commit([commands.FileDeleteAllCommand()])
        self.assertEqual([], self.repo[commit.tree].items())
