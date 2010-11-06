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

from cStringIO import StringIO
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
    TestCase,
    TestSkipped,
    )


class GitFastExporterTests(TestCase):
    """Tests for the GitFastExporter tests."""

    def setUp(self):
        super(GitFastExporterTests, self).setUp()
        self.store = MemoryObjectStore()
        self.stream = StringIO()
        try:
            from dulwich.fastexport import GitFastExporter
        except ImportError:
            raise TestSkipped("python-fastimport not available")
        self.fastexporter = GitFastExporter(self.stream, self.store)

    def test_emit_blob(self):
        b = Blob()
        b.data = "fooBAR"
        self.fastexporter.emit_blob(b)
        self.assertEquals('blob\nmark :1\ndata 6\nfooBAR\n',
            self.stream.getvalue())

    def test_emit_commit(self):
        b = Blob()
        b.data = "FOO"
        t = Tree()
        t.add(stat.S_IFREG | 0644, "foo", b.id)
        c = Commit()
        c.committer = c.author = "Jelmer <jelmer@host>"
        c.author_time = c.commit_time = 1271345553
        c.author_timezone = c.commit_timezone = 0
        c.message = "msg"
        c.tree = t.id
        self.store.add_objects([(b, None), (t, None), (c, None)])
        self.fastexporter.emit_commit(c, "refs/heads/master")
        self.assertEquals("""blob
mark :1
data 3
FOO
commit refs/heads/master
mark :2
author Jelmer <jelmer@host> 1271345553 +0000
committer Jelmer <jelmer@host> 1271345553 +0000
data 3
msg
M 644 1 foo
""", self.stream.getvalue())


class GitImportProcessorTests(TestCase):
    """Tests for the GitImportProcessor tests."""

    def setUp(self):
        super(GitImportProcessorTests, self).setUp()
        self.repo = MemoryRepo()
        try:
            from dulwich.fastexport import GitImportProcessor
        except ImportError:
            raise TestSkipped("python-fastimport not available")
        self.processor = GitImportProcessor(self.repo)

    def test_commit_handler(self):
        from fastimport import commands
        cmd = commands.CommitCommand("refs/heads/foo", "mrkr",
            ("Jelmer", "jelmer@samba.org", 432432432.0, 3600),
            ("Jelmer", "jelmer@samba.org", 432432432.0, 3600),
            "FOO", None, [], [])
        self.processor.commit_handler(cmd)
        commit = self.repo[self.processor.last_commit]
        self.assertEquals("Jelmer <jelmer@samba.org>", commit.author)
        self.assertEquals("Jelmer <jelmer@samba.org>", commit.committer)
        self.assertEquals("FOO", commit.message)
        self.assertEquals([], commit.parents)
        self.assertEquals(432432432.0, commit.commit_time)
        self.assertEquals(432432432.0, commit.author_time)
        self.assertEquals(3600, commit.commit_timezone)
        self.assertEquals(3600, commit.author_timezone)
        self.assertEquals(commit, self.repo["refs/heads/foo"])

    def test_import_stream(self):
        markers = self.processor.import_stream(StringIO("""blob
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
        self.assertEquals(2, len(markers))
        self.assertTrue(isinstance(self.repo[markers["1"]], Blob))
        self.assertTrue(isinstance(self.repo[markers["2"]], Commit))
