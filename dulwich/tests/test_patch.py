# test_patch.py -- tests for patch.py
# Copryight (C) 2010 Jelmer Vernooij <jelmer@samba.org>
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

"""Tests for patch.py."""

from cStringIO import StringIO
from unittest import TestCase

from dulwich.objects import (
    Commit,
    Tree,
    )
from dulwich.patch import (
    write_commit_patch,
    )


class WriteCommitPatchTests(TestCase):

    def test_simple(self):
        f = StringIO()
        c = Commit()
        c.committer = c.author = "Jelmer <jelmer@samba.org>"
        c.commit_time = c.author_time = 1271350201
        c.commit_timezone = c.author_timezone = 0
        c.message = "This is the first line\nAnd this is the second line.\n"
        c.tree = Tree().id
        write_commit_patch(f, c, "CONTENTS", (1, 1), version="custom")
        f.seek(0)
        lines = f.readlines()
        self.assertTrue(lines[0].startswith("From 0b0d34d1b5b596c928adc9a727a4b9e03d025298"))
        self.assertEquals(lines[1], "From: Jelmer <jelmer@samba.org>\n")
        self.assertTrue(lines[2].startswith("Date: "))
        self.assertEquals([
            "Subject: [PATCH 1/1] This is the first line\n",
            "And this is the second line.\n",
            "\n",
            "\n",
            "---\n",
            " 0 files changed\n",
            "\n",
            "CONTENTS-- \n",
            "custom\n"], lines[3:])
