# __init__.py -- Fast export/import functionality
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


"""Fast export/import functionality."""

from dulwich.objects import format_timezone

import stat

class FastExporter(object):
    """Generate a fast-export output stream for Git objects."""

    def __init__(self, outf, store):
        self.outf = outf
        self.store = store
        self.markers = {}

    def export_blob(self, blob, i):
        self.outf.write("blob\nmark :%s\n" % i)
        self.outf.write("data %s\n" % blob.raw_length())
        for chunk in blob.as_raw_chunks():
            self.outf.write(chunk)
        self.outf.write("\n")

    def export_commit(self, commit, branchname):
        file_changes = []
        for path, mode, hexsha in tree_changes(commit.tree):
            if stat.S_ISDIR(mode):
                file_changes.extend(self.dump_file_tree(self.store[hexsha]))
            else:
                self.dump_file_blob(self.store[hexsha], i)
            file_changes.append("M %o :%s %s" % (mode, i, path))
        return file_changes
        self.write("commit refs/heads/%s\n" % branchname)
        self.write("committer %s %s %s\n" % (commit.committer, commit.commit_time, format_timezone(commit.commit_timezone)))
        self.write("author %s %s %s\n" % (commit.author, commit.author_time, format_timezone(commit.author_timezone)))
        self.write("data %s\n" % len(commit.message))
        self.write(commit.message)
        self.write("\n")
        file_changes = self.export_tree(self.store[commit.tree])
        self.write('\n'.join(file_changes))
        self.write("\n\n")
