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

from dulwich.objects import (
    format_timezone,
    )

import stat

class FastExporter(object):
    """Generate a fast-export output stream for Git objects."""

    def __init__(self, outf, store):
        self.outf = outf
        self.store = store
        self.markers = {}
        self._marker_idx = 0

    def _allocate_marker(self):
        self._marker_idx+=1
        return self._marker_idx

    def _dump_blob(self, blob, marker):
        self.outf.write("blob\nmark :%s\n" % marker)
        self.outf.write("data %s\n" % blob.raw_length())
        for chunk in blob.as_raw_chunks():
            self.outf.write(chunk)
        self.outf.write("\n")

    def export_blob(self, blob):
        i = self._allocate_marker()
        self.markers[i] = blob.id
        self._dump_blob(blob, i)
        return i

    def _dump_commit(self, commit, marker, ref, file_changes):
        self.outf.write("commit %s\n" % ref)
        self.outf.write("mark :%s\n" % marker)
        self.outf.write("author %s %s %s\n" % (commit.author,
            commit.author_time, format_timezone(commit.author_timezone)))
        self.outf.write("committer %s %s %s\n" % (commit.committer,
            commit.commit_time, format_timezone(commit.commit_timezone)))
        self.outf.write("data %s\n" % len(commit.message))
        self.outf.write(commit.message)
        self.outf.write("\n")
        self.outf.write('\n'.join(file_changes))
        self.outf.write("\n\n")

    def export_commit(self, commit, ref, base_tree=None):
        file_changes = []
        for (old_path, new_path), (old_mode, new_mode), (old_hexsha, new_hexsha) in \
                self.store.tree_changes(base_tree, commit.tree):
            if new_path is None:
                file_changes.append("D %s" % old_path)
                continue
            if not stat.S_ISDIR(new_mode):
                marker = self.export_blob(self.store[new_hexsha])
            file_changes.append("M %o :%s %s" % (new_mode, marker, new_path))

        i = self._allocate_marker()
        self._dump_commit(commit, i, ref, file_changes)
        return i
