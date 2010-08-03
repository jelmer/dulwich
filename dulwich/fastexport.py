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

from dulwich.index import (
    commit_tree,
    )
from dulwich.objects import (
    Blob,
    Commit,
    format_timezone,
    parse_timezone,
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


class FastImporter(object):
    """Class for importing fastimport streams.

    Please note that this is mostly a stub implementation at the moment,
    doing the bare mimimum.
    """

    def __init__(self, repo):
        self.repo = repo

    def _parse_person(self, line):
        (name, timestr, timezonestr) = line.rsplit(" ", 2)
        return name, int(timestr), parse_timezone(timezonestr)[0]

    def _read_blob(self, stream):
        line = stream.readline()
        if line.startswith("mark :"):
            mark = line[len("mark :"):-1]
            line = stream.readline()
        else:
            mark = None
        if not line.startswith("data "):
            raise ValueError("Blob without valid data line: %s" % line)
        size = int(line[len("data "):])
        o = Blob()
        o.data = stream.read(size)
        stream.readline()
        self.repo.object_store.add_object(o)
        return mark, o.id

    def _read_commit(self, stream, contents, marks):
        line = stream.readline()
        if line.startswith("mark :"):
            mark = line[len("mark :"):-1]
            line = stream.readline()
        else:
            mark = None
        o = Commit()
        o.author = None
        o.author_time = None
        while line.startswith("author "):
            (o.author, o.author_time, o.author_timezone) = \
                    self._parse_person(line[len("author "):-1])
            line = stream.readline()
        while line.startswith("committer "):
            (o.committer, o.commit_time, o.commit_timezone) = \
                    self._parse_person(line[len("committer "):-1])
            line = stream.readline()
        if o.author is None:
            o.author = o.committer
        if o.author_time is None:
            o.author_time = o.commit_time
            o.author_timezone = o.commit_timezone
        if not line.startswith("data "):
            raise ValueError("Blob without valid data line: %s" % line)
        size = int(line[len("data "):])
        o.message = stream.read(size)
        stream.readline()
        line = stream.readline()[:-1]
        while line:
            if line.startswith("M "):
                (kind, modestr, val, path) = line.split(" ")
                if val[0] == ":":
                    val = marks[int(val[1:])]
                contents[path] = (int(modestr, 8), val)
            else:
                raise ValueError(line)
            line = stream.readline()[:-1]
        try:
            o.parents = (self.repo.head(),)
        except KeyError:
            o.parents = ()
        o.tree = commit_tree(self.repo.object_store,
            ((path, hexsha, mode) for (path, (mode, hexsha)) in
                contents.iteritems()))
        self.repo.object_store.add_object(o)
        return mark, o.id

    def import_stream(self, stream):
        """Import from a file-like object.

        :param stream: File-like object to read a fastimport stream from.
        :return: Dictionary with marks
        """
        contents = {}
        marks = {}
        while True:
            line = stream.readline()
            if not line:
                break
            line = line[:-1]
            if line == "" or line[0] == "#":
                continue
            if line.startswith("blob"):
                mark, hexsha = self._read_blob(stream)
                if mark is not None:
                    marks[int(mark)] = hexsha
            elif line.startswith("commit "):
                ref = line[len("commit "):-1]
                mark, hexsha = self._read_commit(stream, contents, marks)
                if mark is not None:
                    marks[int(mark)] = hexsha
                self.repo.refs["HEAD"] = self.repo.refs[ref] = hexsha
            else:
                raise ValueError("invalid command '%s'" % line)
        return marks
