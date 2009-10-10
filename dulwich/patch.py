# patch.py -- For dealing wih packed-style patches.
# Copryight (C) 2009 Jelmer Vernooij <jelmer@samba.org>
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

"""Classes for dealing with git am-style patches.

These patches are basically unified diffs with some extra metadata tacked 
on.
"""

import difflib
import subprocess
import time


def write_commit_patch(f, commit, contents, progress, version=None):
    """Write a individual file patch.

    :param commit: Commit object
    :param progress: Tuple with current patch number and total.
    :return: tuple with filename and contents
    """
    (num, total) = progress
    f.write("From %s %s\n" % (commit.id, time.ctime(commit.commit_time)))
    f.write("From: %s\n" % commit.author)
    f.write("Date: %s\n" % time.strftime("%a, %d %b %Y %H:%M:%S %Z"))
    f.write("Subject: [PATCH %d/%d] %s\n" % (num, total, commit.message))
    f.write("\n")
    f.write("---\n")
    try:
        p = subprocess.Popen(["diffstat"], stdout=subprocess.PIPE, 
                             stdin=subprocess.PIPE)
    except OSError, e:
        pass # diffstat not available?
    else:
        (diffstat, _) = p.communicate(contents)
        f.write(diffstat)
        f.write("\n")
    f.write(contents)
    f.write("-- \n")
    if version is None:
        from dulwich import __version__ as dulwich_version
        f.write("Dulwich %d.%d.%d\n" % dulwich_version)
    else:
        f.write("%s\n" % version)


def get_summary(commit):
    """Determine the summary line for use in a filename.
    
    :param commit: Commit
    :return: Summary string
    """
    return commit.message.splitlines()[0].replace(" ", "-")


def write_blob_diff(f, (old_path, old_mode, old_blob), 
                       (new_path, new_mode, new_blob)):
    """Write diff file header.

    :param f: File-like object to write to
    :param (old_path, old_mode, old_blob): Previous file (None if nonexisting)
    :param (new_path, new_mode, new_blob): New file (None if nonexisting)
    """
    def blob_id(blob):
        if blob is None:
            return "0" * 7
        else:
            return blob.id[:7]
    def lines(blob):
        if blob is not None:
            return blob.data.splitlines(True)
        else:
            return []
    if old_path is None:
        old_path = "/dev/null"
    else:
        old_path = "a/%s" % old_path
    if new_path is None:
        new_path = "/dev/null"
    else:
        new_path = "b/%s" % new_path
    f.write("diff --git %s %s\n" % (old_path, new_path))
    if old_mode != new_mode:
        if new_mode is not None:
            if old_mode is not None:
                f.write("old file mode %o\n" % old_mode)
            f.write("new file mode %o\n" % new_mode) 
        else:
            f.write("deleted file mode %o\n" % old_mode)
    f.write("index %s..%s %o\n" % (
        blob_id(old_blob), blob_id(new_blob), new_mode))
    old_contents = lines(old_blob)
    new_contents = lines(new_blob)
    f.writelines(difflib.unified_diff(old_contents, new_contents, 
        old_path, new_path))
