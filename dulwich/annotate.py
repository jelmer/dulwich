# annotate.py -- Annotate files with last changed revision
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
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

"""Annotate file contents indicating when they were last changed.

Annotated lines are represented as tuples with last modified revision SHA1
and contents.

Please note that this is a very naive annotate implementation; it's
"""

import difflib

from dulwich.object_store import tree_lookup_path
from dulwich.walk import (
    ORDER_DATE,
    Walker,
    )


# Walk over ancestry graph breadth-first
# When checking each revision, find lines that according to difflib.Differ()
# are common between versions.
# Any lines that are not in common were introduced by the newer revision.
# If there were no lines kept from the older version, stop going deeper in the graph.

def update_lines(differ, annotated_lines, new_history_data, new_blob):
    """Update annotation lines with old blob lines.
    """
    old_index = 0
    for diffline in differ.compare(
            [l for (h, l) in annotated_lines],
            new_blob.splitlines()):
        if diffline[0:1] == '?':
            continue
        elif diffline[0:1] == ' ':
            yield annotated_lines[old_index]
            old_index += 1
        elif diffline[0:1] == '+':
            yield (new_history_data, diffline[2:])
        elif diffline[0:1] == '-':
            old_index += 1
        else:
            raise RuntimeError('Unknown character %s returned in diff' % diffline[0])


def annotate_lines(store, commit_id, path, order=ORDER_DATE, lines=None, follow=True):
    """Annotate the lines of a blob.

    :param store: Object store to retrieve objects from
    :param commit_id: Commit id in which to annotate path
    :param path: Path to annotate
    :param order: Order in which to process history (defaults to ORDER_DATE)
    :param lines: Initial lines to compare to (defaults to specified)
    :param follow: Wether to follow changes across renames/copies
    :return: List of (commit, line) entries where
        commit is the oldest commit that changed a line
    """
    walker = Walker(store, include=[commit_id], paths=[path], order=order,
        follow=follow)
    d = difflib.Differ()
    revs = []
    for log_entry in walker:
        for tree_change in log_entry.changes():
            if type(tree_change) is not list:
                tree_change = [tree_change]
            for change in tree_change:
                if change.new.path == path:
                    path = change.old.path
                    revs.append((log_entry.commit, change.new))
                    break

    lines = []
    for (commit, entry) in reversed(revs):
        lines = list(update_lines(d, lines, (commit, entry), store[entry.sha]))
    return lines
