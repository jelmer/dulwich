# annotate.py -- Annotate files with last changed revision
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# You should have received a copy of the licenses; if not, see
# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
# License, Version 2.0.
#

"""Annotate file contents indicating when they were last changed.

Annotated lines are represented as tuples with last modified revision SHA1
and contents.

Please note that this is a very naive annotate implementation. It works,
but its speed could be improved - in particular because it uses
Python's difflib.
"""

__all__ = ["annotate_lines", "update_lines"]

import difflib
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dulwich.objects import Blob
from dulwich.walk import (
    ORDER_DATE,
    Walker,
)

if TYPE_CHECKING:
    from dulwich.diff_tree import TreeChange
    from dulwich.object_store import BaseObjectStore
    from dulwich.objects import Commit, ObjectID, TreeEntry

# Walk over ancestry graph breadth-first
# When checking each revision, find lines that according to difflib.Differ()
# are common between versions.
# Any lines that are not in common were introduced by the newer revision.
# If there were no lines kept from the older version, stop going deeper in the
# graph.


def update_lines(
    annotated_lines: Sequence[tuple[tuple["Commit", "TreeEntry"], bytes]],
    new_history_data: tuple["Commit", "TreeEntry"],
    new_blob: "Blob",
) -> list[tuple[tuple["Commit", "TreeEntry"], bytes]]:
    """Update annotation lines with old blob lines."""
    ret: list[tuple[tuple[Commit, TreeEntry], bytes]] = []
    new_lines = new_blob.splitlines()
    matcher = difflib.SequenceMatcher(
        a=[line for (h, line) in annotated_lines], b=new_lines
    )
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            ret.extend(annotated_lines[i1:i2])
        elif tag in ("insert", "replace"):
            ret.extend([(new_history_data, line) for line in new_lines[j1:j2]])
        elif tag == "delete":
            pass  # don't care
        else:
            raise RuntimeError(f"Unknown tag {tag} returned in diff")
    return ret


def annotate_lines(
    store: "BaseObjectStore",
    commit_id: "ObjectID",
    path: bytes,
    order: str = ORDER_DATE,
    lines: Sequence[tuple[tuple["Commit", "TreeEntry"], bytes]] | None = None,
    follow: bool = True,
) -> list[tuple[tuple["Commit", "TreeEntry"], bytes]]:
    """Annotate the lines of a blob.

    :param store: Object store to retrieve objects from
    :param commit_id: Commit id in which to annotate path
    :param path: Path to annotate
    :param order: Order in which to process history (defaults to ORDER_DATE)
    :param lines: Initial lines to compare to (defaults to specified)
    :param follow: Whether to follow changes across renames/copies
    :return: List of (commit, line) entries where
        commit is the oldest commit that changed a line
    """
    walker = Walker(
        store, include=[commit_id], paths=[path], order=order, follow=follow
    )
    revs: list[tuple[Commit, TreeEntry]] = []
    for log_entry in walker:
        for tree_change in log_entry.changes():
            changes: list[TreeChange]
            if isinstance(tree_change, list):
                changes = tree_change
            else:
                changes = [tree_change]
            for change in changes:
                if change.new is not None and change.new.path == path:
                    if change.old is not None and change.old.path is not None:
                        path = change.old.path
                    revs.append((log_entry.commit, change.new))
                    break

    lines_annotated: list[tuple[tuple[Commit, TreeEntry], bytes]] = []
    for commit, entry in reversed(revs):
        assert entry.sha is not None
        blob_obj = store[entry.sha]
        assert isinstance(blob_obj, Blob)
        lines_annotated = update_lines(lines_annotated, (commit, entry), blob_obj)
    return lines_annotated
