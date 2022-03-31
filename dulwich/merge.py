# merge.py -- Merge support in Dulwich
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
#
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

"""Merge support."""

from collections import namedtuple
import stat
from typing import Union, Iterable, List, Callable, Optional


from .diff_tree import (
    TreeEntry,
    tree_changes,
    CHANGE_ADD,
    CHANGE_COPY,
    CHANGE_DELETE,
    CHANGE_MODIFY,
    CHANGE_RENAME,
    CHANGE_UNCHANGED,
    )
from .merge_base import find_merge_base
from .objects import Blob, Tree


FileMerger = Callable[[bytes, bytes, bytes], bytes]


class MergeConflict(namedtuple(
        'MergeConflict',
        ['this_entry', 'other_entry', 'base_entry', 'message'])):
    """A merge conflict."""


def _merge_entry(
        new_path: str, object_store, this_entry: TreeEntry,
        other_entry: TreeEntry, base_entry: TreeEntry,
        file_merger: Optional[FileMerger]):
    """Run a per entry-merge."""
    if file_merger is None:
        return MergeConflict(
            this_entry, other_entry, base_entry,
            'Conflict in %s but no file merger provided'
            % new_path)
    merged_text = file_merger(
        object_store[this_entry.sha].as_raw_string(),
        object_store[other_entry.sha].as_raw_string(),
        object_store[base_entry.sha].as_raw_string())
    merged_text_blob = Blob.from_string(merged_text)
    object_store.add_object(merged_text_blob)
    # TODO(jelmer): Report conflicts, if any?
    if this_entry.mode in (base_entry.mode, other_entry.mode):
        mode = other_entry.mode
    else:
        if base_entry.mode != other_entry.mode:
            # TODO(jelmer): Add a mode conflict
            raise NotImplementedError
        mode = this_entry.mode
    return TreeEntry(new_path, mode, merged_text_blob.id)


def merge_tree(
        object_store, this_tree: bytes, other_tree: bytes, common_tree: bytes,
        rename_detector=None,
        file_merger: Optional[FileMerger] = None) -> Iterable[
            Union[TreeEntry, MergeConflict]]:
    """Merge two trees.

    Args:
      object_store: object store to retrieve objects from
      this_tree: Tree id of THIS tree
      other_tree: Tree id of OTHER tree
      common_tree: Tree id of COMMON tree
      rename_detector: Rename detector object (see dulwich.diff_tree)
      file_merger: Three-way file merge implementation
    Returns:
      iterator over objects, either TreeEntry (updating an entry)
        or MergeConflict (indicating a conflict)
    """
    changes_this = tree_changes(
        object_store, common_tree, this_tree, rename_detector=rename_detector)
    changes_this_by_common_path = {
        change.old.path: change for change in changes_this if change.old}
    changes_this_by_this_path = {
        change.new.path: change for change in changes_this if change.new}
    for other_change in tree_changes(
            object_store, common_tree, other_tree,
            rename_detector=rename_detector):
        this_change = changes_this_by_common_path.get(other_change.old.path)
        if this_change == other_change:
            continue
        if other_change.type in (CHANGE_ADD, CHANGE_COPY):
            try:
                this_entry = changes_this_by_this_path[other_change.new.path]
            except KeyError:
                yield other_change.new
            else:
                if this_entry != other_change.new:
                    # TODO(jelmer): Three way merge instead, with empty common
                    # base?
                    yield MergeConflict(
                        this_entry, other_change.new, other_change.old,
                        'Both this and other add new file %s' %
                        other_change.new.path)
        elif other_change.type == CHANGE_DELETE:
            if this_change and this_change.type not in (
                    CHANGE_DELETE, CHANGE_UNCHANGED):
                yield MergeConflict(
                    this_change.new, other_change.new, other_change.old,
                    '%s is deleted in other but modified in this' %
                    other_change.old.path)
            else:
                yield TreeEntry(other_change.old.path, None, None)
        elif other_change.type == CHANGE_RENAME:
            if this_change and this_change.type == CHANGE_RENAME:
                if this_change.new.path != other_change.new.path:
                    # TODO(jelmer): Does this need to be a conflict?
                    yield MergeConflict(
                        this_change.new, other_change.new, other_change.old,
                        '%s was renamed by both sides (%s / %s)'
                        % (other_change.old.path, other_change.new.path,
                           this_change.new.path))
                else:
                    yield _merge_entry(
                        other_change.new.path,
                        object_store, this_change.new, other_change.new,
                        other_change.old, file_merger=file_merger)
            elif this_change and this_change.type == CHANGE_MODIFY:
                yield _merge_entry(
                    other_change.new.path,
                    object_store, this_change.new, other_change.new,
                    other_change.old, file_merger=file_merger)
            elif this_change and this_change.type == CHANGE_DELETE:
                yield MergeConflict(
                    this_change.new, other_change.new, other_change.old,
                    '%s is deleted in this but renamed to %s in other' %
                    (other_change.old.path, other_change.new.path))
            elif this_change:
                raise NotImplementedError(
                    '%r and %r' % (this_change, other_change))
            else:
                yield other_change.new
        elif other_change.type == CHANGE_MODIFY:
            if this_change and this_change.type == CHANGE_DELETE:
                yield MergeConflict(
                    this_change.new, other_change.new, other_change.old,
                    '%s is deleted in this but modified in other' %
                    other_change.old.path)
            elif this_change and this_change.type in (
                    CHANGE_MODIFY, CHANGE_RENAME):
                yield _merge_entry(
                    this_change.new.path,
                    object_store, this_change.new, other_change.new,
                    other_change.old, file_merger=file_merger)
            elif this_change:
                raise NotImplementedError(
                    '%r and %r' % (this_change, other_change))
            else:
                yield other_change.new
        else:
            raise NotImplementedError(
                'unsupported change type: %r' % other_change.type)


class MergeResults(object):

    def __init__(self, conflicts=None):
        self.conflicts = conflicts or []

    def __eq__(self, other):
        return (
            isinstance(self, MergeResults) and
            self.conflicts == other.conflicts)


def merge(
        repo, commit_ids: List[bytes], rename_detector=None,
        file_merger: Optional[FileMerger] = None,
        tree_encoding: str = "utf-8") -> MergeResults:
    """Perform a merge.
    """
    from .index import index_entry_from_stat
    import os
    conflicts = []
    [merge_base] = find_merge_base(repo, [repo.head()] + commit_ids)
    [other_id] = commit_ids
    index = repo.open_index()
    this_tree = index.commit(repo.object_store)
    for entry in merge_tree(
            repo.object_store,
            this_tree,
            repo.object_store[other_id].tree,
            repo.object_store[merge_base].tree,
            rename_detector=rename_detector,
            file_merger=file_merger):

        if isinstance(entry, MergeConflict):
            conflicts.append(entry)
            # TODO(jelmer): Need to still write something
        else:
            if stat.S_ISDIR(entry.mode):
                continue
            blob = repo.object_store[entry.sha]
            fs_path = os.path.join(
                repo.path,
                os.fsdecode(entry.path.decode(tree_encoding)))
            if stat.S_ISREG(entry.mode):
                with open(fs_path, 'wb') as f:
                    f.write(blob.as_raw_string())
                os.chmod(fs_path, entry.mode & 0o777)
            elif stat.S_ISLNK(entry.mode):
                os.symlink(blob.as_raw_string(), fs_path)
            else:
                raise NotImplementedError("unknown file mode %s" % entry.mode)
            index[entry.path] = index_entry_from_stat(
                os.stat(fs_path), entry.sha, 0, entry.mode)

    return MergeResults(conflicts=conflicts)
