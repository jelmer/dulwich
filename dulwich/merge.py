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
from .objects import Blob


class MergeConflict(namedtuple(
        'MergeConflict',
        ['this_entry', 'other_entry', 'base_entry', 'message'])):
    """A merge conflict."""


def find_merge_base(repo, commit_ids):
    """Find a reasonable merge base.

    Args:
      repo: Repository object
      commit_ids: List of commit ids
    """
    # TODO(jelmer): replace with a pure-python implementation
    import subprocess
    return subprocess.check_output(
        ['git', 'merge-base'] +
        [x.decode('ascii') for x in commit_ids],
        cwd=repo.path).rstrip(b'\n')


def _merge_entry(new_path, object_store, this_entry, other_entry, base_entry,
                 file_merger):
    """Run a per entry-merge."""
    if file_merger is None:
        return MergeConflict(
            this_entry, other_entry,
            other_entry.old,
            'Conflict in %s but no file merger provided'
            % new_path)
    merged_text = file_merger(
        object_store[this_entry.sha].as_raw_string(),
        object_store[other_entry.sha].as_raw_string(),
        object_store[base_entry.sha].as_raw_string())
    merged_text_blob = Blob.from_string(merged_text)
    object_store.add(merged_text_blob)
    # TODO(jelmer): Report conflicts, if any?
    if this_entry.mode in (base_entry.mode, other_entry.mode):
        mode = other_entry.mode
    else:
        if base_entry.mode != other_entry.mode:
            # TODO(jelmer): Add a mode conflict
            raise NotImplementedError
        mode = this_entry.mode
    yield TreeEntry(new_path, mode, merged_text_blob.id)


def merge_trees(object_store, this_tree, other_tree, common_tree,
                rename_detector=None, file_merger=None):
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
    changes_this = tree_changes(object_store, common_tree, this_tree)
    changes_this_by_common_path = {
        change.old.name: change for change in changes_this if change.old}
    changes_this_by_this_path = {
        change.new.name: change for change in changes_this if change.new}
    for other_change in tree_changes(object_store, common_tree, other_tree):
        this_change = changes_this_by_common_path.get(other_change.old.name)
        if this_change == other_change:
            continue
        if other_change.type in (CHANGE_ADD, CHANGE_COPY):
            try:
                this_entry = changes_this_by_this_path[other_change.new.name]
            except KeyError:
                yield other_change.new.name
            else:
                if this_entry != other_change.new:
                    # TODO(jelmer): Three way merge instead, with empty common
                    # base?
                    yield MergeConflict(
                        this_entry, other_change.new, other_change.old,
                        'Both this and other add new file %s' %
                        other_change.new.name)
        elif other_change.type == CHANGE_DELETE:
            if this_change and this_change.type not in (
                    CHANGE_DELETE, CHANGE_UNCHANGED):
                yield MergeConflict(
                    this_change.new, other_change.new, other_change.old,
                    '%s is deleted in other but modified in this' %
                    other_change.old.name)
            else:
                yield TreeEntry(other_change.old.name, None, None)
        elif other_change.type == CHANGE_RENAME:
            if this_change and this_change.type == CHANGE_RENAME:
                if this_change.new.name != other_change.new.name:
                    # TODO(jelmer): Does this need to be a conflict?
                    yield MergeConflict(
                        this_change.new, other_change.new, other_change.old,
                        '%s was renamed by both sides (%s / %s)'
                        % (other_change.old.name, other_change.new.name,
                           this_change.new.name))
                else:
                    yield _merge_entry(
                        other_change.new.name,
                        object_store, this_change.new, other_change.new,
                        other_change.old, file_merger=file_merger)
            elif this_change and this_change.type == CHANGE_MODIFY:
                yield _merge_entry(
                    other_change.new.name,
                    object_store, this_change.new, other_change.new,
                    other_change.old, file_merger=file_merger)
            elif this_change and this_change.type == CHANGE_DELETE:
                yield MergeConflict(
                    this_change.new, other_change.new, other_change.old,
                    '%s is deleted in this but renamed to %s in other' %
                    (other_change.old.name, other_change.new.name))
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
                    other_change.old.name)
            elif this_change and this_change.type in (
                    CHANGE_MODIFY, CHANGE_RENAME):
                yield _merge_entry(
                    this_change.new.name,
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

    def __init__(self, conflicts):
        self.conflicts = conflicts


def merge(repo, commit_ids, rename_detector=None, file_merger=None):
    """Perform a merge.
    """
    conflicts = []
    merge_base = find_merge_base(repo, commit_ids)
    [other_id] = commit_ids
    index = repo.open_index()
    this_id = index.commit(repo.object_store)
    for entry in merge_trees(
            repo.object_store,
            repo.object_store[this_id].tree,
            repo.object_store[other_id].tree,
            repo.object_store[merge_base].tree,
            rename_detector=rename_detector):

        if isinstance(entry, MergeConflict):
            conflicts.append(entry)

    return conflicts
