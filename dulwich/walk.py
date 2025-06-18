# walk.py -- General implementation of walking commits and their contents.
# Copyright (C) 2010 Google, Inc.
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

"""General implementation of walking commits and their contents."""

import collections
import heapq
from collections.abc import Iterator
from itertools import chain
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

if TYPE_CHECKING:
    from .object_store import BaseObjectStore

from .diff_tree import (
    RENAME_CHANGE_TYPES,
    RenameDetector,
    TreeChange,
    tree_changes,
    tree_changes_for_merge,
)
from .errors import MissingCommitError
from .objects import Commit, ObjectID, Tag

ORDER_DATE = "date"
ORDER_TOPO = "topo"

ALL_ORDERS = (ORDER_DATE, ORDER_TOPO)

# Maximum number of commits to walk past a commit time boundary.
_MAX_EXTRA_COMMITS = 5


class WalkEntry:
    """Object encapsulating a single result from a walk."""

    def __init__(self, walker: "Walker", commit: Commit) -> None:
        self.commit = commit
        self._store = walker.store
        self._get_parents = walker.get_parents
        self._changes: dict[Optional[bytes], list[TreeChange]] = {}
        self._rename_detector = walker.rename_detector

    def changes(
        self, path_prefix: Optional[bytes] = None
    ) -> Union[list[TreeChange], list[list[TreeChange]]]:
        """Get the tree changes for this entry.

        Args:
          path_prefix: Portion of the path in the repository to
            use to filter changes. Must be a directory name. Must be
            a full, valid, path reference (no partial names or wildcards).
        Returns: For commits with up to one parent, a list of TreeChange
            objects; if the commit has no parents, these will be relative to
            the empty tree. For merge commits, a list of lists of TreeChange
            objects; see dulwich.diff.tree_changes_for_merge.
        """
        cached = self._changes.get(path_prefix)
        if cached is None:
            commit = self.commit
            if not self._get_parents(commit):
                changes_func = tree_changes
                parent = None
            elif len(self._get_parents(commit)) == 1:
                changes_func = tree_changes
                parent = cast(Commit, self._store[self._get_parents(commit)[0]]).tree
                if path_prefix:
                    mode, subtree_sha = parent.lookup_path(
                        self._store.__getitem__,
                        path_prefix,
                    )
                    parent = self._store[subtree_sha]
            else:
                # For merge commits, we need to handle multiple parents differently
                parent = [
                    cast(Commit, self._store[p]).tree for p in self._get_parents(commit)
                ]
                # Use a lambda to adapt the signature
                changes_func = cast(
                    Any,
                    lambda store,
                    parent_trees,
                    tree_id,
                    rename_detector=None: tree_changes_for_merge(
                        store, parent_trees, tree_id, rename_detector
                    ),
                )
                if path_prefix:
                    parent_trees = [self._store[p] for p in parent]
                    parent = []
                    for p in parent_trees:
                        try:
                            from .objects import Tree

                            assert isinstance(p, Tree)
                            mode, st = p.lookup_path(
                                self._store.__getitem__,
                                path_prefix,
                            )
                        except KeyError:
                            pass
                        else:
                            parent.append(st)
            commit_tree_sha = commit.tree
            if path_prefix:
                commit_tree = self._store[commit_tree_sha]
                from .objects import Tree

                assert isinstance(commit_tree, Tree)
                mode, commit_tree_sha = commit_tree.lookup_path(
                    self._store.__getitem__,
                    path_prefix,
                )
            cached = list(
                changes_func(
                    self._store,
                    parent,
                    commit_tree_sha,
                    rename_detector=self._rename_detector,
                )
            )
            self._changes[path_prefix] = cached
        return self._changes[path_prefix]

    def __repr__(self) -> str:
        return f"<WalkEntry commit={self.commit.id.decode('ascii')}, changes={self.changes()!r}>"


class _CommitTimeQueue:
    """Priority queue of WalkEntry objects by commit time."""

    def __init__(self, walker: "Walker") -> None:
        self._walker = walker
        self._store = walker.store
        self._get_parents = walker.get_parents
        self._excluded = walker.excluded
        self._pq: list[tuple[int, Commit]] = []
        self._pq_set: set[ObjectID] = set()
        self._seen: set[ObjectID] = set()
        self._done: set[ObjectID] = set()
        self._min_time = walker.since
        self._last: Optional[Commit] = None
        self._extra_commits_left = _MAX_EXTRA_COMMITS
        self._is_finished = False

        for commit_id in chain(walker.include, walker.excluded):
            self._push(commit_id)

    def _push(self, object_id: ObjectID) -> None:
        try:
            obj = self._store[object_id]
        except KeyError as exc:
            raise MissingCommitError(object_id) from exc
        if isinstance(obj, Tag):
            self._push(obj.object[1])
            return
        # TODO(jelmer): What to do about non-Commit and non-Tag objects?
        if not isinstance(obj, Commit):
            return
        commit = obj
        if commit.id not in self._pq_set and commit.id not in self._done:
            heapq.heappush(self._pq, (-commit.commit_time, commit))
            self._pq_set.add(commit.id)
            self._seen.add(commit.id)

    def _exclude_parents(self, commit: Commit) -> None:
        excluded = self._excluded
        seen = self._seen
        todo = [commit]
        while todo:
            commit = todo.pop()
            for parent in self._get_parents(commit):
                if parent not in excluded and parent in seen:
                    # TODO: This is inefficient unless the object store does
                    # some caching (which DiskObjectStore currently does not).
                    # We could either add caching in this class or pass around
                    # parsed queue entry objects instead of commits.
                    todo.append(cast(Commit, self._store[parent]))
                excluded.add(parent)

    def next(self) -> Optional[WalkEntry]:
        if self._is_finished:
            return None
        while self._pq:
            _, commit = heapq.heappop(self._pq)
            sha = commit.id
            self._pq_set.remove(sha)
            if sha in self._done:
                continue
            self._done.add(sha)

            for parent_id in self._get_parents(commit):
                self._push(parent_id)

            reset_extra_commits = True
            is_excluded = sha in self._excluded
            if is_excluded:
                self._exclude_parents(commit)
                if self._pq and all(c.id in self._excluded for _, c in self._pq):
                    _, n = self._pq[0]
                    if self._last and n.commit_time >= self._last.commit_time:
                        # If the next commit is newer than the last one, we
                        # need to keep walking in case its parents (which we
                        # may not have seen yet) are excluded. This gives the
                        # excluded set a chance to "catch up" while the commit
                        # is still in the Walker's output queue.
                        reset_extra_commits = True
                    else:
                        reset_extra_commits = False

            if self._min_time is not None and commit.commit_time < self._min_time:
                # We want to stop walking at min_time, but commits at the
                # boundary may be out of order with respect to their parents.
                # So we walk _MAX_EXTRA_COMMITS more commits once we hit this
                # boundary.
                reset_extra_commits = False

            if reset_extra_commits:
                # We're not at a boundary, so reset the counter.
                self._extra_commits_left = _MAX_EXTRA_COMMITS
            else:
                self._extra_commits_left -= 1
                if not self._extra_commits_left:
                    break

            if not is_excluded:
                self._last = commit
                return WalkEntry(self._walker, commit)
        self._is_finished = True
        return None

    __next__ = next


class Walker:
    """Object for performing a walk of commits in a store.

    Walker objects are initialized with a store and other options and can then
    be treated as iterators of Commit objects.
    """

    def __init__(
        self,
        store: "BaseObjectStore",
        include: list[bytes],
        exclude: Optional[list[bytes]] = None,
        order: str = "date",
        reverse: bool = False,
        max_entries: Optional[int] = None,
        paths: Optional[list[bytes]] = None,
        rename_detector: Optional[RenameDetector] = None,
        follow: bool = False,
        since: Optional[int] = None,
        until: Optional[int] = None,
        get_parents: Callable[[Commit], list[bytes]] = lambda commit: commit.parents,
        queue_cls: type = _CommitTimeQueue,
    ) -> None:
        """Constructor.

        Args:
          store: ObjectStore instance for looking up objects.
          include: Iterable of SHAs of commits to include along with their
            ancestors.
          exclude: Iterable of SHAs of commits to exclude along with their
            ancestors, overriding includes.
          order: ORDER_* constant specifying the order of results.
            Anything other than ORDER_DATE may result in O(n) memory usage.
          reverse: If True, reverse the order of output, requiring O(n)
            memory.
          max_entries: The maximum number of entries to yield, or None for
            no limit.
          paths: Iterable of file or subtree paths to show entries for.
          rename_detector: diff.RenameDetector object for detecting
            renames.
          follow: If True, follow path across renames/copies. Forces a
            default rename_detector.
          since: Timestamp to list commits after.
          until: Timestamp to list commits before.
          get_parents: Method to retrieve the parents of a commit
          queue_cls: A class to use for a queue of commits, supporting the
            iterator protocol. The constructor takes a single argument, the
            Walker.
        """
        # Note: when adding arguments to this method, please also update
        # dulwich.repo.BaseRepo.get_walker
        if order not in ALL_ORDERS:
            raise ValueError(f"Unknown walk order {order}")
        self.store = store
        if isinstance(include, bytes):
            # TODO(jelmer): Really, this should require a single type.
            # Print deprecation warning here?
            include = [include]
        self.include = include
        self.excluded = set(exclude or [])
        self.order = order
        self.reverse = reverse
        self.max_entries = max_entries
        self.paths = (paths and set(paths)) or None
        if follow and not rename_detector:
            rename_detector = RenameDetector(store)
        self.rename_detector = rename_detector
        self.get_parents = get_parents
        self.follow = follow
        self.since = since
        self.until = until

        self._num_entries = 0
        self._queue = queue_cls(self)
        self._out_queue: collections.deque[WalkEntry] = collections.deque()

    def _path_matches(self, changed_path: Optional[bytes]) -> bool:
        if changed_path is None:
            return False
        if self.paths is None:
            return True
        for followed_path in self.paths:
            if changed_path == followed_path:
                return True
            if (
                changed_path.startswith(followed_path)
                and changed_path[len(followed_path)] == b"/"[0]
            ):
                return True
        return False

    def _change_matches(self, change: TreeChange) -> bool:
        assert self.paths
        if not change:
            return False

        old_path = change.old.path
        new_path = change.new.path
        if self._path_matches(new_path):
            if self.follow and change.type in RENAME_CHANGE_TYPES:
                self.paths.add(old_path)
                self.paths.remove(new_path)
            return True
        elif self._path_matches(old_path):
            return True
        return False

    def _should_return(self, entry: WalkEntry) -> Optional[bool]:
        """Determine if a walk entry should be returned..

        Args:
          entry: The WalkEntry to consider.
        Returns: True if the WalkEntry should be returned by this walk, or
            False otherwise (e.g. if it doesn't match any requested paths).
        """
        commit = entry.commit
        if self.since is not None and commit.commit_time < self.since:
            return False
        if self.until is not None and commit.commit_time > self.until:
            return False
        if commit.id in self.excluded:
            return False

        if self.paths is None:
            return True

        if len(self.get_parents(commit)) > 1:
            for path_changes in entry.changes():
                # For merge commits, only include changes with conflicts for
                # this path. Since a rename conflict may include different
                # old.paths, we have to check all of them.
                for change in path_changes:
                    if self._change_matches(change):
                        return True
        else:
            changes = entry.changes()
            # Handle both list[TreeChange] and list[list[TreeChange]]
            if changes and isinstance(changes[0], list):
                # It's list[list[TreeChange]], flatten it
                for change_list in changes:
                    for change in change_list:
                        if self._change_matches(change):
                            return True
            else:
                # It's list[TreeChange]
                from .diff_tree import TreeChange

                for change in changes:
                    if isinstance(change, TreeChange) and self._change_matches(change):
                        return True
        return None

    def _next(self) -> Optional[WalkEntry]:
        max_entries = self.max_entries
        while max_entries is None or self._num_entries < max_entries:
            entry = next(self._queue)
            if entry is not None:
                self._out_queue.append(entry)
            if entry is None or len(self._out_queue) > _MAX_EXTRA_COMMITS:
                if not self._out_queue:
                    return None
                entry = self._out_queue.popleft()
                if self._should_return(entry):
                    self._num_entries += 1
                    return entry
        return None

    def _reorder(
        self, results: Iterator[WalkEntry]
    ) -> Union[Iterator[WalkEntry], list[WalkEntry]]:
        """Possibly reorder a results iterator.

        Args:
          results: An iterator of WalkEntry objects, in the order returned
            from the queue_cls.
        Returns: An iterator or list of WalkEntry objects, in the order
            required by the Walker.
        """
        if self.order == ORDER_TOPO:
            results = _topo_reorder(results, self.get_parents)
        if self.reverse:
            results = reversed(list(results))
        return results

    def __iter__(self) -> Iterator[WalkEntry]:
        return iter(self._reorder(iter(self._next, None)))


def _topo_reorder(
    entries: Iterator[WalkEntry],
    get_parents: Callable[[Commit], list[bytes]] = lambda commit: commit.parents,
) -> Iterator[WalkEntry]:
    """Reorder an iterable of entries topologically.

    This works best assuming the entries are already in almost-topological
    order, e.g. in commit time order.

    Args:
      entries: An iterable of WalkEntry objects.
      get_parents: Optional function for getting the parents of a commit.
    Returns: iterator over WalkEntry objects from entries in FIFO order, except
        where a parent would be yielded before any of its children.
    """
    todo: collections.deque[WalkEntry] = collections.deque()
    pending: dict[bytes, WalkEntry] = {}
    num_children: dict[bytes, int] = collections.defaultdict(int)
    for entry in entries:
        todo.append(entry)
        for p in get_parents(entry.commit):
            num_children[p] += 1

    while todo:
        entry = todo.popleft()
        commit = entry.commit
        commit_id = commit.id
        if num_children[commit_id]:
            pending[commit_id] = entry
            continue
        for parent_id in get_parents(commit):
            num_children[parent_id] -= 1
            if not num_children[parent_id]:
                parent_entry = pending.pop(parent_id, None)
                if parent_entry:
                    todo.appendleft(parent_entry)
        yield entry
