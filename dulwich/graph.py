# vim:ts=4:sw=4:softtabstop=4:smarttab:expandtab
# Copyright (c) 2020 Kevin B. Hendricks, Stratford Ontario Canada
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

"""Implementation of merge-base following the approach of git."""

from collections.abc import Iterator, Mapping, Sequence
from heapq import heappop, heappush
from typing import TYPE_CHECKING, Callable, Generic, Optional, TypeVar

if TYPE_CHECKING:
    from .repo import BaseRepo

from .lru_cache import LRUCache
from .objects import Commit, ObjectID

T = TypeVar("T")


# priority queue using builtin python minheap tools
# why they do not have a builtin maxheap is simply ridiculous but
# liveable with integer time stamps using negation
class WorkList(Generic[T]):
    """Priority queue for commit processing using a min-heap."""

    def __init__(self) -> None:
        """Initialize an empty work list."""
        self.pq: list[tuple[int, T]] = []

    def add(self, item: tuple[int, T]) -> None:
        """Add an item to the work list.

        Args:
            item: Tuple of (timestamp, commit)
        """
        dt, cmt = item
        heappush(self.pq, (-dt, cmt))

    def get(self) -> Optional[tuple[int, T]]:
        """Get the highest priority item from the work list.

        Returns:
            Tuple of (timestamp, commit) or None if empty
        """
        item = heappop(self.pq)
        if item:
            pr, cmt = item
            return -pr, cmt
        return None

    def iter(self) -> Iterator[tuple[int, T]]:
        """Iterate over items in the work list.

        Yields:
            Tuples of (timestamp, commit)
        """
        for pr, cmt in self.pq:
            yield (-pr, cmt)


def _find_lcas(
    lookup_parents: Callable[[ObjectID], list[ObjectID]],
    c1: ObjectID,
    c2s: Sequence[ObjectID],
    lookup_stamp: Callable[[ObjectID], int],
    min_stamp: int = 0,
    shallows: Optional[set[ObjectID]] = None,
) -> list[ObjectID]:
    """Find lowest common ancestors between commits.

    Args:
        lookup_parents: Function to get parent commits
        c1: First commit
        c2s: List of second commits
        lookup_stamp: Function to get commit timestamp
        min_stamp: Minimum timestamp to consider
        shallows: Set of shallow commits

    Returns:
        List of lowest common ancestor commit IDs
    """
    cands = []
    cstates = {}

    # Flags to Record State
    _ANC_OF_1 = 1  # ancestor of commit 1
    _ANC_OF_2 = 2  # ancestor of commit 2
    _DNC = 4  # Do Not Consider
    _LCA = 8  # potential LCA (Lowest Common Ancestor)

    def _has_candidates(
        wlst: WorkList[ObjectID], cstates: Mapping[ObjectID, int]
    ) -> bool:
        """Check if there are any candidate commits in the work list.

        Args:
            wlst: Work list of commits
            cstates: Dictionary of commit states

        Returns:
            True if there are candidates to process
        """
        for dt, cmt in wlst.iter():
            if cmt in cstates:
                if not ((cstates[cmt] & _DNC) == _DNC):
                    return True
        return False

    # initialize the working list states with ancestry info
    # note possibility of c1 being one of c2s should be handled
    wlst: WorkList[bytes] = WorkList()
    cstates[c1] = _ANC_OF_1
    try:
        wlst.add((lookup_stamp(c1), c1))
    except KeyError:
        # If c1 doesn't exist and we have shallow commits, it might be a missing parent
        if shallows is None or not shallows:
            raise
        # For missing commits in shallow repos, use a minimal timestamp
        wlst.add((0, c1))

    for c2 in c2s:
        cflags = cstates.get(c2, 0)
        cstates[c2] = cflags | _ANC_OF_2
        try:
            wlst.add((lookup_stamp(c2), c2))
        except KeyError:
            # If c2 doesn't exist and we have shallow commits, it might be a missing parent
            if shallows is None or not shallows:
                raise
            # For missing commits in shallow repos, use a minimal timestamp
            wlst.add((0, c2))

    # loop while at least one working list commit is still viable (not marked as _DNC)
    # adding any parents to the list in a breadth first manner
    while _has_candidates(wlst, cstates):
        result = wlst.get()
        if result is None:
            break
        dt, cmt = result
        # Look only at ANCESTRY and _DNC flags so that already
        # found _LCAs can still be marked _DNC by lower _LCAS
        cflags = cstates[cmt] & (_ANC_OF_1 | _ANC_OF_2 | _DNC)
        if cflags == (_ANC_OF_1 | _ANC_OF_2):
            # potential common ancestor if not already in candidates add it
            if not (cstates[cmt] & _LCA) == _LCA:
                cstates[cmt] = cstates[cmt] | _LCA
                cands.append((dt, cmt))
            # mark any parents of this node _DNC as all parents
            # would be one generation further removed common ancestors
            cflags = cflags | _DNC
        try:
            parents = lookup_parents(cmt)
        except KeyError:
            # If we can't get parents in a shallow repo, skip this node
            # This is safer than pretending it has no parents
            if shallows is not None and shallows:
                continue
            raise

        if parents:
            for pcmt in parents:
                pflags = cstates.get(pcmt, 0)
                # if this parent was already visited with no new ancestry/flag information
                # do not add it to the working list again
                if (pflags & cflags) == cflags:
                    continue
                try:
                    pdt = lookup_stamp(pcmt)
                except KeyError:
                    # Parent doesn't exist - if we're in a shallow repo, skip it
                    if shallows is not None and shallows:
                        continue
                    raise
                if pdt < min_stamp:
                    continue
                cstates[pcmt] = pflags | cflags
                wlst.add((pdt, pcmt))

    # walk final candidates removing any superseded by _DNC by later lower _LCAs
    # remove any duplicates and sort it so that earliest is first
    results = []
    for dt, cmt in cands:
        if not ((cstates[cmt] & _DNC) == _DNC) and (dt, cmt) not in results:
            results.append((dt, cmt))
    results.sort(key=lambda x: x[0])
    lcas = [cmt for dt, cmt in results]
    return lcas


# actual git sorts these based on commit times
def find_merge_base(repo: "BaseRepo", commit_ids: Sequence[ObjectID]) -> list[ObjectID]:
    """Find lowest common ancestors of commit_ids[0] and *any* of commits_ids[1:].

    Args:
      repo: Repository object
      commit_ids: list of commit ids
    Returns:
      list of lowest common ancestor commit_ids
    """
    cmtcache: LRUCache[ObjectID, Commit] = LRUCache(max_cache=128)
    parents_provider = repo.parents_provider()

    def lookup_stamp(cmtid: ObjectID) -> int:
        if cmtid not in cmtcache:
            obj = repo.object_store[cmtid]
            assert isinstance(obj, Commit)
            cmtcache[cmtid] = obj
        commit_time = cmtcache[cmtid].commit_time
        assert isinstance(commit_time, int)
        return commit_time

    def lookup_parents(cmtid: ObjectID) -> list[ObjectID]:
        commit = None
        if cmtid in cmtcache:
            commit = cmtcache[cmtid]
        # must use parents provider to handle grafts and shallow
        return parents_provider.get_parents(cmtid, commit=commit)

    if not commit_ids:
        return []
    c1 = commit_ids[0]
    if not len(commit_ids) > 1:
        return [c1]
    c2s = list(commit_ids[1:])
    if c1 in c2s:
        return [c1]
    lcas = _find_lcas(
        lookup_parents, c1, c2s, lookup_stamp, shallows=parents_provider.shallows
    )
    return lcas


def find_octopus_base(
    repo: "BaseRepo", commit_ids: Sequence[ObjectID]
) -> list[ObjectID]:
    """Find lowest common ancestors of *all* provided commit_ids.

    Args:
      repo: Repository
      commit_ids:  list of commit ids
    Returns:
      list of lowest common ancestor commit_ids
    """
    cmtcache: LRUCache[ObjectID, Commit] = LRUCache(max_cache=128)
    parents_provider = repo.parents_provider()

    def lookup_stamp(cmtid: ObjectID) -> int:
        if cmtid not in cmtcache:
            obj = repo.object_store[cmtid]
            assert isinstance(obj, Commit)
            cmtcache[cmtid] = obj
        commit_time = cmtcache[cmtid].commit_time
        assert isinstance(commit_time, int)
        return commit_time

    def lookup_parents(cmtid: ObjectID) -> list[ObjectID]:
        commit = None
        if cmtid in cmtcache:
            commit = cmtcache[cmtid]
        # must use parents provider to handle grafts and shallow
        return parents_provider.get_parents(cmtid, commit=commit)

    if not commit_ids:
        return []
    if len(commit_ids) <= 2:
        return find_merge_base(repo, commit_ids)
    lcas = [commit_ids[0]]
    others = commit_ids[1:]
    for cmt in others:
        next_lcas = []
        for ca in lcas:
            res = _find_lcas(
                lookup_parents,
                cmt,
                [ca],
                lookup_stamp,
                shallows=parents_provider.shallows,
            )
            next_lcas.extend(res)
        lcas = next_lcas[:]
    return lcas


def can_fast_forward(repo: "BaseRepo", c1: bytes, c2: bytes) -> bool:
    """Is it possible to fast-forward from c1 to c2?

    Args:
      repo: Repository to retrieve objects from
      c1: Commit id for first commit
      c2: Commit id for second commit
    """
    cmtcache: LRUCache[ObjectID, Commit] = LRUCache(max_cache=128)
    parents_provider = repo.parents_provider()

    def lookup_stamp(cmtid: ObjectID) -> int:
        if cmtid not in cmtcache:
            obj = repo.object_store[cmtid]
            assert isinstance(obj, Commit)
            cmtcache[cmtid] = obj
        commit_time = cmtcache[cmtid].commit_time
        assert isinstance(commit_time, int)
        return commit_time

    def lookup_parents(cmtid: ObjectID) -> list[ObjectID]:
        commit = None
        if cmtid in cmtcache:
            commit = cmtcache[cmtid]
        # must use parents provider to handle grafts and shallow
        return parents_provider.get_parents(cmtid, commit=commit)

    if c1 == c2:
        return True

    # Algorithm: Find the common ancestor
    try:
        min_stamp = lookup_stamp(c1)
    except KeyError:
        # If c1 doesn't exist in the object store, we can't determine fast-forward
        # This can happen in shallow clones where c1 is a missing parent
        # Check if any shallow commits have c1 as a parent
        if parents_provider.shallows:
            # We're in a shallow repository and c1 doesn't exist
            # We can't determine if fast-forward is possible
            return False
        raise

    lcas = _find_lcas(
        lookup_parents,
        c1,
        [c2],
        lookup_stamp,
        min_stamp=min_stamp,
        shallows=parents_provider.shallows,
    )
    return lcas == [c1]


def independent(repo: "BaseRepo", commit_ids: Sequence[ObjectID]) -> list[ObjectID]:
    """Filter commits to only those that are not reachable from others.

    Args:
      repo: Repository object
      commit_ids: list of commit ids to filter

    Returns:
      list of commit ids that are not ancestors of any other commits in the list
    """
    if not commit_ids:
        return []
    if len(commit_ids) == 1:
        return list(commit_ids)

    # Filter out commits that are ancestors of other commits
    independent_commits = []
    for i, commit_id in enumerate(commit_ids):
        is_independent = True
        # Check if this commit is an ancestor of any other commit
        for j, other_id in enumerate(commit_ids):
            if i == j:
                continue
            # If merge base of (commit_id, other_id) is commit_id,
            # then commit_id is an ancestor of other_id
            merge_bases = find_merge_base(repo, [commit_id, other_id])
            if merge_bases == [commit_id]:
                is_independent = False
                break
        if is_independent:
            independent_commits.append(commit_id)

    return independent_commits
