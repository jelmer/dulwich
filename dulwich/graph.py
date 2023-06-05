#!/usr/bin/env python
# vim:ts=4:sw=4:softtabstop=4:smarttab:expandtab
# Copyright (c) 2020 Kevin B. Hendricks, Stratford Ontario Canada
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

"""Implementation of merge-base following the approach of git."""

from collections import deque
from typing import Deque


def _find_lcas(lookup_parents, c1, c2s):
    cands = []
    cstates = {}

    # Flags to Record State
    _ANC_OF_1 = 1  # ancestor of commit 1
    _ANC_OF_2 = 2  # ancestor of commit 2
    _DNC = 4  # Do Not Consider
    _LCA = 8  # potential LCA (Lowest Common Ancestor)

    def _has_candidates(wlst, cstates):
        for cmt in wlst:
            if cmt in cstates:
                if not ((cstates[cmt] & _DNC) == _DNC):
                    return True
        return False

    # initialize the working list states with ancestry info
    # note possibility of c1 being one of c2s should be handled
    wlst: Deque[bytes] = deque()
    cstates[c1] = _ANC_OF_1
    wlst.append(c1)
    for c2 in c2s:
        cflags = cstates.get(c2, 0)
        cstates[c2] = cflags | _ANC_OF_2
        wlst.append(c2)
    
    # loop while at least one working list commit is still viable (not marked as _DNC)
    # adding any parents to the list in a breadth first manner
    while _has_candidates(wlst, cstates):
        cmt = wlst.popleft()
        # Look only at ANCESTRY and _DNC flags so that already
        # found _LCAs can still be marked _DNC by lower _LCAS
        cflags = cstates[cmt] & (_ANC_OF_1 | _ANC_OF_2 | _DNC)
        if cflags == (_ANC_OF_1 | _ANC_OF_2):
            # potential common ancestor if not already in candidates add it
            if not (cstates[cmt] & _LCA) == _LCA:
                cstates[cmt] = cstates[cmt] | _LCA
                cands.append(cmt)
            # mark any parents of this node _DNC as all parents
            # would be one generation further removed common ancestors
            cflags = cflags | _DNC
        parents = lookup_parents(cmt)
        if parents:
            for pcmt in parents:
                pflags = cstates.get(pcmt, 0)
                # if this parent was already visited with no new ancestry/flag information
                # do not add it to the working list again
                if ((pflags & cflags) == cflags):
                    continue
                cstates[pcmt] = pflags | cflags
                wlst.append(pcmt)

    # walk final candidates removing any superseded by _DNC by later lower _LCAs
    results = []
    for cmt in cands:
        if not ((cstates[cmt] & _DNC) == _DNC):
            results.append(cmt)
    return results


def find_merge_base(repo, commit_ids):
    """Find lowest common ancestors of commit_ids[0] and *any* of commits_ids[1:].

    Args:
      repo: Repository object
      commit_ids: list of commit ids
    Returns:
      list of lowest common ancestor commit_ids
    """
    if not commit_ids:
        return []
    c1 = commit_ids[0]
    if not len(commit_ids) > 1:
        return [c1]
    c2s = commit_ids[1:]
    if c1 in c2s:
        return [c1]
    parents_provider = repo.parents_provider()
    return _find_lcas(parents_provider.get_parents, c1, c2s)


def find_octopus_base(repo, commit_ids):
    """Find lowest common ancestors of *all* provided commit_ids.

    Args:
      repo: Repository
      commit_ids:  list of commit ids
    Returns:
      list of lowest common ancestor commit_ids
    """
    if not commit_ids:
        return []
    if len(commit_ids) <= 2:
        return find_merge_base(repo, commit_ids)
    parents_provider = repo.parents_provider()
    lcas = [commit_ids[0]]
    others = commit_ids[1:]
    for cmt in others:
        next_lcas = []
        for ca in lcas:
            res = _find_lcas(parents_provider.get_parents, cmt, [ca])
            next_lcas.extend(res)
        lcas = next_lcas[:]
    return lcas


def can_fast_forward(repo, c1, c2):
    """Is it possible to fast-forward from c1 to c2?

    Args:
      repo: Repository to retrieve objects from
      c1: Commit id for first commit
      c2: Commit id for second commit
    """
    if c1 == c2:
        return True

    # Algorithm: Find the common ancestor
    parents_provider = repo.parents_provider()
    lcas = _find_lcas(parents_provider.get_parents, c1, [c2])
    return lcas == [c1]
