#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim:ts=4:sw=4:softtabstop=4:smarttab:expandtab
"""
Implementation of merge-base following the approach of git
"""
# Copyright (c) 2020 Kevin B. Hendricks, Stratford Ontario Canada
#
# Available under the MIT License

from collections import deque


def _find_lcas(lookup_parents, c1, c2s):
    cands = []
    cstates = {}

    # Flags to Record State
    _ANC_OF_1 = 1  # ancestor of commit 1
    _ANC_OF_2 = 2  # ancestor of commit 2
    _DNC = 4       # Do Not Consider
    _LCA = 8       # potential LCA

    def _has_candidates(wlst, cstates):
        for cmt in wlst:
            if cmt in cstates:
                if not (cstates[cmt] & _DNC):
                    return True
        return False

    # initialize the working list
    wlst = deque()
    cstates[c1] = _ANC_OF_1
    wlst.append(c1)
    for c2 in c2s:
        cstates[c2] = _ANC_OF_2
        wlst.append(c2)

    # loop until no other LCA candidates are viable in working list
    # adding any parents to the list in a breadth first manner
    while _has_candidates(wlst, cstates):
        cmt = wlst.popleft()
        flags = cstates[cmt]
        if flags == (_ANC_OF_1 | _ANC_OF_2):
            # potential common ancestor
            if not (flags & _LCA):
                flags = flags | _LCA
                cstates[cmt] = flags
                cands.append(cmt)
                # mark any parents of this node _DNC as all parents
                # would be one level further removed common ancestors
                flags = flags | _DNC
        parents = lookup_parents(cmt)
        if parents:
            for pcmt in parents:
                if pcmt in cstates:
                    cstates[pcmt] = cstates[pcmt] | flags
                else:
                    cstates[pcmt] = flags
                wlst.append(pcmt)

    # walk final candidates removing any superceded by _DNC by later lower LCAs
    results = []
    for cmt in cands:
        if not (cstates[cmt] & _DNC):
            results.append(cmt)
    return results


def find_merge_base(object_store, commit_ids):
    """Find lowest common ancestors of commit_ids[0] and *any* of commits_ids[1:]

    Args:
      object_store: object store
      commit_ids:  list of commit ids
    Returns:
      list of lowest common ancestor commit_ids
    """
    def lookup_parents(commit_id):
        return object_store[commit_id].parents

    if not commit_ids:
        return []
    c1 = commit_ids[0]
    if not len(commit_ids) > 1:
        return [c1]
    c2s = commit_ids[1:]
    if c1 in c2s:
        return [c1]
    return _find_lcas(lookup_parents, c1, c2s)


def find_octopus_base(object_store, commit_ids):
    """Find lowest common ancestors of *all* provided commit_ids

    Args:
      object_store: Object store
      commit_ids:  list of commit ids
    Returns:
      list of lowest common ancestor commit_ids
    """

    def lookup_parents(commit_id):
        return object_store[commit_id].parents

    if not commit_ids:
        return []
    if len(commit_ids) <= 2:
        return find_merge_base(object_store, commit_ids)
    lcas = [commit_ids[0]]
    others = commit_ids[1:]
    for cmt in others:
        next_lcas = []
        for ca in lcas:
            res = _find_lcas(lookup_parents, cmt, [ca])
            next_lcas.extend(res)
        lcas = next_lcas[:]
    return lcas
