#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim:ts=4:sw=4:softtabstop=4:smarttab:expandtab
"""
Implementation of merge-base following the approach of git
"""
# Copyright (c) 2020 Kevin B. Hendricks, Stratford Ontario Canada
#
# Available under the MIT License

import sys
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


def find_merge_base(r, commit_ids):
    """ find lowest common ancestors of commit_ids[0] and *any* of commits_ids[1:]
       ARGS:
          r: Repo object
          commit_ids:  list of commit ids
       Returns
          list of lowest common ancestor commit_ids
    """

    def lookup_parents(commit_id):
        return r.object_store[commit_id].parents

    if not commit_ids:
        return []
    c1 = commit_ids[0]
    if not len(commit_ids) > 1:
        return [c1]
    c2s = commit_ids[1:]
    if c1 in c2s:
        return [c1]
    return _find_lcas(lookup_parents, c1, c2s)


def find_octopus_base(r, commit_ids):
    """ find lowest common ancestors of *all* provided commit_ids
       ARGS:
          r: Repo object
          commit_ids:  list of commit ids
       Returns
          list of lowest common ancestor commit_ids
    """

    def lookup_parents(commit_id):
        return r.object_store[commit_id].parents

    if not commit_ids:
        return []
    if len(commit_ids) <= 2:
        return find_merge_base(r, commit_ids)
    lcas = [commit_ids[0]]
    others = commit_ids[1:]
    for cmt in others:
        next_lcas = []
        for ca in lcas:
            res = _find_lcas(lookup_parents, cmt, [ca])
            next_lcas.extend(res)
        lcas = next_lcas[:]
    return lcas


def test():

    all_tests_passed = True

    parents = {}

    def lookup_parents(commit_id):
        return parents.get(commit_id, [])

    def run_test(dag, inputs, expected):
        nonlocal parents
        parents = dag
        c1 = inputs[0]
        c2s = inputs[1:]
        res = _find_lcas(lookup_parents, c1, c2s)
        return set(res) == expected

    # two lowest common ancestors
    test1 = {
        '5': ['1', '2'],
        '4': ['3', '1'],
        '3': ['2'],
        '2': ['0'],
        '1': [],
        '0': []
    }
    test_passed = run_test(test1, ['4', '5'], set(['1', '2']))
    print('Test 1: Multiple LCA ', test_passed)
    all_tests_passed = all_tests_passed and test_passed

    # no common ancestor
    test2 = {
        '4': ['2'],
        '3': ['1'],
        '2': [],
        '1': ['0'],
        '0': [],
    }
    test_passed = run_test(test2, ['4', '3'], set([]))
    print('Test 2: No Common Ancestor ', test_passed)
    all_tests_passed = all_tests_passed and test_passed

    # ancestor
    test3 = {
        'G': ['D', 'F'],
        'F': ['E'],
        'D': ['C'],
        'C': ['B'],
        'E': ['B'],
        'B': ['A'],
        'A': []
    }
    test_passed = run_test(test3, ['D', 'C'], set(['C']))
    print('Test 3: Ancestor ', test_passed)
    all_tests_passed = all_tests_passed and test_passed

    # parent
    test4 = {
        'G': ['D', 'F'],
        'F': ['E'],
        'D': ['C'],
        'C': ['B'],
        'E': ['B'],
        'B': ['A'],
        'A': []
    }
    test_passed = run_test(test4, ['G', 'D'], set(['D']))
    print('Test 4: Direct Parent ', test_passed)
    all_tests_passed = all_tests_passed and test_passed

    # Another cross over
    test5 = {
        'G': ['D', 'F'],
        'F': ['E', 'C'],
        'D': ['C', 'E'],
        'C': ['B'],
        'E': ['B'],
        'B': ['A'],
        'A': []
    }
    test_passed = run_test(test5, ['D', 'F'], set(['E', 'C']))
    print('Test 5: Cross Over ', test_passed)
    all_tests_passed = all_tests_passed and test_passed

    # three way merge commit straight from git docs
    test6 = {
        'C': ['C1'],
        'C1': ['C2'],
        'C2': ['C3'],
        'C3': ['C4'],
        'C4': ['2'],
        'B': ['B1'],
        'B1': ['B2'],
        'B2': ['B3'],
        'B3': ['1'],
        'A': ['A1'],
        'A1': ['A2'],
        'A2': ['A3'],
        'A3': ['1'],
        '1': ['2'],
        '2': [],
    }
    # assumes a theoretical merge M exists that merges B and C first
    # which actually means find the first LCA from either of B OR C with A
    test_passed = run_test(test6, ['A', 'B', 'C'], set(['1']))
    all_tests_passed = all_tests_passed and test_passed
    print('Test 6: LCA of 3 commits ', test_passed)

    # octopus algorithm test
    # test straight from git docs of A, B, and C
    # but this time use octopus to find lcas of A, B, and C simultaneously
    test7 = {
        'C': ['C1'],
        'C1': ['C2'],
        'C2': ['C3'],
        'C3': ['C4'],
        'C4': ['2'],
        'B': ['B1'],
        'B1': ['B2'],
        'B2': ['B3'],
        'B3': ['1'],
        'A': ['A1'],
        'A1': ['A2'],
        'A2': ['A3'],
        'A3': ['1'],
        '1': ['2'],
        '2': [],
    }
    parents = test7
    lcas = ['A']
    others = ['B', 'C']
    for cmt in others:
        next_lcas = []
        for ca in lcas:
            res = _find_lcas(lookup_parents, cmt, [ca])
            next_lcas.extend(res)
        lcas = next_lcas[:]
    test_passed = set(lcas) == set(['2'])
    all_tests_passed = all_tests_passed and test_passed
    print('Test 7: Octopus LCA of 3 commits ', test_passed)

    if all_tests_passed:
        print('All Tests Succesfful')
    return 0


if __name__ == '__main__':
    sys.exit(test())
