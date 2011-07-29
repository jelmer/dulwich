# walk.py -- General implementation of walking commits and their contents.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# or (at your option) any later version of the License.
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

"""General implementation of walking commits and their contents."""

import heapq
import itertools

ORDER_DATE = 'date'


class Walker(object):
    """Object for performing a walk of commits in a store.

    Walker objects are initialized with a store and other options and can then
    be treated as iterators of Commit objects.
    """

    def __init__(self, store, include, exclude=None, order=ORDER_DATE,
                 reverse=False):
        """Constructor.

        :param store: ObjectStore instance for looking up objects.
        :param include: Iterable of SHAs of commits to include along with their
            ancestors.
        :param exclude: Iterable of SHAs of commits to exclude along with their
            ancestors, overriding includes.
        :param order: ORDER_* constant specifying the order of results. Anything
            other than ORDER_DATE may result in O(n) memory usage.
        :param reverse: If True, reverse the order of output, requiring O(n)
            memory.
        """
        self._store = store

        if order not in (ORDER_DATE,):
            raise ValueError('Unknown walk order %s' % order)
        self._order = order
        self._reverse = reverse

        exclude = exclude or []
        self._excluded = set(exclude)
        self._pq = []
        self._pq_set = set()
        self._done = set()

        for commit_id in itertools.chain(include, exclude):
            self._push(store[commit_id])

    def _push(self, commit):
        sha = commit.id
        if sha not in self._pq_set and sha not in self._done:
            heapq.heappush(self._pq, (-commit.commit_time, commit))
            self._pq_set.add(sha)

    def _pop(self):
        while self._pq:
            _, commit = heapq.heappop(self._pq)
            sha = commit.id
            self._pq_set.remove(sha)
            if sha in self._done:
                continue

            is_excluded = sha in self._excluded
            if is_excluded:
                self._excluded.update(commit.parents)

            self._done.add(commit.id)
            for parent_id in commit.parents:
                self._push(self._store[parent_id])

            if not is_excluded:
                return commit
        return None

    def _next(self):
        return self._pop()

    def __iter__(self):
        results = iter(self._next, None)
        if self._reverse:
            results = reversed(list(results))
        return iter(results)
