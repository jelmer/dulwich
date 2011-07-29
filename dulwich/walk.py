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


class WalkEntry(object):
    """Object encapsulating a single result from a walk."""

    def __init__(self, commit):
        self.commit = commit

    def __eq__(self, other):
        return isinstance(other, WalkEntry) and self.commit == other.commit


class Walker(object):
    """Object for performing a walk of commits in a store.

    Walker objects are initialized with a store and other options and can then
    be treated as iterators of Commit objects.
    """

    def __init__(self, store, include, exclude=None, order=ORDER_DATE,
                 reverse=False, max_entries=None):
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
        :param max_entries: The maximum number of entries to yield, or None for
            no limit.
        """
        self._store = store

        if order not in (ORDER_DATE,):
            raise ValueError('Unknown walk order %s' % order)
        self._order = order
        self._reverse = reverse
        self._max_entries = max_entries

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
        limit = self._max_entries
        if limit is not None and len(self._done) >= limit:
            return None
        commit = self._pop()
        if commit is None:
            return None
        return WalkEntry(commit)

    def __iter__(self):
        results = iter(self._next, None)
        if self._reverse:
            results = reversed(list(results))
        return iter(results)
