# eventlet.py -- Utility module for querying an ObjectStore with eventlet
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Fabien Boucher <fabien.boucher@enovance.com>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
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

import eventlet

from dulwich.objects import (
    Commit,
    Tag,
    )
from dulwich.object_store import (
    MissingObjectFinder,
    _collect_filetree_revs,
    ObjectStoreIterator,
    )


def _split_commits_and_tags(obj_store, lst,
                            ignore_unknown=False, pool=None):
    """Split object id list into two list with commit SHA1s and tag SHA1s.

    Same implementation as object_store._split_commits_and_tags
    except we use eventlet to parallelize object retrieval.
    """
    commits = set()
    tags = set()

    def find_commit_type(sha):
        try:
            o = obj_store[sha]
        except KeyError:
            if not ignore_unknown:
                raise
        else:
            if isinstance(o, Commit):
                commits.add(sha)
            elif isinstance(o, Tag):
                tags.add(sha)
                commits.add(o.object[1])
            else:
                raise KeyError('Not a commit or a tag: %s' % sha)
    for _ in pool.imap(find_commit_type, lst):
        pass
    return (commits, tags)


class EventletMissingObjectFinder(MissingObjectFinder):
    """Find the objects missing from another object store.

    Same implementation as object_store.MissingObjectFinder
    except we use eventlet to parallelize object retrieval.
    """
    def __init__(self, object_store, haves, wants, concurrency=1,
                 progress=None, get_tagged=None):

        def collect_tree_sha(sha):
            self.sha_done.add(sha)
            cmt = object_store[sha]
            _collect_filetree_revs(object_store, cmt.tree, self.sha_done)

        self.object_store = object_store
        pool = eventlet.GreenPool(size=concurrency)

        have_commits, have_tags = \
            _split_commits_and_tags(object_store, haves,
                                    True, pool)
        want_commits, want_tags = \
            _split_commits_and_tags(object_store, wants,
                                    False, pool)
        all_ancestors = object_store._collect_ancestors(have_commits)[0]
        missing_commits, common_commits = \
            object_store._collect_ancestors(want_commits, all_ancestors)

        self.sha_done = set()
        for _ in pool.imap(collect_tree_sha, common_commits):
            pass
        for t in have_tags:
            self.sha_done.add(t)
        missing_tags = want_tags.difference(have_tags)
        wants = missing_commits.union(missing_tags)
        self.objects_to_send = set([(w, None, False) for w in wants])
        if progress is None:
            self.progress = lambda x: None
        else:
            self.progress = progress
        self._tagged = get_tagged and get_tagged() or {}


class EventletObjectStoreIterator(ObjectStoreIterator):
    """ObjectIterator that works on top of an ObjectStore.

    Same implementation as object_store.ObjectStoreIterator
    except we use eventlet to parallelize object retrieval.
    """
    def __init__(self, store, shas, finder, concurrency=1):
        self.finder = finder
        self.pool = eventlet.GreenPool(size=concurrency)
        super(EventletObjectStoreIterator, self).__init__(store, shas)

    def retrieve(self, args):
        sha, path = args
        return self.store[sha], path

    def __iter__(self):
        for sha, path in self.pool.imap(self.retrieve,
                                        self.itershas()):
            yield sha, path

    def __len__(self):
        if len(self._shas) > 0:
            return len(self._shas)
        pile = eventlet.GreenPile(size_or_pool=self.pool)
        while len(self.finder.objects_to_send):
            for _ in xrange(0, len(self.finder.objects_to_send)):
                pile.spawn(self.finder.next)
            for ret in pile:
                if ret is not None:
                    self._shas.append(ret)
        return len(self._shas)
