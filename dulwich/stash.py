# stash.py
# Copyright (C) 2018 Jelmer Vernooij <jelmer@samba.org>
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

"""Stash handling."""

from __future__ import absolute_import

import errno
import os

from dulwich.file import GitFile
from dulwich.index import (
    commit_tree,
    iter_fresh_objects,
    )
from dulwich.reflog import read_reflog


DEFAULT_STASH_REF = b"refs/stash"


class Stash(object):
    """A Git stash.

    Note that this doesn't currently update the working tree.
    """

    def __init__(self, repo, ref=DEFAULT_STASH_REF):
        self._ref = ref
        self._repo = repo

    def stashes(self):
        reflog_path = os.path.join(
            self._repo.commondir(), 'logs', self._ref)
        try:
            with GitFile(reflog_path, 'rb') as f:
                return reversed(list(read_reflog(f)))
        except EnvironmentError as e:
            if e.errno == errno.ENOENT:
                return []
            raise

    @classmethod
    def from_repo(cls, repo):
        """Create a new stash from a Repo object."""
        return cls(repo)

    def drop(self, index):
        """Drop entry with specified index."""
        raise NotImplementedError(self.drop)

    def pop(self, index):
        raise NotImplementedError(self.drop)

    def push(self, committer=None, author=None, message=None):
        """Create a new stash.

        :param committer: Optional committer name to use
        :param author: Optional author name to use
        :param message: Optional commit message
        """
        # First, create the index commit.
        commit_kwargs = {}
        if committer is not None:
            commit_kwargs['committer'] = committer
        if author is not None:
            commit_kwargs['author'] = author

        index = self._repo.open_index()
        index_tree_id = index.commit(self._repo.object_store)
        index_commit_id = self._repo.do_commit(
            ref=None, tree=index_tree_id,
            message=b"Index stash",
            merge_heads=[self._repo.head()],
            **commit_kwargs)

        # Then, the working tree one.
        stash_tree_id = commit_tree(
                self._repo.object_store,
                iter_fresh_objects(
                    index, self._repo.path,
                    object_store=self._repo.object_store))

        if message is None:
            message = b"A stash on " + self._repo.head()

        # TODO(jelmer): Just pass parents into do_commit()?
        self._repo.refs[self._ref] = self._repo.head()

        cid = self._repo.do_commit(
            ref=self._ref, tree=stash_tree_id,
            message=message,
            merge_heads=[index_commit_id],
            **commit_kwargs)

        return cid

    def __getitem__(self, index):
        return self._stashes()[index]

    def __len__(self):
        return len(self._stashes())
