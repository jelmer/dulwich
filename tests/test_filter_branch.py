# test_filter_branch.py -- Tests for filter_branch module
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for dulwich.filter_branch."""

import unittest

from dulwich.filter_branch import CommitFilter, filter_refs
from dulwich.object_store import MemoryObjectStore
from dulwich.objects import Commit, Tree
from dulwich.refs import DictRefsContainer


class CommitFilterTests(unittest.TestCase):
    """Tests for CommitFilter class."""

    def setUp(self):
        self.store = MemoryObjectStore()
        self.refs = DictRefsContainer({})

        # Create test commits
        tree = Tree()
        self.store.add_object(tree)

        self.c1 = Commit()
        self.c1.tree = tree.id
        self.c1.author = self.c1.committer = b"Test User <test@example.com>"
        self.c1.author_time = self.c1.commit_time = 1000
        self.c1.author_timezone = self.c1.commit_timezone = 0
        self.c1.message = b"First commit"
        self.store.add_object(self.c1)

        self.c2 = Commit()
        self.c2.tree = tree.id
        self.c2.parents = [self.c1.id]
        self.c2.author = self.c2.committer = b"Test User <test@example.com>"
        self.c2.author_time = self.c2.commit_time = 2000
        self.c2.author_timezone = self.c2.commit_timezone = 0
        self.c2.message = b"Second commit"
        self.store.add_object(self.c2)

    def test_filter_author(self):
        """Test filtering author."""

        def new_author(old):
            return b"New Author <new@example.com>"

        filter = CommitFilter(self.store, filter_author=new_author)
        new_sha = filter.process_commit(self.c2.id)

        self.assertNotEqual(new_sha, self.c2.id)
        new_commit = self.store[new_sha]
        self.assertEqual(new_commit.author, b"New Author <new@example.com>")
        self.assertEqual(new_commit.committer, self.c2.committer)

    def test_filter_message(self):
        """Test filtering message."""

        def prefix_message(msg):
            return b"[PREFIX] " + msg

        filter = CommitFilter(self.store, filter_message=prefix_message)
        new_sha = filter.process_commit(self.c2.id)

        self.assertNotEqual(new_sha, self.c2.id)
        new_commit = self.store[new_sha]
        self.assertEqual(new_commit.message, b"[PREFIX] Second commit")

    def test_filter_fn(self):
        """Test custom filter function."""

        def custom_filter(commit):
            return {
                "author": b"Custom <custom@example.com>",
                "message": b"Custom: " + commit.message,
            }

        filter = CommitFilter(self.store, filter_fn=custom_filter)
        new_sha = filter.process_commit(self.c2.id)

        self.assertNotEqual(new_sha, self.c2.id)
        new_commit = self.store[new_sha]
        self.assertEqual(new_commit.author, b"Custom <custom@example.com>")
        self.assertEqual(new_commit.message, b"Custom: Second commit")

    def test_no_changes(self):
        """Test commit with no changes."""
        filter = CommitFilter(self.store)
        new_sha = filter.process_commit(self.c2.id)

        self.assertEqual(new_sha, self.c2.id)

    def test_parent_rewriting(self):
        """Test that parent commits are rewritten."""

        def new_author(old):
            return b"New Author <new@example.com>"

        filter = CommitFilter(self.store, filter_author=new_author)
        new_sha = filter.process_commit(self.c2.id)

        # Check that parent was also rewritten
        new_commit = self.store[new_sha]
        self.assertEqual(len(new_commit.parents), 1)
        new_parent_sha = new_commit.parents[0]
        self.assertNotEqual(new_parent_sha, self.c1.id)

        new_parent = self.store[new_parent_sha]
        self.assertEqual(new_parent.author, b"New Author <new@example.com>")


class FilterRefsTests(unittest.TestCase):
    """Tests for filter_refs function."""

    def setUp(self):
        self.store = MemoryObjectStore()
        self.refs = DictRefsContainer({})

        # Create test commits
        tree = Tree()
        self.store.add_object(tree)

        c1 = Commit()
        c1.tree = tree.id
        c1.author = c1.committer = b"Test User <test@example.com>"
        c1.author_time = c1.commit_time = 1000
        c1.author_timezone = c1.commit_timezone = 0
        c1.message = b"First commit"
        self.store.add_object(c1)

        self.refs[b"refs/heads/master"] = c1.id
        self.c1_id = c1.id

    def test_filter_refs_basic(self):
        """Test basic ref filtering."""

        def new_author(old):
            return b"New Author <new@example.com>"

        filter = CommitFilter(self.store, filter_author=new_author)
        result = filter_refs(
            self.refs,
            self.store,
            [b"refs/heads/master"],
            filter,
        )

        # Check mapping
        self.assertEqual(len(result), 1)
        self.assertIn(self.c1_id, result)
        self.assertNotEqual(result[self.c1_id], self.c1_id)

        # Check ref was updated
        new_sha = self.refs[b"refs/heads/master"]
        self.assertEqual(new_sha, result[self.c1_id])

        # Check original was saved
        original_sha = self.refs[b"refs/original/refs/heads/master"]
        self.assertEqual(original_sha, self.c1_id)

    def test_filter_refs_already_filtered(self):
        """Test error when refs already filtered."""
        # Set up an "already filtered" state
        self.refs[b"refs/original/refs/heads/master"] = b"0" * 40

        filter = CommitFilter(self.store)
        with self.assertRaises(ValueError) as cm:
            filter_refs(
                self.refs,
                self.store,
                [b"refs/heads/master"],
                filter,
            )
        self.assertIn("filtered already", str(cm.exception))

    def test_filter_refs_force(self):
        """Test force filtering."""
        # Set up an "already filtered" state
        self.refs[b"refs/original/refs/heads/master"] = b"0" * 40

        filter = CommitFilter(self.store)
        # Should not raise with force=True
        result = filter_refs(
            self.refs,
            self.store,
            [b"refs/heads/master"],
            filter,
            force=True,
        )
        self.assertEqual(len(result), 1)
