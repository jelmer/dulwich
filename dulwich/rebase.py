# rebase.py -- Git rebase implementation
# Copyright (C) 2025 Dulwich contributors
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

"""Git rebase implementation."""

from typing import Optional, Protocol

from dulwich.graph import find_merge_base
from dulwich.merge import three_way_merge
from dulwich.objects import Commit
from dulwich.objectspec import parse_commit
from dulwich.repo import Repo


class RebaseError(Exception):
    """Base class for rebase errors."""


class RebaseConflict(RebaseError):
    """Raised when a rebase conflict occurs."""

    def __init__(self, conflicted_files: list[bytes]):
        self.conflicted_files = conflicted_files
        super().__init__(
            f"Conflicts in: {', '.join(f.decode('utf-8', 'replace') for f in conflicted_files)}"
        )


class RebaseAbort(RebaseError):
    """Raised when rebase is aborted."""


class RebaseStateManager(Protocol):
    """Protocol for managing rebase state."""

    def save(
        self,
        original_head: Optional[bytes],
        rebasing_branch: Optional[bytes],
        onto: Optional[bytes],
        todo: list[Commit],
        done: list[Commit],
    ) -> None:
        """Save rebase state."""
        ...

    def load(
        self,
    ) -> tuple[
        Optional[bytes],  # original_head
        Optional[bytes],  # rebasing_branch
        Optional[bytes],  # onto
        list[Commit],  # todo
        list[Commit],  # done
    ]:
        """Load rebase state."""
        ...

    def clean(self) -> None:
        """Clean up rebase state."""
        ...

    def exists(self) -> bool:
        """Check if rebase state exists."""
        ...


class DiskRebaseStateManager:
    """Manages rebase state on disk using same files as C Git."""

    def __init__(self, path: str) -> None:
        """Initialize disk rebase state manager.

        Args:
            path: Path to the rebase-merge directory
        """
        self.path = path

    def save(
        self,
        original_head: Optional[bytes],
        rebasing_branch: Optional[bytes],
        onto: Optional[bytes],
        todo: list[Commit],
        done: list[Commit],
    ) -> None:
        """Save rebase state to disk."""
        import os

        # Ensure the directory exists
        os.makedirs(self.path, exist_ok=True)

        # Store the original HEAD ref (e.g. "refs/heads/feature")
        if original_head:
            self._write_file("orig-head", original_head)

        # Store the branch name being rebased
        if rebasing_branch:
            self._write_file("head-name", rebasing_branch)

        # Store the commit we're rebasing onto
        if onto:
            self._write_file("onto", onto)

        # Track progress
        if todo:
            # Store the current commit being rebased (same as C Git)
            current_commit = todo[0]
            self._write_file("stopped-sha", current_commit.id)

            # Store progress counters
            msgnum = len(done) + 1  # Current commit number (1-based)
            end = len(done) + len(todo)  # Total number of commits
            self._write_file("msgnum", str(msgnum).encode())
            self._write_file("end", str(end).encode())

        # TODO: Add support for writing git-rebase-todo for interactive rebase

    def _write_file(self, name: str, content: bytes) -> None:
        """Write content to a file in the rebase directory."""
        import os

        with open(os.path.join(self.path, name), "wb") as f:
            f.write(content)

    def load(
        self,
    ) -> tuple[
        Optional[bytes],
        Optional[bytes],
        Optional[bytes],
        list[Commit],
        list[Commit],
    ]:
        """Load rebase state from disk."""
        original_head = None
        rebasing_branch = None
        onto = None
        todo: list[Commit] = []
        done: list[Commit] = []

        # Load rebase state files
        original_head = self._read_file("orig-head")
        rebasing_branch = self._read_file("head-name")
        onto = self._read_file("onto")

        # TODO: Load todo list and done list for resuming rebase

        return original_head, rebasing_branch, onto, todo, done

    def _read_file(self, name: str) -> Optional[bytes]:
        """Read content from a file in the rebase directory."""
        import os

        try:
            with open(os.path.join(self.path, name), "rb") as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    def clean(self) -> None:
        """Clean up rebase state files."""
        import shutil

        try:
            shutil.rmtree(self.path)
        except FileNotFoundError:
            # Directory doesn't exist, that's ok
            pass

    def exists(self) -> bool:
        """Check if rebase state exists."""
        import os

        return os.path.exists(os.path.join(self.path, "orig-head"))


class MemoryRebaseStateManager:
    """Manages rebase state in memory for MemoryRepo."""

    def __init__(self, repo: Repo) -> None:
        self.repo = repo
        self._state: Optional[dict] = None

    def save(
        self,
        original_head: Optional[bytes],
        rebasing_branch: Optional[bytes],
        onto: Optional[bytes],
        todo: list[Commit],
        done: list[Commit],
    ) -> None:
        """Save rebase state in memory."""
        self._state = {
            "original_head": original_head,
            "rebasing_branch": rebasing_branch,
            "onto": onto,
            "todo": todo[:],  # Copy the lists
            "done": done[:],
        }

    def load(
        self,
    ) -> tuple[
        Optional[bytes],
        Optional[bytes],
        Optional[bytes],
        list[Commit],
        list[Commit],
    ]:
        """Load rebase state from memory."""
        if self._state is None:
            return None, None, None, [], []

        return (
            self._state["original_head"],
            self._state["rebasing_branch"],
            self._state["onto"],
            self._state["todo"][:],  # Return copies
            self._state["done"][:],
        )

    def clean(self) -> None:
        """Clean up rebase state."""
        self._state = None

    def exists(self) -> bool:
        """Check if rebase state exists."""
        return self._state is not None


class Rebaser:
    """Handles git rebase operations."""

    def __init__(self, repo: Repo):
        """Initialize rebaser.

        Args:
            repo: Repository to perform rebase in
        """
        self.repo = repo
        self.object_store = repo.object_store
        self._state_manager = repo.get_rebase_state_manager()

        # Initialize state
        self._original_head: Optional[bytes] = None
        self._onto = None
        self._todo: list[Commit] = []
        self._done: list[Commit] = []
        self._rebasing_branch: Optional[bytes] = None

        # Load any existing rebase state
        self._load_rebase_state()

    def _get_commits_to_rebase(
        self, upstream: bytes, branch: Optional[bytes] = None
    ) -> list[Commit]:
        """Get list of commits to rebase.

        Args:
            upstream: Upstream commit/branch to rebase onto
            branch: Branch to rebase (defaults to current branch)

        Returns:
            List of commits to rebase in chronological order
        """
        # Get the branch commit
        if branch is None:
            # Use current HEAD
            head_ref, head_sha = self.repo.refs.follow(b"HEAD")
            branch_commit = self.repo[head_sha]
        else:
            # Parse the branch reference
            branch_commit = parse_commit(self.repo, branch)

        # Get upstream commit
        upstream_commit = parse_commit(self.repo, upstream)

        # If already up to date, return empty list
        if branch_commit.id == upstream_commit.id:
            return []

        merge_bases = find_merge_base(self.repo, [branch_commit.id, upstream_commit.id])
        if not merge_bases:
            raise RebaseError("No common ancestor found")

        merge_base = merge_bases[0]

        # Get commits between merge base and branch head
        commits = []
        current = branch_commit
        while current.id != merge_base:
            commits.append(current)
            if not current.parents:
                break
            current = self.repo[current.parents[0]]

        # Return in chronological order (oldest first)
        return list(reversed(commits))

    def _cherry_pick(self, commit: Commit, onto: bytes) -> tuple[bytes, list[bytes]]:
        """Cherry-pick a commit onto another commit.

        Args:
            commit: Commit to cherry-pick
            onto: SHA of commit to cherry-pick onto

        Returns:
            Tuple of (new_commit_sha, list_of_conflicted_files)
        """
        # Get the parent of the commit being cherry-picked
        if not commit.parents:
            raise RebaseError(f"Cannot cherry-pick root commit {commit.id}")

        parent = self.repo[commit.parents[0]]
        onto_commit = self.repo[onto]

        # Perform three-way merge
        merged_tree, conflicts = three_way_merge(
            self.object_store, parent, onto_commit, commit
        )

        if conflicts:
            # Store merge state for conflict resolution
            self.repo._put_named_file("rebase-merge/stopped-sha", commit.id)
            return commit.id, conflicts

        # Create new commit
        new_commit = Commit()
        new_commit.tree = merged_tree.id
        new_commit.parents = [onto]
        new_commit.author = commit.author
        new_commit.author_time = commit.author_time
        new_commit.author_timezone = commit.author_timezone
        new_commit.committer = commit.committer
        new_commit.commit_time = commit.commit_time
        new_commit.commit_timezone = commit.commit_timezone
        new_commit.message = commit.message
        new_commit.encoding = commit.encoding

        self.object_store.add_object(merged_tree)
        self.object_store.add_object(new_commit)

        return new_commit.id, []

    def start(
        self,
        upstream: bytes,
        onto: Optional[bytes] = None,
        branch: Optional[bytes] = None,
    ) -> list[Commit]:
        """Start a rebase.

        Args:
            upstream: Upstream branch/commit to rebase onto
            onto: Specific commit to rebase onto (defaults to upstream)
            branch: Branch to rebase (defaults to current branch)

        Returns:
            List of commits that will be rebased
        """
        # Save original HEAD
        self._original_head = self.repo.refs.read_ref(b"HEAD")

        # Save which branch we're rebasing (for later update)
        if branch is not None:
            # Parse the branch ref
            if branch.startswith(b"refs/heads/"):
                self._rebasing_branch = branch
            else:
                # Assume it's a branch name
                self._rebasing_branch = b"refs/heads/" + branch
        else:
            # Use current branch
            if self._original_head is not None and self._original_head.startswith(
                b"ref: "
            ):
                self._rebasing_branch = self._original_head[5:]
            else:
                self._rebasing_branch = None

        # Determine onto commit
        if onto is None:
            onto = upstream
        # Parse the onto commit
        onto_commit = parse_commit(self.repo, onto)
        self._onto = onto_commit.id

        # Get commits to rebase
        commits = self._get_commits_to_rebase(upstream, branch)
        self._todo = commits
        self._done = []

        # Store rebase state
        self._save_rebase_state()

        return commits

    def continue_(self) -> Optional[tuple[bytes, list[bytes]]]:
        """Continue an in-progress rebase.

        Returns:
            None if rebase is complete, or tuple of (commit_sha, conflicts) for next commit
        """
        if not self._todo:
            self._finish_rebase()
            return None

        # Get next commit to rebase
        commit = self._todo.pop(0)

        # Determine what to rebase onto
        if self._done:
            onto = self._done[-1].id
        else:
            onto = self._onto

        # Cherry-pick the commit
        new_sha, conflicts = self._cherry_pick(commit, onto)

        if new_sha:
            # Success - add to done list
            self._done.append(self.repo[new_sha])
            self._save_rebase_state()

            # Continue with next commit if any
            if self._todo:
                return self.continue_()
            else:
                self._finish_rebase()
                return None
        else:
            # Conflicts - save state and return
            self._save_rebase_state()
            return (commit.id, conflicts)

    def is_in_progress(self) -> bool:
        """Check if a rebase is currently in progress."""
        return self._state_manager.exists()

    def abort(self) -> None:
        """Abort an in-progress rebase and restore original state."""
        if not self.is_in_progress():
            raise RebaseError("No rebase in progress")

        # Restore original HEAD
        self.repo.refs[b"HEAD"] = self._original_head

        # Clean up rebase state
        self._clean_rebase_state()

        # Reset instance state
        self._original_head = None
        self._onto = None
        self._todo = []
        self._done = []

    def _finish_rebase(self) -> None:
        """Finish rebase by updating HEAD and cleaning up."""
        if not self._done:
            # No commits were rebased
            return

        # Update HEAD to point to last rebased commit
        last_commit = self._done[-1]

        # Update the branch we're rebasing
        if hasattr(self, "_rebasing_branch") and self._rebasing_branch:
            self.repo.refs[self._rebasing_branch] = last_commit.id
            # If HEAD was pointing to this branch, it will follow automatically
        else:
            # If we don't know which branch, check current HEAD
            head_ref = self.repo.refs[b"HEAD"]
            if head_ref.startswith(b"ref: "):
                branch_ref = head_ref[5:]
                self.repo.refs[branch_ref] = last_commit.id
            else:
                # Detached HEAD
                self.repo.refs[b"HEAD"] = last_commit.id

        # Clean up rebase state
        self._clean_rebase_state()

        # Reset instance state but keep _done for caller
        self._original_head = None
        self._onto = None
        self._todo = []

    def _save_rebase_state(self) -> None:
        """Save rebase state to allow resuming."""
        self._state_manager.save(
            self._original_head,
            self._rebasing_branch,
            self._onto,
            self._todo,
            self._done,
        )

    def _load_rebase_state(self) -> None:
        """Load existing rebase state if present."""
        (
            self._original_head,
            self._rebasing_branch,
            self._onto,
            self._todo,
            self._done,
        ) = self._state_manager.load()

    def _clean_rebase_state(self) -> None:
        """Clean up rebase state files."""
        self._state_manager.clean()


def rebase(
    repo: Repo,
    upstream: bytes,
    onto: Optional[bytes] = None,
    branch: Optional[bytes] = None,
) -> list[bytes]:
    """Perform a git rebase operation.

    Args:
        repo: Repository to rebase in
        upstream: Upstream branch/commit to rebase onto
        onto: Specific commit to rebase onto (defaults to upstream)
        branch: Branch to rebase (defaults to current branch)

    Returns:
        List of new commit SHAs created by rebase

    Raises:
        RebaseConflict: If conflicts occur during rebase
        RebaseError: For other rebase errors
    """
    rebaser = Rebaser(repo)

    # Start rebase
    rebaser.start(upstream, onto, branch)

    # Continue rebase
    result = rebaser.continue_()
    if result is not None:
        # Conflicts
        raise RebaseConflict(result[1])

    # Return the SHAs of the rebased commits
    return [c.id for c in rebaser._done]
