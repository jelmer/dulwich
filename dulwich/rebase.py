# rebase.py -- Git rebase implementation
# Copyright (C) 2025 Dulwich contributors
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
#

"""Git rebase implementation."""

import os
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional, Protocol, TypedDict

from dulwich.graph import find_merge_base
from dulwich.merge import three_way_merge
from dulwich.objects import Commit
from dulwich.objectspec import parse_commit
from dulwich.refs import local_branch_name
from dulwich.repo import BaseRepo, Repo


class RebaseError(Exception):
    """Base class for rebase errors."""


class RebaseConflict(RebaseError):
    """Raised when a rebase conflict occurs."""

    def __init__(self, conflicted_files: list[bytes]):
        """Initialize RebaseConflict.

        Args:
          conflicted_files: List of conflicted file paths
        """
        self.conflicted_files = conflicted_files
        super().__init__(
            f"Conflicts in: {', '.join(f.decode('utf-8', 'replace') for f in conflicted_files)}"
        )


class RebaseAbort(RebaseError):
    """Raised when rebase is aborted."""


class RebaseTodoCommand(Enum):
    """Enum for rebase todo commands."""

    PICK = "pick"
    REWORD = "reword"
    EDIT = "edit"
    SQUASH = "squash"
    FIXUP = "fixup"
    EXEC = "exec"
    BREAK = "break"
    DROP = "drop"
    LABEL = "label"
    RESET = "reset"
    MERGE = "merge"

    @classmethod
    def from_string(cls, s: str) -> "RebaseTodoCommand":
        """Parse a command from its string representation.

        Args:
            s: Command string (can be abbreviated)

        Returns:
            RebaseTodoCommand enum value

        Raises:
            ValueError: If command is not recognized
        """
        s = s.lower()
        # Support abbreviations
        abbreviations = {
            "p": cls.PICK,
            "r": cls.REWORD,
            "e": cls.EDIT,
            "s": cls.SQUASH,
            "f": cls.FIXUP,
            "x": cls.EXEC,
            "b": cls.BREAK,
            "d": cls.DROP,
            "l": cls.LABEL,
            "t": cls.RESET,
            "m": cls.MERGE,
        }

        if s in abbreviations:
            return abbreviations[s]

        # Try full command name
        try:
            return cls(s)
        except ValueError:
            raise ValueError(f"Unknown rebase command: {s}")


@dataclass
class RebaseTodoEntry:
    """Represents a single entry in a rebase todo list."""

    command: RebaseTodoCommand
    commit_sha: Optional[bytes] = None  # Store as hex string encoded as bytes
    short_message: Optional[str] = None
    arguments: Optional[str] = None

    def to_string(self, abbreviate: bool = False) -> str:
        """Convert to git-rebase-todo format string.

        Args:
            abbreviate: Use abbreviated command names

        Returns:
            String representation for todo file
        """
        if abbreviate:
            cmd_map = {
                RebaseTodoCommand.PICK: "p",
                RebaseTodoCommand.REWORD: "r",
                RebaseTodoCommand.EDIT: "e",
                RebaseTodoCommand.SQUASH: "s",
                RebaseTodoCommand.FIXUP: "f",
                RebaseTodoCommand.EXEC: "x",
                RebaseTodoCommand.BREAK: "b",
                RebaseTodoCommand.DROP: "d",
                RebaseTodoCommand.LABEL: "l",
                RebaseTodoCommand.RESET: "t",
                RebaseTodoCommand.MERGE: "m",
            }
            cmd = cmd_map.get(self.command, self.command.value)
        else:
            cmd = self.command.value

        parts = [cmd]

        if self.commit_sha:
            # Use short SHA (first 7 chars) like Git does
            parts.append(self.commit_sha.decode()[:7])

        if self.arguments:
            parts.append(self.arguments)
        elif self.short_message:
            parts.append(self.short_message)

        return " ".join(parts)

    @classmethod
    def from_string(cls, line: str) -> Optional["RebaseTodoEntry"]:
        """Parse a todo entry from a line.

        Args:
            line: Line from git-rebase-todo file

        Returns:
            RebaseTodoEntry or None if line is empty/comment
        """
        line = line.strip()

        # Skip empty lines and comments
        if not line or line.startswith("#"):
            return None

        parts = line.split(None, 2)
        if not parts:
            return None

        command_str = parts[0]
        try:
            command = RebaseTodoCommand.from_string(command_str)
        except ValueError:
            # Unknown command, skip
            return None

        commit_sha = None
        short_message = None
        arguments = None

        if command in (
            RebaseTodoCommand.EXEC,
            RebaseTodoCommand.LABEL,
            RebaseTodoCommand.RESET,
        ):
            # These commands take arguments instead of commit SHA
            if len(parts) > 1:
                arguments = " ".join(parts[1:])
        elif command == RebaseTodoCommand.BREAK:
            # Break has no arguments
            pass
        else:
            # Commands that operate on commits
            if len(parts) > 1:
                # Store SHA as hex string encoded as bytes
                commit_sha = parts[1].encode()

                # Parse commit message if present
                if len(parts) > 2:
                    short_message = parts[2]

        return cls(
            command=command,
            commit_sha=commit_sha,
            short_message=short_message,
            arguments=arguments,
        )


class RebaseTodo:
    """Manages the git-rebase-todo file for interactive rebase."""

    def __init__(self, entries: Optional[list[RebaseTodoEntry]] = None):
        """Initialize RebaseTodo.

        Args:
            entries: List of todo entries
        """
        self.entries = entries or []
        self.current_index = 0

    def add_entry(self, entry: RebaseTodoEntry) -> None:
        """Add an entry to the todo list."""
        self.entries.append(entry)

    def get_current(self) -> Optional[RebaseTodoEntry]:
        """Get the current todo entry."""
        if self.current_index < len(self.entries):
            return self.entries[self.current_index]
        return None

    def advance(self) -> None:
        """Move to the next todo entry."""
        self.current_index += 1

    def is_complete(self) -> bool:
        """Check if all entries have been processed."""
        return self.current_index >= len(self.entries)

    def to_string(self, include_comments: bool = True) -> str:
        """Convert to git-rebase-todo file format.

        Args:
            include_comments: Include helpful comments

        Returns:
            String content for todo file
        """
        lines = []

        # Add entries from current position onward
        for entry in self.entries[self.current_index :]:
            lines.append(entry.to_string())

        if include_comments:
            lines.append("")
            lines.append("# Rebase in progress")
            lines.append("#")
            lines.append("# Commands:")
            lines.append("# p, pick <commit> = use commit")
            lines.append(
                "# r, reword <commit> = use commit, but edit the commit message"
            )
            lines.append("# e, edit <commit> = use commit, but stop for amending")
            lines.append(
                "# s, squash <commit> = use commit, but meld into previous commit"
            )
            lines.append(
                "# f, fixup [-C | -c] <commit> = like 'squash' but keep only the previous"
            )
            lines.append(
                "#                    commit's log message, unless -C is used, in which case"
            )
            lines.append(
                "#                    keep only this commit's message; -c is same as -C but"
            )
            lines.append("#                    opens the editor")
            lines.append(
                "# x, exec <command> = run command (the rest of the line) using shell"
            )
            lines.append(
                "# b, break = stop here (continue rebase later with 'git rebase --continue')"
            )
            lines.append("# d, drop <commit> = remove commit")
            lines.append("# l, label <label> = label current HEAD with a name")
            lines.append("# t, reset <label> = reset HEAD to a label")
            lines.append("# m, merge [-C <commit> | -c <commit>] <label> [# <oneline>]")
            lines.append(
                "# .       create a merge commit using the original merge commit's"
            )
            lines.append(
                "# .       message (or the oneline, if no original merge commit was"
            )
            lines.append(
                "# .       specified); use -c <commit> to reword the commit message"
            )
            lines.append("#")
            lines.append(
                "# These lines can be re-ordered; they are executed from top to bottom."
            )
            lines.append("#")
            lines.append("# If you remove a line here THAT COMMIT WILL BE LOST.")
            lines.append("#")
            lines.append(
                "# However, if you remove everything, the rebase will be aborted."
            )
            lines.append("#")

        return "\n".join(lines)

    @classmethod
    def from_string(cls, content: str) -> "RebaseTodo":
        """Parse a git-rebase-todo file.

        Args:
            content: Content of todo file

        Returns:
            RebaseTodo instance
        """
        entries = []
        for line in content.splitlines():
            entry = RebaseTodoEntry.from_string(line)
            if entry:
                entries.append(entry)

        return cls(entries)

    @classmethod
    def from_commits(cls, commits: Sequence[Commit]) -> "RebaseTodo":
        """Create a todo list from a list of commits.

        Args:
            commits: List of commits to rebase (in chronological order)

        Returns:
            RebaseTodo instance with pick commands for each commit
        """
        entries = []
        for commit in commits:
            # Extract first line of commit message
            message = commit.message.decode("utf-8", errors="replace")
            short_message = message.split("\n")[0][:50]

            entry = RebaseTodoEntry(
                command=RebaseTodoCommand.PICK,
                commit_sha=commit.id,  # Already bytes
                short_message=short_message,
            )
            entries.append(entry)

        return cls(entries)


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

    def save_todo(self, todo: RebaseTodo) -> None:
        """Save interactive rebase todo list."""
        ...

    def load_todo(self) -> Optional[RebaseTodo]:
        """Load interactive rebase todo list."""
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

    def _write_file(self, name: str, content: bytes) -> None:
        """Write content to a file in the rebase directory."""
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

        return original_head, rebasing_branch, onto, todo, done

    def _read_file(self, name: str) -> Optional[bytes]:
        """Read content from a file in the rebase directory."""
        try:
            with open(os.path.join(self.path, name), "rb") as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    def clean(self) -> None:
        """Clean up rebase state files."""
        try:
            shutil.rmtree(self.path)
        except FileNotFoundError:
            # Directory doesn't exist, that's ok
            pass

    def exists(self) -> bool:
        """Check if rebase state exists."""
        return os.path.exists(os.path.join(self.path, "orig-head"))

    def save_todo(self, todo: RebaseTodo) -> None:
        """Save the interactive rebase todo list.

        Args:
            todo: The RebaseTodo object to save
        """
        todo_content = todo.to_string()
        self._write_file("git-rebase-todo", todo_content.encode("utf-8"))

    def load_todo(self) -> Optional[RebaseTodo]:
        """Load the interactive rebase todo list.

        Returns:
            RebaseTodo object or None if no todo file exists
        """
        todo_content = self._read_file("git-rebase-todo")
        if todo_content:
            todo_str = todo_content.decode("utf-8", errors="replace")
            return RebaseTodo.from_string(todo_str)
        return None


class RebaseState(TypedDict):
    """Type definition for rebase state."""

    original_head: Optional[bytes]
    rebasing_branch: Optional[bytes]
    onto: Optional[bytes]
    todo: list[Commit]
    done: list[Commit]


class MemoryRebaseStateManager:
    """Manages rebase state in memory for MemoryRepo."""

    def __init__(self, repo: BaseRepo) -> None:
        """Initialize MemoryRebaseStateManager.

        Args:
          repo: Repository instance
        """
        self.repo = repo
        self._state: Optional[RebaseState] = None
        self._todo: Optional[RebaseTodo] = None

    def save(
        self,
        original_head: Optional[bytes],
        rebasing_branch: Optional[bytes],
        onto: Optional[bytes],
        todo: list[Commit],
        done: list[Commit],
    ) -> None:
        """Save rebase state in memory."""
        self._state = RebaseState(
            original_head=original_head,
            rebasing_branch=rebasing_branch,
            onto=onto,
            todo=todo[:],  # Copy the lists
            done=done[:],
        )

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
        self._todo = None

    def exists(self) -> bool:
        """Check if rebase state exists."""
        return self._state is not None

    def save_todo(self, todo: RebaseTodo) -> None:
        """Save the interactive rebase todo list.

        Args:
            todo: The RebaseTodo object to save
        """
        self._todo = todo

    def load_todo(self) -> Optional[RebaseTodo]:
        """Load the interactive rebase todo list.

        Returns:
            RebaseTodo object or None if no todo exists
        """
        return self._todo


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
        self._onto: Optional[bytes] = None
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
            _head_ref, head_sha = self.repo.refs.follow(b"HEAD")
            if head_sha is None:
                raise ValueError("HEAD does not point to a valid commit")
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
            assert isinstance(current, Commit)
            commits.append(current)
            if not current.parents:
                break
            current = self.repo[current.parents[0]]

        # Return in chronological order (oldest first)
        return list(reversed(commits))

    def _cherry_pick(
        self, commit: Commit, onto: bytes
    ) -> tuple[Optional[bytes], list[bytes]]:
        """Cherry-pick a commit onto another commit.

        Args:
            commit: Commit to cherry-pick
            onto: SHA of commit to cherry-pick onto

        Returns:
            Tuple of (new_commit_sha, list_of_conflicted_files)
        """
        # Get the parent of the commit being cherry-picked
        if not commit.parents:
            raise RebaseError(f"Cannot cherry-pick root commit {commit.id!r}")

        parent = self.repo[commit.parents[0]]
        onto_commit = self.repo[onto]

        assert isinstance(parent, Commit)
        assert isinstance(onto_commit, Commit)

        # Perform three-way merge
        merged_tree, conflicts = three_way_merge(
            self.object_store, parent, onto_commit, commit
        )

        if conflicts:
            # Store merge state for conflict resolution
            self.repo._put_named_file("rebase-merge/stopped-sha", commit.id)
            return None, conflicts

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
                self._rebasing_branch = local_branch_name(branch)
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
            if self._onto is None:
                raise RebaseError("No onto commit set")
            onto = self._onto

        # Cherry-pick the commit
        new_sha, conflicts = self._cherry_pick(commit, onto)

        if new_sha:
            # Success - add to done list
            new_commit = self.repo[new_sha]
            assert isinstance(new_commit, Commit)
            self._done.append(new_commit)
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
        return bool(self._state_manager.exists())

    def abort(self) -> None:
        """Abort an in-progress rebase and restore original state."""
        if not self.is_in_progress():
            raise RebaseError("No rebase in progress")

        # Restore original HEAD
        if self._original_head is None:
            raise RebaseError("No original HEAD to restore")
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
        if self._rebasing_branch:
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


def start_interactive(
    repo: Repo,
    upstream: bytes,
    onto: Optional[bytes] = None,
    branch: Optional[bytes] = None,
    editor_callback: Optional[Callable[[bytes], bytes]] = None,
) -> RebaseTodo:
    """Start an interactive rebase.

    This function generates a todo list and optionally opens an editor for the user
    to modify it before starting the rebase.

    Args:
        repo: Repository to rebase in
        upstream: Upstream branch/commit to rebase onto
        onto: Specific commit to rebase onto (defaults to upstream)
        branch: Branch to rebase (defaults to current branch)
        editor_callback: Optional callback to edit todo content. If None, no editing.
                        Should take bytes and return bytes.

    Returns:
        RebaseTodo object with the (possibly edited) todo list

    Raises:
        RebaseError: If rebase cannot be started
    """
    rebaser = Rebaser(repo)

    # Get commits to rebase
    commits = rebaser.start(upstream, onto, branch)

    if not commits:
        raise RebaseError("No commits to rebase")

    # Generate todo list
    todo = RebaseTodo.from_commits(commits)

    # Save initial todo to disk
    state_manager = repo.get_rebase_state_manager()
    state_manager.save_todo(todo)

    # Let user edit todo if callback provided
    if editor_callback:
        todo_content = todo.to_string().encode("utf-8")
        edited_content = editor_callback(todo_content)

        # Parse edited todo
        edited_todo = RebaseTodo.from_string(
            edited_content.decode("utf-8", errors="replace")
        )

        # Check if user removed all entries (abort)
        if not edited_todo.entries:
            # User removed everything, abort
            rebaser.abort()
            raise RebaseAbort("Rebase aborted - empty todo list")

        todo = edited_todo

        # Save edited todo
        state_manager.save_todo(todo)

    return todo


def edit_todo(repo: Repo, editor_callback: Callable[[bytes], bytes]) -> RebaseTodo:
    """Edit the todo list of an in-progress interactive rebase.

    Args:
        repo: Repository with in-progress rebase
        editor_callback: Callback to edit todo content. Takes bytes, returns bytes.

    Returns:
        Updated RebaseTodo object

    Raises:
        RebaseError: If no rebase is in progress or todo cannot be loaded
    """
    state_manager = repo.get_rebase_state_manager()

    if not state_manager.exists():
        raise RebaseError("No rebase in progress")

    # Load current todo
    todo = state_manager.load_todo()
    if not todo:
        raise RebaseError("No interactive rebase in progress")

    # Edit todo
    todo_content = todo.to_string().encode("utf-8")
    edited_content = editor_callback(todo_content)

    # Parse edited todo
    edited_todo = RebaseTodo.from_string(
        edited_content.decode("utf-8", errors="replace")
    )

    # Save edited todo
    state_manager.save_todo(edited_todo)

    return edited_todo


def process_interactive_rebase(
    repo: Repo,
    todo: Optional[RebaseTodo] = None,
    editor_callback: Optional[Callable[[bytes], bytes]] = None,
) -> tuple[bool, Optional[str]]:
    """Process an interactive rebase.

    This function executes the commands in the todo list sequentially.

    Args:
        repo: Repository to rebase in
        todo: RebaseTodo object (if None, loads from state)
        editor_callback: Optional callback for reword operations

    Returns:
        Tuple of (``is_complete``, ``pause_reason``):

        * ``is_complete``: True if rebase is complete, False if paused
        * ``pause_reason``: Reason for pause (e.g., "edit", "conflict", "break") or None

    Raises:
        RebaseError: If rebase fails
    """
    state_manager = repo.get_rebase_state_manager()
    rebaser = Rebaser(repo)

    # Load todo if not provided
    if todo is None:
        todo = state_manager.load_todo()
        if not todo:
            raise RebaseError("No interactive rebase in progress")

    # Process each todo entry
    while not todo.is_complete():
        entry = todo.get_current()
        if not entry:
            break

        # Handle each command type
        if entry.command == RebaseTodoCommand.PICK:
            # Regular cherry-pick
            result = rebaser.continue_()
            if result is not None:
                # Conflicts
                return False, "conflict"

        elif entry.command == RebaseTodoCommand.REWORD:
            # Cherry-pick then edit message
            result = rebaser.continue_()
            if result is not None:
                # Conflicts
                return False, "conflict"

            # Get the last commit and allow editing its message
            if rebaser._done and editor_callback:
                last_commit = rebaser._done[-1]
                new_message = editor_callback(last_commit.message)

                # Create new commit with edited message
                new_commit = Commit()
                new_commit.tree = last_commit.tree
                new_commit.parents = last_commit.parents
                new_commit.author = last_commit.author
                new_commit.author_time = last_commit.author_time
                new_commit.author_timezone = last_commit.author_timezone
                new_commit.committer = last_commit.committer
                new_commit.commit_time = last_commit.commit_time
                new_commit.commit_timezone = last_commit.commit_timezone
                new_commit.message = new_message
                new_commit.encoding = last_commit.encoding

                repo.object_store.add_object(new_commit)

                # Replace last commit in done list
                rebaser._done[-1] = new_commit

        elif entry.command == RebaseTodoCommand.EDIT:
            # Cherry-pick then pause
            result = rebaser.continue_()
            if result is not None:
                # Conflicts
                return False, "conflict"

            # Pause for user to amend
            todo.advance()
            state_manager.save_todo(todo)
            return False, "edit"

        elif entry.command == RebaseTodoCommand.SQUASH:
            # Combine with previous commit, keeping both messages
            if not rebaser._done:
                raise RebaseError("Cannot squash without a previous commit")

            conflict_result = _squash_commits(
                repo, rebaser, entry, keep_message=True, editor_callback=editor_callback
            )
            if conflict_result == "conflict":
                return False, "conflict"

        elif entry.command == RebaseTodoCommand.FIXUP:
            # Combine with previous commit, discarding this message
            if not rebaser._done:
                raise RebaseError("Cannot fixup without a previous commit")

            conflict_result = _squash_commits(
                repo, rebaser, entry, keep_message=False, editor_callback=None
            )
            if conflict_result == "conflict":
                return False, "conflict"

        elif entry.command == RebaseTodoCommand.DROP:
            # Skip this commit
            if rebaser._todo:
                rebaser._todo.pop(0)

        elif entry.command == RebaseTodoCommand.EXEC:
            # Execute shell command
            if entry.arguments:
                try:
                    subprocess.run(entry.arguments, shell=True, check=True)
                except subprocess.CalledProcessError as e:
                    # Command failed, pause rebase
                    return False, f"exec failed: {e}"

        elif entry.command == RebaseTodoCommand.BREAK:
            # Pause rebase
            todo.advance()
            state_manager.save_todo(todo)
            return False, "break"

        else:
            # Unsupported command
            raise RebaseError(f"Unsupported rebase command: {entry.command.value}")

        # Move to next entry
        todo.advance()

        # Save progress
        state_manager.save_todo(todo)
        rebaser._save_rebase_state()

    # Rebase complete
    rebaser._finish_rebase()
    return True, None


def _squash_commits(
    repo: Repo,
    rebaser: Rebaser,
    entry: RebaseTodoEntry,
    keep_message: bool,
    editor_callback: Optional[Callable[[bytes], bytes]] = None,
) -> Optional[str]:
    """Helper to squash/fixup commits.

    Args:
        repo: Repository
        rebaser: Rebaser instance
        entry: Todo entry for the commit to squash
        keep_message: Whether to keep this commit's message (squash) or discard (fixup)
        editor_callback: Optional callback to edit combined message (for squash)

    Returns:
        None on success, "conflict" on conflict
    """
    if not rebaser._done:
        raise RebaseError("Cannot squash without a previous commit")

    # Get the commit to squash
    if not entry.commit_sha:
        raise RebaseError("No commit SHA for squash/fixup operation")
    commit_to_squash = repo[entry.commit_sha]
    if not isinstance(commit_to_squash, Commit):
        raise RebaseError(f"Expected commit, got {type(commit_to_squash).__name__}")

    # Get the previous commit (target of squash)
    previous_commit = rebaser._done[-1]

    # Cherry-pick the changes onto the previous commit
    parent = repo[commit_to_squash.parents[0]]
    if not isinstance(parent, Commit):
        raise RebaseError(f"Expected parent commit, got {type(parent).__name__}")

    # Perform three-way merge for the tree
    merged_tree, conflicts = three_way_merge(
        repo.object_store, parent, previous_commit, commit_to_squash
    )

    if conflicts:
        return "conflict"

    # Combine messages if squashing (not fixup)
    if keep_message:
        combined_message = previous_commit.message + b"\n\n" + commit_to_squash.message
        if editor_callback:
            combined_message = editor_callback(combined_message)
    else:
        combined_message = previous_commit.message

    # Create new combined commit
    new_commit = Commit()
    new_commit.tree = merged_tree.id
    new_commit.parents = previous_commit.parents
    new_commit.author = previous_commit.author
    new_commit.author_time = previous_commit.author_time
    new_commit.author_timezone = previous_commit.author_timezone
    new_commit.committer = commit_to_squash.committer
    new_commit.commit_time = commit_to_squash.commit_time
    new_commit.commit_timezone = commit_to_squash.commit_timezone
    new_commit.message = combined_message
    new_commit.encoding = previous_commit.encoding

    repo.object_store.add_object(merged_tree)
    repo.object_store.add_object(new_commit)

    # Replace the previous commit with the combined one
    rebaser._done[-1] = new_commit

    # Remove the squashed commit from todo
    if rebaser._todo and rebaser._todo[0].id == commit_to_squash.id:
        rebaser._todo.pop(0)

    return None
