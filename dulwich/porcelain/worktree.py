# worktree.py -- Porcelain-like interface for Git worktrees
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Porcelain-like interface for Git worktrees."""

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..worktree import WorkTreeInfo
    from . import RepoPath


def worktree_list(repo: "RepoPath" = ".") -> list["WorkTreeInfo"]:
    """List all worktrees for a repository.

    Args:
        repo: Path to repository

    Returns:
        List of WorkTreeInfo objects
    """
    from ..worktree import list_worktrees
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        return list_worktrees(r)


def worktree_add(
    repo: "RepoPath" = ".",
    path: str | os.PathLike[str] | None = None,
    branch: str | bytes | None = None,
    commit: str | bytes | None = None,
    detach: bool = False,
    force: bool = False,
) -> str:
    """Add a new worktree.

    Args:
        repo: Path to repository
        path: Path for new worktree
        branch: Branch to checkout (creates if doesn't exist)
        commit: Specific commit to checkout
        detach: Create with detached HEAD
        force: Force creation even if branch is already checked out

    Returns:
        Path to the newly created worktree
    """
    from ..objects import ObjectID
    from ..worktree import add_worktree
    from . import open_repo_closing

    if path is None:
        raise ValueError("Path is required for worktree add")

    with open_repo_closing(repo) as r:
        commit_bytes = commit.encode() if isinstance(commit, str) else commit
        commit_id = ObjectID(commit_bytes) if commit_bytes is not None else None
        wt_repo = add_worktree(
            r, path, branch=branch, commit=commit_id, detach=detach, force=force
        )
        return wt_repo.path


def worktree_remove(
    repo: "RepoPath" = ".",
    path: str | os.PathLike[str] | None = None,
    force: bool = False,
) -> None:
    """Remove a worktree.

    Args:
        repo: Path to repository
        path: Path to worktree to remove
        force: Force removal even if there are local changes
    """
    from ..worktree import remove_worktree
    from . import open_repo_closing

    if path is None:
        raise ValueError("Path is required for worktree remove")

    with open_repo_closing(repo) as r:
        remove_worktree(r, path, force=force)


def worktree_prune(
    repo: "RepoPath" = ".", dry_run: bool = False, expire: int | None = None
) -> list[str]:
    """Prune worktree administrative files.

    Args:
        repo: Path to repository
        dry_run: Only show what would be removed
        expire: Only prune worktrees older than this many seconds

    Returns:
        List of pruned worktree names
    """
    from ..worktree import prune_worktrees
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        return prune_worktrees(r, expire=expire, dry_run=dry_run)


def worktree_lock(
    repo: "RepoPath" = ".",
    path: str | os.PathLike[str] | None = None,
    reason: str | None = None,
) -> None:
    """Lock a worktree to prevent it from being pruned.

    Args:
        repo: Path to repository
        path: Path to worktree to lock
        reason: Optional reason for locking
    """
    from ..worktree import lock_worktree
    from . import open_repo_closing

    if path is None:
        raise ValueError("Path is required for worktree lock")

    with open_repo_closing(repo) as r:
        lock_worktree(r, path, reason=reason)


def worktree_unlock(
    repo: "RepoPath" = ".", path: str | os.PathLike[str] | None = None
) -> None:
    """Unlock a worktree.

    Args:
        repo: Path to repository
        path: Path to worktree to unlock
    """
    from ..worktree import unlock_worktree
    from . import open_repo_closing

    if path is None:
        raise ValueError("Path is required for worktree unlock")

    with open_repo_closing(repo) as r:
        unlock_worktree(r, path)


def worktree_move(
    repo: "RepoPath" = ".",
    old_path: str | os.PathLike[str] | None = None,
    new_path: str | os.PathLike[str] | None = None,
) -> None:
    """Move a worktree to a new location.

    Args:
        repo: Path to repository
        old_path: Current path of worktree
        new_path: New path for worktree
    """
    from ..worktree import move_worktree
    from . import open_repo_closing

    if old_path is None or new_path is None:
        raise ValueError("Both old_path and new_path are required for worktree move")

    with open_repo_closing(repo) as r:
        move_worktree(r, old_path, new_path)


def worktree_repair(
    repo: "RepoPath" = ".",
    paths: list[str | os.PathLike[str]] | None = None,
) -> list[str]:
    """Repair worktree administrative files.

    Args:
        repo: Path to repository
        paths: Optional list of worktree paths to repair. If None, repairs
               connections from the main repository to all linked worktrees.

    Returns:
        List of repaired worktree paths
    """
    from ..worktree import repair_worktree
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        return repair_worktree(r, paths=paths)
