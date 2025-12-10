# submodule.py -- Submodule porcelain
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

"""Porcelain functions for working with submodules."""

import os
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, BinaryIO

from ..config import ConfigFile, read_submodules
from ..objects import Commit
from ..repo import Repo

if TYPE_CHECKING:
    from . import RepoPath


def submodule_add(
    repo: str | os.PathLike[str] | Repo,
    url: str,
    path: str | os.PathLike[str] | None = None,
    name: str | None = None,
) -> None:
    """Add a new submodule.

    Args:
      repo: Path to repository
      url: URL of repository to add as submodule
      path: Path where submodule should live
      name: Name for the submodule
    """
    from . import Error, _canonical_part, open_repo_closing

    with open_repo_closing(repo) as r:
        if path is None:
            path = os.path.relpath(_canonical_part(url), r.path)
        if name is None:
            name = os.fsdecode(path) if path is not None else None

        if name is None:
            raise Error("Submodule name must be specified or derivable from path")

        # TODO(jelmer): Move this logic to dulwich.submodule
        gitmodules_path = os.path.join(r.path, ".gitmodules")
        try:
            config = ConfigFile.from_path(gitmodules_path)
        except FileNotFoundError:
            config = ConfigFile()
            config.path = gitmodules_path
        config.set(("submodule", name), "url", url)
        config.set(("submodule", name), "path", os.fsdecode(path))
        config.write_to_path()


def submodule_init(repo: str | os.PathLike[str] | Repo) -> None:
    """Initialize submodules.

    Args:
      repo: Path to repository
    """
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        config = r.get_config()
        gitmodules_path = os.path.join(r.path, ".gitmodules")
        for path, url, name in read_submodules(gitmodules_path):
            config.set((b"submodule", name), b"active", True)
            config.set((b"submodule", name), b"url", url)
        config.write_to_path()


def submodule_list(repo: "RepoPath") -> Iterator[tuple[str, str]]:
    """List submodules.

    Args:
      repo: Path to repository
    """
    from ..submodule import iter_cached_submodules
    from . import DEFAULT_ENCODING, open_repo_closing

    with open_repo_closing(repo) as r:
        head_commit = r[r.head()]
        assert isinstance(head_commit, Commit)
        for path, sha in iter_cached_submodules(r.object_store, head_commit.tree):
            yield path.decode(DEFAULT_ENCODING), sha.decode(DEFAULT_ENCODING)


def submodule_update(
    repo: str | os.PathLike[str] | Repo,
    paths: Sequence[str | bytes | os.PathLike[str]] | None = None,
    init: bool = False,
    force: bool = False,
    recursive: bool = False,
    errstream: BinaryIO | None = None,
) -> None:
    """Update submodules.

    Args:
      repo: Path to repository
      paths: Optional list of specific submodule paths to update. If None, updates all.
      init: If True, initialize submodules first
      force: Force update even if local changes exist
      recursive: If True, recursively update nested submodules
      errstream: Error stream for error messages
    """
    from ..client import get_transport_and_path
    from ..index import build_index_from_tree
    from ..refs import HEADREF
    from ..submodule import iter_cached_submodules
    from . import (
        DEFAULT_ENCODING,
        clone,
        open_repo_closing,
        reset,
    )

    with open_repo_closing(repo) as r:
        if init:
            submodule_init(r)

        config = r.get_config()
        gitmodules_path = os.path.join(r.path, ".gitmodules")

        # Get list of submodules to update
        submodules_to_update = []
        head_commit = r[r.head()]
        assert isinstance(head_commit, Commit)
        for path, sha in iter_cached_submodules(r.object_store, head_commit.tree):
            path_str = (
                path.decode(DEFAULT_ENCODING) if isinstance(path, bytes) else path
            )
            if paths is None or path_str in paths:
                submodules_to_update.append((path, sha))

        # Read submodule configuration
        for path, target_sha in submodules_to_update:
            path_str = (
                path.decode(DEFAULT_ENCODING) if isinstance(path, bytes) else path
            )

            # Find the submodule name from ..gitmodules
            submodule_name: bytes | None = None
            for sm_path, sm_url, sm_name in read_submodules(gitmodules_path):
                if sm_path == path:
                    submodule_name = sm_name
                    break

            if not submodule_name:
                continue

            # Get the URL from config
            section = (
                b"submodule",
                submodule_name
                if isinstance(submodule_name, bytes)
                else submodule_name.encode(),
            )
            try:
                url_value = config.get(section, b"url")
                if isinstance(url_value, bytes):
                    url = url_value.decode(DEFAULT_ENCODING)
                else:
                    url = url_value
            except KeyError:
                # URL not in config, skip this submodule
                continue

            # Get or create the submodule repository paths
            submodule_path = os.path.join(r.path, path_str)
            submodule_git_dir = os.path.join(r.controldir(), "modules", path_str)

            # Clone or fetch the submodule
            if not os.path.exists(submodule_git_dir):
                # Clone the submodule as bare repository
                os.makedirs(os.path.dirname(submodule_git_dir), exist_ok=True)

                # Clone to the git directory
                sub_repo = clone(url, submodule_git_dir, bare=True, checkout=False)
                sub_repo.close()

                # Create the submodule directory if it doesn't exist
                if not os.path.exists(submodule_path):
                    os.makedirs(submodule_path)

                # Create .git file in the submodule directory
                relative_git_dir = os.path.relpath(submodule_git_dir, submodule_path)
                git_file_path = os.path.join(submodule_path, ".git")
                with open(git_file_path, "w") as f:
                    f.write(f"gitdir: {relative_git_dir}\n")

                # Set up working directory configuration
                with open_repo_closing(submodule_git_dir) as sub_repo:
                    sub_config = sub_repo.get_config()
                    sub_config.set(
                        (b"core",),
                        b"worktree",
                        os.path.abspath(submodule_path).encode(),
                    )
                    sub_config.write_to_path()

                    # Checkout the target commit
                    sub_repo.refs[HEADREF] = target_sha

                    # Build the index and checkout files
                    tree = sub_repo[target_sha]
                    if hasattr(tree, "tree"):  # If it's a commit, get the tree
                        tree_id = tree.tree
                    else:
                        tree_id = target_sha

                    build_index_from_tree(
                        submodule_path,
                        sub_repo.index_path(),
                        sub_repo.object_store,
                        tree_id,
                    )
            else:
                # Fetch and checkout in existing submodule
                with open_repo_closing(submodule_git_dir) as sub_repo:
                    # Fetch from remote
                    client, path_segments = get_transport_and_path(url)
                    client.fetch(path_segments.encode(), sub_repo)

                    # Update to the target commit
                    sub_repo.refs[HEADREF] = target_sha

                    # Reset the working directory
                    reset(sub_repo, "hard", target_sha)

            # Recursively update nested submodules if requested
            if recursive:
                submodule_gitmodules = os.path.join(submodule_path, ".gitmodules")
                if os.path.exists(submodule_gitmodules):
                    submodule_update(
                        submodule_path,
                        paths=None,
                        init=True,  # Always initialize nested submodules
                        force=force,
                        recursive=True,
                        errstream=errstream,
                    )
