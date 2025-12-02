# config.py - Reading and writing Git config files
# Copyright (C) 2011-2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Working with Git submodules."""

__all__ = [
    "ensure_submodule_placeholder",
    "iter_cached_submodules",
]

import os
from collections.abc import Iterator
from typing import TYPE_CHECKING

from .object_store import iter_tree_contents
from .objects import S_ISGITLINK

if TYPE_CHECKING:
    from .objects import ObjectID
    from .pack import ObjectContainer
    from .repo import Repo


def iter_cached_submodules(
    store: "ObjectContainer", root_tree_id: "ObjectID"
) -> Iterator[tuple[bytes, "ObjectID"]]:
    """Iterate over cached submodules.

    Args:
      store: Object store to iterate
      root_tree_id: SHA of root tree

    Returns:
      Iterator over over (path, sha) tuples
    """
    for entry in iter_tree_contents(store, root_tree_id):
        assert entry.mode is not None
        if S_ISGITLINK(entry.mode):
            assert entry.path is not None
            assert entry.sha is not None
            yield entry.path, entry.sha


def ensure_submodule_placeholder(
    repo: "Repo",
    submodule_path: str | bytes,
) -> None:
    """Create a submodule placeholder directory with .git file.

    This creates the minimal structure needed for a submodule:
    - The submodule directory
    - A .git file pointing to the submodule's git directory

    Args:
      repo: Parent repository
      submodule_path: Path to the submodule relative to repo root
    """
    # Ensure path is bytes
    if isinstance(submodule_path, str):
        submodule_path = submodule_path.encode()

    # Get repo path as bytes
    repo_path = repo.path if isinstance(repo.path, bytes) else repo.path.encode()

    # Create full path to submodule
    full_path = os.path.join(repo_path, submodule_path)

    # Create submodule directory if it doesn't exist
    if not os.path.exists(full_path):
        os.makedirs(full_path)

    # Create .git file pointing to the submodule's git directory
    git_filename = b".git" if isinstance(full_path, bytes) else ".git"
    git_file_path = os.path.join(full_path, git_filename)
    if not os.path.exists(git_file_path):
        # Submodule git directories are typically stored in .git/modules/<name>
        # The relative path from the submodule to the parent's .git directory
        # depends on the submodule's depth
        depth = submodule_path.count(b"/") + 1
        relative_git_dir = b"../" * depth + b".git/modules/" + submodule_path

        with open(git_file_path, "wb") as f:
            f.write(b"gitdir: " + relative_git_dir + b"\n")
