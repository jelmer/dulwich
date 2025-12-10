# lfs.py -- LFS porcelain
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

"""Porcelain functions for Git LFS support."""

import fnmatch
import logging
import os
import stat
from collections.abc import Sequence
from typing import Any

from dulwich.index import (
    ConflictedIndexEntry,
    index_entry_from_stat,
)
from dulwich.objects import Blob, Commit, Tree
from dulwich.refs import HEADREF, Ref
from dulwich.repo import Repo


def lfs_track(
    repo: str | os.PathLike[str] | Repo = ".",
    patterns: Sequence[str] | None = None,
) -> list[str]:
    """Track file patterns with Git LFS.

    Args:
      repo: Path to repository
      patterns: List of file patterns to track (e.g., ["*.bin", "*.pdf"])
                If None, returns current tracked patterns

    Returns:
      List of tracked patterns
    """
    from ..attrs import GitAttributes
    from . import add, open_repo_closing

    with open_repo_closing(repo) as r:
        gitattributes_path = os.path.join(r.path, ".gitattributes")

        # Load existing GitAttributes
        if os.path.exists(gitattributes_path):
            gitattributes = GitAttributes.from_file(gitattributes_path)
        else:
            gitattributes = GitAttributes()

        if patterns is None:
            # Return current LFS tracked patterns
            tracked = []
            for pattern_obj, attrs in gitattributes:
                if attrs.get(b"filter") == b"lfs":
                    tracked.append(pattern_obj.pattern.decode())
            return tracked

        # Add new patterns
        for pattern in patterns:
            # Ensure pattern is bytes
            pattern_bytes = pattern.encode() if isinstance(pattern, str) else pattern

            # Set LFS attributes for the pattern
            gitattributes.set_attribute(pattern_bytes, b"filter", b"lfs")
            gitattributes.set_attribute(pattern_bytes, b"diff", b"lfs")
            gitattributes.set_attribute(pattern_bytes, b"merge", b"lfs")
            gitattributes.set_attribute(pattern_bytes, b"text", False)

        # Write updated attributes
        gitattributes.write_to_file(gitattributes_path)

        # Stage the .gitattributes file
        add(r, [".gitattributes"])

        return lfs_track(r)  # Return updated list


def lfs_untrack(
    repo: str | os.PathLike[str] | Repo = ".",
    patterns: Sequence[str] | None = None,
) -> list[str]:
    """Untrack file patterns from Git LFS.

    Args:
      repo: Path to repository
      patterns: List of file patterns to untrack

    Returns:
      List of remaining tracked patterns
    """
    from ..attrs import GitAttributes
    from . import add, open_repo_closing

    if not patterns:
        return lfs_track(repo)

    with open_repo_closing(repo) as r:
        gitattributes_path = os.path.join(r.path, ".gitattributes")

        if not os.path.exists(gitattributes_path):
            return []

        # Load existing GitAttributes
        gitattributes = GitAttributes.from_file(gitattributes_path)

        # Remove specified patterns
        for pattern in patterns:
            pattern_bytes = pattern.encode() if isinstance(pattern, str) else pattern

            # Check if pattern is tracked by LFS
            for pattern_obj, attrs in list(gitattributes):
                if (
                    pattern_obj.pattern == pattern_bytes
                    and attrs.get(b"filter") == b"lfs"
                ):
                    gitattributes.remove_pattern(pattern_bytes)
                    break

        # Write updated attributes
        gitattributes.write_to_file(gitattributes_path)

        # Stage the .gitattributes file
        add(r, [".gitattributes"])

        return lfs_track(r)  # Return updated list


def lfs_init(repo: str | os.PathLike[str] | Repo = ".") -> None:
    """Initialize Git LFS in a repository.

    Args:
      repo: Path to repository

    Returns:
      None
    """
    from ..lfs import LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        # Create LFS store
        LFSStore.from_repo(r, create=True)

        # Set up Git config for LFS
        config = r.get_config()
        config.set((b"filter", b"lfs"), b"process", b"git-lfs filter-process")
        config.set((b"filter", b"lfs"), b"required", b"true")
        config.set((b"filter", b"lfs"), b"clean", b"git-lfs clean -- %f")
        config.set((b"filter", b"lfs"), b"smudge", b"git-lfs smudge -- %f")
        config.write_to_path()


def lfs_clean(
    repo: str | os.PathLike[str] | Repo = ".",
    path: str | os.PathLike[str] | None = None,
) -> bytes:
    """Clean a file by converting it to an LFS pointer.

    Args:
      repo: Path to repository
      path: Path to file to clean (relative to repo root)

    Returns:
      LFS pointer content as bytes
    """
    from ..lfs import LFSFilterDriver, LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        if path is None:
            raise ValueError("Path must be specified")

        # Get LFS store
        lfs_store = LFSStore.from_repo(r)
        filter_driver = LFSFilterDriver(lfs_store, config=r.get_config())

        # Read file content
        full_path = os.path.join(r.path, path)
        with open(full_path, "rb") as f:
            content = f.read()

        # Clean the content (convert to LFS pointer)
        return filter_driver.clean(content)


def lfs_smudge(
    repo: str | os.PathLike[str] | Repo = ".",
    pointer_content: bytes | None = None,
) -> bytes:
    """Smudge an LFS pointer by retrieving the actual content.

    Args:
      repo: Path to repository
      pointer_content: LFS pointer content as bytes

    Returns:
      Actual file content as bytes
    """
    from ..lfs import LFSFilterDriver, LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        if pointer_content is None:
            raise ValueError("Pointer content must be specified")

        # Get LFS store
        lfs_store = LFSStore.from_repo(r)
        filter_driver = LFSFilterDriver(lfs_store, config=r.get_config())

        # Smudge the pointer (retrieve actual content)
        return filter_driver.smudge(pointer_content)


def lfs_ls_files(
    repo: str | os.PathLike[str] | Repo = ".",
    ref: str | bytes | None = None,
) -> list[tuple[bytes, str, int]]:
    """List files tracked by Git LFS.

    Args:
      repo: Path to repository
      ref: Git ref to check (defaults to HEAD)

    Returns:
      List of (path, oid, size) tuples for LFS files
    """
    from ..lfs import LFSPointer
    from ..object_store import iter_tree_contents
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        if ref is None:
            ref = b"HEAD"
        elif isinstance(ref, str):
            ref = ref.encode()

        # Get the commit and tree
        try:
            commit = r[ref]
            assert isinstance(commit, Commit)
            tree = r[commit.tree]
            assert isinstance(tree, Tree)
        except KeyError:
            return []

        lfs_files = []

        # Walk the tree
        for path, mode, sha in iter_tree_contents(r.object_store, tree.id):
            assert path is not None
            assert mode is not None
            assert sha is not None
            if not stat.S_ISREG(mode):
                continue

            # Check if it's an LFS pointer
            obj = r.object_store[sha]
            if not isinstance(obj, Blob):
                raise AssertionError(f"Expected Blob object, got {type(obj).__name__}")
            pointer = LFSPointer.from_bytes(obj.data)
            if pointer is not None:
                lfs_files.append((path, pointer.oid, pointer.size))

        return lfs_files


def lfs_migrate(
    repo: str | os.PathLike[str] | Repo = ".",
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    everything: bool = False,
) -> int:
    """Migrate files to Git LFS.

    Args:
      repo: Path to repository
      include: Patterns of files to include
      exclude: Patterns of files to exclude
      everything: Migrate all files above a certain size

    Returns:
      Number of migrated files
    """
    from ..lfs import LFSFilterDriver, LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        # Initialize LFS if needed
        lfs_store = LFSStore.from_repo(r, create=True)
        filter_driver = LFSFilterDriver(lfs_store, config=r.get_config())

        # Get current index
        index = r.open_index()

        migrated = 0

        # Determine files to migrate
        files_to_migrate = []

        if everything:
            # Migrate all files above 100MB
            for path, entry in index.items():
                full_path = os.path.join(r.path, path.decode())
                if os.path.exists(full_path):
                    size = os.path.getsize(full_path)
                    if size > 100 * 1024 * 1024:  # 100MB
                        files_to_migrate.append(path.decode())
        else:
            # Use include/exclude patterns
            for path, entry in index.items():
                path_str = path.decode()

                # Check include patterns
                if include:
                    matched = any(
                        fnmatch.fnmatch(path_str, pattern) for pattern in include
                    )
                    if not matched:
                        continue

                # Check exclude patterns
                if exclude:
                    excluded = any(
                        fnmatch.fnmatch(path_str, pattern) for pattern in exclude
                    )
                    if excluded:
                        continue

                files_to_migrate.append(path_str)

        # Migrate files
        for path_str in files_to_migrate:
            full_path = os.path.join(r.path, path_str)
            if not os.path.exists(full_path):
                continue

            # Read file content
            with open(full_path, "rb") as f:
                content = f.read()

            # Convert to LFS pointer
            pointer_content = filter_driver.clean(content)

            # Write pointer back to file
            with open(full_path, "wb") as f:
                f.write(pointer_content)

            # Create blob for pointer content and update index
            blob = Blob()
            blob.data = pointer_content
            r.object_store.add_object(blob)

            st = os.stat(full_path)
            index_entry = index_entry_from_stat(st, blob.id, 0)
            path_bytes = path_str.encode() if isinstance(path_str, str) else path_str
            index[path_bytes] = index_entry

            migrated += 1

        # Write updated index
        index.write()

        # Track patterns if include was specified
        if include:
            lfs_track(r, include)

        return migrated


def lfs_pointer_check(
    repo: str | os.PathLike[str] | Repo = ".",
    paths: Sequence[str] | None = None,
) -> dict[str, Any | None]:
    """Check if files are valid LFS pointers.

    Args:
      repo: Path to repository
      paths: List of file paths to check (if None, check all files)

    Returns:
      Dict mapping paths to LFSPointer objects (or None if not a pointer)
    """
    from ..lfs import LFSPointer
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        results = {}

        if paths is None:
            # Check all files in index
            index = r.open_index()
            paths = [path.decode() for path in index]

        for path in paths:
            full_path = os.path.join(r.path, path)
            if os.path.exists(full_path):
                try:
                    with open(full_path, "rb") as f:
                        content = f.read()
                    pointer = LFSPointer.from_bytes(content)
                    results[path] = pointer
                except OSError:
                    results[path] = None
            else:
                results[path] = None

        return results


def lfs_fetch(
    repo: str | os.PathLike[str] | Repo = ".",
    remote: str = "origin",
    refs: list[str | bytes] | None = None,
) -> int:
    """Fetch LFS objects from remote.

    Args:
      repo: Path to repository
      remote: Remote name (default: origin)
      refs: Specific refs to fetch LFS objects for (default: all refs)

    Returns:
      Number of objects fetched
    """
    from ..lfs import LFSClient, LFSPointer, LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        # Get LFS server URL from config
        config = r.get_config()
        lfs_url_bytes = config.get((b"lfs",), b"url")
        if not lfs_url_bytes:
            # Try remote URL
            remote_url = config.get((b"remote", remote.encode()), b"url")
            if remote_url:
                # Append /info/lfs to remote URL
                remote_url_str = remote_url.decode()
                if remote_url_str.endswith(".git"):
                    remote_url_str = remote_url_str[:-4]
                lfs_url = f"{remote_url_str}/info/lfs"
            else:
                raise ValueError(f"No LFS URL configured for remote {remote}")
        else:
            lfs_url = lfs_url_bytes.decode()

        # Get authentication
        auth = None
        # TODO: Support credential helpers and other auth methods

        # Create LFS client and store
        client = LFSClient(lfs_url, auth)
        store = LFSStore.from_repo(r)

        # Find all LFS pointers in the refs
        pointers_to_fetch = []

        if refs is None:
            # Get all refs
            refs = list(r.refs.keys())

        for ref in refs:
            if isinstance(ref, str):
                ref_key = Ref(ref.encode())
            elif isinstance(ref, bytes):
                ref_key = Ref(ref)
            else:
                ref_key = ref
            try:
                commit = r[r.refs[ref_key]]
            except KeyError:
                continue

            # Walk the commit tree
            assert isinstance(commit, Commit)
            for path, mode, sha in r.object_store.iter_tree_contents(commit.tree):
                assert sha is not None
                try:
                    obj = r.object_store[sha]
                except KeyError:
                    pass
                else:
                    if isinstance(obj, Blob):
                        pointer = LFSPointer.from_bytes(obj.data)
                        if pointer and pointer.is_valid_oid():
                            # Check if we already have it
                            try:
                                with store.open_object(pointer.oid):
                                    pass  # Object exists, no need to fetch
                            except KeyError:
                                pointers_to_fetch.append((pointer.oid, pointer.size))

        # Fetch missing objects
        fetched = 0
        for oid, size in pointers_to_fetch:
            content = client.download(oid, size)
            store.write_object([content])
            fetched += 1

        return fetched


def lfs_pull(repo: str | os.PathLike[str] | Repo = ".", remote: str = "origin") -> int:
    """Pull LFS objects for current checkout.

    Args:
      repo: Path to repository
      remote: Remote name (default: origin)

    Returns:
      Number of objects fetched
    """
    from ..lfs import LFSPointer, LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        # First do a fetch for HEAD
        fetched = lfs_fetch(repo, remote, [b"HEAD"])

        # Then checkout LFS files in working directory
        store = LFSStore.from_repo(r)
        index = r.open_index()

        for path, entry in index.items():
            full_path = os.path.join(r.path, path.decode())
            if os.path.exists(full_path):
                with open(full_path, "rb") as f:
                    content = f.read()

                pointer = LFSPointer.from_bytes(content)
                if pointer and pointer.is_valid_oid():
                    try:
                        # Replace pointer with actual content
                        with store.open_object(pointer.oid) as lfs_file:
                            lfs_content = lfs_file.read()
                        with open(full_path, "wb") as f:
                            f.write(lfs_content)
                    except KeyError:
                        # Object not available
                        pass

        return fetched


def lfs_push(
    repo: str | os.PathLike[str] | Repo = ".",
    remote: str = "origin",
    refs: list[str | bytes] | None = None,
) -> int:
    """Push LFS objects to remote.

    Args:
      repo: Path to repository
      remote: Remote name (default: origin)
      refs: Specific refs to push LFS objects for (default: current branch)

    Returns:
      Number of objects pushed
    """
    from ..lfs import LFSClient, LFSPointer, LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        # Get LFS server URL from config
        config = r.get_config()
        lfs_url_bytes = config.get((b"lfs",), b"url")
        if not lfs_url_bytes:
            # Try remote URL
            remote_url = config.get((b"remote", remote.encode()), b"url")
            if remote_url:
                # Append /info/lfs to remote URL
                remote_url_str = remote_url.decode()
                if remote_url_str.endswith(".git"):
                    remote_url_str = remote_url_str[:-4]
                lfs_url = f"{remote_url_str}/info/lfs"
            else:
                raise ValueError(f"No LFS URL configured for remote {remote}")
        else:
            lfs_url = lfs_url_bytes.decode()

        # Get authentication
        auth = None
        # TODO: Support credential helpers and other auth methods

        # Create LFS client and store
        client = LFSClient(lfs_url, auth)
        store = LFSStore.from_repo(r)

        # Find all LFS objects to push
        if refs is None:
            # Push current branch
            head_ref = r.refs.read_ref(HEADREF)
            refs = [head_ref] if head_ref else []

        objects_to_push = set()

        for ref in refs:
            if isinstance(ref, str):
                ref_bytes = ref.encode()
            else:
                ref_bytes = ref
            try:
                if ref_bytes.startswith(b"refs/"):
                    commit = r[r.refs[Ref(ref_bytes)]]
                else:
                    commit = r[ref_bytes]
            except KeyError:
                continue

            # Walk the commit tree
            assert isinstance(commit, Commit)
            for path, mode, sha in r.object_store.iter_tree_contents(commit.tree):
                assert sha is not None
                try:
                    obj = r.object_store[sha]
                except KeyError:
                    pass
                else:
                    if isinstance(obj, Blob):
                        pointer = LFSPointer.from_bytes(obj.data)
                        if pointer and pointer.is_valid_oid():
                            objects_to_push.add((pointer.oid, pointer.size))

        # Push objects
        pushed = 0
        for oid, size in objects_to_push:
            try:
                with store.open_object(oid) as f:
                    content = f.read()
            except KeyError:
                # Object not in local store
                logging.warn("LFS object %s not found locally", oid)
            else:
                client.upload(oid, size, content)
                pushed += 1

        return pushed


def lfs_status(repo: str | os.PathLike[str] | Repo = ".") -> dict[str, list[str]]:
    """Show status of LFS files.

    Args:
      repo: Path to repository

    Returns:
      Dict with status information
    """
    from ..lfs import LFSPointer, LFSStore
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        store = LFSStore.from_repo(r)
        index = r.open_index()

        status: dict[str, list[str]] = {
            "tracked": [],
            "not_staged": [],
            "not_committed": [],
            "not_pushed": [],
            "missing": [],
        }

        # Check working directory files
        for path, entry in index.items():
            path_str = path.decode()
            full_path = os.path.join(r.path, path_str)

            if os.path.exists(full_path):
                with open(full_path, "rb") as f:
                    content = f.read()

                pointer = LFSPointer.from_bytes(content)
                if pointer and pointer.is_valid_oid():
                    status["tracked"].append(path_str)

                    # Check if object exists locally
                    try:
                        with store.open_object(pointer.oid):
                            pass  # Object exists locally
                    except KeyError:
                        status["missing"].append(path_str)

                    # Check if file has been modified
                    if isinstance(entry, ConflictedIndexEntry):
                        continue  # Skip conflicted entries
                    try:
                        staged_obj = r.object_store[entry.sha]
                    except KeyError:
                        pass
                    else:
                        if not isinstance(staged_obj, Blob):
                            raise AssertionError(
                                f"Expected Blob object, got {type(staged_obj).__name__}"
                            )
                        staged_pointer = LFSPointer.from_bytes(staged_obj.data)
                        if staged_pointer and staged_pointer.oid != pointer.oid:
                            status["not_staged"].append(path_str)

        # TODO: Check for not committed and not pushed files

        return status
