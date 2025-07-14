# worktree.py -- Working tree operations for Git repositories
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Working tree operations for Git repositories."""

import os
import stat
import sys
import time
import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from .repo import Repo

from .errors import CommitError, HookError
from .objects import Commit, ObjectID, Tag, Tree
from .refs import Ref
from .repo import check_user_identity, get_user_identity


class WorkTree:
    """Working tree operations for a Git repository.

    This class provides methods for working with the working tree,
    such as staging files, committing changes, and resetting the index.
    """

    def __init__(self, repo: "Repo", path: Union[str, bytes, os.PathLike]) -> None:
        """Initialize a WorkTree for the given repository.

        Args:
            repo: The repository this working tree belongs to
            path: Path to the working tree directory
        """
        self._repo = repo
        raw_path = os.fspath(path)
        if isinstance(raw_path, bytes):
            self.path: str = os.fsdecode(raw_path)
        else:
            self.path = raw_path
        self.path = os.path.abspath(self.path)

    def stage(
        self,
        fs_paths: Union[
            str, bytes, os.PathLike, Iterable[Union[str, bytes, os.PathLike]]
        ],
    ) -> None:
        """Stage a set of paths.

        Args:
          fs_paths: List of paths, relative to the repository path
        """
        root_path_bytes = os.fsencode(self.path)

        if isinstance(fs_paths, (str, bytes, os.PathLike)):
            fs_paths = [fs_paths]
        fs_paths = list(fs_paths)

        from .index import (
            _fs_to_tree_path,
            blob_from_path_and_stat,
            index_entry_from_directory,
            index_entry_from_stat,
        )

        index = self._repo.open_index()
        blob_normalizer = self._repo.get_blob_normalizer()
        for fs_path in fs_paths:
            if not isinstance(fs_path, bytes):
                fs_path = os.fsencode(fs_path)
            if os.path.isabs(fs_path):
                raise ValueError(
                    f"path {fs_path!r} should be relative to "
                    "repository root, not absolute"
                )
            tree_path = _fs_to_tree_path(fs_path)
            full_path = os.path.join(root_path_bytes, fs_path)
            try:
                st = os.lstat(full_path)
            except OSError:
                # File no longer exists
                try:
                    del index[tree_path]
                except KeyError:
                    pass  # already removed
            else:
                if stat.S_ISDIR(st.st_mode):
                    entry = index_entry_from_directory(st, full_path)
                    if entry:
                        index[tree_path] = entry
                    else:
                        try:
                            del index[tree_path]
                        except KeyError:
                            pass
                elif not stat.S_ISREG(st.st_mode) and not stat.S_ISLNK(st.st_mode):
                    try:
                        del index[tree_path]
                    except KeyError:
                        pass
                else:
                    blob = blob_from_path_and_stat(full_path, st)
                    blob = blob_normalizer.checkin_normalize(blob, fs_path)
                    self._repo.object_store.add_object(blob)
                    index[tree_path] = index_entry_from_stat(st, blob.id)
        index.write()

    def unstage(self, fs_paths: list[str]) -> None:
        """Unstage specific file in the index
        Args:
          fs_paths: a list of files to unstage,
            relative to the repository path.
        """
        from .index import IndexEntry, _fs_to_tree_path

        index = self._repo.open_index()
        try:
            tree_id = self._repo[b"HEAD"].tree
        except KeyError:
            # no head mean no commit in the repo
            for fs_path in fs_paths:
                tree_path = _fs_to_tree_path(fs_path)
                del index[tree_path]
            index.write()
            return

        for fs_path in fs_paths:
            tree_path = _fs_to_tree_path(fs_path)
            try:
                tree = self._repo.object_store[tree_id]
                assert isinstance(tree, Tree)
                tree_entry = tree.lookup_path(
                    self._repo.object_store.__getitem__, tree_path
                )
            except KeyError:
                # if tree_entry didn't exist, this file was being added, so
                # remove index entry
                try:
                    del index[tree_path]
                    continue
                except KeyError as exc:
                    raise KeyError(f"file '{tree_path.decode()}' not in index") from exc

            st = None
            try:
                st = os.lstat(os.path.join(self.path, fs_path))
            except FileNotFoundError:
                pass

            index_entry = IndexEntry(
                ctime=(self._repo[b"HEAD"].commit_time, 0),
                mtime=(self._repo[b"HEAD"].commit_time, 0),
                dev=st.st_dev if st else 0,
                ino=st.st_ino if st else 0,
                mode=tree_entry[0],
                uid=st.st_uid if st else 0,
                gid=st.st_gid if st else 0,
                size=len(self._repo[tree_entry[1]].data),
                sha=tree_entry[1],
                flags=0,
                extended_flags=0,
            )

            index[tree_path] = index_entry
        index.write()

    def commit(
        self,
        message: Optional[bytes] = None,
        committer: Optional[bytes] = None,
        author: Optional[bytes] = None,
        commit_timestamp=None,
        commit_timezone=None,
        author_timestamp=None,
        author_timezone=None,
        tree: Optional[ObjectID] = None,
        encoding: Optional[bytes] = None,
        ref: Optional[Ref] = b"HEAD",
        merge_heads: Optional[list[ObjectID]] = None,
        no_verify: bool = False,
        sign: bool = False,
    ):
        """Create a new commit.

        If not specified, committer and author default to
        get_user_identity(..., 'COMMITTER')
        and get_user_identity(..., 'AUTHOR') respectively.

        Args:
          message: Commit message (bytes or callable that takes (repo, commit)
            and returns bytes)
          committer: Committer fullname
          author: Author fullname
          commit_timestamp: Commit timestamp (defaults to now)
          commit_timezone: Commit timestamp timezone (defaults to GMT)
          author_timestamp: Author timestamp (defaults to commit
            timestamp)
          author_timezone: Author timestamp timezone
            (defaults to commit timestamp timezone)
          tree: SHA1 of the tree root to use (if not specified the
            current index will be committed).
          encoding: Encoding
          ref: Optional ref to commit to (defaults to current branch).
            If None, creates a dangling commit without updating any ref.
          merge_heads: Merge heads (defaults to .git/MERGE_HEAD)
          no_verify: Skip pre-commit and commit-msg hooks
          sign: GPG Sign the commit (bool, defaults to False,
            pass True to use default GPG key,
            pass a str containing Key ID to use a specific GPG key)

        Returns:
          New commit SHA1
        """
        try:
            if not no_verify:
                self._repo.hooks["pre-commit"].execute()
        except HookError as exc:
            raise CommitError(exc) from exc
        except KeyError:  # no hook defined, silent fallthrough
            pass

        c = Commit()
        if tree is None:
            index = self._repo.open_index()
            c.tree = index.commit(self._repo.object_store)
        else:
            if len(tree) != 40:
                raise ValueError("tree must be a 40-byte hex sha string")
            c.tree = tree

        config = self._repo.get_config_stack()
        if merge_heads is None:
            merge_heads = self._repo._read_heads("MERGE_HEAD")
        if committer is None:
            committer = get_user_identity(config, kind="COMMITTER")
        check_user_identity(committer)
        c.committer = committer
        if commit_timestamp is None:
            # FIXME: Support GIT_COMMITTER_DATE environment variable
            commit_timestamp = time.time()
        c.commit_time = int(commit_timestamp)
        if commit_timezone is None:
            # FIXME: Use current user timezone rather than UTC
            commit_timezone = 0
        c.commit_timezone = commit_timezone
        if author is None:
            author = get_user_identity(config, kind="AUTHOR")
        c.author = author
        check_user_identity(author)
        if author_timestamp is None:
            # FIXME: Support GIT_AUTHOR_DATE environment variable
            author_timestamp = commit_timestamp
        c.author_time = int(author_timestamp)
        if author_timezone is None:
            author_timezone = commit_timezone
        c.author_timezone = author_timezone
        if encoding is None:
            try:
                encoding = config.get(("i18n",), "commitEncoding")
            except KeyError:
                pass  # No dice
        if encoding is not None:
            c.encoding = encoding
        # Store original message (might be callable)
        original_message = message
        message = None  # Will be set later after parents are set

        # Check if we should sign the commit
        should_sign = sign
        if sign is None:
            # Check commit.gpgSign configuration when sign is not explicitly set
            config = self._repo.get_config_stack()
            try:
                should_sign = config.get_boolean((b"commit",), b"gpgSign")
            except KeyError:
                should_sign = False  # Default to not signing if no config
        keyid = sign if isinstance(sign, str) else None

        if ref is None:
            # Create a dangling commit
            c.parents = merge_heads
        else:
            try:
                old_head = self._repo.refs[ref]
                c.parents = [old_head, *merge_heads]
            except KeyError:
                c.parents = merge_heads

        # Handle message after parents are set
        if callable(original_message):
            message = original_message(self._repo, c)
            if message is None:
                raise ValueError("Message callback returned None")
        else:
            message = original_message

        if message is None:
            # FIXME: Try to read commit message from .git/MERGE_MSG
            raise ValueError("No commit message specified")

        try:
            if no_verify:
                c.message = message
            else:
                c.message = self._repo.hooks["commit-msg"].execute(message)
                if c.message is None:
                    c.message = message
        except HookError as exc:
            raise CommitError(exc) from exc
        except KeyError:  # no hook defined, message not modified
            c.message = message

        if ref is None:
            # Create a dangling commit
            if should_sign:
                c.sign(keyid)
            self._repo.object_store.add_object(c)
        else:
            try:
                old_head = self._repo.refs[ref]
                if should_sign:
                    c.sign(keyid)
                self._repo.object_store.add_object(c)
                ok = self._repo.refs.set_if_equals(
                    ref,
                    old_head,
                    c.id,
                    message=b"commit: " + message,
                    committer=committer,
                    timestamp=commit_timestamp,
                    timezone=commit_timezone,
                )
            except KeyError:
                c.parents = merge_heads
                if should_sign:
                    c.sign(keyid)
                self._repo.object_store.add_object(c)
                ok = self._repo.refs.add_if_new(
                    ref,
                    c.id,
                    message=b"commit: " + message,
                    committer=committer,
                    timestamp=commit_timestamp,
                    timezone=commit_timezone,
                )
            if not ok:
                # Fail if the atomic compare-and-swap failed, leaving the
                # commit and all its objects as garbage.
                raise CommitError(f"{ref!r} changed during commit")

        self._repo._del_named_file("MERGE_HEAD")

        try:
            self._repo.hooks["post-commit"].execute()
        except HookError as e:  # silent failure
            warnings.warn(f"post-commit hook failed: {e}", UserWarning)
        except KeyError:  # no hook defined, silent fallthrough
            pass

        # Trigger auto GC if needed
        from .gc import maybe_auto_gc

        maybe_auto_gc(self._repo)

        return c.id

    def reset_index(self, tree: Optional[bytes] = None):
        """Reset the index back to a specific tree.

        Args:
          tree: Tree SHA to reset to, None for current HEAD tree.
        """
        from .index import (
            build_index_from_tree,
            symlink,
            validate_path_element_default,
            validate_path_element_hfs,
            validate_path_element_ntfs,
        )

        if tree is None:
            head = self._repo[b"HEAD"]
            if isinstance(head, Tag):
                _cls, obj = head.object
                head = self._repo.get_object(obj)
            tree = head.tree
        config = self._repo.get_config()
        honor_filemode = config.get_boolean(b"core", b"filemode", os.name != "nt")
        if config.get_boolean(b"core", b"core.protectNTFS", os.name == "nt"):
            validate_path_element = validate_path_element_ntfs
        elif config.get_boolean(b"core", b"core.protectHFS", sys.platform == "darwin"):
            validate_path_element = validate_path_element_hfs
        else:
            validate_path_element = validate_path_element_default
        if config.get_boolean(b"core", b"symlinks", True):
            symlink_fn = symlink
        else:

            def symlink_fn(source, target) -> None:  # type: ignore
                with open(
                    target, "w" + ("b" if isinstance(source, bytes) else "")
                ) as f:
                    f.write(source)

        blob_normalizer = self._repo.get_blob_normalizer()
        return build_index_from_tree(
            self.path,
            self._repo.index_path(),
            self._repo.object_store,
            tree,
            honor_filemode=honor_filemode,
            validate_path_element=validate_path_element,
            symlink_fn=symlink_fn,
            blob_normalizer=blob_normalizer,
        )

    def _sparse_checkout_file_path(self) -> str:
        """Return the path of the sparse-checkout file in this repo's control dir."""
        return os.path.join(self._repo.controldir(), "info", "sparse-checkout")

    def configure_for_cone_mode(self) -> None:
        """Ensure the repository is configured for cone-mode sparse-checkout."""
        config = self._repo.get_config()
        config.set((b"core",), b"sparseCheckout", b"true")
        config.set((b"core",), b"sparseCheckoutCone", b"true")
        config.write_to_path()

    def infer_cone_mode(self) -> bool:
        """Return True if 'core.sparseCheckoutCone' is set to 'true' in config, else False."""
        config = self._repo.get_config()
        try:
            sc_cone = config.get((b"core",), b"sparseCheckoutCone")
            return sc_cone == b"true"
        except KeyError:
            # If core.sparseCheckoutCone is not set, default to False
            return False

    def get_sparse_checkout_patterns(self) -> list[str]:
        """Return a list of sparse-checkout patterns from info/sparse-checkout.

        Returns:
            A list of patterns. Returns an empty list if the file is missing.
        """
        path = self._sparse_checkout_file_path()
        try:
            with open(path, encoding="utf-8") as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []

    def set_sparse_checkout_patterns(self, patterns: list[str]) -> None:
        """Write the given sparse-checkout patterns into info/sparse-checkout.

        Creates the info/ directory if it does not exist.

        Args:
            patterns: A list of gitignore-style patterns to store.
        """
        info_dir = os.path.join(self._repo.controldir(), "info")
        os.makedirs(info_dir, exist_ok=True)

        path = self._sparse_checkout_file_path()
        with open(path, "w", encoding="utf-8") as f:
            for pat in patterns:
                f.write(pat + "\n")

    def set_cone_mode_patterns(self, dirs: Union[list[str], None] = None) -> None:
        """Write the given cone-mode directory patterns into info/sparse-checkout.

        For each directory to include, add an inclusion line that "undoes" the prior
        ``!/*/`` 'exclude' that re-includes that directory and everything under it.
        Never add the same line twice.
        """
        patterns = ["/*", "!/*/"]
        if dirs:
            for d in dirs:
                d = d.strip("/")
                line = f"/{d}/"
                if d and line not in patterns:
                    patterns.append(line)
        self.set_sparse_checkout_patterns(patterns)
