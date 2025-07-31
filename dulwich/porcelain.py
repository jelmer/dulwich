# e porcelain.py -- Porcelain-like layer on top of Dulwich
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

"""Simple wrapper that provides porcelain-like functions on top of Dulwich.

Currently implemented:
 * archive
 * add
 * bisect{_start,_bad,_good,_skip,_reset,_log,_replay}
 * branch{_create,_delete,_list}
 * check_ignore
 * checkout
 * checkout_branch
 * clone
 * cone mode{_init, _set, _add}
 * commit
 * commit_tree
 * daemon
 * describe
 * diff_tree
 * fetch
 * filter_branch
 * for_each_ref
 * init
 * ls_files
 * ls_remote
 * ls_tree
 * merge
 * merge_tree
 * mv/move
 * prune
 * pull
 * push
 * rm
 * remote{_add}
 * receive_pack
 * reset
 * revert
 * sparse_checkout
 * submodule_add
 * submodule_init
 * submodule_list
 * rev_list
 * tag{_create,_delete,_list}
 * upload_pack
 * update_server_info
 * write_commit_graph
 * status
 * symbolic_ref
 * worktree{_add,_list,_remove,_prune,_lock,_unlock,_move}

These functions are meant to behave similarly to the git subcommands.
Differences in behaviour are considered bugs.

Note: one of the consequences of this is that paths tend to be
interpreted relative to the current working directory rather than relative
to the repository root.

Functions should generally accept both unicode strings and bytestrings
"""

import datetime
import fnmatch
import logging
import os
import posixpath
import stat
import sys
import time
from collections import namedtuple
from collections.abc import Iterator
from contextlib import AbstractContextManager, closing, contextmanager
from dataclasses import dataclass
from io import BytesIO, RawIOBase
from pathlib import Path
from typing import Optional, TypeVar, Union, overload

from . import replace_me
from .archive import tar_stream
from .bisect import BisectState
from .client import get_transport_and_path
from .config import Config, ConfigFile, StackedConfig, read_submodules
from .diff_tree import (
    CHANGE_ADD,
    CHANGE_COPY,
    CHANGE_DELETE,
    CHANGE_MODIFY,
    CHANGE_RENAME,
    RENAME_CHANGE_TYPES,
    TreeChange,
    tree_changes,
)
from .errors import SendPackError
from .graph import can_fast_forward
from .ignore import IgnoreFilterManager
from .index import (
    ConflictedIndexEntry,
    IndexEntry,
    _fs_to_tree_path,
    blob_from_path_and_stat,
    build_file_from_blob,
    build_index_from_tree,
    get_unstaged_changes,
    index_entry_from_stat,
    symlink,
    update_working_tree,
    validate_path_element_default,
    validate_path_element_hfs,
    validate_path_element_ntfs,
)
from .object_store import tree_lookup_path
from .objects import (
    Blob,
    Commit,
    Tag,
    Tree,
    format_timezone,
    parse_timezone,
    pretty_format_tree_entry,
)
from .objectspec import (
    parse_commit,
    parse_object,
    parse_ref,
    parse_reftuples,
    parse_tree,
)
from .pack import write_pack_from_container, write_pack_index
from .patch import (
    get_summary,
    write_commit_patch,
    write_object_diff,
    write_tree_diff,
)
from .protocol import ZERO_SHA, Protocol
from .refs import (
    LOCAL_BRANCH_PREFIX,
    LOCAL_NOTES_PREFIX,
    LOCAL_TAG_PREFIX,
    Ref,
    SymrefLoop,
    _import_remote_refs,
)
from .repo import BaseRepo, Repo, get_user_identity
from .server import (
    FileSystemBackend,
    ReceivePackHandler,
    TCPGitServer,
    UploadPackHandler,
)
from .server import update_server_info as server_update_server_info
from .sparse_patterns import (
    SparseCheckoutConflictError,
    apply_included_paths,
    determine_included_paths,
)

# Module level tuple definition for status output
GitStatus = namedtuple("GitStatus", "staged unstaged untracked")

# TypeVar for preserving BaseRepo subclass types
T = TypeVar("T", bound="BaseRepo")

# Type alias for common repository parameter pattern
RepoPath = Union[str, os.PathLike, Repo]


@dataclass
class CountObjectsResult:
    """Result of counting objects in a repository.

    Attributes:
      count: Number of loose objects
      size: Total size of loose objects in bytes
      in_pack: Number of objects in pack files
      packs: Number of pack files
      size_pack: Total size of pack files in bytes
    """

    count: int
    size: int
    in_pack: Optional[int] = None
    packs: Optional[int] = None
    size_pack: Optional[int] = None


class NoneStream(RawIOBase):
    """Fallback if stdout or stderr are unavailable, does nothing."""

    def read(self, size=-1) -> None:
        return None

    def readall(self) -> bytes:
        return b""

    def readinto(self, b) -> None:
        return None

    def write(self, b) -> None:
        return None


default_bytes_out_stream = getattr(sys.stdout, "buffer", None) or NoneStream()
default_bytes_err_stream = getattr(sys.stderr, "buffer", None) or NoneStream()


DEFAULT_ENCODING = "utf-8"


class Error(Exception):
    """Porcelain-based error."""

    def __init__(self, msg) -> None:
        super().__init__(msg)


class RemoteExists(Error):
    """Raised when the remote already exists."""


class TimezoneFormatError(Error):
    """Raised when the timezone cannot be determined from a given string."""


class CheckoutError(Error):
    """Indicates that a checkout cannot be performed."""


def parse_timezone_format(tz_str):
    """Parse given string and attempt to return a timezone offset.

    Different formats are considered in the following order:

     - Git internal format: <unix timestamp> <timezone offset>
     - RFC 2822: e.g. Mon, 20 Nov 1995 19:12:08 -0500
     - ISO 8601: e.g. 1995-11-20T19:12:08-0500

    Args:
      tz_str: datetime string
    Returns: Timezone offset as integer
    Raises:
      TimezoneFormatError: if timezone information cannot be extracted
    """
    import re

    # Git internal format
    internal_format_pattern = re.compile("^[0-9]+ [+-][0-9]{,4}$")
    if re.match(internal_format_pattern, tz_str):
        try:
            tz_internal = parse_timezone(tz_str.split(" ")[1].encode(DEFAULT_ENCODING))
            return tz_internal[0]
        except ValueError:
            pass

    # RFC 2822
    import email.utils

    rfc_2822 = email.utils.parsedate_tz(tz_str)
    if rfc_2822:
        return rfc_2822[9]

    # ISO 8601

    # Supported offsets:
    # sHHMM, sHH:MM, sHH
    iso_8601_pattern = re.compile(
        "[0-9] ?([+-])([0-9]{2})(?::(?=[0-9]{2}))?([0-9]{2})?$"
    )
    match = re.search(iso_8601_pattern, tz_str)
    total_secs = 0
    if match:
        sign, hours, minutes = match.groups()
        total_secs += int(hours) * 3600
        if minutes:
            total_secs += int(minutes) * 60
        total_secs = -total_secs if sign == "-" else total_secs
        return total_secs

    # YYYY.MM.DD, MM/DD/YYYY, DD.MM.YYYY contain no timezone information

    raise TimezoneFormatError(tz_str)


def get_user_timezones():
    """Retrieve local timezone as described in
    https://raw.githubusercontent.com/git/git/v2.3.0/Documentation/date-formats.txt
    Returns: A tuple containing author timezone, committer timezone.
    """
    local_timezone = time.localtime().tm_gmtoff

    if os.environ.get("GIT_AUTHOR_DATE"):
        author_timezone = parse_timezone_format(os.environ["GIT_AUTHOR_DATE"])
    else:
        author_timezone = local_timezone
    if os.environ.get("GIT_COMMITTER_DATE"):
        commit_timezone = parse_timezone_format(os.environ["GIT_COMMITTER_DATE"])
    else:
        commit_timezone = local_timezone

    return author_timezone, commit_timezone


@overload
def open_repo(path_or_repo: T) -> AbstractContextManager[T]: ...


@overload
def open_repo(
    path_or_repo: Union[str, os.PathLike],
) -> AbstractContextManager[Repo]: ...


def open_repo(
    path_or_repo: Union[str, os.PathLike, T],
) -> AbstractContextManager[Union[T, Repo]]:
    """Open an argument that can be a repository or a path for a repository."""
    if isinstance(path_or_repo, BaseRepo):
        return _noop_context_manager(path_or_repo)
    return Repo(path_or_repo)


@contextmanager
def _noop_context_manager(obj):
    """Context manager that has the same api as closing but does nothing."""
    yield obj


@overload
def open_repo_closing(path_or_repo: T) -> AbstractContextManager[T]: ...


@overload
def open_repo_closing(
    path_or_repo: Union[str, os.PathLike],
) -> AbstractContextManager[Repo]: ...


def open_repo_closing(
    path_or_repo: Union[str, os.PathLike, T],
) -> AbstractContextManager[Union[T, Repo]]:
    """Open an argument that can be a repository or a path for a repository.
    returns a context manager that will close the repo on exit if the argument
    is a path, else does nothing if the argument is a repo.
    """
    if isinstance(path_or_repo, BaseRepo):
        return _noop_context_manager(path_or_repo)
    return closing(Repo(path_or_repo))


def path_to_tree_path(
    repopath: Union[str, os.PathLike], path, tree_encoding=DEFAULT_ENCODING
):
    """Convert a path to a path usable in an index, e.g. bytes and relative to
    the repository root.

    Args:
      repopath: Repository path, absolute or relative to the cwd
      path: A path, absolute or relative to the cwd
    Returns: A path formatted for use in e.g. an index
    """
    # Resolve might returns a relative path on Windows
    # https://bugs.python.org/issue38671
    if sys.platform == "win32":
        path = os.path.abspath(path)

    path = Path(path)
    resolved_path = path.resolve()

    # Resolve and abspath seems to behave differently regarding symlinks,
    # as we are doing abspath on the file path, we need to do the same on
    # the repo path or they might not match
    if sys.platform == "win32":
        repopath = os.path.abspath(repopath)

    repopath = Path(repopath).resolve()

    try:
        relpath = resolved_path.relative_to(repopath)
    except ValueError:
        # If path is a symlink that points to a file outside the repo, we
        # want the relpath for the link itself, not the resolved target
        if path.is_symlink():
            parent = path.parent.resolve()
            relpath = (parent / path.name).relative_to(repopath)
        else:
            raise
    if sys.platform == "win32":
        return str(relpath).replace(os.path.sep, "/").encode(tree_encoding)
    else:
        return bytes(relpath)


class DivergedBranches(Error):
    """Branches have diverged and fast-forward is not possible."""

    def __init__(self, current_sha, new_sha) -> None:
        self.current_sha = current_sha
        self.new_sha = new_sha


def check_diverged(repo, current_sha, new_sha) -> None:
    """Check if updating to a sha can be done with fast forwarding.

    Args:
      repo: Repository object
      current_sha: Current head sha
      new_sha: New head sha
    """
    try:
        can = can_fast_forward(repo, current_sha, new_sha)
    except KeyError:
        can = False
    if not can:
        raise DivergedBranches(current_sha, new_sha)


def archive(
    repo,
    committish: Optional[Union[str, bytes, Commit, Tag]] = None,
    outstream=default_bytes_out_stream,
    errstream=default_bytes_err_stream,
) -> None:
    """Create an archive.

    Args:
      repo: Path of repository for which to generate an archive.
      committish: Commit SHA1 or ref to use
      outstream: Output stream (defaults to stdout)
      errstream: Error stream (defaults to stderr)
    """
    if committish is None:
        committish = "HEAD"
    with open_repo_closing(repo) as repo_obj:
        c = parse_commit(repo_obj, committish)
        for chunk in tar_stream(
            repo_obj.object_store, repo_obj.object_store[c.tree], c.commit_time
        ):
            outstream.write(chunk)


def update_server_info(repo: RepoPath = ".") -> None:
    """Update server info files for a repository.

    Args:
      repo: path to the repository
    """
    with open_repo_closing(repo) as r:
        server_update_server_info(r)


def write_commit_graph(repo: RepoPath = ".", reachable=True) -> None:
    """Write a commit graph file for a repository.

    Args:
      repo: path to the repository or a Repo object
      reachable: if True, include all commits reachable from refs.
                 if False, only include direct ref targets.
    """
    with open_repo_closing(repo) as r:
        # Get all refs
        refs = list(r.refs.as_dict().values())
        if refs:
            r.object_store.write_commit_graph(refs, reachable=reachable)


def symbolic_ref(repo: RepoPath, ref_name, force=False) -> None:
    """Set git symbolic ref into HEAD.

    Args:
      repo: path to the repository
      ref_name: short name of the new ref
      force: force settings without checking if it exists in refs/heads
    """
    with open_repo_closing(repo) as repo_obj:
        ref_path = _make_branch_ref(ref_name)
        if not force and ref_path not in repo_obj.refs.keys():
            raise Error(f"fatal: ref `{ref_name}` is not a ref")
        repo_obj.refs.set_symbolic_ref(b"HEAD", ref_path)


def pack_refs(repo: RepoPath, all=False) -> None:
    with open_repo_closing(repo) as repo_obj:
        repo_obj.refs.pack_refs(all=all)


def commit(
    repo=".",
    message=None,
    author=None,
    author_timezone=None,
    committer=None,
    commit_timezone=None,
    encoding=None,
    no_verify=False,
    signoff=False,
    all=False,
    amend=False,
):
    """Create a new commit.

    Args:
      repo: Path to repository
      message: Optional commit message (string/bytes or callable that takes
        (repo, commit) and returns bytes)
      author: Optional author name and email
      author_timezone: Author timestamp timezone
      committer: Optional committer name and email
      commit_timezone: Commit timestamp timezone
      no_verify: Skip pre-commit and commit-msg hooks
      signoff: GPG Sign the commit (bool, defaults to False,
        pass True to use default GPG key,
        pass a str containing Key ID to use a specific GPG key)
      all: Automatically stage all tracked files that have been modified
      amend: Replace the tip of the current branch by creating a new commit
    Returns: SHA1 of the new commit
    """
    if getattr(message, "encode", None):
        message = message.encode(encoding or DEFAULT_ENCODING)
    if getattr(author, "encode", None):
        author = author.encode(encoding or DEFAULT_ENCODING)
    if getattr(committer, "encode", None):
        committer = committer.encode(encoding or DEFAULT_ENCODING)
    local_timezone = get_user_timezones()
    if author_timezone is None:
        author_timezone = local_timezone[0]
    if commit_timezone is None:
        commit_timezone = local_timezone[1]

    with open_repo_closing(repo) as r:
        # Handle amend logic
        merge_heads = None
        if amend:
            try:
                head_commit = r[r.head()]
            except KeyError:
                raise ValueError("Cannot amend: no existing commit found")

            # If message not provided, use the message from the current HEAD
            if message is None:
                message = head_commit.message
            # If author not provided, use the author from the current HEAD
            if author is None:
                author = head_commit.author
                if author_timezone is None:
                    author_timezone = head_commit.author_timezone
            # Use the parent(s) of the current HEAD as our parent(s)
            merge_heads = list(head_commit.parents)

        # If -a flag is used, stage all modified tracked files
        if all:
            index = r.open_index()
            normalizer = r.get_blob_normalizer()
            filter_callback = normalizer.checkin_normalize
            unstaged_changes = list(
                get_unstaged_changes(index, r.path, filter_callback)
            )

            if unstaged_changes:
                # Convert bytes paths to strings for add function
                modified_files = []
                for path in unstaged_changes:
                    if isinstance(path, bytes):
                        path = path.decode()
                    modified_files.append(path)

                add(r, paths=modified_files)

        commit_kwargs = {
            "message": message,
            "author": author,
            "author_timezone": author_timezone,
            "committer": committer,
            "commit_timezone": commit_timezone,
            "encoding": encoding,
            "no_verify": no_verify,
            "sign": signoff if isinstance(signoff, (str, bool)) else None,
            "merge_heads": merge_heads,
        }

        # For amend, create dangling commit to avoid adding current HEAD as parent
        if amend:
            commit_kwargs["ref"] = None
            commit_sha = r.get_worktree().commit(**commit_kwargs)
            # Update HEAD to point to the new commit
            r.refs[b"HEAD"] = commit_sha
            return commit_sha
        else:
            return r.get_worktree().commit(**commit_kwargs)


def commit_tree(
    repo: RepoPath,
    tree,
    message=None,
    author=None,
    committer=None,
):
    """Create a new commit object.

    Args:
      repo: Path to repository
      tree: An existing tree object
      author: Optional author name and email
      committer: Optional committer name and email
    """
    with open_repo_closing(repo) as r:
        return r.get_worktree().commit(
            message=message, tree=tree, committer=committer, author=author
        )


def init(
    path: Union[str, os.PathLike] = ".", *, bare=False, symlinks: Optional[bool] = None
):
    """Create a new git repository.

    Args:
      path: Path to repository.
      bare: Whether to create a bare repository.
      symlinks: Whether to create actual symlinks (defaults to autodetect)
    Returns: A Repo instance
    """
    if not os.path.exists(path):
        os.mkdir(path)

    if bare:
        return Repo.init_bare(path)
    else:
        return Repo.init(path, symlinks=symlinks)


def clone(
    source,
    target: Optional[Union[str, os.PathLike]] = None,
    bare=False,
    checkout=None,
    errstream=default_bytes_err_stream,
    outstream=None,
    origin: Optional[str] = "origin",
    depth: Optional[int] = None,
    branch: Optional[Union[str, bytes]] = None,
    config: Optional[Config] = None,
    filter_spec=None,
    protocol_version: Optional[int] = None,
    recurse_submodules: bool = False,
    **kwargs,
):
    """Clone a local or remote git repository.

    Args:
      source: Path or URL for source repository
      target: Path to target repository (optional)
      bare: Whether or not to create a bare repository
      checkout: Whether or not to check-out HEAD after cloning
      errstream: Optional stream to write progress to
      outstream: Optional stream to write progress to (deprecated)
      origin: Name of remote from the repository used to clone
      depth: Depth to fetch at
      branch: Optional branch or tag to be used as HEAD in the new repository
        instead of the cloned repository's HEAD.
      config: Configuration to use
      filter_spec: A git-rev-list-style object filter spec, as an ASCII string.
        Only used if the server supports the Git protocol-v2 'filter'
        feature, and ignored otherwise.
      protocol_version: desired Git protocol version. By default the highest
        mutually supported protocol version will be used.
      recurse_submodules: Whether to initialize and clone submodules

    Keyword Args:
      refspecs: refspecs to fetch. Can be a bytestring, a string, or a list of
        bytestring/string.

    Returns: The new repository
    """
    if outstream is not None:
        import warnings

        warnings.warn(
            "outstream= has been deprecated in favour of errstream=.",
            DeprecationWarning,
            stacklevel=3,
        )
        # TODO(jelmer): Capture logging output and stream to errstream

    if config is None:
        config = StackedConfig.default()

    if checkout is None:
        checkout = not bare
    if checkout and bare:
        raise Error("checkout and bare are incompatible")

    if target is None:
        target = source.split("/")[-1]

    if isinstance(branch, str):
        branch = branch.encode(DEFAULT_ENCODING)

    mkdir = not os.path.exists(target)

    (client, path) = get_transport_and_path(source, config=config, **kwargs)

    if filter_spec:
        filter_spec = filter_spec.encode("ascii")

    repo = client.clone(
        path,
        target,
        mkdir=mkdir,
        bare=bare,
        origin=origin,
        checkout=checkout,
        branch=branch,
        progress=errstream.write,
        depth=depth,
        filter_spec=filter_spec,
        protocol_version=protocol_version,
    )

    # Initialize and update submodules if requested
    if recurse_submodules and not bare:
        try:
            submodule_init(repo)
            submodule_update(repo, init=True)
        except FileNotFoundError as e:
            # .gitmodules file doesn't exist - no submodules to process
            logging.debug("No .gitmodules file found: %s", e)
        except KeyError as e:
            # Submodule configuration missing
            logging.warning("Submodule configuration error: %s", e)
            if errstream:
                errstream.write(
                    f"Warning: Submodule configuration error: {e}\n".encode()
                )

    return repo


def add(repo: Union[str, os.PathLike, Repo] = ".", paths=None):
    """Add files to the staging area.

    Args:
      repo: Repository for the files
      paths: Paths to add. If None, stages all untracked and modified files from the
        current working directory (mimicking 'git add .' behavior).
    Returns: Tuple with set of added files and ignored files

    If the repository contains ignored directories, the returned set will
    contain the path to an ignored directory (with trailing slash). Individual
    files within ignored directories will not be returned.

    Note: When paths=None, this function adds all untracked and modified files
    from the entire repository, mimicking 'git add -A' behavior.
    """
    ignored = set()
    with open_repo_closing(repo) as r:
        repo_path = Path(r.path).resolve()
        ignore_manager = IgnoreFilterManager.from_repo(r)

        # Get unstaged changes once for the entire operation
        index = r.open_index()
        normalizer = r.get_blob_normalizer()
        filter_callback = normalizer.checkin_normalize
        all_unstaged_paths = list(get_unstaged_changes(index, r.path, filter_callback))

        if not paths:
            # When no paths specified, add all untracked and modified files from repo root
            paths = [str(repo_path)]
        relpaths = []
        if not isinstance(paths, list):
            paths = [paths]
        for p in paths:
            path = Path(p)
            if not path.is_absolute():
                # Make relative paths relative to the repo directory
                path = repo_path / path

            # Don't resolve symlinks completely - only resolve the parent directory
            # to avoid issues when symlinks point outside the repository
            if path.is_symlink():
                # For symlinks, resolve only the parent directory
                parent_resolved = path.parent.resolve()
                resolved_path = parent_resolved / path.name
            else:
                # For regular files/dirs, resolve normally
                resolved_path = path.resolve()

            try:
                relpath = str(resolved_path.relative_to(repo_path)).replace(os.sep, "/")
            except ValueError as e:
                # Path is not within the repository
                raise ValueError(
                    f"Path {p} is not within repository {repo_path}"
                ) from e

            # Handle directories by scanning their contents
            if resolved_path.is_dir():
                # Check if the directory itself is ignored
                dir_relpath = posixpath.join(relpath, "") if relpath != "." else ""
                if dir_relpath and ignore_manager.is_ignored(dir_relpath):
                    ignored.add(dir_relpath)
                    continue

                # When adding a directory, add all untracked files within it
                current_untracked = list(
                    get_untracked_paths(
                        str(resolved_path),
                        str(repo_path),
                        index,
                    )
                )
                for untracked_path in current_untracked:
                    # If we're scanning a subdirectory, adjust the path
                    if relpath != ".":
                        untracked_path = posixpath.join(relpath, untracked_path)

                    if not ignore_manager.is_ignored(untracked_path):
                        relpaths.append(untracked_path)
                    else:
                        ignored.add(untracked_path)

                # Also add unstaged (modified) files within this directory
                for unstaged_path in all_unstaged_paths:
                    if isinstance(unstaged_path, bytes):
                        unstaged_path_str = unstaged_path.decode("utf-8")
                    else:
                        unstaged_path_str = unstaged_path

                    # Check if this unstaged file is within the directory we're processing
                    unstaged_full_path = repo_path / unstaged_path_str
                    try:
                        unstaged_full_path.relative_to(resolved_path)
                        # File is within this directory, add it
                        if not ignore_manager.is_ignored(unstaged_path_str):
                            relpaths.append(unstaged_path_str)
                        else:
                            ignored.add(unstaged_path_str)
                    except ValueError:
                        # File is not within this directory, skip it
                        continue
                continue

            # FIXME: Support patterns
            if ignore_manager.is_ignored(relpath):
                ignored.add(relpath)
                continue
            relpaths.append(relpath)
        r.get_worktree().stage(relpaths)
    return (relpaths, ignored)


def _is_subdir(subdir, parentdir):
    """Check whether subdir is parentdir or a subdir of parentdir.

    If parentdir or subdir is a relative path, it will be disamgibuated
    relative to the pwd.
    """
    parentdir_abs = os.path.realpath(parentdir) + os.path.sep
    subdir_abs = os.path.realpath(subdir) + os.path.sep
    return subdir_abs.startswith(parentdir_abs)


# TODO: option to remove ignored files also, in line with `git clean -fdx`
def clean(repo: Union[str, os.PathLike, Repo] = ".", target_dir=None) -> None:
    """Remove any untracked files from the target directory recursively.

    Equivalent to running ``git clean -fd`` in target_dir.

    Args:
      repo: Repository where the files may be tracked
      target_dir: Directory to clean - current directory if None
    """
    if target_dir is None:
        target_dir = os.getcwd()

    with open_repo_closing(repo) as r:
        if not _is_subdir(target_dir, r.path):
            raise Error("target_dir must be in the repo's working dir")

        config = r.get_config_stack()
        config.get_boolean((b"clean",), b"requireForce", True)

        # TODO(jelmer): if require_force is set, then make sure that -f, -i or
        # -n is specified.

        index = r.open_index()
        ignore_manager = IgnoreFilterManager.from_repo(r)

        paths_in_wd = _walk_working_dir_paths(target_dir, r.path)
        # Reverse file visit order, so that files and subdirectories are
        # removed before containing directory
        for ap, is_dir in reversed(list(paths_in_wd)):
            if is_dir:
                # All subdirectories and files have been removed if untracked,
                # so dir contains no tracked files iff it is empty.
                is_empty = len(os.listdir(ap)) == 0
                if is_empty:
                    os.rmdir(ap)
            else:
                ip = path_to_tree_path(r.path, ap)
                is_tracked = ip in index

                rp = os.path.relpath(ap, r.path)
                is_ignored = ignore_manager.is_ignored(rp)

                if not is_tracked and not is_ignored:
                    os.remove(ap)


def remove(repo: Union[str, os.PathLike, Repo] = ".", paths=None, cached=False) -> None:
    """Remove files from the staging area.

    Args:
      repo: Repository for the files
      paths: Paths to remove. Can be absolute or relative to the repository root.
    """
    with open_repo_closing(repo) as r:
        index = r.open_index()
        blob_normalizer = r.get_blob_normalizer()

        for p in paths:
            # If path is absolute, use it as-is. Otherwise, treat it as relative to repo
            if os.path.isabs(p):
                full_path = p
            else:
                # Treat relative paths as relative to the repository root
                full_path = os.path.join(r.path, p)
            tree_path = path_to_tree_path(r.path, full_path)
            # Convert to bytes for file operations
            full_path_bytes = os.fsencode(full_path)
            try:
                entry = index[tree_path]
                if isinstance(entry, ConflictedIndexEntry):
                    raise Error(f"{p} has conflicts in the index")
                index_sha = entry.sha
            except KeyError as exc:
                raise Error(f"{p} did not match any files") from exc

            if not cached:
                try:
                    st = os.lstat(full_path_bytes)
                except OSError:
                    pass
                else:
                    try:
                        blob = blob_from_path_and_stat(full_path_bytes, st)
                        # Apply checkin normalization to compare apples to apples
                        if blob_normalizer is not None:
                            blob = blob_normalizer.checkin_normalize(blob, tree_path)
                    except OSError:
                        pass
                    else:
                        try:
                            committed_sha = tree_lookup_path(
                                r.__getitem__, r[r.head()].tree, tree_path
                            )[1]
                        except KeyError:
                            committed_sha = None

                        if blob.id != index_sha and index_sha != committed_sha:
                            raise Error(
                                "file has staged content differing "
                                f"from both the file and head: {p}"
                            )

                        if index_sha != committed_sha:
                            raise Error(f"file has staged changes: {p}")
                        os.remove(full_path_bytes)
            del index[tree_path]
        index.write()


rm = remove


def mv(
    repo: Union[str, os.PathLike, Repo],
    source: Union[str, bytes, os.PathLike],
    destination: Union[str, bytes, os.PathLike],
    force: bool = False,
) -> None:
    """Move or rename a file, directory, or symlink.

    Args:
      repo: Path to the repository
      source: Path to move from
      destination: Path to move to
      force: Force move even if destination exists

    Raises:
      Error: If source doesn't exist, is not tracked, or destination already exists (without force)
    """
    with open_repo_closing(repo) as r:
        index = r.open_index()

        # Handle paths - convert to string if necessary
        if isinstance(source, bytes):
            source = source.decode(sys.getfilesystemencoding())
        elif hasattr(source, "__fspath__"):
            source = os.fspath(source)
        else:
            source = str(source)

        if isinstance(destination, bytes):
            destination = destination.decode(sys.getfilesystemencoding())
        elif hasattr(destination, "__fspath__"):
            destination = os.fspath(destination)
        else:
            destination = str(destination)

        # Get full paths
        if os.path.isabs(source):
            source_full_path = source
        else:
            # Treat relative paths as relative to the repository root
            source_full_path = os.path.join(r.path, source)

        if os.path.isabs(destination):
            destination_full_path = destination
        else:
            # Treat relative paths as relative to the repository root
            destination_full_path = os.path.join(r.path, destination)

        # Check if destination is a directory
        if os.path.isdir(destination_full_path):
            # Move source into destination directory
            basename = os.path.basename(source_full_path)
            destination_full_path = os.path.join(destination_full_path, basename)

        # Convert to tree paths for index
        source_tree_path = path_to_tree_path(r.path, source_full_path)
        destination_tree_path = path_to_tree_path(r.path, destination_full_path)

        # Check if source exists in index
        if source_tree_path not in index:
            raise Error(f"source '{source}' is not under version control")

        # Check if source exists in filesystem
        if not os.path.exists(source_full_path):
            raise Error(f"source '{source}' does not exist")

        # Check if destination already exists
        if os.path.exists(destination_full_path) and not force:
            raise Error(f"destination '{destination}' already exists (use -f to force)")

        # Check if destination is already in index
        if destination_tree_path in index and not force:
            raise Error(
                f"destination '{destination}' already exists in index (use -f to force)"
            )

        # Get the index entry for the source
        source_entry = index[source_tree_path]

        # Convert to bytes for file operations
        source_full_path_bytes = os.fsencode(source_full_path)
        destination_full_path_bytes = os.fsencode(destination_full_path)

        # Create parent directory for destination if needed
        dest_dir = os.path.dirname(destination_full_path_bytes)
        if dest_dir and not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        # Move the file in the filesystem
        if os.path.exists(destination_full_path_bytes) and force:
            os.remove(destination_full_path_bytes)
        os.rename(source_full_path_bytes, destination_full_path_bytes)

        # Update the index
        del index[source_tree_path]
        index[destination_tree_path] = source_entry
        index.write()


move = mv


def commit_decode(commit, contents, default_encoding=DEFAULT_ENCODING):
    if commit.encoding:
        encoding = commit.encoding.decode("ascii")
    else:
        encoding = default_encoding
    return contents.decode(encoding, "replace")


def commit_encode(commit, contents, default_encoding=DEFAULT_ENCODING):
    if commit.encoding:
        encoding = commit.encoding.decode("ascii")
    else:
        encoding = default_encoding
    return contents.encode(encoding)


def print_commit(commit, decode, outstream=sys.stdout) -> None:
    """Write a human-readable commit log entry.

    Args:
      commit: A `Commit` object
      outstream: A stream file to write to
    """
    outstream.write("-" * 50 + "\n")
    outstream.write("commit: " + commit.id.decode("ascii") + "\n")
    if len(commit.parents) > 1:
        outstream.write(
            "merge: "
            + "...".join([c.decode("ascii") for c in commit.parents[1:]])
            + "\n"
        )
    outstream.write("Author: " + decode(commit.author) + "\n")
    if commit.author != commit.committer:
        outstream.write("Committer: " + decode(commit.committer) + "\n")

    time_tuple = time.gmtime(commit.author_time + commit.author_timezone)
    time_str = time.strftime("%a %b %d %Y %H:%M:%S", time_tuple)
    timezone_str = format_timezone(commit.author_timezone).decode("ascii")
    outstream.write("Date:   " + time_str + " " + timezone_str + "\n")
    if commit.message:
        outstream.write("\n")
        outstream.write(decode(commit.message) + "\n")
        outstream.write("\n")


def print_tag(tag, decode, outstream=sys.stdout) -> None:
    """Write a human-readable tag.

    Args:
      tag: A `Tag` object
      decode: Function for decoding bytes to unicode string
      outstream: A stream to write to
    """
    outstream.write("Tagger: " + decode(tag.tagger) + "\n")
    time_tuple = time.gmtime(tag.tag_time + tag.tag_timezone)
    time_str = time.strftime("%a %b %d %Y %H:%M:%S", time_tuple)
    timezone_str = format_timezone(tag.tag_timezone).decode("ascii")
    outstream.write("Date:   " + time_str + " " + timezone_str + "\n")
    outstream.write("\n")
    outstream.write(decode(tag.message))
    outstream.write("\n")


def show_blob(repo: RepoPath, blob, decode, outstream=sys.stdout) -> None:
    """Write a blob to a stream.

    Args:
      repo: A `Repo` object
      blob: A `Blob` object
      decode: Function for decoding bytes to unicode string
      outstream: A stream file to write to
    """
    outstream.write(decode(blob.data))


def show_commit(repo: RepoPath, commit, decode, outstream=sys.stdout) -> None:
    """Show a commit to a stream.

    Args:
      repo: A `Repo` object
      commit: A `Commit` object
      decode: Function for decoding bytes to unicode string
      outstream: Stream to write to
    """
    with open_repo_closing(repo) as r:
        print_commit(commit, decode=decode, outstream=outstream)
        if commit.parents:
            parent_commit = r[commit.parents[0]]
            base_tree = parent_commit.tree
        else:
            base_tree = None
        diffstream = BytesIO()
        write_tree_diff(diffstream, r.object_store, base_tree, commit.tree)
    diffstream.seek(0)
    outstream.write(commit_decode(commit, diffstream.getvalue()))


def show_tree(repo: RepoPath, tree, decode, outstream=sys.stdout) -> None:
    """Print a tree to a stream.

    Args:
      repo: A `Repo` object
      tree: A `Tree` object
      decode: Function for decoding bytes to unicode string
      outstream: Stream to write to
    """
    for n in tree:
        outstream.write(decode(n) + "\n")


def show_tag(repo: RepoPath, tag, decode, outstream=sys.stdout) -> None:
    """Print a tag to a stream.

    Args:
      repo: A `Repo` object
      tag: A `Tag` object
      decode: Function for decoding bytes to unicode string
      outstream: Stream to write to
    """
    with open_repo_closing(repo) as r:
        print_tag(tag, decode, outstream)
        show_object(repo, r[tag.object[1]], decode, outstream)


def show_object(repo: RepoPath, obj, decode, outstream):
    return {
        b"tree": show_tree,
        b"blob": show_blob,
        b"commit": show_commit,
        b"tag": show_tag,
    }[obj.type_name](repo, obj, decode, outstream)


def print_name_status(changes):
    """Print a simple status summary, listing changed files."""
    for change in changes:
        if not change:
            continue
        if isinstance(change, list):
            change = change[0]
        if change.type == CHANGE_ADD:
            path1 = change.new.path
            path2 = ""
            kind = "A"
        elif change.type == CHANGE_DELETE:
            path1 = change.old.path
            path2 = ""
            kind = "D"
        elif change.type == CHANGE_MODIFY:
            path1 = change.new.path
            path2 = ""
            kind = "M"
        elif change.type in RENAME_CHANGE_TYPES:
            path1 = change.old.path
            path2 = change.new.path
            if change.type == CHANGE_RENAME:
                kind = "R"
            elif change.type == CHANGE_COPY:
                kind = "C"
        yield "%-8s%-20s%-20s" % (kind, path1, path2)  # noqa: UP031


def log(
    repo=".",
    paths=None,
    outstream=sys.stdout,
    max_entries=None,
    reverse=False,
    name_status=False,
) -> None:
    """Write commit logs.

    Args:
      repo: Path to repository
      paths: Optional set of specific paths to print entries for
      outstream: Stream to write log output to
      reverse: Reverse order in which entries are printed
      name_status: Print name status
      max_entries: Optional maximum number of entries to display
    """
    with open_repo_closing(repo) as r:
        try:
            include = [r.head()]
        except KeyError:
            include = []
        walker = r.get_walker(
            include=include, max_entries=max_entries, paths=paths, reverse=reverse
        )
        for entry in walker:

            def decode(x):
                return commit_decode(entry.commit, x)

            print_commit(entry.commit, decode, outstream)
            if name_status:
                outstream.writelines(
                    [line + "\n" for line in print_name_status(entry.changes())]
                )


# TODO(jelmer): better default for encoding?
def show(
    repo=".",
    objects=None,
    outstream=sys.stdout,
    default_encoding=DEFAULT_ENCODING,
) -> None:
    """Print the changes in a commit.

    Args:
      repo: Path to repository
      objects: Objects to show (defaults to [HEAD])
      outstream: Stream to write to
      default_encoding: Default encoding to use if none is set in the
        commit
    """
    if objects is None:
        objects = ["HEAD"]
    if not isinstance(objects, list):
        objects = [objects]
    with open_repo_closing(repo) as r:
        for objectish in objects:
            o = parse_object(r, objectish)
            if isinstance(o, Commit):

                def decode(x):
                    return commit_decode(o, x, default_encoding)

            else:

                def decode(x):
                    return x.decode(default_encoding)

            show_object(r, o, decode, outstream)


def diff_tree(
    repo: RepoPath,
    old_tree,
    new_tree,
    outstream=default_bytes_out_stream,
) -> None:
    """Compares the content and mode of blobs found via two tree objects.

    Args:
      repo: Path to repository
      old_tree: Id of old tree
      new_tree: Id of new tree
      outstream: Stream to write to
    """
    with open_repo_closing(repo) as r:
        write_tree_diff(outstream, r.object_store, old_tree, new_tree)


def diff(
    repo=".",
    commit=None,
    commit2=None,
    staged=False,
    paths=None,
    outstream=default_bytes_out_stream,
) -> None:
    """Show diff.

    Args:
      repo: Path to repository
      commit: First commit to compare. If staged is True, compare
              index to this commit. If staged is False, compare working tree
              to this commit. If None, defaults to HEAD for staged and index
              for unstaged.
      commit2: Second commit to compare against first commit. If provided,
               show diff between commit and commit2 (ignoring staged flag).
      staged: If True, show staged changes (index vs commit).
              If False, show unstaged changes (working tree vs commit/index).
              Ignored if commit2 is provided.
      paths: Optional list of paths to limit diff
      outstream: Stream to write to
    """
    from . import diff as diff_module

    with open_repo_closing(repo) as r:
        # Normalize paths to bytes
        if paths is not None and paths:  # Check if paths is not empty
            byte_paths = []
            for p in paths:
                if isinstance(p, str):
                    byte_paths.append(p.encode("utf-8"))
                else:
                    byte_paths.append(p)
            paths = byte_paths
        elif paths == []:  # Convert empty list to None
            paths = None

        # Resolve commit refs to SHAs if provided
        if commit is not None:
            if isinstance(commit, Commit):
                # Already a Commit object
                commit_sha = commit.id
                commit_obj = commit
            else:
                # parse_commit handles both refs and SHAs, and always returns a Commit object
                commit_obj = parse_commit(r, commit)
                commit_sha = commit_obj.id
        else:
            commit_sha = None
            commit_obj = None

        if commit2 is not None:
            # Compare two commits
            if isinstance(commit2, Commit):
                commit2_obj = commit2
            else:
                commit2_obj = parse_commit(r, commit2)

            # Get trees from commits
            old_tree = commit_obj.tree if commit_obj else None
            new_tree = commit2_obj.tree

            # Use tree_changes to get the changes and apply path filtering
            changes = r.object_store.tree_changes(old_tree, new_tree)
            for (oldpath, newpath), (oldmode, newmode), (oldsha, newsha) in changes:
                # Skip if paths are specified and this change doesn't match
                if paths:
                    path_to_check = newpath or oldpath
                    if not any(
                        path_to_check == p or path_to_check.startswith(p + b"/")
                        for p in paths
                    ):
                        continue

                write_object_diff(
                    outstream,
                    r.object_store,
                    (oldpath, oldmode, oldsha),
                    (newpath, newmode, newsha),
                )
        elif staged:
            # Show staged changes (index vs commit)
            diff_module.diff_index_to_tree(r, outstream, commit_sha, paths)
        elif commit is not None:
            # Compare working tree to a specific commit
            assert (
                commit_sha is not None
            )  # mypy: commit_sha is set when commit is not None
            diff_module.diff_working_tree_to_tree(r, outstream, commit_sha, paths)
        else:
            # Compare working tree to index
            diff_module.diff_working_tree_to_index(r, outstream, paths)


def rev_list(repo: RepoPath, commits, outstream=sys.stdout) -> None:
    """Lists commit objects in reverse chronological order.

    Args:
      repo: Path to repository
      commits: Commits over which to iterate
      outstream: Stream to write to
    """
    with open_repo_closing(repo) as r:
        for entry in r.get_walker(include=[r[c].id for c in commits]):
            outstream.write(entry.commit.id + b"\n")


def _canonical_part(url: str) -> str:
    name = url.rsplit("/", 1)[-1]
    if name.endswith(".git"):
        name = name[:-4]
    return name


def submodule_add(
    repo: Union[str, os.PathLike, Repo], url, path=None, name=None
) -> None:
    """Add a new submodule.

    Args:
      repo: Path to repository
      url: URL of repository to add as submodule
      path: Path where submodule should live
      name: Name for the submodule
    """
    with open_repo_closing(repo) as r:
        if path is None:
            path = os.path.relpath(_canonical_part(url), r.path)
        if name is None:
            name = path

        # TODO(jelmer): Move this logic to dulwich.submodule
        gitmodules_path = os.path.join(r.path, ".gitmodules")
        try:
            config = ConfigFile.from_path(gitmodules_path)
        except FileNotFoundError:
            config = ConfigFile()
            config.path = gitmodules_path
        config.set(("submodule", name), "url", url)
        config.set(("submodule", name), "path", path)
        config.write_to_path()


def submodule_init(repo: Union[str, os.PathLike, Repo]) -> None:
    """Initialize submodules.

    Args:
      repo: Path to repository
    """
    with open_repo_closing(repo) as r:
        config = r.get_config()
        gitmodules_path = os.path.join(r.path, ".gitmodules")
        for path, url, name in read_submodules(gitmodules_path):
            config.set((b"submodule", name), b"active", True)
            config.set((b"submodule", name), b"url", url)
        config.write_to_path()


def submodule_list(repo: RepoPath):
    """List submodules.

    Args:
      repo: Path to repository
    """
    from .submodule import iter_cached_submodules

    with open_repo_closing(repo) as r:
        for path, sha in iter_cached_submodules(r.object_store, r[r.head()].tree):
            yield path, sha.decode(DEFAULT_ENCODING)


def submodule_update(
    repo: Union[str, os.PathLike, Repo],
    paths=None,
    init=False,
    force=False,
    errstream=None,
) -> None:
    """Update submodules.

    Args:
      repo: Path to repository
      paths: Optional list of specific submodule paths to update. If None, updates all.
      init: If True, initialize submodules first
      force: Force update even if local changes exist
    """
    from .submodule import iter_cached_submodules

    with open_repo_closing(repo) as r:
        if init:
            submodule_init(r)

        config = r.get_config()
        gitmodules_path = os.path.join(r.path, ".gitmodules")

        # Get list of submodules to update
        submodules_to_update = []
        for path, sha in iter_cached_submodules(r.object_store, r[r.head()].tree):
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

            # Find the submodule name from .gitmodules
            submodule_name: Optional[bytes] = None
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
            submodule_git_dir = os.path.join(r.path, ".git", "modules", path_str)

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
                depth = path_str.count("/") + 1
                relative_git_dir = "../" * depth + ".git/modules/" + path_str
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
                    sub_repo.refs[b"HEAD"] = target_sha

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
                    client.fetch(path_segments, sub_repo)

                    # Update to the target commit
                    sub_repo.refs[b"HEAD"] = target_sha

                    # Reset the working directory
                    reset(sub_repo, "hard", target_sha)


def tag_create(
    repo,
    tag: Union[str, bytes],
    author: Optional[Union[str, bytes]] = None,
    message: Optional[Union[str, bytes]] = None,
    annotated=False,
    objectish: Union[str, bytes] = "HEAD",
    tag_time=None,
    tag_timezone=None,
    sign: bool = False,
    encoding: str = DEFAULT_ENCODING,
) -> None:
    """Creates a tag in git via dulwich calls.

    Args:
      repo: Path to repository
      tag: tag string
      author: tag author (optional, if annotated is set)
      message: tag message (optional)
      annotated: whether to create an annotated tag
      objectish: object the tag should point at, defaults to HEAD
      tag_time: Optional time for annotated tag
      tag_timezone: Optional timezone for annotated tag
      sign: GPG Sign the tag (bool, defaults to False,
        pass True to use default GPG key,
        pass a str containing Key ID to use a specific GPG key)
    """
    with open_repo_closing(repo) as r:
        object = parse_object(r, objectish)

        if isinstance(tag, str):
            tag = tag.encode(encoding)

        if annotated:
            # Create the tag object
            tag_obj = Tag()
            if author is None:
                author = get_user_identity(r.get_config_stack())
            elif isinstance(author, str):
                author = author.encode(encoding)
            else:
                assert isinstance(author, bytes)
            tag_obj.tagger = author
            if isinstance(message, str):
                message = message.encode(encoding)
            elif isinstance(message, bytes):
                pass
            else:
                message = b""
            tag_obj.message = message + "\n".encode(encoding)
            tag_obj.name = tag
            tag_obj.object = (type(object), object.id)
            if tag_time is None:
                tag_time = int(time.time())
            tag_obj.tag_time = tag_time
            if tag_timezone is None:
                tag_timezone = get_user_timezones()[1]
            elif isinstance(tag_timezone, str):
                tag_timezone = parse_timezone(tag_timezone.encode())
            tag_obj.tag_timezone = tag_timezone

            # Check if we should sign the tag
            should_sign = sign
            if sign is None:
                # Check tag.gpgSign configuration when sign is not explicitly set
                config = r.get_config_stack()
                try:
                    should_sign = config.get_boolean((b"tag",), b"gpgSign")
                except KeyError:
                    should_sign = False  # Default to not signing if no config
            if should_sign:
                keyid = sign if isinstance(sign, str) else None
                # If sign is True but no keyid specified, check user.signingKey config
                if should_sign is True and keyid is None:
                    config = r.get_config_stack()
                    try:
                        keyid = config.get((b"user",), b"signingKey").decode("ascii")
                    except KeyError:
                        # No user.signingKey configured, will use default GPG key
                        pass
                tag_obj.sign(keyid)

            r.object_store.add_object(tag_obj)
            tag_id = tag_obj.id
        else:
            tag_id = object.id

        r.refs[_make_tag_ref(tag)] = tag_id


def tag_list(repo: RepoPath, outstream=sys.stdout):
    """List all tags.

    Args:
      repo: Path to repository
      outstream: Stream to write tags to
    """
    with open_repo_closing(repo) as r:
        tags = sorted(r.refs.as_dict(b"refs/tags"))
        return tags


def tag_delete(repo: RepoPath, name) -> None:
    """Remove a tag.

    Args:
      repo: Path to repository
      name: Name of tag to remove
    """
    with open_repo_closing(repo) as r:
        if isinstance(name, bytes):
            names = [name]
        elif isinstance(name, list):
            names = name
        else:
            raise Error(f"Unexpected tag name type {name!r}")
        for name in names:
            del r.refs[_make_tag_ref(name)]


def _make_notes_ref(name: bytes) -> bytes:
    """Make a notes ref name."""
    if name.startswith(b"refs/notes/"):
        return name
    return LOCAL_NOTES_PREFIX + name


def notes_add(
    repo, object_sha, note, ref=b"commits", author=None, committer=None, message=None
):
    """Add or update a note for an object.

    Args:
      repo: Path to repository
      object_sha: SHA of the object to annotate
      note: Note content
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)
      author: Author identity (defaults to committer)
      committer: Committer identity (defaults to config)
      message: Commit message for the notes update

    Returns:
      SHA of the new notes commit
    """
    with open_repo_closing(repo) as r:
        # Parse the object to get its SHA
        obj = parse_object(r, object_sha)
        object_sha = obj.id

        if isinstance(note, str):
            note = note.encode(DEFAULT_ENCODING)
        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.set_note(
            object_sha,
            note,
            notes_ref,
            author=author,
            committer=committer,
            message=message,
            config=config,
        )


def notes_remove(
    repo, object_sha, ref=b"commits", author=None, committer=None, message=None
):
    """Remove a note for an object.

    Args:
      repo: Path to repository
      object_sha: SHA of the object to remove notes from
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)
      author: Author identity (defaults to committer)
      committer: Committer identity (defaults to config)
      message: Commit message for the notes removal

    Returns:
      SHA of the new notes commit, or None if no note existed
    """
    with open_repo_closing(repo) as r:
        # Parse the object to get its SHA
        obj = parse_object(r, object_sha)
        object_sha = obj.id

        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.remove_note(
            object_sha,
            notes_ref,
            author=author,
            committer=committer,
            message=message,
            config=config,
        )


def notes_show(repo: Union[str, os.PathLike, Repo], object_sha, ref=b"commits"):
    """Show the note for an object.

    Args:
      repo: Path to repository
      object_sha: SHA of the object
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)

    Returns:
      Note content as bytes, or None if no note exists
    """
    with open_repo_closing(repo) as r:
        # Parse the object to get its SHA
        obj = parse_object(r, object_sha)
        object_sha = obj.id

        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.get_note(object_sha, notes_ref, config=config)


def notes_list(repo: RepoPath, ref=b"commits"):
    """List all notes in a notes ref.

    Args:
      repo: Path to repository
      ref: Notes ref to use (defaults to "commits" for refs/notes/commits)

    Returns:
      List of tuples of (object_sha, note_content)
    """
    with open_repo_closing(repo) as r:
        if isinstance(ref, str):
            ref = ref.encode(DEFAULT_ENCODING)

        notes_ref = _make_notes_ref(ref)
        config = r.get_config_stack()

        return r.notes.list_notes(notes_ref, config=config)


def reset(
    repo: Union[str, os.PathLike, Repo],
    mode,
    treeish: Union[str, bytes, Commit, Tree, Tag] = "HEAD",
) -> None:
    """Reset current HEAD to the specified state.

    Args:
      repo: Path to repository
      mode: Mode ("hard", "soft", "mixed")
      treeish: Treeish to reset to
    """
    with open_repo_closing(repo) as r:
        # Parse the target tree
        tree = parse_tree(r, treeish)
        # Only parse as commit if treeish is not a Tree object
        if isinstance(treeish, Tree):
            # For Tree objects, we can't determine the commit, skip updating HEAD
            target_commit = None
        else:
            target_commit = parse_commit(r, treeish)

        # Update HEAD to point to the target commit
        if target_commit is not None:
            r.refs[b"HEAD"] = target_commit.id

        if mode == "soft":
            # Soft reset: only update HEAD, leave index and working tree unchanged
            return

        elif mode == "mixed":
            # Mixed reset: update HEAD and index, but leave working tree unchanged
            from .object_store import iter_tree_contents

            # Open the index
            index = r.open_index()

            # Clear the current index
            index.clear()

            # Populate index from the target tree
            for entry in iter_tree_contents(r.object_store, tree.id):
                # Create an IndexEntry from the tree entry
                # Use zeros for filesystem-specific fields since we're not touching the working tree
                index_entry = IndexEntry(
                    ctime=(0, 0),
                    mtime=(0, 0),
                    dev=0,
                    ino=0,
                    mode=entry.mode,
                    uid=0,
                    gid=0,
                    size=0,  # Size will be 0 since we're not reading from disk
                    sha=entry.sha,
                    flags=0,
                )
                index[entry.path] = index_entry

            # Write the updated index
            index.write()

        elif mode == "hard":
            # Hard reset: update HEAD, index, and working tree
            # Get configuration for working directory update
            config = r.get_config()
            honor_filemode = config.get_boolean(b"core", b"filemode", os.name != "nt")

            if config.get_boolean(b"core", b"core.protectNTFS", os.name == "nt"):
                validate_path_element = validate_path_element_ntfs
            elif config.get_boolean(
                b"core", b"core.protectHFS", sys.platform == "darwin"
            ):
                validate_path_element = validate_path_element_hfs
            else:
                validate_path_element = validate_path_element_default

            if config.get_boolean(b"core", b"symlinks", True):
                symlink_fn = symlink
            else:

                def symlink_fn(  # type: ignore
                    source, target, target_is_directory=False, *, dir_fd=None
                ) -> None:
                    mode = "w" + ("b" if isinstance(source, bytes) else "")
                    with open(target, mode) as f:
                        f.write(source)

            # Update working tree and index
            blob_normalizer = r.get_blob_normalizer()
            # For reset --hard, use current index tree as old tree to get proper deletions
            index = r.open_index()
            if len(index) > 0:
                index_tree_id = index.commit(r.object_store)
            else:
                # Empty index
                index_tree_id = None

            changes = tree_changes(
                r.object_store, index_tree_id, tree.id, want_unchanged=True
            )
            update_working_tree(
                r,
                index_tree_id,
                tree.id,
                change_iterator=changes,
                honor_filemode=honor_filemode,
                validate_path_element=validate_path_element,
                symlink_fn=symlink_fn,
                force_remove_untracked=True,
                blob_normalizer=blob_normalizer,
                allow_overwrite_modified=True,  # Allow overwriting modified files
            )
        else:
            raise Error(f"Invalid reset mode: {mode}")


def get_remote_repo(
    repo: Repo, remote_location: Optional[Union[str, bytes]] = None
) -> tuple[Optional[str], str]:
    config = repo.get_config()
    if remote_location is None:
        remote_location = get_branch_remote(repo)
    if isinstance(remote_location, str):
        encoded_location = remote_location.encode()
    else:
        encoded_location = remote_location

    section = (b"remote", encoded_location)

    remote_name: Optional[str] = None

    if config.has_section(section):
        remote_name = encoded_location.decode()
        encoded_location = config.get(section, "url")
    else:
        remote_name = None

    return (remote_name, encoded_location.decode())


def push(
    repo,
    remote_location=None,
    refspecs=None,
    outstream=default_bytes_out_stream,
    errstream=default_bytes_err_stream,
    force=False,
    **kwargs,
):
    """Remote push with dulwich via dulwich.client.

    Args:
      repo: Path to repository
      remote_location: Location of the remote
      refspecs: Refs to push to remote
      outstream: A stream file to write output
      errstream: A stream file to write errors
      force: Force overwriting refs
    """
    # Open the repo
    with open_repo_closing(repo) as r:
        (remote_name, remote_location) = get_remote_repo(r, remote_location)
        # Check if mirror mode is enabled
        mirror_mode = False
        if remote_name:
            try:
                mirror_mode = r.get_config_stack().get_boolean(
                    (b"remote", remote_name.encode()), b"mirror"
                )
            except KeyError:
                pass

        if mirror_mode:
            # Mirror mode: push all refs and delete non-existent ones
            refspecs = []
            for ref in r.refs.keys():
                # Push all refs to the same name on remote
                refspecs.append(ref + b":" + ref)
        elif refspecs is None:
            refspecs = [active_branch(r)]

        # Get the client and path
        client, path = get_transport_and_path(
            remote_location, config=r.get_config_stack(), **kwargs
        )

        selected_refs = []
        remote_changed_refs = {}

        def update_refs(refs):
            selected_refs.extend(parse_reftuples(r.refs, refs, refspecs, force=force))
            new_refs = {}

            # In mirror mode, delete remote refs that don't exist locally
            if mirror_mode:
                local_refs = set(r.refs.keys())
                for remote_ref in refs.keys():
                    if remote_ref not in local_refs:
                        new_refs[remote_ref] = ZERO_SHA
                        remote_changed_refs[remote_ref] = None
            # TODO: Handle selected_refs == {None: None}
            for lh, rh, force_ref in selected_refs:
                if lh is None:
                    new_refs[rh] = ZERO_SHA
                    remote_changed_refs[rh] = None
                else:
                    try:
                        localsha = r.refs[lh]
                    except KeyError as exc:
                        raise Error(f"No valid ref {lh} in local repository") from exc
                    if not force_ref and rh in refs:
                        check_diverged(r, refs[rh], localsha)
                    new_refs[rh] = localsha
                    remote_changed_refs[rh] = localsha
            return new_refs

        err_encoding = getattr(errstream, "encoding", None) or DEFAULT_ENCODING
        remote_location = client.get_url(path)
        try:
            result = client.send_pack(
                path,
                update_refs,
                generate_pack_data=r.generate_pack_data,
                progress=errstream.write,
            )
        except SendPackError as exc:
            raise Error(
                "Push to " + remote_location + " failed -> " + exc.args[0].decode(),
            ) from exc
        else:
            errstream.write(
                b"Push to " + remote_location.encode(err_encoding) + b" successful.\n"
            )

        for ref, error in (result.ref_status or {}).items():
            if error is not None:
                errstream.write(
                    b"Push of ref %s failed: %s\n" % (ref, error.encode(err_encoding))
                )
            else:
                errstream.write(b"Ref %s updated\n" % ref)

        if remote_name is not None:
            _import_remote_refs(r.refs, remote_name, remote_changed_refs)

        return result

    # Trigger auto GC if needed
    from .gc import maybe_auto_gc

    with open_repo_closing(repo) as r:
        maybe_auto_gc(r)


def pull(
    repo,
    remote_location=None,
    refspecs=None,
    outstream=default_bytes_out_stream,
    errstream=default_bytes_err_stream,
    fast_forward=True,
    ff_only=False,
    force=False,
    filter_spec=None,
    protocol_version=None,
    **kwargs,
) -> None:
    """Pull from remote via dulwich.client.

    Args:
      repo: Path to repository
      remote_location: Location of the remote
      refspecs: refspecs to fetch. Can be a bytestring, a string, or a list of
        bytestring/string.
      outstream: A stream file to write to output
      errstream: A stream file to write to errors
      fast_forward: If True, raise an exception when fast-forward is not possible
      ff_only: If True, only allow fast-forward merges. Raises DivergedBranches
        when branches have diverged rather than performing a merge.
      force: If True, allow overwriting local changes in the working tree.
        If False, pull will abort if it would overwrite uncommitted changes.
      filter_spec: A git-rev-list-style object filter spec, as an ASCII string.
        Only used if the server supports the Git protocol-v2 'filter'
        feature, and ignored otherwise.
      protocol_version: desired Git protocol version. By default the highest
        mutually supported protocol version will be used
    """
    # Open the repo
    with open_repo_closing(repo) as r:
        (remote_name, remote_location) = get_remote_repo(r, remote_location)

        selected_refs = []

        if refspecs is None:
            refspecs = [b"HEAD"]

        def determine_wants(remote_refs, *args, **kwargs):
            selected_refs.extend(
                parse_reftuples(remote_refs, r.refs, refspecs, force=force)
            )
            return [
                remote_refs[lh]
                for (lh, rh, force_ref) in selected_refs
                if remote_refs[lh] not in r.object_store
            ]

        client, path = get_transport_and_path(
            remote_location, config=r.get_config_stack(), **kwargs
        )
        if filter_spec:
            filter_spec = filter_spec.encode("ascii")
        fetch_result = client.fetch(
            path,
            r,
            progress=errstream.write,
            determine_wants=determine_wants,
            filter_spec=filter_spec,
            protocol_version=protocol_version,
        )

        # Store the old HEAD tree before making changes
        try:
            old_head = r.refs[b"HEAD"]
            old_tree_id = r[old_head].tree
        except KeyError:
            old_tree_id = None

        merged = False
        for lh, rh, force_ref in selected_refs:
            if not force_ref and rh in r.refs:
                try:
                    check_diverged(r, r.refs.follow(rh)[1], fetch_result.refs[lh])
                except DivergedBranches as exc:
                    if ff_only or fast_forward:
                        raise
                    else:
                        # Perform merge
                        merge_result, conflicts = _do_merge(r, fetch_result.refs[lh])
                        if conflicts:
                            raise Error(
                                f"Merge conflicts occurred: {conflicts}"
                            ) from exc
                        merged = True
                        # Skip updating ref since merge already updated HEAD
                        continue
            r.refs[rh] = fetch_result.refs[lh]

        # Only update HEAD if we didn't perform a merge
        if selected_refs and not merged:
            r[b"HEAD"] = fetch_result.refs[selected_refs[0][1]]

        # Update working tree to match the new HEAD
        # Skip if merge was performed as merge already updates the working tree
        if not merged and old_tree_id is not None:
            new_tree_id = r[b"HEAD"].tree
            blob_normalizer = r.get_blob_normalizer()
            changes = tree_changes(r.object_store, old_tree_id, new_tree_id)
            update_working_tree(
                r,
                old_tree_id,
                new_tree_id,
                change_iterator=changes,
                blob_normalizer=blob_normalizer,
                allow_overwrite_modified=force,
            )
        if remote_name is not None:
            _import_remote_refs(r.refs, remote_name, fetch_result.refs)

    # Trigger auto GC if needed
    from .gc import maybe_auto_gc

    with open_repo_closing(repo) as r:
        maybe_auto_gc(r)


def status(
    repo: Union[str, os.PathLike, Repo] = ".",
    ignored=False,
    untracked_files="normal",
):
    """Returns staged, unstaged, and untracked changes relative to the HEAD.

    Args:
      repo: Path to repository or repository object
      ignored: Whether to include ignored files in untracked
      untracked_files: How to handle untracked files, defaults to "all":
          "no": do not return untracked files
          "normal": return untracked directories, not their contents
          "all": include all files in untracked directories
        Using untracked_files="no" can be faster than "all" when the worktree
          contains many untracked files/directories.
        Using untracked_files="normal" provides a good balance, only showing
          directories that are entirely untracked without listing all their contents.

    Returns: GitStatus tuple,
        staged -  dict with lists of staged paths (diff index/HEAD)
        unstaged -  list of unstaged paths (diff index/working-tree)
        untracked - list of untracked, un-ignored & non-.git paths
    """
    with open_repo_closing(repo) as r:
        # 1. Get status of staged
        tracked_changes = get_tree_changes(r)
        # 2. Get status of unstaged
        index = r.open_index()
        normalizer = r.get_blob_normalizer()
        filter_callback = normalizer.checkin_normalize
        unstaged_changes = list(get_unstaged_changes(index, r.path, filter_callback))

        untracked_paths = get_untracked_paths(
            r.path,
            r.path,
            index,
            exclude_ignored=not ignored,
            untracked_files=untracked_files,
        )
        if sys.platform == "win32":
            untracked_changes = [
                path.replace(os.path.sep, "/") for path in untracked_paths
            ]
        else:
            untracked_changes = list(untracked_paths)

        return GitStatus(tracked_changes, unstaged_changes, untracked_changes)


def _walk_working_dir_paths(frompath, basepath, prune_dirnames=None):
    """Get path, is_dir for files in working dir from frompath.

    Args:
      frompath: Path to begin walk
      basepath: Path to compare to
      prune_dirnames: Optional callback to prune dirnames during os.walk
        dirnames will be set to result of prune_dirnames(dirpath, dirnames)
    """
    for dirpath, dirnames, filenames in os.walk(frompath):
        # Skip .git and below.
        if ".git" in dirnames:
            dirnames.remove(".git")
            if dirpath != basepath:
                continue

        if ".git" in filenames:
            filenames.remove(".git")
            if dirpath != basepath:
                continue

        if dirpath != frompath:
            yield dirpath, True

        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            yield filepath, False

        if prune_dirnames:
            dirnames[:] = prune_dirnames(dirpath, dirnames)


def get_untracked_paths(
    frompath, basepath, index, exclude_ignored=False, untracked_files="all"
):
    """Get untracked paths.

    Args:
      frompath: Path to walk
      basepath: Path to compare to
      index: Index to check against
      exclude_ignored: Whether to exclude ignored paths
      untracked_files: How to handle untracked files:
        - "no": return an empty list
        - "all": return all files in untracked directories
        - "normal": return untracked directories without listing their contents

    Note: ignored directories will never be walked for performance reasons.
      If exclude_ignored is False, only the path to an ignored directory will
      be yielded, no files inside the directory will be returned
    """
    if untracked_files not in ("no", "all", "normal"):
        raise ValueError("untracked_files must be one of (no, all, normal)")

    if untracked_files == "no":
        return

    with open_repo_closing(basepath) as r:
        ignore_manager = IgnoreFilterManager.from_repo(r)

    ignored_dirs = []
    # List to store untracked directories found during traversal
    untracked_dir_list = []

    def directory_has_non_ignored_files(dir_path, base_rel_path):
        """Recursively check if directory contains any non-ignored files."""
        try:
            for entry in os.listdir(dir_path):
                entry_path = os.path.join(dir_path, entry)
                rel_entry = os.path.join(base_rel_path, entry)

                if os.path.isfile(entry_path):
                    if ignore_manager.is_ignored(rel_entry) is not True:
                        return True
                elif os.path.isdir(entry_path):
                    if directory_has_non_ignored_files(entry_path, rel_entry):
                        return True
            return False
        except OSError:
            # If we can't read the directory, assume it has non-ignored files
            return True

    def prune_dirnames(dirpath, dirnames):
        for i in range(len(dirnames) - 1, -1, -1):
            path = os.path.join(dirpath, dirnames[i])
            ip = os.path.join(os.path.relpath(path, basepath), "")

            # Check if directory is ignored
            if ignore_manager.is_ignored(ip) is True:
                if not exclude_ignored:
                    ignored_dirs.append(
                        os.path.join(os.path.relpath(path, frompath), "")
                    )
                del dirnames[i]
                continue

            # For "normal" mode, check if the directory is entirely untracked
            if untracked_files == "normal":
                # Convert directory path to tree path for index lookup
                dir_tree_path = path_to_tree_path(basepath, path)

                # Check if any file in this directory is tracked
                dir_prefix = dir_tree_path + b"/" if dir_tree_path else b""
                has_tracked_files = any(name.startswith(dir_prefix) for name in index)

                if not has_tracked_files:
                    # This directory is entirely untracked
                    rel_path_base = os.path.relpath(path, basepath)
                    rel_path_from = os.path.join(os.path.relpath(path, frompath), "")

                    # If excluding ignored, check if directory contains any non-ignored files
                    if exclude_ignored:
                        if not directory_has_non_ignored_files(path, rel_path_base):
                            # Directory only contains ignored files, skip it
                            del dirnames[i]
                            continue

                    # Check if it should be excluded due to ignore rules
                    is_ignored = ignore_manager.is_ignored(rel_path_base)
                    if not exclude_ignored or not is_ignored:
                        untracked_dir_list.append(rel_path_from)
                    del dirnames[i]

        return dirnames

    # For "all" mode, use the original behavior
    if untracked_files == "all":
        for ap, is_dir in _walk_working_dir_paths(
            frompath, basepath, prune_dirnames=prune_dirnames
        ):
            if not is_dir:
                ip = path_to_tree_path(basepath, ap)
                if ip not in index:
                    if not exclude_ignored or not ignore_manager.is_ignored(
                        os.path.relpath(ap, basepath)
                    ):
                        yield os.path.relpath(ap, frompath)
    else:  # "normal" mode
        # Walk directories, handling both files and directories
        for ap, is_dir in _walk_working_dir_paths(
            frompath, basepath, prune_dirnames=prune_dirnames
        ):
            # This part won't be reached for pruned directories
            if is_dir:
                # Check if this directory is entirely untracked
                dir_tree_path = path_to_tree_path(basepath, ap)
                dir_prefix = dir_tree_path + b"/" if dir_tree_path else b""
                has_tracked_files = any(name.startswith(dir_prefix) for name in index)
                if not has_tracked_files:
                    if not exclude_ignored or not ignore_manager.is_ignored(
                        os.path.relpath(ap, basepath)
                    ):
                        yield os.path.join(os.path.relpath(ap, frompath), "")
            else:
                # Check individual files in directories that contain tracked files
                ip = path_to_tree_path(basepath, ap)
                if ip not in index:
                    if not exclude_ignored or not ignore_manager.is_ignored(
                        os.path.relpath(ap, basepath)
                    ):
                        yield os.path.relpath(ap, frompath)

        # Yield any untracked directories found during pruning
        yield from untracked_dir_list

    yield from ignored_dirs


def get_tree_changes(repo: RepoPath):
    """Return add/delete/modify changes to tree by comparing index to HEAD.

    Args:
      repo: repo path or object
    Returns: dict with lists for each type of change
    """
    with open_repo_closing(repo) as r:
        index = r.open_index()

        # Compares the Index to the HEAD & determines changes
        # Iterate through the changes and report add/delete/modify
        # TODO: call out to dulwich.diff_tree somehow.
        tracked_changes: dict[str, list[Union[str, bytes]]] = {
            "add": [],
            "delete": [],
            "modify": [],
        }
        try:
            tree_id = r[b"HEAD"].tree
        except KeyError:
            tree_id = None

        for change in index.changes_from_tree(r.object_store, tree_id):
            if not change[0][0]:
                assert change[0][1] is not None
                tracked_changes["add"].append(change[0][1])
            elif not change[0][1]:
                assert change[0][0] is not None
                tracked_changes["delete"].append(change[0][0])
            elif change[0][0] == change[0][1]:
                assert change[0][0] is not None
                tracked_changes["modify"].append(change[0][0])
            else:
                raise NotImplementedError("git mv ops not yet supported")
        return tracked_changes


def daemon(path=".", address=None, port=None) -> None:
    """Run a daemon serving Git requests over TCP/IP.

    Args:
      path: Path to the directory to serve.
      address: Optional address to listen on (defaults to ::)
      port: Optional port to listen on (defaults to TCP_GIT_PORT)
    """
    # TODO(jelmer): Support git-daemon-export-ok and --export-all.
    backend = FileSystemBackend(path)
    server = TCPGitServer(backend, address, port)
    server.serve_forever()


def web_daemon(path=".", address=None, port=None) -> None:
    """Run a daemon serving Git requests over HTTP.

    Args:
      path: Path to the directory to serve
      address: Optional address to listen on (defaults to ::)
      port: Optional port to listen on (defaults to 80)
    """
    from .web import (
        WSGIRequestHandlerLogger,
        WSGIServerLogger,
        make_server,
        make_wsgi_chain,
    )

    backend = FileSystemBackend(path)
    app = make_wsgi_chain(backend)
    server = make_server(
        address,
        port,
        app,
        handler_class=WSGIRequestHandlerLogger,
        server_class=WSGIServerLogger,
    )
    server.serve_forever()


def upload_pack(path=".", inf=None, outf=None) -> int:
    """Upload a pack file after negotiating its contents using smart protocol.

    Args:
      path: Path to the repository
      inf: Input stream to communicate with client
      outf: Output stream to communicate with client
    """
    if outf is None:
        outf = getattr(sys.stdout, "buffer", sys.stdout)
    if inf is None:
        inf = getattr(sys.stdin, "buffer", sys.stdin)
    path = os.path.expanduser(path)
    backend = FileSystemBackend(path)

    def send_fn(data) -> None:
        outf.write(data)
        outf.flush()

    proto = Protocol(inf.read, send_fn)
    handler = UploadPackHandler(backend, [path], proto)
    # FIXME: Catch exceptions and write a single-line summary to outf.
    handler.handle()
    return 0


def receive_pack(path=".", inf=None, outf=None) -> int:
    """Receive a pack file after negotiating its contents using smart protocol.

    Args:
      path: Path to the repository
      inf: Input stream to communicate with client
      outf: Output stream to communicate with client
    """
    if outf is None:
        outf = getattr(sys.stdout, "buffer", sys.stdout)
    if inf is None:
        inf = getattr(sys.stdin, "buffer", sys.stdin)
    path = os.path.expanduser(path)
    backend = FileSystemBackend(path)

    def send_fn(data) -> None:
        outf.write(data)
        outf.flush()

    proto = Protocol(inf.read, send_fn)
    handler = ReceivePackHandler(backend, [path], proto)
    # FIXME: Catch exceptions and write a single-line summary to outf.
    handler.handle()
    return 0


def _make_branch_ref(name: Union[str, bytes]) -> Ref:
    if isinstance(name, str):
        name = name.encode(DEFAULT_ENCODING)
    return LOCAL_BRANCH_PREFIX + name


def _make_tag_ref(name: Union[str, bytes]) -> Ref:
    if isinstance(name, str):
        name = name.encode(DEFAULT_ENCODING)
    return LOCAL_TAG_PREFIX + name


def branch_delete(repo: RepoPath, name) -> None:
    """Delete a branch.

    Args:
      repo: Path to the repository
      name: Name of the branch
    """
    with open_repo_closing(repo) as r:
        if isinstance(name, list):
            names = name
        else:
            names = [name]
        for name in names:
            del r.refs[_make_branch_ref(name)]


def branch_create(
    repo: Union[str, os.PathLike, Repo], name, objectish=None, force=False
) -> None:
    """Create a branch.

    Args:
      repo: Path to the repository
      name: Name of the new branch
      objectish: Target object to point new branch at (defaults to HEAD)
      force: Force creation of branch, even if it already exists
    """
    with open_repo_closing(repo) as r:
        if objectish is None:
            objectish = "HEAD"

        # Try to expand branch shorthand before parsing
        original_objectish = objectish
        objectish_bytes = (
            objectish.encode(DEFAULT_ENCODING)
            if isinstance(objectish, str)
            else objectish
        )
        if b"refs/remotes/" + objectish_bytes in r.refs:
            objectish = b"refs/remotes/" + objectish_bytes
        elif b"refs/heads/" + objectish_bytes in r.refs:
            objectish = b"refs/heads/" + objectish_bytes

        object = parse_object(r, objectish)
        refname = _make_branch_ref(name)
        ref_message = (
            b"branch: Created from " + original_objectish.encode(DEFAULT_ENCODING)
            if isinstance(original_objectish, str)
            else b"branch: Created from " + original_objectish
        )
        if force:
            r.refs.set_if_equals(refname, None, object.id, message=ref_message)
        else:
            if not r.refs.add_if_new(refname, object.id, message=ref_message):
                raise Error(f"Branch with name {name} already exists.")

        # Check if we should set up tracking
        config = r.get_config_stack()
        try:
            auto_setup_merge = config.get((b"branch",), b"autoSetupMerge").decode()
        except KeyError:
            auto_setup_merge = "true"  # Default value

        # Determine if the objectish refers to a remote-tracking branch
        objectish_ref = None
        if original_objectish != "HEAD":
            # Try to resolve objectish as a ref
            objectish_bytes = (
                original_objectish.encode(DEFAULT_ENCODING)
                if isinstance(original_objectish, str)
                else original_objectish
            )
            if objectish_bytes in r.refs:
                objectish_ref = objectish_bytes
            elif b"refs/remotes/" + objectish_bytes in r.refs:
                objectish_ref = b"refs/remotes/" + objectish_bytes
            elif b"refs/heads/" + objectish_bytes in r.refs:
                objectish_ref = b"refs/heads/" + objectish_bytes
        else:
            # HEAD might point to a remote-tracking branch
            head_ref = r.refs.follow(b"HEAD")[0][1]
            if head_ref.startswith(b"refs/remotes/"):
                objectish_ref = head_ref

        # Set up tracking if appropriate
        if objectish_ref and (
            (auto_setup_merge == "always")
            or (
                auto_setup_merge == "true"
                and objectish_ref.startswith(b"refs/remotes/")
            )
        ):
            # Extract remote name and branch from the ref
            if objectish_ref.startswith(b"refs/remotes/"):
                parts = objectish_ref[len(b"refs/remotes/") :].split(b"/", 1)
                if len(parts) == 2:
                    remote_name = parts[0]
                    remote_branch = b"refs/heads/" + parts[1]

                    # Set up tracking
                    repo_config = r.get_config()
                    branch_name_bytes = (
                        name.encode(DEFAULT_ENCODING) if isinstance(name, str) else name
                    )
                    repo_config.set(
                        (b"branch", branch_name_bytes), b"remote", remote_name
                    )
                    repo_config.set(
                        (b"branch", branch_name_bytes), b"merge", remote_branch
                    )
                    repo_config.write_to_path()


def branch_list(repo: RepoPath):
    """List all branches.

    Args:
      repo: Path to the repository
    Returns:
      List of branch names (without refs/heads/ prefix)
    """
    with open_repo_closing(repo) as r:
        branches = list(r.refs.keys(base=LOCAL_BRANCH_PREFIX))

        # Check for branch.sort configuration
        config = r.get_config_stack()
        try:
            sort_key = config.get((b"branch",), b"sort").decode()
        except KeyError:
            # Default is refname (alphabetical)
            sort_key = "refname"

        # Parse sort key
        reverse = False
        if sort_key.startswith("-"):
            reverse = True
            sort_key = sort_key[1:]

        # Apply sorting
        if sort_key == "refname":
            # Simple alphabetical sort (default)
            branches.sort(reverse=reverse)
        elif sort_key in ("committerdate", "authordate"):
            # Sort by date
            def get_commit_date(branch_name):
                ref = LOCAL_BRANCH_PREFIX + branch_name
                sha = r.refs[ref]
                commit = r.object_store[sha]
                if sort_key == "committerdate":
                    return commit.commit_time
                else:  # authordate
                    return commit.author_time

            # Sort branches by date
            # Note: Python's sort naturally orders smaller values first (ascending)
            # For dates, this means oldest first by default
            # Use a stable sort with branch name as secondary key for consistent ordering
            if reverse:
                # For reverse sort, we want newest dates first but alphabetical names second
                branches.sort(key=lambda b: (-get_commit_date(b), b))
            else:
                branches.sort(key=lambda b: (get_commit_date(b), b))
        else:
            # Unknown sort key, fall back to default
            branches.sort()

        return branches


def active_branch(repo: RepoPath):
    """Return the active branch in the repository, if any.

    Args:
      repo: Repository to open
    Returns:
      branch name
    Raises:
      KeyError: if the repository does not have a working tree
      IndexError: if HEAD is floating
    """
    with open_repo_closing(repo) as r:
        active_ref = r.refs.follow(b"HEAD")[0][1]
        if not active_ref.startswith(LOCAL_BRANCH_PREFIX):
            raise ValueError(active_ref)
        return active_ref[len(LOCAL_BRANCH_PREFIX) :]


def get_branch_remote(repo: Union[str, os.PathLike, Repo]):
    """Return the active branch's remote name, if any.

    Args:
      repo: Repository to open
    Returns:
      remote name
    Raises:
      KeyError: if the repository does not have a working tree
    """
    with open_repo_closing(repo) as r:
        branch_name = active_branch(r.path)
        config = r.get_config()
        try:
            remote_name = config.get((b"branch", branch_name), b"remote")
        except KeyError:
            remote_name = b"origin"
    return remote_name


def get_branch_merge(repo: RepoPath, branch_name=None):
    """Return the branch's merge reference (upstream branch), if any.

    Args:
      repo: Repository to open
      branch_name: Name of the branch (defaults to active branch)

    Returns:
      merge reference name (e.g. b"refs/heads/main")

    Raises:
      KeyError: if the branch does not have a merge configuration
    """
    with open_repo_closing(repo) as r:
        if branch_name is None:
            branch_name = active_branch(r.path)
        config = r.get_config()
        return config.get((b"branch", branch_name), b"merge")


def set_branch_tracking(
    repo: Union[str, os.PathLike, Repo], branch_name, remote_name, remote_ref
):
    """Set up branch tracking configuration.

    Args:
      repo: Repository to open
      branch_name: Name of the local branch
      remote_name: Name of the remote (e.g. b"origin")
      remote_ref: Remote reference to track (e.g. b"refs/heads/main")
    """
    with open_repo_closing(repo) as r:
        config = r.get_config()
        config.set((b"branch", branch_name), b"remote", remote_name)
        config.set((b"branch", branch_name), b"merge", remote_ref)
        config.write_to_path()


def fetch(
    repo,
    remote_location=None,
    outstream=sys.stdout,
    errstream=default_bytes_err_stream,
    message=None,
    depth=None,
    prune=False,
    prune_tags=False,
    force=False,
    **kwargs,
):
    """Fetch objects from a remote server.

    Args:
      repo: Path to the repository
      remote_location: String identifying a remote server
      outstream: Output stream (defaults to stdout)
      errstream: Error stream (defaults to stderr)
      message: Reflog message (defaults to b"fetch: from <remote_name>")
      depth: Depth to fetch at
      prune: Prune remote removed refs
      prune_tags: Prune reomte removed tags
    Returns:
      Dictionary with refs on the remote
    """
    with open_repo_closing(repo) as r:
        (remote_name, remote_location) = get_remote_repo(r, remote_location)
        if message is None:
            message = b"fetch: from " + remote_location.encode(DEFAULT_ENCODING)
        client, path = get_transport_and_path(
            remote_location, config=r.get_config_stack(), **kwargs
        )
        fetch_result = client.fetch(path, r, progress=errstream.write, depth=depth)
        if remote_name is not None:
            _import_remote_refs(
                r.refs,
                remote_name,
                fetch_result.refs,
                message,
                prune=prune,
                prune_tags=prune_tags,
            )

    # Trigger auto GC if needed
    from .gc import maybe_auto_gc

    with open_repo_closing(repo) as r:
        maybe_auto_gc(r)

    return fetch_result


def for_each_ref(
    repo: Union[Repo, str] = ".",
    pattern: Optional[Union[str, bytes]] = None,
) -> list[tuple[bytes, bytes, bytes]]:
    """Iterate over all refs that match the (optional) pattern.

    Args:
      repo: Path to the repository
      pattern: Optional glob (7) patterns to filter the refs with
    Returns: List of bytes tuples with: (sha, object_type, ref_name)
    """
    if isinstance(pattern, str):
        pattern = os.fsencode(pattern)

    with open_repo_closing(repo) as r:
        refs = r.get_refs()

    if pattern:
        matching_refs: dict[bytes, bytes] = {}
        pattern_parts = pattern.split(b"/")
        for ref, sha in refs.items():
            matches = False

            # git for-each-ref uses glob (7) style patterns, but fnmatch
            # is greedy and also matches slashes, unlike glob.glob.
            # We have to check parts of the pattern individually.
            # See https://github.com/python/cpython/issues/72904
            ref_parts = ref.split(b"/")
            if len(ref_parts) > len(pattern_parts):
                continue

            for pat, ref_part in zip(pattern_parts, ref_parts):
                matches = fnmatch.fnmatchcase(ref_part, pat)
                if not matches:
                    break

            if matches:
                matching_refs[ref] = sha

        refs = matching_refs

    ret: list[tuple[bytes, bytes, bytes]] = [
        (sha, r.get_object(sha).type_name, ref)
        for ref, sha in sorted(
            refs.items(),
            key=lambda ref_sha: ref_sha[0],
        )
        if ref != b"HEAD"
    ]

    return ret


def ls_remote(remote, config: Optional[Config] = None, **kwargs):
    """List the refs in a remote.

    Args:
      remote: Remote repository location
      config: Configuration to use
    Returns:
      LsRemoteResult object with refs and symrefs
    """
    if config is None:
        config = StackedConfig.default()
    client, host_path = get_transport_and_path(remote, config=config, **kwargs)
    return client.get_refs(host_path)


def repack(repo: RepoPath) -> None:
    """Repack loose files in a repository.

    Currently this only packs loose objects.

    Args:
      repo: Path to the repository
    """
    with open_repo_closing(repo) as r:
        r.object_store.pack_loose_objects()


def pack_objects(
    repo,
    object_ids,
    packf,
    idxf,
    delta_window_size=None,
    deltify=None,
    reuse_deltas=True,
    pack_index_version=None,
) -> None:
    """Pack objects into a file.

    Args:
      repo: Path to the repository
      object_ids: List of object ids to write
      packf: File-like object to write to
      idxf: File-like object to write to (can be None)
      delta_window_size: Sliding window size for searching for deltas;
                         Set to None for default window size.
      deltify: Whether to deltify objects
      reuse_deltas: Allow reuse of existing deltas while deltifying
      pack_index_version: Pack index version to use (1, 2, or 3). If None, uses default version.
    """
    with open_repo_closing(repo) as r:
        entries, data_sum = write_pack_from_container(
            packf.write,
            r.object_store,
            [(oid, None) for oid in object_ids],
            deltify=deltify,
            delta_window_size=delta_window_size,
            reuse_deltas=reuse_deltas,
        )
    if idxf is not None:
        entries = sorted([(k, v[0], v[1]) for (k, v) in entries.items()])
        write_pack_index(idxf, entries, data_sum, version=pack_index_version)


def ls_tree(
    repo,
    treeish: Union[str, bytes, Commit, Tree, Tag] = b"HEAD",
    outstream=sys.stdout,
    recursive=False,
    name_only=False,
) -> None:
    """List contents of a tree.

    Args:
      repo: Path to the repository
      treeish: Tree id to list
      outstream: Output stream (defaults to stdout)
      recursive: Whether to recursively list files
      name_only: Only print item name
    """

    def list_tree(store, treeid, base) -> None:
        for name, mode, sha in store[treeid].iteritems():
            if base:
                name = posixpath.join(base, name)
            if name_only:
                outstream.write(name + b"\n")
            else:
                outstream.write(pretty_format_tree_entry(name, mode, sha))
            if stat.S_ISDIR(mode) and recursive:
                list_tree(store, sha, name)

    with open_repo_closing(repo) as r:
        tree = parse_tree(r, treeish)
        list_tree(r.object_store, tree.id, "")


def remote_add(
    repo: RepoPath,
    name: Union[bytes, str],
    url: Union[bytes, str],
) -> None:
    """Add a remote.

    Args:
      repo: Path to the repository
      name: Remote name
      url: Remote URL
    """
    if not isinstance(name, bytes):
        name = name.encode(DEFAULT_ENCODING)
    if not isinstance(url, bytes):
        url = url.encode(DEFAULT_ENCODING)
    with open_repo_closing(repo) as r:
        c = r.get_config()
        section = (b"remote", name)
        if c.has_section(section):
            raise RemoteExists(section)
        c.set(section, b"url", url)
        c.write_to_path()


def remote_remove(repo: Repo, name: Union[bytes, str]) -> None:
    """Remove a remote.

    Args:
      repo: Path to the repository
      name: Remote name
    """
    if not isinstance(name, bytes):
        name = name.encode(DEFAULT_ENCODING)
    with open_repo_closing(repo) as r:
        c = r.get_config()
        section = (b"remote", name)
        del c[section]
        c.write_to_path()


def _quote_path(path: str) -> str:
    """Quote a path using C-style quoting similar to git's core.quotePath.

    Args:
        path: Path to quote

    Returns:
        Quoted path string
    """
    # Check if path needs quoting (non-ASCII or special characters)
    needs_quoting = False
    for char in path:
        if ord(char) > 127 or char in '"\\':
            needs_quoting = True
            break

    if not needs_quoting:
        return path

    # Apply C-style quoting
    quoted = '"'
    for char in path:
        if ord(char) > 127:
            # Non-ASCII character, encode as octal escape
            utf8_bytes = char.encode("utf-8")
            for byte in utf8_bytes:
                quoted += f"\\{byte:03o}"
        elif char == '"':
            quoted += '\\"'
        elif char == "\\":
            quoted += "\\\\"
        else:
            quoted += char
    quoted += '"'
    return quoted


def check_ignore(repo: RepoPath, paths, no_index=False, quote_path=True):
    r"""Debug gitignore files.

    Args:
      repo: Path to the repository
      paths: List of paths to check for
      no_index: Don't check index
      quote_path: If True, quote non-ASCII characters in returned paths using
                  C-style octal escapes (e.g. "тест.txt" becomes "\\321\\202\\320\\265\\321\\201\\321\\202.txt").
                  If False, return raw unicode paths.
    Returns: List of ignored files
    """
    with open_repo_closing(repo) as r:
        index = r.open_index()
        ignore_manager = IgnoreFilterManager.from_repo(r)
        for original_path in paths:
            if not no_index and path_to_tree_path(r.path, original_path) in index:
                continue

            # Preserve whether the original path had a trailing slash
            had_trailing_slash = original_path.endswith(("/", os.path.sep))

            if os.path.isabs(original_path):
                path = os.path.relpath(original_path, r.path)
                # Normalize Windows paths to use forward slashes
                if os.path.sep != "/":
                    path = path.replace(os.path.sep, "/")
            else:
                path = original_path

            # Restore trailing slash if it was in the original
            if had_trailing_slash and not path.endswith("/"):
                path = path + "/"

            # For directories, check with trailing slash to get correct ignore behavior
            test_path = path
            path_without_slash = path.rstrip("/")
            is_directory = os.path.isdir(os.path.join(r.path, path_without_slash))

            # If this is a directory path, ensure we test it correctly
            if is_directory and not path.endswith("/"):
                test_path = path + "/"

            if ignore_manager.is_ignored(test_path):
                # Return relative path (like git does) when absolute path was provided
                if os.path.isabs(original_path):
                    output_path = path
                else:
                    output_path = original_path
                yield _quote_path(output_path) if quote_path else output_path


def update_head(repo: RepoPath, target, detached=False, new_branch=None) -> None:
    """Update HEAD to point at a new branch/commit.

    Note that this does not actually update the working tree.

    Args:
      repo: Path to the repository
      detached: Create a detached head
      target: Branch or committish to switch to
      new_branch: New branch to create
    """
    with open_repo_closing(repo) as r:
        if new_branch is not None:
            to_set = _make_branch_ref(new_branch)
        else:
            to_set = b"HEAD"
        if detached:
            # TODO(jelmer): Provide some way so that the actual ref gets
            # updated rather than what it points to, so the delete isn't
            # necessary.
            del r.refs[to_set]
            r.refs[to_set] = parse_commit(r, target).id
        else:
            r.refs.set_symbolic_ref(to_set, parse_ref(r, target))
        if new_branch is not None:
            r.refs.set_symbolic_ref(b"HEAD", to_set)


def checkout(
    repo: Union[str, os.PathLike, Repo],
    target: Optional[Union[str, bytes, Commit, Tag]] = None,
    force: bool = False,
    new_branch: Optional[Union[bytes, str]] = None,
    paths: Optional[list[Union[bytes, str]]] = None,
) -> None:
    """Switch to a branch or commit, updating both HEAD and the working tree.

    This is similar to 'git checkout', allowing you to switch to a branch,
    tag, or specific commit. Unlike update_head, this function also updates
    the working tree to match the target.

    Args:
      repo: Path to repository or repository object
      target: Branch name, tag, or commit SHA to checkout. If None and paths is specified,
              restores files from HEAD
      force: Force checkout even if there are local changes
      new_branch: Create a new branch at target (like git checkout -b)
      paths: List of specific paths to checkout. If specified, only these paths are updated
             and HEAD is not changed

    Raises:
      CheckoutError: If checkout cannot be performed due to conflicts
      KeyError: If the target reference cannot be found
    """
    with open_repo_closing(repo) as r:
        # Store the original target for later reference checks
        original_target = target

        worktree = r.get_worktree()

        # Handle path-specific checkout (like git checkout -- <paths>)
        if paths is not None:
            # Convert paths to bytes
            byte_paths = []
            for path in paths:
                if isinstance(path, str):
                    byte_paths.append(path.encode(DEFAULT_ENCODING))
                else:
                    byte_paths.append(path)

            # If no target specified, use HEAD
            if target is None:
                try:
                    target = r.refs[b"HEAD"]
                except KeyError:
                    raise CheckoutError("No HEAD reference found")
            else:
                if isinstance(target, str):
                    target = target.encode(DEFAULT_ENCODING)

            # Get the target commit and tree
            target_tree = parse_tree(r, target)

            # Get blob normalizer for line ending conversion
            blob_normalizer = r.get_blob_normalizer()

            # Restore specified paths from target tree
            for path in byte_paths:
                try:
                    # Look up the path in the target tree
                    mode, sha = target_tree.lookup_path(
                        r.object_store.__getitem__, path
                    )
                    obj = r[sha]
                    assert isinstance(obj, Blob), "Expected a Blob object"
                except KeyError:
                    # Path doesn't exist in target tree
                    pass
                else:
                    # Create directories if needed
                    # Handle path as string
                    if isinstance(path, bytes):
                        path_str = path.decode(DEFAULT_ENCODING)
                    else:
                        path_str = path
                    file_path = os.path.join(r.path, path_str)
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)

                    # Write the file content
                    if stat.S_ISREG(mode):
                        # Apply checkout filters (smudge)
                        if blob_normalizer:
                            obj = blob_normalizer.checkout_normalize(obj, path)

                        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
                        if sys.platform == "win32":
                            flags |= os.O_BINARY

                        with os.fdopen(os.open(file_path, flags, mode), "wb") as f:
                            f.write(obj.data)

                    # Update the index
                    worktree.stage(path)

            return

        # Normal checkout (switching branches/commits)
        if target is None:
            raise ValueError("Target must be specified for branch/commit checkout")

        if isinstance(target, str):
            target_bytes = target.encode(DEFAULT_ENCODING)
        elif isinstance(target, bytes):
            target_bytes = target
        else:
            # For Commit/Tag objects, we'll use their SHA
            target_bytes = target.id

        if isinstance(new_branch, str):
            new_branch = new_branch.encode(DEFAULT_ENCODING)

        # Parse the target to get the commit
        assert (
            original_target is not None
        )  # Guaranteed by earlier check for normal checkout
        target_commit = parse_commit(r, original_target)
        target_tree_id = target_commit.tree

        # Get current HEAD tree for comparison
        try:
            current_head = r.refs[b"HEAD"]
            current_commit = r[current_head]
            assert isinstance(current_commit, Commit), "Expected a Commit object"
            current_tree_id = current_commit.tree
        except KeyError:
            # No HEAD yet (empty repo)
            current_tree_id = None

        # Check for uncommitted changes if not forcing
        if not force and current_tree_id is not None:
            status_report = status(r)
            changes = []
            # staged is a dict with 'add', 'delete', 'modify' keys
            if isinstance(status_report.staged, dict):
                changes.extend(status_report.staged.get("add", []))
                changes.extend(status_report.staged.get("delete", []))
                changes.extend(status_report.staged.get("modify", []))
            # unstaged is a list
            changes.extend(status_report.unstaged)
            if changes:
                # Check if any changes would conflict with checkout
                target_tree = r[target_tree_id]
                assert isinstance(target_tree, Tree), "Expected a Tree object"
                for change in changes:
                    if isinstance(change, str):
                        change = change.encode(DEFAULT_ENCODING)

                    try:
                        target_tree.lookup_path(r.object_store.__getitem__, change)
                    except KeyError:
                        # File doesn't exist in target tree - change can be preserved
                        pass
                    else:
                        # File exists in target tree - would overwrite local changes
                        raise CheckoutError(
                            f"Your local changes to '{change.decode()}' would be "
                            "overwritten by checkout. Please commit or stash before switching."
                        )

        # Get configuration for working directory update
        config = r.get_config()
        honor_filemode = config.get_boolean(b"core", b"filemode", os.name != "nt")

        if config.get_boolean(b"core", b"core.protectNTFS", os.name == "nt"):
            validate_path_element = validate_path_element_ntfs
        else:
            validate_path_element = validate_path_element_default

        if config.get_boolean(b"core", b"symlinks", True):
            symlink_fn = symlink
        else:

            def symlink_fn(source, target) -> None:  # type: ignore
                mode = "w" + ("b" if isinstance(source, bytes) else "")
                with open(target, mode) as f:
                    f.write(source)

        # Get blob normalizer for line ending conversion
        blob_normalizer = r.get_blob_normalizer()

        # Update working tree
        tree_change_iterator: Iterator[TreeChange] = tree_changes(
            r.object_store, current_tree_id, target_tree_id
        )
        update_working_tree(
            r,
            current_tree_id,
            target_tree_id,
            change_iterator=tree_change_iterator,
            honor_filemode=honor_filemode,
            validate_path_element=validate_path_element,
            symlink_fn=symlink_fn,
            force_remove_untracked=force,
            blob_normalizer=blob_normalizer,
            allow_overwrite_modified=force,
        )

        # Update HEAD
        if new_branch:
            # Create new branch and switch to it
            branch_create(r, new_branch, objectish=target_commit.id.decode("ascii"))
            update_head(r, new_branch)

            # Set up tracking if creating from a remote branch
            from .refs import LOCAL_REMOTE_PREFIX, parse_remote_ref

            if isinstance(original_target, bytes) and target_bytes.startswith(
                LOCAL_REMOTE_PREFIX
            ):
                try:
                    remote_name, branch_name = parse_remote_ref(target_bytes)
                    # Set tracking to refs/heads/<branch> on the remote
                    set_branch_tracking(
                        r, new_branch, remote_name, b"refs/heads/" + branch_name
                    )
                except ValueError:
                    # Invalid remote ref format, skip tracking setup
                    pass
        else:
            # Check if target is a branch name (with or without refs/heads/ prefix)
            branch_ref = None
            if (
                isinstance(original_target, (str, bytes))
                and target_bytes in r.refs.keys()
            ):
                if target_bytes.startswith(LOCAL_BRANCH_PREFIX):
                    branch_ref = target_bytes
            else:
                # Try adding refs/heads/ prefix
                potential_branch = (
                    _make_branch_ref(target_bytes)
                    if isinstance(original_target, (str, bytes))
                    else None
                )
                if potential_branch in r.refs.keys():
                    branch_ref = potential_branch

            if branch_ref:
                # It's a branch - update HEAD symbolically
                update_head(r, branch_ref)
            else:
                # It's a tag, other ref, or commit SHA - detached HEAD
                update_head(r, target_commit.id.decode("ascii"), detached=True)


def reset_file(
    repo,
    file_path: str,
    target: Union[str, bytes, Commit, Tree, Tag] = b"HEAD",
    symlink_fn=None,
) -> None:
    """Reset the file to specific commit or branch.

    Args:
      repo: dulwich Repo object
      file_path: file to reset, relative to the repository path
      target: branch or commit or b'HEAD' to reset
    """
    tree = parse_tree(repo, treeish=target)
    tree_path = _fs_to_tree_path(file_path)

    file_entry = tree.lookup_path(repo.object_store.__getitem__, tree_path)
    full_path = os.path.join(os.fsencode(repo.path), tree_path)
    blob = repo.object_store[file_entry[1]]
    mode = file_entry[0]
    build_file_from_blob(blob, mode, full_path, symlink_fn=symlink_fn)


@replace_me(since="0.22.9", remove_in="0.24.0")
def checkout_branch(
    repo: Union[str, os.PathLike, Repo],
    target: Union[bytes, str],
    force: bool = False,
) -> None:
    """Switch branches or restore working tree files.

    This is now a wrapper around the general checkout() function.
    Preserved for backward compatibility.

    Args:
      repo: dulwich Repo object
      target: branch name or commit sha to checkout
      force: true or not to force checkout
    """
    # Simply delegate to the new checkout function
    return checkout(repo, target, force=force)


def sparse_checkout(
    repo: Union[str, os.PathLike, Repo],
    patterns=None,
    force: bool = False,
    cone: Union[bool, None] = None,
):
    """Perform a sparse checkout in the repository (either 'full' or 'cone mode').

    Perform sparse checkout in either 'cone' (directory-based) mode or
    'full pattern' (.gitignore) mode, depending on the ``cone`` parameter.

    If ``cone`` is ``None``, the mode is inferred from the repository's
    ``core.sparseCheckoutCone`` config setting.

    Steps:
      1) If ``patterns`` is provided, write them to ``.git/info/sparse-checkout``.
      2) Determine which paths in the index are included vs. excluded.
         - If ``cone=True``, use "cone-compatible" directory-based logic.
         - If ``cone=False``, use standard .gitignore-style matching.
      3) Update the index's skip-worktree bits and add/remove files in
         the working tree accordingly.
      4) If ``force=False``, refuse to remove files that have local modifications.

    Args:
      repo: Path to the repository or a Repo object.
      patterns: Optional list of sparse-checkout patterns to write.
      force: Whether to force removal of locally modified files (default False).
      cone: Boolean indicating cone mode (True/False). If None, read from config.

    Returns:
      None
    """
    with open_repo_closing(repo) as repo_obj:
        # --- 0) Possibly infer 'cone' from config ---
        if cone is None:
            cone = repo_obj.get_worktree().infer_cone_mode()

        # --- 1) Read or write patterns ---
        if patterns is None:
            lines = repo_obj.get_worktree().get_sparse_checkout_patterns()
            if lines is None:
                raise Error("No sparse checkout patterns found.")
        else:
            lines = patterns
            repo_obj.get_worktree().set_sparse_checkout_patterns(patterns)

        # --- 2) Determine the set of included paths ---
        index = repo_obj.open_index()
        included_paths = determine_included_paths(index, lines, cone)

        # --- 3) Apply those results to the index & working tree ---
        try:
            apply_included_paths(repo_obj, included_paths, force=force)
        except SparseCheckoutConflictError as exc:
            raise CheckoutError(*exc.args) from exc


def cone_mode_init(repo: Union[str, os.PathLike, Repo]):
    """Initialize a repository to use sparse checkout in 'cone' mode.

    Sets ``core.sparseCheckout`` and ``core.sparseCheckoutCone`` in the config.
    Writes an initial ``.git/info/sparse-checkout`` file that includes only
    top-level files (and excludes all subdirectories), e.g. ``["/*", "!/*/"]``.
    Then performs a sparse checkout to update the working tree accordingly.

    If no directories are specified, then only top-level files are included:
    https://git-scm.com/docs/git-sparse-checkout#_internalscone_mode_handling

    Args:
      repo: Path to the repository or a Repo object.

    Returns:
      None
    """
    with open_repo_closing(repo) as repo_obj:
        repo_obj.get_worktree().configure_for_cone_mode()
        patterns = ["/*", "!/*/"]  # root-level files only
        sparse_checkout(repo_obj, patterns, force=True, cone=True)


def cone_mode_set(repo: Union[str, os.PathLike, Repo], dirs, force=False):
    """Overwrite the existing 'cone-mode' sparse patterns with a new set of directories.

    Ensures ``core.sparseCheckout`` and ``core.sparseCheckoutCone`` are enabled.
    Writes new patterns so that only the specified directories (and top-level files)
    remain in the working tree, and applies the sparse checkout update.

    Args:
      repo: Path to the repository or a Repo object.
      dirs: List of directory names to include.
      force: Whether to forcibly discard local modifications (default False).

    Returns:
      None
    """
    with open_repo_closing(repo) as repo_obj:
        repo_obj.get_worktree().configure_for_cone_mode()
        repo_obj.get_worktree().set_cone_mode_patterns(dirs=dirs)
        new_patterns = repo_obj.get_worktree().get_sparse_checkout_patterns()
        # Finally, apply the patterns and update the working tree
        sparse_checkout(repo_obj, new_patterns, force=force, cone=True)


def cone_mode_add(repo: Union[str, os.PathLike, Repo], dirs, force=False):
    """Add new directories to the existing 'cone-mode' sparse-checkout patterns.

    Reads the current patterns from ``.git/info/sparse-checkout``, adds pattern
    lines to include the specified directories, and then performs a sparse
    checkout to update the working tree accordingly.

    Args:
      repo: Path to the repository or a Repo object.
      dirs: List of directory names to add to the sparse-checkout.
      force: Whether to forcibly discard local modifications (default False).

    Returns:
      None
    """
    with open_repo_closing(repo) as repo_obj:
        repo_obj.get_worktree().configure_for_cone_mode()
        # Do not pass base patterns as dirs
        base_patterns = ["/*", "!/*/"]
        existing_dirs = [
            pat.strip("/")
            for pat in repo_obj.get_worktree().get_sparse_checkout_patterns()
            if pat not in base_patterns
        ]
        added_dirs = existing_dirs + (dirs or [])
        repo_obj.get_worktree().set_cone_mode_patterns(dirs=added_dirs)
        new_patterns = repo_obj.get_worktree().get_sparse_checkout_patterns()
        sparse_checkout(repo_obj, patterns=new_patterns, force=force, cone=True)


def check_mailmap(repo: RepoPath, contact):
    """Check canonical name and email of contact.

    Args:
      repo: Path to the repository
      contact: Contact name and/or email
    Returns: Canonical contact data
    """
    with open_repo_closing(repo) as r:
        from .mailmap import Mailmap

        try:
            mailmap = Mailmap.from_path(os.path.join(r.path, ".mailmap"))
        except FileNotFoundError:
            mailmap = Mailmap()
        return mailmap.lookup(contact)


def fsck(repo: RepoPath):
    """Check a repository.

    Args:
      repo: A path to the repository
    Returns: Iterator over errors/warnings
    """
    with open_repo_closing(repo) as r:
        # TODO(jelmer): check pack files
        # TODO(jelmer): check graph
        # TODO(jelmer): check refs
        for sha in r.object_store:
            o = r.object_store[sha]
            try:
                o.check()
            except Exception as e:
                yield (sha, e)


def stash_list(repo: Union[str, os.PathLike, Repo]):
    """List all stashes in a repository."""
    with open_repo_closing(repo) as r:
        from .stash import Stash

        stash = Stash.from_repo(r)
        return enumerate(list(stash.stashes()))


def stash_push(repo: Union[str, os.PathLike, Repo]) -> None:
    """Push a new stash onto the stack."""
    with open_repo_closing(repo) as r:
        from .stash import Stash

        stash = Stash.from_repo(r)
        stash.push()


def stash_pop(repo: Union[str, os.PathLike, Repo]) -> None:
    """Pop a stash from the stack."""
    with open_repo_closing(repo) as r:
        from .stash import Stash

        stash = Stash.from_repo(r)
        stash.pop(0)


def stash_drop(repo: Union[str, os.PathLike, Repo], index) -> None:
    """Drop a stash from the stack."""
    with open_repo_closing(repo) as r:
        from .stash import Stash

        stash = Stash.from_repo(r)
        stash.drop(index)


def ls_files(repo: RepoPath):
    """List all files in an index."""
    with open_repo_closing(repo) as r:
        return sorted(r.open_index())


def find_unique_abbrev(object_store, object_id, min_length=7):
    """Find the shortest unique abbreviation for an object ID.

    Args:
      object_store: Object store to search in
      object_id: The full object ID to abbreviate
      min_length: Minimum length of abbreviation (default 7)

    Returns:
      The shortest unique prefix of the object ID (at least min_length chars)
    """
    if isinstance(object_id, bytes):
        hex_id = object_id.decode("ascii")
    else:
        hex_id = object_id

    # Start with minimum length
    for length in range(min_length, len(hex_id) + 1):
        prefix = hex_id[:length]
        matches = 0

        # Check if this prefix is unique
        for obj_id in object_store:
            if obj_id.decode("ascii").startswith(prefix):
                matches += 1
                if matches > 1:
                    # Not unique, need more characters
                    break

        if matches == 1:
            # Found unique prefix
            return prefix

    # If we get here, return the full ID
    return hex_id


def describe(repo: Union[str, os.PathLike, Repo], abbrev=None):
    """Describe the repository version.

    Args:
      repo: git repository
      abbrev: number of characters of commit to take, default is 7
    Returns: a string description of the current git revision

    Examples: "gabcdefh", "v0.1" or "v0.1-5-gabcdefh".
    """
    abbrev_slice = slice(0, abbrev if abbrev is not None else 7)
    # Get the repository
    with open_repo_closing(repo) as r:
        # Get a list of all tags
        refs = r.get_refs()
        tags = {}
        for key, value in refs.items():
            key_str = key.decode()
            obj = r.get_object(value)
            if "tags" not in key_str:
                continue

            _, tag = key_str.rsplit("/", 1)

            if isinstance(obj, Tag):
                # Annotated tag case
                commit = r.get_object(obj.object[1])
            else:
                # Lightweight tag case - obj is already the commit
                commit = obj

            if not isinstance(commit, Commit):
                raise AssertionError(
                    f"Expected Commit object, got {type(commit).__name__}"
                )

            tags[tag] = [
                datetime.datetime(*time.gmtime(commit.commit_time)[:6]),
                commit.id.decode("ascii"),
            ]

        sorted_tags = sorted(tags.items(), key=lambda tag: tag[1][0], reverse=True)  # type: ignore[arg-type, return-value]

        # Get the latest commit
        latest_commit = r[r.head()]

        # If there are no tags, return the latest commit
        if len(sorted_tags) == 0:
            if abbrev is not None:
                return "g{}".format(latest_commit.id.decode("ascii")[abbrev_slice])
            return f"g{find_unique_abbrev(r.object_store, latest_commit.id)}"

        # We're now 0 commits from the top
        commit_count = 0

        # Walk through all commits
        walker = r.get_walker()
        for entry in walker:
            # Check if tag
            commit_id = entry.commit.id.decode("ascii")
            for tag_item in sorted_tags:
                tag_name = tag_item[0]
                tag_commit = tag_item[1][1]
                if commit_id == tag_commit:
                    if commit_count == 0:
                        return tag_name
                    else:
                        if abbrev is not None:
                            abbrev_hash = latest_commit.id.decode("ascii")[abbrev_slice]
                        else:
                            abbrev_hash = find_unique_abbrev(
                                r.object_store, latest_commit.id
                            )
                        return f"{tag_name}-{commit_count}-g{abbrev_hash}"

            commit_count += 1

        # Return plain commit if no parent tag can be found
        if abbrev is not None:
            return "g{}".format(latest_commit.id.decode("ascii")[abbrev_slice])
        return f"g{find_unique_abbrev(r.object_store, latest_commit.id)}"


def get_object_by_path(
    repo, path, committish: Optional[Union[str, bytes, Commit, Tag]] = None
):
    """Get an object by path.

    Args:
      repo: A path to the repository
      path: Path to look up
      committish: Commit to look up path in
    Returns: A `ShaFile` object
    """
    if committish is None:
        committish = "HEAD"
    # Get the repository
    with open_repo_closing(repo) as r:
        commit = parse_commit(r, committish)
        base_tree = commit.tree
        if not isinstance(path, bytes):
            path = commit_encode(commit, path)
        (mode, sha) = tree_lookup_path(r.object_store.__getitem__, base_tree, path)
        return r[sha]


def write_tree(repo: RepoPath):
    """Write a tree object from the index.

    Args:
      repo: Repository for which to write tree
    Returns: tree id for the tree that was written
    """
    with open_repo_closing(repo) as r:
        return r.open_index().commit(r.object_store)


def _do_merge(
    r,
    merge_commit_id,
    no_commit=False,
    no_ff=False,
    message=None,
    author=None,
    committer=None,
):
    """Internal merge implementation that operates on an open repository.

    Args:
      r: Open repository object
      merge_commit_id: SHA of commit to merge
      no_commit: If True, do not create a merge commit
      no_ff: If True, force creation of a merge commit
      message: Optional merge commit message
      author: Optional author for merge commit
      committer: Optional committer for merge commit

    Returns:
      Tuple of (merge_commit_sha, conflicts) where merge_commit_sha is None
      if no_commit=True or there were conflicts
    """
    from .graph import find_merge_base
    from .merge import three_way_merge

    # Get HEAD commit
    try:
        head_commit_id = r.refs[b"HEAD"]
    except KeyError:
        raise Error("No HEAD reference found")

    head_commit = r[head_commit_id]
    merge_commit = r[merge_commit_id]

    # Check if fast-forward is possible
    merge_bases = find_merge_base(r, [head_commit_id, merge_commit_id])

    if not merge_bases:
        raise Error("No common ancestor found")

    # Use the first merge base
    base_commit_id = merge_bases[0]

    # Check if we're trying to merge the same commit
    if head_commit_id == merge_commit_id:
        # Already up to date
        return (None, [])

    # Check for fast-forward
    if base_commit_id == head_commit_id and not no_ff:
        # Fast-forward merge
        r.refs[b"HEAD"] = merge_commit_id
        # Update the working directory
        changes = tree_changes(r.object_store, head_commit.tree, merge_commit.tree)
        update_working_tree(
            r, head_commit.tree, merge_commit.tree, change_iterator=changes
        )
        return (merge_commit_id, [])

    if base_commit_id == merge_commit_id:
        # Already up to date
        return (None, [])

    # Perform three-way merge
    base_commit = r[base_commit_id]
    gitattributes = r.get_gitattributes()
    config = r.get_config()
    merged_tree, conflicts = three_way_merge(
        r.object_store, base_commit, head_commit, merge_commit, gitattributes, config
    )

    # Add merged tree to object store
    r.object_store.add_object(merged_tree)

    # Update index and working directory
    changes = tree_changes(r.object_store, head_commit.tree, merged_tree.id)
    update_working_tree(r, head_commit.tree, merged_tree.id, change_iterator=changes)

    if conflicts or no_commit:
        # Don't create a commit if there are conflicts or no_commit is True
        return (None, conflicts)

    # Create merge commit
    merge_commit_obj = Commit()
    merge_commit_obj.tree = merged_tree.id
    merge_commit_obj.parents = [head_commit_id, merge_commit_id]

    # Set author/committer
    if author is None:
        author = get_user_identity(r.get_config_stack())
    if committer is None:
        committer = author

    merge_commit_obj.author = author
    merge_commit_obj.committer = committer

    # Set timestamps
    timestamp = int(time.time())
    timezone = 0  # UTC
    merge_commit_obj.author_time = timestamp
    merge_commit_obj.author_timezone = timezone
    merge_commit_obj.commit_time = timestamp
    merge_commit_obj.commit_timezone = timezone

    # Set commit message
    if message is None:
        message = f"Merge commit '{merge_commit_id.decode()[:7]}'\n"
    merge_commit_obj.message = message.encode() if isinstance(message, str) else message

    # Add commit to object store
    r.object_store.add_object(merge_commit_obj)

    # Update HEAD
    r.refs[b"HEAD"] = merge_commit_obj.id

    return (merge_commit_obj.id, [])


def merge(
    repo: Union[str, os.PathLike, Repo],
    committish: Union[str, bytes, Commit, Tag],
    no_commit=False,
    no_ff=False,
    message=None,
    author=None,
    committer=None,
):
    """Merge a commit into the current branch.

    Args:
      repo: Repository to merge into
      committish: Commit to merge
      no_commit: If True, do not create a merge commit
      no_ff: If True, force creation of a merge commit
      message: Optional merge commit message
      author: Optional author for merge commit
      committer: Optional committer for merge commit

    Returns:
      Tuple of (merge_commit_sha, conflicts) where merge_commit_sha is None
      if no_commit=True or there were conflicts

    Raises:
      Error: If there is no HEAD reference or commit cannot be found
    """
    with open_repo_closing(repo) as r:
        # Parse the commit to merge
        try:
            merge_commit_id = parse_commit(r, committish).id
        except KeyError:
            raise Error(
                f"Cannot find commit '{committish.decode() if isinstance(committish, bytes) else committish}'"
            )

        result = _do_merge(
            r, merge_commit_id, no_commit, no_ff, message, author, committer
        )

        # Trigger auto GC if needed
        from .gc import maybe_auto_gc

        maybe_auto_gc(r)

        return result


def unpack_objects(pack_path, target="."):
    """Unpack objects from a pack file into the repository.

    Args:
      pack_path: Path to the pack file to unpack
      target: Path to the repository to unpack into

    Returns:
      Number of objects unpacked
    """
    from .pack import Pack

    with open_repo_closing(target) as r:
        pack_basename = os.path.splitext(pack_path)[0]
        with Pack(pack_basename) as pack:
            count = 0
            for unpacked in pack.iter_unpacked():
                obj = unpacked.sha_file()
                r.object_store.add_object(obj)
                count += 1
            return count


def merge_tree(
    repo,
    base_tree: Optional[Union[str, bytes, Tree, Commit, Tag]],
    our_tree: Union[str, bytes, Tree, Commit, Tag],
    their_tree: Union[str, bytes, Tree, Commit, Tag],
):
    """Perform a three-way tree merge without touching the working directory.

    This is similar to git merge-tree, performing a merge at the tree level
    without creating commits or updating any references.

    Args:
      repo: Repository containing the trees
      base_tree: Tree-ish of the common ancestor (or None for no common ancestor)
      our_tree: Tree-ish of our side of the merge
      their_tree: Tree-ish of their side of the merge

    Returns:
      tuple: A tuple of (merged_tree_id, conflicts) where:
        - merged_tree_id is the SHA-1 of the merged tree
        - conflicts is a list of paths (as bytes) that had conflicts

    Raises:
      KeyError: If any of the tree-ish arguments cannot be resolved
    """
    from .merge import Merger

    with open_repo_closing(repo) as r:
        # Resolve tree-ish arguments to actual trees
        base = parse_tree(r, base_tree) if base_tree else None
        ours = parse_tree(r, our_tree)
        theirs = parse_tree(r, their_tree)

        # Perform the merge
        gitattributes = r.get_gitattributes()
        config = r.get_config()
        merger = Merger(r.object_store, gitattributes, config)
        merged_tree, conflicts = merger.merge_trees(base, ours, theirs)

        # Add the merged tree to the object store
        r.object_store.add_object(merged_tree)

        return merged_tree.id, conflicts


def cherry_pick(
    repo: Union[str, os.PathLike, Repo],
    committish: Union[str, bytes, Commit, Tag, None],
    no_commit=False,
    continue_=False,
    abort=False,
):
    r"""Cherry-pick a commit onto the current branch.

    Args:
      repo: Repository to cherry-pick into
      committish: Commit to cherry-pick (can be None only when ``continue_`` or abort is True)
      no_commit: If True, do not create a commit after applying changes
      ``continue_``: Continue an in-progress cherry-pick after resolving conflicts
      abort: Abort an in-progress cherry-pick

    Returns:
      The SHA of the newly created commit, or None if no_commit=True or there were conflicts

    Raises:
      Error: If there is no HEAD reference, commit cannot be found, or operation fails
    """
    from .merge import three_way_merge

    # Validate that committish is provided when needed
    if not (continue_ or abort) and committish is None:
        raise ValueError("committish is required when not using --continue or --abort")

    with open_repo_closing(repo) as r:
        # Handle abort
        if abort:
            # Clean up any cherry-pick state
            try:
                os.remove(os.path.join(r.controldir(), "CHERRY_PICK_HEAD"))
            except FileNotFoundError:
                pass
            try:
                os.remove(os.path.join(r.controldir(), "MERGE_MSG"))
            except FileNotFoundError:
                pass
            # Reset index to HEAD
            r.get_worktree().reset_index(r[b"HEAD"].tree)
            return None

        # Handle continue
        if continue_:
            # Check if there's a cherry-pick in progress
            cherry_pick_head_path = os.path.join(r.controldir(), "CHERRY_PICK_HEAD")
            try:
                with open(cherry_pick_head_path, "rb") as f:
                    cherry_pick_commit_id = f.read().strip()
                cherry_pick_commit = r[cherry_pick_commit_id]
            except FileNotFoundError:
                raise Error("No cherry-pick in progress")

            # Check for unresolved conflicts
            if r.open_index().has_conflicts():
                raise Error("Unresolved conflicts remain")

            # Create the commit
            tree_id = r.open_index().commit(r.object_store)

            # Read saved message if any
            merge_msg_path = os.path.join(r.controldir(), "MERGE_MSG")
            try:
                with open(merge_msg_path, "rb") as f:
                    message = f.read()
            except FileNotFoundError:
                message = cherry_pick_commit.message

            new_commit = r.get_worktree().commit(
                message=message,
                tree=tree_id,
                author=cherry_pick_commit.author,
                author_timestamp=cherry_pick_commit.author_time,
                author_timezone=cherry_pick_commit.author_timezone,
            )

            # Clean up state files
            try:
                os.remove(cherry_pick_head_path)
            except FileNotFoundError:
                pass
            try:
                os.remove(merge_msg_path)
            except FileNotFoundError:
                pass

            return new_commit

        # Normal cherry-pick operation
        # Get current HEAD
        try:
            head_commit = r[b"HEAD"]
        except KeyError:
            raise Error("No HEAD reference found")

        # Parse the commit to cherry-pick
        # committish cannot be None here due to validation above
        assert committish is not None
        try:
            cherry_pick_commit = parse_commit(r, committish)
        except KeyError:
            raise Error(
                f"Cannot find commit '{committish.decode() if isinstance(committish, bytes) else committish}'"
            )

        # Check if commit has parents
        if not cherry_pick_commit.parents:
            raise Error("Cannot cherry-pick root commit")

        # Get parent of cherry-pick commit
        parent_commit = r[cherry_pick_commit.parents[0]]

        # Perform three-way merge
        merged_tree, conflicts = three_way_merge(
            r.object_store, parent_commit, head_commit, cherry_pick_commit
        )

        # Add merged tree to object store
        r.object_store.add_object(merged_tree)

        # Update working tree and index
        # Reset index to match merged tree
        r.get_worktree().reset_index(merged_tree.id)

        # Update working tree from the new index
        # Allow overwriting because we're applying the merge result
        changes = tree_changes(r.object_store, head_commit.tree, merged_tree.id)
        update_working_tree(
            r,
            head_commit.tree,
            merged_tree.id,
            change_iterator=changes,
            allow_overwrite_modified=True,
        )

        if conflicts:
            # Save state for later continuation
            with open(os.path.join(r.controldir(), "CHERRY_PICK_HEAD"), "wb") as f:
                f.write(cherry_pick_commit.id + b"\n")

            # Save commit message
            with open(os.path.join(r.controldir(), "MERGE_MSG"), "wb") as f:
                f.write(cherry_pick_commit.message)

            raise Error(
                f"Conflicts in: {', '.join(c.decode('utf-8', 'replace') for c in conflicts)}\n"
                f"Fix conflicts and run 'dulwich cherry-pick --continue'"
            )

        if no_commit:
            return None

        # Create the commit
        new_commit = r.get_worktree().commit(
            message=cherry_pick_commit.message,
            tree=merged_tree.id,
            author=cherry_pick_commit.author,
            author_timestamp=cherry_pick_commit.author_time,
            author_timezone=cherry_pick_commit.author_timezone,
        )

        return new_commit


def revert(
    repo: Union[str, os.PathLike, Repo],
    commits: Union[str, bytes, Commit, Tag, list[Union[str, bytes, Commit, Tag]]],
    no_commit=False,
    message=None,
    author=None,
    committer=None,
):
    """Revert one or more commits.

    This creates a new commit that undoes the changes introduced by the
    specified commits. Unlike reset, revert creates a new commit that
    preserves history.

    Args:
      repo: Path to repository or repository object
      commits: List of commit-ish (SHA, ref, etc.) to revert, or a single commit-ish
      no_commit: If True, apply changes to index/working tree but don't commit
      message: Optional commit message (default: "Revert <original subject>")
      author: Optional author for revert commit
      committer: Optional committer for revert commit

    Returns:
      SHA1 of the new revert commit, or None if no_commit=True

    Raises:
      Error: If revert fails due to conflicts or other issues
    """
    from .merge import three_way_merge

    # Normalize commits to a list
    if isinstance(commits, (str, bytes, Commit, Tag)):
        commits = [commits]

    with open_repo_closing(repo) as r:
        # Convert string refs to bytes
        commits_to_revert = []
        for commit_ref in commits:
            if isinstance(commit_ref, str):
                commit_ref = commit_ref.encode("utf-8")
            commit = parse_commit(r, commit_ref)
            commits_to_revert.append(commit)

        # Get current HEAD
        try:
            head_commit_id = r.refs[b"HEAD"]
        except KeyError:
            raise Error("No HEAD reference found")

        head_commit = r[head_commit_id]
        current_tree = head_commit.tree

        # Process commits in order
        for commit_to_revert in commits_to_revert:
            # For revert, we want to apply the inverse of the commit
            # This means using the commit's tree as "base" and its parent as "theirs"

            if not commit_to_revert.parents:
                raise Error(
                    f"Cannot revert commit {commit_to_revert.id.decode() if isinstance(commit_to_revert.id, bytes) else commit_to_revert.id} - it has no parents"
                )

            # For simplicity, we only handle commits with one parent (no merge commits)
            if len(commit_to_revert.parents) > 1:
                raise Error(
                    f"Cannot revert merge commit {commit_to_revert.id.decode() if isinstance(commit_to_revert.id, bytes) else commit_to_revert.id} - not yet implemented"
                )

            parent_commit = r[commit_to_revert.parents[0]]

            # Perform three-way merge:
            # - base: the commit we're reverting (what we want to remove)
            # - ours: current HEAD (what we have now)
            # - theirs: parent of commit being reverted (what we want to go back to)
            merged_tree, conflicts = three_way_merge(
                r.object_store,
                commit_to_revert,  # base
                r[head_commit_id],  # ours
                parent_commit,  # theirs
            )

            if conflicts:
                # Update working tree with conflicts
                changes = tree_changes(r.object_store, current_tree, merged_tree.id)
                update_working_tree(
                    r, current_tree, merged_tree.id, change_iterator=changes
                )
                conflicted_paths = [c.decode("utf-8", "replace") for c in conflicts]
                raise Error(f"Conflicts while reverting: {', '.join(conflicted_paths)}")

            # Add merged tree to object store
            r.object_store.add_object(merged_tree)

            # Update working tree
            changes = tree_changes(r.object_store, current_tree, merged_tree.id)
            update_working_tree(
                r, current_tree, merged_tree.id, change_iterator=changes
            )
            current_tree = merged_tree.id

            if not no_commit:
                # Create revert commit
                revert_commit = Commit()
                revert_commit.tree = merged_tree.id
                revert_commit.parents = [head_commit_id]

                # Set author/committer
                if author is None:
                    author = get_user_identity(r.get_config_stack())
                if committer is None:
                    committer = author

                revert_commit.author = author
                revert_commit.committer = committer

                # Set timestamps
                timestamp = int(time.time())
                timezone = 0  # UTC
                revert_commit.author_time = timestamp
                revert_commit.author_timezone = timezone
                revert_commit.commit_time = timestamp
                revert_commit.commit_timezone = timezone

                # Set message
                if message is None:
                    # Extract original commit subject
                    original_message = commit_to_revert.message
                    if isinstance(original_message, bytes):
                        original_message = original_message.decode("utf-8", "replace")
                    subject = original_message.split("\n")[0]
                    message = f'Revert "{subject}"\n\nThis reverts commit {commit_to_revert.id.decode("ascii")}.'.encode()
                elif isinstance(message, str):
                    message = message.encode("utf-8")

                revert_commit.message = message

                # Add commit to object store
                r.object_store.add_object(revert_commit)

                # Update HEAD
                r.refs[b"HEAD"] = revert_commit.id
                head_commit_id = revert_commit.id

        return head_commit_id if not no_commit else None


def gc(
    repo,
    auto: bool = False,
    aggressive: bool = False,
    prune: bool = True,
    grace_period: Optional[int] = 1209600,  # 2 weeks default
    dry_run: bool = False,
    progress=None,
):
    """Run garbage collection on a repository.

    Args:
      repo: Path to the repository or a Repo object
      auto: If True, only run gc if needed
      aggressive: If True, use more aggressive settings
      prune: If True, prune unreachable objects
      grace_period: Grace period in seconds for pruning (default 2 weeks)
      dry_run: If True, only report what would be done
      progress: Optional progress callback

    Returns:
      GCStats object with garbage collection statistics
    """
    from .gc import garbage_collect

    with open_repo_closing(repo) as r:
        return garbage_collect(
            r,
            auto=auto,
            aggressive=aggressive,
            prune=prune,
            grace_period=grace_period,
            dry_run=dry_run,
            progress=progress,
        )


def prune(
    repo,
    grace_period: Optional[int] = None,
    dry_run: bool = False,
    progress=None,
):
    """Prune/clean up a repository's object store.

    This removes temporary files that were left behind by interrupted
    pack operations.

    Args:
      repo: Path to the repository or a Repo object
      grace_period: Grace period in seconds for removing temporary files
                    (default 2 weeks)
      dry_run: If True, only report what would be done
      progress: Optional progress callback
    """
    with open_repo_closing(repo) as r:
        if progress:
            progress("Pruning temporary files")
        if not dry_run:
            r.object_store.prune(grace_period=grace_period)


def count_objects(repo: RepoPath = ".", verbose=False) -> CountObjectsResult:
    """Count unpacked objects and their disk usage.

    Args:
      repo: Path to repository or repository object
      verbose: Whether to return verbose information

    Returns:
      CountObjectsResult object with detailed statistics
    """
    with open_repo_closing(repo) as r:
        object_store = r.object_store

        # Count loose objects
        loose_count = 0
        loose_size = 0
        for sha in object_store._iter_loose_objects():
            loose_count += 1
            from .object_store import DiskObjectStore

            assert isinstance(object_store, DiskObjectStore)
            path = object_store._get_shafile_path(sha)
            try:
                stat_info = os.stat(path)
                # Git uses disk usage, not file size. st_blocks is always in
                # 512-byte blocks per POSIX standard
                if hasattr(stat_info, "st_blocks"):
                    # Available on Linux and macOS
                    loose_size += stat_info.st_blocks * 512  # type: ignore
                else:
                    # Fallback for Windows
                    loose_size += stat_info.st_size
            except FileNotFoundError:
                # Object may have been removed between iteration and stat
                pass

        if not verbose:
            return CountObjectsResult(count=loose_count, size=loose_size)

        # Count pack information
        pack_count = len(object_store.packs)
        in_pack_count = 0
        pack_size = 0

        for pack in object_store.packs:
            in_pack_count += len(pack)
            # Get pack file size
            pack_path = pack._data_path
            try:
                pack_size += os.path.getsize(pack_path)
            except FileNotFoundError:
                pass
            # Get index file size
            idx_path = pack._idx_path
            try:
                pack_size += os.path.getsize(idx_path)
            except FileNotFoundError:
                pass

        return CountObjectsResult(
            count=loose_count,
            size=loose_size,
            in_pack=in_pack_count,
            packs=pack_count,
            size_pack=pack_size,
        )


def rebase(
    repo: Union[Repo, str],
    upstream: Union[bytes, str],
    onto: Optional[Union[bytes, str]] = None,
    branch: Optional[Union[bytes, str]] = None,
    abort: bool = False,
    continue_rebase: bool = False,
    skip: bool = False,
) -> list[bytes]:
    """Rebase commits onto another branch.

    Args:
      repo: Repository to rebase in
      upstream: Upstream branch/commit to rebase onto
      onto: Specific commit to rebase onto (defaults to upstream)
      branch: Branch to rebase (defaults to current branch)
      abort: Abort an in-progress rebase
      continue_rebase: Continue an in-progress rebase
      skip: Skip current commit and continue rebase

    Returns:
      List of new commit SHAs created by rebase

    Raises:
      Error: If rebase fails or conflicts occur
    """
    from .rebase import RebaseConflict, RebaseError, Rebaser

    with open_repo_closing(repo) as r:
        rebaser = Rebaser(r)

        if abort:
            try:
                rebaser.abort()
                return []
            except RebaseError as e:
                raise Error(str(e))

        if continue_rebase:
            try:
                result = rebaser.continue_()
                if result is None:
                    # Rebase complete
                    return []
                elif isinstance(result, tuple) and result[1]:
                    # Still have conflicts
                    raise Error(
                        f"Conflicts in: {', '.join(f.decode('utf-8', 'replace') for f in result[1])}"
                    )
            except RebaseError as e:
                raise Error(str(e))

        # Convert string refs to bytes
        if isinstance(upstream, str):
            upstream = upstream.encode("utf-8")
        if isinstance(onto, str):
            onto = onto.encode("utf-8") if onto else None
        if isinstance(branch, str):
            branch = branch.encode("utf-8") if branch else None

        try:
            # Start rebase
            rebaser.start(upstream, onto, branch)

            # Continue rebase automatically
            result = rebaser.continue_()
            if result is not None:
                # Conflicts
                raise RebaseConflict(result[1])

            # Return the SHAs of the rebased commits
            return [c.id for c in rebaser._done]

        except RebaseConflict as e:
            raise Error(str(e))
        except RebaseError as e:
            raise Error(str(e))


def annotate(
    repo: RepoPath,
    path,
    committish: Optional[Union[str, bytes, Commit, Tag]] = None,
):
    """Annotate the history of a file.

    :param repo: Path to the repository
    :param path: Path to annotate
    :param committish: Commit id to find path in
    :return: List of ((Commit, TreeChange), line) tuples
    """
    if committish is None:
        committish = "HEAD"
    from dulwich.annotate import annotate_lines

    with open_repo_closing(repo) as r:
        commit_id = parse_commit(r, committish).id
        # Ensure path is bytes
        if isinstance(path, str):
            path = path.encode()
        return annotate_lines(r.object_store, commit_id, path)


blame = annotate


def filter_branch(
    repo=".",
    branch="HEAD",
    *,
    filter_fn=None,
    filter_author=None,
    filter_committer=None,
    filter_message=None,
    tree_filter=None,
    index_filter=None,
    parent_filter=None,
    commit_filter=None,
    subdirectory_filter=None,
    prune_empty=False,
    tag_name_filter=None,
    force=False,
    keep_original=True,
    refs=None,
):
    """Rewrite branch history by creating new commits with filtered properties.

    This is similar to git filter-branch, allowing you to rewrite commit
    history by modifying trees, parents, author, committer, or commit messages.

    Args:
      repo: Path to repository
      branch: Branch to rewrite (defaults to HEAD)
      filter_fn: Optional callable that takes a Commit object and returns
        a dict of updated fields (author, committer, message, etc.)
      filter_author: Optional callable that takes author bytes and returns
        updated author bytes or None to keep unchanged
      filter_committer: Optional callable that takes committer bytes and returns
        updated committer bytes or None to keep unchanged
      filter_message: Optional callable that takes commit message bytes
        and returns updated message bytes
      tree_filter: Optional callable that takes (tree_sha, temp_dir) and returns
        new tree SHA after modifying working directory
      index_filter: Optional callable that takes (tree_sha, temp_index_path) and
        returns new tree SHA after modifying index
      parent_filter: Optional callable that takes parent list and returns
        modified parent list
      commit_filter: Optional callable that takes (Commit, tree_sha) and returns
        new commit SHA or None to skip commit
      subdirectory_filter: Optional subdirectory path to extract as new root
      prune_empty: Whether to prune commits that become empty
      tag_name_filter: Optional callable to rename tags
      force: Force operation even if branch has been filtered before
      keep_original: Keep original refs under refs/original/
      refs: List of refs to rewrite (defaults to [branch])

    Returns:
      Dict mapping old commit SHAs to new commit SHAs

    Raises:
      Error: If branch is already filtered and force is False
    """
    from .filter_branch import CommitFilter, filter_refs

    with open_repo_closing(repo) as r:
        # Parse branch/committish
        if isinstance(branch, str):
            branch = branch.encode()

        # Determine which refs to process
        if refs is None:
            if branch == b"HEAD":
                # Resolve HEAD to actual branch
                try:
                    resolved = r.refs.follow(b"HEAD")
                    if resolved and resolved[0]:
                        # resolved is a list of (refname, sha) tuples
                        resolved_ref = resolved[0][-1]
                        if resolved_ref and resolved_ref != b"HEAD":
                            refs = [resolved_ref]
                        else:
                            # HEAD points directly to a commit
                            refs = [b"HEAD"]
                    else:
                        refs = [b"HEAD"]
                except SymrefLoop:
                    refs = [b"HEAD"]
            else:
                # Convert branch name to full ref if needed
                if not branch.startswith(b"refs/"):
                    branch = b"refs/heads/" + branch
                refs = [branch]

        # Convert subdirectory filter to bytes if needed
        if subdirectory_filter and isinstance(subdirectory_filter, str):
            subdirectory_filter = subdirectory_filter.encode()

        # Create commit filter
        commit_filter = CommitFilter(
            r.object_store,
            filter_fn=filter_fn,
            filter_author=filter_author,
            filter_committer=filter_committer,
            filter_message=filter_message,
            tree_filter=tree_filter,
            index_filter=index_filter,
            parent_filter=parent_filter,
            commit_filter=commit_filter,
            subdirectory_filter=subdirectory_filter,
            prune_empty=prune_empty,
            tag_name_filter=tag_name_filter,
        )

        # Tag callback for renaming tags
        def rename_tag(old_ref, new_ref):
            # Copy tag to new name
            r.refs[new_ref] = r.refs[old_ref]
            # Delete old tag
            del r.refs[old_ref]

        # Filter refs
        try:
            return filter_refs(
                r.refs,
                r.object_store,
                refs,
                commit_filter,
                keep_original=keep_original,
                force=force,
                tag_callback=rename_tag if tag_name_filter else None,
            )
        except ValueError as e:
            raise Error(str(e)) from e


def format_patch(
    repo=".",
    committish=None,
    outstream=sys.stdout,
    outdir=None,
    n=1,
    stdout=False,
    version=None,
) -> list[str]:
    """Generate patches suitable for git am.

    Args:
      repo: Path to repository
      committish: Commit-ish or commit range to generate patches for.
        Can be a single commit id, or a tuple of (start, end) commit ids
        for a range. If None, formats the last n commits from HEAD.
      outstream: Stream to write to if stdout=True
      outdir: Directory to write patch files to (default: current directory)
      n: Number of patches to generate if committish is None
      stdout: Write patches to stdout instead of files
      version: Version string to include in patches (default: Dulwich version)

    Returns:
      List of patch filenames that were created (empty if stdout=True)
    """
    if outdir is None:
        outdir = "."

    filenames = []

    with open_repo_closing(repo) as r:
        # Determine which commits to format
        commits_to_format = []

        if committish is None:
            # Get the last n commits from HEAD
            try:
                walker = r.get_walker()
                for entry in walker:
                    commits_to_format.append(entry.commit)
                    if len(commits_to_format) >= n:
                        break
                commits_to_format.reverse()
            except KeyError:
                # No HEAD or empty repository
                pass
        elif isinstance(committish, tuple):
            # Handle commit range (start, end)
            start_commit, end_commit = committish

            # Extract commit IDs from commit objects if needed
            from .objects import Commit

            start_id = (
                start_commit.id if isinstance(start_commit, Commit) else start_commit
            )
            end_id = end_commit.id if isinstance(end_commit, Commit) else end_commit

            # Walk from end back to start
            walker = r.get_walker(include=[end_id], exclude=[start_id])
            for entry in walker:
                commits_to_format.append(entry.commit)
            commits_to_format.reverse()
        else:
            # Single commit
            commit = r.object_store[committish]
            commits_to_format.append(commit)

        # Generate patches
        total = len(commits_to_format)
        for i, commit in enumerate(commits_to_format, 1):
            # Get the parent
            if commit.parents:
                parent_id = commit.parents[0]
                parent = r.object_store[parent_id]
            else:
                parent = None

            # Generate the diff
            from io import BytesIO

            diff_content = BytesIO()
            if parent:
                write_tree_diff(
                    diff_content,
                    r.object_store,
                    parent.tree,
                    commit.tree,
                )
            else:
                # Initial commit - diff against empty tree
                write_tree_diff(
                    diff_content,
                    r.object_store,
                    None,
                    commit.tree,
                )

            # Generate patch with commit metadata
            if stdout:
                write_commit_patch(
                    outstream.buffer if hasattr(outstream, "buffer") else outstream,
                    commit,
                    diff_content.getvalue(),
                    (i, total),
                    version=version,
                )
            else:
                # Generate filename
                summary = get_summary(commit)
                filename = os.path.join(outdir, f"{i:04d}-{summary}.patch")

                with open(filename, "wb") as f:
                    write_commit_patch(
                        f,
                        commit,
                        diff_content.getvalue(),
                        (i, total),
                        version=version,
                    )
                filenames.append(filename)

    return filenames


def bisect_start(
    repo: Union[str, os.PathLike, Repo] = ".",
    bad: Optional[Union[str, bytes, Commit, Tag]] = None,
    good: Optional[
        Union[str, bytes, Commit, Tag, list[Union[str, bytes, Commit, Tag]]]
    ] = None,
    paths=None,
    no_checkout=False,
    term_bad="bad",
    term_good="good",
):
    """Start a new bisect session.

    Args:
        repo: Path to repository or a Repo object
        bad: The bad commit (defaults to HEAD)
        good: List of good commits or a single good commit
        paths: Optional paths to limit bisect to
        no_checkout: If True, don't checkout commits during bisect
        term_bad: Term to use for bad commits (default: "bad")
        term_good: Term to use for good commits (default: "good")
    """
    with open_repo_closing(repo) as r:
        state = BisectState(r)

        # Convert single good commit to list
        if good is not None and not isinstance(good, list):
            good = [good]

        # Parse commits
        bad_sha = parse_commit(r, bad).id if bad else None
        good_shas = [parse_commit(r, g).id for g in good] if good else None

        state.start(bad_sha, good_shas, paths, no_checkout, term_bad, term_good)

        # Return the next commit to test if we have both good and bad
        if bad_sha and good_shas:
            next_sha = state._find_next_commit()
            if next_sha and not no_checkout:
                # Checkout the next commit
                old_tree = r[r.head()].tree if r.head() else None
                r.refs[b"HEAD"] = next_sha
                commit = r[next_sha]
                changes = tree_changes(r.object_store, old_tree, commit.tree)
                update_working_tree(r, old_tree, commit.tree, change_iterator=changes)
            return next_sha


def bisect_bad(
    repo: Union[str, os.PathLike, Repo] = ".",
    rev: Optional[Union[str, bytes, Commit, Tag]] = None,
):
    """Mark a commit as bad.

    Args:
        repo: Path to repository or a Repo object
        rev: Commit to mark as bad (defaults to HEAD)

    Returns:
        The SHA of the next commit to test, or None if bisect is complete
    """
    with open_repo_closing(repo) as r:
        state = BisectState(r)
        rev_sha = parse_commit(r, rev).id if rev else None
        next_sha = state.mark_bad(rev_sha)

        if next_sha:
            # Checkout the next commit
            old_tree = r[r.head()].tree if r.head() else None
            r.refs[b"HEAD"] = next_sha
            commit = r[next_sha]
            changes = tree_changes(r.object_store, old_tree, commit.tree)
            update_working_tree(r, old_tree, commit.tree, change_iterator=changes)

        return next_sha


def bisect_good(
    repo: Union[str, os.PathLike, Repo] = ".",
    rev: Optional[Union[str, bytes, Commit, Tag]] = None,
):
    """Mark a commit as good.

    Args:
        repo: Path to repository or a Repo object
        rev: Commit to mark as good (defaults to HEAD)

    Returns:
        The SHA of the next commit to test, or None if bisect is complete
    """
    with open_repo_closing(repo) as r:
        state = BisectState(r)
        rev_sha = parse_commit(r, rev).id if rev else None
        next_sha = state.mark_good(rev_sha)

        if next_sha:
            # Checkout the next commit
            old_tree = r[r.head()].tree if r.head() else None
            r.refs[b"HEAD"] = next_sha
            commit = r[next_sha]
            changes = tree_changes(r.object_store, old_tree, commit.tree)
            update_working_tree(r, old_tree, commit.tree, change_iterator=changes)

        return next_sha


def bisect_skip(
    repo: Union[str, os.PathLike, Repo] = ".",
    revs: Optional[
        Union[str, bytes, Commit, Tag, list[Union[str, bytes, Commit, Tag]]]
    ] = None,
):
    """Skip one or more commits.

    Args:
        repo: Path to repository or a Repo object
        revs: List of commits to skip (defaults to [HEAD])

    Returns:
        The SHA of the next commit to test, or None if bisect is complete
    """
    with open_repo_closing(repo) as r:
        state = BisectState(r)

        if revs is None:
            rev_shas = None
        else:
            # Convert single rev to list
            if not isinstance(revs, list):
                revs = [revs]
            rev_shas = [parse_commit(r, rev).id for rev in revs]

        next_sha = state.skip(rev_shas)

        if next_sha:
            # Checkout the next commit
            old_tree = r[r.head()].tree if r.head() else None
            r.refs[b"HEAD"] = next_sha
            commit = r[next_sha]
            changes = tree_changes(r.object_store, old_tree, commit.tree)
            update_working_tree(r, old_tree, commit.tree, change_iterator=changes)

        return next_sha


def bisect_reset(
    repo: Union[str, os.PathLike, Repo] = ".",
    commit: Optional[Union[str, bytes, Commit, Tag]] = None,
):
    """Reset bisect state and return to original branch/commit.

    Args:
        repo: Path to repository or a Repo object
        commit: Optional commit to reset to (defaults to original branch/commit)
    """
    with open_repo_closing(repo) as r:
        state = BisectState(r)
        # Get old tree before reset
        try:
            old_tree = r[r.head()].tree
        except KeyError:
            old_tree = None

        commit_sha = parse_commit(r, commit).id if commit else None
        state.reset(commit_sha)

        # Update working tree to new HEAD
        try:
            new_head = r.head()
            if new_head:
                new_commit = r[new_head]
                changes = tree_changes(r.object_store, old_tree, new_commit.tree)
                update_working_tree(
                    r, old_tree, new_commit.tree, change_iterator=changes
                )
        except KeyError:
            # No HEAD after reset
            pass


def bisect_log(repo: Union[str, os.PathLike, Repo] = "."):
    """Get the bisect log.

    Args:
        repo: Path to repository or a Repo object

    Returns:
        The bisect log as a string
    """
    with open_repo_closing(repo) as r:
        state = BisectState(r)
        return state.get_log()


def bisect_replay(repo: Union[str, os.PathLike, Repo], log_file):
    """Replay a bisect log.

    Args:
        repo: Path to repository or a Repo object
        log_file: Path to the log file or file-like object
    """
    with open_repo_closing(repo) as r:
        state = BisectState(r)

        if isinstance(log_file, str):
            with open(log_file) as f:
                log_content = f.read()
        else:
            log_content = log_file.read()

        state.replay(log_content)


def reflog(repo: RepoPath = ".", ref=b"HEAD", all=False):
    """Show reflog entries for a reference or all references.

    Args:
        repo: Path to repository or a Repo object
        ref: Reference name (defaults to HEAD)
        all: If True, show reflogs for all refs (ignores ref parameter)

    Yields:
        If all=False: ReflogEntry objects
        If all=True: Tuples of (ref_name, ReflogEntry) for all refs with reflogs
    """
    import os

    from .reflog import iter_reflogs

    if isinstance(ref, str):
        ref = ref.encode("utf-8")

    with open_repo_closing(repo) as r:
        if not all:
            yield from r.read_reflog(ref)
        else:
            logs_dir = os.path.join(r.controldir(), "logs")
            # Use iter_reflogs to discover all reflogs
            for ref_bytes in iter_reflogs(logs_dir):
                # Read the reflog entries for this ref
                for entry in r.read_reflog(ref_bytes):
                    yield (ref_bytes, entry)


def lfs_track(repo: Union[str, os.PathLike, Repo] = ".", patterns=None):
    """Track file patterns with Git LFS.

    Args:
      repo: Path to repository
      patterns: List of file patterns to track (e.g., ["*.bin", "*.pdf"])
                If None, returns current tracked patterns

    Returns:
      List of tracked patterns
    """
    from .attrs import GitAttributes

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
            if isinstance(pattern, str):
                pattern = pattern.encode()

            # Set LFS attributes for the pattern
            gitattributes.set_attribute(pattern, b"filter", b"lfs")
            gitattributes.set_attribute(pattern, b"diff", b"lfs")
            gitattributes.set_attribute(pattern, b"merge", b"lfs")
            gitattributes.set_attribute(pattern, b"text", False)

        # Write updated attributes
        gitattributes.write_to_file(gitattributes_path)

        # Stage the .gitattributes file
        add(r, [".gitattributes"])

        return lfs_track(r)  # Return updated list


def lfs_untrack(repo: Union[str, os.PathLike, Repo] = ".", patterns=None):
    """Untrack file patterns from Git LFS.

    Args:
      repo: Path to repository
      patterns: List of file patterns to untrack

    Returns:
      List of remaining tracked patterns
    """
    from .attrs import GitAttributes

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
            if isinstance(pattern, str):
                pattern = pattern.encode()

            # Check if pattern is tracked by LFS
            for pattern_obj, attrs in list(gitattributes):
                if pattern_obj.pattern == pattern and attrs.get(b"filter") == b"lfs":
                    gitattributes.remove_pattern(pattern)
                    break

        # Write updated attributes
        gitattributes.write_to_file(gitattributes_path)

        # Stage the .gitattributes file
        add(r, [".gitattributes"])

        return lfs_track(r)  # Return updated list


def lfs_init(repo: Union[str, os.PathLike, Repo] = "."):
    """Initialize Git LFS in a repository.

    Args:
      repo: Path to repository

    Returns:
      None
    """
    from .lfs import LFSStore

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


def lfs_clean(repo: Union[str, os.PathLike, Repo] = ".", path=None):
    """Clean a file by converting it to an LFS pointer.

    Args:
      repo: Path to repository
      path: Path to file to clean (relative to repo root)

    Returns:
      LFS pointer content as bytes
    """
    from .lfs import LFSFilterDriver, LFSStore

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


def lfs_smudge(repo: Union[str, os.PathLike, Repo] = ".", pointer_content=None):
    """Smudge an LFS pointer by retrieving the actual content.

    Args:
      repo: Path to repository
      pointer_content: LFS pointer content as bytes

    Returns:
      Actual file content as bytes
    """
    from .lfs import LFSFilterDriver, LFSStore

    with open_repo_closing(repo) as r:
        if pointer_content is None:
            raise ValueError("Pointer content must be specified")

        # Get LFS store
        lfs_store = LFSStore.from_repo(r)
        filter_driver = LFSFilterDriver(lfs_store, config=r.get_config())

        # Smudge the pointer (retrieve actual content)
        return filter_driver.smudge(pointer_content)


def lfs_ls_files(repo: Union[str, os.PathLike, Repo] = ".", ref=None):
    """List files tracked by Git LFS.

    Args:
      repo: Path to repository
      ref: Git ref to check (defaults to HEAD)

    Returns:
      List of (path, oid, size) tuples for LFS files
    """
    from .lfs import LFSPointer
    from .object_store import iter_tree_contents

    with open_repo_closing(repo) as r:
        if ref is None:
            ref = b"HEAD"
        elif isinstance(ref, str):
            ref = ref.encode()

        # Get the commit and tree
        try:
            commit = r[ref]
            tree = r[commit.tree]
        except KeyError:
            return []

        lfs_files = []

        # Walk the tree
        for path, mode, sha in iter_tree_contents(r.object_store, tree.id):
            if not stat.S_ISREG(mode):
                continue

            # Check if it's an LFS pointer
            obj = r.object_store[sha]
            if not isinstance(obj, Blob):
                raise AssertionError(f"Expected Blob object, got {type(obj).__name__}")
            pointer = LFSPointer.from_bytes(obj.data)
            if pointer is not None:
                lfs_files.append((path.decode(), pointer.oid, pointer.size))

        return lfs_files


def lfs_migrate(
    repo: Union[str, os.PathLike, Repo] = ".",
    include=None,
    exclude=None,
    everything=False,
):
    """Migrate files to Git LFS.

    Args:
      repo: Path to repository
      include: Patterns of files to include
      exclude: Patterns of files to exclude
      everything: Migrate all files above a certain size

    Returns:
      Number of migrated files
    """
    from .lfs import LFSFilterDriver, LFSStore

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


def lfs_pointer_check(repo: Union[str, os.PathLike, Repo] = ".", paths=None):
    """Check if files are valid LFS pointers.

    Args:
      repo: Path to repository
      paths: List of file paths to check (if None, check all files)

    Returns:
      Dict mapping paths to LFSPointer objects (or None if not a pointer)
    """
    from .lfs import LFSPointer

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


def lfs_fetch(repo: Union[str, os.PathLike, Repo] = ".", remote="origin", refs=None):
    """Fetch LFS objects from remote.

    Args:
      repo: Path to repository
      remote: Remote name (default: origin)
      refs: Specific refs to fetch LFS objects for (default: all refs)

    Returns:
      Number of objects fetched
    """
    from .lfs import LFSClient, LFSPointer, LFSStore

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
                ref = ref.encode()
            try:
                commit = r[r.refs[ref]]
            except KeyError:
                continue

            # Walk the commit tree
            for entry in r.object_store.iter_tree_contents(commit.tree):
                try:
                    obj = r.object_store[entry.sha]
                except KeyError:
                    pass
                else:
                    if isinstance(obj, Blob):
                        pointer = LFSPointer.from_bytes(obj.data)
                        if pointer and pointer.is_valid_oid():
                            # Check if we already have it
                            try:
                                store.open_object(pointer.oid)
                            except KeyError:
                                pointers_to_fetch.append((pointer.oid, pointer.size))

        # Fetch missing objects
        fetched = 0
        for oid, size in pointers_to_fetch:
            content = client.download(oid, size)
            store.write_object([content])
            fetched += 1

        return fetched


def lfs_pull(repo: Union[str, os.PathLike, Repo] = ".", remote="origin"):
    """Pull LFS objects for current checkout.

    Args:
      repo: Path to repository
      remote: Remote name (default: origin)

    Returns:
      Number of objects fetched
    """
    from .lfs import LFSPointer, LFSStore

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


def lfs_push(repo: Union[str, os.PathLike, Repo] = ".", remote="origin", refs=None):
    """Push LFS objects to remote.

    Args:
      repo: Path to repository
      remote: Remote name (default: origin)
      refs: Specific refs to push LFS objects for (default: current branch)

    Returns:
      Number of objects pushed
    """
    from .lfs import LFSClient, LFSPointer, LFSStore

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
            refs = [r.refs.read_ref(b"HEAD")]

        objects_to_push = set()

        for ref in refs:
            if isinstance(ref, str):
                ref = ref.encode()
            try:
                if ref.startswith(b"refs/"):
                    commit = r[r.refs[ref]]
                else:
                    commit = r[ref]
            except KeyError:
                continue

            # Walk the commit tree
            for entry in r.object_store.iter_tree_contents(commit.tree):
                try:
                    obj = r.object_store[entry.sha]
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


def lfs_status(repo: Union[str, os.PathLike, Repo] = "."):
    """Show status of LFS files.

    Args:
      repo: Path to repository

    Returns:
      Dict with status information
    """
    from .lfs import LFSPointer, LFSStore

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
                        store.open_object(pointer.oid)
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


def worktree_list(repo="."):
    """List all worktrees for a repository.

    Args:
        repo: Path to repository

    Returns:
        List of WorkTreeInfo objects
    """
    from .worktree import list_worktrees

    with open_repo_closing(repo) as r:
        return list_worktrees(r)


def worktree_add(
    repo=".", path=None, branch=None, commit=None, detach=False, force=False
):
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
    from .worktree import add_worktree

    if path is None:
        raise ValueError("Path is required for worktree add")

    with open_repo_closing(repo) as r:
        wt_repo = add_worktree(
            r, path, branch=branch, commit=commit, detach=detach, force=force
        )
        return wt_repo.path


def worktree_remove(repo=".", path=None, force=False):
    """Remove a worktree.

    Args:
        repo: Path to repository
        path: Path to worktree to remove
        force: Force removal even if there are local changes
    """
    from .worktree import remove_worktree

    if path is None:
        raise ValueError("Path is required for worktree remove")

    with open_repo_closing(repo) as r:
        remove_worktree(r, path, force=force)


def worktree_prune(repo=".", dry_run=False, expire=None):
    """Prune worktree administrative files.

    Args:
        repo: Path to repository
        dry_run: Only show what would be removed
        expire: Only prune worktrees older than this many seconds

    Returns:
        List of pruned worktree names
    """
    from .worktree import prune_worktrees

    with open_repo_closing(repo) as r:
        return prune_worktrees(r, expire=expire, dry_run=dry_run)


def worktree_lock(repo=".", path=None, reason=None):
    """Lock a worktree to prevent it from being pruned.

    Args:
        repo: Path to repository
        path: Path to worktree to lock
        reason: Optional reason for locking
    """
    from .worktree import lock_worktree

    if path is None:
        raise ValueError("Path is required for worktree lock")

    with open_repo_closing(repo) as r:
        lock_worktree(r, path, reason=reason)


def worktree_unlock(repo=".", path=None):
    """Unlock a worktree.

    Args:
        repo: Path to repository
        path: Path to worktree to unlock
    """
    from .worktree import unlock_worktree

    if path is None:
        raise ValueError("Path is required for worktree unlock")

    with open_repo_closing(repo) as r:
        unlock_worktree(r, path)


def worktree_move(repo=".", old_path=None, new_path=None):
    """Move a worktree to a new location.

    Args:
        repo: Path to repository
        old_path: Current path of worktree
        new_path: New path for worktree
    """
    from .worktree import move_worktree

    if old_path is None or new_path is None:
        raise ValueError("Both old_path and new_path are required for worktree move")

    with open_repo_closing(repo) as r:
        move_worktree(r, old_path, new_path)
