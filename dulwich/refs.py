# refs.py -- For dealing with git refs
# Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@jelmer.uk>
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


"""Ref handling."""

import os
import types
import warnings
from collections.abc import Iterable, Iterator, Mapping
from contextlib import suppress
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Optional,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from .file import _GitFile

from .errors import PackedRefsException, RefFormatError
from .file import GitFile, ensure_dir_exists
from .objects import ZERO_SHA, ObjectID, Tag, git_line, valid_hexsha
from .pack import ObjectContainer

Ref = bytes

HEADREF = b"HEAD"
SYMREF = b"ref: "
LOCAL_BRANCH_PREFIX = b"refs/heads/"
LOCAL_TAG_PREFIX = b"refs/tags/"
LOCAL_REMOTE_PREFIX = b"refs/remotes/"
LOCAL_NOTES_PREFIX = b"refs/notes/"
LOCAL_REPLACE_PREFIX = b"refs/replace/"
BAD_REF_CHARS = set(b"\177 ~^:?*[")
PEELED_TAG_SUFFIX = b"^{}"

# For backwards compatibility
ANNOTATED_TAG_SUFFIX = PEELED_TAG_SUFFIX


class SymrefLoop(Exception):
    """There is a loop between one or more symrefs."""

    def __init__(self, ref: bytes, depth: int) -> None:
        """Initialize SymrefLoop exception."""
        self.ref = ref
        self.depth = depth


def parse_symref_value(contents: bytes) -> bytes:
    """Parse a symref value.

    Args:
      contents: Contents to parse
    Returns: Destination
    """
    if contents.startswith(SYMREF):
        return contents[len(SYMREF) :].rstrip(b"\r\n")
    raise ValueError(contents)


def check_ref_format(refname: Ref) -> bool:
    """Check if a refname is correctly formatted.

    Implements all the same rules as git-check-ref-format[1].

    [1]
    http://www.kernel.org/pub/software/scm/git/docs/git-check-ref-format.html

    Args:
      refname: The refname to check
    Returns: True if refname is valid, False otherwise
    """
    # These could be combined into one big expression, but are listed
    # separately to parallel [1].
    if b"/." in refname or refname.startswith(b"."):
        return False
    if b"/" not in refname:
        return False
    if b".." in refname:
        return False
    for i, c in enumerate(refname):
        if ord(refname[i : i + 1]) < 0o40 or c in BAD_REF_CHARS:
            return False
    if refname[-1] in b"/.":
        return False
    if refname.endswith(b".lock"):
        return False
    if b"@{" in refname:
        return False
    if b"\\" in refname:
        return False
    return True


def parse_remote_ref(ref: bytes) -> tuple[bytes, bytes]:
    """Parse a remote ref into remote name and branch name.

    Args:
      ref: Remote ref like b"refs/remotes/origin/main"

    Returns:
      Tuple of (remote_name, branch_name)

    Raises:
      ValueError: If ref is not a valid remote ref
    """
    if not ref.startswith(LOCAL_REMOTE_PREFIX):
        raise ValueError(f"Not a remote ref: {ref!r}")

    # Remove the prefix
    remainder = ref[len(LOCAL_REMOTE_PREFIX) :]

    # Split into remote name and branch name
    parts = remainder.split(b"/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid remote ref format: {ref!r}")

    remote_name, branch_name = parts
    return (remote_name, branch_name)


class RefsContainer:
    """A container for refs."""

    def __init__(
        self,
        logger: Optional[
            Callable[
                [
                    bytes,
                    bytes,
                    bytes,
                    Optional[bytes],
                    Optional[int],
                    Optional[int],
                    bytes,
                ],
                None,
            ]
        ] = None,
    ) -> None:
        """Initialize RefsContainer with optional logger function."""
        self._logger = logger

    def _log(
        self,
        ref: bytes,
        old_sha: Optional[bytes],
        new_sha: Optional[bytes],
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> None:
        if self._logger is None:
            return
        if message is None:
            return
        # Use ZERO_SHA for None values, matching git behavior
        if old_sha is None:
            old_sha = ZERO_SHA
        if new_sha is None:
            new_sha = ZERO_SHA
        self._logger(ref, old_sha, new_sha, committer, timestamp, timezone, message)

    def set_symbolic_ref(
        self,
        name: bytes,
        other: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> None:
        """Make a ref point at another ref.

        Args:
          name: Name of the ref to set
          other: Name of the ref to point at
          committer: Optional committer name/email
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Optional message
        """
        raise NotImplementedError(self.set_symbolic_ref)

    def get_packed_refs(self) -> dict[Ref, ObjectID]:
        """Get contents of the packed-refs file.

        Returns: Dictionary mapping ref names to SHA1s

        Note: Will return an empty dictionary when no packed-refs file is
            present.
        """
        raise NotImplementedError(self.get_packed_refs)

    def add_packed_refs(self, new_refs: Mapping[Ref, Optional[ObjectID]]) -> None:
        """Add the given refs as packed refs.

        Args:
          new_refs: A mapping of ref names to targets; if a target is None that
            means remove the ref
        """
        raise NotImplementedError(self.add_packed_refs)

    def get_peeled(self, name: bytes) -> Optional[ObjectID]:
        """Return the cached peeled value of a ref, if available.

        Args:
          name: Name of the ref to peel
        Returns: The peeled value of the ref. If the ref is known not point to
            a tag, this will be the SHA the ref refers to. If the ref may point
            to a tag, but no cached information is available, None is returned.
        """
        return None

    def import_refs(
        self,
        base: Ref,
        other: Mapping[Ref, ObjectID],
        committer: Optional[bytes] = None,
        timestamp: Optional[bytes] = None,
        timezone: Optional[bytes] = None,
        message: Optional[bytes] = None,
        prune: bool = False,
    ) -> None:
        """Import refs from another repository.

        Args:
          base: Base ref to import into (e.g., b'refs/remotes/origin')
          other: Dictionary of refs to import
          committer: Optional committer for reflog
          timestamp: Optional timestamp for reflog
          timezone: Optional timezone for reflog
          message: Optional message for reflog
          prune: If True, remove refs not in other
        """
        if prune:
            to_delete = set(self.subkeys(base))
        else:
            to_delete = set()
        for name, value in other.items():
            if value is None:
                to_delete.add(name)
            else:
                self.set_if_equals(
                    b"/".join((base, name)), None, value, message=message
                )
            if to_delete:
                try:
                    to_delete.remove(name)
                except KeyError:
                    pass
        for ref in to_delete:
            self.remove_if_equals(b"/".join((base, ref)), None, message=message)

    def allkeys(self) -> set[Ref]:
        """All refs present in this container."""
        raise NotImplementedError(self.allkeys)

    def __iter__(self) -> Iterator[Ref]:
        """Iterate over all reference keys."""
        return iter(self.allkeys())

    def keys(self, base: Optional[bytes] = None) -> set[bytes]:
        """Refs present in this container.

        Args:
          base: An optional base to return refs under.
        Returns: An unsorted set of valid refs in this container, including
            packed refs.
        """
        if base is not None:
            return self.subkeys(base)
        else:
            return self.allkeys()

    def subkeys(self, base: bytes) -> set[bytes]:
        """Refs present in this container under a base.

        Args:
          base: The base to return refs under.
        Returns: A set of valid refs in this container under the base; the base
            prefix is stripped from the ref names returned.
        """
        keys = set()
        base_len = len(base) + 1
        for refname in self.allkeys():
            if refname.startswith(base):
                keys.add(refname[base_len:])
        return keys

    def as_dict(self, base: Optional[bytes] = None) -> dict[Ref, ObjectID]:
        """Return the contents of this container as a dictionary."""
        ret = {}
        keys = self.keys(base)
        if base is None:
            base = b""
        else:
            base = base.rstrip(b"/")
        for key in keys:
            try:
                ret[key] = self[(base + b"/" + key).strip(b"/")]
            except (SymrefLoop, KeyError):
                continue  # Unable to resolve

        return ret

    def _check_refname(self, name: bytes) -> None:
        """Ensure a refname is valid and lives in refs or is HEAD.

        HEAD is not a valid refname according to git-check-ref-format, but this
        class needs to be able to touch HEAD. Also, check_ref_format expects
        refnames without the leading 'refs/', but this class requires that
        so it cannot touch anything outside the refs dir (or HEAD).

        Args:
          name: The name of the reference.

        Raises:
          KeyError: if a refname is not HEAD or is otherwise not valid.
        """
        if name in (HEADREF, b"refs/stash"):
            return
        if not name.startswith(b"refs/") or not check_ref_format(name[5:]):
            raise RefFormatError(name)

    def read_ref(self, refname: bytes) -> Optional[bytes]:
        """Read a reference without following any references.

        Args:
          refname: The name of the reference
        Returns: The contents of the ref file, or None if it does
            not exist.
        """
        contents = self.read_loose_ref(refname)
        if not contents:
            contents = self.get_packed_refs().get(refname, None)
        return contents

    def read_loose_ref(self, name: bytes) -> Optional[bytes]:
        """Read a loose reference and return its contents.

        Args:
          name: the refname to read
        Returns: The contents of the ref file, or None if it does
            not exist.
        """
        raise NotImplementedError(self.read_loose_ref)

    def follow(self, name: bytes) -> tuple[list[bytes], Optional[bytes]]:
        """Follow a reference name.

        Returns: a tuple of (refnames, sha), wheres refnames are the names of
            references in the chain
        """
        contents: Optional[bytes] = SYMREF + name
        depth = 0
        refnames = []
        while contents and contents.startswith(SYMREF):
            refname = contents[len(SYMREF) :]
            refnames.append(refname)
            contents = self.read_ref(refname)
            if not contents:
                break
            depth += 1
            if depth > 5:
                raise SymrefLoop(name, depth)
        return refnames, contents

    def __contains__(self, refname: bytes) -> bool:
        """Check if a reference exists."""
        if self.read_ref(refname):
            return True
        return False

    def __getitem__(self, name: bytes) -> ObjectID:
        """Get the SHA1 for a reference name.

        This method follows all symbolic references.
        """
        _, sha = self.follow(name)
        if sha is None:
            raise KeyError(name)
        return sha

    def set_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        new_ref: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Set a refname to new_ref only if it currently equals old_ref.

        This method follows all symbolic references if applicable for the
        subclass, and can be used to perform an atomic compare-and-swap
        operation.

        Args:
          name: The refname to set.
          old_ref: The old sha the refname must refer to, or None to set
            unconditionally.
          new_ref: The new sha the refname will refer to.
          committer: Optional committer name/email
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Message for reflog
        Returns: True if the set was successful, False otherwise.
        """
        raise NotImplementedError(self.set_if_equals)

    def add_if_new(
        self,
        name: bytes,
        ref: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Add a new reference only if it does not already exist.

        Args:
          name: Ref name
          ref: Ref value
          committer: Optional committer name/email
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Optional message for reflog
        """
        raise NotImplementedError(self.add_if_new)

    def __setitem__(self, name: bytes, ref: bytes) -> None:
        """Set a reference name to point to the given SHA1.

        This method follows all symbolic references if applicable for the
        subclass.

        Note: This method unconditionally overwrites the contents of a
            reference. To update atomically only if the reference has not
            changed, use set_if_equals().

        Args:
          name: The refname to set.
          ref: The new sha the refname will refer to.
        """
        if not (valid_hexsha(ref) or ref.startswith(SYMREF)):
            raise ValueError(f"{ref!r} must be a valid sha (40 chars) or a symref")
        self.set_if_equals(name, None, ref)

    def remove_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Remove a refname only if it currently equals old_ref.

        This method does not follow symbolic references, even if applicable for
        the subclass. It can be used to perform an atomic compare-and-delete
        operation.

        Args:
          name: The refname to delete.
          old_ref: The old sha the refname must refer to, or None to
            delete unconditionally.
          committer: Optional committer name/email
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Message for reflog
        Returns: True if the delete was successful, False otherwise.
        """
        raise NotImplementedError(self.remove_if_equals)

    def __delitem__(self, name: bytes) -> None:
        """Remove a refname.

        This method does not follow symbolic references, even if applicable for
        the subclass.

        Note: This method unconditionally deletes the contents of a reference.
            To delete atomically only if the reference has not changed, use
            remove_if_equals().

        Args:
          name: The refname to delete.
        """
        self.remove_if_equals(name, None)

    def get_symrefs(self) -> dict[bytes, bytes]:
        """Get a dict with all symrefs in this container.

        Returns: Dictionary mapping source ref to target ref
        """
        ret = {}
        for src in self.allkeys():
            try:
                ref_value = self.read_ref(src)
                assert ref_value is not None
                dst = parse_symref_value(ref_value)
            except ValueError:
                pass
            else:
                ret[src] = dst
        return ret

    def pack_refs(self, all: bool = False) -> None:
        """Pack loose refs into packed-refs file.

        Args:
            all: If True, pack all refs. If False, only pack tags.
        """
        raise NotImplementedError(self.pack_refs)


class DictRefsContainer(RefsContainer):
    """RefsContainer backed by a simple dict.

    This container does not support symbolic or packed references and is not
    threadsafe.
    """

    def __init__(
        self,
        refs: dict[bytes, bytes],
        logger: Optional[
            Callable[
                [
                    bytes,
                    Optional[bytes],
                    Optional[bytes],
                    Optional[bytes],
                    Optional[int],
                    Optional[int],
                    Optional[bytes],
                ],
                None,
            ]
        ] = None,
    ) -> None:
        """Initialize DictRefsContainer with refs dictionary and optional logger."""
        super().__init__(logger=logger)
        self._refs = refs
        self._peeled: dict[bytes, ObjectID] = {}
        self._watchers: set[Any] = set()

    def allkeys(self) -> set[bytes]:
        """Return all reference keys."""
        return set(self._refs.keys())

    def read_loose_ref(self, name: bytes) -> Optional[bytes]:
        """Read a loose reference."""
        return self._refs.get(name, None)

    def get_packed_refs(self) -> dict[bytes, bytes]:
        """Get packed references."""
        return {}

    def _notify(self, ref: bytes, newsha: Optional[bytes]) -> None:
        for watcher in self._watchers:
            watcher._notify((ref, newsha))

    def set_symbolic_ref(
        self,
        name: Ref,
        other: Ref,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> None:
        """Make a ref point at another ref.

        Args:
          name: Name of the ref to set
          other: Name of the ref to point at
          committer: Optional committer name for reflog
          timestamp: Optional timestamp for reflog
          timezone: Optional timezone for reflog
          message: Optional message for reflog
        """
        old = self.follow(name)[-1]
        new = SYMREF + other
        self._refs[name] = new
        self._notify(name, new)
        self._log(
            name,
            old,
            new,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )

    def set_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        new_ref: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Set a refname to new_ref only if it currently equals old_ref.

        This method follows all symbolic references, and can be used to perform
        an atomic compare-and-swap operation.

        Args:
          name: The refname to set.
          old_ref: The old sha the refname must refer to, or None to set
            unconditionally.
          new_ref: The new sha the refname will refer to.
          committer: Optional committer name for reflog
          timestamp: Optional timestamp for reflog
          timezone: Optional timezone for reflog
          message: Optional message for reflog

        Returns:
          True if the set was successful, False otherwise.
        """
        if old_ref is not None and self._refs.get(name, ZERO_SHA) != old_ref:
            return False
        # Only update the specific ref requested, not the whole chain
        self._check_refname(name)
        old = self._refs.get(name)
        self._refs[name] = new_ref
        self._notify(name, new_ref)
        self._log(
            name,
            old,
            new_ref,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )
        return True

    def add_if_new(
        self,
        name: Ref,
        ref: ObjectID,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Add a new reference only if it does not already exist.

        Args:
          name: Ref name
          ref: Ref value
          committer: Optional committer name for reflog
          timestamp: Optional timestamp for reflog
          timezone: Optional timezone for reflog
          message: Optional message for reflog

        Returns:
          True if the add was successful, False otherwise.
        """
        if name in self._refs:
            return False
        self._refs[name] = ref
        self._notify(name, ref)
        self._log(
            name,
            None,
            ref,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )
        return True

    def remove_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Remove a refname only if it currently equals old_ref.

        This method does not follow symbolic references. It can be used to
        perform an atomic compare-and-delete operation.

        Args:
          name: The refname to delete.
          old_ref: The old sha the refname must refer to, or None to
            delete unconditionally.
          committer: Optional committer name for reflog
          timestamp: Optional timestamp for reflog
          timezone: Optional timezone for reflog
          message: Optional message for reflog

        Returns:
          True if the delete was successful, False otherwise.
        """
        if old_ref is not None and self._refs.get(name, ZERO_SHA) != old_ref:
            return False
        try:
            old = self._refs.pop(name)
        except KeyError:
            pass
        else:
            self._notify(name, None)
            self._log(
                name,
                old,
                None,
                committer=committer,
                timestamp=timestamp,
                timezone=timezone,
                message=message,
            )
        return True

    def get_peeled(self, name: bytes) -> Optional[bytes]:
        """Get peeled version of a reference."""
        return self._peeled.get(name)

    def _update(self, refs: Mapping[bytes, bytes]) -> None:
        """Update multiple refs; intended only for testing."""
        # TODO(dborowitz): replace this with a public function that uses
        # set_if_equal.
        for ref, sha in refs.items():
            self.set_if_equals(ref, None, sha)

    def _update_peeled(self, peeled: Mapping[bytes, bytes]) -> None:
        """Update cached peeled refs; intended only for testing."""
        self._peeled.update(peeled)


class InfoRefsContainer(RefsContainer):
    """Refs container that reads refs from a info/refs file."""

    def __init__(self, f: BinaryIO) -> None:
        """Initialize InfoRefsContainer from info/refs file."""
        self._refs: dict[bytes, bytes] = {}
        self._peeled: dict[bytes, bytes] = {}
        refs = read_info_refs(f)
        (self._refs, self._peeled) = split_peeled_refs(refs)

    def allkeys(self) -> set[bytes]:
        """Return all reference keys."""
        return set(self._refs.keys())

    def read_loose_ref(self, name: bytes) -> Optional[bytes]:
        """Read a loose reference."""
        return self._refs.get(name, None)

    def get_packed_refs(self) -> dict[bytes, bytes]:
        """Get packed references."""
        return {}

    def get_peeled(self, name: bytes) -> Optional[bytes]:
        """Get peeled version of a reference."""
        try:
            return self._peeled[name]
        except KeyError:
            return self._refs[name]


class DiskRefsContainer(RefsContainer):
    """Refs container that reads refs from disk."""

    def __init__(
        self,
        path: Union[str, bytes, os.PathLike[str]],
        worktree_path: Optional[Union[str, bytes, os.PathLike[str]]] = None,
        logger: Optional[
            Callable[
                [
                    bytes,
                    bytes,
                    bytes,
                    Optional[bytes],
                    Optional[int],
                    Optional[int],
                    bytes,
                ],
                None,
            ]
        ] = None,
    ) -> None:
        """Initialize DiskRefsContainer."""
        super().__init__(logger=logger)
        # Convert path-like objects to strings, then to bytes for Git compatibility
        self.path = os.fsencode(os.fspath(path))
        if worktree_path is None:
            self.worktree_path = self.path
        else:
            self.worktree_path = os.fsencode(os.fspath(worktree_path))
        self._packed_refs: Optional[dict[bytes, bytes]] = None
        self._peeled_refs: Optional[dict[bytes, bytes]] = None

    def __repr__(self) -> str:
        """Return string representation of DiskRefsContainer."""
        return f"{self.__class__.__name__}({self.path!r})"

    def _iter_dir(
        self,
        path: bytes,
        base: bytes,
        dir_filter: Optional[Callable[[bytes], bool]] = None,
    ) -> Iterator[bytes]:
        refspath = os.path.join(path, base.rstrip(b"/"))
        prefix_len = len(os.path.join(path, b""))

        for root, dirs, files in os.walk(refspath):
            directory = root[prefix_len:]
            if os.path.sep != "/":
                directory = directory.replace(os.fsencode(os.path.sep), b"/")
            if dir_filter is not None:
                dirs[:] = [
                    d for d in dirs if dir_filter(b"/".join([directory, d, b""]))
                ]

            for filename in files:
                refname = b"/".join([directory, filename])
                if check_ref_format(refname):
                    yield refname

    def _iter_loose_refs(self, base: bytes = b"refs/") -> Iterator[bytes]:
        base = base.rstrip(b"/") + b"/"
        search_paths: list[tuple[bytes, Optional[Callable[[bytes], bool]]]] = []
        if base != b"refs/":
            path = self.worktree_path if is_per_worktree_ref(base) else self.path
            search_paths.append((path, None))
        elif self.worktree_path == self.path:
            # Iterate through all the refs from the main worktree
            search_paths.append((self.path, None))
        else:
            # Iterate through all the shared refs from the commondir, excluding per-worktree refs
            search_paths.append((self.path, lambda r: not is_per_worktree_ref(r)))
            # Iterate through all the per-worktree refs from the worktree's gitdir
            search_paths.append((self.worktree_path, is_per_worktree_ref))

        for path, dir_filter in search_paths:
            yield from self._iter_dir(path, base, dir_filter=dir_filter)

    def subkeys(self, base: bytes) -> set[bytes]:
        """Return subkeys under a given base reference path."""
        subkeys = set()

        for key in self._iter_loose_refs(base):
            if key.startswith(base):
                subkeys.add(key[len(base) :].strip(b"/"))

        for key in self.get_packed_refs():
            if key.startswith(base):
                subkeys.add(key[len(base) :].strip(b"/"))
        return subkeys

    def allkeys(self) -> set[bytes]:
        """Return all reference keys."""
        allkeys = set()
        if os.path.exists(self.refpath(HEADREF)):
            allkeys.add(HEADREF)

        allkeys.update(self._iter_loose_refs())
        allkeys.update(self.get_packed_refs())
        return allkeys

    def refpath(self, name: bytes) -> bytes:
        """Return the disk path of a ref."""
        path = name
        if os.path.sep != "/":
            path = path.replace(b"/", os.fsencode(os.path.sep))

        root_dir = self.worktree_path if is_per_worktree_ref(name) else self.path
        return os.path.join(root_dir, path)

    def get_packed_refs(self) -> dict[bytes, bytes]:
        """Get contents of the packed-refs file.

        Returns: Dictionary mapping ref names to SHA1s

        Note: Will return an empty dictionary when no packed-refs file is
            present.
        """
        # TODO: invalidate the cache on repacking
        if self._packed_refs is None:
            # set both to empty because we want _peeled_refs to be
            # None if and only if _packed_refs is also None.
            self._packed_refs = {}
            self._peeled_refs = {}
            path = os.path.join(self.path, b"packed-refs")
            try:
                f = GitFile(path, "rb")
            except FileNotFoundError:
                return {}
            with f:
                first_line = next(iter(f)).rstrip()
                if first_line.startswith(b"# pack-refs") and b" peeled" in first_line:
                    for sha, name, peeled in read_packed_refs_with_peeled(f):
                        self._packed_refs[name] = sha
                        if peeled:
                            self._peeled_refs[name] = peeled
                else:
                    f.seek(0)
                    for sha, name in read_packed_refs(f):
                        self._packed_refs[name] = sha
        return self._packed_refs

    def add_packed_refs(self, new_refs: Mapping[Ref, Optional[ObjectID]]) -> None:
        """Add the given refs as packed refs.

        Args:
          new_refs: A mapping of ref names to targets; if a target is None that
            means remove the ref
        """
        if not new_refs:
            return

        path = os.path.join(self.path, b"packed-refs")

        with GitFile(path, "wb") as f:
            # reread cached refs from disk, while holding the lock
            packed_refs = self.get_packed_refs().copy()

            for ref, target in new_refs.items():
                # sanity check
                if ref == HEADREF:
                    raise ValueError("cannot pack HEAD")

                # remove any loose refs pointing to this one -- please
                # note that this bypasses remove_if_equals as we don't
                # want to affect packed refs in here
                with suppress(OSError):
                    os.remove(self.refpath(ref))

                if target is not None:
                    packed_refs[ref] = target
                else:
                    packed_refs.pop(ref, None)

            write_packed_refs(f, packed_refs, self._peeled_refs)

            self._packed_refs = packed_refs

    def get_peeled(self, name: bytes) -> Optional[bytes]:
        """Return the cached peeled value of a ref, if available.

        Args:
          name: Name of the ref to peel
        Returns: The peeled value of the ref. If the ref is known not point to
            a tag, this will be the SHA the ref refers to. If the ref may point
            to a tag, but no cached information is available, None is returned.
        """
        self.get_packed_refs()
        if (
            self._peeled_refs is None
            or self._packed_refs is None
            or name not in self._packed_refs
        ):
            # No cache: no peeled refs were read, or this ref is loose
            return None
        if name in self._peeled_refs:
            return self._peeled_refs[name]
        else:
            # Known not peelable
            return self[name]

    def read_loose_ref(self, name: bytes) -> Optional[bytes]:
        """Read a reference file and return its contents.

        If the reference file a symbolic reference, only read the first line of
        the file. Otherwise, only read the first 40 bytes.

        Args:
          name: the refname to read, relative to refpath
        Returns: The contents of the ref file, or None if the file does not
            exist.

        Raises:
          IOError: if any other error occurs
        """
        filename = self.refpath(name)
        try:
            with GitFile(filename, "rb") as f:
                header = f.read(len(SYMREF))
                if header == SYMREF:
                    # Read only the first line
                    return header + next(iter(f)).rstrip(b"\r\n")
                else:
                    # Read only the first 40 bytes
                    return header + f.read(40 - len(SYMREF))
        except (OSError, UnicodeError):
            # don't assume anything specific about the error; in
            # particular, invalid or forbidden paths can raise weird
            # errors depending on the specific operating system
            return None

    def _remove_packed_ref(self, name: bytes) -> None:
        if self._packed_refs is None:
            return
        filename = os.path.join(self.path, b"packed-refs")
        # reread cached refs from disk, while holding the lock
        f = GitFile(filename, "wb")
        try:
            self._packed_refs = None
            self.get_packed_refs()

            if self._packed_refs is None or name not in self._packed_refs:
                f.abort()
                return

            del self._packed_refs[name]
            if self._peeled_refs is not None:
                with suppress(KeyError):
                    del self._peeled_refs[name]
            write_packed_refs(f, self._packed_refs, self._peeled_refs)
            f.close()
        except BaseException:
            f.abort()
            raise

    def set_symbolic_ref(
        self,
        name: bytes,
        other: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> None:
        """Make a ref point at another ref.

        Args:
          name: Name of the ref to set
          other: Name of the ref to point at
          committer: Optional committer name
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Optional message to describe the change
        """
        self._check_refname(name)
        self._check_refname(other)
        filename = self.refpath(name)
        f = GitFile(filename, "wb")
        try:
            f.write(SYMREF + other + b"\n")
            sha = self.follow(name)[-1]
            self._log(
                name,
                sha,
                sha,
                committer=committer,
                timestamp=timestamp,
                timezone=timezone,
                message=message,
            )
        except BaseException:
            f.abort()
            raise
        else:
            f.close()

    def set_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        new_ref: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Set a refname to new_ref only if it currently equals old_ref.

        This method follows all symbolic references, and can be used to perform
        an atomic compare-and-swap operation.

        Args:
          name: The refname to set.
          old_ref: The old sha the refname must refer to, or None to set
            unconditionally.
          new_ref: The new sha the refname will refer to.
          committer: Optional committer name
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Set message for reflog
        Returns: True if the set was successful, False otherwise.
        """
        self._check_refname(name)
        try:
            realnames, _ = self.follow(name)
            realname = realnames[-1]
        except (KeyError, IndexError, SymrefLoop):
            realname = name
        filename = self.refpath(realname)

        # make sure none of the ancestor folders is in packed refs
        probe_ref = os.path.dirname(realname)
        packed_refs = self.get_packed_refs()
        while probe_ref:
            if packed_refs.get(probe_ref, None) is not None:
                raise NotADirectoryError(filename)
            probe_ref = os.path.dirname(probe_ref)

        ensure_dir_exists(os.path.dirname(filename))
        with GitFile(filename, "wb") as f:
            if old_ref is not None:
                try:
                    # read again while holding the lock to handle race conditions
                    orig_ref = self.read_loose_ref(realname)
                    if orig_ref is None:
                        orig_ref = self.get_packed_refs().get(realname, ZERO_SHA)
                    if orig_ref != old_ref:
                        f.abort()
                        return False
                except OSError:
                    f.abort()
                    raise

            # Check if ref already has the desired value while holding the lock
            # This avoids fsync when ref is unchanged but still detects lock conflicts
            current_ref = self.read_loose_ref(realname)
            if current_ref is None:
                current_ref = packed_refs.get(realname, None)

            if current_ref is not None and current_ref == new_ref:
                # Ref already has desired value, abort write to avoid fsync
                f.abort()
                return True

            try:
                f.write(new_ref + b"\n")
            except OSError:
                f.abort()
                raise
            self._log(
                realname,
                old_ref,
                new_ref,
                committer=committer,
                timestamp=timestamp,
                timezone=timezone,
                message=message,
            )
        return True

    def add_if_new(
        self,
        name: bytes,
        ref: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Add a new reference only if it does not already exist.

        This method follows symrefs, and only ensures that the last ref in the
        chain does not exist.

        Args:
          name: The refname to set.
          ref: The new sha the refname will refer to.
          committer: Optional committer name
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Optional message for reflog
        Returns: True if the add was successful, False otherwise.
        """
        try:
            realnames, contents = self.follow(name)
            if contents is not None:
                return False
            realname = realnames[-1]
        except (KeyError, IndexError):
            realname = name
        self._check_refname(realname)
        filename = self.refpath(realname)
        ensure_dir_exists(os.path.dirname(filename))
        with GitFile(filename, "wb") as f:
            if os.path.exists(filename) or name in self.get_packed_refs():
                f.abort()
                return False
            try:
                f.write(ref + b"\n")
            except OSError:
                f.abort()
                raise
            else:
                self._log(
                    name,
                    None,
                    ref,
                    committer=committer,
                    timestamp=timestamp,
                    timezone=timezone,
                    message=message,
                )
        return True

    def remove_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Remove a refname only if it currently equals old_ref.

        This method does not follow symbolic references. It can be used to
        perform an atomic compare-and-delete operation.

        Args:
          name: The refname to delete.
          old_ref: The old sha the refname must refer to, or None to
            delete unconditionally.
          committer: Optional committer name
          timestamp: Optional timestamp
          timezone: Optional timezone
          message: Optional message
        Returns: True if the delete was successful, False otherwise.
        """
        self._check_refname(name)
        filename = self.refpath(name)
        ensure_dir_exists(os.path.dirname(filename))
        f = GitFile(filename, "wb")
        try:
            if old_ref is not None:
                orig_ref = self.read_loose_ref(name)
                if orig_ref is None:
                    orig_ref = self.get_packed_refs().get(name, ZERO_SHA)
                if orig_ref != old_ref:
                    return False

            # remove the reference file itself
            try:
                found = os.path.lexists(filename)
            except OSError:
                # may only be packed, or otherwise unstorable
                found = False

            if found:
                os.remove(filename)

            self._remove_packed_ref(name)
            self._log(
                name,
                old_ref,
                None,
                committer=committer,
                timestamp=timestamp,
                timezone=timezone,
                message=message,
            )
        finally:
            # never write, we just wanted the lock
            f.abort()

        # outside of the lock, clean-up any parent directory that might now
        # be empty. this ensures that re-creating a reference of the same
        # name of what was previously a directory works as expected
        parent = name
        while True:
            try:
                parent, _ = parent.rsplit(b"/", 1)
            except ValueError:
                break

            if parent == b"refs":
                break
            parent_filename = self.refpath(parent)
            try:
                os.rmdir(parent_filename)
            except OSError:
                # this can be caused by the parent directory being
                # removed by another process, being not empty, etc.
                # in any case, this is non fatal because we already
                # removed the reference, just ignore it
                break

        return True

    def pack_refs(self, all: bool = False) -> None:
        """Pack loose refs into packed-refs file.

        Args:
            all: If True, pack all refs. If False, only pack tags.
        """
        refs_to_pack: dict[Ref, Optional[ObjectID]] = {}
        for ref in self.allkeys():
            if ref == HEADREF:
                # Never pack HEAD
                continue
            if all or ref.startswith(LOCAL_TAG_PREFIX):
                try:
                    sha = self[ref]
                    if sha:
                        refs_to_pack[ref] = sha
                except KeyError:
                    # Broken ref, skip it
                    pass

        if refs_to_pack:
            self.add_packed_refs(refs_to_pack)


def _split_ref_line(line: bytes) -> tuple[bytes, bytes]:
    """Split a single ref line into a tuple of SHA1 and name."""
    fields = line.rstrip(b"\n\r").split(b" ")
    if len(fields) != 2:
        raise PackedRefsException(f"invalid ref line {line!r}")
    sha, name = fields
    if not valid_hexsha(sha):
        raise PackedRefsException(f"Invalid hex sha {sha!r}")
    if not check_ref_format(name):
        raise PackedRefsException(f"invalid ref name {name!r}")
    return (sha, name)


def read_packed_refs(f: IO[bytes]) -> Iterator[tuple[bytes, bytes]]:
    """Read a packed refs file.

    Args:
      f: file-like object to read from
    Returns: Iterator over tuples with SHA1s and ref names.
    """
    for line in f:
        if line.startswith(b"#"):
            # Comment
            continue
        if line.startswith(b"^"):
            raise PackedRefsException("found peeled ref in packed-refs without peeled")
        yield _split_ref_line(line)


def read_packed_refs_with_peeled(
    f: IO[bytes],
) -> Iterator[tuple[bytes, bytes, Optional[bytes]]]:
    """Read a packed refs file including peeled refs.

    Assumes the "# pack-refs with: peeled" line was already read. Yields tuples
    with ref names, SHA1s, and peeled SHA1s (or None).

    Args:
      f: file-like object to read from, seek'ed to the second line
    """
    last = None
    for line in f:
        if line.startswith(b"#"):
            continue
        line = line.rstrip(b"\r\n")
        if line.startswith(b"^"):
            if not last:
                raise PackedRefsException("unexpected peeled ref line")
            if not valid_hexsha(line[1:]):
                raise PackedRefsException(f"Invalid hex sha {line[1:]!r}")
            sha, name = _split_ref_line(last)
            last = None
            yield (sha, name, line[1:])
        else:
            if last:
                sha, name = _split_ref_line(last)
                yield (sha, name, None)
            last = line
    if last:
        sha, name = _split_ref_line(last)
        yield (sha, name, None)


def write_packed_refs(
    f: IO[bytes],
    packed_refs: Mapping[bytes, bytes],
    peeled_refs: Optional[Mapping[bytes, bytes]] = None,
) -> None:
    """Write a packed refs file.

    Args:
      f: empty file-like object to write to
      packed_refs: dict of refname to sha of packed refs to write
      peeled_refs: dict of refname to peeled value of sha
    """
    if peeled_refs is None:
        peeled_refs = {}
    else:
        f.write(b"# pack-refs with: peeled\n")
    for refname in sorted(packed_refs.keys()):
        f.write(git_line(packed_refs[refname], refname))
        if refname in peeled_refs:
            f.write(b"^" + peeled_refs[refname] + b"\n")


def read_info_refs(f: BinaryIO) -> dict[bytes, bytes]:
    """Read info/refs file.

    Args:
      f: File-like object to read from

    Returns:
      Dictionary mapping ref names to SHA1s
    """
    ret = {}
    for line in f.readlines():
        (sha, name) = line.rstrip(b"\r\n").split(b"\t", 1)
        ret[name] = sha
    return ret


def write_info_refs(
    refs: Mapping[bytes, bytes], store: ObjectContainer
) -> Iterator[bytes]:
    """Generate info refs."""
    # TODO: Avoid recursive import :(
    from .object_store import peel_sha

    for name, sha in sorted(refs.items()):
        # get_refs() includes HEAD as a special case, but we don't want to
        # advertise it
        if name == HEADREF:
            continue
        try:
            o = store[sha]
        except KeyError:
            continue
        _unpeeled, peeled = peel_sha(store, sha)
        yield o.id + b"\t" + name + b"\n"
        if o.id != peeled.id:
            yield peeled.id + b"\t" + name + PEELED_TAG_SUFFIX + b"\n"


def is_local_branch(x: bytes) -> bool:
    """Check if a ref name is a local branch."""
    return x.startswith(LOCAL_BRANCH_PREFIX)


def local_branch_name(name: bytes) -> bytes:
    """Build a full branch ref from a short name.

    Args:
      name: Short branch name (e.g., b"master") or full ref

    Returns:
      Full branch ref name (e.g., b"refs/heads/master")

    Examples:
      >>> local_branch_name(b"master")
      b'refs/heads/master'
      >>> local_branch_name(b"refs/heads/master")
      b'refs/heads/master'
    """
    if name.startswith(LOCAL_BRANCH_PREFIX):
        return name
    return LOCAL_BRANCH_PREFIX + name


def local_tag_name(name: bytes) -> bytes:
    """Build a full tag ref from a short name.

    Args:
      name: Short tag name (e.g., b"v1.0") or full ref

    Returns:
      Full tag ref name (e.g., b"refs/tags/v1.0")

    Examples:
      >>> local_tag_name(b"v1.0")
      b'refs/tags/v1.0'
      >>> local_tag_name(b"refs/tags/v1.0")
      b'refs/tags/v1.0'
    """
    if name.startswith(LOCAL_TAG_PREFIX):
        return name
    return LOCAL_TAG_PREFIX + name


def local_replace_name(name: bytes) -> bytes:
    """Build a full replace ref from a short name.

    Args:
      name: Short replace name (object SHA) or full ref

    Returns:
      Full replace ref name (e.g., b"refs/replace/<sha>")

    Examples:
      >>> local_replace_name(b"abc123")
      b'refs/replace/abc123'
      >>> local_replace_name(b"refs/replace/abc123")
      b'refs/replace/abc123'
    """
    if name.startswith(LOCAL_REPLACE_PREFIX):
        return name
    return LOCAL_REPLACE_PREFIX + name


def extract_branch_name(ref: bytes) -> bytes:
    """Extract branch name from a full branch ref.

    Args:
      ref: Full branch ref (e.g., b"refs/heads/master")

    Returns:
      Short branch name (e.g., b"master")

    Raises:
      ValueError: If ref is not a local branch

    Examples:
      >>> extract_branch_name(b"refs/heads/master")
      b'master'
      >>> extract_branch_name(b"refs/heads/feature/foo")
      b'feature/foo'
    """
    if not ref.startswith(LOCAL_BRANCH_PREFIX):
        raise ValueError(f"Not a local branch ref: {ref!r}")
    return ref[len(LOCAL_BRANCH_PREFIX) :]


def extract_tag_name(ref: bytes) -> bytes:
    """Extract tag name from a full tag ref.

    Args:
      ref: Full tag ref (e.g., b"refs/tags/v1.0")

    Returns:
      Short tag name (e.g., b"v1.0")

    Raises:
      ValueError: If ref is not a local tag

    Examples:
      >>> extract_tag_name(b"refs/tags/v1.0")
      b'v1.0'
    """
    if not ref.startswith(LOCAL_TAG_PREFIX):
        raise ValueError(f"Not a local tag ref: {ref!r}")
    return ref[len(LOCAL_TAG_PREFIX) :]


def shorten_ref_name(ref: bytes) -> bytes:
    """Convert a full ref name to its short form.

    Args:
      ref: Full ref name (e.g., b"refs/heads/master")

    Returns:
      Short ref name (e.g., b"master")

    Examples:
      >>> shorten_ref_name(b"refs/heads/master")
      b'master'
      >>> shorten_ref_name(b"refs/remotes/origin/main")
      b'origin/main'
      >>> shorten_ref_name(b"refs/tags/v1.0")
      b'v1.0'
      >>> shorten_ref_name(b"HEAD")
      b'HEAD'
    """
    if ref.startswith(LOCAL_BRANCH_PREFIX):
        return ref[len(LOCAL_BRANCH_PREFIX) :]
    elif ref.startswith(LOCAL_REMOTE_PREFIX):
        return ref[len(LOCAL_REMOTE_PREFIX) :]
    elif ref.startswith(LOCAL_TAG_PREFIX):
        return ref[len(LOCAL_TAG_PREFIX) :]
    return ref


T = TypeVar("T", dict[bytes, bytes], dict[bytes, Optional[bytes]])


def strip_peeled_refs(refs: T) -> T:
    """Remove all peeled refs."""
    return {
        ref: sha for (ref, sha) in refs.items() if not ref.endswith(PEELED_TAG_SUFFIX)
    }


def split_peeled_refs(refs: T) -> tuple[T, dict[bytes, bytes]]:
    """Split peeled refs from regular refs."""
    peeled: dict[bytes, bytes] = {}
    regular = {k: v for k, v in refs.items() if not k.endswith(PEELED_TAG_SUFFIX)}

    for ref, sha in refs.items():
        if ref.endswith(PEELED_TAG_SUFFIX):
            # Only add to peeled dict if sha is not None
            if sha is not None:
                peeled[ref[: -len(PEELED_TAG_SUFFIX)]] = sha

    return regular, peeled


def _set_origin_head(
    refs: RefsContainer, origin: bytes, origin_head: Optional[bytes]
) -> None:
    # set refs/remotes/origin/HEAD
    origin_base = b"refs/remotes/" + origin + b"/"
    if origin_head and origin_head.startswith(LOCAL_BRANCH_PREFIX):
        origin_ref = origin_base + HEADREF
        target_ref = origin_base + extract_branch_name(origin_head)
        if target_ref in refs:
            refs.set_symbolic_ref(origin_ref, target_ref)


def _set_default_branch(
    refs: RefsContainer,
    origin: bytes,
    origin_head: Optional[bytes],
    branch: Optional[bytes],
    ref_message: Optional[bytes],
) -> bytes:
    """Set the default branch."""
    origin_base = b"refs/remotes/" + origin + b"/"
    if branch:
        origin_ref = origin_base + branch
        if origin_ref in refs:
            local_ref = local_branch_name(branch)
            refs.add_if_new(local_ref, refs[origin_ref], ref_message)
            head_ref = local_ref
        elif local_tag_name(branch) in refs:
            head_ref = local_tag_name(branch)
        else:
            raise ValueError(f"{os.fsencode(branch)!r} is not a valid branch or tag")
    elif origin_head:
        head_ref = origin_head
        if origin_head.startswith(LOCAL_BRANCH_PREFIX):
            origin_ref = origin_base + extract_branch_name(origin_head)
        else:
            origin_ref = origin_head
        try:
            refs.add_if_new(head_ref, refs[origin_ref], ref_message)
        except KeyError:
            pass
    else:
        raise ValueError("neither origin_head nor branch are provided")
    return head_ref


def _set_head(
    refs: RefsContainer, head_ref: bytes, ref_message: Optional[bytes]
) -> Optional[bytes]:
    if head_ref.startswith(LOCAL_TAG_PREFIX):
        # detach HEAD at specified tag
        head = refs[head_ref]
        if isinstance(head, Tag):
            _cls, obj = head.object
            head = obj.get_object(obj).id
        del refs[HEADREF]
        refs.set_if_equals(HEADREF, None, head, message=ref_message)
    else:
        # set HEAD to specific branch
        try:
            head = refs[head_ref]
            refs.set_symbolic_ref(HEADREF, head_ref)
            refs.set_if_equals(HEADREF, None, head, message=ref_message)
        except KeyError:
            head = None
    return head


def _import_remote_refs(
    refs_container: RefsContainer,
    remote_name: str,
    refs: dict[bytes, Optional[bytes]],
    message: Optional[bytes] = None,
    prune: bool = False,
    prune_tags: bool = False,
) -> None:
    stripped_refs = strip_peeled_refs(refs)
    branches = {
        extract_branch_name(n): v
        for (n, v) in stripped_refs.items()
        if n.startswith(LOCAL_BRANCH_PREFIX) and v is not None
    }
    refs_container.import_refs(
        b"refs/remotes/" + remote_name.encode(),
        branches,
        message=message,
        prune=prune,
    )
    tags = {
        extract_tag_name(n): v
        for (n, v) in stripped_refs.items()
        if n.startswith(LOCAL_TAG_PREFIX)
        and not n.endswith(PEELED_TAG_SUFFIX)
        and v is not None
    }
    refs_container.import_refs(
        LOCAL_TAG_PREFIX, tags, message=message, prune=prune_tags
    )


def serialize_refs(
    store: ObjectContainer, refs: Mapping[bytes, bytes]
) -> dict[bytes, bytes]:
    """Serialize refs with peeled refs.

    Args:
      store: Object store to peel refs from
      refs: Dictionary of ref names to SHAs

    Returns:
      Dictionary with refs and peeled refs (marked with ^{})
    """
    # TODO: Avoid recursive import :(
    from .object_store import peel_sha

    ret = {}
    for ref, sha in refs.items():
        try:
            unpeeled, peeled = peel_sha(store, sha)
        except KeyError:
            warnings.warn(
                "ref {} points at non-present sha {}".format(
                    ref.decode("utf-8", "replace"), sha.decode("ascii")
                ),
                UserWarning,
            )
            continue
        else:
            if isinstance(unpeeled, Tag):
                ret[ref + PEELED_TAG_SUFFIX] = peeled.id
            ret[ref] = unpeeled.id
    return ret


class locked_ref:
    """Lock a ref while making modifications.

    Works as a context manager.
    """

    def __init__(self, refs_container: DiskRefsContainer, refname: Ref) -> None:
        """Initialize a locked ref.

        Args:
          refs_container: The DiskRefsContainer to lock the ref in
          refname: The ref name to lock
        """
        self._refs_container = refs_container
        self._refname = refname
        self._file: Optional[_GitFile] = None
        self._realname: Optional[Ref] = None
        self._deleted = False

    def __enter__(self) -> "locked_ref":
        """Enter the context manager and acquire the lock.

        Returns:
          This locked_ref instance

        Raises:
          OSError: If the lock cannot be acquired
        """
        self._refs_container._check_refname(self._refname)
        try:
            realnames, _ = self._refs_container.follow(self._refname)
            self._realname = realnames[-1]
        except (KeyError, IndexError, SymrefLoop):
            self._realname = self._refname

        filename = self._refs_container.refpath(self._realname)
        ensure_dir_exists(os.path.dirname(filename))
        f = GitFile(filename, "wb")
        self._file = f
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[types.TracebackType],
    ) -> None:
        """Exit the context manager and release the lock.

        Args:
          exc_type: Type of exception if one occurred
          exc_value: Exception instance if one occurred
          traceback: Traceback if an exception occurred
        """
        if self._file:
            if exc_type is not None or self._deleted:
                self._file.abort()
            else:
                self._file.close()

    def get(self) -> Optional[bytes]:
        """Get the current value of the ref."""
        if not self._file:
            raise RuntimeError("locked_ref not in context")

        assert self._realname is not None
        current_ref = self._refs_container.read_loose_ref(self._realname)
        if current_ref is None:
            current_ref = self._refs_container.get_packed_refs().get(
                self._realname, None
            )
        return current_ref

    def ensure_equals(self, expected_value: Optional[bytes]) -> bool:
        """Ensure the ref currently equals the expected value.

        Args:
            expected_value: The expected current value of the ref
        Returns:
            True if the ref equals the expected value, False otherwise
        """
        current_value = self.get()
        return current_value == expected_value

    def set(self, new_ref: bytes) -> None:
        """Set the ref to a new value.

        Args:
            new_ref: The new SHA1 or symbolic ref value
        """
        if not self._file:
            raise RuntimeError("locked_ref not in context")

        if not (valid_hexsha(new_ref) or new_ref.startswith(SYMREF)):
            raise ValueError(f"{new_ref!r} must be a valid sha (40 chars) or a symref")

        self._file.seek(0)
        self._file.truncate()
        self._file.write(new_ref + b"\n")
        self._deleted = False

    def set_symbolic_ref(self, target: Ref) -> None:
        """Make this ref point at another ref.

        Args:
            target: Name of the ref to point at
        """
        if not self._file:
            raise RuntimeError("locked_ref not in context")

        self._refs_container._check_refname(target)
        self._file.seek(0)
        self._file.truncate()
        self._file.write(SYMREF + target + b"\n")
        self._deleted = False

    def delete(self) -> None:
        """Delete the ref file while holding the lock."""
        if not self._file:
            raise RuntimeError("locked_ref not in context")

        # Delete the actual ref file while holding the lock
        if self._realname:
            filename = self._refs_container.refpath(self._realname)
            try:
                if os.path.lexists(filename):
                    os.remove(filename)
            except FileNotFoundError:
                pass
            self._refs_container._remove_packed_ref(self._realname)

        self._deleted = True


class NamespacedRefsContainer(RefsContainer):
    """Wrapper that adds namespace prefix to all ref operations.

    This implements Git's GIT_NAMESPACE feature, which stores refs under
    refs/namespaces/<namespace>/ and filters operations to only show refs
    within that namespace.

    Example:
        With namespace "foo", a ref "refs/heads/master" is stored as
        "refs/namespaces/foo/refs/heads/master" in the underlying container.
    """

    def __init__(self, refs: RefsContainer, namespace: bytes) -> None:
        """Initialize NamespacedRefsContainer.

        Args:
          refs: The underlying refs container to wrap
          namespace: The namespace prefix (e.g., b"foo" or b"foo/bar")
        """
        super().__init__(logger=refs._logger)
        self._refs = refs
        # Build namespace prefix: refs/namespaces/<namespace>/
        # Support nested namespaces: foo/bar -> refs/namespaces/foo/refs/namespaces/bar/
        namespace_parts = namespace.split(b"/")
        self._namespace_prefix = b""
        for part in namespace_parts:
            self._namespace_prefix += b"refs/namespaces/" + part + b"/"

    def _apply_namespace(self, name: bytes) -> bytes:
        """Apply namespace prefix to a ref name."""
        # HEAD and other special refs are not namespaced
        if name == HEADREF or not name.startswith(b"refs/"):
            return name
        return self._namespace_prefix + name

    def _strip_namespace(self, name: bytes) -> Optional[bytes]:
        """Remove namespace prefix from a ref name.

        Returns None if the ref is not in our namespace.
        """
        # HEAD and other special refs are not namespaced
        if name == HEADREF or not name.startswith(b"refs/"):
            return name
        if name.startswith(self._namespace_prefix):
            return name[len(self._namespace_prefix) :]
        return None

    def allkeys(self) -> set[bytes]:
        """Return all reference keys in this namespace."""
        keys = set()
        for key in self._refs.allkeys():
            stripped = self._strip_namespace(key)
            if stripped is not None:
                keys.add(stripped)
        return keys

    def read_loose_ref(self, name: bytes) -> Optional[bytes]:
        """Read a loose reference."""
        return self._refs.read_loose_ref(self._apply_namespace(name))

    def get_packed_refs(self) -> dict[Ref, ObjectID]:
        """Get packed refs within this namespace."""
        packed = {}
        for name, value in self._refs.get_packed_refs().items():
            stripped = self._strip_namespace(name)
            if stripped is not None:
                packed[stripped] = value
        return packed

    def add_packed_refs(self, new_refs: Mapping[Ref, Optional[ObjectID]]) -> None:
        """Add packed refs with namespace prefix."""
        namespaced_refs = {
            self._apply_namespace(name): value for name, value in new_refs.items()
        }
        self._refs.add_packed_refs(namespaced_refs)

    def get_peeled(self, name: bytes) -> Optional[ObjectID]:
        """Return the cached peeled value of a ref."""
        return self._refs.get_peeled(self._apply_namespace(name))

    def set_symbolic_ref(
        self,
        name: bytes,
        other: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> None:
        """Make a ref point at another ref."""
        self._refs.set_symbolic_ref(
            self._apply_namespace(name),
            self._apply_namespace(other),
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )

    def set_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        new_ref: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Set a refname to new_ref only if it currently equals old_ref."""
        return self._refs.set_if_equals(
            self._apply_namespace(name),
            old_ref,
            new_ref,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )

    def add_if_new(
        self,
        name: bytes,
        ref: bytes,
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Add a new reference only if it does not already exist."""
        return self._refs.add_if_new(
            self._apply_namespace(name),
            ref,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )

    def remove_if_equals(
        self,
        name: bytes,
        old_ref: Optional[bytes],
        committer: Optional[bytes] = None,
        timestamp: Optional[int] = None,
        timezone: Optional[int] = None,
        message: Optional[bytes] = None,
    ) -> bool:
        """Remove a refname only if it currently equals old_ref."""
        return self._refs.remove_if_equals(
            self._apply_namespace(name),
            old_ref,
            committer=committer,
            timestamp=timestamp,
            timezone=timezone,
            message=message,
        )

    def pack_refs(self, all: bool = False) -> None:
        """Pack loose refs into packed-refs file.

        Note: This packs all refs in the underlying container, not just
        those in the namespace.
        """
        self._refs.pack_refs(all=all)


def filter_ref_prefix(refs: T, prefixes: Iterable[bytes]) -> T:
    """Filter refs to only include those with a given prefix.

    Args:
      refs: A dictionary of refs.
      prefixes: The prefixes to filter by.
    """
    filtered = {k: v for k, v in refs.items() if any(k.startswith(p) for p in prefixes)}
    return filtered


def is_per_worktree_ref(ref: bytes) -> bool:
    """Returns whether a reference is stored per worktree or not.

    Per-worktree references are:
    - all pseudorefs, e.g. HEAD
    - all references stored inside "refs/bisect/", "refs/worktree/" and "refs/rewritten/"

    All refs starting with "refs/" are shared, except for the ones listed above.

    See https://git-scm.com/docs/git-worktree#_refs.
    """
    return not ref.startswith(b"refs/") or ref.startswith(
        (b"refs/bisect/", b"refs/worktree/", b"refs/rewritten/")
    )
