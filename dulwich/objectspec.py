# objectspec.py -- Object specification
# Copyright (C) 2014 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Object specification."""

__all__ = [
    "AmbiguousShortId",
    "parse_commit",
    "parse_commit_range",
    "parse_object",
    "parse_ref",
    "parse_refs",
    "parse_reftuple",
    "parse_reftuples",
    "parse_tree",
    "scan_for_short_id",
    "to_bytes",
]

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .objects import Commit, ObjectID, RawObjectID, ShaFile, Tag, Tree
from .refs import Ref, local_branch_name, local_tag_name
from .repo import BaseRepo

if TYPE_CHECKING:
    from .object_store import BaseObjectStore
    from .refs import Ref, RefsContainer
    from .repo import Repo


def to_bytes(text: str | bytes) -> bytes:
    """Convert text to bytes.

    Args:
      text: Text to convert (str or bytes)

    Returns:
      Bytes representation of text
    """
    if isinstance(text, str):
        return text.encode("ascii")
    return text


def _resolve_object(
    repo: "Repo", ref: Ref | ObjectID | RawObjectID | bytes
) -> "ShaFile":
    """Resolve a reference to an object using multiple strategies."""
    try:
        return repo[ref]
    except KeyError:
        try:
            ref_sha = parse_ref(repo, ref)
            return repo[ref_sha]
        except KeyError:
            try:
                return repo.object_store[ref]  # type: ignore[index]
            except (KeyError, ValueError):
                # Re-raise original KeyError for consistency
                raise KeyError(ref)


def _parse_number_suffix(suffix: bytes) -> tuple[int, bytes]:
    """Parse a number from the start of suffix, return (number, remaining)."""
    if not suffix or not suffix[0:1].isdigit():
        return 1, suffix

    end = 1
    while end < len(suffix) and suffix[end : end + 1].isdigit():
        end += 1
    return int(suffix[:end]), suffix[end:]


def parse_object(repo: "Repo", objectish: bytes | str) -> "ShaFile":
    """Parse a string referring to an object.

    Args:
      repo: A `Repo` object
      objectish: A string referring to an object
    Returns: A git object
    Raises:
      KeyError: If the object can not be found
    """
    objectish = to_bytes(objectish)

    # Handle :<path> - lookup path in tree or index
    if b":" in objectish:
        rev, path = objectish.split(b":", 1)
        if not rev:
            # Index path lookup: :path or :N:path where N is stage 0-3
            stage = 0
            if path and path[0:1].isdigit() and len(path) > 2 and path[1:2] == b":":
                stage = int(path[0:1])
                if stage > 3:
                    raise ValueError(f"Invalid stage number: {stage}. Must be 0-3.")
                path = path[2:]

            # Open the index and look up the path

            try:
                index = repo.open_index()
            except AttributeError:
                raise NotImplementedError(
                    "Index path lookup requires a non-bare repository"
                )

            if path not in index:
                raise KeyError(f"Path {path!r} not found in index")

            entry = index[path]
            # Handle ConflictedIndexEntry (merge stages)
            from .index import ConflictedIndexEntry

            if isinstance(entry, ConflictedIndexEntry):
                if stage == 0:
                    raise ValueError(
                        f"Path {path!r} has unresolved conflicts. "
                        "Use :1:path, :2:path, or :3:path to access specific stages."
                    )
                elif stage == 1:
                    if entry.ancestor is None:
                        raise KeyError(f"Path {path!r} has no ancestor (stage 1)")
                    return repo[entry.ancestor.sha]
                elif stage == 2:
                    if entry.this is None:
                        raise KeyError(f"Path {path!r} has no 'this' version (stage 2)")
                    return repo[entry.this.sha]
                elif stage == 3:
                    if entry.other is None:
                        raise KeyError(
                            f"Path {path!r} has no 'other' version (stage 3)"
                        )
                    return repo[entry.other.sha]
            else:
                # Regular IndexEntry - only stage 0 is valid
                if stage != 0:
                    raise ValueError(
                        f"Path {path!r} has no conflicts. Only :0:{path!r} or :{path!r} is valid."
                    )
                return repo[entry.sha]

        # Regular tree lookup: rev:path
        tree = parse_tree(repo, rev)
        _mode, sha = tree.lookup_path(repo.object_store.__getitem__, path)
        return repo[sha]

    # Handle @{N} or @{time} - reflog lookup
    if b"@{" in objectish:
        base, rest = objectish.split(b"@{", 1)
        if not rest.endswith(b"}"):
            raise ValueError("Invalid @{} syntax")
        spec = rest[:-1]

        ref = base if base else b"HEAD"
        entries = list(repo.read_reflog(ref))
        entries.reverse()  # Git uses reverse chronological order

        if spec.isdigit():
            # Check if it's a small index or a timestamp
            # Git treats values < number of entries as indices, larger values as timestamps
            num = int(spec)
            if num < len(entries):
                # Treat as numeric index: @{N}
                return repo[entries[num].new_sha]
            # Otherwise fall through to treat as timestamp

        # Time specification: @{time} (includes large numeric values)
        from .approxidate import parse_approxidate

        target_time = parse_approxidate(spec)

        # Find the most recent entry at or before the target time
        for reflog_entry in entries:
            if reflog_entry.timestamp <= target_time:
                return repo[reflog_entry.new_sha]

        # If no entry is old enough, raise an error
        if entries:
            oldest_time = entries[-1].timestamp
            raise ValueError(
                f"Reflog for {ref!r} has no entries at or before {spec!r}. "
                f"Oldest entry is at timestamp {oldest_time}"
            )
        else:
            raise ValueError(f"Reflog for {ref!r} is empty")

    # Handle ^{} - tag dereferencing
    if objectish.endswith(b"^{}"):
        obj = _resolve_object(repo, objectish[:-3])
        while isinstance(obj, Tag):
            _obj_type, obj_sha = obj.object
            obj = repo[obj_sha]
        return obj

    # Handle ~ and ^ operators
    for sep in [b"~", b"^"]:
        if sep in objectish:
            base, suffix = objectish.split(sep, 1)
            if not base:
                raise ValueError(f"Empty base before {sep!r}")

            obj = _resolve_object(repo, base)
            num, suffix = _parse_number_suffix(suffix)

            if sep == b"~":
                # Follow first parent N times
                commit = obj if isinstance(obj, Commit) else parse_commit(repo, obj.id)
                for _ in range(num):
                    if not commit.parents:
                        raise ValueError(
                            f"Commit {commit.id.decode('ascii', 'replace')} has no parents"
                        )
                    parent_obj = repo[commit.parents[0]]
                    assert isinstance(parent_obj, Commit)
                    commit = parent_obj
                obj = commit
            else:  # sep == b"^"
                # Get N-th parent (or commit itself if N=0)
                commit = obj if isinstance(obj, Commit) else parse_commit(repo, obj.id)
                if num == 0:
                    obj = commit
                elif num > len(commit.parents):
                    raise ValueError(
                        f"Commit {commit.id.decode('ascii', 'replace')} does not have parent #{num}"
                    )
                else:
                    obj = repo[commit.parents[num - 1]]

            # Process remaining operators recursively
            return parse_object(repo, obj.id + suffix) if suffix else obj

    # No operators, just return the object
    return _resolve_object(repo, objectish)


def parse_tree(repo: "BaseRepo", treeish: bytes | str | Tree | Commit | Tag) -> "Tree":
    """Parse a string referring to a tree.

    Args:
      repo: A repository object
      treeish: A string referring to a tree, or a Tree, Commit, or Tag object
    Returns: A Tree object
    Raises:
      KeyError: If the object can not be found
    """
    # If already a Tree, return it directly
    if isinstance(treeish, Tree):
        return treeish

    # If it's a Commit, return its tree
    if isinstance(treeish, Commit):
        tree = repo[treeish.tree]
        assert isinstance(tree, Tree)
        return tree

    # For Tag objects or strings, use the existing logic
    if isinstance(treeish, Tag):
        treeish = treeish.id
    else:
        treeish = to_bytes(treeish)
    treeish_typed: Ref | ObjectID
    try:
        treeish_typed = parse_ref(repo.refs, treeish)
    except KeyError:  # treeish is commit sha
        treeish_typed = ObjectID(treeish)
    try:
        o = repo[treeish_typed]
    except KeyError:
        # Try parsing as commit (handles short hashes)
        try:
            commit = parse_commit(repo, treeish)
            assert isinstance(commit, Commit)
            tree = repo[commit.tree]
            assert isinstance(tree, Tree)
            return tree
        except KeyError:
            raise KeyError(treeish)
    if isinstance(o, Commit):
        tree = repo[o.tree]
        assert isinstance(tree, Tree)
        return tree
    elif isinstance(o, Tag):
        # Tag handling - dereference and recurse
        _obj_type, obj_sha = o.object
        return parse_tree(repo, obj_sha)
    assert isinstance(o, Tree)
    return o


def parse_ref(container: "Repo | RefsContainer", refspec: str | bytes) -> "Ref":
    """Parse a string referring to a reference.

    Args:
      container: A RefsContainer object
      refspec: A string referring to a ref
    Returns: A ref
    Raises:
      KeyError: If the ref can not be found
    """
    refspec = to_bytes(refspec)
    possible_refs = [
        refspec,
        b"refs/" + refspec,
        local_tag_name(refspec),
        local_branch_name(refspec),
        b"refs/remotes/" + refspec,
        b"refs/remotes/" + refspec + b"/HEAD",
    ]
    for ref in possible_refs:
        if ref in container:
            return Ref(ref)
    raise KeyError(refspec)


def parse_reftuple(
    lh_container: "Repo | RefsContainer",
    rh_container: "Repo | RefsContainer",
    refspec: str | bytes,
    force: bool = False,
) -> tuple["Ref | None", "Ref | None", bool]:
    """Parse a reftuple spec.

    Args:
      lh_container: A RefsContainer object
      rh_container: A RefsContainer object
      refspec: A string
      force: Whether to force the operation
    Returns: A tuple with left and right ref
    Raises:
      KeyError: If one of the refs can not be found
    """
    refspec = to_bytes(refspec)
    if refspec.startswith(b"+"):
        force = True
        refspec = refspec[1:]
    lh_bytes: bytes | None
    rh_bytes: bytes | None
    if b":" in refspec:
        (lh_bytes, rh_bytes) = refspec.split(b":")
    else:
        lh_bytes = rh_bytes = refspec

    lh: Ref | None
    if lh_bytes == b"":
        lh = None
    else:
        lh = parse_ref(lh_container, lh_bytes)

    rh: Ref | None
    if rh_bytes == b"":
        rh = None
    else:
        try:
            rh = parse_ref(rh_container, rh_bytes)
        except KeyError:
            # TODO: check force?
            if b"/" not in rh_bytes:
                rh = Ref(local_branch_name(rh_bytes))
            else:
                rh = Ref(rh_bytes)
    return (lh, rh, force)


def parse_reftuples(
    lh_container: "Repo | RefsContainer",
    rh_container: "Repo | RefsContainer",
    refspecs: bytes | Sequence[bytes],
    force: bool = False,
) -> list[tuple["Ref | None", "Ref | None", bool]]:
    """Parse a list of reftuple specs to a list of reftuples.

    Args:
      lh_container: A RefsContainer object
      rh_container: A RefsContainer object
      refspecs: A list of refspecs or a string
      force: Force overwriting for all reftuples
    Returns: A list of refs
    Raises:
      KeyError: If one of the refs can not be found
    """
    if isinstance(refspecs, bytes):
        refspecs = [refspecs]
    ret = []
    # TODO: Support * in refspecs
    for refspec in refspecs:
        ret.append(parse_reftuple(lh_container, rh_container, refspec, force=force))
    return ret


def parse_refs(
    container: "Repo | RefsContainer",
    refspecs: bytes | str | Sequence[bytes | str],
) -> list["Ref"]:
    """Parse a list of refspecs to a list of refs.

    Args:
      container: A RefsContainer object
      refspecs: A list of refspecs or a string
    Returns: A list of refs
    Raises:
      KeyError: If one of the refs can not be found
    """
    # TODO: Support * in refspecs
    if isinstance(refspecs, (bytes, str)):
        refspecs = [refspecs]
    ret = []
    for refspec in refspecs:
        ret.append(parse_ref(container, refspec))
    return ret


def parse_commit_range(
    repo: "Repo", committish: str | bytes
) -> tuple["Commit", "Commit"] | None:
    """Parse a string referring to a commit range.

    Args:
      repo: A `Repo` object
      committish: A string referring to a commit or range (e.g., "HEAD~3..HEAD")

    Returns:
      None if committish is a single commit reference
      A tuple of (start_commit, end_commit) if it's a range
    Raises:
      KeyError: When the commits can not be found
      ValueError: If the range can not be parsed
    """
    committish = to_bytes(committish)
    if b".." not in committish:
        return None

    parts = committish.split(b"..", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid commit range: {committish.decode('utf-8')}")

    start_ref = parts[0]
    end_ref = parts[1] if parts[1] else b"HEAD"

    start_commit = parse_commit(repo, start_ref)
    end_commit = parse_commit(repo, end_ref)

    return (start_commit, end_commit)


class AmbiguousShortId(Exception):
    """The short id is ambiguous."""

    def __init__(self, prefix: bytes, options: list[ShaFile]) -> None:
        """Initialize AmbiguousShortId.

        Args:
          prefix: The ambiguous prefix
          options: List of matching objects
        """
        self.prefix = prefix
        self.options = options


def scan_for_short_id(
    object_store: "BaseObjectStore", prefix: bytes, tp: type[ShaFile]
) -> ShaFile:
    """Scan an object store for a short id."""
    ret = []
    for object_id in object_store.iter_prefix(prefix):
        o = object_store[object_id]
        if isinstance(o, tp):
            ret.append(o)
    if not ret:
        raise KeyError(prefix)
    if len(ret) == 1:
        return ret[0]
    raise AmbiguousShortId(prefix, ret)


def parse_commit(repo: "BaseRepo", committish: str | bytes | Commit | Tag) -> "Commit":
    """Parse a string referring to a single commit.

    Args:
      repo: A repository object
      committish: A string referring to a single commit, or a Commit or Tag object.
    Returns: A Commit object
    Raises:
      KeyError: When the reference commits can not be found
      ValueError: If the range can not be parsed
    """

    def dereference_tag(obj: ShaFile) -> "Commit":
        """Follow tag references until we reach a non-tag object."""
        while isinstance(obj, Tag):
            _obj_type, obj_sha = obj.object
            try:
                obj = repo.object_store[obj_sha]
            except KeyError:
                # Tag points to a missing object
                raise KeyError(obj_sha)
        if not isinstance(obj, Commit):
            raise ValueError(
                f"Expected commit, got {obj.type_name.decode('ascii', 'replace')}"
            )
        return obj

    # If already a Commit object, return it directly
    if isinstance(committish, Commit):
        return committish

    # If it's a Tag object, dereference it
    if isinstance(committish, Tag):
        return dereference_tag(committish)

    committish = to_bytes(committish)
    try:
        obj = repo[committish]
    except KeyError:
        pass
    else:
        return dereference_tag(obj)
    try:
        obj = repo[parse_ref(repo.refs, committish)]
    except KeyError:
        pass
    else:
        return dereference_tag(obj)
    if len(committish) >= 4 and len(committish) < 40:
        try:
            int(committish, 16)
        except ValueError:
            pass
        else:
            try:
                obj = scan_for_short_id(repo.object_store, committish, Commit)
            except KeyError:
                pass
            else:
                return dereference_tag(obj)
    raise KeyError(committish)


# TODO: parse_path_in_tree(), which handles e.g. v1.0:Documentation
