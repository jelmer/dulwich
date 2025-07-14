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

from typing import TYPE_CHECKING, Optional, Union

from .objects import Commit, ShaFile, Tag, Tree

if TYPE_CHECKING:
    from .refs import Ref, RefsContainer
    from .repo import Repo


def to_bytes(text: Union[str, bytes]) -> bytes:
    if getattr(text, "encode", None) is not None:
        text = text.encode("ascii")  # type: ignore
    return text  # type: ignore


def _resolve_object(repo: "Repo", ref: bytes) -> "ShaFile":
    """Resolve a reference to an object using multiple strategies."""
    try:
        return repo[ref]
    except KeyError:
        try:
            ref_sha = parse_ref(repo, ref)
            return repo[ref_sha]
        except KeyError:
            try:
                return repo.object_store[ref]
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


def parse_object(repo: "Repo", objectish: Union[bytes, str]) -> "ShaFile":
    """Parse a string referring to an object.

    Args:
      repo: A `Repo` object
      objectish: A string referring to an object
    Returns: A git object
    Raises:
      KeyError: If the object can not be found
    """
    objectish = to_bytes(objectish)

    # Handle :<path> - lookup path in tree
    if b":" in objectish:
        rev, path = objectish.split(b":", 1)
        if not rev:
            raise NotImplementedError("Index path lookup (:path) not yet supported")
        tree = parse_tree(repo, rev)
        mode, sha = tree.lookup_path(repo.object_store.__getitem__, path)
        return repo[sha]

    # Handle @{N} - reflog lookup
    if b"@{" in objectish:
        base, rest = objectish.split(b"@{", 1)
        if not rest.endswith(b"}"):
            raise ValueError("Invalid @{} syntax")
        spec = rest[:-1]
        if not spec.isdigit():
            raise NotImplementedError(f"Only @{{N}} supported, not @{{{spec!r}}}")

        ref = base if base else b"HEAD"
        entries = list(repo.read_reflog(ref))
        entries.reverse()  # Git uses reverse chronological order
        index = int(spec)
        if index >= len(entries):
            raise ValueError(f"Reflog for {ref!r} has only {len(entries)} entries")
        return repo[entries[index].new_sha]

    # Handle ^{} - tag dereferencing
    if objectish.endswith(b"^{}"):
        obj = _resolve_object(repo, objectish[:-3])
        while isinstance(obj, Tag):
            obj_type, obj_sha = obj.object
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
                    commit = repo[commit.parents[0]]
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


def parse_tree(repo: "Repo", treeish: Union[bytes, str, Tree, Commit, Tag]) -> "Tree":
    """Parse a string referring to a tree.

    Args:
      repo: A `Repo` object
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
        return repo[treeish.tree]

    # For Tag objects or strings, use the existing logic
    if isinstance(treeish, Tag):
        treeish = treeish.id
    else:
        treeish = to_bytes(treeish)
    try:
        treeish = parse_ref(repo, treeish)
    except KeyError:  # treeish is commit sha
        pass
    try:
        o = repo[treeish]
    except KeyError:
        # Try parsing as commit (handles short hashes)
        try:
            commit = parse_commit(repo, treeish)
            return repo[commit.tree]
        except KeyError:
            raise KeyError(treeish)
    if o.type_name == b"commit":
        return repo[o.tree]
    elif o.type_name == b"tag":
        # Tag handling - dereference and recurse
        obj_type, obj_sha = o.object
        return parse_tree(repo, obj_sha)
    return o


def parse_ref(
    container: Union["Repo", "RefsContainer"], refspec: Union[str, bytes]
) -> "Ref":
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
        b"refs/tags/" + refspec,
        b"refs/heads/" + refspec,
        b"refs/remotes/" + refspec,
        b"refs/remotes/" + refspec + b"/HEAD",
    ]
    for ref in possible_refs:
        if ref in container:
            return ref
    raise KeyError(refspec)


def parse_reftuple(
    lh_container: Union["Repo", "RefsContainer"],
    rh_container: Union["Repo", "RefsContainer"],
    refspec: Union[str, bytes],
    force: bool = False,
) -> tuple[Optional["Ref"], Optional["Ref"], bool]:
    """Parse a reftuple spec.

    Args:
      lh_container: A RefsContainer object
      rh_container: A RefsContainer object
      refspec: A string
    Returns: A tuple with left and right ref
    Raises:
      KeyError: If one of the refs can not be found
    """
    refspec = to_bytes(refspec)
    if refspec.startswith(b"+"):
        force = True
        refspec = refspec[1:]
    lh: Optional[bytes]
    rh: Optional[bytes]
    if b":" in refspec:
        (lh, rh) = refspec.split(b":")
    else:
        lh = rh = refspec
    if lh == b"":
        lh = None
    else:
        lh = parse_ref(lh_container, lh)
    if rh == b"":
        rh = None
    else:
        try:
            rh = parse_ref(rh_container, rh)
        except KeyError:
            # TODO: check force?
            if b"/" not in rh:
                rh = b"refs/heads/" + rh
    return (lh, rh, force)


def parse_reftuples(
    lh_container: Union["Repo", "RefsContainer"],
    rh_container: Union["Repo", "RefsContainer"],
    refspecs: Union[bytes, list[bytes]],
    force: bool = False,
):
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
    if not isinstance(refspecs, list):
        refspecs = [refspecs]
    ret = []
    # TODO: Support * in refspecs
    for refspec in refspecs:
        ret.append(parse_reftuple(lh_container, rh_container, refspec, force=force))
    return ret


def parse_refs(container, refspecs):
    """Parse a list of refspecs to a list of refs.

    Args:
      container: A RefsContainer object
      refspecs: A list of refspecs or a string
    Returns: A list of refs
    Raises:
      KeyError: If one of the refs can not be found
    """
    # TODO: Support * in refspecs
    if not isinstance(refspecs, list):
        refspecs = [refspecs]
    ret = []
    for refspec in refspecs:
        ret.append(parse_ref(container, refspec))
    return ret


def parse_commit_range(
    repo: "Repo", committish: Union[str, bytes]
) -> Optional[tuple["Commit", "Commit"]]:
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

    def __init__(self, prefix, options) -> None:
        self.prefix = prefix
        self.options = options


def scan_for_short_id(object_store, prefix, tp):
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


def parse_commit(repo: "Repo", committish: Union[str, bytes, Commit, Tag]) -> "Commit":
    """Parse a string referring to a single commit.

    Args:
      repo: A` Repo` object
      committish: A string referring to a single commit, or a Commit or Tag object.
    Returns: A Commit object
    Raises:
      KeyError: When the reference commits can not be found
      ValueError: If the range can not be parsed
    """

    def dereference_tag(obj):
        """Follow tag references until we reach a non-tag object."""
        while isinstance(obj, Tag):
            obj_type, obj_sha = obj.object
            try:
                obj = repo.object_store[obj_sha]
            except KeyError:
                # Tag points to a missing object
                raise KeyError(obj_sha)
        if not isinstance(obj, Commit):
            raise ValueError(f"Expected commit, got {obj.type_name}")
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
        obj = repo[parse_ref(repo, committish)]
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
