"""Git merge implementation."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import merge3
    from merge3 import SequenceMatcherProtocol
else:
    try:
        import merge3
    except ImportError:
        merge3 = None  # type: ignore[assignment]

from dulwich.attrs import GitAttributes
from dulwich.config import Config
from dulwich.merge_drivers import get_merge_driver_registry
from dulwich.object_store import BaseObjectStore
from dulwich.objects import S_ISGITLINK, Blob, Commit, Tree, is_blob, is_tree


def make_merge3(
    base: Sequence[bytes],
    a: Sequence[bytes],
    b: Sequence[bytes],
    is_cherrypick: bool = False,
    sequence_matcher: Optional[type["SequenceMatcherProtocol[bytes]"]] = None,
) -> "merge3.Merge3[bytes]":
    """Return a Merge3 object, or raise ImportError if merge3 is not installed."""
    if merge3 is None:
        raise ImportError(
            "merge3 module is required for three-way merging. "
            "Install it with: pip install merge3"
        )
    return merge3.Merge3(base, a, b, is_cherrypick, sequence_matcher)


class MergeConflict(Exception):
    """Raised when a merge conflict occurs."""

    def __init__(self, path: bytes, message: str) -> None:
        """Initialize MergeConflict.

        Args:
          path: Path to the conflicted file
          message: Conflict description
        """
        self.path = path
        super().__init__(f"Merge conflict in {path!r}: {message}")


def _can_merge_lines(
    base_lines: Sequence[bytes], a_lines: Sequence[bytes], b_lines: Sequence[bytes]
) -> bool:
    """Check if lines can be merged without conflict."""
    # If one side is unchanged, we can take the other side
    if base_lines == a_lines:
        return True
    elif base_lines == b_lines:
        return True
    else:
        # For now, treat any difference as a conflict
        # A more sophisticated algorithm would check for non-overlapping changes
        return False


if merge3 is not None:

    def _merge3_to_bytes(m: "merge3.Merge3[bytes]") -> bytes:
        """Convert merge3 result to bytes with conflict markers.

        Args:
            m: Merge3 object

        Returns:
            Merged content as bytes
        """
        result: list[bytes] = []
        for group in m.merge_groups():  # type: ignore[no-untyped-call,unused-ignore]
            if group[0] == "unchanged":
                result.extend(group[1])
            elif group[0] == "a":
                result.extend(group[1])
            elif group[0] == "b":
                result.extend(group[1])
            elif group[0] == "same":
                result.extend(group[1])
            elif group[0] == "conflict":
                # Check if this is a real conflict or just different changes
                base_lines, a_lines, b_lines = group[1], group[2], group[3]

                # Try to merge line by line
                if _can_merge_lines(base_lines, a_lines, b_lines):
                    merged_lines = _merge_lines(base_lines, a_lines, b_lines)
                    result.extend(merged_lines)
                else:
                    # Real conflict - add conflict markers
                    result.append(b"<<<<<<< ours\n")
                    result.extend(a_lines)
                    result.append(b"=======\n")
                    result.extend(b_lines)
                    result.append(b">>>>>>> theirs\n")

        return b"".join(result)


def _merge_lines(
    base_lines: Sequence[bytes], a_lines: Sequence[bytes], b_lines: Sequence[bytes]
) -> Sequence[bytes]:
    """Merge lines when possible."""
    if base_lines == a_lines:
        return b_lines
    elif base_lines == b_lines:
        return a_lines
    else:
        # This shouldn't happen if _can_merge_lines returned True
        return a_lines


def merge_blobs(
    base_blob: Optional[Blob],
    ours_blob: Optional[Blob],
    theirs_blob: Optional[Blob],
    path: Optional[bytes] = None,
    gitattributes: Optional[GitAttributes] = None,
    config: Optional[Config] = None,
) -> tuple[bytes, bool]:
    """Perform three-way merge on blob contents.

    Args:
        base_blob: Common ancestor blob (can be None)
        ours_blob: Our version of the blob (can be None)
        theirs_blob: Their version of the blob (can be None)
        path: Optional path of the file being merged
        gitattributes: Optional GitAttributes object for checking merge drivers
        config: Optional Config object for loading merge driver configuration

    Returns:
        Tuple of (merged_content, had_conflicts)
    """
    # Check for merge driver
    merge_driver_name = None
    if path and gitattributes:
        attrs = gitattributes.match_path(path)
        merge_value = attrs.get(b"merge")
        if merge_value and isinstance(merge_value, bytes) and merge_value != b"text":
            merge_driver_name = merge_value.decode("utf-8", errors="replace")

    # Use merge driver if found
    if merge_driver_name:
        registry = get_merge_driver_registry(config)
        driver = registry.get_driver(merge_driver_name)
        if driver:
            # Get content from blobs
            base_content = base_blob.data if base_blob else b""
            ours_content = ours_blob.data if ours_blob else b""
            theirs_content = theirs_blob.data if theirs_blob else b""

            # Use merge driver
            merged_content, success = driver.merge(
                ancestor=base_content,
                ours=ours_content,
                theirs=theirs_content,
                path=path.decode("utf-8", errors="replace") if path else None,
                marker_size=7,
            )
            # Convert success (no conflicts) to had_conflicts (conflicts occurred)
            had_conflicts = not success
            return merged_content, had_conflicts

    # Fall back to default merge behavior
    # Handle deletion cases
    if ours_blob is None and theirs_blob is None:
        return b"", False

    if base_blob is None:
        # No common ancestor
        if ours_blob is None:
            assert theirs_blob is not None
            return theirs_blob.data, False
        elif theirs_blob is None:
            return ours_blob.data, False
        elif ours_blob.data == theirs_blob.data:
            return ours_blob.data, False
        else:
            # Both added different content - conflict
            m = make_merge3(
                [],
                ours_blob.data.splitlines(True),
                theirs_blob.data.splitlines(True),
            )
            return _merge3_to_bytes(m), True

    # Get content for each version
    base_content = base_blob.data if base_blob else b""
    ours_content = ours_blob.data if ours_blob else b""
    theirs_content = theirs_blob.data if theirs_blob else b""

    # Check if either side deleted
    if ours_blob is None or theirs_blob is None:
        if ours_blob is None and theirs_blob is None:
            return b"", False
        elif ours_blob is None:
            # We deleted, check if they modified
            if base_content == theirs_content:
                return b"", False  # They didn't modify, accept deletion
            else:
                # Conflict: we deleted, they modified
                m = make_merge3(
                    base_content.splitlines(True),
                    [],
                    theirs_content.splitlines(True),
                )
                return _merge3_to_bytes(m), True
        else:
            # They deleted, check if we modified
            if base_content == ours_content:
                return b"", False  # We didn't modify, accept deletion
            else:
                # Conflict: they deleted, we modified
                m = make_merge3(
                    base_content.splitlines(True),
                    ours_content.splitlines(True),
                    [],
                )
                return _merge3_to_bytes(m), True

    # Both sides exist, check if merge is needed
    if ours_content == theirs_content:
        return ours_content, False
    elif base_content == ours_content:
        return theirs_content, False
    elif base_content == theirs_content:
        return ours_content, False

    # Perform three-way merge
    m = make_merge3(
        base_content.splitlines(True),
        ours_content.splitlines(True),
        theirs_content.splitlines(True),
    )

    # Check for conflicts and generate merged content
    merged_content = _merge3_to_bytes(m)
    has_conflicts = b"<<<<<<< ours" in merged_content

    return merged_content, has_conflicts


class Merger:
    """Handles git merge operations."""

    def __init__(
        self,
        object_store: BaseObjectStore,
        gitattributes: Optional[GitAttributes] = None,
        config: Optional[Config] = None,
    ) -> None:
        """Initialize merger.

        Args:
            object_store: Object store to read objects from
            gitattributes: Optional GitAttributes object for checking merge drivers
            config: Optional Config object for loading merge driver configuration
        """
        self.object_store = object_store
        self.gitattributes = gitattributes
        self.config = config

    def merge_blobs(
        self,
        base_blob: Optional[Blob],
        ours_blob: Optional[Blob],
        theirs_blob: Optional[Blob],
        path: Optional[bytes] = None,
    ) -> tuple[bytes, bool]:
        """Perform three-way merge on blob contents.

        Args:
            base_blob: Common ancestor blob (can be None)
            ours_blob: Our version of the blob (can be None)
            theirs_blob: Their version of the blob (can be None)
            path: Optional path of the file being merged

        Returns:
            Tuple of (merged_content, had_conflicts)
        """
        return merge_blobs(
            base_blob, ours_blob, theirs_blob, path, self.gitattributes, self.config
        )

    def merge_trees(
        self, base_tree: Optional[Tree], ours_tree: Tree, theirs_tree: Tree
    ) -> tuple[Tree, list[bytes]]:
        """Perform three-way merge on trees.

        Args:
            base_tree: Common ancestor tree (can be None for no common ancestor)
            ours_tree: Our version of the tree
            theirs_tree: Their version of the tree

        Returns:
            tuple of (merged_tree, list_of_conflicted_paths)
        """
        conflicts: list[bytes] = []
        merged_entries: dict[bytes, tuple[Optional[int], Optional[bytes]]] = {}

        # Get all paths from all trees
        all_paths = set()

        if base_tree:
            for entry in base_tree.items():
                assert entry.path is not None
                all_paths.add(entry.path)

        for entry in ours_tree.items():
            assert entry.path is not None
            all_paths.add(entry.path)

        for entry in theirs_tree.items():
            assert entry.path is not None
            all_paths.add(entry.path)

        # Process each path
        for path in sorted(all_paths):
            base_entry = None
            if base_tree:
                try:
                    base_entry = base_tree.lookup_path(
                        self.object_store.__getitem__, path
                    )
                except KeyError:
                    pass

            try:
                ours_entry = ours_tree.lookup_path(self.object_store.__getitem__, path)
            except KeyError:
                ours_entry = None

            try:
                theirs_entry = theirs_tree.lookup_path(
                    self.object_store.__getitem__, path
                )
            except KeyError:
                theirs_entry = None

            # Extract mode and sha
            _base_mode, base_sha = base_entry if base_entry else (None, None)
            ours_mode, ours_sha = ours_entry if ours_entry else (None, None)
            theirs_mode, theirs_sha = theirs_entry if theirs_entry else (None, None)

            # Handle deletions
            if ours_sha is None and theirs_sha is None:
                continue  # Deleted in both

            # Handle additions
            if base_sha is None:
                if ours_sha == theirs_sha and ours_mode == theirs_mode:
                    # Same addition in both
                    merged_entries[path] = (ours_mode, ours_sha)
                elif ours_sha is None:
                    # Added only in theirs
                    merged_entries[path] = (theirs_mode, theirs_sha)
                elif theirs_sha is None:
                    # Added only in ours
                    merged_entries[path] = (ours_mode, ours_sha)
                else:
                    # Different additions - conflict
                    conflicts.append(path)
                    # For now, keep ours
                    merged_entries[path] = (ours_mode, ours_sha)
                continue

            # Check for mode conflicts
            if (
                ours_mode != theirs_mode
                and ours_mode is not None
                and theirs_mode is not None
            ):
                conflicts.append(path)
                # For now, keep ours
                merged_entries[path] = (ours_mode, ours_sha)
                continue

            # Handle modifications
            if ours_sha == theirs_sha:
                # Same modification or no change
                if ours_sha is not None:
                    merged_entries[path] = (ours_mode, ours_sha)
            elif base_sha == ours_sha and theirs_sha is not None:
                # Only theirs modified
                merged_entries[path] = (theirs_mode, theirs_sha)
            elif base_sha == theirs_sha and ours_sha is not None:
                # Only ours modified
                merged_entries[path] = (ours_mode, ours_sha)
            elif ours_sha is None:
                # We deleted
                if base_sha == theirs_sha:
                    # They didn't modify, accept deletion
                    pass
                else:
                    # They modified, we deleted - conflict
                    conflicts.append(path)
            elif theirs_sha is None:
                # They deleted
                if base_sha == ours_sha:
                    # We didn't modify, accept deletion
                    pass
                else:
                    # We modified, they deleted - conflict
                    conflicts.append(path)
                    merged_entries[path] = (ours_mode, ours_sha)
            else:
                # Both modified differently
                # For trees and submodules, this is a conflict
                if S_ISGITLINK(ours_mode or 0) or S_ISGITLINK(theirs_mode or 0):
                    conflicts.append(path)
                    merged_entries[path] = (ours_mode, ours_sha)
                elif (ours_mode or 0) & 0o170000 == 0o040000 or (
                    theirs_mode or 0
                ) & 0o170000 == 0o040000:
                    # Tree conflict
                    conflicts.append(path)
                    merged_entries[path] = (ours_mode, ours_sha)
                else:
                    # Try to merge blobs
                    base_blob = None
                    if base_sha:
                        base_obj = self.object_store[base_sha]
                        if is_blob(base_obj):
                            base_blob = base_obj
                        else:
                            raise TypeError(
                                f"Expected blob for {path!r}, got {base_obj.type_name.decode()}"
                            )

                    ours_blob = None
                    if ours_sha:
                        ours_obj = self.object_store[ours_sha]
                        if is_blob(ours_obj):
                            ours_blob = ours_obj
                        else:
                            raise TypeError(
                                f"Expected blob for {path!r}, got {ours_obj.type_name.decode()}"
                            )

                    theirs_blob = None
                    if theirs_sha:
                        theirs_obj = self.object_store[theirs_sha]
                        if is_blob(theirs_obj):
                            theirs_blob = theirs_obj
                        else:
                            raise TypeError(
                                f"Expected blob for {path!r}, got {theirs_obj.type_name.decode()}"
                            )

                    assert isinstance(base_blob, Blob)
                    assert isinstance(ours_blob, Blob)
                    assert isinstance(theirs_blob, Blob)
                    merged_content, had_conflict = self.merge_blobs(
                        base_blob, ours_blob, theirs_blob, path
                    )

                    if had_conflict:
                        conflicts.append(path)

                    # Store merged blob
                    merged_blob = Blob.from_string(merged_content)
                    self.object_store.add_object(merged_blob)
                    merged_entries[path] = (ours_mode or theirs_mode, merged_blob.id)

        # Build merged tree
        merged_tree = Tree()
        for path, (mode, sha) in sorted(merged_entries.items()):
            if mode is not None and sha is not None:
                merged_tree.add(path, mode, sha)

        return merged_tree, conflicts


def _create_virtual_commit(
    object_store: BaseObjectStore,
    tree: Tree,
    parents: list[bytes],
    message: bytes = b"Virtual merge base",
) -> Commit:
    """Create a virtual commit object for recursive merging.

    Args:
        object_store: Object store to add the commit to
        tree: Tree object for the commit
        parents: List of parent commit IDs
        message: Commit message

    Returns:
        The created Commit object
    """
    # Add the tree to the object store
    object_store.add_object(tree)

    # Create a virtual commit
    commit = Commit()
    commit.tree = tree.id
    commit.parents = parents
    commit.author = b"Dulwich Recursive Merge <dulwich@example.com>"
    commit.committer = commit.author
    commit.commit_time = 0
    commit.author_time = 0
    commit.commit_timezone = 0
    commit.author_timezone = 0
    commit.encoding = b"UTF-8"
    commit.message = message

    # Add the commit to the object store
    object_store.add_object(commit)

    return commit


def recursive_merge(
    object_store: BaseObjectStore,
    merge_bases: list[bytes],
    ours_commit: Commit,
    theirs_commit: Commit,
    gitattributes: Optional[GitAttributes] = None,
    config: Optional[Config] = None,
) -> tuple[Tree, list[bytes]]:
    """Perform a recursive merge with multiple merge bases.

    This implements Git's recursive merge strategy, which handles cases where
    there are multiple common ancestors (criss-cross merges). The algorithm:

    1. If there's 0 or 1 merge base, perform a simple three-way merge
    2. If there are multiple merge bases, merge them recursively to create
       a virtual merge base, then use that for the final three-way merge

    Args:
        object_store: Object store to read/write objects
        merge_bases: List of merge base commit IDs
        ours_commit: Our commit
        theirs_commit: Their commit
        gitattributes: Optional GitAttributes object for checking merge drivers
        config: Optional Config object for loading merge driver configuration

    Returns:
        tuple of (merged_tree, list_of_conflicted_paths)
    """
    if not merge_bases:
        # No common ancestor - use None as base
        return three_way_merge(
            object_store, None, ours_commit, theirs_commit, gitattributes, config
        )
    elif len(merge_bases) == 1:
        # Single merge base - simple three-way merge
        base_commit_obj = object_store[merge_bases[0]]
        if not isinstance(base_commit_obj, Commit):
            raise TypeError(
                f"Expected commit, got {base_commit_obj.type_name.decode()}"
            )
        return three_way_merge(
            object_store,
            base_commit_obj,
            ours_commit,
            theirs_commit,
            gitattributes,
            config,
        )
    else:
        # Multiple merge bases - need to create a virtual merge base
        # Start by merging the first two bases
        virtual_base_id = merge_bases[0]
        virtual_commit_obj = object_store[virtual_base_id]
        if not isinstance(virtual_commit_obj, Commit):
            raise TypeError(
                f"Expected commit, got {virtual_commit_obj.type_name.decode()}"
            )

        # Recursively merge each additional base
        for next_base_id in merge_bases[1:]:
            next_base_obj = object_store[next_base_id]
            if not isinstance(next_base_obj, Commit):
                raise TypeError(
                    f"Expected commit, got {next_base_obj.type_name.decode()}"
                )

            # Find merge base of these two bases
            # Import here to avoid circular dependency

            # We need access to the repo for find_merge_base
            # For now, we'll perform a simple three-way merge without recursion
            # between the two virtual commits
            # A proper implementation would require passing the repo object

            # Perform three-way merge of the two bases (using None as their base)
            merged_tree, _conflicts = three_way_merge(
                object_store,
                None,  # No common ancestor for virtual merge bases
                virtual_commit_obj,
                next_base_obj,
                gitattributes,
                config,
            )

            # Create a virtual commit with this merged tree
            virtual_commit_obj = _create_virtual_commit(
                object_store,
                merged_tree,
                [virtual_base_id, next_base_id],
            )
            virtual_base_id = virtual_commit_obj.id

        # Now use the virtual merge base for the final merge
        return three_way_merge(
            object_store,
            virtual_commit_obj,
            ours_commit,
            theirs_commit,
            gitattributes,
            config,
        )


def three_way_merge(
    object_store: BaseObjectStore,
    base_commit: Optional[Commit],
    ours_commit: Commit,
    theirs_commit: Commit,
    gitattributes: Optional[GitAttributes] = None,
    config: Optional[Config] = None,
) -> tuple[Tree, list[bytes]]:
    """Perform a three-way merge between commits.

    Args:
        object_store: Object store to read/write objects
        base_commit: Common ancestor commit (None if no common ancestor)
        ours_commit: Our commit
        theirs_commit: Their commit
        gitattributes: Optional GitAttributes object for checking merge drivers
        config: Optional Config object for loading merge driver configuration

    Returns:
        tuple of (merged_tree, list_of_conflicted_paths)
    """
    merger = Merger(object_store, gitattributes, config)

    base_tree = None
    if base_commit:
        base_obj = object_store[base_commit.tree]
        if is_tree(base_obj):
            base_tree = base_obj
        else:
            raise TypeError(f"Expected tree, got {base_obj.type_name.decode()}")

    ours_obj = object_store[ours_commit.tree]
    if is_tree(ours_obj):
        ours_tree = ours_obj
    else:
        raise TypeError(f"Expected tree, got {ours_obj.type_name.decode()}")

    theirs_obj = object_store[theirs_commit.tree]
    if is_tree(theirs_obj):
        theirs_tree = theirs_obj
    else:
        raise TypeError(f"Expected tree, got {theirs_obj.type_name.decode()}")

    assert base_tree is None or isinstance(base_tree, Tree)
    assert isinstance(ours_tree, Tree)
    assert isinstance(theirs_tree, Tree)
    return merger.merge_trees(base_tree, ours_tree, theirs_tree)


def octopus_merge(
    object_store: BaseObjectStore,
    merge_bases: list[bytes],
    head_commit: Commit,
    other_commits: list[Commit],
    gitattributes: Optional[GitAttributes] = None,
    config: Optional[Config] = None,
) -> tuple[Tree, list[bytes]]:
    """Perform an octopus merge of multiple commits.

    The octopus merge strategy merges multiple branches sequentially into a single
    commit with multiple parents. It refuses to proceed if any merge would result
    in conflicts that require manual resolution.

    Args:
        object_store: Object store to read/write objects
        merge_bases: List of common ancestor commit IDs for all commits
        head_commit: Current HEAD commit (ours)
        other_commits: List of commits to merge (theirs)
        gitattributes: Optional GitAttributes object for checking merge drivers
        config: Optional Config object for loading merge driver configuration

    Returns:
        tuple of (merged_tree, list_of_conflicted_paths)
        If any conflicts occur during the sequential merges, the function returns
        early with the conflicts list populated.

    Raises:
        TypeError: If any object is not of the expected type
    """
    if not other_commits:
        raise ValueError("octopus_merge requires at least one commit to merge")

    # Start with the head commit's tree as our current state
    current_commit = head_commit

    # Merge each commit sequentially
    for i, other_commit in enumerate(other_commits):
        # Find the merge base between current state and the commit we're merging
        # For octopus merges, we use the octopus base for all commits
        if merge_bases:
            base_commit_id = merge_bases[0]
            base_commit = object_store[base_commit_id]
            if not isinstance(base_commit, Commit):
                raise TypeError(f"Expected Commit, got {type(base_commit)}")
        else:
            base_commit = None

        # Perform three-way merge
        merged_tree, conflicts = three_way_merge(
            object_store,
            base_commit,
            current_commit,
            other_commit,
            gitattributes,
            config,
        )

        # Octopus merge refuses to proceed if there are conflicts
        if conflicts:
            return merged_tree, conflicts

        # Add merged tree to object store
        object_store.add_object(merged_tree)

        # Create a temporary commit object with the merged tree for the next iteration
        # This allows us to continue merging additional commits
        if i < len(other_commits) - 1:
            temp_commit = Commit()
            temp_commit.tree = merged_tree.id
            # For intermediate merges, we use the same parent as current
            temp_commit.parents = (
                current_commit.parents
                if current_commit.parents
                else [current_commit.id]
            )
            # Set minimal required commit fields
            temp_commit.author = current_commit.author
            temp_commit.committer = current_commit.committer
            temp_commit.author_time = current_commit.author_time
            temp_commit.commit_time = current_commit.commit_time
            temp_commit.author_timezone = current_commit.author_timezone
            temp_commit.commit_timezone = current_commit.commit_timezone
            temp_commit.message = b"Temporary octopus merge commit"
            object_store.add_object(temp_commit)
            current_commit = temp_commit

    return merged_tree, []
