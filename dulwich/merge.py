"""Git merge implementation."""

from typing import Optional

try:
    import merge3
except ImportError:
    merge3 = None  # type: ignore

from dulwich.object_store import BaseObjectStore
from dulwich.objects import S_ISGITLINK, Blob, Commit, Tree, is_blob, is_tree


class MergeConflict(Exception):
    """Raised when a merge conflict occurs."""

    def __init__(self, path: bytes, message: str) -> None:
        self.path = path
        super().__init__(f"Merge conflict in {path!r}: {message}")


def _can_merge_lines(
    base_lines: list[bytes], a_lines: list[bytes], b_lines: list[bytes]
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

    def _merge3_to_bytes(m: merge3.Merge3) -> bytes:
        """Convert merge3 result to bytes with conflict markers.

        Args:
            m: Merge3 object

        Returns:
            Merged content as bytes
        """
        result = []
        for group in m.merge_groups():
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
    base_lines: list[bytes], a_lines: list[bytes], b_lines: list[bytes]
) -> list[bytes]:
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
) -> tuple[bytes, bool]:
    """Perform three-way merge on blob contents.

    Args:
        base_blob: Common ancestor blob (can be None)
        ours_blob: Our version of the blob (can be None)
        theirs_blob: Their version of the blob (can be None)

    Returns:
        Tuple of (merged_content, had_conflicts)
    """
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
            m = merge3.Merge3(
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
                m = merge3.Merge3(
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
                m = merge3.Merge3(
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
    m = merge3.Merge3(
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

    def __init__(self, object_store: BaseObjectStore) -> None:
        """Initialize merger.

        Args:
            object_store: Object store to read objects from
        """
        self.object_store = object_store

    @staticmethod
    def merge_blobs(
        base_blob: Optional[Blob],
        ours_blob: Optional[Blob],
        theirs_blob: Optional[Blob],
    ) -> tuple[bytes, bool]:
        """Perform three-way merge on blob contents.

        Args:
            base_blob: Common ancestor blob (can be None)
            ours_blob: Our version of the blob (can be None)
            theirs_blob: Their version of the blob (can be None)

        Returns:
            Tuple of (merged_content, had_conflicts)
        """
        return merge_blobs(base_blob, ours_blob, theirs_blob)

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
        conflicts = []
        merged_entries = {}

        # Get all paths from all trees
        all_paths = set()

        if base_tree:
            for entry in base_tree.items():
                all_paths.add(entry.path)

        for entry in ours_tree.items():
            all_paths.add(entry.path)

        for entry in theirs_tree.items():
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
            base_mode, base_sha = base_entry if base_entry else (None, None)
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

                    merged_content, had_conflict = self.merge_blobs(
                        base_blob, ours_blob, theirs_blob
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


def three_way_merge(
    object_store: BaseObjectStore,
    base_commit: Optional[Commit],
    ours_commit: Commit,
    theirs_commit: Commit,
) -> tuple[Tree, list[bytes]]:
    """Perform a three-way merge between commits.

    Args:
        object_store: Object store to read/write objects
        base_commit: Common ancestor commit (None if no common ancestor)
        ours_commit: Our commit
        theirs_commit: Their commit

    Returns:
        tuple of (merged_tree, list_of_conflicted_paths)
    """
    merger = Merger(object_store)

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

    return merger.merge_trees(base_tree, ours_tree, theirs_tree)
