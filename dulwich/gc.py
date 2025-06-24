"""Git garbage collection implementation."""

import collections
import time
from dataclasses import dataclass, field
from typing import Optional

from dulwich.object_store import BaseObjectStore, PackBasedObjectStore
from dulwich.objects import Commit, ObjectID, Tag, Tree
from dulwich.refs import RefsContainer


@dataclass
class GCStats:
    """Statistics from garbage collection."""

    pruned_objects: set[bytes] = field(default_factory=set)
    bytes_freed: int = 0
    packs_before: int = 0
    packs_after: int = 0
    loose_objects_before: int = 0
    loose_objects_after: int = 0


def find_reachable_objects(
    object_store: BaseObjectStore,
    refs_container: RefsContainer,
    include_reflogs: bool = True,
    progress=None,
) -> set[bytes]:
    """Find all reachable objects in the repository.

    Args:
        object_store: Object store to search
        refs_container: Reference container
        include_reflogs: Whether to include reflog entries
        progress: Optional progress callback

    Returns:
        Set of reachable object SHAs
    """
    reachable = set()
    pending: collections.deque[ObjectID] = collections.deque()

    # Start with all refs
    for ref in refs_container.allkeys():
        try:
            sha = refs_container[ref]  # This follows symbolic refs
            if sha and sha not in reachable:
                pending.append(sha)
                reachable.add(sha)
        except KeyError:
            # Broken ref
            if progress:
                progress(f"Warning: Broken ref {ref.decode('utf-8', 'replace')}")
            continue

    # TODO: Add reflog support when reflog functionality is available

    # Walk all reachable objects
    while pending:
        sha = pending.popleft()

        if progress:
            progress(f"Checking object {sha.decode('ascii', 'replace')}")

        try:
            obj = object_store[sha]
        except KeyError:
            continue

        # Add referenced objects
        if isinstance(obj, Commit):
            # Tree
            if obj.tree not in reachable:
                pending.append(obj.tree)
                reachable.add(obj.tree)
            # Parents
            for parent in obj.parents:
                if parent not in reachable:
                    pending.append(parent)
                    reachable.add(parent)
        elif isinstance(obj, Tree):
            # Tree entries
            for entry in obj.items():
                if entry.sha not in reachable:
                    pending.append(entry.sha)
                    reachable.add(entry.sha)
        elif isinstance(obj, Tag):
            # Tagged object
            if obj.object[1] not in reachable:
                pending.append(obj.object[1])
                reachable.add(obj.object[1])

    return reachable


def find_unreachable_objects(
    object_store: BaseObjectStore,
    refs_container: RefsContainer,
    include_reflogs: bool = True,
    progress=None,
) -> set[bytes]:
    """Find all unreachable objects in the repository.

    Args:
        object_store: Object store to search
        refs_container: Reference container
        include_reflogs: Whether to include reflog entries
        progress: Optional progress callback

    Returns:
        Set of unreachable object SHAs
    """
    reachable = find_reachable_objects(
        object_store, refs_container, include_reflogs, progress
    )

    unreachable = set()
    for sha in object_store:
        if sha not in reachable:
            unreachable.add(sha)

    return unreachable


def prune_unreachable_objects(
    object_store: PackBasedObjectStore,
    refs_container: RefsContainer,
    grace_period: Optional[int] = None,
    dry_run: bool = False,
    progress=None,
) -> tuple[set[bytes], int]:
    """Remove unreachable objects from the repository.

    Args:
        object_store: Object store to prune
        refs_container: Reference container
        grace_period: Grace period in seconds (objects newer than this are kept)
        dry_run: If True, only report what would be deleted
        progress: Optional progress callback

    Returns:
        Tuple of (set of pruned object SHAs, total bytes freed)
    """
    unreachable = find_unreachable_objects(
        object_store, refs_container, progress=progress
    )

    pruned = set()
    bytes_freed = 0

    for sha in unreachable:
        try:
            obj = object_store[sha]

            # Check grace period
            if grace_period is not None:
                try:
                    mtime = object_store.get_object_mtime(sha)
                    age = time.time() - mtime
                    if age < grace_period:
                        if progress:
                            progress(
                                f"Keeping {sha.decode('ascii', 'replace')} (age: {age:.0f}s < grace period: {grace_period}s)"
                            )
                        continue
                except KeyError:
                    # Object not found, skip it
                    continue

            if progress:
                progress(f"Pruning {sha.decode('ascii', 'replace')}")

            # Calculate size before attempting deletion
            obj_size = len(obj.as_raw_string())

            if not dry_run:
                object_store.delete_loose_object(sha)

            # Only count as pruned if we get here (deletion succeeded or dry run)
            pruned.add(sha)
            bytes_freed += obj_size

        except KeyError:
            # Object already gone
            pass
        except OSError as e:
            # File system errors during deletion
            if progress:
                progress(f"Error pruning {sha.decode('ascii', 'replace')}: {e}")
    return pruned, bytes_freed


def garbage_collect(
    repo,
    auto: bool = False,
    aggressive: bool = False,
    prune: bool = True,
    grace_period: Optional[int] = 1209600,  # 2 weeks default
    dry_run: bool = False,
    progress=None,
) -> GCStats:
    """Run garbage collection on a repository.

    Args:
        repo: Repository to garbage collect
        auto: Whether this is an automatic gc
        aggressive: Whether to use aggressive settings
        prune: Whether to prune unreachable objects
        grace_period: Grace period for pruning in seconds
        dry_run: If True, only report what would be done
        progress: Optional progress callback

    Returns:
        GCStats object with garbage collection statistics
    """
    stats = GCStats()

    object_store = repo.object_store
    refs_container = repo.refs

    # Count initial state
    stats.packs_before = len(list(object_store.packs))
    # TODO: Count loose objects when we have a method for it

    # Find unreachable objects to exclude from repacking
    unreachable_to_prune = set()
    if prune:
        if progress:
            progress("Finding unreachable objects")
        unreachable = find_unreachable_objects(
            object_store, refs_container, progress=progress
        )

        # Apply grace period check
        for sha in unreachable:
            try:
                if grace_period is not None:
                    try:
                        mtime = object_store.get_object_mtime(sha)
                        age = time.time() - mtime
                        if age < grace_period:
                            if progress:
                                progress(
                                    f"Keeping {sha.decode('ascii', 'replace')} (age: {age:.0f}s < grace period: {grace_period}s)"
                                )
                            continue
                    except KeyError:
                        # Object not found, skip it
                        continue

                unreachable_to_prune.add(sha)
                obj = object_store[sha]
                stats.bytes_freed += len(obj.as_raw_string())
            except KeyError:
                pass

        stats.pruned_objects = unreachable_to_prune

    # Pack refs
    if progress:
        progress("Packing references")
    if not dry_run:
        repo.refs.pack_refs()

    # Delete loose unreachable objects
    if prune and not dry_run:
        for sha in unreachable_to_prune:
            if object_store.contains_loose(sha):
                try:
                    object_store.delete_loose_object(sha)
                except OSError:
                    pass

    # Repack everything, excluding unreachable objects
    # This handles both loose object packing and pack consolidation
    if progress:
        progress("Repacking repository")
    if not dry_run:
        if prune and unreachable_to_prune:
            # Repack excluding unreachable objects
            object_store.repack(exclude=unreachable_to_prune)
        else:
            # Normal repack
            object_store.repack()

    # Prune orphaned temporary files
    if progress:
        progress("Pruning temporary files")
    if not dry_run:
        object_store.prune(grace_period=grace_period)

    # Count final state
    stats.packs_after = len(list(object_store.packs))
    # TODO: Count loose objects when we have a method for it

    return stats
