#!/usr/bin/env python3

"""Fix dulwich history by removing .git directories and updating old timestamps.

Usage: ./fix-history.py <source-branch> <target-branch> [--update-tags] [--rewrite-tag-commits]
Example: ./fix-history.py master main --update-tags --rewrite-tag-commits
"""

import argparse
import sys
import time
from dataclasses import dataclass

from dulwich.objects import Commit, ObjectID, Tag, Tree
from dulwich.refs import Ref
from dulwich.repo import Repo

BANNED_NAMES = [b".git"]


@dataclass
class RewriteResult:
    """Result of rewriting history."""

    commit_map: dict[ObjectID, ObjectID]
    tree_map: dict[ObjectID, ObjectID]
    filtered_commits: set[ObjectID]


def create_fixed_commit(
    old_commit: Commit, new_tree_id: ObjectID, new_parents: list[ObjectID]
) -> Commit:
    """Create a new commit from an old one with fixes applied.

    Args:
        old_commit: The original commit
        new_tree_id: The new tree SHA
        new_parents: List of parent commit SHAs

    Returns:
        A new commit with fixes applied
    """
    new_commit = Commit()
    new_commit.tree = new_tree_id
    new_commit.author = old_commit.author
    new_commit.committer = old_commit.committer
    new_commit.author_time = old_commit.author_time
    new_commit.commit_time = old_commit.commit_time
    new_commit.author_timezone = old_commit.author_timezone
    new_commit.commit_timezone = old_commit.commit_timezone
    new_commit.message = old_commit.message
    new_commit.encoding = old_commit.encoding
    new_commit.parents = new_parents

    # Fix dates
    fix_commit_dates(new_commit)

    # Fix email addresses
    if b"jvernooij@evroc.com" in old_commit.author:
        new_commit.author = "Jelmer Vernooĳ <jelmer@jelmer.uk>".encode()

    if b"jvernooij@evroc.com" in old_commit.committer:
        new_commit.committer = "Jelmer Vernooĳ <jelmer@jelmer.uk>".encode()

    return new_commit


def fix_tree(
    repo: Repo, tree_id: ObjectID, seen_trees: set[ObjectID] | None = None
) -> ObjectID:
    """Recursively fix a tree by removing .git entries."""
    if seen_trees is None:
        seen_trees = set()

    if tree_id in seen_trees:
        return tree_id
    seen_trees.add(tree_id)

    try:
        tree = repo[tree_id]
    except KeyError:
        return tree_id

    if not isinstance(tree, Tree):
        return tree_id

    # Check if this tree contains .git entries
    modified = False
    new_items = []

    for item in tree.items():
        name, mode, sha = item

        if name in BANNED_NAMES:
            modified = True
            continue

        # Recursively fix subtrees
        if mode == 0o040000:  # Directory mode
            new_sha = fix_tree(repo, sha, seen_trees)
            if new_sha != sha:
                modified = True
                sha = new_sha

        new_items.append((name, mode, sha))

    if not modified:
        return tree_id

    print(f"Removing .git entry from tree {tree_id.decode()}")

    # Create new tree without .git entries
    new_tree = Tree()
    for name, mode, sha in new_items:
        new_tree.add(name, mode, sha)

    repo.object_store.add_object(new_tree)
    return new_tree.id


def fix_commit_dates(commit: Commit) -> None:
    """Fix commit dates if they're before 1990."""
    # Unix timestamp for 1990-01-01
    min_timestamp = 315532800
    max_timestamp = int(time.time())

    # Fix author date
    if commit.author_time < min_timestamp:
        new_time = commit.author_time * 10
        if min_timestamp <= new_time <= max_timestamp:
            print(f"Fixed author date: {commit.author_time} -> {new_time}")
            commit.author_time = new_time

    # Fix committer date
    if commit.commit_time < min_timestamp:
        new_time = commit.commit_time * 10
        if min_timestamp <= new_time <= max_timestamp:
            print(f"Fixed committer date: {commit.commit_time} -> {new_time}")
            commit.commit_time = new_time


def rewrite_commit(
    repo: Repo,
    commit_sha: ObjectID,
    commit_map: dict[ObjectID, ObjectID],
    tree_map: dict[ObjectID, ObjectID],
    filtered_commits: set[ObjectID],
) -> ObjectID | None:
    """Rewrite a single commit and its ancestors.

    This is used to rewrite commits that weren't part of the main branch
    but are referenced by tags. Uses dulwich's walker for efficient traversal,
    stopping at commits that have already been rewritten.
    """
    # If already mapped, return the mapped version
    if commit_sha in commit_map:
        return commit_map[commit_sha]

    # Use walker to efficiently get commits in topological order
    # Exclude commits that are already mapped to avoid reprocessing
    exclude = list(commit_map.keys())

    try:
        # Get commits in reverse topological order (parents before children)
        walker = repo.get_walker(
            include=[commit_sha], exclude=exclude, order="topo", reverse=True
        )
        commits_to_process = []

        for entry in walker:
            commits_to_process.append(entry.commit)

        print(
            f"  Processing {len(commits_to_process)} unmapped commits for tag target {commit_sha.decode()[:8]}"
        )

    except Exception as e:
        print(f"Warning: Could not walk commits from {commit_sha.decode()[:8]}: {e}")
        return commit_sha

    # Process commits in order (parents before children)
    for old_commit in commits_to_process:
        commit_sha_current = old_commit.id

        # Skip if already mapped
        if commit_sha_current in commit_map:
            continue

        # Handle filtered commits
        if commit_sha_current in filtered_commits:
            if old_commit.parents:
                parent_sha = old_commit.parents[0]
                if parent_sha in commit_map:
                    commit_map[commit_sha_current] = commit_map[parent_sha]
            continue

        # Map parent commits
        new_parents = []
        for parent_sha in old_commit.parents:
            if parent_sha in commit_map:
                mapped = commit_map[parent_sha]
                if mapped is not None:
                    new_parents.append(mapped)
            else:
                # Parent should have been processed already due to topological order
                # Use original as fallback
                new_parents.append(parent_sha)

        # Fix the tree
        old_tree_id = old_commit.tree
        if old_tree_id not in tree_map:
            tree_map[old_tree_id] = fix_tree(repo, old_tree_id)
        new_tree_id = tree_map[old_tree_id]

        # Create new commit with fixes
        new_commit = create_fixed_commit(old_commit, new_tree_id, new_parents)

        # Add new commit to object store
        repo.object_store.add_object(new_commit)
        commit_map[commit_sha_current] = new_commit.id

    return commit_map.get(commit_sha)


def rewrite_history(
    repo: Repo, source_branch: str, target_branch: str
) -> RewriteResult | None:
    """Rewrite history to fix issues."""
    print(f"=== Rewriting history from {source_branch} to {target_branch} ===")

    # Commits to filter out completely
    filtered_commits: set[ObjectID] = {
        ObjectID(b"336232af1246017ce037b87e913d23e2c2a3bbbd"),
        ObjectID(b"e673babfc11d0b4001d9d08b9b9cef57c6aa67f5"),
    }

    # Get the head commit of the source branch
    try:
        source_ref = Ref(f"refs/heads/{source_branch}".encode())
        head_sha = repo.refs[source_ref]
    except KeyError:
        print(f"Error: Branch '{source_branch}' not found")
        return None

    # Map old commit SHAs to new ones
    commit_map: dict[ObjectID, ObjectID] = {}
    tree_map: dict[ObjectID, ObjectID] = {}

    # Get all commits in topological order
    walker = repo.get_walker([head_sha], order="topo", reverse=True)
    commits = list(walker)

    print(f"Processing {len(commits)} commits...")

    for i, commit_entry in enumerate(commits):
        old_commit = commit_entry.commit

        if i % 100 == 0:
            print(f"Processed {i}/{len(commits)} commits...")

        # Skip filtered commits entirely
        if old_commit.id in filtered_commits:
            # Map this commit to its parent (skip it in the history)
            if old_commit.parents:
                # If the parent has been remapped, use the remapped version
                parent_sha = old_commit.parents[0]
                commit_map[old_commit.id] = commit_map[parent_sha]
            else:
                # This is a root commit, skip it by not adding to commit_map
                pass
            continue

        # Fix the tree
        old_tree_id = old_commit.tree
        if old_tree_id not in tree_map:
            tree_map[old_tree_id] = fix_tree(repo, old_tree_id)
        new_tree_id = tree_map[old_tree_id]

        # Map parent commits
        new_parents = []
        for parent_sha in old_commit.parents:
            parent_sha = commit_map[parent_sha]
            new_parents.append(parent_sha)

        # Create new commit with fixes (note: Drop extra fields)
        new_commit = create_fixed_commit(old_commit, new_tree_id, new_parents)

        if old_commit.parents != new_parents:
            assert old_commit.id != new_commit.id

        # Add new commit to object store
        repo.object_store.add_object(new_commit)
        assert old_commit.id not in commit_map
        commit_map[old_commit.id] = new_commit.id

    # Update the target branch
    new_head = commit_map[head_sha]
    target_ref = Ref(f"refs/heads/{target_branch}".encode())
    repo.refs[target_ref] = new_head

    print(
        f"✓ Created branch '{target_branch}' with {len([k for k, v in commit_map.items() if k != v])} modified commits"
    )
    return RewriteResult(
        commit_map=commit_map, tree_map=tree_map, filtered_commits=filtered_commits
    )


def update_tags(
    repo: Repo, rewrite_result: RewriteResult, rewrite_non_branch_commits: bool = False
) -> list[str]:
    """Update tags to point to rewritten commits.

    Args:
        repo: The repository
        rewrite_result: RewriteResult containing commit_map, tree_map, and filtered_commits
        rewrite_non_branch_commits: If True, also rewrite commits that tags point to
                                     even if they weren't part of the main branch rewrite

    Returns:
        List of tag names that were updated or rewritten
    """
    print("")
    print("=== Updating tags ===")

    commit_map = rewrite_result.commit_map
    tree_map = rewrite_result.tree_map
    filtered_commits = rewrite_result.filtered_commits

    updated_tags = []
    skipped_tags = []
    rewritten_tags = []

    # Iterate through all refs looking for tags
    for ref_name, ref_value in list(repo.refs.as_dict().items()):
        if not ref_name.startswith(b"refs/tags/"):
            continue

        tag_name = ref_name[len(b"refs/tags/") :].decode()

        # Try to get the tag object
        try:
            tag_obj = repo[ref_value]
        except KeyError:
            print(f"Warning: Could not find object for tag '{tag_name}'")
            continue

        # Handle annotated tags (Tag objects)
        if isinstance(tag_obj, Tag):
            # Get the commit that the tag points to
            target_sha = tag_obj.object[1]

            if target_sha in commit_map:
                new_target_sha = commit_map[target_sha]

                if new_target_sha != target_sha:
                    # Create a new tag object pointing to the rewritten commit
                    new_tag = Tag()
                    new_tag.name = tag_obj.name
                    new_tag.object = (tag_obj.object[0], new_target_sha)
                    new_tag.tag_time = tag_obj.tag_time
                    new_tag.tag_timezone = tag_obj.tag_timezone
                    new_tag.tagger = tag_obj.tagger
                    new_tag.message = tag_obj.message

                    # Add the new tag object to the object store
                    repo.object_store.add_object(new_tag)

                    # Update the ref to point to the new tag object
                    repo.refs[ref_name] = new_tag.id

                    print(
                        f"Updated annotated tag '{tag_name}': {target_sha.decode()[:8]} -> {new_target_sha.decode()[:8]}"
                    )
                    updated_tags.append(tag_name)
                else:
                    skipped_tags.append(tag_name)
            elif rewrite_non_branch_commits:
                # Rewrite this commit and its ancestors
                print(
                    f"Rewriting history for tag '{tag_name}' (commit {target_sha.decode()[:8]} not in branch)"
                )
                rewritten_sha = rewrite_commit(
                    repo, target_sha, commit_map, tree_map, filtered_commits
                )

                if rewritten_sha and rewritten_sha != target_sha:
                    # Create a new tag object pointing to the rewritten commit
                    new_tag = Tag()
                    new_tag.name = tag_obj.name
                    new_tag.object = (tag_obj.object[0], rewritten_sha)
                    new_tag.tag_time = tag_obj.tag_time
                    new_tag.tag_timezone = tag_obj.tag_timezone
                    new_tag.tagger = tag_obj.tagger
                    new_tag.message = tag_obj.message

                    # Add the new tag object to the object store
                    repo.object_store.add_object(new_tag)

                    # Update the ref to point to the new tag object
                    repo.refs[ref_name] = new_tag.id

                    print(
                        f"Rewrote and updated annotated tag '{tag_name}': {target_sha.decode()[:8]} -> {rewritten_sha.decode()[:8]}"
                    )
                    rewritten_tags.append(tag_name)
                else:
                    skipped_tags.append(tag_name)
            else:
                print(
                    f"Warning: Tag '{tag_name}' points to commit not in history, skipping"
                )
                skipped_tags.append(tag_name)

        # Handle lightweight tags (direct references to commits)
        elif isinstance(tag_obj, Commit):
            commit_sha = ref_value

            if commit_sha in commit_map:
                new_commit_sha = commit_map[commit_sha]

                if new_commit_sha != commit_sha:
                    # Update the ref to point to the new commit
                    repo.refs[ref_name] = new_commit_sha

                    print(
                        f"Updated lightweight tag '{tag_name}': {commit_sha.decode()[:8]} -> {new_commit_sha.decode()[:8]}"
                    )
                    updated_tags.append(tag_name)
                else:
                    skipped_tags.append(tag_name)
            elif rewrite_non_branch_commits:
                # Rewrite this commit and its ancestors
                print(
                    f"Rewriting history for tag '{tag_name}' (commit {commit_sha.decode()[:8]} not in branch)"
                )
                rewritten_sha = rewrite_commit(
                    repo, commit_sha, commit_map, tree_map, filtered_commits
                )

                if rewritten_sha and rewritten_sha != commit_sha:
                    # Update the ref to point to the new commit
                    repo.refs[ref_name] = rewritten_sha

                    print(
                        f"Rewrote and updated lightweight tag '{tag_name}': {commit_sha.decode()[:8]} -> {rewritten_sha.decode()[:8]}"
                    )
                    rewritten_tags.append(tag_name)
                else:
                    skipped_tags.append(tag_name)
            else:
                print(
                    f"Warning: Tag '{tag_name}' points to commit not in history, skipping"
                )
                skipped_tags.append(tag_name)
        else:
            print(
                f"Warning: Tag '{tag_name}' points to non-commit/non-tag object, skipping"
            )
            skipped_tags.append(tag_name)

    print(f"✓ Updated {len(updated_tags)} tags")
    if rewritten_tags:
        print(
            f"✓ Rewrote and updated {len(rewritten_tags)} tags (commits not in branch)"
        )
    if skipped_tags:
        print(
            f"  Skipped {len(skipped_tags)} tags (unchanged or not in rewritten history)"
        )

    return updated_tags + rewritten_tags


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix dulwich history by removing .git directories and updating old timestamps.",
        epilog="This will create a new branch <target-branch> with the rewritten history from <source-branch>",
    )
    parser.add_argument("source_branch", help="Source branch to rewrite from")
    parser.add_argument(
        "target_branch", help="Target branch to create with rewritten history"
    )
    parser.add_argument(
        "--update-tags",
        action="store_true",
        help="Update existing tags to point to rewritten commits",
    )
    parser.add_argument(
        "--rewrite-tag-commits",
        action="store_true",
        help="Also rewrite commits that tags point to, even if they aren't in the main branch history",
    )

    args = parser.parse_args()

    source_branch = args.source_branch
    target_branch = args.target_branch
    update_tags_flag = args.update_tags
    rewrite_tag_commits_flag = args.rewrite_tag_commits

    # Validate flags
    if rewrite_tag_commits_flag and not update_tags_flag:
        print("Error: --rewrite-tag-commits requires --update-tags")
        sys.exit(1)

    print("=== Dulwich History Fix Script ===")
    print("This script will:")
    print("1. Remove .git directories from tree objects")
    print("2. Fix any commits with dates before 1990")
    print(
        f"3. Create new branch '{target_branch}' from '{source_branch}' with fixed history"
    )
    if update_tags_flag:
        print("4. Update existing tags to point to rewritten commits")
        if rewrite_tag_commits_flag:
            print(
                "   - Including rewriting commits that tags point to outside the branch"
            )
    print("")
    print(f"Source branch: {source_branch}")
    print(f"Target branch: {target_branch}")
    if update_tags_flag:
        print("Update tags: Yes")
        if rewrite_tag_commits_flag:
            print("Rewrite tag commits: Yes")
    print("")

    # Open the repository
    try:
        repo = Repo(".")
    except Exception as e:
        print(f"Error: Could not open repository: {e}")
        sys.exit(1)

    # Check if source branch exists
    source_ref = Ref(f"refs/heads/{source_branch}".encode())
    if source_ref not in repo.refs:
        print(f"Error: Source branch '{source_branch}' does not exist")
        sys.exit(1)

    # Check if target branch already exists
    target_ref = Ref(f"refs/heads/{target_branch}".encode())
    if target_ref in repo.refs:
        print(f"Error: Target branch '{target_branch}' already exists")
        print("Please delete it first or choose a different name")
        sys.exit(1)

    # Identify problematic trees
    print("")
    print("=== Identifying problematic trees ===")
    bad_trees = []
    for sha in repo.object_store:
        obj = repo[sha]
        if isinstance(obj, Tree):
            for name, mode, item_sha in obj.items():
                if name in BANNED_NAMES:
                    bad_trees.append(sha)
                    break

    print(f"Found {len(bad_trees)} trees with .git directories")

    # Check for commits with bad dates
    print("")
    print("=== Identifying problematic commits ===")
    bad_dates = []
    for sha in repo.object_store:
        obj = repo[sha]
        if isinstance(obj, Commit):
            if obj.commit_time < 315532800 or obj.author_time < 315532800:
                bad_dates.append(sha)

    print(f"Found {len(bad_dates)} commits with dates before 1990")

    # Rewrite history
    print("")
    rewrite_result = rewrite_history(repo, source_branch, target_branch)
    if not rewrite_result:
        sys.exit(1)

    # Update tags if requested
    if update_tags_flag:
        update_tags(repo, rewrite_result, rewrite_tag_commits_flag)

    print("")
    print("=== Complete ===")
    print(
        f"Successfully created branch '{target_branch}' with fixed history from '{source_branch}'"
    )
    print("")
    print("Summary of changes:")
    print("- Removed .git directories from tree objects")
    print("- Fixed commit timestamps that were before 1990")
    print(f"- Created clean history in branch '{target_branch}'")
    if update_tags_flag:
        if rewrite_tag_commits_flag:
            print(
                "- Updated tags to point to rewritten commits (including commits outside branch)"
            )
        else:
            print("- Updated tags to point to rewritten commits")
    print("")
    print("IMPORTANT NEXT STEPS:")
    print(f"1. Review the changes: git log --oneline {target_branch}")
    print(
        f"2. Compare commit count: git rev-list --count {source_branch} vs git rev-list --count {target_branch}"
    )
    if update_tags_flag:
        print("3. Review updated tags: git tag -l")
    print(
        "3. If satisfied, you can:"
        if not update_tags_flag
        else "4. If satisfied, you can:"
    )
    print(f"   - Push the new branch: git push origin {target_branch}")
    if update_tags_flag:
        print("   - Force push updated tags: git push origin --tags --force")
    print("   - Set it as default branch on GitHub/GitLab")
    print(f"   - Update local checkout: git checkout {target_branch}")
    print("")
    print(f"The original branch '{source_branch}' remains unchanged.")
    if update_tags_flag:
        print(
            "WARNING: Tags have been updated. You may need to force push them to remote."
        )
        if rewrite_tag_commits_flag:
            print(
                "WARNING: Tag commits outside the branch were also rewritten. Review carefully."
            )


if __name__ == "__main__":
    main()
