#!/usr/bin/env python3

"""Fix dulwich history by removing .git directories and updating old timestamps.

Usage: ./fix-history.py <source-branch> <target-branch>
Example: ./fix-history.py master main
"""

import sys
import time

from dulwich.objects import Commit, Tree
from dulwich.repo import Repo


def fix_tree(repo, tree_id, seen_trees=None):
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

        # Skip .git entries
        if name == b".git":
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


def fix_commit_dates(commit):
    """Fix commit dates if they're before 1990."""
    modified = False

    # Unix timestamp for 1990-01-01
    min_timestamp = 315532800
    max_timestamp = int(time.time())

    # Fix author date
    if commit.author_time < min_timestamp:
        new_time = commit.author_time * 10
        if min_timestamp <= new_time <= max_timestamp:
            print(f"Fixed author date: {commit.author_time} -> {new_time}")
            commit.author_time = new_time
            modified = True

    # Fix committer date
    if commit.commit_time < min_timestamp:
        new_time = commit.commit_time * 10
        if min_timestamp <= new_time <= max_timestamp:
            print(f"Fixed committer date: {commit.commit_time} -> {new_time}")
            commit.commit_time = new_time
            modified = True

    return modified


def rewrite_history(repo, source_branch, target_branch):
    """Rewrite history to fix issues."""
    print(f"=== Rewriting history from {source_branch} to {target_branch} ===")

    # Get the head commit of the source branch
    try:
        source_ref = f"refs/heads/{source_branch}".encode()
        head_sha = repo.refs[source_ref]
    except KeyError:
        print(f"Error: Branch '{source_branch}' not found")
        return False

    # Map old commit SHAs to new ones
    commit_map = {}
    tree_map = {}

    # Get all commits in topological order
    walker = repo.get_walker([head_sha])
    commits = list(walker)
    commits.reverse()  # Process from oldest to newest

    print(f"Processing {len(commits)} commits...")

    for i, commit_entry in enumerate(commits):
        old_commit = commit_entry.commit

        if i % 100 == 0:
            print(f"Processed {i}/{len(commits)} commits...")

        # Fix the tree
        old_tree_id = old_commit.tree
        if old_tree_id not in tree_map:
            tree_map[old_tree_id] = fix_tree(repo, old_tree_id)
        new_tree_id = tree_map[old_tree_id]

        # Create new commit
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
        # note: Drop extra fields

        # Fix dates
        date_modified = fix_commit_dates(new_commit)

        # Map parent commits
        new_parents = []
        for parent_sha in old_commit.parents:
            if parent_sha in commit_map:
                new_parents.append(commit_map[parent_sha])
            else:
                new_parents.append(parent_sha)
        new_commit.parents = new_parents

        # Check if commit actually changed
        if (
            new_tree_id == old_tree_id
            and not date_modified
            and new_parents == list(old_commit.parents)
        ):
            # No changes needed, reuse old commit
            commit_map[old_commit.id] = old_commit.id
        else:
            # Add new commit to object store
            repo.object_store.add_object(new_commit)
            commit_map[old_commit.id] = new_commit.id

    # Update the target branch
    new_head = commit_map[head_sha]
    target_ref = f"refs/heads/{target_branch}".encode()
    repo.refs[target_ref] = new_head

    print(
        f"✓ Created branch '{target_branch}' with {len([k for k, v in commit_map.items() if k != v])} modified commits"
    )
    return True


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <source-branch> <target-branch>")
        print(f"Example: {sys.argv[0]} master main")
        print("")
        print(
            "This will create a new branch <target-branch> with the rewritten history from <source-branch>"
        )
        sys.exit(1)

    source_branch = sys.argv[1]
    target_branch = sys.argv[2]

    print("=== Dulwich History Fix Script ===")
    print("This script will:")
    print("1. Remove .git directories from tree objects")
    print("2. Fix any commits with dates before 1990")
    print(
        f"3. Create new branch '{target_branch}' from '{source_branch}' with fixed history"
    )
    print("")
    print(f"Source branch: {source_branch}")
    print(f"Target branch: {target_branch}")
    print("")

    # Open the repository
    try:
        repo = Repo(".")
    except Exception as e:
        print(f"Error: Could not open repository: {e}")
        sys.exit(1)

    # Check if source branch exists
    source_ref = f"refs/heads/{source_branch}".encode()
    if source_ref not in repo.refs:
        print(f"Error: Source branch '{source_branch}' does not exist")
        sys.exit(1)

    # Check if target branch already exists
    target_ref = f"refs/heads/{target_branch}".encode()
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
                if name == b".git":
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
    if not rewrite_history(repo, source_branch, target_branch):
        sys.exit(1)

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
    print("")
    print("IMPORTANT NEXT STEPS:")
    print(f"1. Review the changes: git log --oneline {target_branch}")
    print(
        f"2. Compare commit count: git rev-list --count {source_branch} vs git rev-list --count {target_branch}"
    )
    print("3. If satisfied, you can:")
    print(f"   - Push the new branch: git push origin {target_branch}")
    print("   - Set it as default branch on GitHub/GitLab")
    print(f"   - Update local checkout: git checkout {target_branch}")
    print("")
    print(f"The original branch '{source_branch}' remains unchanged.")


if __name__ == "__main__":
    main()
