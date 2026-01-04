#!/usr/bin/env python3

"""Fix dulwich history by removing .git directories and updating old timestamps.

Usage: ./fix-history.py <source-branch> <target-branch> [--update-tags]
Example: ./fix-history.py master main --update-tags
"""

import argparse
import sys
import time

from dulwich.objects import Commit, Tag, Tree
from dulwich.repo import Repo

BANNED_NAMES = [b".git"]


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


def fix_commit_dates(commit):
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


def rewrite_history(repo, source_branch, target_branch):
    """Rewrite history to fix issues."""
    print(f"=== Rewriting history from {source_branch} to {target_branch} ===")

    # Commits to filter out completely
    filtered_commits = {
        b"336232af1246017ce037b87e913d23e2c2a3bbbd",
        b"e673babfc11d0b4001d9d08b9b9cef57c6aa67f5",
    }

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
        fix_commit_dates(new_commit)

        if b"jvernooij@evroc.com" in old_commit.author:
            new_commit.author = "Jelmer Vernooĳ <jelmer@jelmer.uk>".encode()

        if b"jvernooij@evroc.com" in old_commit.committer:
            new_commit.committer = "Jelmer Vernooĳ <jelmer@jelmer.uk>".encode()

        # Map parent commits
        new_parents = []
        for parent_sha in old_commit.parents:
            parent_sha = commit_map[parent_sha]
            new_parents.append(parent_sha)
        new_commit.parents = new_parents

        if old_commit.parents != new_parents:
            assert old_commit.id != new_commit.id

        # Add new commit to object store
        repo.object_store.add_object(new_commit)
        assert old_commit.id not in commit_map
        commit_map[old_commit.id] = new_commit.id

    # Update the target branch
    new_head = commit_map[head_sha]
    target_ref = f"refs/heads/{target_branch}".encode()
    repo.refs[target_ref] = new_head

    print(
        f"✓ Created branch '{target_branch}' with {len([k for k, v in commit_map.items() if k != v])} modified commits"
    )
    return commit_map


def update_tags(repo, commit_map):
    """Update tags to point to rewritten commits."""
    print("")
    print("=== Updating tags ===")

    updated_tags = []
    skipped_tags = []

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
    if skipped_tags:
        print(
            f"  Skipped {len(skipped_tags)} tags (unchanged or not in rewritten history)"
        )

    return updated_tags


def main():
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

    args = parser.parse_args()

    source_branch = args.source_branch
    target_branch = args.target_branch
    update_tags_flag = args.update_tags

    print("=== Dulwich History Fix Script ===")
    print("This script will:")
    print("1. Remove .git directories from tree objects")
    print("2. Fix any commits with dates before 1990")
    print(
        f"3. Create new branch '{target_branch}' from '{source_branch}' with fixed history"
    )
    if update_tags_flag:
        print("4. Update existing tags to point to rewritten commits")
    print("")
    print(f"Source branch: {source_branch}")
    print(f"Target branch: {target_branch}")
    if update_tags_flag:
        print("Update tags: Yes")
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
    commit_map = rewrite_history(repo, source_branch, target_branch)
    if not commit_map:
        sys.exit(1)

    # Update tags if requested
    if update_tags_flag:
        update_tags(repo, commit_map)

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


if __name__ == "__main__":
    main()
