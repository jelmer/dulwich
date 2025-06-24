#!/usr/bin/env python3
"""Example of using filter-branch to rewrite commit history.

This demonstrates how to use dulwich's filter-branch functionality to:
- Change author/committer information
- Modify commit messages
- Apply custom filters

The example shows both the high-level porcelain interface and the
lower-level filter_branch module API.
"""

import sys

from dulwich import porcelain
from dulwich.filter_branch import CommitFilter, filter_refs
from dulwich.repo import Repo


def example_change_author(repo_path):
    """Example: Change all commits to have a new author."""
    print("Changing author for all commits...")

    def new_author(old_author):
        # Change any commit by "Old Author" to "New Author"
        if b"Old Author" in old_author:
            return b"New Author <new@example.com>"
        return old_author

    result = porcelain.filter_branch(repo_path, "HEAD", filter_author=new_author)

    print(f"Rewrote {len(result)} commits")
    return result


def example_prefix_messages(repo_path):
    """Example: Add a prefix to all commit messages."""
    print("Adding prefix to commit messages...")

    def add_prefix(message):
        return b"[PROJECT-123] " + message

    result = porcelain.filter_branch(repo_path, "HEAD", filter_message=add_prefix)

    print(f"Rewrote {len(result)} commits")
    return result


def example_custom_filter(repo_path):
    """Example: Custom filter that changes multiple fields."""
    print("Applying custom filter...")

    def custom_filter(commit):
        # This filter:
        # - Standardizes author format
        # - Adds issue number to message if missing
        # - Updates committer to match author

        changes = {}

        # Standardize author format
        if b"<" not in commit.author:
            changes["author"] = commit.author + b" <unknown@example.com>"

        # Add issue number if missing
        if not commit.message.startswith(b"[") and not commit.message.startswith(
            b"Merge"
        ):
            changes["message"] = b"[LEGACY] " + commit.message

        # Make committer match author
        if commit.author != commit.committer:
            changes["committer"] = commit.author

        return changes if changes else None

    result = porcelain.filter_branch(repo_path, "HEAD", filter_fn=custom_filter)

    print(f"Rewrote {len(result)} commits")
    return result


def example_low_level_api(repo_path):
    """Example: Using the low-level filter_branch module API."""
    print("Using low-level filter_branch API...")

    with Repo(repo_path) as repo:
        # Create a custom filter
        def transform_message(msg):
            # Add timestamp and uppercase first line
            lines = msg.split(b"\n")
            if lines:
                lines[0] = lines[0].upper()
            return b"[TRANSFORMED] " + b"\n".join(lines)

        # Create the commit filter
        commit_filter = CommitFilter(
            repo.object_store,
            filter_message=transform_message,
            filter_author=lambda a: b"Transformed Author <transformed@example.com>",
        )

        # Filter the master branch
        result = filter_refs(
            repo.refs,
            repo.object_store,
            [b"refs/heads/master"],
            commit_filter,
            keep_original=True,
            force=False,
        )

        print(f"Rewrote {len(result)} commits using low-level API")
        return result


def main():
    if len(sys.argv) < 2:
        print("Usage: filter_branch.py <repo_path> [example]")
        print("Examples: change_author, prefix_messages, custom_filter, low_level")
        sys.exit(1)

    repo_path = sys.argv[1]
    example = sys.argv[2] if len(sys.argv) > 2 else "change_author"

    examples = {
        "change_author": example_change_author,
        "prefix_messages": example_prefix_messages,
        "custom_filter": example_custom_filter,
        "low_level": example_low_level_api,
    }

    if example not in examples:
        print(f"Unknown example: {example}")
        print(f"Available examples: {', '.join(examples.keys())}")
        sys.exit(1)

    try:
        examples[example](repo_path)
        print("Filter-branch completed successfully!")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
