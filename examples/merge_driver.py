#!/usr/bin/python3
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

"""Simple example demonstrating merge driver usage in dulwich.

This example:
1. Creates a test repository with .gitattributes
2. Implements a JSON merge driver
3. Creates two branches with conflicting JSON changes
4. Merges one commit into another using the custom driver
"""

import json
import os
from typing import Optional

from dulwich import porcelain
from dulwich.merge_drivers import get_merge_driver_registry
from dulwich.repo import Repo


class JSONMergeDriver:
    """Simple merge driver for JSON files."""

    def merge(
        self,
        ancestor: bytes,
        ours: bytes,
        theirs: bytes,
        path: Optional[str] = None,
        marker_size: int = 7,
    ) -> tuple[bytes, bool]:
        """Merge JSON files by combining objects."""
        try:
            # Parse JSON content
            ancestor_data = json.loads(ancestor.decode()) if ancestor.strip() else {}
            ours_data = json.loads(ours.decode()) if ours.strip() else {}
            theirs_data = json.loads(theirs.decode()) if theirs.strip() else {}

            # Simple merge: combine all fields
            merged: dict = {}
            merged.update(ancestor_data)
            merged.update(ours_data)
            merged.update(theirs_data)

            # Convert back to JSON with nice formatting
            result = json.dumps(merged, indent=2).encode()
            return result, True

        except (json.JSONDecodeError, UnicodeDecodeError):
            # Fall back to simple concatenation on parse error
            result = ours + b"\n<<<<<<< MERGE CONFLICT >>>>>>>\n" + theirs
            return result, False


# Create temporary directory for test repo
# Initialize repository
repo = Repo.init("merge-driver", mkdir=True)

# Create .gitattributes file
gitattributes_path = os.path.join(repo.path, ".gitattributes")
with open(gitattributes_path, "w") as f:
    f.write("*.json merge=jsondriver\n")

# Create initial JSON file
config_path = os.path.join(repo.path, "config.json")
initial_config = {"name": "test-project", "version": "1.0.0"}
with open(config_path, "w") as f:
    json.dump(initial_config, f, indent=2)

# Add and commit initial files
repo.get_worktree().stage([".gitattributes", "config.json"])
initial_commit = repo.get_worktree().commit(
    b"Initial commit", committer=b"Test <test@example.com>"
)

# Register our custom merge driver globally
registry = get_merge_driver_registry()
registry.register_driver("jsondriver", JSONMergeDriver())

# Create and switch to feature branch
porcelain.branch_create(repo, "feature")
repo.refs[b"HEAD"] = repo.refs[b"refs/heads/feature"]

# Make changes on feature branch
feature_config = {
    "name": "test-project",
    "version": "1.0.0",
    "author": "Alice",
    "features": ["logging", "database"],
}
with open(config_path, "w") as f:
    json.dump(feature_config, f, indent=2)

repo.get_worktree().stage(["config.json"])
feature_commit = repo.get_worktree().commit(
    b"Add author and features", committer=b"Alice <alice@example.com>"
)

# Switch back to master
repo.refs[b"HEAD"] = repo.refs[b"refs/heads/master"]

# Make different changes on master
master_config = {
    "name": "test-project",
    "version": "1.1.0",
    "description": "A test project for merge drivers",
    "license": "Apache-2.0",
}
with open(config_path, "w") as f:
    json.dump(master_config, f, indent=2)

repo.get_worktree().stage(["config.json"])
master_commit = repo.get_worktree().commit(
    b"Add description and license", committer=b"Bob <bob@example.com>"
)

# Perform the merge using porcelain.merge
# The merge should use our custom JSON driver for config.json
merge_result = porcelain.merge(repo, "feature")
# Show the merged content
with open(config_path) as f:
    merged_content = f.read()

print("\nMerged config.json content:")
print(merged_content)
