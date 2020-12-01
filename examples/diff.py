"""Diff.

This trivial script demonstrates how to extract the unified diff for a single
commit in a local repository.

Example usage:
    python examples/diff.py
"""

import sys

from dulwich.repo import Repo

from dulwich.patch import write_tree_diff


REPO_PATH = "."
COMMIT_ID = b"a6602654997420bcfd0bee2a0563d9416afe34b4"

REPOSITORY = Repo(REPO_PATH)

commit = REPOSITORY[COMMIT_ID]
parent_commit = REPOSITORY[commit.parents[0]]
outstream = getattr(sys.stdout, 'buffer', sys.stdout)
write_tree_diff(
    outstream, REPOSITORY.object_store, parent_commit.tree, commit.tree)
