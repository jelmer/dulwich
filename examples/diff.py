#!/usr/bin/python
# This trivial script demonstrates how to extract the unified diff for a single
# commit in a local repository.
#
# Example usage:
#  python examples/diff.py

import sys

from dulwich.patch import write_tree_diff
from dulwich.repo import Repo

repo_path = "."
commit_id = b"a6602654997420bcfd0bee2a0563d9416afe34b4"

r = Repo(repo_path)

commit = r[commit_id]
parent_commit = r[commit.parents[0]]
outstream = getattr(sys.stdout, "buffer", sys.stdout)
write_tree_diff(outstream, r.object_store, parent_commit.tree, commit.tree)
