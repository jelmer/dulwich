#!/usr/bin/python
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

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
write_tree_diff(sys.stdout.buffer, r.object_store, parent_commit.tree, commit.tree)
