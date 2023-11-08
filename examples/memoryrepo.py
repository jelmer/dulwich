#!/usr/bin/python
# This script creates a clone of a remote repository in local memory,
# then adds a single file and pushes the result back.
#
# Example usage:
#  python examples/memoryrepo.py git+ssh://github.com/jelmer/testrepo

import stat
import sys

from dulwich import porcelain
from dulwich.objects import Blob
from dulwich.repo import MemoryRepo

local_repo = MemoryRepo()
local_repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

fetch_result = porcelain.fetch(local_repo, sys.argv[1])
local_repo.refs[b"refs/heads/master"] = fetch_result.refs[b"refs/heads/master"]
print(local_repo.refs.as_dict())

last_tree = local_repo[local_repo[b"HEAD"].tree]
new_blob = Blob.from_string(b"Some contents")
local_repo.object_store.add_object(new_blob)
last_tree.add(b"test", stat.S_IFREG, new_blob.id)
local_repo.object_store.add_object(last_tree)

local_repo.do_commit(
    message=b"Add a file called 'test'", ref=b"refs/heads/master", tree=last_tree.id
)

porcelain.push(local_repo, sys.argv[1], "master")
