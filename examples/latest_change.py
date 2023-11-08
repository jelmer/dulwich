#!/usr/bin/python
# Example printing the last author of a specified file

import sys
import time

from dulwich.repo import Repo

if len(sys.argv) < 2:
    print(f"usage: {sys.argv[0]} filename")
    sys.exit(1)

r = Repo(".")

path = sys.argv[1].encode("utf-8")

w = r.get_walker(paths=[path], max_entries=1)
try:
    c = next(iter(w)).commit
except StopIteration:
    print("No file %s anywhere in history." % sys.argv[1])
else:
    print(
        "{} was last changed by {} at {} (commit {})".format(
            sys.argv[1], c.author, time.ctime(c.author_time), c.id
        )
    )
