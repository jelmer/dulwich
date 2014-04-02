#!/usr/bin/python
# Example printing the last author of a specified file

import sys
import time
from dulwich.repo import Repo

if len(sys.argv) < 2:
    print("usage: %s filename" % (sys.argv[0], ))
    sys.exit(1)

r = Repo(".")

w = r.get_walker(paths=[sys.argv[1]], max_entries=1)
try:
    c = next(iter(w)).commit
except StopIteration:
    print("No file %s anywhere in history." % sys.argv[1])
else:
    print("%s was last changed at %s by %s (commit %s)" % (
        sys.argv[1], c.author, time.ctime(c.author_time), c.id))
