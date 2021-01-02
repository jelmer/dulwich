#!/usr/bin/python
# This trivial script demonstrates how to clone a remote repository.
#
# Example usage:
#  python examples/clone.py git://github.com/jelmer/dulwich dulwich-clone

import sys
from getopt import getopt
from dulwich import porcelain

opts, args = getopt(sys.argv, "", [])
opts = dict(opts)

if len(args) < 2:
    print("usage: %s host:path path" % (args[0], ))
    sys.exit(1)

porcelain.clone(args[1], args[2])
