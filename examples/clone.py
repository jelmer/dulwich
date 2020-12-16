"""Clone.

This trivial script demonstrates how to clone a remote repository.

Example usage:
    2. python examples/clone.py git://github.com/jelmer/dulwich.git dulwich
    1. python examples/clone.py git://github.com/jelmer/dulwich.git
"""


import sys

from os.path import basename

from getopt import getopt

from dulwich import porcelain


_, args = getopt(sys.argv, "", [])


if len(args) < 2:
    print("usage: %s host:path path" % (args[0], ))
    sys.exit(1)

elif len(args) < 3:
    repository = basename(args[1].split(":")[-1])
    if repository[-4:] == ".git":
        args.append(repository[:-4])
    else:
        args.append(repository)

    porcelain.clone(args[1], args[2])

else:
    porcelain.clone(args[1], args[2])
