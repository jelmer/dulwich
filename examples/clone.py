"""Clone.

This trivial script demonstrates how to clone or lock a remote repository.

Example usage:
  1. python examples/clone.py git://github.com/jelmer/dulwich
  2. python examples/clone.py git://github.com/jelmer/dulwich.git
  3. python examples/clone.py git://github.com/jelmer/dulwich.git dulwich
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
    target_path = basename(args[1].split(":")[-1])
    if target_path[-4:] == ".git":
        target_path = target_path[:-4]
else:
    target_path = args[2]

porcelain.clone(args[1], target_path)
