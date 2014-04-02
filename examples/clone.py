#!/usr/bin/python
# This trivial script demonstrates how to clone a remote repository.
#
# Example usage:
#  python examples/clone.py git://github.com/jelmer/dulwich dulwich-clone

import sys
from getopt import getopt
from dulwich.repo import Repo
from dulwich.client import get_transport_and_path

opts, args = getopt(sys.argv, "", [])
opts = dict(opts)

if len(args) < 2:
    print("usage: %s host:path path" % (args[0], ))
    sys.exit(1)

# Connect to the remote repository
client, host_path = get_transport_and_path(args[1])
path = args[2]

# Create the local repository
r = Repo.init(path, mkdir=True)

# Fetch the remote objects
remote_refs = client.fetch(host_path, r,
    determine_wants=r.object_store.determine_wants_all,
    progress=sys.stdout.write)

# Update the local head to point at the right object
r["HEAD"] = remote_refs["HEAD"]

r._build_tree()
