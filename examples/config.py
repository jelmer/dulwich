#!/usr/bin/python
# Read the config file for a git repository.
#
# Example usage:
#  python examples/config.py

from dulwich.repo import Repo

repo = Repo(".")
config = repo.get_config()

print(config.get("core", "filemode"))
print(config.get(("remote", "origin"), "url"))
