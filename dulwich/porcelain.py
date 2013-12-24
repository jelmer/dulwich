# porcelain.py -- Porcelain-like layer on top of Dulwich
# Copyright (C) 2013 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

import os
import sys

from time import time
from re import search
from collections import OrderedDict

from dulwich import index
from dulwich.client import get_transport_and_path
from dulwich.patch import write_tree_diff
from dulwich.repo import (BaseRepo, Repo)
from dulwich.server import update_server_info as server_update_server_info
from dulwich.objects import Tag, Commit, parse_timezone
from dulwich.diff_tree import tree_changes

"""Simple wrapper that provides porcelain-like functions on top of Dulwich.

Currently implemented:
 * archive
 * add
 * clone
 * commit
 * commit-tree
 * diff-tree
 * init
 * remove
 * rev-list
 * update-server-info
 * symbolic-ref
 * tag
 * status
 * stage-files
 * return-tags
 * reset-hard-head

These functions are meant to behave similarly to the git subcommands.
Differences in behaviour are considered bugs.
"""

__docformat__ = 'restructuredText'


def open_repo(path_or_repo):
    """Open an argument that can be a repository or a path for a repository."""
    if isinstance(path_or_repo, BaseRepo):
        return path_or_repo
    return Repo(path_or_repo)


def archive(location, committish=None, outstream=sys.stdout,
            errstream=sys.stderr):
    """Create an archive.

    :param location: Location of repository for which to generate an archive.
    :param committish: Commit SHA1 or ref to use
    :param outstream: Output stream (defaults to stdout)
    :param errstream: Error stream (defaults to stderr)
    """

    client, path = get_transport_and_path(location)
    if committish is None:
        committish = "HEAD"
    client.archive(path, committish, outstream.write, errstream.write)


def update_server_info(repo="."):
    """Update server info files for a repository.

    :param repo: path to the repository
    """
    r = open_repo(repo)
    server_update_server_info(r)


def symbolic_ref(repo, ref_name, force=False):
    """Set git symbolic ref into HEAD.

    :param repo: path to the repository
    :param ref_name: short name of the new ref
    :param force: force settings without checking if it exists in refs/heads
    """
    repo_obj = open_repo(repo)
    ref_path = 'refs/heads/%s' % ref_name
    if not force and ref_path not in repo_obj.refs.keys():
        raise ValueError('fatal: ref `%s` is not a ref' % ref_name)
    repo_obj.refs.set_symbolic_ref('HEAD', ref_path)


def commit(repo=".", message=None, author=None, committer=None):
    """Create a new commit.

    :param repo: Path to repository
    :param message: Optional commit message
    :param author: Optional author name and email
    :param committer: Optional committer name and email
    :return: SHA1 of the new commit
    """
    # FIXME: Support --all argument
    # FIXME: Support --signoff argument
    r = open_repo(repo)
    return r.do_commit(message=message, author=author,
        committer=committer)


def commit_tree(repo, tree, message=None, author=None, committer=None):
    """Create a new commit object.

    :param repo: Path to repository
    :param tree: An existing tree object
    :param author: Optional author name and email
    :param committer: Optional committer name and email
    """
    r = open_repo(repo)
    return r.do_commit(message=message, tree=tree, committer=committer,
            author=author)


def init(path=".", bare=False):
    """Create a new git repository.

    :param path: Path to repository.
    :param bare: Whether to create a bare repository.
    :return: A Repo instance
    """
    if not os.path.exists(path):
        os.mkdir(path)

    if bare:
        return Repo.init_bare(path)
    else:
        return Repo.init(path)


def clone(source, target=None, bare=False, checkout=None, outstream=sys.stdout):
    """Clone a local or remote git repository.

    :param source: Path or URL for source repository
    :param target: Path to target repository (optional)
    :param bare: Whether or not to create a bare repository
    :param outstream: Optional stream to write progress to
    :return: The new repository
    """
    if checkout is None:
        checkout = (not bare)
    if checkout and bare:
        raise ValueError("checkout and bare are incompatible")
    client, host_path = get_transport_and_path(source)

    if target is None:
        target = host_path.split("/")[-1]

    if not os.path.exists(target):
        os.mkdir(target)
    if bare:
        r = Repo.init_bare(target)
    else:
        r = Repo.init(target)
    remote_refs = client.fetch(host_path, r,
        determine_wants=r.object_store.determine_wants_all,
        progress=outstream.write)
    r["HEAD"] = remote_refs["HEAD"]
    if checkout:
        outstream.write('Checking out HEAD')
        index.build_index_from_tree(r.path, r.index_path(),
                                    r.object_store, r["HEAD"].tree)

    return r


def add(repo=".", paths=None):
    """Add files to the staging area.

    :param repo: Repository for the files
    :param paths: Paths to add
    """
    # FIXME: Support patterns, directories, no argument.
    r = open_repo(repo)
    r.stage(paths)


def rm(repo=".", paths=None):
    """Remove files from the staging area.

    :param repo: Repository for the files
    :param paths: Paths to remove
    """
    r = open_repo(repo)
    index = r.open_index()
    for p in paths:
        del index[p]
    index.write()


def print_commit(commit, outstream):
    """Write a human-readable commit log entry.

    :param commit: A `Commit` object
    :param outstream: A stream file to write to
    """
    outstream.write("-" * 50 + "\n")
    outstream.write("commit: %s\n" % commit.id)
    if len(commit.parents) > 1:
        outstream.write("merge: %s\n" % "...".join(commit.parents[1:]))
    outstream.write("author: %s\n" % commit.author)
    outstream.write("committer: %s\n" % commit.committer)
    outstream.write("\n")
    outstream.write(commit.message + "\n")
    outstream.write("\n")


def log(repo=".", outstream=sys.stdout):
    """Write commit logs.

    :param repo: Path to repository
    :param outstream: Stream to write log output to
    """
    r = open_repo(repo)
    walker = r.get_walker()
    for entry in walker:
        print_commit(entry.commit, outstream)


def show(repo=".", committish=None, outstream=sys.stdout):
    """Print the changes in a commit.

    :param repo: Path to repository
    :param committish: Commit to write
    :param outstream: Stream to write to
    """
    if committish is None:
        committish = "HEAD"
    r = open_repo(repo)
    commit = r[committish]
    parent_commit = r[commit.parents[0]]
    print_commit(commit, outstream)
    write_tree_diff(outstream, r.object_store, parent_commit.tree, commit.tree)


def diff_tree(repo, old_tree, new_tree, outstream=sys.stdout):
    """Compares the content and mode of blobs found via two tree objects.

    :param repo: Path to repository
    :param old_tree: Id of old tree
    :param new_tree: Id of new tree
    :param outstream: Stream to write to
    """
    r = open_repo(repo)
    write_tree_diff(outstream, r.object_store, old_tree, new_tree)


def rev_list(repo, commits, outstream=sys.stdout):
    """Lists commit objects in reverse chronological order.

    :param repo: Path to repository
    :param commits: Commits over which to iterate
    :param outstream: Stream to write to
    """
    r = open_repo(repo)
    for entry in r.get_walker(include=[r[c].id for c in commits]):
        outstream.write("%s\n" % entry.commit.id)


def tag(repo, tag, author, message):
    """Creates a tag in git via dulwich calls:

    :param repo: Path to repository
    :param tag: tag string
    :param author: tag author
    :param repo: tag message
    """

    r = open_repo(repo)

    # Create the tag object
    tag_obj = Tag()
    tag_obj.tagger = author
    tag_obj.message = message
    tag_obj.name = tag
    tag_obj.object = (Commit, repo.refs['HEAD'])
    tag_obj.tag_time = int(time())
    tag_obj.tag_timezone = parse_timezone('-0200')[0]

    # Add tag to the object store
    r.object_store.add_object(tag_obj)
    r['refs/tags/' + tag] = tag_obj.id


def status(repo, outstream=sys.stdout):
    """Return the git status

    :param repo: Path to repository
    """
    r = open_repo(repo)

    index = r.open_index()
    changes = list(tree_changes(r, index.commit(r.object_store),
                                r['HEAD'].tree))

    for change in changes:
        outstream.write("%s\n" % change.__str__())

    return changes


def stage_files(repo):
    """Stage modified files in the repo

    :param repo: Path to repository
    """
    r = Repo(repo)

    # Iterate through files, those modified will be staged
    for elem in os.walk(r):
        relative_path = elem[0].split('./')[-1]
        if not search(r'\.git', elem[0]):
            files = [relative_path + '/' +
                     filename for filename in elem[2]]
            r.stage(files)


def return_tags(repo, outstream=sys.stdout):
    """Get all tags & corresponding commit shas

    :param repo: Path to repository
    """
    r = Repo(repo)
    tags = r.refs.as_dict("refs/tags")
    ordered_tags = {}

    # Get the commit hashes associated with the tags
    for tag, tag_commit in tags.items():
        if tag not in ordered_tags:
            ordered_tags[tag] = r.object_store.peel_sha(tag_commit)
    # Sort by commit_time, then by tag name, as multiple tags can have
    # the same commit_time for their commits
    ordered_tags = OrderedDict(sorted(ordered_tags.items(),
                                      key=lambda t: (t[1].commit_time, t)))

    # TODO - ensure that the tags write in order
    for key in ordered_tags.keys():
        outstream.write("%s\n" % key)

    return ordered_tags


def reset_hard_head(repo):
    """ Perform 'git checkout .' - syncs staged changes.  This is a useful way
    to 'git reset --hard  HEAD'

    :param repo: Path to repository
    """

    r = Repo(repo)

    indexfile = r.index_path()
    tree = r["HEAD"].tree
    index.build_index_from_tree(r.path, indexfile, r.object_store, tree)