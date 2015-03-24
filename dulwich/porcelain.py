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

"""Simple wrapper that provides porcelain-like functions on top of Dulwich.

Currently implemented:
 * archive
 * add
 * branch{_create,_delete,_list}
 * clone
 * commit
 * commit-tree
 * daemon
 * diff-tree
 * fetch
 * init
 * pull
 * push
 * rm
 * receive-pack
 * reset
 * rev-list
 * tag{_create,_delete,_list}
 * upload-pack
 * update-server-info
 * status
 * symbolic-ref

These functions are meant to behave similarly to the git subcommands.
Differences in behaviour are considered bugs.
"""

__docformat__ = 'restructuredText'

from collections import namedtuple
import os
import sys
import time

from dulwich import index
from dulwich.client import get_transport_and_path
from dulwich.errors import (
    SendPackError,
    UpdateRefsError,
    )
from dulwich.index import get_unstaged_changes
from dulwich.objects import (
    Tag,
    parse_timezone,
    )
from dulwich.objectspec import parse_object
from dulwich.patch import write_tree_diff
from dulwich.protocol import Protocol
from dulwich.repo import (BaseRepo, Repo)
from dulwich.server import (
    FileSystemBackend,
    TCPGitServer,
    ReceivePackHandler,
    UploadPackHandler,
    update_server_info as server_update_server_info,
    )


# Module level tuple definition for status output
GitStatus = namedtuple('GitStatus', 'staged unstaged untracked')


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
    # TODO(jelmer): This invokes C git; this introduces a dependency.
    # Instead, dulwich should have its own archiver implementation.
    client.archive(path, committish, outstream.write, errstream.write,
                   errstream.write)


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
        r.reset_index()

    return r


def add(repo=".", paths=None):
    """Add files to the staging area.

    :param repo: Repository for the files
    :param paths: Paths to add.  No value passed stages all modified files.
    """
    # FIXME: Support patterns, directories.
    r = open_repo(repo)
    if not paths:
        # If nothing is specified, add all non-ignored files.
        paths = []
        for dirpath, dirnames, filenames in os.walk(r.path):
            # Skip .git and below.
            if '.git' in dirnames:
                dirnames.remove('.git')
            for filename in filenames:
                paths.append(os.path.join(dirpath[len(r.path)+1:], filename))
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


def print_commit(commit, outstream=sys.stdout):
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


def print_tag(tag, outstream=sys.stdout):
    """Write a human-readable tag.

    :param tag: A `Tag` object
    :param outstream: A stream to write to
    """
    outstream.write("Tagger: %s\n" % tag.tagger)
    outstream.write("Date:   %s\n" % tag.tag_time)
    outstream.write("\n")
    outstream.write("%s\n" % tag.message)
    outstream.write("\n")


def show_blob(repo, blob, outstream=sys.stdout):
    """Write a blob to a stream.

    :param repo: A `Repo` object
    :param blob: A `Blob` object
    :param outstream: A stream file to write to
    """
    outstream.write(blob.data)


def show_commit(repo, commit, outstream=sys.stdout):
    """Show a commit to a stream.

    :param repo: A `Repo` object
    :param commit: A `Commit` object
    :param outstream: Stream to write to
    """
    print_commit(commit, outstream)
    parent_commit = repo[commit.parents[0]]
    write_tree_diff(outstream, repo.object_store, parent_commit.tree, commit.tree)


def show_tree(repo, tree, outstream=sys.stdout):
    """Print a tree to a stream.

    :param repo: A `Repo` object
    :param tree: A `Tree` object
    :param outstream: Stream to write to
    """
    for n in tree:
        outstream.write("%s\n" % n)


def show_tag(repo, tag, outstream=sys.stdout):
    """Print a tag to a stream.

    :param repo: A `Repo` object
    :param tag: A `Tag` object
    :param outstream: Stream to write to
    """
    print_tag(tag, outstream)
    show_object(repo, repo[tag.object[1]], outstream)


def show_object(repo, obj, outstream):
    return {
        "tree": show_tree,
        "blob": show_blob,
        "commit": show_commit,
        "tag": show_tag,
            }[obj.type_name](repo, obj, outstream)


def log(repo=".", outstream=sys.stdout, max_entries=None):
    """Write commit logs.

    :param repo: Path to repository
    :param outstream: Stream to write log output to
    :param max_entries: Optional maximum number of entries to display
    """
    r = open_repo(repo)
    walker = r.get_walker(max_entries=max_entries)
    for entry in walker:
        print_commit(entry.commit, outstream)


def show(repo=".", objects=None, outstream=sys.stdout):
    """Print the changes in a commit.

    :param repo: Path to repository
    :param objects: Objects to show (defaults to [HEAD])
    :param outstream: Stream to write to
    """
    if objects is None:
        objects = ["HEAD"]
    if not isinstance(objects, list):
        objects = [objects]
    r = open_repo(repo)
    for objectish in objects:
        show_object(r, parse_object(r, objectish), outstream)


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


def tag(*args, **kwargs):
    import warnings
    warnings.warn(DeprecationWarning, "tag has been deprecated in favour of tag_create.")
    return tag_create(*args, **kwargs)


def tag_create(repo, tag, author=None, message=None, annotated=False,
        objectish="HEAD", tag_time=None, tag_timezone=None):
    """Creates a tag in git via dulwich calls:

    :param repo: Path to repository
    :param tag: tag string
    :param author: tag author (optional, if annotated is set)
    :param message: tag message (optional)
    :param annotated: whether to create an annotated tag
    :param objectish: object the tag should point at, defaults to HEAD
    :param tag_time: Optional time for annotated tag
    :param tag_timezone: Optional timezone for annotated tag
    """

    r = open_repo(repo)
    object = parse_object(r, objectish)

    if annotated:
        # Create the tag object
        tag_obj = Tag()
        if author is None:
            # TODO(jelmer): Don't use repo private method.
            author = r._get_user_identity()
        tag_obj.tagger = author
        tag_obj.message = message
        tag_obj.name = tag
        tag_obj.object = (type(object), object.id)
        tag_obj.tag_time = tag_time
        if tag_time is None:
            tag_time = int(time.time())
        if tag_timezone is None:
            # TODO(jelmer) Use current user timezone rather than UTC
            tag_timezone = 0
        elif isinstance(tag_timezone, str):
            tag_timezone = parse_timezone(tag_timezone)
        tag_obj.tag_timezone = tag_timezone
        r.object_store.add_object(tag_obj)
        tag_id = tag_obj.id
    else:
        tag_id = object.id

    r.refs['refs/tags/' + tag] = tag_id


def list_tags(*args, **kwargs):
    import warnings
    warnings.warn(DeprecationWarning, "list_tags has been deprecated in favour of tag_list.")
    return tag_list(*args, **kwargs)


def tag_list(repo, outstream=sys.stdout):
    """List all tags.

    :param repo: Path to repository
    :param outstream: Stream to write tags to
    """
    r = open_repo(repo)
    tags = list(r.refs.as_dict("refs/tags"))
    tags.sort()
    return tags


def tag_delete(repo, name):
    """Remove a tag.

    :param repo: Path to repository
    :param name: Name of tag to remove
    """
    r = open_repo(repo)
    if isinstance(name, str):
        names = [name]
    elif isinstance(name, list):
        names = name
    else:
        raise TypeError("Unexpected tag name type %r" % name)
    for name in names:
        del r.refs["refs/tags/" + name]


def reset(repo, mode, committish="HEAD"):
    """Reset current HEAD to the specified state.

    :param repo: Path to repository
    :param mode: Mode ("hard", "soft", "mixed")
    """

    if mode != "hard":
        raise ValueError("hard is the only mode currently supported")

    r = open_repo(repo)

    tree = r[committish].tree
    r.reset_index()


def push(repo, remote_location, refs_path,
         outstream=sys.stdout, errstream=sys.stderr):
    """Remote push with dulwich via dulwich.client

    :param repo: Path to repository
    :param remote_location: Location of the remote
    :param refs_path: relative path to the refs to push to remote
    :param outstream: A stream file to write output
    :param errstream: A stream file to write errors
    """

    # Open the repo
    r = open_repo(repo)

    # Get the client and path
    client, path = get_transport_and_path(remote_location)

    def update_refs(refs):
        new_refs = r.get_refs()
        refs[refs_path] = new_refs['HEAD']
        del new_refs['HEAD']
        return refs

    try:
        client.send_pack(path, update_refs,
            r.object_store.generate_pack_contents, progress=errstream.write)
        outstream.write("Push to %s successful.\n" % remote_location)
    except (UpdateRefsError, SendPackError) as e:
        outstream.write("Push to %s failed.\n" % remote_location)
        errstream.write("Push to %s failed -> '%s'\n" % e.message)


def pull(repo, remote_location, refs_path,
         outstream=sys.stdout, errstream=sys.stderr):
    """Pull from remote via dulwich.client

    :param repo: Path to repository
    :param remote_location: Location of the remote
    :param refs_path: relative path to the fetched refs
    :param outstream: A stream file to write to output
    :param errstream: A stream file to write to errors
    """

    # Open the repo
    r = open_repo(repo)

    client, path = get_transport_and_path(remote_location)
    remote_refs = client.fetch(path, r, progress=errstream.write)
    r['HEAD'] = remote_refs[refs_path]

    # Perform 'git checkout .' - syncs staged changes
    tree = r["HEAD"].tree
    r.reset_index()


def status(repo="."):
    """Returns staged, unstaged, and untracked changes relative to the HEAD.

    :param repo: Path to repository or repository object
    :return: GitStatus tuple,
        staged -    list of staged paths (diff index/HEAD)
        unstaged -  list of unstaged paths (diff index/working-tree)
        untracked - list of untracked, un-ignored & non-.git paths
    """
    r = open_repo(repo)

    # 1. Get status of staged
    tracked_changes = get_tree_changes(r)
    # 2. Get status of unstaged
    unstaged_changes = list(get_unstaged_changes(r.open_index(), r.path))
    # TODO - Status of untracked - add untracked changes, need gitignore.
    untracked_changes = []
    return GitStatus(tracked_changes, unstaged_changes, untracked_changes)


def get_tree_changes(repo):
    """Return add/delete/modify changes to tree by comparing index to HEAD.

    :param repo: repo path or object
    :return: dict with lists for each type of change
    """
    r = open_repo(repo)
    index = r.open_index()

    # Compares the Index to the HEAD & determines changes
    # Iterate through the changes and report add/delete/modify
    # TODO: call out to dulwich.diff_tree somehow.
    tracked_changes = {
        'add': [],
        'delete': [],
        'modify': [],
    }
    for change in index.changes_from_tree(r.object_store, r['HEAD'].tree):
        if not change[0][0]:
            tracked_changes['add'].append(change[0][1])
        elif not change[0][1]:
            tracked_changes['delete'].append(change[0][0])
        elif change[0][0] == change[0][1]:
            tracked_changes['modify'].append(change[0][0])
        else:
            raise AssertionError('git mv ops not yet supported')
    return tracked_changes


def daemon(path=".", address=None, port=None):
    """Run a daemon serving Git requests over TCP/IP.

    :param path: Path to the directory to serve.
    :param address: Optional address to listen on (defaults to ::)
    :param port: Optional port to listen on (defaults to TCP_GIT_PORT)
    """
    # TODO(jelmer): Support git-daemon-export-ok and --export-all.
    backend = FileSystemBackend(path)
    server = TCPGitServer(backend, address, port)
    server.serve_forever()


def web_daemon(path=".", address=None, port=None):
    """Run a daemon serving Git requests over HTTP.

    :param path: Path to the directory to serve
    :param address: Optional address to listen on (defaults to ::)
    :param port: Optional port to listen on (defaults to 80)
    """
    from dulwich.web import (
        make_wsgi_chain,
        make_server,
        WSGIRequestHandlerLogger,
        WSGIServerLogger)

    backend = FileSystemBackend(path)
    app = make_wsgi_chain(backend)
    server = make_server(address, port, app,
                         handler_class=WSGIRequestHandlerLogger,
                         server_class=WSGIServerLogger)
    server.serve_forever()


def upload_pack(path=".", inf=sys.stdin, outf=sys.stdout):
    """Upload a pack file after negotiating its contents using smart protocol.

    :param path: Path to the repository
    :param inf: Input stream to communicate with client
    :param outf: Output stream to communicate with client
    """
    backend = FileSystemBackend()
    def send_fn(data):
        outf.write(data)
        outf.flush()
    proto = Protocol(inf.read, send_fn)
    handler = UploadPackHandler(backend, [path], proto)
    # FIXME: Catch exceptions and write a single-line summary to outf.
    handler.handle()
    return 0


def receive_pack(path=".", inf=sys.stdin, outf=sys.stdout):
    """Receive a pack file after negotiating its contents using smart protocol.

    :param path: Path to the repository
    :param inf: Input stream to communicate with client
    :param outf: Output stream to communicate with client
    """
    backend = FileSystemBackend()
    def send_fn(data):
        outf.write(data)
        outf.flush()
    proto = Protocol(inf.read, send_fn)
    handler = ReceivePackHandler(backend, [path], proto)
    # FIXME: Catch exceptions and write a single-line summary to outf.
    handler.handle()
    return 0


def branch_delete(repo, name):
    """Delete a branch.

    :param repo: Path to the repository
    :param name: Name of the branch
    """
    r = open_repo(repo)
    if isinstance(name, str):
        names = [name]
    elif isinstance(name, list):
        names = name
    else:
        raise TypeError("Unexpected branch name type %r" % name)
    for name in names:
        del r.refs["refs/heads/" + name]


def branch_create(repo, name, objectish=None, force=False):
    """Create a branch.

    :param repo: Path to the repository
    :param name: Name of the new branch
    :param objectish: Target object to point new branch at (defaults to HEAD)
    :param force: Force creation of branch, even if it already exists
    """
    r = open_repo(repo)
    if isinstance(name, str):
        names = [name]
    elif isinstance(name, list):
        names = name
    else:
        raise TypeError("Unexpected branch name type %r" % name)
    if objectish is None:
        objectish = "HEAD"
    object = parse_object(r, objectish)
    refname = "refs/heads/" + name
    if refname in r.refs and not force:
        raise KeyError("Branch with name %s already exists." % name)
    r.refs[refname] = object.id


def branch_list(repo):
    """List all branches.

    :param repo: Path to the repository
    """
    r = open_repo(repo)
    return r.refs.keys(base="refs/heads/")


def fetch(repo, remote_location, outstream=sys.stdout, errstream=sys.stderr):
    """Fetch objects from a remote server.

    :param repo: Path to the repository
    :param remote_location: String identifying a remote server
    :param outstream: Output stream (defaults to stdout)
    :param errstream: Error stream (defaults to stderr)
    :return: Dictionary with refs on the remote
    """
    r = open_repo(repo)
    client, path = get_transport_and_path(remote_location)
    remote_refs = client.fetch(path, r, progress=errstream.write)
    return remote_refs
