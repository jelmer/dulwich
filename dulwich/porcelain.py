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

from dulwich.client import get_transport_and_path
from dulwich.repo import (BaseRepo, Repo)
from dulwich.server import update_server_info as server_update_server_info

"""Simple wrapper that provides porcelain-like functions on top of Dulwich.

Currently implemented:
 * archive

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


def commit(repo=".", message=None):
    """Create a new commit.

    :param repo: Path to repository
    :param message: Optional commit message
    """
    # FIXME: Support --all argument
    # FIXME: Support --signoff argument
    r = open_repo(repo)
    r.do_commit(message=message)


def init(path=".", bare=False):
    """Create a new git repository.

    :param path: Path to repository.
    :param bare: Whether to create a bare repository.
    """
    if not os.path.exists(path):
        os.mkdir(path)

    if bare:
        Repo.init_bare(path)
    else:
        Repo.init(path)


def clone(source, target=None, bare=False, outstream=sys.stdout):
    """Clone a local or remote git repository.

    :param source: Path or URL for source repository
    :param target: Path to target repository (optional)
    :param bare: Whether or not to create a bare repository
    :param outstream: Optional stream to write progress to
    """
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
