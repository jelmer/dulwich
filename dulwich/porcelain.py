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
from dulwich.repo import Repo
from dulwich.server import update_server_info as server_update_server_info

"""Simple wrapper that provides porcelain-like functions on top of Dulwich.

Currently implemented:
 * archive

"""

__docformat__ = 'restructuredText'


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


def update_server_info(path="."):
    """Update server info files for a repository.

    :param path: path to the repository
    """
    r = Repo(path)
    server_update_server_info(r)


def commit(path=".", message=None):
    """Create a new commit.

    :param path: Path to repository
    :param message: Optional commit message
    """
    # FIXME: Support --all argument
    # FIXME: Support --signoff argument
    r = Repo(path)
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
