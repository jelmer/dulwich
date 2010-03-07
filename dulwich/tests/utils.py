# utils.py -- Test utilities for Dulwich.
# Copyright (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) any later version of
# the License.
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

"""Utility functions common to Dulwich tests."""


import os
import shutil
import tempfile

from dulwich.repo import Repo


def open_repo(name):
    """Open a copy of a repo in a temporary directory.

    Use this function for accessing repos in dulwich/tests/data/repos to avoid
    accidentally or intentionally modifying those repos in place. Use
    tear_down_repo to delete any temp files created.

    :param name: The name of the repository, relative to
        dulwich/tests/data/repos
    :returns: An initialized Repo object that lives in a temporary directory.
    """
    temp_dir = tempfile.mkdtemp()
    repo_dir = os.path.join(os.path.dirname(__file__), 'data', 'repos', name)
    temp_repo_dir = os.path.join(temp_dir, name)
    shutil.copytree(repo_dir, temp_repo_dir, symlinks=True)
    return Repo(temp_repo_dir)


def tear_down_repo(repo):
    """Tear down a test repository."""
    temp_dir = os.path.dirname(repo.path.rstrip(os.sep))
    shutil.rmtree(temp_dir)
