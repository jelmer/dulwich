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


import datetime
import os
import shutil
import tempfile
import time

from dulwich.objects import Commit
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


def make_object(cls, **attrs):
    """Make an object for testing and assign some members.

    This method creates a new subclass to allow arbitrary attribute
    reassignment, which is not otherwise possible with objects having __slots__.

    :param attrs: dict of attributes to set on the new object.
    :return: A newly initialized object of type cls.
    """

    class TestObject(cls):
        """Class that inherits from the given class, but without __slots__.

        Note that classes with __slots__ can't have arbitrary attributes monkey-
        patched in, so this is a class that is exactly the same only with a
        __dict__ instead of __slots__.
        """
        pass

    obj = TestObject()
    for name, value in attrs.iteritems():
        setattr(obj, name, value)
    return obj


def make_commit(**attrs):
    """Make a Commit object with a default set of members.

    :param attrs: dict of attributes to overwrite from the default values.
    :return: A newly initialized Commit object.
    """
    default_time = int(time.mktime(datetime.datetime(2010, 1, 1).timetuple()))
    all_attrs = {'author': 'Test Author <test@nodomain.com>',
                 'author_time': default_time,
                 'author_timezone': 0,
                 'committer': 'Test Committer <test@nodomain.com>',
                 'commit_time': default_time,
                 'commit_timezone': 0,
                 'message': 'Test message.',
                 'parents': [],
                 'tree': '0' * 40}
    all_attrs.update(attrs)
    return make_object(Commit, **all_attrs)
