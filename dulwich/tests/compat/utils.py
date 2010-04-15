# utils.py -- Git compatibility utilities
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

"""Utilities for interacting with cgit."""

import os
import subprocess
import tempfile
import unittest

from dulwich.repo import Repo

from dulwich.tests import (
    TestSkipped,
    )

_DEFAULT_GIT = 'git'


def git_version(git_path=_DEFAULT_GIT):
    """Attempt to determine the version of git currently installed.

    :param git_path: Path to the git executable; defaults to the version in
        the system path.
    :return: A tuple of ints of the form (major, minor, point), or None if no
        git installation was found.
    """
    try:
        _, output = run_git(['--version'], git_path=git_path,
                            capture_stdout=True)
    except OSError:
        return None
    version_prefix = 'git version '
    if not output.startswith(version_prefix):
        return None
    output = output[len(version_prefix):]
    nums = output.split('.')
    if len(nums) == 2:
        nums.add('0')
    else:
        nums = nums[:3]
    try:
        return tuple(int(x) for x in nums)
    except ValueError:
        return None


def require_git_version(required_version, git_path=_DEFAULT_GIT):
    """Require git version >= version, or skip the calling test."""
    found_version = git_version(git_path=git_path)
    if found_version < required_version:
        required_version = '.'.join(map(str, required_version))
        found_version = '.'.join(map(str, found_version))
        raise TestSkipped('Test requires git >= %s, found %s' %
                         (required_version, found_version))


def run_git(args, git_path=_DEFAULT_GIT, input=None, capture_stdout=False,
            **popen_kwargs):
    """Run a git command.

    Input is piped from the input parameter and output is sent to the standard
    streams, unless capture_stdout is set.

    :param args: A list of args to the git command.
    :param git_path: Path to to the git executable.
    :param input: Input data to be sent to stdin.
    :param capture_stdout: Whether to capture and return stdout.
    :param popen_kwargs: Additional kwargs for subprocess.Popen;
        stdin/stdout args are ignored.
    :return: A tuple of (returncode, stdout contents). If capture_stdout is
        False, None will be returned as stdout contents.
    :raise OSError: if the git executable was not found.
    """
    args = [git_path] + args
    popen_kwargs['stdin'] = subprocess.PIPE
    if capture_stdout:
        popen_kwargs['stdout'] = subprocess.PIPE
    else:
        popen_kwargs.pop('stdout', None)
    p = subprocess.Popen(args, **popen_kwargs)
    stdout, stderr = p.communicate(input=input)
    return (p.returncode, stdout)


def run_git_or_fail(args, git_path=_DEFAULT_GIT, input=None, **popen_kwargs):
    """Run a git command, capture stdout/stderr, and fail if git fails."""
    popen_kwargs['stderr'] = subprocess.STDOUT
    returncode, stdout = run_git(args, git_path=git_path, input=input,
                                 capture_stdout=True, **popen_kwargs)
    assert returncode == 0
    return stdout


def import_repo(name):
    """Import a repo from a fast-export file in a temporary directory.

    These are used rather than binary repos for compat tests because they are
    more compact an human-editable, and we already depend on git.

    :param name: The name of the repository export file, relative to
        dulwich/tests/data/repos
    :returns: An initialized Repo object that lives in a temporary directory.
    """
    temp_dir = tempfile.mkdtemp()
    export_path = os.path.join(os.path.dirname(__file__), os.pardir, 'data',
                               'repos', name)
    temp_repo_dir = os.path.join(temp_dir, name)
    export_file = open(export_path, 'rb')
    run_git_or_fail(['init', '--bare', temp_repo_dir])
    run_git_or_fail(['fast-import'], input=export_file.read(),
                    cwd=temp_repo_dir)
    export_file.close()
    return Repo(temp_repo_dir)


class CompatTestCase(unittest.TestCase):
    """Test case that requires git for compatibility checks.

    Subclasses can change the git version required by overriding
    min_git_version.
    """

    min_git_version = (1, 5, 0)

    def setUp(self):
        require_git_version(self.min_git_version)
