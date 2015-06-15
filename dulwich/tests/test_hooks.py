# test_hooks.py -- Tests for executing hooks
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

"""Tests for executing hooks."""

import os
import stat
import shutil
import sys
import tempfile

from dulwich import errors

from dulwich.hooks import (
    PreCommitShellHook,
    PostCommitShellHook,
    CommitMsgShellHook,
)

from dulwich.tests import TestCase


class ShellHookTests(TestCase):

    def setUp(self):
        super(ShellHookTests, self).setUp()
        if os.name != 'posix':
            self.skipTest('shell hook tests requires POSIX shell')

    def test_hook_pre_commit(self):
        pre_commit_fail = """#!/bin/sh
exit 1
"""

        pre_commit_success = """#!/bin/sh
exit 0
"""

        repo_dir = os.path.join(tempfile.mkdtemp())
        os.mkdir(os.path.join(repo_dir, 'hooks'))
        self.addCleanup(shutil.rmtree, repo_dir)

        pre_commit = os.path.join(repo_dir, 'hooks', 'pre-commit')
        hook = PreCommitShellHook(repo_dir)

        with open(pre_commit, 'w') as f:
            f.write(pre_commit_fail)
        os.chmod(pre_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        self.assertRaises(errors.HookError, hook.execute)

        with open(pre_commit, 'w') as f:
            f.write(pre_commit_success)
        os.chmod(pre_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        hook.execute()

    def test_hook_commit_msg(self):

        commit_msg_fail = """#!/bin/sh
exit 1
"""

        commit_msg_success = """#!/bin/sh
exit 0
"""

        repo_dir = os.path.join(tempfile.mkdtemp())
        os.mkdir(os.path.join(repo_dir, 'hooks'))
        self.addCleanup(shutil.rmtree, repo_dir)

        commit_msg = os.path.join(repo_dir, 'hooks', 'commit-msg')
        hook = CommitMsgShellHook(repo_dir)

        with open(commit_msg, 'w') as f:
            f.write(commit_msg_fail)
        os.chmod(commit_msg, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        self.assertRaises(errors.HookError, hook.execute, b'failed commit')

        with open(commit_msg, 'w') as f:
            f.write(commit_msg_success)
        os.chmod(commit_msg, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        hook.execute(b'empty commit')

    def test_hook_post_commit(self):

        (fd, path) = tempfile.mkstemp()
        os.close(fd)

        post_commit_msg = """#!/bin/sh
rm """ + path + "\n"

        post_commit_msg_fail = """#!/bin/sh
exit 1
"""

        repo_dir = os.path.join(tempfile.mkdtemp())
        os.mkdir(os.path.join(repo_dir, 'hooks'))
        self.addCleanup(shutil.rmtree, repo_dir)

        post_commit = os.path.join(repo_dir, 'hooks', 'post-commit')
        hook = PostCommitShellHook(repo_dir)

        with open(post_commit, 'w') as f:
            f.write(post_commit_msg_fail)
        os.chmod(post_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        self.assertRaises(errors.HookError, hook.execute)

        with open(post_commit, 'w') as f:
            f.write(post_commit_msg)
        os.chmod(post_commit, stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)

        hook.execute()
        self.assertFalse(os.path.exists(path))
