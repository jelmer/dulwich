# hooks.py -- for dealing with git hooks
# Copyright (C) 2012-2013 Jelmer Vernooij and others.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License or (at your option) a later version of the License.
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

"""Access to hooks."""

import os
import subprocess
import tempfile

from dulwich.errors import (
    HookError,
)


class Hook(object):
    """Generic hook object."""

    def execute(self, *args):
        """Execute the hook with the given args

        :param args: argument list to hook
        :raise HookError: hook execution failure
        :return: a hook may return a useful value
        """
        raise NotImplementedError(self.execute)


class ShellHook(Hook):
    """Hook by executable file

    Implements standard githooks(5) [0]:

    [0] http://www.kernel.org/pub/software/scm/git/docs/githooks.html
    """

    def __init__(self, name, path, numparam,
                 pre_exec_callback=None, post_exec_callback=None):
        """Setup shell hook definition

        :param name: name of hook for error messages
        :param path: absolute path to executable file
        :param numparam: number of requirements parameters
        :param pre_exec_callback: closure for setup before execution
            Defaults to None. Takes in the variable argument list from the
            execute functions and returns a modified argument list for the
            shell hook.
        :param post_exec_callback: closure for cleanup after execution
            Defaults to None. Takes in a boolean for hook success and the
            modified argument list and returns the final hook return value
            if applicable
        """
        self.name = name
        self.filepath = path
        self.numparam = numparam

        self.pre_exec_callback = pre_exec_callback
        self.post_exec_callback = post_exec_callback

    def execute(self, *args):
        """Execute the hook with given args"""

        if len(args) != self.numparam:
            raise HookError("Hook %s executed with wrong number of args. \
                            Expected %d. Saw %d. %s"
                            % (self.name, self.numparam, len(args)))

        if (self.pre_exec_callback is not None):
            args = self.pre_exec_callback(*args)

        try:
            ret = subprocess.call([self.filepath] + list(args))
            if ret != 0:
                if (self.post_exec_callback is not None):
                    self.post_exec_callback(0, *args)
                raise HookError("Hook %s exited with non-zero status"
                                % (self.name))
            if (self.post_exec_callback is not None):
                return self.post_exec_callback(1, *args)
        except OSError:  # no file. silent failure.
            if (self.post_exec_callback is not None):
                self.post_exec_callback(0, *args)


class PreCommitShellHook(ShellHook):
    """pre-commit shell hook"""

    def __init__(self, controldir):
        filepath = os.path.join(controldir, 'hooks', 'pre-commit')

        ShellHook.__init__(self, 'pre-commit', filepath, 0)


class PostCommitShellHook(ShellHook):
    """post-commit shell hook"""

    def __init__(self, controldir):
        filepath = os.path.join(controldir, 'hooks', 'post-commit')

        ShellHook.__init__(self, 'post-commit', filepath, 0)


class CommitMsgShellHook(ShellHook):
    """commit-msg shell hook

    :param args[0]: commit message
    :return: new commit message or None
    """

    def __init__(self, controldir):
        filepath = os.path.join(controldir, 'hooks', 'commit-msg')

        def prepare_msg(*args):
            (fd, path) = tempfile.mkstemp()

            f = os.fdopen(fd, 'wb')
            try:
                f.write(args[0])
            finally:
                f.close()

            return (path,)

        def clean_msg(success, *args):
            if success:
                f = open(args[0], 'rb')
                try:
                    new_msg = f.read()
                finally:
                    f.close()
                os.unlink(args[0])
                return new_msg
            os.unlink(args[0])

        ShellHook.__init__(self, 'commit-msg', filepath, 1,
                           prepare_msg, clean_msg)
