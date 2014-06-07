# __init__.py -- The tests for dulwich
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
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

"""Tests for Dulwich."""

import doctest
import os
import shutil
import subprocess
import sys
import tempfile


# If Python itself provides an exception, use that
import unittest
from unittest import TestCase as _TestCase


def get_safe_env(env=None):
    """Returns the environment "env" (or a copy of "os.environ" by default)
    modified to avoid side-effects caused by user's ~/.gitconfig"""

    if env is None:
        env = os.environ.copy()
    # On Windows it's not enough to set "HOME" to a non-existing
    # directory. Git.cmd takes the first existing directory out of
    # "%HOME%", "%HOMEDRIVE%%HOMEPATH%" and "%USERPROFILE%".
    for e in 'HOME', 'HOMEPATH', 'USERPROFILE':
        env[e] = '/nosuchdir'
    return env


class TestCase(_TestCase):

    def makeSafeEnv(self):
        """Create environment with homedirectory-related variables stripped.

        Modifies os.environ for the duration of a test case to avoid
        side-effects caused by the user's ~/.gitconfig and other
        files in their home directory.
        """
        old_env = os.environ
        def restore():
            os.environ = old_env
        self.addCleanup(restore)
        new_env = dict(os.environ)
        for e in ['HOME', 'HOMEPATH', 'USERPROFILE']:
            new_env[e] = '/nosuchdir'
        os.environ = new_env

    def setUp(self):
        super(TestCase, self).setUp()
        self.makeSafeEnv()


class BlackboxTestCase(TestCase):
    """Blackbox testing."""

    bin_directory = os.path.abspath(os.path.join(os.path.dirname(__file__),
        "..", "..", "bin"))

    def bin_path(self, name):
        """Determine the full path of a binary.

        :param name: Name of the script
        :return: Full path
        """
        return os.path.join(self.bin_directory, name)

    def run_command(self, name, args):
        """Run a Dulwich command.

        :param name: Name of the command, as it exists in bin/
        :param args: Arguments to the command
        """
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(sys.path)

        # Since they don't have any extensions, Windows can't recognize
        # executablility of the Python files in /bin. Even then, we'd have to
        # expect the user to set up file associations for .py files.
        #
        # Save us from all that headache and call python with the bin script.
        argv = [sys.executable, self.bin_path(name)] + args
        return subprocess.Popen(argv,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True,
            env=env)


def self_test_suite():
    names = [
        'blackbox',
        'client',
        'config',
        'diff_tree',
        'fastexport',
        'file',
        'grafts',
        'greenthreads',
        'hooks',
        'index',
        'lru_cache',
        'objects',
        'objectspec',
        'object_store',
        'missing_obj_finder',
        'pack',
        'patch',
        'porcelain',
        'protocol',
        'refs',
        'repository',
        'server',
        'walk',
        'web',
        ]
    module_names = ['dulwich.tests.test_' + name for name in names]
    loader = unittest.TestLoader()
    return loader.loadTestsFromNames(module_names)


def tutorial_test_suite():
    tutorial = [
        'introduction',
        'file-format',
        'repo',
        'object-store',
        'remote',
        'conclusion',
        ]
    tutorial_files = ["../../docs/tutorial/%s.txt" % name for name in tutorial]
    def setup(test):
        test.__old_cwd = os.getcwd()
        test.__dulwich_tempdir = tempfile.mkdtemp()
        os.chdir(test.__dulwich_tempdir)
    def teardown(test):
        os.chdir(test.__old_cwd)
        shutil.rmtree(test.__dulwich_tempdir)
    return doctest.DocFileSuite(setUp=setup, tearDown=teardown,
        *tutorial_files)


def nocompat_test_suite():
    result = unittest.TestSuite()
    result.addTests(self_test_suite())
    from dulwich.contrib import test_suite as contrib_test_suite
    result.addTests(tutorial_test_suite())
    result.addTests(contrib_test_suite())
    return result


def compat_test_suite():
    result = unittest.TestSuite()
    from dulwich.tests.compat import test_suite as compat_test_suite
    result.addTests(compat_test_suite())
    return result


def test_suite():
    result = unittest.TestSuite()
    result.addTests(self_test_suite())
    result.addTests(tutorial_test_suite())
    from dulwich.tests.compat import test_suite as compat_test_suite
    result.addTests(compat_test_suite())
    from dulwich.contrib import test_suite as contrib_test_suite
    result.addTests(contrib_test_suite())
    return result
