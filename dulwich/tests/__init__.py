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
import unittest
import shutil
import subprocess
import sys
import tempfile

try:
    from testtools.testcase import TestCase
except ImportError:
    from unittest import TestCase

try:
    # If Python itself provides an exception, use that
    from unittest import SkipTest as TestSkipped
except ImportError:
    # Check if the nose exception can be used
    try:
        import nose
    except ImportError:
        try:
            import testtools.testcase
        except ImportError:
            class TestSkipped(Exception):
                def __init__(self, msg):
                    self.msg = msg
        else:
            TestSkipped = testtools.testcase.TestCase.skipException
    else:
        TestSkipped = nose.SkipTest
        try:
            import testtools.testcase
        except ImportError:
            pass
        else:
            # Make testtools use the same exception class as nose
            testtools.testcase.TestCase.skipException = TestSkipped


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
        args.insert(0, self.bin_path(name))

        # Since they don't have any extensions, Windows can't recognize
        # executablility of the Python files in /bin. Even then, we'd have to
        # expect the user to set up file associations for .py files.
        #
        # Save us from all that headache and call python with the bin script.
        if os.name == "nt":
            args.insert(0, sys.executable)
        return subprocess.Popen(args,
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env)


def self_test_suite():
    names = [
        'blackbox',
        'client',
        'fastexport',
        'file',
        'index',
        'lru_cache',
        'objects',
        'object_store',
        'pack',
        'patch',
        'protocol',
        'repository',
        'server',
        'web',
        ]
    module_names = ['dulwich.tests.test_' + name for name in names]
    loader = unittest.TestLoader()
    return loader.loadTestsFromNames(module_names)


def tutorial_test_suite():
    tutorial = [
        '0-introduction',
        '1-repo',
        '2-object-store',
        '3-conclusion',
        ]
    tutorial_files = ["../../docs/tutorial/%s.txt" % name for name in tutorial]
    def setup(test):
        test.__dulwich_tempdir = tempfile.mkdtemp()
        os.chdir(test.__dulwich_tempdir)
    def teardown(test):
        shutil.rmtree(test.__dulwich_tempdir)
    return doctest.DocFileSuite(setUp=setup, tearDown=teardown,
        *tutorial_files)


def nocompat_test_suite():
    result = unittest.TestSuite()
    result.addTests(self_test_suite())
    result.addTests(tutorial_test_suite())
    return result


def test_suite():
    result = unittest.TestSuite()
    result.addTests(self_test_suite())
    result.addTests(tutorial_test_suite())
    from dulwich.tests.compat import test_suite as compat_test_suite
    result.addTests(compat_test_suite())
    return result
