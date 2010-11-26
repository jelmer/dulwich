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


def test_suite():
    names = [
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
    result = unittest.TestSuite()
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromNames(module_names)
    result.addTests(suite)
    tutorial = [
        '0-introduction',
        ]
    tutorial_files = ["../../docs/tutorial/%s.txt" % name for name in tutorial]
    suite = doctest.DocFileSuite(*tutorial_files)
    result.addTests(suite)
    return result
