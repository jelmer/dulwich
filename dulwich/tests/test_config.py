# test_config.py -- tests for cofig.py
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

"""Tests for config

"""


import errno
import os
import tempfile

from dulwich.config import GitConfigParser
from dulwich.repo import (
    Repo,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    open_repo,
    tear_down_repo,
    )


default_config="""[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
"""
dulwich_config="""[core]
	repositoryformatversion = 0
	filemode = true
[remote "upstream"]
	url = https://github.com/jelmer/dulwich.git
	fetch = +refs/heads/*:refs/remotes/upstream/*
"""
comprehensive_config="""[core]
    penguin = very blue
    Movie = BadPhysics
    UPPERCASE = true
    penguin = kingpin
[Cores]
    WhatEver = Second
baz = multiple \
lines
[beta] ; silly comment # another comment
noIndent= sillyValue ; 'nother silly comment

# empty line
        ; comment
        haha   ="beta" # last silly comment
haha = hello
    haha = bello
[nextSection] noNewline = ouch
[a.b]
    c = d
[a]
    x = y
    b = c
[b]
    x = y
# Hallo
    #Bello
[branch "eins"]
    x = 1
[branch.eins]
    y = 1
    [branch "1 234 blabl/a"]
[section]
    ; comment \\
    continued = cont\\
inued
    noncont   = not continued ; \\
    quotecont = "cont;\\
inued"
"""

class ConfigTests(TestCase):

    def setUp(self):
        super(ConfigTests, self).setUp()
        self.parser = GitConfigParser()

        try:
            (defaultconf_fd, self.defaultconf_path) = tempfile.mkstemp()
        except OSError, e:
            raise

        defaultconf = os.fdopen(defaultconf_fd, 'w+')

        defaultconf.write(default_config)
        defaultconf.close()

        try:
            (dulwichconf_fd, self.dulwichconf_path) = tempfile.mkstemp()
        except OSError, e:
            raise

        dulwichconf = os.fdopen(dulwichconf_fd, 'w+')

        dulwichconf.write(dulwich_config)
        dulwichconf.close()

        try:
            (comprehensiveconf_fd, self.comprehensiveconf_path) = tempfile.mkstemp()
        except OSError, e:
            raise

        comprehensiveconf = os.fdopen(comprehensiveconf_fd, 'w+')

        comprehensiveconf.write(comprehensive_config)
        comprehensiveconf.close()

    def tearDown(self):
        os.remove(self.defaultconf_path)
        os.remove(self.dulwichconf_path)

    def test_file_read(self):
        self.maxDiff = None

        defaultconf_fp = open(self.defaultconf_path)

        defaultconf_dict = GitConfigParser.read_file(defaultconf_fp,self.defaultconf_path)
        self.assertEqual({
            "core" : {
                "_name" : "core",
                "subsections" : {},
                "options" : {
                    "repositoryformatversion" : { "_name" : "repositoryformatversion", "value" : ["0",] },
                    "filemode" : { "_name" : "filemode", "value" : ["true",] },
                    "bare" : { "_name" : "bare", "value" : ["true",] },
                    },
                }
            }, defaultconf_dict)

        comprehensiveconf_fp = open(self.comprehensiveconf_path)

        comprehensiveconf_dict = GitConfigParser.read_file(comprehensiveconf_fp,self.comprehensiveconf_path)
        self.assertEqual({
            "core" : {
                "_name" : "core",
                "subsections" : {},
                "options" : {
                    "penguin" : { "_name" : "penguin", "value" : [ "very blue", "kingpin" ] },
                    "movie" : { "_name" : "Movie", "value" : [ "BadPhysics" ] },
                    "uppercase" : { "_name" : "UPPERCASE" , "value" : [ "true" ] },
                    },
                },
            "cores" : {
                "_name" : "Cores",
                "subsections" : {},
                "options" : {
                    "whatever" : { "_name" : "WhatEver", "value" : [ "Second" ] },
                    "baz" : { "_name" : "baz", "value" : [ "multiple lines" ] },
                    },
                },
            "beta" : {
                "_name" : "beta",
                "subsections" : {},
                "options" : {
                    "noindent" : { "_name" : "noIndent", "value" : [ "sillyValue" ] },
                    "haha" : { "_name" : "haha", "value" : [ "\"beta\"", "hello", "bello" ] },
                    },
                },
            "nextsection" : {
                "_name" : "nextSection",
                "subsections" : {},
                "options" : {
                    "nonewline" : { "_name" : "noNewline", "value" : [ "ouch" ] },
                    },
                },
            "a" : {
                "_name" : "a",
                "subsections" : {
                    "b" : {
                        "_name" : "b",
                        "options" : {
                            "c" : { "_name" : "c", "value" : [ "d" ] },
                            },
                        },
                    },
                "options" : {
                    "x" : { "_name" : "x", "value" : [ "y" ] },
                    "b" : { "_name" : "b", "value" : [ "c" ] },
                    },
                },
            "b" : {
                "_name" : "b",
                "subsections" : {},
                "options" : {
                    "x" : { "_name" : "x", "value" : [ "y" ] },
                    },
                },
            "branch" : {
                "_name" : "branch",
                "subsections" : {
                    "eins" : {
                        "_name" : "\"eins\"",
                        "options" : {
                            "x" : { "_name" : "x", "value" : [ "1" ] },
                            "y" : { "_name" : "y", "value" : [ "1" ] },
                            },
                        },
                    "1 234 blabl/a" : {
                        "_name" : "\"1 234 blabl/a\"",
                        "options" : {},
                        },
                    },
                "options" : {},
                },
            "section" : {
                "_name" : "section",
                "subsections" : {},
                "options" : {
                    "continued" : { "_name" : "continued", "value" : [ "continued" ] },
                    "noncont" : { "_name" : "noncont", "value" : [ "not continued" ] },
                    "quotecont" : { "_name" : "quotecont", "value" : [ "\"cont;inued\"" ] },
                    },
                },
            }, comprehensiveconf_dict)

