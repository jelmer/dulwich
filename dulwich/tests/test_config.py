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

default_config = """[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
"""
dulwich_config = """[core]
	repositoryformatversion = 0
	filemode = true
[remote "upstream"]
	url = https://github.com/jelmer/dulwich.git
	fetch = +refs/heads/*:refs/remotes/upstream/*
"""
comprehensive_config = """[core]
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
[foo]
	bar = "foo"bar"baz"
"""

comprehensive_config_clean = """[core]
	penguin = very blue
	penguin = kingpin
	Movie = BadPhysics
	UPPERCASE = true
[Cores]
	WhatEver = Second
	baz = multiple lines
[beta]
	noIndent = sillyValue
	haha = "beta"
	haha = hello
	haha = bello
[nextSection]
	noNewline = ouch
[a]
	x = y
	b = c
[a.b]
	c = d
[b]
	x = y
[branch "eins"]
	x = 1
	y = 1
[branch "1 234 blabl/a"]
[section]
	continued = continued
	noncont = not continued
	quotecont = "cont;inued"
[foo]
	bar = "foo"bar"baz"
"""

comprehensive_config_dict = {
    "core": {
        "_name": "core",
        "subsections": {},
        "options": {
            "penguin": {"_name": "penguin", "value": ["very blue", "kingpin"]},
            "movie": {"_name": "Movie", "value": ["BadPhysics"]},
            "uppercase": {"_name": "UPPERCASE", "value": ["true"]},
           },
       },
    "cores": {
        "_name": "Cores",
        "subsections": {},
        "options": {
            "whatever": {"_name": "WhatEver", "value": ["Second"]},
            "baz": {"_name": "baz", "value": ["multiple lines"]},
           },
       },
    "beta": {
        "_name": "beta",
        "subsections": {},
        "options": {
            "noindent": {"_name": "noIndent", "value": ["sillyValue"]},
            "haha": {"_name": "haha", "value": ["\"beta\"", "hello", "bello"]},
           },
       },
    "nextsection": {
        "_name": "nextSection",
        "subsections": {},
        "options": {
            "nonewline": {"_name": "noNewline", "value": ["ouch"]},
           },
       },
    "a": {
        "_name": "a",
        "subsections": {
            "b": {
                "_name": "b",
                "options": {
                    "c": {"_name": "c", "value": ["d"]},
                   },
               },
           },
        "options": {
            "x": {"_name": "x", "value": ["y"]},
            "b": {"_name": "b", "value": ["c"]},
           },
       },
    "b": {
        "_name": "b",
        "subsections": {},
        "options": {
            "x": {"_name": "x", "value": ["y"]},
           },
       },
    "branch": {
        "_name": "branch",
        "subsections": {
            "eins": {
                "_name": "\"eins\"",
                "options": {
                    "x": {"_name": "x", "value": ["1"]},
                    "y": {"_name": "y", "value": ["1"]},
                   },
               },
            "1 234 blabl/a": {
                "_name": "\"1 234 blabl/a\"",
                "options": {},
               },
           },
        "options": {},
       },
    "section": {
        "_name": "section",
        "subsections": {},
        "options": {
            "continued": {"_name": "continued", "value": ["continued"]},
            "noncont": {"_name": "noncont", "value": ["not continued"]},
            "quotecont": {"_name": "quotecont", "value": ["\"cont;inued\""]},
           },
       },
    "foo": {
        "_name": "foo",
        "subsections": {},
        "options": {
            "bar": {"_name": "bar", "value": ["\"foo\"bar\"baz\""]},
            },
        },
   }

comprehensive_config_dict_parsed = {
    "core": {
        "_name": "core",
        "subsections": {},
        "options": {
            "penguin": {"_name": "penguin", "value": ["very blue", "kingpin"]},
            "movie": {"_name": "Movie", "value": ["BadPhysics"]},
            "uppercase": {"_name": "UPPERCASE", "value": ["true"]},
           },
       },
    "cores": {
        "_name": "Cores",
        "subsections": {},
        "options": {
            "whatever": {"_name": "WhatEver", "value": ["Second"]},
            "baz": {"_name": "baz", "value": ["multiple lines"]},
           },
       },
    "beta": {
        "_name": "beta",
        "subsections": {},
        "options": {
            "noindent": {"_name": "noIndent", "value": ["sillyValue"]},
            "haha": {"_name": "haha", "value": ["\"beta\"", "hello", "bello"]},
           },
       },
    "nextsection": {
        "_name": "nextSection",
        "subsections": {},
        "options": {
            "nonewline": {"_name": "noNewline", "value": ["ouch"]},
           },
       },
    "a": {
        "_name": "a",
        "subsections": {
            "b": {
                "_name": "b",
                "options": {
                    "c": {"_name": "c", "value": ["d"]},
                   },
               },
           },
        "options": {
            "x": {"_name": "x", "value": ["y"]},
            "b": {"_name": "b", "value": ["c"]},
           },
       },
    "b": {
        "_name": "b",
        "subsections": {},
        "options": {
            "x": {"_name": "x", "value": ["y"]},
           },
       },
    "branch": {
        "_name": "branch",
        "subsections": {
            "eins": {
                "_name": "\"eins\"",
                "options": {
                    "x": {"_name": "x", "value": ["1"]},
                    "y": {"_name": "y", "value": ["1"]},
                   },
               },
            "1 234 blabl/a": {
                "_name": "\"1 234 blabl/a\"",
                "options": {},
               },
           },
        "options": {},
       },
    "section": {
        "_name": "section",
        "subsections": {},
        "options": {
            "continued": {"_name": "continued", "value": ["continued"]},
            "noncont": {"_name": "noncont", "value": ["not continued"]},
            "quotecont": {"_name": "quotecont", "value": ["\"cont;inued\""]},
           },
       },
    "foo": {
        "_name": "foo",
        "subsections": {},
        "options": {
            "bar": {"_name": "bar", "value": ["foobarbaz"]},
            },
        },
   }

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
            (comprehensiveconf_fd, self.comprehensiveconf_path) = \
                    tempfile.mkstemp()
        except OSError, e:
            raise

        comprehensiveconf = os.fdopen(comprehensiveconf_fd, 'w+')

        comprehensiveconf.write(comprehensive_config)
        comprehensiveconf.close()

    def tearDown(self):
        os.remove(self.defaultconf_path)
        os.remove(self.dulwichconf_path)
        os.remove(self.comprehensiveconf_path)

    def test_read(self):
        parser = self.parser

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        self.assertEqual(comprehensive_config_dict, parser.configdict)

    def test_getitem(self):
        parser = self.parser

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        self.assertEqual(comprehensive_config_dict['core'], parser['core'])
        self.assertEqual(comprehensive_config_dict['core'], parser['CORE'])
        self.assertEqual(comprehensive_config_dict['a'], parser['a'])
        self.assertEqual(comprehensive_config_dict['a']['subsections']['b'], parser['a.b'])
        self.assertEqual(comprehensive_config_dict['a']['subsections']['b']['options']['c'], parser['a.b.c'])

        self.assertRaises(KeyError, lambda: parser['does not exist'])
        self.assertRaises(KeyError, lambda: parser['a.b.d'])
        self.assertRaises(KeyError, lambda: parser['a.b.c.d'])

    def test_setitem(self):
        parser = self.parser

        parser.read(exclusive_filename=self.defaultconf_path)

        parser['core.bare'] = 'false'
        self.assertEqual('false', parser.configdict['core']['options']['bare'])

        parser['Core.filemode'] = 'false'
        self.assertEqual('false', parser.configdict['core']['options']['filemode'])

        parser['Cores.a'] = 'b'
        self.assertEqual(True,  'cores' in parser.configdict)
        self.assertEqual('Cores', parser.configdict['cores']['_name'])
        self.assertEqual('b', parser.configdict['cores']['options']['a'])

        self.assertRaises(KeyError, parser.__setitem__, 'does not exist', 'random')

    def test_delitem(self):
        parser = self.parser

        parser.read(exclusive_filename=self.defaultconf_path)

        del parser['core.bare']
        self.assertRaises(KeyError, lambda: parser.configdict['core']['options']['bare'])
        del parser['Core']
        self.assertRaises(KeyError, lambda: parser.configdict['core'])

        self.assertRaises(KeyError, parser.__delitem__, 'does not exist')

    def test_pop(self):
        parser = self.parser

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        self.assertEqual(comprehensive_config_dict['a']['subsections']['b'], parser.pop('a.b'))
        self.assertEqual(comprehensive_config_dict['a']['options']['b'], parser.pop('a.b'))
        self.assertEqual(False, 'a.b' in parser)
        self.assertEqual(comprehensive_config_dict['beta']['options']['haha'], parser.pop('beta.haha'))
        self.assertEqual(False, 'beta.haha' in parser)

    def test_clear(self):
        parser = self.parser
        self.assertEqual({}, parser)

        parser.read(exclusive_filename=self.comprehensiveconf_path)
        self.assertEqual(comprehensive_config_dict, parser.configdict)

        parser.clear()
        self.assertEqual({}, parser)

    def test_contains(self):
        parser = self.parser

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        self.assertEqual(True, 'core' in parser)
        self.assertEqual(True, 'Core.UPPERCASE' in parser)
        self.assertEqual(True, 'branch.eins.x' in parser)
        self.assertEqual(True, 'branch.1 234 blabl/a' in parser)

        self.assertEqual(False, 'does not exist' in parser)

    def test_items(self):
        parser = self.parser

        parser.read(exclusive_filename=self.defaultconf_path)

        self.assertEqual([
            ('core.repositoryformatversion', '0'),
            ('core.filemode', 'true'),
            ('core.bare', 'true')], parser.items())

    def test_keys(self):
        parser = self.parser

        parser.read(exclusive_filename=self.defaultconf_path)

        self.assertEqual([
            'core.repositoryformatversion',
            'core.filemode',
            'core.bare'], parser.keys())

    def test_values(self):
        parser = self.parser

        parser.read(exclusive_filename=self.defaultconf_path)

        self.assertEqual(['0', 'true', 'true'], parser.values())

    def test_iter(self):
        parser = self.parser

        parser.read(exclusive_filename=self.defaultconf_path)

        it = iter(parser)
        self.assertEqual('core.repositoryformatversion', it.next())
        self.assertEqual('core.filemode', it.next())
        self.assertEqual('core.bare', it.next())
        self.assertRaises(StopIteration, lambda: it.next())

        parser.clear()

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        it = iter(parser)
        self.assertEqual('core.penguin', it.next())
        self.assertEqual('core.penguin', it.next())
        self.assertEqual('core.movie', it.next())
        self.assertEqual('core.uppercase', it.next())
        self.assertEqual('cores.whatever', it.next())
        self.assertEqual('cores.baz', it.next())
        self.assertEqual('beta.noindent', it.next())
        self.assertEqual('beta.haha', it.next())
        self.assertEqual('beta.haha', it.next())
        self.assertEqual('beta.haha', it.next())
        self.assertEqual('nextsection.nonewline', it.next())
        self.assertEqual('a.x', it.next())
        self.assertEqual('a.b', it.next())
        self.assertEqual('a.b.c', it.next())
        self.assertEqual('b.x', it.next())
        self.assertEqual('branch.eins.x', it.next())
        self.assertEqual('branch.eins.y', it.next())
        self.assertEqual('section.continued', it.next())
        self.assertEqual('section.noncont', it.next())
        self.assertEqual('section.quotecont', it.next())
        self.assertEqual('foo.bar', it.next())
        self.assertRaises(StopIteration, lambda: it.next())

    def test_len(self):
        parser = self.parser

        parser.read(exclusive_filename=self.defaultconf_path)

        self.assertEqual(3, len(parser))

        parser.clear()

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        self.assertEqual(21, len(parser))

    def test_eq(self):
        parser = self.parser
        oparser = GitConfigParser()

        parser.read(exclusive_filename=self.defaultconf_path)
        oparser.read(exclusive_filename=self.defaultconf_path)
        self.assertEqual(parser, oparser)

        self.assertEqual({
            'core.repositoryformatversion': '0',
	        'core.filemode': 'true',
	        'core.bare': 'true',
            }, parser)

        self.assertEqual({
            'core': {
                '_name': 'core',
                'subsections': {},
                'options': {
                    'repositoryformatversion': {
                        '_name': 'repositoryformatversion',
                        'value': ['0'],
                        },
	                'filemode': {
                        '_name': 'filemode',
                        'value': ['true'],
                        },
	                'bare': {
                        '_name': 'bare',
                        'value': ['true'],
                        }
                    },
                },
            }, parser)

        oparser.clear()
        oparser.read(exclusive_filename=self.comprehensiveconf_path)
        self.assertEqual(False, parser == oparser)

        self.assertEqual(comprehensive_config_dict, oparser)

    def test_decode(self):
        parser = self.parser

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        self.assertEqual(comprehensive_config_dict_parsed['foo']['options']['bar']['value'][0], parser.decode_value(parser['foo.bar']['value'][0]))

    def test_write(self):
        parser = self.parser
        oparser = GitConfigParser()

        parser.read(exclusive_filename=self.defaultconf_path)

        (fd, path) = tempfile.mkstemp()
        f = os.fdopen(fd, 'w+')
        parser.write(f)
        f.close()

        self.assertEqual(default_config, open(path).read())

        oparser.read(exclusive_filename=path)
        self.assertEqual(oparser, parser)

        oparser.clear()
        parser.clear()

        parser.read(exclusive_filename=self.comprehensiveconf_path)

        (comprehensive_fd, comprehensive_path) = tempfile.mkstemp()
        comprehensive_f = os.fdopen(comprehensive_fd, 'w+')
        parser.write(comprehensive_f)
        comprehensive_f.close()

        self.assertEqual(comprehensive_config_clean, open(comprehensive_path).read())

        oparser.read(exclusive_filename=comprehensive_path)
        self.assertEqual(oparser, parser)

        oparser.clear()
        parser.clear()

        (empty_fd, empty_path) = tempfile.mkstemp()
        empty_f = os.fdopen(empty_fd, 'w+')
        parser.write(empty_f)
        empty_f.close()

        self.assertEqual("", open(empty_path).read())

        oparser.read(exclusive_filename=empty_path)
        self.assertEqual(oparser, parser)

        os.remove(path)
        os.remove(empty_path)
