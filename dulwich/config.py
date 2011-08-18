# config.py -- For dealing with git repositories.
# Copyright (C) 2001-2010 Python Software Foundation
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


"""Git Configuration parser"""

import re

# exception classes
class Error(Exception):
    """Base class for ConfigParser exceptions."""

    def _get_message(self):
        """Getter for 'message'; needed only to override deprecation in
        BaseException."""
        return self.__message

    def _set_message(self, value):
        """Setter for 'message'; needed only to override deprecation in
        BaseException."""
        self.__message = value

    # BaseException.message has been deprecated since Python 2.6.  To prevent
    # DeprecationWarning from popping up over this pre-existing attribute, use
    # a new property that takes lookup precedence.
    message = property(_get_message, _set_message)

    def __init__(self, msg=''):
        self.message = msg
        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

    __str__ = __repr__

class ParsingError(Error):
    """Raised when a configuration file does not follow legal syntax."""

    def __init__(self, filename):
        Error.__init__(self, 'File contains parsing errors: %s' % filename)
        self.filename = filename
        self.errors = []

    def append(self, lineno, line):
        self.errors.append((lineno, line))
        self.message += '\n\t[line %2d]: %s' % (lineno, line)

class MissingSectionHeaderError(ParsingError):
    """Raised when a key-value pair is found before any section header."""

    def __init__(self, filename, lineno, line):
        Error.__init__(
            self,
            'File contains no section headers.\nfile: %s, line: %d\n%r' %
            (filename, lineno, line))
        self.filename = filename
        self.lineno = lineno
        self.line = line


class GitConfigParser(object):
    """parses and manages a git configuration file

    Implements collections.MutableMapping

    A git configuration file consists of sections ([section], [section
    "subsection"], or [section.subsection]) and options within each section,
    key = value. See git-config(1) for more details.  

    In general, the parser supports two types of accesses:
        keyed by "section.subsection.option" as a flat dict (aka git syntax)
        keyed by section and then option as nested dicts (aka dict syntax)

    Both section and options are case insensitive. Subsection is case
    sensitive when enclosed in "" in the config file or when using dict
    syntax. Otherwise, subsection is case insensitive
    """


    # Splits section and subsection
    FULLSECTCRE = re.compile(
            r'^\s*\['
            r'(?P<section>[^]\s\.]+)'       # section
            r'((?:\s*"(?P<subsection>[^"]+)")|(?:\.(?P<isubsection>[^]\s]+)))?'
                                            # "subsection" or .isubsection
            r'\s*\]'
            )

    # Options can have whitespace in front and may have values
    OPTCRE = re.compile(
        r'^\s*(?P<option>[^=\s]+)'          # very permissive, incuding leading whitespace
        r'\s*=?\s*'                         # any number of space/tab,
                                            # followed by separator (=)
                                            # followed by any # space/tab
        r'(?P<value>.*)$'                   # everything up to eol
        )

    # Values are complicated
    VALCRE = re.compile(
            r'^'                            # Only match from beginning
            r'(?P<whitespace>\s+)|'         # Any amount of whitespace
            r'(?P<continuation>\\\s*$)|'    # A continuation character
            r'(?P<escbslash>\\\\)|'         # An escaped backslash
            r'\\(?P<escquote>([\'"]))|'     # An escaped quote
            r'(?P<escnl>\\n)|'              # An escaped newline
            r'(?P<esctab>\\t)|'             # An escaped tab
            r'(?P<escbs>\\b)|'              # An escaped backspace
            r'(?P<quoted>"([^"\\]*(?:(?:\\n|\\[tbn"\\])[^"\\]*)*)")|'
                                            # quote-delimited value
            r'(?P<noesc>([^\t \\\n]+))'     # normal value
        )

    # Utility functions
    @staticmethod
    def findkey(configdict,section,subsection,option,case):
        try:
            sect = configdict[section.lower()]
            if subsection:
                if case:
                    subsect = sect["subsections"][subsection]
                else:
                    subsect = sect["subsections"][subsection.lower()]
                if subsect:
                    if option:
                        return subsect[option.lower()]
                    return subsect
            else:
                if option:
                    return sect["options"][option.lower()]
                return sect
        except KeyError:
            return None
        return None


    @staticmethod
    def read_file(fp, fpname):
        """Read git configuration from file to a dict
        """
        cursect = None                        # None, or a dictionary
        optname = None
        lineno = 0
        e = None                              # None, or an exception
        config = {}
        oline = ''
        while True:
            if len(oline) == 0:
                oline = fp.readline()
                lineno = lineno + 1
            if not oline:
                break
            line = oline.strip()
            # comment or blank line?
            if len(line) == 0 or line[0] in '#;':
                oline = ''
                continue
            # a section header or option header?
            else:
                # is it a section header?
                mo = GitConfigParser.FULLSECTCRE.match(line)
                if mo:
                    sectname, subsectname, isubsectname = mo.group('section',
                            'subsection', 'isubsection')

                    # Make sure section exists
                    cursect = GitConfigParser.findkey(config,sectname,None,None,None)
                    if cursect is None:
                        cursect = config[sectname.lower()] = {
                                "_name" : sectname,
                                "subsections" : {},
                                "options" : {},
                                }

                    # Make sure subsection exists
                    if subsectname is not None:
                        cursect = GitConfigParser.findkey(config,sectname,subsectname,None,True)
                        if cursect is None:
                            cursect = config[sectname.lower()]["subsections"][subsectname] = {
                                    "_name" : "\"%s\"" % subsectname,
                                    "options" : {},
                                    }
                    elif isubsectname is not None:
                        cursect = GitConfigParser.findkey(config,sectname,isubsectname,None,False)
                        if cursect is None:
                            cursect = config[sectname.lower()]["subsections"][isubsectname.lower()] = {
                                    "_name" : isubsectname,
                                    "options" : {},
                                    }
                    oline = line[mo.end():]
                # no section header in the file?
                elif cursect is None:
                    raise MissingSectionHeaderError(fpname, lineno, line)
                # an option line?
                else:
                    mo = GitConfigParser.OPTCRE.match(line)
                    if mo:
                        optname, optval = mo.group('option', 'value')
                        optname = optname.rstrip()
                        optval = optval.strip()
                        val = ''

                        try:
                            opt = cursect["options"][optname.lower()]
                        except KeyError:
                            opt = cursect["options"][optname.lower()] = {
                                    "_name" : optname,
                                    "value" : [],
                                    }

                        # Parse value character by character
                        while(True):
                            if len(optval) == 0 or optval[0] in '#;':
                                break

                            mo = GitConfigParser.VALCRE.match(optval)
                            if mo:
                                if mo.group('whitespace') is not None:
                                    val += ' '
                                elif mo.group('continuation') is not None:
                                    optval = fp.readline().rstrip()
                                    lineno = lineno + 1
                                    continue
                                elif mo.group('escbslash') is not None:
                                    val += '\\'
                                elif mo.group('escquote') is not None:
                                    val += mo.group('escquote')
                                elif mo.group('escnl') is not None:
                                    val += "\n"
                                elif mo.group('esctab') is not None:
                                    val += "\t"
                                elif mo.group('escbs') is not None:
                                    val += "\b"
                                elif mo.group('quoted') is not None:
                                    v = mo.group('quoted')
                                    # parse quoted value
                                    val += v
                                elif mo.group('noesc') is not None:
                                    val += mo.group('noesc')
                                optval = optval[mo.end():]
                            else:
                                if not e:
                                    e = ParsingError(fpname)
                                e.append(lineno, repr(line))
                        cursect["options"][optname.lower()]["value"].append(val.strip())
                    else:
                        # a non-fatal parsing error occurred.  set up the
                        # exception but keep going. the exception will be
                        # raised at the end of the file and will contain a
                        # list of all bogus lines
                        if not e:
                            e = ParsingError(fpname)
                        e.append(lineno, repr(line))
                    oline = ''
        # if any parsing errors occurred, raise an exception
        if e:
            raise e

        return config
