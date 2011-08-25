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

try:
    from collections import OrderedDict
except ImportError:
    from dulwich import OrderedDict

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
    "subsection"], or [section.subsection]) and options within each
    section, key = value. See git-config(1) for more details.

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
        r'^\s*(?P<option>[^=\s]+)'          # leading whitespace
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

    def __init__(self):
        self.configdict = OrderedDict()

    # Utility functions
    def has_section(self, section):
        """ Check if section exists

        :param section: section name
        :return: section dict or None
        """

        try:
            return self.configdict[section.lower()]
        except KeyError:
            return None

    def has_subsection(self, section, subsection, case):
        """ Check if subsection in section exists

        :note: section should exist
        :param section: section name
        :param subsection; subsection name
        :param case: subsection case sensititivity
        :return: subsection dict or None
        """

        try:
            if case:
                subsect = self.configdict[section.lower()]['subsections'][subsection]
            else:
                subsect = self.configdict[section.lower()]['subsections'][subsection.lower()]
            return subsect if len(subsect) != 0 else None
        except KeyError:
            return None

    def has_option(self, section, subsection, case, option):
        """ Check if option in section exists

        :note: section and subsection should exist
        :param section: section name
        :param subsection; subsection name
        :param option: option name
        :param case: subsection case sensititivity True or False
        :return: option dict or None
        """

        try:
            if case:
                return self.configdict[section.lower()]['subsections'][subsection]['options'][option]
            else:
                return self.configdict[section.lower()]['subsections'][subsection.lower()]['options'][option.lower()]
        except KeyError:
            return None

    def add_section(self, section):
        """Initialize a new section

        :note: section should not exist
        :param section: section name
        :return: section dict
        :raises DuplicateSectionError: if section already exists
        """

        if self.has_section(section):
            raise DuplicateSectionError(section)

        self.configdict[section.lower()] = {
            '_name': section,
            'subsections': OrderedDict(),
            'options': OrderedDict(),
            }
        return self.configdict[section.lower()]

    def add_subsection(self, section, subsection, case):
        """Initialize a new subsection in section

        :note: section should exist and subsection should not exist.
            All lowercase case sensitive subsection alwaysconflicts
            with a case insenstive subsection
        :param section: section name
        :param subsection; subsection name
        :param case: subsection case sensititivity True or False
        :return: subsection dict
        :raises DuplicateSubSectionError: if subsection already exists
            in section
        """

        if self.has_subsection(section, subsection, case):
            raise DuplicateSectionError(section)

        if case:
            self.configdict[section.lower()]['subsections'][subsection] = {
                '_name': '"%s"' % subsection,
                'options': OrderedDict(),
                }
            return self.configdict[section.lower()]['subsections'][subsection]
        else:
            self.configdict[section.lower()]['subsections'][subsection.lower()] = {
                '_name': subsection,
                'options': OrderedDict(),
                }
            return self.configdict[section.lower()]['subsections'][subsection.lower()]

    # Mappings Interface
    # __contains__, keys, items, values, get, __eq__, and __ne__

    def __contains__(self, key):
        """Return existence of a section, subsection, or option

        :param key: section.subsection.option (subsection and option
            are optional)


        :return True or False
        """

        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

    # MutableMapping Interface
    # __delitem__, __getitem__, __iter__, __len__, __setitem__

    def __getitem__(self, key):
        """Get a section, subsection, or a single option

        :param key: section.subsection.option (subsection and option
            are optional)
        :return: section or subsection dict, or option as string
        """

        if key is not None:
            if key.count('.') == 1:  # subsection or option
                (section, subkey) = key.split('.')
                try:
                    return self.configdict[section.lower()]['subsections'][subkey]
                except KeyError:
                    return self.configdict[section.lower()]['options'][subkey.lower()]
            elif key.count('.') == 2:  # option
                (section, subsection, option) = key.split('.')
                return self.configdict[section.lower()]['subsections'][subsection]['options'][option.lower()]
            elif key.count('.') > 2:
                raise KeyError(key)
            else:  # key is a section
                return self.configdict[key.lower()]
        raise KeyError(key)

    def __setitem__(self, key, value):
        """Add or change an option

        :param key: section.subsection.option (subsection is optional)
        :return: (ignored)
        """

        if key is not None:
            if key.count('.') == 1:  # section.option
                (section, option) = key.split('.')
                sect = self.has_section(section)
                if sect is None:
                    sect = self.add_section(section)
                self.configdict[section.lower()]['options'][option.lower()] = value
            elif key.count('.') == 2:  # section.subsection.option
                (section, subsection, option) = key.split('.')
                sect = self.has_section(section)
                if sect is None:
                    sect = self.add_section(section)
                subsect = self.has_subsection(section, subsection, True)
                if subsect is None:
                    subsect = self.add_subsection(section, subsection, True)
                self.configdict[section.lower()]['subsections'][subsection]['options'][option.lower()]
            elif key.count('.') == 0 or key.count('.') > 2:
                raise KeyError(key)
        else:
            raise KeyError(key)

    def __delitem__(self, key):
        """Delete a section, subsection, or a single option

        :param key: section.subsection.option (subsection and option
            are optional)
        :return: (ignored)
        """

        if key is not None:
            if key.count('.') == 1:  # subsection or option
                (section, subkey) = key.split('.')
                try:
                    del self.configdict[section.lower()]['subsections'][subkey]
                except KeyError:
                    del self.configdict[section.lower()]['options'][subkey.lower()]
            elif key.count('.') == 2:  # option
                (section, subsection, option) = key.split('.')
                del self.configdict[section.lower()]['subsections'][subsection]['options'][option.lower()]
            elif key.count('.') > 2:
                raise KeyError(key)
            else:  # key is a section
                del self.configdict[key.lower()]
        else:
            raise KeyError(key)

    def read(self, repo_path=None, exclusive_filename=None, bare_repo=False):
        if exclusive_filename is not None:
            exclusive_filename_fp = open(exclusive_filename)
            self._read_file(exclusive_filename_fp, exclusive_filename)

            return True
        else:
            if repo_path is not None:
                repo_config_filename = repo_path + '/config' if bare_repo else repo_path + '/.git/config'
                repo_config_fp = open(repo_config_filename)
                repo_config = self._read_file(repo_config_fp, repo_config_filename)


                return True
            else:
                return False

    def _read_file(self, fp, fpname):
        """Read git configuration from file to a dict
        """
        cursect = None
        optname = None
        lineno = 0
        e = None
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
                mo = self.FULLSECTCRE.match(line)
                if mo:
                    sectname, subsectname, isubsectname = \
                            mo.group('section', 'subsection',
                                    'isubsection')

                    cursect = self.has_section(sectname)
                    if cursect is None:
                        cursect = self.add_section(sectname)

                    if subsectname is not None:
                        cursect = self.has_subsection(sectname,
                                subsectname, True)
                        if cursect is None:
                            cursect = self.add_subsection(sectname,
                                subsectname, True)
                    if isubsectname is not None:
                        cursect = self.has_subsection(sectname,
                                isubsectname, False)
                        if cursect is None:
                            cursect = self.add_subsection(sectname,
                                isubsectname, False)
                    oline = line[mo.end():]
                # no section header in the file?
                elif cursect is None:
                    raise MissingSectionHeaderError(fpname, lineno, line)
                # an option line?
                else:
                    mo = self.OPTCRE.match(line)
                    if mo:
                        optname, optval = mo.group('option', 'value')
                        optname = optname.rstrip()
                        optval = optval.strip()
                        val = ''

                        try:
                            opt = cursect["options"][optname.lower()]
                        except KeyError:
                            opt = cursect["options"][optname.lower()] = {
                                    "_name": optname,
                                    "value": [],
                                    }

                        # Parse value character by character
                        while(True):
                            if len(optval) == 0 or optval[0] in '#;':
                                break

                            mo = self.VALCRE.match(optval)
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
