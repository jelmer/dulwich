# errors.py -- errors for python-git
# Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# of the License.
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

class WrongObjectException(Exception):
  """Baseclass for all the _ is not a _ exceptions on objects.

  Do not instantiate directly.

  Subclasses should define a _type attribute that indicates what
  was expected if they were raised.
  """

  def __init__(self, sha, *args, **kwargs):
    string = "%s is not a %s" % (sha, self._type)
    Exception.__init__(self, string)

class NotCommitError(WrongObjectException):
  """Indicates that the sha requested does not point to a commit."""

  _type = 'commit'

class NotTreeError(WrongObjectException):
  """Indicates that the sha requested does not point to a tree."""

  _type = 'tree'

class NotBlobError(WrongObjectException):
  """Indicates that the sha requested does not point to a blob."""

  _type = 'blob'

class MissingCommitError(Exception):
  """Indicates that a commit was not found in the repository"""

  def __init__(self, sha, *args, **kwargs):
    Exception.__init__(self, "%s is not in the revision store" % sha)

