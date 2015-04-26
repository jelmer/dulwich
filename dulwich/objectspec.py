# objectspec.py -- Object specification
# Copyright (C) 2014 Jelmer Vernooij <jelmer@samba.org>
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

"""Object specification."""


def parse_object(repo, objectish):
    """Parse a string referring to an object.

    :param repo: A `Repo` object
    :param objectish: A string referring to an object
    :return: A git object
    :raise KeyError: If the object can not be found
    """
    if getattr(objectish, "encode", None) is not None:
        objectish = objectish.encode('ascii')
    return repo[objectish]


def parse_commit_range(repo, committishs):
    """Parse a string referring to a range of commits.

    :param repo: A `Repo` object
    :param committishs: A string referring to a range of commits.
    :return: An iterator over `Commit` objects
    :raise KeyError: When the reference commits can not be found
    :raise ValueError: If the range can not be parsed
    """
    if getattr(committishs, "encode", None) is not None:
        committishs = committishs.encode('ascii')
    return iter([repo[committishs]])
