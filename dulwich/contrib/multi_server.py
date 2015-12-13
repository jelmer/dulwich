# multi_server.py -- High Availability server
# Copyright (C) 2015 Jelmer Vernooij <jelmer@jelmer.uk>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# or (at your option) a later version of the License.
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

"""Highly available Git server, backed by etcd.

On startup, each server needs to be pointed at:

 * A etcd server URL (can be a suburl)
 * A local storage path

The refs in etcd are considered canonical. Each server has a copy of the object
store that *may* be out of date.
"""

import etcd


from dulwich.refs import RefsContainer


class EtcdRefsContainer(RefsContainer):
    """A refs container backed by etcd.
    """

    def __init__(self, client, base_key):
        """Create a new EtcdRefsContainer.

        :param client: A etcd.Client object to use
        :param base_key: Base key
        """
        self._client = client
        self._base_key = base_key

    def allkeys(self):
        raise NotImplementedError

    def read_loose_ref(self, name):
        raise NotImplementedError

    def get_packed_refs(self):
        return {}

    def set_symbolic_ref(self, name, other):
        raise NotImplementedError

    def set_if_equals(self, name, old_ref, new_ref):
        # TODO(jelmer): call test_and_set
        raise NotImplementedError

    def add_if_new(self, name, ref):
        raise NotImplementedError

    def remove_if_equals(self, name, old_ref):
        raise NotImplementedError

    def get_peeled(self, name):
        # TODO(jelmer): Implement
        return None


def main():
    pass


if __name__ == '__main__':
    main()
