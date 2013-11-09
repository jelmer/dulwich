# swift.py -- Repo implementation atop OpenStack SWIFT
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Fabien Boucher <fabien.boucher@enovance.com>
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

"""Repo implementation atop OpenStack SWIFT."""

# TODO: Refactor to share more code with dulwich/repo.py.

import os
import posixpath
import tempfile

from urlparse import urlparse

from cStringIO import StringIO
from ConfigParser import ConfigParser

from geventhttpclient import HTTPClient

from dulwich.repo import (
    InfoRefsContainer,
    BaseRepo,
    OBJECTDIR,
    )
from dulwich.pack import (
    PackData,
    Pack,
    PackIndexer,
    PackStreamCopier,
    write_pack_header,
    compute_file_sha,
    iter_sha1,
    write_pack_index_v2,
    load_pack_index_file,
    read_pack_header,
    _compute_object_size,
    unpack_object,
    write_pack_object,
    )
from lru_cache import LRUSizeCache
from dulwich.object_store import (
    PackBasedObjectStore,
    PACKDIR,
    INFODIR,
    ObjectStoreIterator,
    MissingObjectFinder,
    )
from dulwich.refs import (
    read_info_refs,
    write_info_refs,
    )

try:
    from simplejson import loads as json_loads
    from simplejson import dumps as json_dumps
except ImportError:
    from json import loads as json_loads
    from json import dumps as json_dumps


"""
# Configuration file sample
[swift]
auth_url = http://127.0.0.1:5000/v2.0
auth_ver = 2
username = admin;admin
password = pass
region_name = RegionOne
enpoint_type = internalURL
# Concurrency worker
concurrency = 20
# Amount of HTTP client connections
http_pool_length = 10
# Chunk size to read from pack (Bytes)
chunk_length = 12228
# Cache size (MBytes)
cache_length = 20
"""

try:
    import gevent
    from dulwich.greenthreads import GreenThreadsMissingObjectFinder
    from dulwich.greenthreads import GreenThreadsObjectStoreIterator
    gevent_support = True
except ImportError:
    gevent_support = False


def load_conf(path=None, file=None):
    """Load configuration in global var CONF

    :param path: The path to the configuration file
    :param file: If provided read instead the file like object
    """
    conf = ConfigParser(allow_no_value=True)
    if file:
        conf.readfp(file)
        return conf
    confpath = None
    if not path:
        try:
            confpath = os.environ['DULWICH_SWIFT_CFG']
        except KeyError:
            raise Exception("You need to specify a configuration file")
    else:
        confpath = path
    if not os.path.isfile(confpath):
        raise Exception("Unable to read configuration file %s" % confpath)
    conf.read(confpath)
    return conf


def swift_load_pack_index(scon, filename):
    """Read a pack index file from Swift

    :param scon: a `SwiftConnector` instance
    :param filename: Path to the index file objectise
    :return: a `PackIndexer` instance
    """
    f = scon.get_object(filename)
    try:
        return load_pack_index_file(filename, f)
    finally:
        f.close()


class SwiftException(Exception):
    pass


class SwiftConnector(object):
    """A Connector to swift that manage authentication and errors catching
    """

    def __init__(self, root, conf):
        """ Initialize a SwiftConnector

        :param root: The swift container that will act as Git bare repository
        :param conf: A ConfigParser Object
        """
        self.conf = conf
        self.auth_ver = self.conf.get("swift", "auth_ver")
        if self.auth_ver not in ["1", "2"]:
            raise NotImplementedError("Wrong authentication version \
                    use either 1 or 2")
        self.auth_url = self.conf.get("swift", "auth_url")
        self.user = self.conf.get("swift", "username")
        self.password = self.conf.get("swift", "password")
        self.concurrency = self.conf.getint('swift', 'concurrency') or 10
        self.http_pool_length = \
            self.conf.getint('swift', 'http_pool_length') or 10
        self.region_name = self.conf.get("swift", "region_name") or "RegionOne"
        self.endpoint_type = \
            self.conf.get("swift", "endpoint_type") or "internalURL"
        self.root = root
        block_size = 1024 * 12  # 12KB
        if self.auth_ver == "1":
            self.storage_url, self.token = self.swift_auth_v1()
        else:
            self.storage_url, self.token = self.swift_auth_v2()

        token_header = {'X-Auth-Token': self.token}
        self.httpclient = \
            HTTPClient.from_url(self.storage_url,
                                concurrency=self.http_pool_length,
                                block_size=block_size,
                                headers=token_header)
        self.base_path = posixpath.join(urlparse(self.storage_url).path,
                                        self.root)

    def swift_auth_v1(self):
        self.user = self.user.replace(";", ":")
        auth_httpclient = HTTPClient.from_url(self.auth_url)
        headers = {'X-Auth-User': self.user,
                   'X-Auth-Key': self.password}
        prefix_uri = urlparse(self.auth_url).path
        ret = auth_httpclient.request('GET',
                                      prefix_uri,
                                      headers=headers)

        if ret.status_code != 200:
            raise SwiftException('AUTH v1.0 request failed on %s with error code %s (%s)'
                                 % (ret.status_code,
                                    str(auth_httpclient.get_base_url()) + prefix_uri,
                                    str(ret.items())))
        storage_url = ret['X-Storage-Url']
        token = ret['X-Auth-Token']
        return storage_url, token

    def swift_auth_v2(self):
        self.tenant, self.user = self.user.split(';')
        auth_dict = {}
        auth_dict['auth'] = {'passwordCredentials':
                             {
                                 'username': self.user,
                                 'password': self.password,
                             },
                             'tenantName': self.tenant}
        auth_json = json_dumps(auth_dict)
        headers = {'Content-Type': 'application/json'}
        auth_httpclient = HTTPClient.from_url(self.auth_url)
        prefix_uri = urlparse(self.auth_url).path
        if not prefix_uri.endswith('tokens'):
            prefix_uri = posixpath.join(prefix_uri, '/tokens')
        ret = auth_httpclient.request('POST',
                                      prefix_uri,
                                      body=auth_json,
                                      headers=headers)

        if ret.status_code != 200 and ret.status_code != 203:
            raise SwiftException('AUTH v2.0 request failed on %s with error code %s (%s)'
                                 % (ret.status_code,
                                    str(auth_httpclient.get_base_url()) + prefix_uri,
                                    str(ret.items())))
        auth_ret_json = json_loads(ret.read())
        token = auth_ret_json['access']['token']['id']
        catalogs = auth_ret_json['access']['serviceCatalog']
        object_store = [o_store for o_store in catalogs if
                        o_store['type'] == 'object-store'][0]
        endpoints = object_store['endpoints']
        endpoint = [endp for endp in endpoints if
                    endp["region"] == self.region_name][0]
        return endpoint[self.endpoint_type], token

    def test_root_exists(self):
        """Check that Swift container exist

        :return: True if exist or None it not
        """
        ret = self.httpclient.request('HEAD',
                                      self.base_path)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException('HEAD request failed with error code %s'
                                 % ret.status_code)
        return True

    def create_root(self):
        """Create the Swift container

        :raise: `SwiftException` if unable to create
        """
        if not self.test_root_exists():
            ret = self.httpclient.request('PUT',
                                          self.base_path)
            if ret.status_code < 200 or ret.status_code > 300:
                raise SwiftException('PUT request failed with error code %s'
                                     % ret.status_code)

    def get_container_objects(self):
        """Retrieve objects list in a container

        :return: A list of dict that describe objects
                 or None if container does not exist
        """
        qs = '?format=json'
        path = self.base_path + qs
        ret = self.httpclient.request('GET',
                                      path)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException('GET request failed with error code %s'
                                 % ret.status_code)
        content = ret.read()
        return json_loads(content)

    def get_object_stat(self, name):
        """Retrieve object stat

        :param name: The object name
        :return: A dict that describe the object
                 or None if object does not exist
        """
        path = self.base_path + '/' + name
        ret = self.httpclient.request('HEAD',
                                      path)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException('HEAD request failed with error code %s'
                                 % ret.status_code)
        resp_headers = {}
        for header, value in ret.iteritems():
            resp_headers[header.lower()] = value
        return resp_headers

    def put_object(self, name, content):
        """Put an object

        :param name: The object name
        :param content: A file object
        :raise: `SwiftException` if unable to create
        """
        content.seek(0)
        data = content.read()
        content.close()
        path = self.base_path + '/' + name
        headers = {'Content-Length': str(len(data))}
        ret = self.httpclient.request('PUT',
                                      path,
                                      body=data,
                                      headers=headers)
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException('PUT request failed with error code %s'
                                 % ret.status_code)

    def get_object(self, name, range=None):
        """Retrieve an object

        :param name: The object name
        :param range: A string range like "0-10" to
                      retrieve specified bytes in object content
        :return: A file like instance
                 or bytestring if range is specified
        """
        headers = {}
        if range:
            headers['Range'] = 'bytes=%s' % range
        path = self.base_path + '/' + name
        ret = self.httpclient.request('GET',
                                      path,
                                      headers=headers)
        if ret.status_code == 404:
            return None
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException('GET request failed with error code %s'
                                 % ret.status_code)
        content = ret.read()

        if range:
            return content
        return StringIO(content)

    def del_object(self, name):
        """Delete an object

        :param name: The object name
        :raise: `SwiftException` if unable to delete
        """
        path = self.base_path + '/' + name
        ret = self.httpclient.request('DELETE',
                                      path)
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException('DELETE request failed with error code %s'
                                 % ret.status_code)

    def del_root(self):
        """Delete the root container by removing container content

        :raise: `SwiftException` if unable to delete
        """
        for obj in self.get_container_objects():
            self.del_object(obj['name'])
        ret = self.httpclient.request('DELETE',
                                      self.base_path)
        if ret.status_code < 200 or ret.status_code > 300:
            raise SwiftException('DELETE request failed with error code %s'
                                 % ret.status_code)


class SwiftPackReader(object):
    """A SwiftPackReader that mimic read and sync method

    The reader allows to read a specified amount of bytes from
    a given offset of a Swift object. A read offset is kept internaly.
    The reader will read from Swift a specified amount of data to complete
    its internal buffer. chunk_length specifiy the amount of data
    to read from Swift.
    """

    def __init__(self, scon, filename, pack_length):
        """Initialize a SwiftPackReader

        :param scon: a `SwiftConnector` instance
        :param filename: the pack filename
        :param pack_length: The size of the pack object
        """
        self.scon = scon
        self.filename = filename
        self.pack_length = pack_length
        self.offset = 0
        self.base_offset = 0
        self.buff = ''
        self.buff_length = int(self.scon.conf.get("swift", "chunk_length"))

    def _read(self, more=False):
        if more:
            self.buff_length = self.buff_length * 2
        l = self.base_offset
        r = min(self.base_offset + self.buff_length,
                self.pack_length)
        ret = self.scon.get_object(self.filename,
                                   range="%s-%s" % (l, r))
        self.buff = ret

    def read(self, length):
        """Read a specified amount of Bytes form the pack object

        :param length: amount of bytes to read
        :return: bytestring
        """
        end = self.offset+length
        if self.base_offset + end > self.pack_length:
            data = self.buff[self.offset:]
            self.offset = end
            return "".join(data)
        try:
            self.buff[end]
        except IndexError:
            # Need to read more from swift
            self._read(more=True)
            return self.read(length)
        data = self.buff[self.offset:end]
        self.offset = end
        return "".join(data)

    def seek(self, offset):
        """Seek to a specified offset

        :param offset: the offset to seek to
        """
        self.base_offset = offset
        self._read()
        self.offset = 0

    def read_checksum(self):
        """Read the checksum from the pack

        :return: the checksum bytestring
        """
        return self.scon.get_object(self.filename, range="-20")


class SwiftPackData(PackData):
    """The data contained in a packfile.

    We use the SwiftPackReader to read bytes from packs stored in Swift
    using the Range header feature of Swift.
    """

    def __init__(self, scon, filename):
        """ Initialize a SwiftPackReader

        :param scon: a `SwiftConnector` instance
        :param filename: the pack filename
        """
        self.scon = scon
        self._filename = filename
        self._header_size = 12
        headers = self.scon.get_object_stat(self._filename)
        self.pack_length = int(headers['content-length'])
        pack_reader = SwiftPackReader(self.scon, self._filename,
                                      self.pack_length)
        (version, self._num_objects) = read_pack_header(pack_reader.read)
        LRUCache_length = int(self.scon.conf.get("swift", "cache_length"))
        self._offset_cache = LRUSizeCache(1024*1024*LRUCache_length,
                                          compute_size=_compute_object_size)
        self.pack = None

    def get_object_at(self, offset):
        if offset in self._offset_cache:
            return self._offset_cache[offset]
        assert isinstance(offset, long) or isinstance(offset, int),\
            'offset was %r' % offset
        assert offset >= self._header_size
        pack_reader = SwiftPackReader(self.scon, self._filename,
                                      self.pack_length)
        pack_reader.seek(offset)
        unpacked, _ = unpack_object(pack_reader.read)
        return (unpacked.pack_type_num, unpacked._obj())

    def get_stored_checksum(self):
        pack_reader = SwiftPackReader(self.scon, self._filename,
                                      self.pack_length)
        return pack_reader.read_checksum()


class SwiftPack(Pack):
    """A Git pack object.

    Same implementation as pack.Pack except that _idx_load and
    _data_load are bounded to Swift version of load_pack_index and
    PackData.
    """

    def __init__(self, *args, **kwargs):
        self.scon = kwargs['scon']
        del kwargs['scon']
        super(SwiftPack, self).__init__(*args, **kwargs)
        self._idx_load = lambda: swift_load_pack_index(self.scon,
                                                       self._idx_path)
        self._data_load = lambda: SwiftPackData(self.scon, self._data_path)


class SwiftObjectStore(PackBasedObjectStore):
    """A Swift Object Store

    Allow to manage a bare Git repository from Openstack Swift.
    This object store only supports pack files and not loose objects.
    """
    def __init__(self, scon):
        """Open a Swift object store.

        :param scon: A `SwiftConnector` instance
        """
        super(SwiftObjectStore, self).__init__()
        self.scon = scon
        self.root = self.scon.root
        self.pack_dir = posixpath.join(OBJECTDIR, PACKDIR)
        self._alternates = None

    def _iter_loose_objects(self):
        """Loose objects are not supported by this repository
        """
        return []

    def iter_shas(self, finder):
        """An iterator over pack's ObjectStore

        :return: a `ObjectStoreIterator` or `SwiftObjectStoreIterator` instance
                 if eventlet is enabled
        """
        shas = iter(finder.next, None)
        if gevent_support:
            concurrency = int(self.scon.conf.get('swift', 'concurrency'))
            return GreenThreadsObjectStoreIterator(self, shas, finder,
                                                   concurrency)
        else:
            return ObjectStoreIterator(self, shas)

    def find_missing_objects(self, *args, **kwargs):
        if gevent_support:
            kwargs['concurrency'] = int(self.scon.conf.get('swift',
                                                           'concurrency'))
            return GreenThreadsMissingObjectFinder(self, *args, **kwargs)
        else:
            return MissingObjectFinder(self, *args, **kwargs)

    def _load_packs(self):
        """Load all packs from Swift

        :return: a list of `SwiftPack` instances
        """
        objects = self.scon.get_container_objects()
        pack_files = [o['name'].replace(".pack", "")
                      for o in objects if o['name'].endswith(".pack")]
        return [SwiftPack(pack, scon=self.scon) for pack in pack_files]

    def add_pack(self):
        """Add a new pack to this object store.

        :return: Fileobject to write to and a commit function to
            call when the pack is finished.
        """
        f = StringIO()

        def commit():
            f.seek(0)
            pack = PackData(file=f, filename="")
            entries = pack.sorted_entries()
            if len(entries):
                basename = posixpath.join(self.pack_dir,
                                          "pack-%s" %
                                          iter_sha1(entry[0] for
                                                    entry in entries))
                index = StringIO()
                write_pack_index_v2(index, entries, pack.get_stored_checksum())
                self.scon.put_object(basename + ".pack", f)
                self.scon.put_object(basename + ".idx", index)
                final_pack = SwiftPack(basename, scon=self.scon)
                final_pack.check_length_and_checksum()
                self._add_known_pack(final_pack)
                return final_pack
            else:
                return None

        def abort():
            pass
        return f, commit, abort

    def add_object(self, obj):
        self.add_objects([(obj, None), ])

    def _pack_cache_stale(self):
        return False

    def _get_loose_object(self, sha):
        return None

    def add_thin_pack(self, read_all, read_some):
        """Read a thin pack

        Read it from a stream and complete it in a temporary file.
        Then the pack and the corresponding index file are uploaded to Swift.
        """
        fd, path = tempfile.mkstemp(prefix='tmp_pack_')
        f = os.fdopen(fd, 'w+b')
        try:
            indexer = PackIndexer(f, resolve_ext_ref=self.get_raw)
            copier = PackStreamCopier(read_all, read_some, f,
                                      delta_iter=indexer)
            copier.verify()
            return self._complete_thin_pack(f, path, copier, indexer)
        finally:
            f.close()
            os.unlink(path)

    def _complete_thin_pack(self, f, path, copier, indexer):
        entries = list(indexer)

        # Update the header with the new number of objects.
        f.seek(0)
        write_pack_header(f, len(entries) + len(indexer.ext_refs()))

        # Must flush before reading (http://bugs.python.org/issue3207)
        f.flush()

        # Rescan the rest of the pack, computing the SHA with the new header.
        new_sha = compute_file_sha(f, end_ofs=-20)

        # Must reposition before writing (http://bugs.python.org/issue3207)
        f.seek(0, os.SEEK_CUR)

        # Complete the pack.
        for ext_sha in indexer.ext_refs():
            assert len(ext_sha) == 20
            type_num, data = self.get_raw(ext_sha)
            offset = f.tell()
            crc32 = write_pack_object(f, type_num, data, sha=new_sha)
            entries.append((ext_sha, offset, crc32))
        pack_sha = new_sha.digest()
        f.write(pack_sha)
        f.flush()

        # Move the pack in.
        entries.sort()
        pack_base_name = posixpath.join(
            self.pack_dir, 'pack-' + iter_sha1(e[0] for e in entries))
        self.scon.put_object(pack_base_name + '.pack', f)

        # Write the index.
        filename = pack_base_name + '.idx'
        index_file = StringIO()
        write_pack_index_v2(index_file, entries, pack_sha)
        self.scon.put_object(filename, index_file)

        # Add the pack to the store and return it.
        final_pack = SwiftPack(pack_base_name, scon=self.scon)
        final_pack.check_length_and_checksum()
        self._add_known_pack(final_pack)
        return final_pack


class SwiftInfoRefsContainer(InfoRefsContainer):
    """Manage references in info/refs object.
    """

    def __init__(self, scon, store):
        self.scon = scon
        self.filename = 'info/refs'
        self.store = store
        f = self.scon.get_object(self.filename)
        if not f:
            f = StringIO('')
        super(SwiftInfoRefsContainer, self).__init__(f)

    def _load_check_ref(self, name, old_ref):
        self._check_refname(name)
        f = self.scon.get_object(self.filename)
        if not f:
            return {}
        refs = read_info_refs(f)
        if old_ref is not None:
            if refs[name] != old_ref:
                return False
        return refs

    def _write_refs(self, refs):
        f = StringIO()
        f.writelines(write_info_refs(refs, self.store))
        self.scon.put_object(self.filename, f)

    def set_if_equals(self, name, old_ref, new_ref):
        """Set a refname to new_ref only if it currently equals old_ref.
        """
        refs = self._load_check_ref(name, old_ref)
        if not isinstance(refs, dict):
            return False
        refs[name] = new_ref
        self._write_refs(refs)
        self._refs[name] = new_ref
        return True

    def remove_if_equals(self, name, old_ref):
        """Remove a refname only if it currently equals old_ref.
        """
        refs = self._load_check_ref(name, old_ref)
        if not isinstance(refs, dict):
            return False
        del refs[name]
        self._write_refs(refs)
        del self._refs[name]
        return True


class SwiftRepo(BaseRepo):

    def __init__(self, root, conf):
        """Init a Git bare Repository on top of a Swift container.

        References are managed in info/refs objects by
        `SwiftInfoRefsContainer`. The root attribute is the Swift
        container that contain the Git bare repository.

        :param root: The container which contains the bare repo
        :param conf: A ConfigParser object
        """
        self.root = root.lstrip('/')
        self.conf = conf
        self.scon = SwiftConnector(self.root, self.conf)
        objects = self.scon.get_container_objects()
        if not objects:
            raise Exception('There is not any GIT repo here : %s' % self.root)
        objects = [o['name'].split('/')[0] for o in objects]
        if OBJECTDIR not in objects:
            raise Exception('This repository (%s) is not bare.' % self.root)
        self.bare = True
        self._controldir = self.root
        object_store = SwiftObjectStore(self.scon)
        refs = SwiftInfoRefsContainer(self.scon, object_store)
        BaseRepo.__init__(self, object_store, refs)

    def _put_named_file(self, filename, contents):
        """Put an object in a Swift container

        :param filename: the path to the object to put on Swift
        :param contents: the content as bytestring
        """
        f = StringIO()
        f.write(contents)
        self.scon.put_object(filename, f)

    @classmethod
    def init_bare(cls, scon, conf):
        """Create a new bare repository

        :param scon: a `SwiftConnector``instance
        :param conf: a ConfigParser object
        :return: a `SwiftRepo` instance
        """
        scon.create_root()
        for obj in [posixpath.join(OBJECTDIR, PACKDIR),
                    posixpath.join(INFODIR, 'refs')]:
            scon.put_object(obj, StringIO(''))
        ret = cls(scon.root, conf)
        ret._init_files(True)
        return ret
