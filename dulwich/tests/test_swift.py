# test_swift.py -- Unittests for the Swift backend.
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

import ConfigParser
import posixpath

from time import time
from cStringIO import StringIO
from contextlib import nested
from mock import patch

from dulwich import swift
from dulwich.tests import (
    TestCase,
    )
from dulwich.tests.utils import (
    build_pack,
    )
from dulwich.objects import (
    Blob,
    Commit,
    Tree,
    Tag,
    parse_timezone,
    )
from dulwich.pack import (
    REF_DELTA,
    )

config_file = """[swift]
auth_url = http://127.0.0.1:8080/auth/%(version_str)s
auth_ver = %(version_int)s
username = test;tester
password = testing
concurrency = %(concurrency)s
chunk_length = %(chunk_length)s
cache_length = %(cache_length)s
"""

def_config_file = {'version_str': 'v1.0',
                   'version_int': 1,
                   'concurrency': 1,
                   'chunk_length': 12228,
                   'cache_length': 1}


def fake_get_auth(*args, **kwargs):
    url = 'http://127.0.0.1:8080/v1.0/AUTH_fakeuser'
    token = '12' * 10
    return url, token


def raise_client_exception_404(*args, **kwargs):
    raise swift.ClientException('', http_status=404)


def raise_client_exception(*args, **kwargs):
    raise swift.ClientException('')


def fake_get_container(*args, **kwargs):
    return ({}, ('a', 'b'))


def fake_get_object(*args, **kwargs):
    return ({}, 'content')


def create_swift_connector(store={}):
    return lambda root: FakeSwiftConnector(root,
                                           store=store)


class FakeSwiftConnector(object):

    def __init__(self, root, store=None):
        if store:
            self.store = store
        else:
            self.store = {}
        self.root = root

    def put_object(self, name, content):
        name = posixpath.join(self.root, name)
        if hasattr(content, 'seek'):
            content.seek(0)
            content = content.read()
        self.store[name] = content

    def get_object(self, name, range=None):
        name = posixpath.join(self.root, name)
        if not range:
            try:
                return StringIO(self.store[name])
            except KeyError:
                return None
        else:
            l, r = range.split('-')
            try:
                if not l:
                    r = -int(r)
                    return self.store[name][r:]
                else:
                    return self.store[name][int(l):int(r)]
            except KeyError:
                return None

    def get_container_objects(self):
        return [{'name': k.replace(self.root + '/', '')}
                for k in self.store]

    def create_root(self):
        if self.root in self.store.keys():
            pass
        else:
            self.store[self.root] = ''

    def get_object_stat(self, name):
        name = posixpath.join(self.root, name)
        if not name in self.store:
            return None
        return {'content-length': len(self.store[name])}


class TestSwiftObjectStore(TestCase):

    def _create_commit(self, data, marker='Default', blob=None):
        if not blob:
            blob = Blob.from_string('The blob content %s' % marker)
        tree = Tree()
        tree.add("thefile_%s" % marker, 0100644, blob.id)
        cmt = Commit()
        if data:
            assert isinstance(data[-1], Commit)
            cmt.parents = [data[-1].id]
        cmt.tree = tree.id
        author = "John Doe %s <john@doe.net>" % marker
        cmt.author = cmt.committer = author
        tz = parse_timezone('-0200')[0]
        cmt.commit_time = cmt.author_time = int(time())
        cmt.commit_timezone = cmt.author_timezone = tz
        cmt.encoding = "UTF-8"
        cmt.message = "The commit message %s" % marker
        tag = Tag()
        tag.tagger = "john@doe.net"
        tag.message = "Annotated tag"
        tag.tag_timezone = parse_timezone('-0200')[0]
        tag.tag_time = cmt.author_time
        tag.object = (Commit, cmt.id)
        tag.name = "v_%s_0.1" % marker
        return blob, tree, tag, cmt

    def _create_commits(self, length=1, marker='Default'):
        data = []
        for i in xrange(0, length):
            _marker = "%s_%s" % (marker, i)
            blob, tree, tag, cmt = self._create_commit(data, _marker)
            data.extend([blob, tree, tag, cmt])
        return data

    def _put_pack(self, sos, commit_amount=1, marker='Default'):
        odata = self._create_commits(length=commit_amount, marker=marker)
        data = [(d.type_num, d.as_raw_string()) for d in odata]
        f = StringIO()
        build_pack(f, data, store=sos)
        sos.add_thin_pack(f.read, None)
        return odata

    def test_load_packs(self):
        store = {'fakerepo/objects/pack/pack-'+'1'*40+'.idx': '',
                 'fakerepo/objects/pack/pack-'+'1'*40+'.pack': '',
                 'fakerepo/objects/pack/pack-'+'2'*40+'.idx': '',
                 'fakerepo/objects/pack/pack-'+'2'*40+'.pack': ''}
        fsc = FakeSwiftConnector('fakerepo', store=store)
        sos = swift.SwiftObjectStore(fsc)
        packs = sos._load_packs()
        self.assertEqual(len(packs), 2)
        for pack in packs:
            assert isinstance(pack, swift.SwiftPack)

    def test_add_thin_pack(self):
        fsc = FakeSwiftConnector('fakerepo')
        sos = swift.SwiftObjectStore(fsc)
        self._put_pack(sos, 1, 'Default')
        self.assertEqual(len(fsc.store), 2)

    def test_find_missing_objects(self):
        commit_amount = 3
        fsc = FakeSwiftConnector('fakerepo')
        sos = swift.SwiftObjectStore(fsc)
        odata = self._put_pack(sos, commit_amount, 'Default')
        head = odata[-1].id
        i = sos.iter_shas(sos.find_missing_objects([],
                                                   [head, ],
                                                   progress=None,
                                                   get_tagged=None))
        self.assertEqual(len(i), commit_amount * 3)
        shas = [d.id for d in odata]
        for sha, path in i:
            self.assertIn(sha.id, shas)

    def test_find_missing_objects_with_tag(self):
        commit_amount = 3
        fsc = FakeSwiftConnector('fakerepo')
        sos = swift.SwiftObjectStore(fsc)
        odata = self._put_pack(sos, commit_amount, 'Default')
        head = odata[-1].id
        peeled_sha = dict([(sha.object[1], sha.id)
                           for sha in odata if isinstance(sha, Tag)])
        get_tagged = lambda: peeled_sha
        i = sos.iter_shas(sos.find_missing_objects([],
                                                   [head, ],
                                                   progress=None,
                                                   get_tagged=get_tagged))
        self.assertEqual(len(i), commit_amount * 4)
        shas = [d.id for d in odata]
        for sha, path in i:
            self.assertIn(sha.id, shas)

    def test_find_missing_objects_with_common(self):
        commit_amount = 3
        fsc = FakeSwiftConnector('fakerepo')
        sos = swift.SwiftObjectStore(fsc)
        odata = self._put_pack(sos, commit_amount, 'Default')
        head = odata[-1].id
        have = odata[7].id
        i = sos.iter_shas(sos.find_missing_objects([have, ],
                                                   [head, ],
                                                   progress=None,
                                                   get_tagged=None))
        self.assertEqual(len(i), 3)

    def test_find_missing_objects_multiple_packs(self):
        fsc = FakeSwiftConnector('fakerepo')
        sos = swift.SwiftObjectStore(fsc)
        commit_amount_a = 3
        odataa = self._put_pack(sos, commit_amount_a, 'Default1')
        heada = odataa[-1].id
        commit_amount_b = 2
        odatab = self._put_pack(sos, commit_amount_b, 'Default2')
        headb = odatab[-1].id
        i = sos.iter_shas(sos.find_missing_objects([],
                                                   [heada, headb],
                                                   progress=None,
                                                   get_tagged=None))
        self.assertEqual(len(fsc.store), 4)
        self.assertEqual(len(i),
                         commit_amount_a * 3 +
                         commit_amount_b * 3)
        shas = [d.id for d in odataa]
        shas.extend([d.id for d in odatab])
        for sha, path in i:
            self.assertIn(sha.id, shas)

    def test_add_thin_pack_ext_ref(self):
        fsc = FakeSwiftConnector('fakerepo')
        sos = swift.SwiftObjectStore(fsc)
        odata = self._put_pack(sos, 1, 'Default1')
        ref_blob_content = odata[0].as_raw_string()
        ref_blob_id = odata[0].id
        new_blob = Blob.from_string(ref_blob_content.replace('blob',
                                                             'yummy blob'))
        blob, tree, tag, cmt = \
            self._create_commit([], marker='Default2', blob=new_blob)
        data = [(REF_DELTA, (ref_blob_id, blob.as_raw_string())),
                (tree.type_num, tree.as_raw_string()),
                (cmt.type_num, cmt.as_raw_string()),
                (tag.type_num, cmt.as_raw_string())]
        f = StringIO()
        build_pack(f, data, store=sos)
        sos.add_thin_pack(f.read, None)
        self.assertEqual(len(fsc.store), 4)


class TestSwiftRepo(TestCase):

    def test_init(self):
        store = {'fakerepo/objects/pack': ''}
        with patch('dulwich.swift.SwiftConnector',
                   new_callable=create_swift_connector,
                   store=store):
            swift.SwiftRepo('fakerepo')

    def test_init_no_data(self):
        with patch('dulwich.swift.SwiftConnector',
                   new_callable=create_swift_connector):
            self.assertRaises(Exception, swift.SwiftRepo, 'fakerepo')

    def test_init_bad_data(self):
        store = {'fakerepo/.git/objects/pack': ''}
        with patch('dulwich.swift.SwiftConnector',
                   new_callable=create_swift_connector,
                   store=store):
            self.assertRaises(Exception, swift.SwiftRepo, 'fakerepo')

    def test_put_named_file(self):
        store = {'fakerepo/objects/pack': ''}
        with patch('dulwich.swift.SwiftConnector',
                   new_callable=create_swift_connector,
                   store=store):
            repo = swift.SwiftRepo('fakerepo')
            desc = 'Fake repo'
            repo._put_named_file('description', desc)
        self.assertEqual(repo.scon.store['fakerepo/description'],
                         desc)

    def test_init_bare(self):
        fsc = FakeSwiftConnector('fakeroot')
        with patch('dulwich.swift.SwiftConnector',
                   new_callable=create_swift_connector,
                   store=fsc.store):
            swift.SwiftRepo.init_bare(fsc)
        self.assertIn('fakeroot/objects/pack', fsc.store)
        self.assertIn('fakeroot/info/refs', fsc.store)
        self.assertIn('fakeroot/description', fsc.store)


class TestSwiftInfoRefsContainer(TestCase):

    def setUp(self):
        super(TestSwiftInfoRefsContainer, self).setUp()
        content = \
            "22effb216e3a82f97da599b8885a6cadb488b4c5\trefs/heads/master\n" + \
            "cca703b0e1399008b53a1a236d6b4584737649e4\trefs/heads/dev"
        self.store = {'fakerepo/info/refs': content}

    def test_init(self):
        """ info/refs does not exists"""
        fsc = FakeSwiftConnector('fakerepo')
        irc = swift.SwiftInfoRefsContainer(fsc)
        self.assertEqual(len(irc._refs), 0)
        fsc = FakeSwiftConnector('fakerepo')
        fsc.store = self.store
        irc = swift.SwiftInfoRefsContainer(fsc)
        self.assertIn('refs/heads/dev', irc.allkeys())
        self.assertIn('refs/heads/master', irc.allkeys())

    def test_set_if_equals(self):
        fsc = FakeSwiftConnector('fakerepo')
        fsc.store = self.store
        irc = swift.SwiftInfoRefsContainer(fsc)
        irc.set_if_equals('refs/heads/dev',
                          "cca703b0e1399008b53a1a236d6b4584737649e4", '1'*40)
        self.assertEqual(irc['refs/heads/dev'], '1'*40)

    def test_remove_if_equals(self):
        fsc = FakeSwiftConnector('fakerepo')
        fsc.store = self.store
        irc = swift.SwiftInfoRefsContainer(fsc)
        irc.remove_if_equals('refs/heads/dev',
                             "cca703b0e1399008b53a1a236d6b4584737649e4")
        self.assertNotIn('refs/heads/dev', irc.allkeys())


class TestSwiftConnector(TestCase):

    def setUp(self):
        super(TestSwiftConnector, self).setUp()
        conf = config_file % def_config_file
        swift.CONF = ConfigParser.ConfigParser()
        swift.CONF.readfp(StringIO(conf))

    def test_init_connector(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        self.assertEqual(conn.auth_ver, '1')
        self.assertEqual(conn.auth_url,
                         'http://127.0.0.1:8080/auth/v1.0')
        self.assertEqual(conn.user, 'test:tester')
        self.assertEqual(conn.password, 'testing')
        self.assertEqual(conn.root, 'fakerepo')
        self.assertEqual(conn.storage_url,
                         'http://127.0.0.1:8080/v1.0/AUTH_fakeuser')
        self.assertEqual(conn.token, '12' * 10)
        swift.CONF.set('swift', 'auth_ver', '2')
        swift.CONF.set('swift', 'auth_url', 'http://127.0.0.1:8080/auth/v2.0')
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        self.assertEqual(conn.user, 'tester')
        self.assertEqual(conn.tenant, 'test')

    def test_root_exists(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.head_container')]
        with nested(*ctx):
            self.assertEqual(conn.test_root_exists(), True)

    def test_root_not_exists(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.head_container',
                     raise_client_exception_404)]
        with nested(*ctx):
            self.assertEqual(conn.test_root_exists(), None)

    def test_create_root(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.head_container'),
               patch('swiftclient.client.put_container')]
        with nested(*ctx):
            self.assertEqual(conn.create_root(), None)

    def test_create_root_fails(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.head_container'),
               patch('swiftclient.client.put_container',
                     raise_client_exception)]
        with nested(*ctx):
            self.assertRaises(swift.SwiftException,
                              lambda: conn.create_root())

    def test_get_container_objects(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.get_container',
                     fake_get_container)]
        with nested(*ctx):
            self.assertEqual(len(conn.get_container_objects()), 2)

    def test_get_container_objects_fails(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.get_container',
                     raise_client_exception_404)]
        with nested(*ctx):
            self.assertEqual(conn.get_container_objects(), None)

    def test_get_object_stat(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.head_object',
                     lambda *args, **kwargs: {})]
        with nested(*ctx):
            self.assertEqual(conn.get_object_stat('a'), {})

    def test_get_object_stat_fails(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.head_object',
                     raise_client_exception_404)]
        with nested(*ctx):
            self.assertEqual(conn.get_object_stat('a'), None)

    def test_put_object(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.put_object')]
        with nested(*ctx):
            self.assertEqual(conn.put_object('a', 'content'), None)

    def test_put_object_fails(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.put_object',
                     raise_client_exception)]
        with nested(*ctx):
            self.assertRaises(swift.SwiftException,
                              lambda: conn.put_object('a', 'content'))

    def test_get_object(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.get_object', fake_get_object)]
        with nested(*ctx):
            self.assertEqual(conn.get_object('a').read(), 'content')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.get_object', fake_get_object)]
        with nested(*ctx):
            self.assertEqual(conn.get_object('a', range='0-6'), 'content')

    def test_get_object_fails(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.get_object',
                     raise_client_exception_404)]
        with nested(*ctx):
            self.assertEqual(conn.get_object('a'), None)

    def test_del_object(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.delete_object')]
        with nested(*ctx):
            self.assertEqual(conn.del_object('a'), None)

    def test_del_root(self):
        with patch('swiftclient.client.get_auth', fake_get_auth):
            conn = swift.SwiftConnector('fakerepo')
        ctx = [patch('swiftclient.client.http_connection'),
               patch('swiftclient.client.get_container',
                     lambda *args, **kwargs: (None, ({'name': None},))),
               patch('swiftclient.client.delete_container'),
               patch('swiftclient.client.delete_object')]
        with nested(*ctx):
            self.assertEqual(conn.del_root(), None)
