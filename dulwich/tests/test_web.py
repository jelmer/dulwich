# test_web.py -- Tests for the git HTTP server
# Copryight (C) 2010 Google, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2
# or (at your option) any later version of the License.
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

"""Tests for the Git HTTP server."""

from cStringIO import StringIO
import re
from unittest import TestCase

from dulwich.objects import (
    Blob,
    )
from dulwich.web import (
    HTTP_OK,
    HTTP_NOT_FOUND,
    HTTP_FORBIDDEN,
    send_file,
    get_info_refs,
    handle_service_request,
    _LengthLimitedFile,
    HTTPGitRequest,
    HTTPGitApplication,
    )


class WebTestCase(TestCase):
    """Base TestCase that sets up some useful instance vars."""
    def setUp(self):
        self._environ = {}
        self._req = HTTPGitRequest(self._environ, self._start_response)
        self._status = None
        self._headers = []

    def _start_response(self, status, headers):
        self._status = status
        self._headers = list(headers)


class DumbHandlersTestCase(WebTestCase):

    def test_send_file_not_found(self):
        list(send_file(self._req, None, 'text/plain'))
        self.assertEquals(HTTP_NOT_FOUND, self._status)

    def test_send_file(self):
        f = StringIO('foobar')
        output = ''.join(send_file(self._req, f, 'text/plain'))
        self.assertEquals('foobar', output)
        self.assertEquals(HTTP_OK, self._status)
        self.assertTrue(('Content-Type', 'text/plain') in self._headers)
        self.assertTrue(f.closed)

    def test_send_file_buffered(self):
        bufsize = 10240
        xs = 'x' * bufsize
        f = StringIO(2 * xs)
        self.assertEquals([xs, xs],
                          list(send_file(self._req, f, 'text/plain')))
        self.assertEquals(HTTP_OK, self._status)
        self.assertTrue(('Content-Type', 'text/plain') in self._headers)
        self.assertTrue(f.closed)

    def test_send_file_error(self):
        class TestFile(object):
            def __init__(self):
                self.closed = False

            def read(self, size=-1):
                raise IOError

            def close(self):
                self.closed = True

        f = TestFile()
        list(send_file(self._req, f, 'text/plain'))
        self.assertEquals(HTTP_NOT_FOUND, self._status)
        self.assertTrue(f.closed)

    def test_get_info_refs(self):
        self._environ['QUERY_STRING'] = ''

        class TestTag(object):
            def __init__(self, sha, obj_class, obj_sha):
                self.sha = lambda: sha
                self.object = (obj_class, obj_sha)

        class TestBlob(object):
            def __init__(self, sha):
                self.sha = lambda: sha

        blob1 = TestBlob('111')
        blob2 = TestBlob('222')
        blob3 = TestBlob('333')

        tag1 = TestTag('aaa', Blob, '222')

        class TestRepo(object):

            def __init__(self, objects, peeled):
                self._objects = dict((o.sha(), o) for o in objects)
                self._peeled = peeled

            def get_peeled(self, sha):
                return self._peeled[sha]

            def __getitem__(self, sha):
                return self._objects[sha]

            def get_refs(self):
                return {
                    'HEAD': '000',
                    'refs/heads/master': blob1.sha(),
                    'refs/tags/tag-tag': tag1.sha(),
                    'refs/tags/blob-tag': blob3.sha(),
                    }

        class TestBackend(object):
            def __init__(self):
                objects = [blob1, blob2, blob3, tag1]
                self.repo = TestRepo(objects, {
                    'HEAD': '000',
                    'refs/heads/master': blob1.sha(),
                    'refs/tags/tag-tag': blob2.sha(),
                    'refs/tags/blob-tag': blob3.sha(),
                    })

            def open_repository(self, path):
                assert path == '/'
                return self.repo

        mat = re.search('.*', '//info/refs')
        self.assertEquals(['111\trefs/heads/master\n',
                           '333\trefs/tags/blob-tag\n',
                           'aaa\trefs/tags/tag-tag\n',
                           '222\trefs/tags/tag-tag^{}\n'],
                          list(get_info_refs(self._req, TestBackend(), mat)))


class SmartHandlersTestCase(WebTestCase):

    class _TestUploadPackHandler(object):
        def __init__(self, backend, args, proto, stateless_rpc=False,
                     advertise_refs=False):
            self.args = args
            self.proto = proto
            self.stateless_rpc = stateless_rpc
            self.advertise_refs = advertise_refs

        def handle(self):
            self.proto.write('handled input: %s' % self.proto.recv(1024))

    def _MakeHandler(self, *args, **kwargs):
        self._handler = self._TestUploadPackHandler(*args, **kwargs)
        return self._handler

    def services(self):
        return {'git-upload-pack': self._MakeHandler}

    def test_handle_service_request_unknown(self):
        mat = re.search('.*', '/git-evil-handler')
        list(handle_service_request(self._req, 'backend', mat))
        self.assertEquals(HTTP_FORBIDDEN, self._status)

    def test_handle_service_request(self):
        self._environ['wsgi.input'] = StringIO('foo')
        mat = re.search('.*', '/git-upload-pack')
        output = ''.join(handle_service_request(self._req, 'backend', mat,
                                                services=self.services()))
        self.assertEqual('handled input: foo', output)
        response_type = 'application/x-git-upload-pack-response'
        self.assertTrue(('Content-Type', response_type) in self._headers)
        self.assertFalse(self._handler.advertise_refs)
        self.assertTrue(self._handler.stateless_rpc)

    def test_handle_service_request_with_length(self):
        self._environ['wsgi.input'] = StringIO('foobar')
        self._environ['CONTENT_LENGTH'] = 3
        mat = re.search('.*', '/git-upload-pack')
        output = ''.join(handle_service_request(self._req, 'backend', mat,
                                                services=self.services()))
        self.assertEqual('handled input: foo', output)
        response_type = 'application/x-git-upload-pack-response'
        self.assertTrue(('Content-Type', response_type) in self._headers)

    def test_get_info_refs_unknown(self):
        self._environ['QUERY_STRING'] = 'service=git-evil-handler'
        list(get_info_refs(self._req, 'backend', None,
                           services=self.services()))
        self.assertEquals(HTTP_FORBIDDEN, self._status)

    def test_get_info_refs(self):
        self._environ['wsgi.input'] = StringIO('foo')
        self._environ['QUERY_STRING'] = 'service=git-upload-pack'

        mat = re.search('.*', '/git-upload-pack')
        output = ''.join(get_info_refs(self._req, 'backend', mat,
                                       services=self.services()))
        self.assertEquals(('001e# service=git-upload-pack\n'
                           '0000'
                           # input is ignored by the handler
                           'handled input: '), output)
        self.assertTrue(self._handler.advertise_refs)
        self.assertTrue(self._handler.stateless_rpc)


class LengthLimitedFileTestCase(TestCase):
    def test_no_cutoff(self):
        f = _LengthLimitedFile(StringIO('foobar'), 1024)
        self.assertEquals('foobar', f.read())

    def test_cutoff(self):
        f = _LengthLimitedFile(StringIO('foobar'), 3)
        self.assertEquals('foo', f.read())
        self.assertEquals('', f.read())

    def test_multiple_reads(self):
        f = _LengthLimitedFile(StringIO('foobar'), 3)
        self.assertEquals('fo', f.read(2))
        self.assertEquals('o', f.read(2))
        self.assertEquals('', f.read())


class HTTPGitRequestTestCase(WebTestCase):
    def test_not_found(self):
        self._req.cache_forever()  # cache headers should be discarded
        message = 'Something not found'
        self.assertEquals(message, self._req.not_found(message))
        self.assertEquals(HTTP_NOT_FOUND, self._status)
        self.assertEquals(set([('Content-Type', 'text/plain')]),
                          set(self._headers))

    def test_forbidden(self):
        self._req.cache_forever()  # cache headers should be discarded
        message = 'Something not found'
        self.assertEquals(message, self._req.forbidden(message))
        self.assertEquals(HTTP_FORBIDDEN, self._status)
        self.assertEquals(set([('Content-Type', 'text/plain')]),
                          set(self._headers))

    def test_respond_ok(self):
        self._req.respond()
        self.assertEquals([], self._headers)
        self.assertEquals(HTTP_OK, self._status)

    def test_respond(self):
        self._req.nocache()
        self._req.respond(status=402, content_type='some/type',
                          headers=[('X-Foo', 'foo'), ('X-Bar', 'bar')])
        self.assertEquals(set([
            ('X-Foo', 'foo'),
            ('X-Bar', 'bar'),
            ('Content-Type', 'some/type'),
            ('Expires', 'Fri, 01 Jan 1980 00:00:00 GMT'),
            ('Pragma', 'no-cache'),
            ('Cache-Control', 'no-cache, max-age=0, must-revalidate'),
            ]), set(self._headers))
        self.assertEquals(402, self._status)


class HTTPGitApplicationTestCase(TestCase):
    def setUp(self):
        self._app = HTTPGitApplication('backend')

    def test_call(self):
        def test_handler(req, backend, mat):
            # tests interface used by all handlers
            self.assertEquals(environ, req.environ)
            self.assertEquals('backend', backend)
            self.assertEquals('/foo', mat.group(0))
            return 'output'

        self._app.services = {
            ('GET', re.compile('/foo$')): test_handler,
        }
        environ = {
            'PATH_INFO': '/foo',
            'REQUEST_METHOD': 'GET',
            }
        self.assertEquals('output', self._app(environ, None))
