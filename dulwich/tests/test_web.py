# test_web.py -- Tests for the git HTTP server
# Copyright (C) 2010 Google, Inc.
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
import gzip
import re
import os

from dulwich.object_store import (
    MemoryObjectStore,
    )
from dulwich.objects import (
    Blob,
    Tag,
    )
from dulwich.repo import (
    BaseRepo,
    MemoryRepo,
    )
from dulwich.server import (
    DictBackend,
    )
from dulwich.tests import (
    TestCase,
    )
from dulwich.web import (
    HTTP_OK,
    HTTP_NOT_FOUND,
    HTTP_FORBIDDEN,
    HTTP_ERROR,
    GunzipFilter,
    send_file,
    get_text_file,
    get_loose_object,
    get_pack_file,
    get_idx_file,
    get_info_refs,
    get_info_packs,
    handle_service_request,
    _LengthLimitedFile,
    HTTPGitRequest,
    HTTPGitApplication,
    )

from dulwich.tests.utils import (
    make_object,
    )


class TestHTTPGitRequest(HTTPGitRequest):
    """HTTPGitRequest with overridden methods to help test caching."""

    def __init__(self, *args, **kwargs):
        HTTPGitRequest.__init__(self, *args, **kwargs)
        self.cached = None

    def nocache(self):
        self.cached = False

    def cache_forever(self):
        self.cached = True


class WebTestCase(TestCase):
    """Base TestCase with useful instance vars and utility functions."""

    _req_class = TestHTTPGitRequest

    def setUp(self):
        super(WebTestCase, self).setUp()
        self._environ = {}
        self._req = self._req_class(self._environ, self._start_response,
                                    handlers=self._handlers())
        self._status = None
        self._headers = []
        self._output = StringIO()

    def _start_response(self, status, headers):
        self._status = status
        self._headers = list(headers)
        return self._output.write

    def _handlers(self):
        return None

    def assertContentTypeEquals(self, expected):
        self.assertTrue(('Content-Type', expected) in self._headers)


def _test_backend(objects, refs=None, named_files=None):
    if not refs:
        refs = {}
    if not named_files:
        named_files = {}
    repo = MemoryRepo.init_bare(objects, refs)
    for path, contents in named_files.iteritems():
        repo._put_named_file(path, contents)
    return DictBackend({'/': repo})


class DumbHandlersTestCase(WebTestCase):

    def test_send_file_not_found(self):
        list(send_file(self._req, None, 'text/plain'))
        self.assertEqual(HTTP_NOT_FOUND, self._status)

    def test_send_file(self):
        f = StringIO('foobar')
        output = ''.join(send_file(self._req, f, 'some/thing'))
        self.assertEqual('foobar', output)
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('some/thing')
        self.assertTrue(f.closed)

    def test_send_file_buffered(self):
        bufsize = 10240
        xs = 'x' * bufsize
        f = StringIO(2 * xs)
        self.assertEqual([xs, xs],
                          list(send_file(self._req, f, 'some/thing')))
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('some/thing')
        self.assertTrue(f.closed)

    def test_send_file_error(self):
        class TestFile(object):
            def __init__(self, exc_class):
                self.closed = False
                self._exc_class = exc_class

            def read(self, size=-1):
                raise self._exc_class()

            def close(self):
                self.closed = True

        f = TestFile(IOError)
        list(send_file(self._req, f, 'some/thing'))
        self.assertEqual(HTTP_ERROR, self._status)
        self.assertTrue(f.closed)
        self.assertFalse(self._req.cached)

        # non-IOErrors are reraised
        f = TestFile(AttributeError)
        self.assertRaises(AttributeError, list,
                          send_file(self._req, f, 'some/thing'))
        self.assertTrue(f.closed)
        self.assertFalse(self._req.cached)

    def test_get_text_file(self):
        backend = _test_backend([], named_files={'description': 'foo'})
        mat = re.search('.*', 'description')
        output = ''.join(get_text_file(self._req, backend, mat))
        self.assertEqual('foo', output)
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('text/plain')
        self.assertFalse(self._req.cached)

    def test_get_loose_object(self):
        blob = make_object(Blob, data='foo')
        backend = _test_backend([blob])
        mat = re.search('^(..)(.{38})$', blob.id)
        output = ''.join(get_loose_object(self._req, backend, mat))
        self.assertEqual(blob.as_legacy_object(), output)
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('application/x-git-loose-object')
        self.assertTrue(self._req.cached)

    def test_get_loose_object_missing(self):
        mat = re.search('^(..)(.{38})$', '1' * 40)
        list(get_loose_object(self._req, _test_backend([]), mat))
        self.assertEqual(HTTP_NOT_FOUND, self._status)

    def test_get_loose_object_error(self):
        blob = make_object(Blob, data='foo')
        backend = _test_backend([blob])
        mat = re.search('^(..)(.{38})$', blob.id)

        def as_legacy_object_error():
            raise IOError

        blob.as_legacy_object = as_legacy_object_error
        list(get_loose_object(self._req, backend, mat))
        self.assertEqual(HTTP_ERROR, self._status)

    def test_get_pack_file(self):
        pack_name = os.path.join('objects', 'pack', 'pack-%s.pack' % ('1' * 40))
        backend = _test_backend([], named_files={pack_name: 'pack contents'})
        mat = re.search('.*', pack_name)
        output = ''.join(get_pack_file(self._req, backend, mat))
        self.assertEqual('pack contents', output)
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('application/x-git-packed-objects')
        self.assertTrue(self._req.cached)

    def test_get_idx_file(self):
        idx_name = os.path.join('objects', 'pack', 'pack-%s.idx' % ('1' * 40))
        backend = _test_backend([], named_files={idx_name: 'idx contents'})
        mat = re.search('.*', idx_name)
        output = ''.join(get_idx_file(self._req, backend, mat))
        self.assertEqual('idx contents', output)
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('application/x-git-packed-objects-toc')
        self.assertTrue(self._req.cached)

    def test_get_info_refs(self):
        self._environ['QUERY_STRING'] = ''

        blob1 = make_object(Blob, data='1')
        blob2 = make_object(Blob, data='2')
        blob3 = make_object(Blob, data='3')

        tag1 = make_object(Tag, name='tag-tag',
                           tagger='Test <test@example.com>',
                           tag_time=12345,
                           tag_timezone=0,
                           message='message',
                           object=(Blob, blob2.id))

        objects = [blob1, blob2, blob3, tag1]
        refs = {
          'HEAD': '000',
          'refs/heads/master': blob1.id,
          'refs/tags/tag-tag': tag1.id,
          'refs/tags/blob-tag': blob3.id,
          }
        backend = _test_backend(objects, refs=refs)

        mat = re.search('.*', '//info/refs')
        self.assertEqual(['%s\trefs/heads/master\n' % blob1.id,
                           '%s\trefs/tags/blob-tag\n' % blob3.id,
                           '%s\trefs/tags/tag-tag\n' % tag1.id,
                           '%s\trefs/tags/tag-tag^{}\n' % blob2.id],
                          list(get_info_refs(self._req, backend, mat)))
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('text/plain')
        self.assertFalse(self._req.cached)

    def test_get_info_packs(self):
        class TestPack(object):
            def __init__(self, sha):
                self._sha = sha

            def name(self):
                return self._sha

        packs = [TestPack(str(i) * 40) for i in xrange(1, 4)]

        class TestObjectStore(MemoryObjectStore):
            # property must be overridden, can't be assigned
            @property
            def packs(self):
                return packs

        store = TestObjectStore()
        repo = BaseRepo(store, None)
        backend = DictBackend({'/': repo})
        mat = re.search('.*', '//info/packs')
        output = ''.join(get_info_packs(self._req, backend, mat))
        expected = 'P pack-%s.pack\n' * 3
        expected %= ('1' * 40, '2' * 40, '3' * 40)
        self.assertEqual(expected, output)
        self.assertEqual(HTTP_OK, self._status)
        self.assertContentTypeEquals('text/plain')
        self.assertFalse(self._req.cached)


class SmartHandlersTestCase(WebTestCase):

    class _TestUploadPackHandler(object):
        def __init__(self, backend, args, proto, http_req=None,
                     advertise_refs=False):
            self.args = args
            self.proto = proto
            self.http_req = http_req
            self.advertise_refs = advertise_refs

        def handle(self):
            self.proto.write('handled input: %s' % self.proto.recv(1024))

    def _make_handler(self, *args, **kwargs):
        self._handler = self._TestUploadPackHandler(*args, **kwargs)
        return self._handler

    def _handlers(self):
        return {'git-upload-pack': self._make_handler}

    def test_handle_service_request_unknown(self):
        mat = re.search('.*', '/git-evil-handler')
        list(handle_service_request(self._req, 'backend', mat))
        self.assertEqual(HTTP_FORBIDDEN, self._status)
        self.assertFalse(self._req.cached)

    def _run_handle_service_request(self, content_length=None):
        self._environ['wsgi.input'] = StringIO('foo')
        if content_length is not None:
            self._environ['CONTENT_LENGTH'] = content_length
        mat = re.search('.*', '/git-upload-pack')
        handler_output = ''.join(
          handle_service_request(self._req, 'backend', mat))
        write_output = self._output.getvalue()
        # Ensure all output was written via the write callback.
        self.assertEqual('', handler_output)
        self.assertEqual('handled input: foo', write_output)
        self.assertContentTypeEquals('application/x-git-upload-pack-result')
        self.assertFalse(self._handler.advertise_refs)
        self.assertTrue(self._handler.http_req)
        self.assertFalse(self._req.cached)

    def test_handle_service_request(self):
        self._run_handle_service_request()

    def test_handle_service_request_with_length(self):
        self._run_handle_service_request(content_length='3')

    def test_handle_service_request_empty_length(self):
        self._run_handle_service_request(content_length='')

    def test_get_info_refs_unknown(self):
        self._environ['QUERY_STRING'] = 'service=git-evil-handler'
        list(get_info_refs(self._req, 'backend', None))
        self.assertEqual(HTTP_FORBIDDEN, self._status)
        self.assertFalse(self._req.cached)

    def test_get_info_refs(self):
        self._environ['wsgi.input'] = StringIO('foo')
        self._environ['QUERY_STRING'] = 'service=git-upload-pack'

        mat = re.search('.*', '/git-upload-pack')
        handler_output = ''.join(get_info_refs(self._req, 'backend', mat))
        write_output = self._output.getvalue()
        self.assertEqual(('001e# service=git-upload-pack\n'
                           '0000'
                           # input is ignored by the handler
                           'handled input: '), write_output)
        # Ensure all output was written via the write callback.
        self.assertEqual('', handler_output)
        self.assertTrue(self._handler.advertise_refs)
        self.assertTrue(self._handler.http_req)
        self.assertFalse(self._req.cached)


class LengthLimitedFileTestCase(TestCase):
    def test_no_cutoff(self):
        f = _LengthLimitedFile(StringIO('foobar'), 1024)
        self.assertEqual('foobar', f.read())

    def test_cutoff(self):
        f = _LengthLimitedFile(StringIO('foobar'), 3)
        self.assertEqual('foo', f.read())
        self.assertEqual('', f.read())

    def test_multiple_reads(self):
        f = _LengthLimitedFile(StringIO('foobar'), 3)
        self.assertEqual('fo', f.read(2))
        self.assertEqual('o', f.read(2))
        self.assertEqual('', f.read())


class HTTPGitRequestTestCase(WebTestCase):

    # This class tests the contents of the actual cache headers
    _req_class = HTTPGitRequest

    def test_not_found(self):
        self._req.cache_forever()  # cache headers should be discarded
        message = 'Something not found'
        self.assertEqual(message, self._req.not_found(message))
        self.assertEqual(HTTP_NOT_FOUND, self._status)
        self.assertEqual(set([('Content-Type', 'text/plain')]),
                          set(self._headers))

    def test_forbidden(self):
        self._req.cache_forever()  # cache headers should be discarded
        message = 'Something not found'
        self.assertEqual(message, self._req.forbidden(message))
        self.assertEqual(HTTP_FORBIDDEN, self._status)
        self.assertEqual(set([('Content-Type', 'text/plain')]),
                          set(self._headers))

    def test_respond_ok(self):
        self._req.respond()
        self.assertEqual([], self._headers)
        self.assertEqual(HTTP_OK, self._status)

    def test_respond(self):
        self._req.nocache()
        self._req.respond(status=402, content_type='some/type',
                          headers=[('X-Foo', 'foo'), ('X-Bar', 'bar')])
        self.assertEqual(set([
          ('X-Foo', 'foo'),
          ('X-Bar', 'bar'),
          ('Content-Type', 'some/type'),
          ('Expires', 'Fri, 01 Jan 1980 00:00:00 GMT'),
          ('Pragma', 'no-cache'),
          ('Cache-Control', 'no-cache, max-age=0, must-revalidate'),
          ]), set(self._headers))
        self.assertEqual(402, self._status)


class HTTPGitApplicationTestCase(TestCase):

    def setUp(self):
        super(HTTPGitApplicationTestCase, self).setUp()
        self._app = HTTPGitApplication('backend')

        self._environ = {
            'PATH_INFO': '/foo',
            'REQUEST_METHOD': 'GET',
        }

    def _test_handler(self, req, backend, mat):
        # tests interface used by all handlers
        self.assertEqual(self._environ, req.environ)
        self.assertEqual('backend', backend)
        self.assertEqual('/foo', mat.group(0))
        return 'output'

    def _add_handler(self, app):
        req = self._environ['REQUEST_METHOD']
        app.services = {
          (req, re.compile('/foo$')): self._test_handler,
        }

    def test_call(self):
        self._add_handler(self._app)
        self.assertEqual('output', self._app(self._environ, None))

    def test_fallback_app(self):
        def test_app(environ, start_response):
            return 'output'

        app = HTTPGitApplication('backend', fallback_app=test_app)
        self.assertEqual('output', app(self._environ, None))


class GunzipTestCase(HTTPGitApplicationTestCase):
    """TestCase for testing the GunzipFilter, ensuring the wsgi.input
    is correctly decompressed and headers are corrected.
    """

    def setUp(self):
        super(GunzipTestCase, self).setUp()
        self._app = GunzipFilter(self._app)
        self._environ['HTTP_CONTENT_ENCODING'] = 'gzip'
        self._environ['REQUEST_METHOD'] = 'POST'

    def _get_zstream(self, text):
        zstream = StringIO()
        zfile = gzip.GzipFile(fileobj=zstream, mode='w')
        zfile.write(text)
        zfile.close()
        return zstream

    def test_call(self):
        self._add_handler(self._app.app)
        orig = self.__class__.__doc__
        zstream = self._get_zstream(orig)
        zlength = zstream.tell()
        zstream.seek(0)
        self.assertLess(zlength, len(orig))
        self.assertEqual(self._environ['HTTP_CONTENT_ENCODING'], 'gzip')
        self._environ['CONTENT_LENGTH'] = zlength
        self._environ['wsgi.input'] = zstream
        app_output = self._app(self._environ, None)
        buf = self._environ['wsgi.input']
        self.assertIsNot(buf, zstream)
        buf.seek(0)
        self.assertEqual(orig, buf.read())
        self.assertIs(None, self._environ.get('CONTENT_LENGTH'))
        self.assertNotIn('HTTP_CONTENT_ENCODING', self._environ)
