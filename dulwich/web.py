# web.py -- WSGI smart-http server
# Copyright (C) 2010 Google, Inc.
# Copyright (C) 2012 Jelmer Vernooij <jelmer@samba.org>
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

"""HTTP server for dulwich that implements the git smart HTTP protocol."""

from io import BytesIO
import shutil
import tempfile
import gzip
import os
import re
import sys
import time
from wsgiref.simple_server import (
    WSGIRequestHandler,
    ServerHandler,
    WSGIServer,
    make_server,
    )

try:
    from urlparse import parse_qs
except ImportError:
    from urllib.parse import parse_qs


from dulwich import log_utils
from dulwich.protocol import (
    ReceivableProtocol,
    )
from dulwich.repo import (
    Repo,
    )
from dulwich.server import (
    DictBackend,
    DEFAULT_HANDLERS,
    generate_info_refs,
    generate_objects_info_packs,
    )


logger = log_utils.getLogger(__name__)


# HTTP error strings
HTTP_OK = '200 OK'
HTTP_NOT_FOUND = '404 Not Found'
HTTP_FORBIDDEN = '403 Forbidden'
HTTP_ERROR = '500 Internal Server Error'


def date_time_string(timestamp=None):
    # From BaseHTTPRequestHandler.date_time_string in BaseHTTPServer.py in the
    # Python 2.6.5 standard library, following modifications:
    #  - Made a global rather than an instance method.
    #  - weekdayname and monthname are renamed and locals rather than class
    #    variables.
    # Copyright (c) 2001-2010 Python Software Foundation; All Rights Reserved
    weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    months = [None,
              'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    if timestamp is None:
        timestamp = time.time()
    year, month, day, hh, mm, ss, wd, y, z = time.gmtime(timestamp)
    return '%s, %02d %3s %4d %02d:%02d:%02d GMD' % (
            weekdays[wd], day, months[month], year, hh, mm, ss)


def url_prefix(mat):
    """Extract the URL prefix from a regex match.

    :param mat: A regex match object.
    :returns: The URL prefix, defined as the text before the match in the
        original string. Normalized to start with one leading slash and end with
        zero.
    """
    return '/' + mat.string[:mat.start()].strip('/')


def get_repo(backend, mat):
    """Get a Repo instance for the given backend and URL regex match."""
    return backend.open_repository(url_prefix(mat))


def send_file(req, f, content_type):
    """Send a file-like object to the request output.

    :param req: The HTTPGitRequest object to send output to.
    :param f: An open file-like object to send; will be closed.
    :param content_type: The MIME type for the file.
    :return: Iterator over the contents of the file, as chunks.
    """
    if f is None:
        yield req.not_found('File not found')
        return
    try:
        req.respond(HTTP_OK, content_type)
        while True:
            data = f.read(10240)
            if not data:
                break
            yield data
        f.close()
    except IOError:
        f.close()
        yield req.error('Error reading file')
    except:
        f.close()
        raise


def _url_to_path(url):
    return url.replace('/', os.path.sep)


def get_text_file(req, backend, mat):
    req.nocache()
    path = _url_to_path(mat.group())
    logger.info('Sending plain text file %s', path)
    return send_file(req, get_repo(backend, mat).get_named_file(path),
                     'text/plain')


def get_loose_object(req, backend, mat):
    sha = (mat.group(1) + mat.group(2)).encode('ascii')
    logger.info('Sending loose object %s', sha)
    object_store = get_repo(backend, mat).object_store
    if not object_store.contains_loose(sha):
        yield req.not_found('Object not found')
        return
    try:
        data = object_store[sha].as_legacy_object()
    except IOError:
        yield req.error('Error reading object')
        return
    req.cache_forever()
    req.respond(HTTP_OK, 'application/x-git-loose-object')
    yield data


def get_pack_file(req, backend, mat):
    req.cache_forever()
    path = _url_to_path(mat.group())
    logger.info('Sending pack file %s', path)
    return send_file(req, get_repo(backend, mat).get_named_file(path),
                     'application/x-git-packed-objects')


def get_idx_file(req, backend, mat):
    req.cache_forever()
    path = _url_to_path(mat.group())
    logger.info('Sending pack file %s', path)
    return send_file(req, get_repo(backend, mat).get_named_file(path),
                     'application/x-git-packed-objects-toc')


def get_info_refs(req, backend, mat):
    params = parse_qs(req.environ['QUERY_STRING'])
    service = params.get('service', [None])[0]
    if service and not req.dumb:
        handler_cls = req.handlers.get(service, None)
        if handler_cls is None:
            yield req.forbidden('Unsupported service')
            return
        req.nocache()
        write = req.respond(HTTP_OK, 'application/x-%s-advertisement' % service)
        proto = ReceivableProtocol(BytesIO().read, write)
        handler = handler_cls(backend, [url_prefix(mat)], proto,
                              http_req=req, advertise_refs=True)
        handler.proto.write_pkt_line(b'# service=' + service.encode('ascii') + b'\n')
        handler.proto.write_pkt_line(None)
        handler.handle()
    else:
        # non-smart fallback
        # TODO: select_getanyfile() (see http-backend.c)
        req.nocache()
        req.respond(HTTP_OK, 'text/plain')
        logger.info('Emulating dumb info/refs')
        repo = get_repo(backend, mat)
        for text in generate_info_refs(repo):
            yield text


def get_info_packs(req, backend, mat):
    req.nocache()
    req.respond(HTTP_OK, 'text/plain')
    logger.info('Emulating dumb info/packs')
    return generate_objects_info_packs(get_repo(backend, mat))


class _LengthLimitedFile(object):
    """Wrapper class to limit the length of reads from a file-like object.

    This is used to ensure EOF is read from the wsgi.input object once
    Content-Length bytes are read. This behavior is required by the WSGI spec
    but not implemented in wsgiref as of 2.5.
    """

    def __init__(self, input, max_bytes):
        self._input = input
        self._bytes_avail = max_bytes

    def read(self, size=-1):
        if self._bytes_avail <= 0:
            return b''
        if size == -1 or size > self._bytes_avail:
            size = self._bytes_avail
        self._bytes_avail -= size
        return self._input.read(size)

    # TODO: support more methods as necessary


def handle_service_request(req, backend, mat):
    service = mat.group().lstrip('/')
    logger.info('Handling service request for %s', service)
    handler_cls = req.handlers.get(service, None)
    if handler_cls is None:
        yield req.forbidden('Unsupported service')
        return
    req.nocache()
    write = req.respond(HTTP_OK, 'application/x-%s-result' % service)
    proto = ReceivableProtocol(req.environ['wsgi.input'].read, write)
    handler = handler_cls(backend, [url_prefix(mat)], proto, http_req=req)
    handler.handle()


class HTTPGitRequest(object):
    """Class encapsulating the state of a single git HTTP request.

    :ivar environ: the WSGI environment for the request.
    """

    def __init__(self, environ, start_response, dumb=False, handlers=None):
        self.environ = environ
        self.dumb = dumb
        self.handlers = handlers
        self._start_response = start_response
        self._cache_headers = []
        self._headers = []

    def add_header(self, name, value):
        """Add a header to the response."""
        self._headers.append((name, value))

    def respond(self, status=HTTP_OK, content_type=None, headers=None):
        """Begin a response with the given status and other headers."""
        if headers:
            self._headers.extend(headers)
        if content_type:
            self._headers.append(('Content-Type', content_type))
        self._headers.extend(self._cache_headers)

        return self._start_response(status, self._headers)

    def not_found(self, message):
        """Begin a HTTP 404 response and return the text of a message."""
        self._cache_headers = []
        logger.info('Not found: %s', message)
        self.respond(HTTP_NOT_FOUND, 'text/plain')
        return message

    def forbidden(self, message):
        """Begin a HTTP 403 response and return the text of a message."""
        self._cache_headers = []
        logger.info('Forbidden: %s', message)
        self.respond(HTTP_FORBIDDEN, 'text/plain')
        return message

    def error(self, message):
        """Begin a HTTP 500 response and return the text of a message."""
        self._cache_headers = []
        logger.error('Error: %s', message)
        self.respond(HTTP_ERROR, 'text/plain')
        return message

    def nocache(self):
        """Set the response to never be cached by the client."""
        self._cache_headers = [
          ('Expires', 'Fri, 01 Jan 1980 00:00:00 GMT'),
          ('Pragma', 'no-cache'),
          ('Cache-Control', 'no-cache, max-age=0, must-revalidate'),
          ]

    def cache_forever(self):
        """Set the response to be cached forever by the client."""
        now = time.time()
        self._cache_headers = [
          ('Date', date_time_string(now)),
          ('Expires', date_time_string(now + 31536000)),
          ('Cache-Control', 'public, max-age=31536000'),
          ]


class HTTPGitApplication(object):
    """Class encapsulating the state of a git WSGI application.

    :ivar backend: the Backend object backing this application
    """

    services = {
      ('GET', re.compile('/HEAD$')): get_text_file,
      ('GET', re.compile('/info/refs$')): get_info_refs,
      ('GET', re.compile('/objects/info/alternates$')): get_text_file,
      ('GET', re.compile('/objects/info/http-alternates$')): get_text_file,
      ('GET', re.compile('/objects/info/packs$')): get_info_packs,
      ('GET', re.compile('/objects/([0-9a-f]{2})/([0-9a-f]{38})$')): get_loose_object,
      ('GET', re.compile('/objects/pack/pack-([0-9a-f]{40})\\.pack$')): get_pack_file,
      ('GET', re.compile('/objects/pack/pack-([0-9a-f]{40})\\.idx$')): get_idx_file,

      ('POST', re.compile('/git-upload-pack$')): handle_service_request,
      ('POST', re.compile('/git-receive-pack$')): handle_service_request,
    }

    def __init__(self, backend, dumb=False, handlers=None, fallback_app=None):
        self.backend = backend
        self.dumb = dumb
        self.handlers = dict(DEFAULT_HANDLERS)
        self.fallback_app = fallback_app
        if handlers is not None:
            self.handlers.update(handlers)

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']
        method = environ['REQUEST_METHOD']
        req = HTTPGitRequest(environ, start_response, dumb=self.dumb,
                             handlers=self.handlers)
        # environ['QUERY_STRING'] has qs args
        handler = None
        for smethod, spath in self.services.keys():
            if smethod != method:
                continue
            mat = spath.search(path)
            if mat:
                handler = self.services[smethod, spath]
                break

        if handler is None:
            if self.fallback_app is not None:
                return self.fallback_app(environ, start_response)
            else:
                return req.not_found('Sorry, that method is not supported')

        return handler(req, self.backend, mat)


class GunzipFilter(object):
    """WSGI middleware that unzips gzip-encoded requests before
    passing on to the underlying application.
    """

    def __init__(self, application):
        self.app = application

    def __call__(self, environ, start_response):
        if environ.get('HTTP_CONTENT_ENCODING', '') == 'gzip':
            if hasattr(environ['wsgi.input'], 'seek'):
                wsgi_input = environ['wsgi.input']
            else:
                # The gzip implementation in the standard library of Python 2.x
                # requires the '.seek()' and '.tell()' methods to be available
                # on the input stream.  Read the data into a temporary file to
                # work around this limitation.
                wsgi_input = tempfile.SpooledTemporaryFile(16 * 1024 * 1024)
                shutil.copyfileobj(environ['wsgi.input'], wsgi_input)
                wsgi_input.seek(0)

            environ['wsgi.input'] = gzip.GzipFile(filename=None, fileobj=wsgi_input, mode='r')
            del environ['HTTP_CONTENT_ENCODING']
            if 'CONTENT_LENGTH' in environ:
                del environ['CONTENT_LENGTH']

        return self.app(environ, start_response)


class LimitedInputFilter(object):
    """WSGI middleware that limits the input length of a request to that
    specified in Content-Length.
    """

    def __init__(self, application):
        self.app = application

    def __call__(self, environ, start_response):
        # This is not necessary if this app is run from a conforming WSGI
        # server. Unfortunately, there's no way to tell that at this point.
        # TODO: git may used HTTP/1.1 chunked encoding instead of specifying
        # content-length
        content_length = environ.get('CONTENT_LENGTH', '')
        if content_length:
            environ['wsgi.input'] = _LengthLimitedFile(
                environ['wsgi.input'], int(content_length))
        return self.app(environ, start_response)


def make_wsgi_chain(*args, **kwargs):
    """Factory function to create an instance of HTTPGitApplication,
    correctly wrapped with needed middleware.
    """
    app = HTTPGitApplication(*args, **kwargs)
    wrapped_app = GunzipFilter(LimitedInputFilter(app))
    return wrapped_app


class ServerHandlerLogger(ServerHandler):
    """ServerHandler that uses dulwich's logger for logging exceptions."""

    def log_exception(self, exc_info):
        if sys.version < (2, 7):
            logger.exception('Exception happened during processing of request')
        else:
            logger.exception('Exception happened during processing of request',
                             exc_info=exc_info)

    def log_message(self, format, *args):
        logger.info(format, *args)

    def log_error(self, *args):
        logger.error(*args)


class WSGIRequestHandlerLogger(WSGIRequestHandler):
    """WSGIRequestHandler that uses dulwich's logger for logging exceptions."""

    def log_exception(self, exc_info):
        logger.exception('Exception happened during processing of request',
                         exc_info=exc_info)

    def log_message(self, format, *args):
        logger.info(format, *args)

    def log_error(self, *args):
        logger.error(*args)

    def handle(self):
        """Handle a single HTTP request"""

        self.raw_requestline = self.rfile.readline()
        if not self.parse_request(): # An error code has been sent, just exit
            return

        handler = ServerHandlerLogger(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self      # backpointer for logging
        handler.run(self.server.get_app())


class WSGIServerLogger(WSGIServer):

    def handle_error(self, request, client_address):
        """Handle an error. """
        logger.exception('Exception happened during processing of request from %s' % str(client_address))


def main(argv=sys.argv):
    """Entry point for starting an HTTP git server."""
    import optparse
    parser = optparse.OptionParser()
    parser.add_option("-l", "--listen_address", dest="listen_address",
                      default="localhost",
                      help="Binding IP address.")
    parser.add_option("-p", "--port", dest="port", type=int,
                      default=8000,
                      help="Port to listen on.")
    options, args = parser.parse_args(argv)

    if len(args) > 1:
        gitdir = args[1]
    else:
        gitdir = os.getcwd()

    log_utils.default_logging_config()
    backend = DictBackend({'/': Repo(gitdir)})
    app = make_wsgi_chain(backend)
    server = make_server(options.listen_address, options.port, app,
                         handler_class=WSGIRequestHandlerLogger,
                         server_class=WSGIServerLogger)
    logger.info('Listening for HTTP connections on %s:%d',
                options.listen_address, options.port)
    server.serve_forever()


if __name__ == '__main__':
    main()
