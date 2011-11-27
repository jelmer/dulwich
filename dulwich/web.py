# web.py -- WSGI smart-http server
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

"""HTTP server for dulwich that implements the git smart HTTP protocol."""

from cStringIO import StringIO
import os
import re
import sys
import time

try:
    from urlparse import parse_qs
except ImportError:
    from dulwich._compat import parse_qs
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
    f = get_repo(backend, mat).get_named_file(path)
    if f is None:
        raise NoSuchFileException('File not found')
    return send_file(req, f, 'text/plain')


def send_loose_object(req, object_store, sha):
    try:
        data = object_store[sha].as_legacy_object()
    except IOError:
        yield req.error('Error reading object')
        return
    req.cache_forever()
    req.respond(HTTP_OK, 'application/x-git-loose-object')
    yield data


def get_loose_object(req, backend, mat):
    sha = mat.group(1) + mat.group(2)
    logger.info('Sending loose object %s', sha)
    object_store = get_repo(backend, mat).object_store
    if not object_store.contains_loose(sha):
        raise NoSuchFileException('Object not found')
    return send_loose_object(req, object_store, sha)


def get_pack_file(req, backend, mat):
    req.cache_forever()
    path = _url_to_path(mat.group())
    logger.info('Sending pack file %s', path)
    f = get_repo(backend, mat).get_named_file(path)
    if f is None:
        raise NoSuchFileException('File not found')
    return send_file(req, f, 'application/x-git-packed-objects')


def get_idx_file(req, backend, mat):
    req.cache_forever()
    path = _url_to_path(mat.group())
    logger.info('Sending pack file %s', path)
    f = get_repo(backend, mat).get_named_file(path)
    if f is None:
        raise NoSuchFileException('File not found')
    return send_file(req, f, 'application/x-git-packed-objects-toc')


def get_info_refs(req, backend, mat):
    params = parse_qs(req.environ['QUERY_STRING'])
    service = params.get('service', [None])[0]
    if service and not req.dumb:
        handler_cls = req.handlers.get(service, None)
        if handler_cls is None:
            yield req.forbidden('Unsupported service %s' % service)
            return
        req.nocache()
        write = req.respond(HTTP_OK, 'application/x-%s-advertisement' % service)
        proto = ReceivableProtocol(StringIO().read, write)
        handler = handler_cls(backend, [url_prefix(mat)], proto,
                              http_req=req, advertise_refs=True)
        handler.proto.write_pkt_line('# service=%s\n' % service)
        handler.proto.write_pkt_line(None)
        handler.handle()
    else:
        # non-smart fallback
        # TODO: select_getanyfile() (see http-backend.c)
        req.nocache()
        req.respond(HTTP_OK, 'text/plain')
        logger.info('Emulating dumb info/refs')
        repo = get_repo(backend, mat)
        refs = repo.get_refs()
        for name in sorted(refs.iterkeys()):
            # get_refs() includes HEAD as a special case, but we don't want to
            # advertise it
            if name == 'HEAD':
                continue
            sha = refs[name]
            o = repo[sha]
            if not o:
                continue
            yield '%s\t%s\n' % (sha, name)
            peeled_sha = repo.get_peeled(name)
            if peeled_sha != sha:
                yield '%s\t%s^{}\n' % (peeled_sha, name)


def get_info_packs(req, backend, mat):
    req.nocache()
    req.respond(HTTP_OK, 'text/plain')
    logger.info('Emulating dumb info/packs')
    for pack in get_repo(backend, mat).object_store.packs:
        yield 'P pack-%s.pack\n' % pack.name()


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
            return ''
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
        yield req.forbidden('Unsupported service %s' % service)
        return
    req.nocache()
    write = req.respond(HTTP_OK, 'application/x-%s-result' % service)

    input = req.environ['wsgi.input']
    # This is not necessary if this app is run from a conforming WSGI server.
    # Unfortunately, there's no way to tell that at this point.
    # TODO: git may used HTTP/1.1 chunked encoding instead of specifying
    # content-length
    content_length = req.environ.get('CONTENT_LENGTH', '')
    if content_length:
        input = _LengthLimitedFile(input, int(content_length))
    proto = ReceivableProtocol(input.read, write)
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

    def __init__(self, backend, dumb=False, handlers=None):
        self.backend = backend
        self.dumb = dumb
        self.handlers = dict(DEFAULT_HANDLERS)
        if handlers is not None:
            self.handlers.update(handlers)

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']
        method = environ['REQUEST_METHOD']
        req = HTTPGitRequest(environ, start_response, dumb=self.dumb,
                             handlers=self.handlers)
        # environ['QUERY_STRING'] has qs args
        handler = None
        for smethod, spath in self.services.iterkeys():
            if smethod != method:
                continue
            mat = spath.search(path)
            if mat:
                handler = self.services[smethod, spath]
                break
        if handler is None:
            return req.not_found('Sorry, that method is not supported')

        # When a dumb client asks for a file, and the server replies with 404,
        # it will immediately disconnect. Unfortunately for us, wsgi doesn't
        # know this, and treats everything as a normal message. Thus it still
        # tries to send a message (e.g. "<html>oops something went horribly
        # wrong</html>") which leads to - you guessed it - broken pipe errors.
        # This nifty little trick instructs wsgi to treat this request as a
        # file, meaning don't even bother to send a message. Just hangup.

        try:
            return handler(req, self.backend, mat)
        except NoSuchFileException, e:
            if self.dumb:
                file_wrapper = environ.get('wsgi.file_wrapper', None)
                set_sendfile = environ.get('wsgi.set_sendfile', None)
                if file_wrapper and set_sendfile:
                    req.not_found(None)
                    set_sendfile(True)
                    return file_wrapper(StringIO())
            return (req.not_found(e.message),)

class NoSuchFileException(Exception):
    """A file doesn't exist."""

    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


# The reference server implementation is based on wsgiref, which is not
# distributed with python 2.4. If wsgiref is not present, users will not be able
# to use the HTTP server without a little extra work.
try:
    from wsgiref.simple_server import (
        WSGIRequestHandler,
        ServerHandler,
        make_server,
        )

    class HTTPGitServerHandler(ServerHandler):
        """The default sendfile is hard-coded to return False. This class exists
        solely to provide the ability to set this.
        """

        def __init__(self, stdin, stdout, stderr, environ):
            ServerHandler.__init__(self, stdin, stdout, stderr, environ)
            self._sendfile = False

        def sendfile(self):
            """ServerHandler.finish_response uses this (partially) to decide
            whether or not to send an error message to a client.
            """
            return self._sendfile

        def setup_environ(self):
            ServerHandler.setup_environ(self)

            # A little bit hackish, but this gives the HTTPGitApplication class
            # the ability to easily tell WSGI that this request is supposed to
            # be returning a file.
            def set_sendfile(val):
                self._sendfile = val
            self.environ['wsgi.set_sendfile'] = set_sendfile

    class HTTPGitRequestHandler(WSGIRequestHandler):
        """Handler that uses dulwich's logger for logging exceptions."""

        def log_exception(self, exc_info):
            logger.exception('Exception happened during processing of request',
                             exc_info=exc_info)

        def log_message(self, format, *args):
            logger.info(format, *args)

        def log_error(self, *args):
            logger.error(*args)

        # Copied verbatim from simple_server.py, changed ServerHandler to HTTPGitServerHandler
        def handle(self):
            """Handle a single HTTP request"""

            self.raw_requestline = self.rfile.readline()
            if not self.parse_request(): # An error code has been sent, just exit
                return

            handler = HTTPGitServerHandler(
                self.rfile, self.wfile, self.get_stderr(), self.get_environ()
            )
            handler.request_handler = self      # backpointer for logging
            handler.run(self.server.get_app())

    def main(argv=sys.argv):
        """Entry point for starting an HTTP git server."""
        if len(argv) > 1:
            gitdir = argv[1]
        else:
            gitdir = os.getcwd()

        # TODO: allow serving on other addresses/ports via command-line flag
        listen_addr=''
        port = 8000

        log_utils.default_logging_config()
        backend = DictBackend({'/': Repo(gitdir)})
        app = HTTPGitApplication(backend)
        server = make_server(listen_addr, port, app,
                             handler_class=HTTPGitRequestHandler)
        logger.info('Listening for HTTP connections on %s:%d', listen_addr,
                    port)
        server.serve_forever()

except ImportError:
    # No wsgiref found; don't provide the reference functionality, but leave the
    # rest of the WSGI-based implementation.
    def main(argv=sys.argv):
        """Stub entry point for failing to start a server without wsgiref."""
        sys.stderr.write('Sorry, the wsgiref module is required for dul-web.\n')
        sys.exit(1)
