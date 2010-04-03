# web.py -- WSGI smart-http server
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

"""HTTP server for dulwich that implements the git smart HTTP protocol."""

from cStringIO import StringIO
import re
import time
import urlparse

from dulwich.server import (
    ReceivePackHandler,
    UploadPackHandler,
    )

HTTP_OK = '200 OK'
HTTP_NOT_FOUND = '404 Not Found'
HTTP_FORBIDDEN = '403 Forbidden'


def date_time_string(timestamp=None):
    # Based on BaseHTTPServer.py in python2.5
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
    return backend.open_repository(url_prefix(mat)).repo


def send_file(req, f, content_type):
    """Send a file-like object to the request output.

    :param req: The HTTPGitRequest object to send output to.
    :param f: An open file-like object to send; will be closed.
    :param content_type: The MIME type for the file.
    :yield: The contents of the file.
    """
    if f is None:
        yield req.not_found('File not found')
        return
    try:
        try:
            req.respond(HTTP_OK, content_type)
            while True:
                data = f.read(10240)
                if not data:
                    break
                yield data
        except IOError:
            yield req.not_found('Error reading file')
    finally:
        f.close()


def get_text_file(req, backend, mat):
    req.nocache()
    return send_file(req, get_repo(backend, mat).get_named_file(mat.group()),
                     'text/plain')


def get_loose_object(req, backend, mat):
    sha = mat.group(1) + mat.group(2)
    object_store = get_repo(backend, mat).object_store
    if not object_store.contains_loose(sha):
        yield req.not_found('Object not found')
        return
    try:
        data = object_store[sha].as_legacy_object()
    except IOError:
        yield req.not_found('Error reading object')
    req.cache_forever()
    req.respond(HTTP_OK, 'application/x-git-loose-object')
    yield data


def get_pack_file(req, backend, mat):
    req.cache_forever()
    return send_file(req, get_repo(backend, mat).get_named_file(mat.group()),
                     'application/x-git-packed-objects')


def get_idx_file(req, backend, mat):
    req.cache_forever()
    return send_file(req, get_repo(backend, mat).get_named_file(mat.group()),
                     'application/x-git-packed-objects-toc')


default_services = {'git-upload-pack': UploadPackHandler,
                    'git-receive-pack': ReceivePackHandler}
def get_info_refs(req, backend, mat, services=None):
    if services is None:
        services = default_services
    params = urlparse.parse_qs(req.environ['QUERY_STRING'])
    service = params.get('service', [None])[0]
    if service and not req.dumb:
        handler_cls = services.get(service, None)
        if handler_cls is None:
            yield req.forbidden('Unsupported service %s' % service)
            return
        req.nocache()
        req.respond(HTTP_OK, 'application/x-%s-advertisement' % service)
        output = StringIO()
        dummy_input = StringIO()  # GET request, handler doesn't need to read
        handler = handler_cls(backend, [url_prefix(mat)],
                              dummy_input.read, output.write,
                              stateless_rpc=True, advertise_refs=True)
        handler.proto.write_pkt_line('# service=%s\n' % service)
        handler.proto.write_pkt_line(None)
        handler.handle()
        yield output.getvalue()
    else:
        # non-smart fallback
        # TODO: select_getanyfile() (see http-backend.c)
        req.nocache()
        req.respond(HTTP_OK, 'text/plain')
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


def handle_service_request(req, backend, mat, services=None):
    if services is None:
        services = default_services
    service = mat.group().lstrip('/')
    handler_cls = services.get(service, None)
    if handler_cls is None:
        yield req.forbidden('Unsupported service %s' % service)
        return
    req.nocache()
    req.respond(HTTP_OK, 'application/x-%s-response' % service)

    output = StringIO()
    input = req.environ['wsgi.input']
    # This is not necessary if this app is run from a conforming WSGI server.
    # Unfortunately, there's no way to tell that at this point.
    # TODO: git may used HTTP/1.1 chunked encoding instead of specifying
    # content-length
    if 'CONTENT_LENGTH' in req.environ:
        input = _LengthLimitedFile(input, int(req.environ['CONTENT_LENGTH']))
    handler = handler_cls(backend, [url_prefix(mat)], input.read, output.write,
                          stateless_rpc=True)
    handler.handle()
    yield output.getvalue()


class HTTPGitRequest(object):
    """Class encapsulating the state of a single git HTTP request.

    :ivar environ: the WSGI environment for the request.
    """

    def __init__(self, environ, start_response, dumb=False):
        self.environ = environ
        self.dumb = dumb
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

        self._start_response(status, self._headers)

    def not_found(self, message):
        """Begin a HTTP 404 response and return the text of a message."""
        self._cache_headers = []
        self.respond(HTTP_NOT_FOUND, 'text/plain')
        return message

    def forbidden(self, message):
        """Begin a HTTP 403 response and return the text of a message."""
        self._cache_headers = []
        self.respond(HTTP_FORBIDDEN, 'text/plain')
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

    def __init__(self, backend, dumb=False):
        self.backend = backend
        self.dumb = dumb

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']
        method = environ['REQUEST_METHOD']
        req = HTTPGitRequest(environ, start_response, self.dumb)
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
        return handler(req, self.backend, mat)
