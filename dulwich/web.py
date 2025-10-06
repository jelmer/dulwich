# web.py -- WSGI smart-http server
# Copyright (C) 2010 Google, Inc.
# Copyright (C) 2012 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# You should have received a copy of the licenses; if not, see
# <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
# and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
# License, Version 2.0.
#

"""HTTP server for dulwich that implements the git smart HTTP protocol."""

__all__ = [
    "HTTP_FORBIDDEN",
    "HTTP_NOT_FOUND",
    "HTTP_OK",
    "GunzipFilter",
    "HTTPGitApplication",
    "HTTPGitRequest",
    "LimitedInputFilter",
    "WSGIRequestHandlerLogger",
    "WSGIServerLogger",
    "date_time_string",
    "generate_info_refs",
    "generate_objects_info_packs",
    "get_info_packs",
    "get_info_refs",
    "get_loose_object",
    "get_pack_file",
    "get_text_file",
    "handle_service_request",
    "main",
    "make_server",
    "make_wsgi_chain",
    "send_file",
]

import os
import re
import sys
import time
from collections.abc import Iterable, Iterator, Sequence
from io import BytesIO
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    ClassVar,
    Optional,
    Union,
    cast,
)
from urllib.parse import parse_qs
from wsgiref.simple_server import (
    ServerHandler,
    WSGIRequestHandler,
    WSGIServer,
    make_server,
)

# wsgiref.types was added in Python 3.11
if sys.version_info >= (3, 11):
    from wsgiref.types import StartResponse, WSGIApplication, WSGIEnvironment
else:
    # Fallback type definitions for Python < 3.11
    if TYPE_CHECKING:
        # For type checking, use the _typeshed types if available
        try:
            from _typeshed.wsgi import StartResponse, WSGIApplication, WSGIEnvironment
        except ImportError:
            # Define our own protocol types for type checking
            from typing import Protocol as TypingProtocol

            class StartResponse(TypingProtocol):  # type: ignore[no-redef]
                """WSGI start_response callable protocol."""

                def __call__(
                    self,
                    status: str,
                    response_headers: list[tuple[str, str]],
                    exc_info: Optional[
                        tuple[type, BaseException, TracebackType]
                    ] = None,
                ) -> Callable[[bytes], None]:
                    """Start the response with status and headers."""
                    ...

            WSGIEnvironment = dict[str, Any]  # type: ignore[misc]
            WSGIApplication = Callable[  # type: ignore[misc]
                [WSGIEnvironment, StartResponse], Iterable[bytes]
            ]
    else:
        # At runtime, just use type aliases since these are only for type hints
        StartResponse = Any
        WSGIEnvironment = dict[str, Any]
        WSGIApplication = Callable

from dulwich import log_utils

from .errors import NotGitRepository
from .protocol import ReceivableProtocol
from .repo import BaseRepo, Repo
from .server import (
    DEFAULT_HANDLERS,
    Backend,
    DictBackend,
    Handler,
    generate_info_refs,
    generate_objects_info_packs,
)

if TYPE_CHECKING:
    from typing import Protocol as TypingProtocol

    from .protocol import Protocol

    class HandlerConstructor(TypingProtocol):
        """Protocol for handler constructors."""

        def __call__(
            self,
            backend: Backend,
            args: list[bytes],
            proto: Protocol,
            stateless_rpc: bool = False,
            advertise_refs: bool = False,
        ) -> Handler:
            """Create a handler instance.

            Args:
                backend: The backend to use for the handler
                args: Arguments for the handler
                proto: Protocol object for communication
                stateless_rpc: Whether to use stateless RPC mode
                advertise_refs: Whether to advertise references

            Returns:
                A Handler instance
            """
            ...


logger = log_utils.getLogger(__name__)


# HTTP error strings
HTTP_OK = "200 OK"
HTTP_NOT_FOUND = "404 Not Found"
HTTP_FORBIDDEN = "403 Forbidden"
HTTP_ERROR = "500 Internal Server Error"


NO_CACHE_HEADERS = [
    ("Expires", "Fri, 01 Jan 1980 00:00:00 GMT"),
    ("Pragma", "no-cache"),
    ("Cache-Control", "no-cache, max-age=0, must-revalidate"),
]


def cache_forever_headers(now: Optional[float] = None) -> list[tuple[str, str]]:
    """Generate headers for caching forever.

    Args:
      now: Timestamp to use as base (defaults to current time)

    Returns:
      List of (header_name, header_value) tuples for caching forever
    """
    if now is None:
        now = time.time()
    return [
        ("Date", date_time_string(now)),
        ("Expires", date_time_string(now + 31536000)),
        ("Cache-Control", "public, max-age=31536000"),
    ]


def date_time_string(timestamp: Optional[float] = None) -> str:
    """Convert a timestamp to an HTTP date string.

    Args:
      timestamp: Unix timestamp to convert (defaults to current time)

    Returns:
      HTTP date string in RFC 1123 format
    """
    # From BaseHTTPRequestHandler.date_time_string in BaseHTTPServer.py in the
    # Python 2.6.5 standard library, following modifications:
    #  - Made a global rather than an instance method.
    #  - weekdayname and monthname are renamed and locals rather than class
    #    variables.
    # Copyright (c) 2001-2010 Python Software Foundation; All Rights Reserved
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    months = [
        None,
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    if timestamp is None:
        timestamp = time.time()
    year, month, day, hh, mm, ss, wd = time.gmtime(timestamp)[:7]
    return "%s, %02d %3s %4d %02d:%02d:%02d GMD" % (  # noqa: UP031
        weekdays[wd],
        day,
        months[month],
        year,
        hh,
        mm,
        ss,
    )


def url_prefix(mat: re.Match[str]) -> str:
    """Extract the URL prefix from a regex match.

    Args:
      mat: A regex match object.
    Returns: The URL prefix, defined as the text before the match in the
        original string. Normalized to start with one leading slash and end
        with zero.
    """
    return "/" + mat.string[: mat.start()].strip("/")


def get_repo(backend: "Backend", mat: re.Match[str]) -> BaseRepo:
    """Get a Repo instance for the given backend and URL regex match."""
    return cast(BaseRepo, backend.open_repository(url_prefix(mat)))


def send_file(
    req: "HTTPGitRequest", f: Optional[BinaryIO], content_type: str
) -> Iterator[bytes]:
    """Send a file-like object to the request output.

    Args:
      req: The HTTPGitRequest object to send output to.
      f: An open file-like object to send; will be closed.
      content_type: The MIME type for the file.
    Returns: Iterator over the contents of the file, as chunks.
    """
    if f is None:
        yield req.not_found("File not found")
        return
    try:
        req.respond(HTTP_OK, content_type)
        while True:
            data = f.read(10240)
            if not data:
                break
            yield data
    except OSError:
        yield req.error("Error reading file")
    finally:
        f.close()


def _url_to_path(url: str) -> str:
    return url.replace("/", os.path.sep)


def get_text_file(
    req: "HTTPGitRequest", backend: "Backend", mat: re.Match[str]
) -> Iterator[bytes]:
    """Send a plain text file from the repository.

    Args:
      req: The HTTP request object
      backend: The git backend
      mat: The regex match for the requested path

    Returns:
      Iterator yielding file contents as bytes
    """
    req.nocache()
    path = _url_to_path(mat.group())
    logger.info("Sending plain text file %s", path)
    return send_file(req, get_repo(backend, mat).get_named_file(path), "text/plain")


def get_loose_object(
    req: "HTTPGitRequest", backend: "Backend", mat: re.Match[str]
) -> Iterator[bytes]:
    """Send a loose git object.

    Args:
      req: The HTTP request object
      backend: The git backend
      mat: The regex match containing object path segments

    Returns:
      Iterator yielding object contents as bytes
    """
    sha = (mat.group(1) + mat.group(2)).encode("ascii")
    logger.info("Sending loose object %s", sha)
    object_store = get_repo(backend, mat).object_store
    if not object_store.contains_loose(sha):
        yield req.not_found("Object not found")
        return
    try:
        data = object_store[sha].as_legacy_object()
    except OSError:
        yield req.error("Error reading object")
        return
    req.cache_forever()
    req.respond(HTTP_OK, "application/x-git-loose-object")
    yield data


def get_pack_file(
    req: "HTTPGitRequest", backend: "Backend", mat: re.Match[str]
) -> Iterator[bytes]:
    """Send a git pack file.

    Args:
      req: The HTTP request object
      backend: The git backend
      mat: The regex match for the requested pack file

    Returns:
      Iterator yielding pack file contents as bytes
    """
    req.cache_forever()
    path = _url_to_path(mat.group())
    logger.info("Sending pack file %s", path)
    return send_file(
        req,
        get_repo(backend, mat).get_named_file(path),
        "application/x-git-packed-objects",
    )


def get_idx_file(
    req: "HTTPGitRequest", backend: "Backend", mat: re.Match[str]
) -> Iterator[bytes]:
    """Send a git pack index file.

    Args:
      req: The HTTP request object
      backend: The git backend
      mat: The regex match for the requested index file

    Returns:
      Iterator yielding index file contents as bytes
    """
    req.cache_forever()
    path = _url_to_path(mat.group())
    logger.info("Sending pack file %s", path)
    return send_file(
        req,
        get_repo(backend, mat).get_named_file(path),
        "application/x-git-packed-objects-toc",
    )


def get_info_refs(
    req: "HTTPGitRequest", backend: "Backend", mat: re.Match[str]
) -> Iterator[bytes]:
    """Send git info/refs for discovery.

    Args:
      req: The HTTP request object
      backend: The git backend
      mat: The regex match for the info/refs request

    Returns:
      Iterator yielding refs advertisement or info/refs contents
    """
    params = parse_qs(req.environ["QUERY_STRING"])
    service = params.get("service", [None])[0]
    try:
        repo = get_repo(backend, mat)
    except NotGitRepository as e:
        yield req.not_found(str(e))
        return
    if service and not req.dumb:
        if req.handlers is None:
            yield req.forbidden("No handlers configured")
            return
        handler_cls = req.handlers.get(service.encode("ascii"), None)
        if handler_cls is None:
            yield req.forbidden("Unsupported service")
            return
        req.nocache()
        write = req.respond(HTTP_OK, f"application/x-{service}-advertisement")

        def write_fn(data: bytes) -> Optional[int]:
            result = write(data)
            return len(data) if result is not None else None

        proto = ReceivableProtocol(BytesIO().read, write_fn)
        from typing import Any, cast

        handler = handler_cls(
            backend,
            cast(Any, [url_prefix(mat)]),  # handler_cls could expect bytes or str
            proto,
            stateless_rpc=True,
            advertise_refs=True,
        )
        assert handler is not None
        handler.proto.write_pkt_line(b"# service=" + service.encode("ascii") + b"\n")
        handler.proto.write_pkt_line(None)
        handler.handle()
    else:
        # non-smart fallback
        # TODO: select_getanyfile() (see http-backend.c)
        req.nocache()
        req.respond(HTTP_OK, "text/plain")
        logger.info("Emulating dumb info/refs")
        yield from generate_info_refs(repo)


def get_info_packs(
    req: "HTTPGitRequest", backend: "Backend", mat: re.Match[str]
) -> Iterator[bytes]:
    """Send git info/packs file listing available packs.

    Args:
      req: The HTTP request object
      backend: The git backend
      mat: The regex match for the info/packs request

    Returns:
      Iterator yielding pack listing as bytes
    """
    req.nocache()
    req.respond(HTTP_OK, "text/plain")
    logger.info("Emulating dumb info/packs")
    return generate_objects_info_packs(get_repo(backend, mat))


def _chunk_iter(f: BinaryIO) -> Iterator[bytes]:
    while True:
        line = f.readline()
        length = int(line.rstrip(), 16)
        chunk = f.read(length + 2)
        if length == 0:
            break
        yield chunk[:-2]


class ChunkReader:
    """Reader for chunked transfer encoding streams."""

    def __init__(self, f: BinaryIO) -> None:
        """Initialize ChunkReader.

        Args:
            f: Binary file-like object to read from
        """
        self._iter = _chunk_iter(f)
        self._buffer: list[bytes] = []

    def read(self, n: int) -> bytes:
        """Read n bytes from the chunked stream.

        Args:
          n: Number of bytes to read

        Returns:
          Up to n bytes of data
        """
        while sum(map(len, self._buffer)) < n:
            try:
                self._buffer.append(next(self._iter))
            except StopIteration:
                break
        f = b"".join(self._buffer)
        ret = f[:n]
        self._buffer = [f[n:]]
        return ret


class _LengthLimitedFile:
    """Wrapper class to limit the length of reads from a file-like object.

    This is used to ensure EOF is read from the wsgi.input object once
    Content-Length bytes are read. This behavior is required by the WSGI spec
    but not implemented in wsgiref as of 2.5.
    """

    def __init__(self, input: BinaryIO, max_bytes: int) -> None:
        self._input = input
        self._bytes_avail = max_bytes

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes from the limited input.

        Args:
          size: Maximum number of bytes to read, or -1 for all available

        Returns:
          Up to size bytes of data
        """
        if self._bytes_avail <= 0:
            return b""
        if size == -1 or size > self._bytes_avail:
            size = self._bytes_avail
        self._bytes_avail -= size
        return self._input.read(size)

    # TODO: support more methods as necessary


def handle_service_request(
    req: "HTTPGitRequest", backend: "Backend", mat: re.Match[str]
) -> Iterator[bytes]:
    """Handle a git service request (upload-pack or receive-pack).

    Args:
      req: The HTTP request object
      backend: The git backend
      mat: The regex match for the service request

    Returns:
      Iterator yielding service response as bytes
    """
    service = mat.group().lstrip("/")
    logger.info("Handling service request for %s", service)
    if req.handlers is None:
        yield req.forbidden("No handlers configured")
        return
    handler_cls = req.handlers.get(service.encode("ascii"), None)
    if handler_cls is None:
        yield req.forbidden("Unsupported service")
        return
    try:
        get_repo(backend, mat)
    except NotGitRepository as e:
        yield req.not_found(str(e))
        return
    req.nocache()
    write = req.respond(HTTP_OK, f"application/x-{service}-result")

    def write_fn(data: bytes) -> Optional[int]:
        result = write(data)
        return len(data) if result is not None else None

    if req.environ.get("HTTP_TRANSFER_ENCODING") == "chunked":
        read = ChunkReader(req.environ["wsgi.input"]).read
    else:
        read = req.environ["wsgi.input"].read
    proto = ReceivableProtocol(read, write_fn)
    # TODO(jelmer): Find a way to pass in repo, rather than having handler_cls
    # reopen.
    handler = handler_cls(
        backend, [url_prefix(mat).encode("utf-8")], proto, stateless_rpc=True
    )
    assert handler is not None
    handler.handle()


class HTTPGitRequest:
    """Class encapsulating the state of a single git HTTP request.

    Attributes:
      environ: the WSGI environment for the request.
    """

    def __init__(
        self,
        environ: WSGIEnvironment,
        start_response: StartResponse,
        dumb: bool = False,
        handlers: Optional[
            dict[bytes, Union["HandlerConstructor", Callable[..., Any]]]
        ] = None,
    ) -> None:
        """Initialize HTTPGitRequest.

        Args:
            environ: WSGI environment dictionary
            start_response: WSGI start_response callable
            dumb: Whether to use dumb HTTP protocol
            handlers: Optional handler overrides
        """
        self.environ = environ
        self.dumb = dumb
        self.handlers = handlers
        self._start_response = start_response
        self._cache_headers: list[tuple[str, str]] = []
        self._headers: list[tuple[str, str]] = []

    def add_header(self, name: str, value: str) -> None:
        """Add a header to the response."""
        self._headers.append((name, value))

    def respond(
        self,
        status: str = HTTP_OK,
        content_type: Optional[str] = None,
        headers: Optional[Sequence[tuple[str, str]]] = None,
    ) -> Callable[[bytes], object]:
        """Begin a response with the given status and other headers."""
        if headers:
            self._headers.extend(headers)
        if content_type:
            self._headers.append(("Content-Type", content_type))
        self._headers.extend(self._cache_headers)

        return self._start_response(status, self._headers)

    def not_found(self, message: str) -> bytes:
        """Begin a HTTP 404 response and return the text of a message."""
        self._cache_headers = []
        logger.info("Not found: %s", message)
        self.respond(HTTP_NOT_FOUND, "text/plain")
        return message.encode("ascii")

    def forbidden(self, message: str) -> bytes:
        """Begin a HTTP 403 response and return the text of a message."""
        self._cache_headers = []
        logger.info("Forbidden: %s", message)
        self.respond(HTTP_FORBIDDEN, "text/plain")
        return message.encode("ascii")

    def error(self, message: str) -> bytes:
        """Begin a HTTP 500 response and return the text of a message."""
        self._cache_headers = []
        logger.error("Error: %s", message)
        self.respond(HTTP_ERROR, "text/plain")
        return message.encode("ascii")

    def nocache(self) -> None:
        """Set the response to never be cached by the client."""
        self._cache_headers = NO_CACHE_HEADERS

    def cache_forever(self) -> None:
        """Set the response to be cached forever by the client."""
        self._cache_headers = cache_forever_headers()


class HTTPGitApplication:
    """Class encapsulating the state of a git WSGI application.

    Attributes:
      backend: the Backend object backing this application
    """

    services: ClassVar[
        dict[
            tuple[str, re.Pattern[str]],
            Callable[[HTTPGitRequest, Backend, re.Match[str]], Iterator[bytes]],
        ]
    ] = {
        ("GET", re.compile("/HEAD$")): get_text_file,
        ("GET", re.compile("/info/refs$")): get_info_refs,
        ("GET", re.compile("/objects/info/alternates$")): get_text_file,
        ("GET", re.compile("/objects/info/http-alternates$")): get_text_file,
        ("GET", re.compile("/objects/info/packs$")): get_info_packs,
        (
            "GET",
            re.compile("/objects/([0-9a-f]{2})/([0-9a-f]{38})$"),
        ): get_loose_object,
        (
            "GET",
            re.compile("/objects/pack/pack-([0-9a-f]{40})\\.pack$"),
        ): get_pack_file,
        (
            "GET",
            re.compile("/objects/pack/pack-([0-9a-f]{40})\\.idx$"),
        ): get_idx_file,
        ("POST", re.compile("/git-upload-pack$")): handle_service_request,
        ("POST", re.compile("/git-receive-pack$")): handle_service_request,
    }

    def __init__(
        self,
        backend: Backend,
        dumb: bool = False,
        handlers: Optional[
            dict[bytes, Union["HandlerConstructor", Callable[..., Any]]]
        ] = None,
        fallback_app: Optional[WSGIApplication] = None,
    ) -> None:
        """Initialize HTTPGitApplication.

        Args:
            backend: Backend object for git operations
            dumb: Whether to use dumb HTTP protocol
            handlers: Optional handler overrides
            fallback_app: Optional fallback WSGI application
        """
        self.backend = backend
        self.dumb = dumb
        self.handlers: dict[bytes, Union[HandlerConstructor, Callable[..., Any]]] = (
            dict(DEFAULT_HANDLERS)
        )
        self.fallback_app = fallback_app
        if handlers is not None:
            self.handlers.update(handlers)

    def __call__(
        self,
        environ: WSGIEnvironment,
        start_response: StartResponse,
    ) -> Iterable[bytes]:
        """Handle WSGI request."""
        path = environ["PATH_INFO"]
        method = environ["REQUEST_METHOD"]
        req = HTTPGitRequest(
            environ, start_response, dumb=self.dumb, handlers=self.handlers
        )
        # environ['QUERY_STRING'] has qs args
        handler = None
        mat = None
        for smethod, spath in self.services.keys():
            if smethod != method:
                continue
            mat = spath.search(path)
            if mat:
                handler = self.services[smethod, spath]
                break

        if handler is None or mat is None:
            if self.fallback_app is not None:
                return self.fallback_app(environ, start_response)
            else:
                return [req.not_found("Sorry, that method is not supported")]

        return handler(req, self.backend, mat)


class GunzipFilter:
    """WSGI middleware that unzips gzip-encoded requests before passing on to the underlying application."""

    def __init__(self, application: WSGIApplication) -> None:
        """Initialize GunzipFilter with WSGI application."""
        self.app = application

    def __call__(
        self,
        environ: WSGIEnvironment,
        start_response: StartResponse,
    ) -> Iterable[bytes]:
        """Handle WSGI request with gzip decompression."""
        import gzip

        if environ.get("HTTP_CONTENT_ENCODING", "") == "gzip":
            environ["wsgi.input"] = gzip.GzipFile(
                filename=None, fileobj=environ["wsgi.input"], mode="rb"
            )
            del environ["HTTP_CONTENT_ENCODING"]
            environ.pop("CONTENT_LENGTH", None)

        return self.app(environ, start_response)


class LimitedInputFilter:
    """WSGI middleware that limits the input length of a request to that specified in Content-Length."""

    def __init__(self, application: WSGIApplication) -> None:
        """Initialize LimitedInputFilter with WSGI application."""
        self.app = application

    def __call__(
        self,
        environ: WSGIEnvironment,
        start_response: StartResponse,
    ) -> Iterable[bytes]:
        """Handle WSGI request with input length limiting."""
        # This is not necessary if this app is run from a conforming WSGI
        # server. Unfortunately, there's no way to tell that at this point.
        # TODO: git may used HTTP/1.1 chunked encoding instead of specifying
        # content-length
        content_length = environ.get("CONTENT_LENGTH", "")
        if content_length:
            environ["wsgi.input"] = _LengthLimitedFile(
                environ["wsgi.input"], int(content_length)
            )
        return self.app(environ, start_response)


def make_wsgi_chain(
    backend: Backend,
    dumb: bool = False,
    handlers: Optional[dict[bytes, Callable[..., Any]]] = None,
    fallback_app: Optional[WSGIApplication] = None,
) -> WSGIApplication:
    """Factory function to create an instance of HTTPGitApplication.

    Correctly wrapped with needed middleware.
    """
    app = HTTPGitApplication(
        backend, dumb=dumb, handlers=handlers, fallback_app=fallback_app
    )
    wrapped_app = LimitedInputFilter(GunzipFilter(app))
    return wrapped_app


class ServerHandlerLogger(ServerHandler):
    """ServerHandler that uses dulwich's logger for logging exceptions."""

    def log_exception(
        self,
        exc_info: Union[
            tuple[type[BaseException], BaseException, TracebackType],
            tuple[None, None, None],
            None,
        ],
    ) -> None:
        """Log exception using dulwich logger."""
        logger.exception(
            "Exception happened during processing of request",
            exc_info=exc_info,
        )

    def log_message(self, format: str, *args: object) -> None:
        """Log message using dulwich logger."""
        logger.info(format, *args)

    def log_error(self, *args: object) -> None:
        """Log error using dulwich logger."""
        logger.error(*args)


class WSGIRequestHandlerLogger(WSGIRequestHandler):
    """WSGIRequestHandler that uses dulwich's logger for logging exceptions."""

    def log_exception(
        self,
        exc_info: Union[
            tuple[type[BaseException], BaseException, TracebackType],
            tuple[None, None, None],
            None,
        ],
    ) -> None:
        """Log exception using dulwich logger."""
        logger.exception(
            "Exception happened during processing of request",
            exc_info=exc_info,
        )

    def log_message(self, format: str, *args: object) -> None:
        """Log message using dulwich logger."""
        logger.info(format, *args)

    def log_error(self, *args: object) -> None:
        """Log error using dulwich logger."""
        logger.error(*args)

    def handle(self) -> None:
        """Handle a single HTTP request."""
        self.raw_requestline = self.rfile.readline()
        if not self.parse_request():  # An error code has been sent, just exit
            return

        handler = ServerHandlerLogger(
            self.rfile,
            self.wfile,  # type: ignore
            self.get_stderr(),
            self.get_environ(),
        )
        handler.request_handler = self  # type: ignore  # backpointer for logging
        handler.run(self.server.get_app())  # type: ignore


class WSGIServerLogger(WSGIServer):
    """WSGIServer that uses dulwich's logger for error handling."""

    def handle_error(self, request: object, client_address: tuple[str, int]) -> None:
        """Handle an error."""
        logger.exception(
            f"Exception happened during processing of request from {client_address!s}"
        )


def main(argv: list[str] = sys.argv) -> None:
    """Entry point for starting an HTTP git server."""
    import optparse

    parser = optparse.OptionParser()
    parser.add_option(
        "-l",
        "--listen_address",
        dest="listen_address",
        default="localhost",
        help="Binding IP address.",
    )
    parser.add_option(
        "-p",
        "--port",
        dest="port",
        type=int,
        default=8000,
        help="Port to listen on.",
    )
    options, args = parser.parse_args(argv)

    if len(args) > 1:
        gitdir = args[1]
    else:
        gitdir = os.getcwd()

    log_utils.default_logging_config()
    from typing import cast

    from dulwich.server import BackendRepo

    backend = DictBackend({"/": cast(BackendRepo, Repo(gitdir))})
    app = make_wsgi_chain(backend)
    server = make_server(
        options.listen_address,
        options.port,
        app,
        handler_class=WSGIRequestHandlerLogger,
        server_class=WSGIServerLogger,
    )
    logger.info(
        "Listening for HTTP connections on %s:%d",
        options.listen_address,
        options.port,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
