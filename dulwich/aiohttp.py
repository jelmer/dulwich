# aiohttp.py -- aiohttp smart client/server
# Copyright (C) 2022 Jelmer Vernooij <jelmer@jelmer.uk>
#
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as public by the Free Software Foundation; version 2.0
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

"""aiohttp client/server support."""

import os
import sys
import time

from aiohttp import web

from . import log_utils
from .protocol import AsyncProtocol
from .server import (
    DEFAULT_HANDLERS,
    DictBackend,
    generate_objects_info_packs,
    generate_info_refs,
)
from .repo import Repo, NotGitRepository
from .web import date_time_string, NO_CACHE_HEADERS


logger = log_utils.getLogger(__name__)


def cache_forever_headers():
    now = time.time()
    return {
        "Date": date_time_string(now),
        "Expires": date_time_string(now + 31536000),
        "Cache-Control": "public, max-age=31536000",
    }


async def send_file(req, f, headers):
    """Send a file-like object to the request output.

    Args:
      req: The HTTPGitRequest object to send output to.
      f: An open file-like object to send; will be closed.
      headers: Headers to send
    Returns: Iterator over the contents of the file, as chunks.
    """
    if f is None:
        raise web.HTTPNotFound(text="File not found")
    response = web.StreamResponse(status=200, reason='OK', headers=headers)
    await response.prepare(req)
    try:
        while True:
            data = f.read(10240)
            if not data:
                break
            await response.write(data)
    except IOError:
        raise web.HTTPInternalServerError(text="Error reading file")
    finally:
        f.close()
    await response.write_eof()
    return response


class HTTPGitApplication(web.Application):

    async def _get_loose_object(self, request):
        sha = (request.match_info['dir']
               + request.match_info['filename']).encode("ascii")
        logger.info("Sending loose object %s", sha)
        try:
            object_store = self.backend.open_repository('/').object_store
        except NotGitRepository as e:
            raise web.HTTPNotFound(text=str(e))
        if not object_store.contains_loose(sha):
            raise web.HTTPNotFound(text="Object not found")
        try:
            data = object_store[sha].as_legacy_object()
        except IOError:
            raise web.HTTPInternalServerError(text="Error reading object")
        headers = {
            'Content-Type': "application/x-git-loose-object"
        }
        headers.update(cache_forever_headers())
        return web.Response(status=200, headers=headers, body=data)

    async def _get_text_file(self, request):
        headers = {
            'Content-Type': 'text/plain',
        }
        headers.update(NO_CACHE_HEADERS)
        path = request.match_info['file']
        logger.info("Sending plain text file %s", path)
        repo = self.backend.open_repository('/')
        return await send_file(
            request, repo.get_named_file(path), headers)

    async def _get_info_refs(self, request):
        service = request.query.get("service")
        try:
            repo = self.backend.open_repository('/')
        except NotGitRepository as e:
            raise web.HTTPNotFound(text=str(e))
        if service:
            handler_cls = self.handlers.get(service.encode("ascii"), None)
            if handler_cls is None:
                raise web.HTTPForbidden(text="Unsupported service")
            headers = {
                'Content-Type': "application/x-%s-advertisement" % service}
            headers.update(NO_CACHE_HEADERS)
            response = web.StreamResponse(headers=headers, status=200)
            await response.prepare(request)
            proto = AsyncProtocol(request.content.readexactly, response.write)
            handler = handler_cls(
                self.backend,
                ['/'],
                proto,
                stateless_rpc=True,
                advertise_refs=True,
            )
            await handler.proto.write_pkt_line(
                b"# service=" + service.encode("ascii") + b"\n")
            await handler.proto.write_pkt_line(None)
            await handler.handle_async()
            await response.write_eof()
            return response
        else:
            # non-smart fallback
            headers = {'Content-Type': 'text/plain'}
            headers.update(NO_CACHE_HEADERS)
            logger.info("Emulating dumb info/refs")
            return web.Response(body=b''.join(generate_info_refs(repo)), headers=headers)

    async def _get_info_packs(self, request):
        headers = {'Content-Type': 'text/plain'}
        headers.update(NO_CACHE_HEADERS)
        logger.info("Emulating dumb info/packs")
        try:
            repo = self.backend.open_repository('/')
        except NotGitRepository as e:
            raise web.HTTPNotFound(text=str(e))
        return web.Response(
            body=b''.join(generate_objects_info_packs(repo)), headers=headers)

    async def _get_pack_file(self, request):
        headers = {'Content-Type': "application/x-git-packed-objects"}
        headers.update(cache_forever_headers())
        sha = request.match_info['sha']
        path = 'objects/pack/pack-%s.pack' % sha
        logger.info("Sending pack file %s", path)
        repo = self.backend.open_repository('/')
        return await send_file(
            request,
            repo.get_named_file(path),
            headers=headers,
        )

    async def _get_index_file(self, request):
        headers = {
            'Content-Type': "application/x-git-packed-objects-toc"
        }
        headers.update(cache_forever_headers())
        sha = request.match_info['sha']
        path = 'objects/pack/pack-%s.idx' % sha
        logger.info("Sending pack file %s", path)
        repo = self.backend.open_repository('/')
        return await send_file(
            request,
            repo.get_named_file(path),
            headers=headers
        )

    async def _handle_service_request(self, request):
        service = request.match_info['service']
        logger.info("Handling service request for %s", service)
        handler_cls = self.handlers.get(service.encode("ascii"), None)
        if handler_cls is None:
            raise web.HTTPForbidden(text="Unsupported service")
        headers = {
            'Content-Type': "application/x-%s-result" % service
        }
        headers.update(NO_CACHE_HEADERS)
        response = web.StreamResponse(status=200, headers=headers)
        await response.prepare(request)
        proto = AsyncProtocol(request.content.readexactly, response.write)
        handler = handler_cls(self.backend, ['/'], proto, stateless_rpc=True)
        await handler.handle_async()
        await response.write_eof()
        return response

    def __init__(self, backend):
        super(HTTPGitApplication, self).__init__()
        self.backend = backend
        self.handlers = dict(DEFAULT_HANDLERS)
        self.router.add_get(
            '/{file:HEAD}', self._get_text_file)
        self.router.add_get(
            '/info/refs', self._get_info_refs)
        self.router.add_get(
            '/{file:objects/info/alternates}', self._get_text_file)
        self.router.add_get(
            '/{file:objects/info/http-alternates}', self._get_text_file)
        self.router.add_get(
            '/objects/info/packs', self._get_info_packs)
        self.router.add_get(
            '/objects/{dir:[0-9a-f]{2}}/{file:[0-9a-f]{38}}',
            self._get_loose_object)
        self.router.add_get(
            '/objects/pack/pack-{sha:[0-9a-f]{40}}\\.pack', self._get_pack_file)
        self.router.add_get(
            '/objects/pack/pack-{sha:[0-9a-f]{40}}\\.idx', self._get_index_file)
        self.router.add_post(
            '/{service:git-upload-pack|git-receive-pack}',
            self._handle_service_request)


def main(argv=sys.argv):
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
    backend = DictBackend({"/": Repo(gitdir)})
    app = HTTPGitApplication(backend)
    logger.info(
        "Listening for HTTP connections on %s:%d",
        options.listen_address,
        options.port,
    )
    web.run_app(app, port=options.port, host=options.listen_address)


if __name__ == "__main__":
    main()
