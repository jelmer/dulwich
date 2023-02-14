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

import asyncio
from io import BytesIO
import sys

from aiohttp import web

from . import log_utils
from .protocol import ReceivableProtocol, HangupException
from .server import (
    DEFAULT_HANDLERS,
    DictBackend,
    generate_objects_info_packs,
    generate_info_refs,
)
from .repo import Repo
from .web import NO_CACHE_HEADERS, cache_forever_headers


logger = log_utils.getLogger(__name__)


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


async def get_loose_object(request):
    sha = (request.match_info['dir']
           + request.match_info['filename']).encode("ascii")
    logger.info("Sending loose object %s", sha)
    object_store = request.app['repo'].object_store
    if not object_store.contains_loose(sha):
        raise web.HTTPNotFound(text="Object not found")
    try:
        data = object_store[sha].as_legacy_object()
    except IOError:
        raise web.HTTPInternalServerError(text="Error reading object")
    headers = {'Content-Type': "application/x-git-loose-object"}
    headers.update(cache_forever_headers())
    return web.Response(status=200, headers=headers, body=data)


async def get_text_file(request):
    headers = {'Content-Type': 'text/plain'}
    headers.update(NO_CACHE_HEADERS)
    path = request.match_info['file']
    logger.info("Sending plain text file %s", path)
    repo = request.app['repo']
    return await send_file(
        request, repo.get_named_file(path), headers)


async def refs_request(repo, request, handlers=None):
    service = request.query.get("service")
    if service:
        handler_cls = request.get(service.encode("ascii"), None)
        if handler_cls is None:
            raise web.HTTPForbidden(text="Unsupported service")
        headers = {
            'Content-Type': "application/x-%s-advertisement" % service}
        headers.update(NO_CACHE_HEADERS)

        response = web.StreamResponse(status=200, headers=headers)

        await response.prepare(request)

        out = BytesIO()
        proto = ReceivableProtocol(BytesIO().read, out.write)
        handler = handler_cls(
            DictBackend({".": repo}), ["."], proto, stateless_rpc=True, advertise_refs=True
        )
        handler.proto.write_pkt_line(b"# service=" + service.encode("ascii") + b"\n")
        handler.proto.write_pkt_line(None)

        # TODO(jelmer): Implement this with proper async code
        await asyncio.to_thread(handler.handle)

        await response.write(out.getvalue())

        await response.write_eof()

        return response
    else:
        # non-smart fallback
        headers = {'Content-Type': 'text/plain'}
        headers.update(NO_CACHE_HEADERS)
        logger.info("Emulating dumb info/refs")
        return web.Response(body=b''.join(generate_info_refs(repo)), headers=headers)


async def get_info_refs(request):
    repo = request.app['repo']
    return await refs_request(repo, request, request.app['handlers'])


async def get_info_packs(request):
    headers = {'Content-Type': 'text/plain'}
    headers.update(NO_CACHE_HEADERS)
    logger.info("Emulating dumb info/packs")
    return web.Response(
        body=b''.join(generate_objects_info_packs(request.app['repo'])),
        headers=headers)


async def get_pack_file(request):
    headers = {'Content-Type': "application/x-git-packed-objects"}
    headers.update(cache_forever_headers())
    sha = request.match_info['sha']
    path = 'objects/pack/pack-%s.pack' % sha
    logger.info("Sending pack file %s", path)
    return await send_file(
        request,
        request.app['repo'].get_named_file(path),
        headers=headers,
    )


async def get_index_file(request):
    headers = {
        'Content-Type': "application/x-git-packed-objects-toc"
    }
    headers.update(cache_forever_headers())
    sha = request.match_info['sha']
    path = 'objects/pack/pack-%s.idx' % sha
    logger.info("Sending pack file %s", path)
    return await send_file(
        request,
        request.app['repo'].get_named_file(path),
        headers=headers
    )


async def service_request(repo, request, handlers=None):
    service = request.match_info['service']
    if handlers is None:
        handlers = dict(DEFAULT_HANDLERS)
    logger.info("Handling service request for %s", service)
    handler_cls = handlers.get(service.encode("ascii"), None)
    if handler_cls is None:
        raise web.HTTPForbidden(text="Unsupported service")
    headers = {
        'Content-Type': "application/x-%s-result" % service
    }
    headers.update(NO_CACHE_HEADERS)

    response = web.StreamResponse(status=200, headers=headers)

    await response.prepare(request)

    inf = BytesIO(await request.read())
    outf = BytesIO()

    def handle():
        proto = ReceivableProtocol(inf.read, outf.write)
        handler = handler_cls(DictBackend({".": repo}), ["."], proto, stateless_rpc=True)
        try:
            handler.handle()
        except HangupException:
            response.force_close()

    # TODO(jelmer): Implement this with proper async code
    await asyncio.to_thread(handle)

    await response.write(outf.getvalue())

    await response.write_eof()
    return response


async def handle_service_request(request):
    repo = request.app['repo']

    return await service_request(repo, request, request.app['handlers'])


def create_repo_app(repo, handlers=None, dumb=False):
    app = web.Application()
    app['repo'] = repo
    if handlers is None:
        handlers = dict(DEFAULT_HANDLERS)
    app['handlers'] = handlers
    app['dumb'] = dumb
    app.router.add_get('/info/refs', get_info_refs)
    app.router.add_post(
        '/{service:git-upload-pack|git-receive-pack}',
        handle_service_request)
    if dumb:
        app.router.add_get('/{file:HEAD}', get_text_file)
        app.router.add_get('/{file:objects/info/alternates}', get_text_file)
        app.router.add_get(
            '/{file:objects/info/http-alternates}', get_text_file)
        app.router.add_get('/objects/info/packs', get_info_packs)
        app.router.add_get(
            '/objects/{dir:[0-9a-f]{2}}/{file:[0-9a-f]{38}}', get_loose_object)
        app.router.add_get(
            '/objects/pack/pack-{sha:[0-9a-f]{40}}\\.pack', get_pack_file)
        app.router.add_get(
            '/objects/pack/pack-{sha:[0-9a-f]{40}}\\.idx', get_index_file)
    return app


def main(argv=None):
    """Entry point for starting an HTTP git server."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--listen_address",
        dest="listen_address",
        default="localhost",
        help="Binding IP address.",
    )
    parser.add_argument(
        "-p",
        "--port",
        dest="port",
        type=int,
        default=8000,
        help="Port to listen on.",
    )
    parser.add_argument('gitdir', type=str, default='.', nargs='?')
    args = parser.parse_args(argv)

    log_utils.default_logging_config()
    app = create_repo_app(Repo(args.gitdir))
    logger.info(
        "Listening for HTTP connections on %s:%d",
        args.listen_address,
        args.port,
    )
    web.run_app(app, port=args.port, host=args.listen_address)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
