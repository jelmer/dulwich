# aiohttp.py -- aiohttp smart client/server
# Copyright (C) 2022 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""aiohttp client/server support."""

import asyncio
import sys
from io import BytesIO
from typing import BinaryIO, cast

from aiohttp import web

from .. import log_utils
from ..errors import HangupException
from ..objects import ObjectID
from ..protocol import ReceivableProtocol
from ..repo import Repo
from ..server import (
    DEFAULT_HANDLERS,
    BackendRepo,
    DictBackend,
    generate_info_refs,
    generate_objects_info_packs,
)
from ..web import NO_CACHE_HEADERS, cache_forever_headers

logger = log_utils.getLogger(__name__)

# Application keys for type-safe access to app state
REPO_KEY = web.AppKey("repo", Repo)
HANDLERS_KEY = web.AppKey("handlers", dict)
DUMB_KEY = web.AppKey("dumb", bool)


async def send_file(
    req: web.Request, f: BinaryIO | None, headers: dict[str, str]
) -> web.StreamResponse:
    """Send a file-like object to the request output.

    Args:
      req: The HTTPGitRequest object to send output to.
      f: An open file-like object to send; will be closed.
      headers: Headers to send
    Returns: Iterator over the contents of the file, as chunks.
    """
    if f is None:
        raise web.HTTPNotFound(text="File not found")
    response = web.StreamResponse(status=200, reason="OK", headers=headers)
    await response.prepare(req)
    try:
        while True:
            data = f.read(10240)
            if not data:
                break
            await response.write(data)
    except OSError:
        raise web.HTTPInternalServerError(text="Error reading file")
    finally:
        f.close()
    await response.write_eof()
    return response


async def get_loose_object(request: web.Request) -> web.Response:
    """Handle request for a loose object.

    Args:
      request: aiohttp request object
    Returns: Response with the loose object data
    """
    sha = ObjectID(
        (request.match_info["dir"] + request.match_info["file"]).encode("ascii")
    )
    logger.info("Sending loose object %s", sha)
    object_store = request.app[REPO_KEY].object_store
    if not object_store.contains_loose(sha):
        raise web.HTTPNotFound(text="Object not found")
    try:
        data = object_store[sha].as_legacy_object()
    except OSError:
        raise web.HTTPInternalServerError(text="Error reading object")
    headers = {"Content-Type": "application/x-git-loose-object"}
    headers.update(cache_forever_headers())
    return web.Response(status=200, headers=headers, body=data)


async def get_text_file(request: web.Request) -> web.StreamResponse:
    """Handle request for a text file.

    Args:
      request: aiohttp request object
    Returns: Response with the text file contents
    """
    headers = {"Content-Type": "text/plain"}
    headers.update(NO_CACHE_HEADERS)
    path = request.match_info["file"]
    logger.info("Sending plain text file %s", path)
    repo = request.app[REPO_KEY]
    return await send_file(request, repo.get_named_file(path), headers)


async def refs_request(
    repo: Repo, request: web.Request, handlers: dict[bytes, type] | None = None
) -> web.StreamResponse | web.Response:
    """Handle a refs request.

    Args:
      repo: Repository object
      request: aiohttp request object
      handlers: Optional dict of service handlers
    Returns: Response with refs information
    """
    service = request.query.get("service")
    if service:
        if handlers is None:
            handlers = dict(DEFAULT_HANDLERS)
        handler_cls = handlers.get(service.encode("ascii"), None)
        if handler_cls is None:
            raise web.HTTPForbidden(text="Unsupported service")
        headers = {"Content-Type": f"application/x-{service}-advertisement"}
        headers.update(NO_CACHE_HEADERS)

        response = web.StreamResponse(status=200, headers=headers)

        await response.prepare(request)

        out = BytesIO()
        proto = ReceivableProtocol(BytesIO().read, out.write)
        handler = handler_cls(
            DictBackend({b".": cast(BackendRepo, repo)}),
            [b"."],
            proto,
            stateless_rpc=True,
            advertise_refs=True,
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
        headers = {"Content-Type": "text/plain"}
        headers.update(NO_CACHE_HEADERS)
        logger.info("Emulating dumb info/refs")
        return web.Response(body=b"".join(generate_info_refs(repo)), headers=headers)


async def get_info_refs(request: web.Request) -> web.StreamResponse | web.Response:
    """Handle request for /info/refs.

    Args:
      request: aiohttp request object
    Returns: Response with refs information
    """
    repo = request.app[REPO_KEY]
    return await refs_request(repo, request, request.app[HANDLERS_KEY])


async def get_info_packs(request: web.Request) -> web.Response:
    """Handle request for /info/packs.

    Args:
      request: aiohttp request object
    Returns: Response with pack information
    """
    headers = {"Content-Type": "text/plain"}
    headers.update(NO_CACHE_HEADERS)
    logger.info("Emulating dumb info/packs")
    return web.Response(
        body=b"".join(generate_objects_info_packs(request.app[REPO_KEY])),
        headers=headers,
    )


async def get_pack_file(request: web.Request) -> web.StreamResponse:
    """Handle request for a pack file.

    Args:
      request: aiohttp request object
    Returns: Response with the pack file data
    """
    headers = {"Content-Type": "application/x-git-packed-objects"}
    headers.update(cache_forever_headers())
    sha = request.match_info["sha"]
    path = f"objects/pack/pack-{sha}.pack"
    logger.info("Sending pack file %s", path)
    return await send_file(
        request,
        request.app[REPO_KEY].get_named_file(path),
        headers=headers,
    )


async def get_index_file(request: web.Request) -> web.StreamResponse:
    """Handle request for a pack index file.

    Args:
      request: aiohttp request object
    Returns: Response with the pack index file data
    """
    headers = {"Content-Type": "application/x-git-packed-objects-toc"}
    headers.update(cache_forever_headers())
    sha = request.match_info["sha"]
    path = f"objects/pack/pack-{sha}.idx"
    logger.info("Sending pack file %s", path)
    return await send_file(
        request, request.app["repo"].get_named_file(path), headers=headers
    )


async def service_request(
    repo: Repo, request: web.Request, handlers: dict[bytes, type] | None = None
) -> web.StreamResponse:
    """Handle a git service request (upload-pack or receive-pack).

    Args:
      repo: Repository object
      request: aiohttp request object
      handlers: Optional dict of service handlers
    Returns: Response with service result
    """
    service = request.match_info["service"]
    if handlers is None:
        handlers = dict(DEFAULT_HANDLERS)
    logger.info("Handling service request for %s", service)
    handler_cls = handlers.get(service.encode("ascii"), None)
    if handler_cls is None:
        raise web.HTTPForbidden(text="Unsupported service")
    headers = {"Content-Type": f"application/x-{service}-result"}
    headers.update(NO_CACHE_HEADERS)

    response = web.StreamResponse(status=200, headers=headers)

    await response.prepare(request)

    inf = BytesIO(await request.read())
    outf = BytesIO()

    def handle() -> None:
        proto = ReceivableProtocol(inf.read, outf.write)
        handler = handler_cls(
            DictBackend({b".": cast(BackendRepo, repo)}),
            [b"."],
            proto,
            stateless_rpc=True,
        )
        try:
            handler.handle()
        except HangupException:
            response.force_close()

    # TODO(jelmer): Implement this with proper async code
    await asyncio.to_thread(handle)

    await response.write(outf.getvalue())

    await response.write_eof()
    return response


async def handle_service_request(request: web.Request) -> web.StreamResponse:
    """Handle a service request endpoint.

    Args:
      request: aiohttp request object
    Returns: Response with service result
    """
    repo = request.app[REPO_KEY]

    return await service_request(repo, request, request.app[HANDLERS_KEY])


def create_repo_app(
    repo: Repo, handlers: dict[bytes, type] | None = None, dumb: bool = False
) -> web.Application:
    """Create an aiohttp application for serving a git repository.

    Args:
      repo: Repository object to serve
      handlers: Optional dict of service handlers
      dumb: Whether to enable dumb HTTP protocol support
    Returns: Configured aiohttp Application
    """
    app = web.Application()
    app[REPO_KEY] = repo
    if handlers is None:
        handlers = dict(DEFAULT_HANDLERS)
    app[HANDLERS_KEY] = handlers
    app[DUMB_KEY] = dumb
    app.router.add_get("/info/refs", get_info_refs)
    app.router.add_post(
        "/{service:git-upload-pack|git-receive-pack}", handle_service_request
    )
    if dumb:
        app.router.add_get("/{file:HEAD}", get_text_file)
        app.router.add_get("/{file:objects/info/alternates}", get_text_file)
        app.router.add_get("/{file:objects/info/http-alternates}", get_text_file)
        app.router.add_get("/objects/info/packs", get_info_packs)
        app.router.add_get(
            "/objects/{dir:[0-9a-f]{2}}/{file:[0-9a-f]{38}}", get_loose_object
        )
        app.router.add_get(
            "/objects/pack/pack-{sha:[0-9a-f]{40}}\\.pack", get_pack_file
        )
        app.router.add_get(
            "/objects/pack/pack-{sha:[0-9a-f]{40}}\\.idx", get_index_file
        )
    return app


def main(argv: list[str] | None = None) -> None:
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
    parser.add_argument("gitdir", type=str, default=".", nargs="?")
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
    main(sys.argv[1:])
