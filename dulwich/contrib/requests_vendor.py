# requests_vendor.py -- requests implementation of the AbstractHttpGitClient interface
# Copyright (C) 2022 Eden Shalit <epopcop@gmail.com>
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


"""Requests HTTP client support for Dulwich.

To use this implementation as the HTTP implementation in Dulwich, override
the dulwich.client.HttpGitClient attribute:

  >>> from dulwich import client as _mod_client
  >>> from dulwich.contrib.requests_vendor import RequestsHttpGitClient
  >>> _mod_client.HttpGitClient = RequestsHttpGitClient

This implementation is experimental and does not have any tests.
"""

from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from ..config import ConfigFile

from requests import Session

from ..client import (
    AbstractHttpGitClient,
    HTTPProxyUnauthorized,
    HTTPUnauthorized,
    default_user_agent_string,
)
from ..errors import GitProtocolError, NotGitRepository


class RequestsHttpGitClient(AbstractHttpGitClient):
    def __init__(
        self,
        base_url: str,
        dumb: Optional[bool] = None,
        config: Optional["ConfigFile"] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs: object,
    ) -> None:
        self._username = username
        self._password = password

        self.session = get_session(config)

        if username is not None:
            self.session.auth = (username, password)  # type: ignore[assignment]

        super().__init__(
            base_url=base_url, dumb=bool(dumb) if dumb is not None else False, **kwargs
        )

    def _http_request(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        data: Optional[bytes] = None,
        allow_compression: bool = False,
    ) -> tuple[Any, Callable[[int], bytes]]:
        req_headers = self.session.headers.copy()  # type: ignore[attr-defined]
        if headers is not None:
            req_headers.update(headers)

        if allow_compression:
            req_headers["Accept-Encoding"] = "gzip"
        else:
            req_headers["Accept-Encoding"] = "identity"

        if data:
            resp = self.session.post(url, headers=req_headers, data=data)
        else:
            resp = self.session.get(url, headers=req_headers)

        if resp.status_code == 404:
            raise NotGitRepository
        if resp.status_code == 401:
            raise HTTPUnauthorized(resp.headers.get("WWW-Authenticate"), url)
        if resp.status_code == 407:
            raise HTTPProxyUnauthorized(resp.headers.get("Proxy-Authenticate"), url)
        if resp.status_code != 200:
            raise GitProtocolError(f"unexpected http resp {resp.status_code} for {url}")

        # Add required fields as stated in AbstractHttpGitClient._http_request
        resp.content_type = resp.headers.get("Content-Type")  # type: ignore[attr-defined]
        resp.redirect_location = ""  # type: ignore[attr-defined]
        if resp.history:
            resp.redirect_location = resp.url  # type: ignore[attr-defined]

        read = BytesIO(resp.content).read

        return resp, read


def get_session(config: Optional["ConfigFile"]) -> Session:
    session = Session()
    session.headers.update({"Pragma": "no-cache"})

    proxy_server: Optional[str] = None
    user_agent: Optional[str] = None
    ca_certs: Optional[str] = None
    ssl_verify: Optional[bool] = None

    if config is not None:
        try:
            proxy_bytes = config.get(b"http", b"proxy")
            if isinstance(proxy_bytes, bytes):
                proxy_server = proxy_bytes.decode()
        except KeyError:
            pass

        try:
            agent_bytes = config.get(b"http", b"useragent")
            if isinstance(agent_bytes, bytes):
                user_agent = agent_bytes.decode()
        except KeyError:
            pass

        try:
            ssl_verify = config.get_boolean(b"http", b"sslVerify")
        except KeyError:
            ssl_verify = True

        try:
            certs_bytes = config.get(b"http", b"sslCAInfo")
            if isinstance(certs_bytes, bytes):
                ca_certs = certs_bytes.decode()
        except KeyError:
            ca_certs = None

    if user_agent is None:
        user_agent = default_user_agent_string()
    if user_agent is not None:
        session.headers.update({"User-agent": user_agent})

    if ca_certs:
        session.verify = ca_certs
    elif ssl_verify is False:
        session.verify = ssl_verify

    if proxy_server is not None:
        session.proxies.update({"http": proxy_server, "https": proxy_server})
    return session
