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
#

"""Requests HTTP client support for Dulwich.

To use this implementation as the HTTP implementation in Dulwich, override
the dulwich.client.HttpGitClient attribute:

  >>> from dulwich import client as _mod_client
  >>> from dulwich.contrib.requests_vendor import RequestsHttpGitClient
  >>> _mod_client.HttpGitClient = RequestsHttpGitClient

This implementation is experimental and does not have any tests.
"""

__all__ = [
    "RequestsHttpGitClient",
    "get_session",
]

from collections.abc import Callable, Iterator
from io import BytesIO
from typing import TYPE_CHECKING, Any

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
    """HTTP Git client using the requests library."""

    def __init__(
        self,
        base_url: str,
        dumb: bool | None = None,
        config: "ConfigFile | None" = None,
        username: str | None = None,
        password: str | None = None,
        thin_packs: bool = True,
        report_activity: Callable[[int, str], None] | None = None,
        quiet: bool = False,
        include_tags: bool = False,
    ) -> None:
        """Initialize RequestsHttpGitClient.

        Args:
          base_url: Base URL of the Git repository
          dumb: Whether to use dumb HTTP transport
          config: Git configuration file
          username: Username for authentication
          password: Password for authentication
          thin_packs: Whether to use thin packs
          report_activity: Function to report activity
          quiet: Whether to suppress output
          include_tags: Whether to include tags
        """
        self._username = username
        self._password = password

        self.session = get_session(config)

        if username is not None:
            self.session.auth = (username, password)  # type: ignore[assignment]

        super().__init__(
            base_url=base_url,
            dumb=bool(dumb) if dumb is not None else False,
            thin_packs=thin_packs,
            report_activity=report_activity,
            quiet=quiet,
            include_tags=include_tags,
        )

    def _http_request(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        data: bytes | Iterator[bytes] | None = None,
        raise_for_status: bool = True,
    ) -> tuple[Any, Callable[[int], bytes]]:
        req_headers = self.session.headers.copy()  # type: ignore[attr-defined]
        if headers is not None:
            req_headers.update(headers)

        # Accept compression by default
        req_headers.setdefault("Accept-Encoding", "gzip")

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


def get_session(config: "ConfigFile | None") -> Session:
    """Create a requests session with Git configuration.

    Args:
      config: Git configuration file

    Returns:
      Configured requests Session
    """
    session = Session()
    session.headers.update({"Pragma": "no-cache"})

    proxy_server: str | None = None
    user_agent: str | None = None
    ca_certs: str | None = None
    ssl_verify: bool | None = None

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
