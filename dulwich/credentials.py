# credentials.py -- support for git credential helpers

# Copyright (C) 2022 Daniele Trifirò <daniele@iterative.ai>
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

"""Support for git credential helpers

https://git-scm.com/book/en/v2/Git-Tools-Credential-Storage

Currently Dulwich supports only the `get` operation

"""
import os
import shlex
import shutil
import subprocess
import sys
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

from dulwich.config import (ConfigDict, SectionLike, StackedConfig,
                            get_xdg_config_home_path)

DEFAULT_GIT_CREDENTIALS_PATHS = [
    os.path.expanduser("~/.git-credentials"),
    get_xdg_config_home_path("git", "credentials"),
]


def match_urls(url: ParseResult, url_prefix: ParseResult) -> bool:
    base_match = (
        url.scheme == url_prefix.scheme
        and url.hostname == url_prefix.hostname
        and url.port == url_prefix.port
    )
    user_match = url.username == url_prefix.username if url_prefix.username else True
    path_match = url.path.rstrip("/").startswith(url_prefix.path.rstrip())
    return base_match and user_match and path_match


def match_partial_url(valid_url: ParseResult, partial_url: str) -> bool:
    """matches a parsed url with a partial url (no scheme/netloc)"""
    if "://" not in partial_url:
        parsed = urlparse("scheme://" + partial_url)
    else:
        parsed = urlparse(partial_url)
        if valid_url.scheme != parsed.scheme:
            return False

    if any(
        (
            (parsed.hostname and valid_url.hostname != parsed.hostname),
            (parsed.username and valid_url.username != parsed.username),
            (parsed.port and valid_url.port != parsed.port),
            (parsed.path and parsed.path.rstrip("/") != valid_url.path.rstrip("/")),
        ),
    ):
        return False

    return True


def urlmatch_credential_sections(
    config: ConfigDict, url: Optional[str]
) -> Iterator[SectionLike]:
    """Returns credential sections from the config which match the given URL"""
    encoding = config.encoding or sys.getdefaultencoding()
    parsed_url = urlparse(url or "")
    for config_section in config.sections():
        if config_section[0] != b"credential":
            continue

        if len(config_section) < 2:
            yield config_section
            continue

        config_url = config_section[1].decode(encoding)
        parsed_config_url = urlparse(config_url)
        if parsed_config_url.scheme and parsed_config_url.netloc:
            is_match = match_urls(parsed_url, parsed_config_url)
        else:
            is_match = match_partial_url(parsed_url, config_url)

        if is_match:
            yield config_section


class CredentialNotFoundError(Exception):
    """An error occurred while retrieving credentials or no credentials available."""


class CredentialHelper:
    """Helper for retrieving credentials for http/https git remotes

    Usage:
    >>> helper = CredentialHelper("store") # Use `git credential-store`
    >>> credentials = helper.get("https://github.com/dtrifiro/aprivaterepo")
    >>> username = credentials["username"]
    >>> password = credentials["password"]
    """

    def __init__(self, command: str):
        self._command = command
        self._run_kwargs: Dict[str, Any] = {}
        if self._command[0] == "!":
            # On Windows this will only work in git-bash and/or WSL2
            self._run_kwargs["shell"] = True

    def _prepare_command(self) -> Union[str, List[str]]:
        if self._command[0] == "!":
            return self._command[1:]

        if sys.platform != "win32":
            argv = shlex.split(self._command)
        else:
            # On windows, subprocess.run uses subprocess.list2cmdline() to
            # join arguments when providing a list, so we can just split
            # using whitespace.
            argv = self._command.split()

        if os.path.isabs(argv[0]):
            return argv

        executable = f"git-credential-{argv[0]}"
        if not shutil.which(executable) and shutil.which("git"):
            # If the helper cannot be found in PATH, it might be
            # a C git helper in GIT_EXEC_PATH
            git_exec_path = subprocess.check_output(
                ("git", "--exec-path"),
                universal_newlines=True,  # TODO: replace universal_newlines with `text` when dropping 3.6
            ).strip()
            if shutil.which(executable, path=git_exec_path):
                executable = os.path.join(git_exec_path, executable)

        return [executable, *argv[1:]]

    def get(
        self,
        *,
        protocol: Optional[str] = None,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
    ) -> Tuple[bytes, bytes]:
        cmd = self._prepare_command()
        if isinstance(cmd, str):
            cmd += " get"
        else:
            cmd.append("get")

        helper_input = []
        if protocol:
            helper_input.append(f"protocol={protocol}")
        if hostname:
            helper_input.append(
                f"host={hostname}{':' + str(port) if port is not None else ''}"
            )
        if username:
            helper_input.append(f"username={username}")

        if not helper_input:
            raise ValueError("One of protocol, hostname must be provided")

        helper_input.append("")

        try:
            res = subprocess.run(  # type: ignore # breaks on 3.6
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                input=os.linesep.join(helper_input).encode("ascii"),
                **self._run_kwargs,
            )
        except subprocess.CalledProcessError as exc:
            raise CredentialNotFoundError(exc.stderr) from exc
        except FileNotFoundError as exc:
            raise CredentialNotFoundError("Helper not found") from exc

        credentials = {}
        for line in res.stdout.strip().splitlines():
            try:
                key, value = line.split(b"=")
                credentials[key] = value
            except ValueError:
                continue

        if not all(
            (credentials, b"username" in credentials, b"password" in credentials)
        ):
            raise CredentialNotFoundError("Could not get credentials from helper")

        return credentials[b"username"], credentials[b"password"]

    def store(self, *args, **kwargs):
        """Store the credential, if applicable to the helper"""
        raise NotImplementedError

    def erase(self, *args, **kwargs):
        """Remove a matching credential, if any, from the helper’s storage"""
        raise NotImplementedError


def get_credentials_from_store(
    scheme: bytes,
    hostname: bytes,
    username: Optional[bytes] = None,
    fnames: List[str] = DEFAULT_GIT_CREDENTIALS_PATHS,
):
    for fname in fnames:
        try:
            with open(fname, "rb") as f:
                for line in f:
                    parsed_line = urlparse(line.strip())
                    if (
                        parsed_line.scheme == scheme
                        and parsed_line.hostname == hostname
                        and (username is None or parsed_line.username == username)
                    ):
                        return parsed_line.username, parsed_line.password
        except FileNotFoundError:
            # If the file doesn't exist, try the next one.
            continue


def get_credentials_from_helper(base_url: str, config) -> Tuple[bytes, bytes]:
    """Retrieves credentials for the given url from git credential helpers"""
    if isinstance(config, StackedConfig):
        backends = config.backends
    else:
        backends = [config]

    for conf in backends:
        # We will try to match credential sections' url with the given url,
        # falling back to the generic section if there's no match
        for section in urlmatch_credential_sections(conf, base_url):
            try:
                command = conf.get(section, "helper")
            except KeyError:
                # no helper configured
                continue

            helper = CredentialHelper(
                command.decode(conf.encoding or sys.getdefaultencoding())
            )
            parsed = urlparse(base_url)
            try:
                return helper.get(
                    protocol=parsed.scheme,
                    hostname=parsed.hostname,
                    port=parsed.port,
                    username=parsed.username,
                )
            except CredentialNotFoundError:
                continue

    raise CredentialNotFoundError
