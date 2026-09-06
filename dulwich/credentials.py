# credentials.py -- support for git credential helpers

# Copyright (C) 2022 Daniele Trifirò <daniele@iterative.ai>
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

"""Support for git credential helpers.

https://git-scm.com/book/en/v2/Git-Tools-Credential-Storage

"""

__all__ = [
    "CredentialHelper",
    "CredentialNotFound",
    "InvalidCredentialDescription",
    "fill_credential",
    "format_credential_description",
    "helpers_for_url",
    "match_partial_url",
    "match_urls",
    "parse_credential_description",
    "url_for_credential",
    "urlmatch_credential_sections",
]

import os
import shutil
import subprocess
from collections.abc import Iterable, Iterator, Mapping
from urllib.parse import ParseResult, urlparse

from .config import Config, SectionLike


def match_urls(url: ParseResult, url_prefix: ParseResult) -> bool:
    """Check if a URL matches a URL prefix.

    Args:
      url: Parsed URL to check
      url_prefix: Parsed URL prefix to match against

    Returns:
      True if url matches the prefix
    """
    base_match = (
        url.scheme == url_prefix.scheme
        and url.hostname == url_prefix.hostname
        and url.port == url_prefix.port
    )
    user_match = url.username == url_prefix.username if url_prefix.username else True
    # Match the path on path-segment boundaries, like git's urlmatch: a config
    # path scopes the entry to that path and anything below it, so "/foo"
    # matches "/foo" and "/foo/bar" but not "/foobar".
    prefix_path = url_prefix.path.rstrip("/")
    path_match = url.path == prefix_path or url.path.startswith(prefix_path + "/")
    return base_match and user_match and path_match


def match_partial_url(valid_url: ParseResult, partial_url: str) -> bool:
    """Matches a parsed url with a partial url (no scheme/netloc)."""
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
    config: Config, url: str | None
) -> Iterator[SectionLike]:
    """Returns credential sections from the config which match the given URL."""
    parsed_url = urlparse(url or "")
    for config_section in config.sections():
        if config_section[0] != b"credential":
            continue

        if len(config_section) < 2:
            yield config_section
            continue

        # git documents config files as UTF-8, and a credential subsection is
        # a URL, which RFC 3986 restricts to ASCII. Decoding with the config's
        # own ``encoding`` is wrong for a StackedConfig, whose backends may
        # each declare a different one.
        config_url = config_section[1].decode("utf-8", errors="replace")
        parsed_config_url = urlparse(config_url)
        if parsed_config_url.scheme and parsed_config_url.netloc:
            is_match = match_urls(parsed_url, parsed_config_url)
        else:
            is_match = match_partial_url(parsed_url, config_url)

        if is_match:
            yield config_section


class InvalidCredentialDescription(Exception):
    """A credential description could not be parsed."""


class CredentialNotFound(Exception):
    """No helper could supply the requested credential."""


# gitcredentials(7): "url" is expanded into its components rather than being a
# field of its own, and the components it sets are overridden by any explicit
# key that follows it.
_URL_KEY = "url"

# Attributes a helper may return. Anything else is dropped rather than passed
# on, so a helper cannot inject an attribute that changes where a later helper
# (or the caller) sends the credential.
CREDENTIAL_ATTRIBUTES = (
    "protocol",
    "host",
    "path",
    "username",
    "password",
    "password_expiry_utc",
    "oauth_refresh_token",
)


def parse_credential_description(lines: Iterable[str]) -> dict[str, str]:
    """Parse the git credential description format.

    A sequence of ``key=value`` lines terminated by a blank line or by the end
    of input. A ``url`` key is expanded into its components, and, per
    gitcredentials(7), an explicit key that follows overrides what the URL set.

    Args:
      lines: Lines of the description, with or without trailing newlines.

    Returns:
      The parsed attributes.

    Raises:
      InvalidCredentialDescription: If a line has no ``=``.
    """
    credential: dict[str, str] = {}
    for raw in lines:
        line = raw.rstrip("\n")
        if not line:
            break
        key, sep, value = line.partition("=")
        if not sep:
            raise InvalidCredentialDescription(
                f"credential description line has no '=': {line!r}"
            )
        if key == _URL_KEY:
            credential.update(_split_url(value))
        else:
            credential[key] = value
    return credential


def _split_url(url: str) -> dict[str, str]:
    """Expand a ``url=`` line into the components it implies."""
    parsed = urlparse(url)
    components: dict[str, str] = {}
    if parsed.scheme:
        components["protocol"] = parsed.scheme
    if parsed.hostname:
        host = parsed.hostname
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"
        components["host"] = host
    if parsed.path and parsed.path != "/":
        components["path"] = parsed.path.lstrip("/")
    if parsed.username:
        components["username"] = parsed.username
    if parsed.password:
        components["password"] = parsed.password
    return components


def format_credential_description(credential: Mapping[str, str]) -> str:
    """Render attributes in the git credential description format.

    Args:
      credential: The attributes to render.

    Returns:
      The description, terminated by a blank line.

    Raises:
      ValueError: If a value contains a newline or a NUL. Git rejects these
        because the format is line-oriented: a newline in a value would be
        read back as a separate attribute, which is how a hostile value in one
        field becomes an injected password in another.
    """
    lines = []
    for key, value in credential.items():
        if "\n" in value or "\x00" in value:
            raise ValueError(f"credential value for {key!r} contains a newline or NUL")
        lines.append(f"{key}={value}")
    return "".join(line + "\n" for line in lines) + "\n"


class CredentialHelper:
    """An external git credential helper.

    Args:
      command: The value of a ``credential.helper`` config entry.
    """

    def __init__(self, command: str) -> None:
        """Set up a helper from its configured command."""
        self.command = command

    def __repr__(self) -> str:
        """Return a representation naming the configured command."""
        return f"{type(self).__name__}({self.command!r})"

    def __eq__(self, other: object) -> bool:
        """Two helpers are equal when they name the same command."""
        return isinstance(other, CredentialHelper) and other.command == self.command

    def __hash__(self) -> int:
        """Hash consistently with __eq__."""
        return hash(self.command)

    def _argv(self, operation: str) -> tuple[list[str], bool]:
        """Build the command line for an operation.

        Returns:
          The argv, and whether it must run through a shell.
        """
        command = self.command
        # gitcredentials(7): a leading "!" means the rest is a shell command.
        if command.startswith("!"):
            return [f"{command[1:]} {operation}"], True
        # An absolute path, or anything carrying a path separator, is used
        # as-is; a bare word names git-credential-<word>. Testing for a
        # separator rather than only os.path.isabs is what makes "./helper"
        # work.
        if os.path.isabs(command) or os.sep in command or "/" in command:
            return [command, operation], False
        return [f"git-credential-{command}", operation], False

    def is_available(self) -> bool:
        """Whether the helper program can be found.

        A configured-but-missing helper is ordinary -- a keychain helper in a
        dotfiles repository shared across machines -- and git skips it rather
        than failing, so callers can use this to do the same.
        """
        argv, shell = self._argv("get")
        if shell:
            return True
        return shutil.which(argv[0]) is not None

    def _run(self, operation: str, credential: Mapping[str, str]) -> str | None:
        argv, shell = self._argv(operation)
        try:
            process = subprocess.run(
                argv[0] if shell else argv,
                shell=shell,
                input=format_credential_description(credential),
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            return None
        if process.returncode != 0:
            return None
        return process.stdout

    def get(self, credential: Mapping[str, str]) -> dict[str, str] | None:
        """Ask the helper to supply missing attributes.

        Returns:
          The attributes the helper supplied, or None if it declined or could
          not be run.
        """
        output = self._run("get", credential)
        if output is None:
            return None
        try:
            supplied = parse_credential_description(output.splitlines())
        except InvalidCredentialDescription:
            return None
        return {k: v for k, v in supplied.items() if k in CREDENTIAL_ATTRIBUTES}

    def store(self, credential: Mapping[str, str]) -> None:
        """Tell the helper that the credential worked."""
        self._run("store", credential)

    def erase(self, credential: Mapping[str, str]) -> None:
        """Tell the helper that the credential did not work."""
        self._run("erase", credential)


def helpers_for_url(config: Config, url: str | None) -> list[CredentialHelper]:
    """Return the credential helpers configured for a URL, in order.

    Both ``[credential] helper`` and ``[credential "<url>"] helper`` are
    consulted, most general first, matching git's precedence.

    An empty ``helper`` value resets the list. Git documents this as the way to
    discard helpers configured in a more general scope; without it a repository
    could not opt out of a system-wide helper.
    """
    helpers: list[CredentialHelper] = []
    for section in urlmatch_credential_sections(config, url):
        for value in config.get_multivar(section, "helper"):
            command = value.decode("utf-8", errors="replace")
            if not command:
                helpers.clear()
            else:
                helpers.append(CredentialHelper(command))
    return helpers


def fill_credential(config: Config, credential: Mapping[str, str]) -> dict[str, str]:
    """Complete a credential, consulting config and then the helpers.

    Args:
      config: Configuration to read ``credential.*`` from.
      credential: The attributes known so far.

    Returns:
      The completed attributes.

    Raises:
      CredentialNotFound: If no username and password could be obtained.
    """
    filled = dict(credential)
    url = url_for_credential(filled)

    # A configured username seeds the request, but a configured password is
    # deliberately not read: git has no `credential.password`, and inventing
    # one here would put a plaintext password in a config file on a path
    # nothing else in dulwich reads.
    if "username" not in filled:
        for section in urlmatch_credential_sections(config, url):
            try:
                username = config.get(section, "username")
            except KeyError:
                continue
            filled["username"] = username.decode("utf-8", errors="replace")

    for helper in helpers_for_url(config, url):
        supplied = helper.get(filled)
        if supplied:
            filled.update(supplied)
        if "username" in filled and "password" in filled:
            break

    if "username" not in filled or "password" not in filled:
        raise CredentialNotFound(
            "no credential helper supplied a username and password"
        )
    return filled


def url_for_credential(credential: Mapping[str, str]) -> str | None:
    """Rebuild the URL that a set of attributes describes."""
    protocol = credential.get("protocol")
    host = credential.get("host")
    if not protocol or not host:
        return None
    url = f"{protocol}://{host}"
    path = credential.get("path")
    if path:
        url = f"{url}/{path}"
    return url
