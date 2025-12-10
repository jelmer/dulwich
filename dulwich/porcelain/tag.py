# tag.py -- Porcelain-like tag functions for Dulwich
# Copyright (C) 2013 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Porcelain-like tag functions for Dulwich."""

import sys
import time
from typing import TYPE_CHECKING, TextIO

from dulwich.objects import Tag, parse_timezone

from ..objectspec import (
    parse_object,
)
from ..refs import (
    Ref,
    local_tag_name,
)
from ..repo import get_user_identity

if TYPE_CHECKING:
    from . import RepoPath


def _make_tag_ref(name: str | bytes) -> Ref:
    from . import DEFAULT_ENCODING

    if isinstance(name, str):
        name = name.encode(DEFAULT_ENCODING)
    return local_tag_name(name)


def verify_tag(
    repo: "RepoPath",
    tagname: str | bytes,
    keyids: list[str] | None = None,
) -> None:
    """Verify GPG signature on a tag.

    Args:
      repo: Path to repository
      tagname: Name of tag to verify
      keyids: Optional list of trusted key IDs. If provided, the tag
        must be signed by one of these keys. If not provided, just verifies
        that the tag has a valid signature.

    Raises:
      gpg.errors.BadSignatures: if GPG signature verification fails
      gpg.errors.MissingSignatures: if tag was not signed by a key
        specified in keyids
    """
    from . import Error, open_repo_closing

    with open_repo_closing(repo) as r:
        if isinstance(tagname, str):
            tagname = tagname.encode()
        tag_ref = _make_tag_ref(tagname)
        tag_id = r.refs[tag_ref]
        tag_obj = r[tag_id]
        if not isinstance(tag_obj, Tag):
            raise Error(f"{tagname!r} does not point to a tag object")
        tag_obj.verify(keyids)


def tag_create(
    repo: "RepoPath",
    tag: str | bytes,
    author: str | bytes | None = None,
    message: str | bytes | None = None,
    annotated: bool = False,
    objectish: str | bytes = "HEAD",
    tag_time: int | None = None,
    tag_timezone: int | None = None,
    sign: bool | None = None,
    encoding: str | None = None,
) -> None:
    """Creates a tag in git via dulwich calls.

    Args:
      repo: Path to repository
      tag: tag string
      author: tag author (optional, if annotated is set)
      message: tag message (optional)
      annotated: whether to create an annotated tag
      objectish: object the tag should point at, defaults to HEAD
      tag_time: Optional time for annotated tag
      tag_timezone: Optional timezone for annotated tag
      sign: GPG Sign the tag (bool, defaults to False,
        pass True to use default GPG key,
        pass a str containing Key ID to use a specific GPG key)
      encoding: Encoding to use for tag messages
    """
    from . import (
        DEFAULT_ENCODING,
        get_user_timezones,
        open_repo_closing,
    )

    if encoding is None:
        encoding = DEFAULT_ENCODING
    with open_repo_closing(repo) as r:
        object = parse_object(r, objectish)

        if isinstance(tag, str):
            tag = tag.encode(encoding)

        if annotated:
            # Create the tag object
            tag_obj = Tag()
            if author is None:
                author = get_user_identity(r.get_config_stack())
            elif isinstance(author, str):
                author = author.encode(encoding)
            else:
                assert isinstance(author, bytes)
            tag_obj.tagger = author
            if isinstance(message, str):
                message = message.encode(encoding)
            elif isinstance(message, bytes):
                pass
            else:
                message = b""
            tag_obj.message = message + "\n".encode(encoding)
            tag_obj.name = tag
            tag_obj.object = (type(object), object.id)
            if tag_time is None:
                tag_time = int(time.time())
            tag_obj.tag_time = tag_time
            if tag_timezone is None:
                tag_timezone = get_user_timezones()[1]
            elif isinstance(tag_timezone, str):
                tag_timezone = parse_timezone(tag_timezone.encode())
            tag_obj.tag_timezone = tag_timezone

            # Check if we should sign the tag
            config = r.get_config_stack()

            if sign is None:
                # Check tag.gpgSign configuration when sign is not explicitly set
                try:
                    should_sign = config.get_boolean(
                        (b"tag",), b"gpgsign", default=False
                    )
                except KeyError:
                    should_sign = False  # Default to not signing if no config
            else:
                should_sign = sign

            # Get the signing key from config if signing is enabled
            keyid = None
            if should_sign:
                try:
                    keyid_bytes = config.get((b"user",), b"signingkey")
                    keyid = keyid_bytes.decode() if keyid_bytes else None
                except KeyError:
                    keyid = None
                tag_obj.sign(keyid)

            r.object_store.add_object(tag_obj)
            tag_id = tag_obj.id
        else:
            tag_id = object.id

        r.refs[_make_tag_ref(tag)] = tag_id


def tag_list(repo: "RepoPath", outstream: TextIO = sys.stdout) -> list[Ref]:
    """List all tags.

    Args:
      repo: Path to repository
      outstream: Stream to write tags to
    """
    from . import open_repo_closing

    with open_repo_closing(repo) as r:
        tags: list[Ref] = sorted(r.refs.as_dict(Ref(b"refs/tags")))
        return tags


def tag_delete(repo: "RepoPath", name: str | bytes) -> None:
    """Remove a tag.

    Args:
      repo: Path to repository
      name: Name of tag to remove
    """
    from . import Error, open_repo_closing

    with open_repo_closing(repo) as r:
        if isinstance(name, bytes):
            names = [name]
        elif isinstance(name, list):
            names = name
        else:
            raise Error(f"Unexpected tag name type {name!r}")
        for name in names:
            del r.refs[_make_tag_ref(name)]
