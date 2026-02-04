# bundle_uri.py -- Bundle URI support
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Bundle URI support for faster clones and fetches.

This module implements support for Git bundle URIs as specified in
https://git-scm.com/docs/bundle-uri

Bundle URIs allow Git servers to advertise locations where clients can
download pre-computed bundles to speed up clones and fetches.
"""

__all__ = [
    "BundleList",
    "BundleListEntry",
    "BundleURIError",
    "fetch_bundle_uri",
    "parse_bundle_list",
]

from collections.abc import Callable
from dataclasses import dataclass, field
from io import BytesIO
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

if TYPE_CHECKING:
    from .bundle import Bundle
    from .repo import BaseRepo

# Bundle URI protocol v2 capability
CAPABILITY_BUNDLE_URI = b"bundle-uri"


class BundleURIError(Exception):
    """Error related to bundle URI operations."""


@dataclass
class BundleListEntry:
    """Represents a single bundle in a bundle list.

    Attributes:
        id: The bundle identifier (alphanumeric and dashes only)
        uri: The URI to download the bundle from (absolute or relative)
        filter: Object filter (e.g., "blob:none" for blobless clones)
        creation_token: A non-negative integer for sorting bundles
        location: Real-world location hint (e.g., "eastus", "europe")
    """

    id: str
    uri: str
    filter: str | None = None
    creation_token: int | None = None
    location: str | None = None


@dataclass
class BundleList:
    """Represents a bundle list as defined in the bundle URI specification.

    A bundle list contains metadata about available bundles that can be
    downloaded to speed up clones and fetches.

    Attributes:
        version: Bundle list version (currently only 1 is supported)
        mode: Either "all" (need all bundles) or "any" (any one suffices)
        heuristic: Optional heuristic name (e.g., "creationToken")
        entries: List of BundleListEntry objects
    """

    version: int = 1
    mode: str = "all"
    heuristic: str | None = None
    entries: list[BundleListEntry] = field(default_factory=list)


def parse_bundle_list(data: bytes, base_uri: str | None = None) -> BundleList:
    """Parse a bundle list from Git config format.

    Args:
        data: The raw bundle list data in Git config format
        base_uri: Base URI for resolving relative URIs in the bundle list

    Returns:
        A BundleList object representing the parsed bundle list

    Raises:
        BundleURIError: If the bundle list format is invalid
    """
    from .config import ConfigFile

    try:
        config = ConfigFile.from_file(BytesIO(data))
    except Exception as e:
        raise BundleURIError(f"Failed to parse bundle list as config: {e}") from e

    # Parse bundle section
    try:
        version = int(config.get((b"bundle",), b"version").decode("utf-8"))
    except KeyError:
        raise BundleURIError("bundle.version is required")
    except (ValueError, UnicodeDecodeError) as e:
        raise BundleURIError(f"Invalid bundle.version: {e}") from e

    if version != 1:
        raise BundleURIError(f"Unsupported bundle list version: {version}")

    try:
        mode = config.get((b"bundle",), b"mode").decode("utf-8")
    except KeyError:
        raise BundleURIError("bundle.mode is required")
    except UnicodeDecodeError as e:
        raise BundleURIError(f"Invalid bundle.mode encoding: {e}") from e

    if mode not in ("all", "any"):
        raise BundleURIError(f"Invalid bundle.mode: {mode}")

    heuristic = None
    try:
        heuristic = config.get((b"bundle",), b"heuristic").decode("utf-8")
    except KeyError:
        pass
    except UnicodeDecodeError as e:
        raise BundleURIError(f"Invalid bundle.heuristic encoding: {e}") from e

    bundle_list = BundleList(version=version, mode=mode, heuristic=heuristic)

    # Parse bundle entries (bundle.<id>.* sections)
    seen_ids: set[str] = set()
    for section_tuple in config.keys():
        if len(section_tuple) != 2 or section_tuple[0] != b"bundle":
            continue

        bundle_id = section_tuple[1].decode("utf-8")
        if bundle_id in seen_ids:
            continue
        seen_ids.add(bundle_id)

        # Get the URI (required)
        try:
            uri = config.get(section_tuple, b"uri").decode("utf-8")
        except KeyError:
            # No URI means this is not a bundle entry
            continue
        except UnicodeDecodeError as e:
            raise BundleURIError(
                f"Invalid URI encoding for bundle {bundle_id}: {e}"
            ) from e

        # Resolve relative URIs
        if base_uri and not _is_absolute_uri(uri):
            uri = _resolve_relative_uri(base_uri, uri)

        entry = BundleListEntry(id=bundle_id, uri=uri)

        # Parse optional fields
        try:
            entry.filter = config.get(section_tuple, b"filter").decode("utf-8")
        except KeyError:
            pass
        except UnicodeDecodeError as e:
            raise BundleURIError(
                f"Invalid filter encoding for bundle {bundle_id}: {e}"
            ) from e

        try:
            token_str = config.get(section_tuple, b"creationToken").decode("utf-8")
            entry.creation_token = int(token_str)
            if entry.creation_token < 0:
                raise BundleURIError(
                    f"creationToken must be non-negative for bundle {bundle_id}"
                )
        except KeyError:
            pass
        except (ValueError, UnicodeDecodeError) as e:
            raise BundleURIError(
                f"Invalid creationToken for bundle {bundle_id}: {e}"
            ) from e

        try:
            entry.location = config.get(section_tuple, b"location").decode("utf-8")
        except KeyError:
            pass
        except UnicodeDecodeError as e:
            raise BundleURIError(
                f"Invalid location encoding for bundle {bundle_id}: {e}"
            ) from e

        bundle_list.entries.append(entry)

    return bundle_list


def _is_absolute_uri(uri: str) -> bool:
    """Check if a URI is absolute (has a scheme)."""
    parsed = urlparse(uri)
    return bool(parsed.scheme)


def _resolve_relative_uri(base_uri: str, relative_uri: str) -> str:
    """Resolve a relative URI against a base URI.

    Args:
        base_uri: The base URI to resolve against
        relative_uri: The relative URI to resolve

    Returns:
        The resolved absolute URI
    """
    if relative_uri.startswith("/"):
        # Relative to domain root
        parsed = urlparse(base_uri)
        return f"{parsed.scheme}://{parsed.netloc}{relative_uri}"
    else:
        # Relative to base URI path
        return urljoin(base_uri, relative_uri)


def _is_bundle_file(data: bytes) -> bool:
    """Check if data starts with a bundle file header."""
    return data.startswith((b"# v2 git bundle\n", b"# v3 git bundle\n"))


def fetch_bundle_uri(
    uri: str,
    progress: Callable[[bytes], None] | None = None,
    timeout: float | None = None,
) -> tuple["Bundle | None", "BundleList | None"]:
    """Fetch content from a bundle URI.

    The URI may point to either a bundle file or a bundle list.

    Args:
        uri: The URI to fetch from (HTTP/HTTPS)
        progress: Optional callback for progress reporting
        timeout: Optional timeout for the HTTP request

    Returns:
        A tuple of (Bundle, None) if the URI points to a bundle file,
        or (None, BundleList) if the URI points to a bundle list.

    Raises:
        BundleURIError: If the fetch fails or the content is invalid
    """
    import urllib3

    from .bundle import read_bundle

    parsed = urlparse(uri)
    if parsed.scheme not in ("http", "https"):
        raise BundleURIError(f"Unsupported URI scheme: {parsed.scheme}")

    http = urllib3.PoolManager(timeout=timeout)
    try:
        response = http.request("GET", uri)

        if response.status != 200:
            raise BundleURIError(f"HTTP error {response.status} fetching {uri}")

        data = response.data
    except urllib3.exceptions.HTTPError as e:
        raise BundleURIError(f"HTTP error fetching {uri}: {e}") from e
    finally:
        http.clear()

    if progress:
        progress(f"Received {len(data)} bytes from {uri}".encode())

    # Try to parse as a bundle first
    if _is_bundle_file(data):
        try:
            bundle = read_bundle(BytesIO(data))
            return bundle, None
        except Exception as e:
            raise BundleURIError(f"Failed to parse bundle from {uri}: {e}") from e

    # Try to parse as a bundle list
    try:
        bundle_list = parse_bundle_list(data, base_uri=uri)
        return None, bundle_list
    except BundleURIError:
        raise
    except Exception as e:
        raise BundleURIError(
            f"Content at {uri} is neither a bundle nor a bundle list: {e}"
        ) from e


def apply_bundle_uri(
    repo: "BaseRepo",
    uri: str,
    filter_spec: str | None = None,
    stored_creation_token: int | None = None,
    progress: Callable[[bytes], None] | None = None,
) -> tuple[int | None, dict[bytes, bytes]]:
    """Apply bundles from a bundle URI to a repository.

    This function fetches bundles from the given URI and applies them
    to the repository, potentially speeding up subsequent fetches.

    Args:
        repo: The target repository to apply bundles to
        uri: The bundle URI to fetch from
        filter_spec: Object filter to match (e.g., "blob:none")
        stored_creation_token: Previously stored creation token to skip
            bundles that have already been applied
        progress: Optional callback for progress reporting

    Returns:
        A tuple of (``creation_token``, ``refs``) where:
        - ``creation_token`` is the highest creation token seen (for
          storing and skipping in future fetches), or None if not available
        - ``refs`` is a dict mapping ref names to object IDs from the bundles

    Raises:
        BundleURIError: If fetching or applying bundles fails
    """
    bundle, bundle_list = fetch_bundle_uri(uri, progress=progress)

    all_refs: dict[bytes, bytes] = {}

    if bundle is not None:
        # Single bundle case
        try:
            bundle.store_objects(
                repo.object_store,
                progress=lambda msg: progress(msg.encode("utf-8"))
                if progress
                else None,
            )
            for ref, sha in bundle.references.items():
                all_refs[ref] = sha
        finally:
            bundle.close()
        return None, all_refs

    # Bundle list case
    assert bundle_list is not None

    if bundle_list.mode == "any":
        # Pick any one bundle that matches our filter
        matching_entries = _filter_bundle_entries(bundle_list.entries, filter_spec)
        if not matching_entries:
            if progress:
                progress(b"No matching bundles found in bundle list")
            return None, {}

        # Try entries in order until one succeeds
        for entry in matching_entries:
            try:
                entry_bundle, _ = fetch_bundle_uri(entry.uri, progress=progress)
                if entry_bundle is not None:
                    try:
                        entry_bundle.store_objects(
                            repo.object_store,
                            progress=lambda msg: progress(msg.encode("utf-8"))
                            if progress
                            else None,
                        )
                        for ref, sha in entry_bundle.references.items():
                            all_refs[ref] = sha
                    finally:
                        entry_bundle.close()
                    return entry.creation_token, all_refs
            except BundleURIError as e:
                if progress:
                    progress(f"Failed to fetch bundle {entry.id}: {e}".encode())
                continue

        raise BundleURIError("Failed to fetch any bundle from the bundle list")

    # mode == "all" - need all bundles
    matching_entries = _filter_bundle_entries(bundle_list.entries, filter_spec)

    if not matching_entries:
        if progress:
            progress(b"No matching bundles found in bundle list")
        return None, {}

    # Sort by creation token if using that heuristic
    if bundle_list.heuristic == "creationToken":
        # Sort by creation token descending (newest first for downloading)
        matching_entries = sorted(
            matching_entries,
            key=lambda e: e.creation_token if e.creation_token is not None else 0,
            reverse=True,
        )

        # Filter out bundles we've already seen
        if stored_creation_token is not None:
            matching_entries = [
                e
                for e in matching_entries
                if e.creation_token is not None
                and e.creation_token > stored_creation_token
            ]

            if not matching_entries:
                if progress:
                    progress(b"All bundles already applied (based on creation token)")
                return stored_creation_token, {}

    # Download bundles
    downloaded_bundles: list[tuple[BundleListEntry, Bundle]] = []
    latest_token: int | None = None
    failed_bundles = []

    for entry in matching_entries:
        try:
            entry_bundle, nested_list = fetch_bundle_uri(entry.uri, progress=progress)
            if entry_bundle is not None:
                downloaded_bundles.append((entry, entry_bundle))
                if entry.creation_token is not None:
                    if latest_token is None or entry.creation_token > latest_token:
                        latest_token = entry.creation_token
            elif nested_list is not None:
                # Nested bundle lists are not supported yet
                if progress:
                    progress(f"Skipping nested bundle list at {entry.uri}".encode())
        except BundleURIError as e:
            failed_bundles.append(entry.id)
            if progress:
                progress(f"Failed to fetch bundle {entry.id}: {e}".encode())

    if not downloaded_bundles:
        error_msg = "No bundles could be downloaded"
        if failed_bundles:
            error_msg += f" (failed: {', '.join(failed_bundles)})"
        if progress:
            progress(error_msg.encode())
        return stored_creation_token, {}

    # Sort bundles by creation token ascending for application (oldest first)
    if bundle_list.heuristic == "creationToken":
        downloaded_bundles.sort(
            key=lambda x: x[0].creation_token if x[0].creation_token is not None else 0
        )

    # Apply bundles in order
    for entry, entry_bundle in downloaded_bundles:
        try:
            # Check prerequisites
            missing_prereqs = []
            for prereq_sha, _ in entry_bundle.prerequisites:
                if prereq_sha not in repo.object_store:
                    missing_prereqs.append(prereq_sha)

            if missing_prereqs:
                if progress:
                    progress(
                        f"Bundle {entry.id} has missing prerequisites, skipping".encode()
                    )
                continue

            entry_bundle.store_objects(
                repo.object_store,
                progress=lambda msg: progress(msg.encode("utf-8"))
                if progress
                else None,
            )
            for ref, sha in entry_bundle.references.items():
                all_refs[ref] = sha

            if progress:
                progress(f"Applied bundle {entry.id}".encode())
        finally:
            entry_bundle.close()

    return latest_token, all_refs


def _filter_bundle_entries(
    entries: list[BundleListEntry], filter_spec: str | None
) -> list[BundleListEntry]:
    """Filter bundle entries based on object filter specification.

    Args:
        entries: List of bundle entries to filter
        filter_spec: Object filter to match (e.g., "blob:none"), or None
            for full clones

    Returns:
        List of matching bundle entries
    """
    result = []
    for entry in entries:
        if filter_spec is None:
            # Full clone - only include bundles without filters
            if entry.filter is None:
                result.append(entry)
        else:
            # Partial clone - match filter exactly
            if entry.filter == filter_spec:
                result.append(entry)
    return result


def parse_bundle_uri_advertisement(
    capabilities: list[tuple[bytes, bytes | None]],
) -> list[tuple[str, str | None]]:
    """Parse bundle URI advertisement from protocol v2 capabilities.

    Extracts bundle-uri related capabilities from a Git protocol v2
    capability advertisement. The server may advertise bundle URIs
    as part of its capabilities to allow clients to bootstrap clones.

    Args:
        capabilities: List of (key, value) capability pairs from the server

    Returns:
        List of (key, value) pairs where key starts with "bundle-uri".
        The key is the capability name (e.g., "bundle-uri") and value
        is the URI string or None if no value was provided. Returns
        an empty list if no bundle URI capabilities are advertised.
    """
    result = []
    for key, value in capabilities:
        if key.startswith(b"bundle-uri"):
            key_str = key.decode("utf-8")
            value_str = value.decode("utf-8") if value else None
            result.append((key_str, value_str))
    return result
