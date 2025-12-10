# rerere.py -- Git rerere (reuse recorded resolution) implementation
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Git rerere (reuse recorded resolution) implementation.

This module implements Git's rerere functionality, which records and reuses
manual conflict resolutions. When a merge conflict occurs, rerere records the
conflict and the resolution. If the same conflict happens again, rerere can
automatically apply the recorded resolution.
"""

__all__ = [
    "RerereCache",
    "is_rerere_autoupdate",
    "is_rerere_enabled",
    "rerere_auto",
]

import hashlib
import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dulwich.config import StackedConfig
    from dulwich.repo import Repo


def _compute_conflict_id(preimage: bytes) -> str:
    """Compute a unique ID for a conflict based on its preimage.

    The preimage is the conflict with all conflict markers normalized
    to a canonical form (without side names).

    Args:
        preimage: The normalized conflict content

    Returns:
        Hex digest of the SHA-1 hash
    """
    return hashlib.sha1(preimage).hexdigest()


def _normalize_conflict_markers(content: bytes) -> bytes:
    """Normalize conflict markers to a canonical form.

    This removes the side names (ours/theirs) from conflict markers
    to create a preimage that only depends on the structure of the conflict.

    Args:
        content: File content with conflict markers

    Returns:
        Normalized content with generic conflict markers
    """
    lines = content.split(b"\n")
    result = []

    for line in lines:
        # Normalize conflict start markers
        if line.startswith(b"<<<<<<<"):
            result.append(b"<<<<<<<")
        # Normalize conflict end markers
        elif line.startswith(b">>>>>>>"):
            result.append(b">>>>>>>")
        # Keep separator as is
        elif line == b"=======":
            result.append(line)
        else:
            result.append(line)

    return b"\n".join(result)


def _extract_conflict_regions(content: bytes) -> list[tuple[bytes, bytes, bytes]]:
    """Extract conflict regions from file content.

    Args:
        content: File content with conflict markers

    Returns:
        List of tuples (ours, separator, theirs) for each conflict region
    """
    conflicts = []
    lines = content.split(b"\n")
    i = 0

    while i < len(lines):
        if lines[i].startswith(b"<<<<<<<"):
            # Found conflict start
            ours_lines = []
            theirs_lines = []
            i += 1

            # Collect "ours" lines
            while i < len(lines) and not lines[i].startswith(b"======="):
                ours_lines.append(lines[i])
                i += 1

            if i >= len(lines):
                break

            # Skip separator
            i += 1

            # Collect "theirs" lines
            while i < len(lines) and not lines[i].startswith(b">>>>>>>"):
                theirs_lines.append(lines[i])
                i += 1

            if i >= len(lines):
                break

            ours = b"\n".join(ours_lines)
            theirs = b"\n".join(theirs_lines)
            conflicts.append((ours, b"=======", theirs))

        i += 1

    return conflicts


def _has_conflict_markers(content: bytes) -> bool:
    """Check if content contains conflict markers.

    Args:
        content: File content to check

    Returns:
        True if conflict markers are present
    """
    return b"<<<<<<<" in content and b"=======" in content and b">>>>>>>" in content


def _remove_conflict_markers(content: bytes) -> bytes:
    """Remove all conflict markers from resolved content.

    This is used to extract the resolution from a file that has been
    manually resolved by the user.

    Args:
        content: Resolved file content

    Returns:
        Content with conflict markers removed
    """
    lines = content.split(b"\n")
    result = []
    in_conflict = False

    for line in lines:
        if line.startswith(b"<<<<<<<"):
            in_conflict = True
            continue
        elif line.startswith(b">>>>>>>"):
            in_conflict = False
            continue
        elif line == b"=======":
            continue

        if not in_conflict:
            result.append(line)

    return b"\n".join(result)


class RerereCache:
    """Manages the rerere cache in .git/rr-cache/."""

    def __init__(self, rr_cache_dir: bytes | str) -> None:
        """Initialize RerereCache.

        Args:
            rr_cache_dir: Path to the .git/rr-cache directory
        """
        if isinstance(rr_cache_dir, bytes):
            self.rr_cache_dir = os.fsdecode(rr_cache_dir)
        else:
            self.rr_cache_dir = rr_cache_dir

    @classmethod
    def from_repo(cls, repo: "Repo") -> "RerereCache":
        """Create a RerereCache from a repository.

        Args:
            repo: A Dulwich repository object

        Returns:
            RerereCache instance for the repository
        """
        rr_cache_dir = os.path.join(repo.controldir(), "rr-cache")
        return cls(rr_cache_dir)

    def _ensure_cache_dir(self) -> None:
        """Ensure the rr-cache directory exists."""
        os.makedirs(self.rr_cache_dir, exist_ok=True)

    def _get_conflict_dir(self, conflict_id: str) -> str:
        """Get the directory path for a specific conflict.

        Args:
            conflict_id: The conflict ID (SHA-1 hash)

        Returns:
            Path to the conflict directory
        """
        return os.path.join(self.rr_cache_dir, conflict_id)

    def _get_preimage_path(self, conflict_id: str) -> str:
        """Get the path to the preimage file for a conflict.

        Args:
            conflict_id: The conflict ID

        Returns:
            Path to the preimage file
        """
        return os.path.join(self._get_conflict_dir(conflict_id), "preimage")

    def _get_postimage_path(self, conflict_id: str) -> str:
        """Get the path to the postimage file for a conflict.

        Args:
            conflict_id: The conflict ID

        Returns:
            Path to the postimage file
        """
        return os.path.join(self._get_conflict_dir(conflict_id), "postimage")

    def record_conflict(self, path: bytes, content: bytes) -> str | None:
        """Record a conflict in the rerere cache.

        Args:
            path: Path to the conflicted file
            content: File content with conflict markers

        Returns:
            The conflict ID if recorded, None if no conflict markers found
        """
        if not _has_conflict_markers(content):
            return None

        self._ensure_cache_dir()

        # Normalize conflict markers and compute ID
        preimage = _normalize_conflict_markers(content)
        conflict_id = _compute_conflict_id(preimage)

        # Create conflict directory
        conflict_dir = self._get_conflict_dir(conflict_id)
        os.makedirs(conflict_dir, exist_ok=True)

        # Write preimage
        preimage_path = self._get_preimage_path(conflict_id)
        with open(preimage_path, "wb") as f:
            f.write(content)

        return conflict_id

    def record_resolution(self, conflict_id: str, content: bytes) -> None:
        """Record a resolution for a previously recorded conflict.

        Args:
            conflict_id: The conflict ID to record resolution for
            content: Resolved file content (without conflict markers)
        """
        # Write postimage
        postimage_path = self._get_postimage_path(conflict_id)

        # Ensure directory exists
        conflict_dir = self._get_conflict_dir(conflict_id)
        os.makedirs(conflict_dir, exist_ok=True)

        with open(postimage_path, "wb") as f:
            f.write(content)

    def has_resolution(self, conflict_id: str) -> bool:
        """Check if a resolution exists for a conflict.

        Args:
            conflict_id: The conflict ID

        Returns:
            True if a postimage exists for this conflict
        """
        postimage_path = self._get_postimage_path(conflict_id)
        return os.path.exists(postimage_path)

    def get_resolution(self, conflict_id: str) -> bytes | None:
        """Get the recorded resolution for a conflict.

        Args:
            conflict_id: The conflict ID

        Returns:
            The resolution content, or None if not found
        """
        postimage_path = self._get_postimage_path(conflict_id)

        try:
            with open(postimage_path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def apply_resolution(self, conflict_id: str, content: bytes) -> bytes | None:
        """Apply a recorded resolution to current conflict content.

        Args:
            conflict_id: The conflict ID
            content: Current file content with conflict markers

        Returns:
            Resolved content, or None if resolution couldn't be applied
        """
        resolution = self.get_resolution(conflict_id)
        if resolution is None:
            return None

        # For now, return the resolution directly
        # A more sophisticated implementation would merge the resolution
        # with the current conflict
        return resolution

    def forget(self, conflict_id: str) -> None:
        """Forget a recorded conflict and its resolution.

        Args:
            conflict_id: The conflict ID to forget
        """
        conflict_dir = self._get_conflict_dir(conflict_id)

        # Remove preimage and postimage files
        for filename in ["preimage", "postimage"]:
            path = os.path.join(conflict_dir, filename)
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

        # Remove conflict directory if empty
        try:
            os.rmdir(conflict_dir)
        except OSError:
            pass

    def clear(self) -> None:
        """Clear all recorded conflicts and resolutions."""
        if not os.path.exists(self.rr_cache_dir):
            return

        # Remove all conflict directories
        for entry in os.listdir(self.rr_cache_dir):
            conflict_dir = os.path.join(self.rr_cache_dir, entry)
            if os.path.isdir(conflict_dir):
                # Remove all files in the directory
                for filename in os.listdir(conflict_dir):
                    os.remove(os.path.join(conflict_dir, filename))
                os.rmdir(conflict_dir)

    def gc(self, max_age_days: int = 60) -> None:
        """Garbage collect old conflict resolutions.

        Args:
            max_age_days: Maximum age in days for keeping resolutions
        """
        if not os.path.exists(self.rr_cache_dir):
            return

        cutoff_time = time.time() - (max_age_days * 24 * 60 * 60)

        for entry in os.listdir(self.rr_cache_dir):
            conflict_dir = os.path.join(self.rr_cache_dir, entry)
            if not os.path.isdir(conflict_dir):
                continue

            postimage_path = os.path.join(conflict_dir, "postimage")

            # Only remove if postimage exists and is old
            if os.path.exists(postimage_path):
                mtime = os.path.getmtime(postimage_path)
                if mtime < cutoff_time:
                    self.forget(entry)

    def status(self) -> list[tuple[str, bool]]:
        """Get the status of all conflicts in the cache.

        Returns:
            List of tuples (conflict_id, has_resolution)
        """
        if not os.path.exists(self.rr_cache_dir):
            return []

        result = []
        for entry in os.listdir(self.rr_cache_dir):
            conflict_dir = os.path.join(self.rr_cache_dir, entry)
            if os.path.isdir(conflict_dir):
                has_res = self.has_resolution(entry)
                result.append((entry, has_res))

        return sorted(result)

    def diff(self, conflict_id: str) -> tuple[bytes | None, bytes | None]:
        """Get the preimage and postimage for a conflict.

        Args:
            conflict_id: The conflict ID

        Returns:
            Tuple of (preimage, postimage), either may be None
        """
        preimage_path = self._get_preimage_path(conflict_id)
        postimage_path = self._get_postimage_path(conflict_id)

        preimage = None
        postimage = None

        try:
            with open(preimage_path, "rb") as f:
                preimage = f.read()
        except FileNotFoundError:
            pass

        try:
            with open(postimage_path, "rb") as f:
                postimage = f.read()
        except FileNotFoundError:
            pass

        return preimage, postimage


def is_rerere_enabled(config: "StackedConfig") -> bool:
    """Check if rerere is enabled in the config.

    Args:
        config: Git configuration

    Returns:
        True if rerere is enabled
    """
    try:
        enabled = config.get((b"rerere",), b"enabled")
        if enabled is None:
            return False
        if isinstance(enabled, bytes):
            return enabled.lower() in (b"true", b"1", b"yes", b"on")
        return bool(enabled)
    except KeyError:
        return False


def is_rerere_autoupdate(config: "StackedConfig") -> bool:
    """Check if rerere.autoupdate is enabled in the config.

    Args:
        config: Git configuration

    Returns:
        True if rerere.autoupdate is enabled
    """
    try:
        autoupdate = config.get((b"rerere",), b"autoupdate")
        if autoupdate is None:
            return False
        if isinstance(autoupdate, bytes):
            return autoupdate.lower() in (b"true", b"1", b"yes", b"on")
        return bool(autoupdate)
    except KeyError:
        return False


def rerere_auto(
    repo: "Repo",
    working_tree_path: bytes | str,
    conflicts: list[bytes],
) -> tuple[list[tuple[bytes, str]], list[bytes]]:
    """Automatically record conflicts and apply known resolutions.

    This is the main entry point for rerere integration with merge operations.
    It should be called after a merge that resulted in conflicts.

    Args:
        repo: Repository object
        working_tree_path: Path to the working tree
        conflicts: List of conflicted file paths

    Returns:
        Tuple of:
        - List of tuples (path, conflict_id) for recorded conflicts
        - List of paths where resolutions were automatically applied
    """
    config = repo.get_config_stack()
    if not is_rerere_enabled(config):
        return [], []

    cache = RerereCache.from_repo(repo)
    recorded = []
    resolved = []

    if isinstance(working_tree_path, bytes):
        working_tree_path = os.fsdecode(working_tree_path)

    autoupdate = is_rerere_autoupdate(config)

    # Record conflicts from the working tree and apply known resolutions
    for path in conflicts:
        # Read the file from the working tree
        file_path = os.path.join(working_tree_path, os.fsdecode(path))

        try:
            with open(file_path, "rb") as f:
                content = f.read()
        except FileNotFoundError:
            # File was deleted in conflict
            continue

        # Record the conflict
        conflict_id = cache.record_conflict(path, content)
        if not conflict_id:
            continue

        recorded.append((path, conflict_id))

        # Check if we have a resolution for this conflict
        if autoupdate and cache.has_resolution(conflict_id):
            resolution = cache.get_resolution(conflict_id)
            if resolution is not None:
                # Apply the resolution to the working tree
                with open(file_path, "wb") as f:
                    f.write(resolution)
                resolved.append(path)

    return recorded, resolved
