# line_ending.py -- Line ending conversion functions
# Copyright (C) 2018-2018 Boris Feld <boris.feld@comet.ml>
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
r"""All line-ending related functions, from conversions to config processing.

Line-ending normalization is a complex beast. Here is some notes and details
about how it seems to work.

The normalization is a two-fold process that happens at two moments:

- When reading a file from the index and to the working directory. For example
  when doing a ``git clone`` or ``git checkout`` call. This is called the
  smudge filter (repository -> working tree).
- When writing a file to the index from the working directory. For example
  when doing a ``git add`` call. This is called the clean filter (working tree
  -> repository).

Note that when checking status (getting unstaged changes), whether or not
normalization is done on write depends on whether or not the file in the
working dir has also been normalized on read:

- For autocrlf=true all files are always normalized on both read and write.
- For autocrlf=input files are only normalized on write if they are newly
  "added". Since files which are already committed are not normalized on
  checkout into the working tree, they are also left alone when staging
  modifications into the index.

One thing to know is that Git does line-ending normalization only on text
files. How does Git know that a file is text? We can either mark a file as a
text file, a binary file or ask Git to automatically decides. Git has an
heuristic to detect if a file is a text file or a binary file. It seems based
on the percentage of non-printable characters in files.

The code for this heuristic is here:
https://git.kernel.org/pub/scm/git/git.git/tree/convert.c#n46

Dulwich have an implementation with a slightly different heuristic, the
`dulwich.patch.is_binary` function.

The binary detection heuristic implementation is close to the one in JGit:
https://github.com/eclipse/jgit/blob/f6873ffe522bbc3536969a3a3546bf9a819b92bf/org.eclipse.jgit/src/org/eclipse/jgit/diff/RawText.java#L300

There is multiple variables that impact the normalization.

First, a repository can contains a ``.gitattributes`` file (or more than one...)
that can further customize the operation on some file patterns, for example:

    \*.txt text

Force all ``.txt`` files to be treated as text files and to have their lines
endings normalized.

    \*.jpg -text

Force all ``.jpg`` files to be treated as binary files and to not have their
lines endings converted.

    \*.vcproj text eol=crlf

Force all ``.vcproj`` files to be treated as text files and to have their lines
endings converted into ``CRLF`` in working directory no matter the native EOL of
the platform.

    \*.sh text eol=lf

Force all ``.sh`` files to be treated as text files and to have their lines
endings converted into ``LF`` in working directory no matter the native EOL of
the platform.

If the ``eol`` attribute is not defined, Git uses the ``core.eol`` configuration
value described later.

    \* text=auto

Force all files to be scanned by the text file heuristic detection and to have
their line endings normalized in case they are detected as text files.

Git also have a obsolete attribute named ``crlf`` that can be translated to the
corresponding text attribute value.

Then there are some configuration option (that can be defined at the
repository or user level):

- core.autocrlf
- core.eol

``core.autocrlf`` is taken into account for all files that doesn't have a ``text``
attribute defined in ``.gitattributes``; it takes three possible values:

    - ``true``: This forces all files on the working directory to have CRLF
      line-endings in the working directory and convert line-endings to LF
      when writing to the index. When autocrlf is set to true, eol value is
      ignored.
    - ``input``: Quite similar to the ``true`` value but only applies the clean
      filter, ie line-ending of new files added to the index will get their
      line-endings converted to LF.
    - ``false`` (default): No normalization is done.

``core.eol`` is the top-level configuration to define the line-ending to use
when applying the smudge filter. It takes three possible values:

    - ``lf``: When normalization is done, force line-endings to be ``LF`` in the
      working directory.
    - ``crlf``: When normalization is done, force line-endings to be ``CRLF`` in
      the working directory.
    - ``native`` (default): When normalization is done, force line-endings to be
      the platform's native line ending.

One thing to remember is when line-ending normalization is done on a file, Git
always normalize line-ending to ``LF`` when writing to the index.

There are sources that seems to indicate that Git won't do line-ending
normalization when a file contains mixed line-endings. I think this logic
might be in text / binary detection heuristic but couldn't find it yet.

Sources:
- https://git-scm.com/docs/git-config#git-config-coreeol
- https://git-scm.com/docs/git-config#git-config-coreautocrlf
- https://git-scm.com/docs/gitattributes#_checking_out_and_checking_in
- https://adaptivepatchwork.com/2012/03/01/mind-the-end-of-your-line/
"""

__all__ = [
    "CRLF",
    "LF",
    "BlobNormalizer",
    "LineEndingFilter",
    "TreeBlobNormalizer",
    "check_safecrlf",
    "convert_crlf_to_lf",
    "convert_lf_to_crlf",
    "get_checkin_filter",
    "get_checkin_filter_autocrlf",
    "get_checkout_filter",
    "get_checkout_filter_autocrlf",
    "get_clean_filter",
    "get_clean_filter_autocrlf",
    "get_smudge_filter",
    "get_smudge_filter_autocrlf",
    "normalize_blob",
]

import logging
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .config import StackedConfig
    from .object_store import BaseObjectStore

from . import replace_me
from .attrs import GitAttributes, Pattern
from .filters import FilterBlobNormalizer, FilterContext, FilterDriver, FilterRegistry
from .object_store import iter_tree_contents
from .objects import Blob, ObjectID
from .patch import is_binary

CRLF = b"\r\n"
LF = b"\n"

logger = logging.getLogger(__name__)


class LineEndingFilter(FilterDriver):
    """Filter driver for line ending conversion."""

    def __init__(
        self,
        clean_conversion: Callable[[bytes], bytes] | None = None,
        smudge_conversion: Callable[[bytes], bytes] | None = None,
        binary_detection: bool = True,
        safecrlf: bytes = b"false",
    ):
        """Initialize LineEndingFilter."""
        self.clean_conversion = clean_conversion
        self.smudge_conversion = smudge_conversion
        self.binary_detection = binary_detection
        self.safecrlf = safecrlf

    @classmethod
    def from_config(
        cls, config: "StackedConfig | None", for_text_attr: bool = False
    ) -> "LineEndingFilter":
        """Create a LineEndingFilter from git configuration.

        Args:
            config: Git configuration stack
            for_text_attr: If True, always normalize on checkin (for text attribute)

        Returns:
            Configured LineEndingFilter instance
        """
        if config is None:
            # Default filter
            if for_text_attr:
                # For text attribute: always normalize on checkin
                return cls(
                    clean_conversion=convert_crlf_to_lf,
                    smudge_conversion=None,
                    binary_detection=True,
                )
            else:
                # No config: no conversion
                return cls()

        # Get core.eol setting
        try:
            core_eol_raw = config.get("core", "eol")
            core_eol: str = (
                core_eol_raw.decode("ascii")
                if isinstance(core_eol_raw, bytes)
                else str(core_eol_raw)
            )
        except KeyError:
            core_eol = "native"

        # Get core.autocrlf setting
        try:
            autocrlf_raw = config.get("core", "autocrlf")
            autocrlf: bytes = (
                autocrlf_raw.lower()
                if isinstance(autocrlf_raw, bytes)
                else str(autocrlf_raw).lower().encode("ascii")
            )
        except KeyError:
            autocrlf = b"false"

        # Get core.safecrlf setting
        try:
            safecrlf_raw = config.get("core", "safecrlf")
            safecrlf = (
                safecrlf_raw
                if isinstance(safecrlf_raw, bytes)
                else safecrlf_raw.encode("utf-8")
            )
        except KeyError:
            safecrlf = b"false"

        if for_text_attr:
            # For text attribute: always normalize to LF on checkin
            # Smudge behavior depends on core.eol and core.autocrlf
            smudge_filter = get_smudge_filter(core_eol, autocrlf)
            clean_filter: Callable[[bytes], bytes] | None = convert_crlf_to_lf
        else:
            # Normal autocrlf behavior
            smudge_filter = get_smudge_filter(core_eol, autocrlf)
            clean_filter = get_clean_filter(core_eol, autocrlf)

        return cls(
            clean_conversion=clean_filter,
            smudge_conversion=smudge_filter,
            binary_detection=True,
            safecrlf=safecrlf,
        )

    def clean(self, data: bytes, path: bytes = b"") -> bytes:
        """Apply line ending conversion for checkin (working tree -> repository)."""
        if self.clean_conversion is None:
            return data

        # Skip binary files if detection is enabled
        if self.binary_detection and is_binary(data):
            return data

        converted = self.clean_conversion(data)

        # Check if conversion is safe
        if self.safecrlf != b"false":
            check_safecrlf(data, converted, self.safecrlf, path)

        return converted

    def smudge(self, data: bytes, path: bytes = b"") -> bytes:
        """Apply line ending conversion for checkout (repository -> working tree)."""
        if self.smudge_conversion is None:
            return data

        # Skip binary files if detection is enabled
        if self.binary_detection and is_binary(data):
            return data

        converted = self.smudge_conversion(data)

        # Check if conversion is safe
        if self.safecrlf != b"false":
            check_safecrlf(data, converted, self.safecrlf, path)

        return converted

    def cleanup(self) -> None:
        """Clean up any resources held by this filter driver."""
        # LineEndingFilter doesn't hold any resources that need cleanup

    def reuse(self, config: "StackedConfig", filter_name: str) -> bool:
        """Check if this filter driver should be reused with the given configuration."""
        # LineEndingFilter is lightweight and should always be recreated
        # to ensure it uses the latest configuration
        return False


def convert_crlf_to_lf(text_hunk: bytes) -> bytes:
    """Convert CRLF in text hunk into LF.

    Args:
      text_hunk: A bytes string representing a text hunk
    Returns: The text hunk with the same type, with CRLF replaced into LF
    """
    return text_hunk.replace(CRLF, LF)


def convert_lf_to_crlf(text_hunk: bytes) -> bytes:
    """Convert LF in text hunk into CRLF.

    Args:
      text_hunk: A bytes string representing a text hunk
    Returns: The text hunk with the same type, with LF replaced into CRLF
    """
    # Single-pass conversion: split on LF and join with CRLF
    # This avoids the double replacement issue
    parts = text_hunk.split(LF)
    # Remove any trailing CR to avoid CRCRLF
    cleaned_parts = []
    for i, part in enumerate(parts):
        if i < len(parts) - 1 and part.endswith(b"\r"):
            cleaned_parts.append(part[:-1])
        else:
            cleaned_parts.append(part)
    return CRLF.join(cleaned_parts)


def check_safecrlf(
    original: bytes, converted: bytes, safecrlf: bytes, path: bytes = b""
) -> None:
    """Check if CRLF conversion is safe according to core.safecrlf setting.

    Args:
        original: Original content before conversion
        converted: Content after conversion
        safecrlf: Value of core.safecrlf config (b"true", b"warn", or b"false")
        path: Path to the file being checked (for error messages)

    Raises:
        ValueError: If safecrlf is "true" and conversion would lose data
    """
    if safecrlf == b"false":
        return

    # Check if conversion is reversible
    if safecrlf in (b"true", b"warn"):
        # For CRLF->LF conversion, check if converting back would recover original
        if CRLF in original and CRLF not in converted:
            # This was a CRLF->LF conversion
            recovered = convert_lf_to_crlf(converted)
            if recovered != original:
                msg = (
                    f"CRLF would be replaced by LF in {path.decode('utf-8', 'replace')}"
                )
                if safecrlf == b"true":
                    raise ValueError(msg)
                else:  # warn
                    logger.warning(msg)

        # For LF->CRLF conversion, check if converting back would recover original
        elif LF in original and CRLF in converted and CRLF not in original:
            # This was a LF->CRLF conversion
            recovered = convert_crlf_to_lf(converted)
            if recovered != original:
                msg = (
                    f"LF would be replaced by CRLF in {path.decode('utf-8', 'replace')}"
                )
                if safecrlf == b"true":
                    raise ValueError(msg)
                else:  # warn
                    logger.warning(msg)


def get_smudge_filter(
    core_eol: str, core_autocrlf: bytes
) -> Callable[[bytes], bytes] | None:
    """Returns the correct smudge filter based on the passed arguments."""
    # Git attributes handling is done by the filter infrastructure
    return get_smudge_filter_autocrlf(core_autocrlf)


def get_clean_filter(
    core_eol: str, core_autocrlf: bytes
) -> Callable[[bytes], bytes] | None:
    """Returns the correct clean filter based on the passed arguments."""
    # Git attributes handling is done by the filter infrastructure
    return get_clean_filter_autocrlf(core_autocrlf)


def get_smudge_filter_autocrlf(
    core_autocrlf: bytes,
) -> Callable[[bytes], bytes] | None:
    """Returns the correct smudge filter base on autocrlf value.

    Args:
      core_autocrlf: The bytes configuration value of core.autocrlf.
        Valid values are: b'true', b'false' or b'input'.
    Returns: Either None if no filter has to be applied or a function
        accepting a single argument, a binary text hunk
    """
    if core_autocrlf == b"true":
        return convert_lf_to_crlf

    return None


def get_clean_filter_autocrlf(
    core_autocrlf: bytes,
) -> Callable[[bytes], bytes] | None:
    """Returns the correct clean filter base on autocrlf value.

    Args:
      core_autocrlf: The bytes configuration value of core.autocrlf.
        Valid values are: b'true', b'false' or b'input'.
    Returns: Either None if no filter has to be applied or a function
        accepting a single argument, a binary text hunk
    """
    if core_autocrlf == b"true" or core_autocrlf == b"input":
        return convert_crlf_to_lf

    # Checking filter should never be `convert_lf_to_crlf`
    return None


# Backwards compatibility wrappers
@replace_me(since="0.23.1", remove_in="0.25.0")
def get_checkout_filter(
    core_eol: str, core_autocrlf: bool | str, git_attributes: Mapping[str, Any]
) -> Callable[[bytes], bytes] | None:
    """Deprecated: Use get_smudge_filter instead."""
    # Convert core_autocrlf to bytes for compatibility
    if isinstance(core_autocrlf, bool):
        autocrlf_bytes = b"true" if core_autocrlf else b"false"
    else:
        autocrlf_bytes = (
            core_autocrlf.encode("utf-8")
            if isinstance(core_autocrlf, str)
            else core_autocrlf
        )
    return get_smudge_filter(core_eol, autocrlf_bytes)


@replace_me(since="0.23.1", remove_in="0.25.0")
def get_checkin_filter(
    core_eol: str, core_autocrlf: bool | str, git_attributes: Mapping[str, Any]
) -> Callable[[bytes], bytes] | None:
    """Deprecated: Use get_clean_filter instead."""
    # Convert core_autocrlf to bytes for compatibility
    if isinstance(core_autocrlf, bool):
        autocrlf_bytes = b"true" if core_autocrlf else b"false"
    else:
        autocrlf_bytes = (
            core_autocrlf.encode("utf-8")
            if isinstance(core_autocrlf, str)
            else core_autocrlf
        )
    return get_clean_filter(core_eol, autocrlf_bytes)


@replace_me(since="0.23.1", remove_in="0.25.0")
def get_checkout_filter_autocrlf(
    core_autocrlf: bytes,
) -> Callable[[bytes], bytes] | None:
    """Deprecated: Use get_smudge_filter_autocrlf instead."""
    return get_smudge_filter_autocrlf(core_autocrlf)


@replace_me(since="0.23.1", remove_in="0.25.0")
def get_checkin_filter_autocrlf(
    core_autocrlf: bytes,
) -> Callable[[bytes], bytes] | None:
    """Deprecated: Use get_clean_filter_autocrlf instead."""
    return get_clean_filter_autocrlf(core_autocrlf)


class BlobNormalizer(FilterBlobNormalizer):
    """An object to store computation result of which filter to apply based on configuration, gitattributes, path and operation (checkin or checkout).

    This class maintains backward compatibility while using the filter infrastructure.
    """

    def __init__(
        self,
        config_stack: "StackedConfig",
        gitattributes: Mapping[str, Any],
        core_eol: str = "native",
        autocrlf: bytes = b"false",
        safecrlf: bytes = b"false",
    ) -> None:
        """Initialize FilteringBlobNormalizer."""
        # Set up a filter registry with line ending filters
        filter_registry = FilterRegistry(config_stack)

        # Create line ending filter if needed
        smudge_filter = get_smudge_filter(core_eol, autocrlf)
        clean_filter = get_clean_filter(core_eol, autocrlf)

        # Always register a text filter that can be used by gitattributes
        # Even if autocrlf is false, gitattributes text=true should work
        line_ending_filter = LineEndingFilter(
            clean_conversion=clean_filter or convert_crlf_to_lf,
            smudge_conversion=smudge_filter or convert_lf_to_crlf,
            binary_detection=True,
            safecrlf=safecrlf,
        )
        filter_registry.register_driver("text", line_ending_filter)

        # Convert dict gitattributes to GitAttributes object for parent class
        git_attrs_patterns = []
        for pattern_str, attrs in gitattributes.items():
            if isinstance(pattern_str, str):
                pattern_bytes = pattern_str.encode("utf-8")
            else:
                pattern_bytes = pattern_str
            pattern = Pattern(pattern_bytes)
            git_attrs_patterns.append((pattern, attrs))

        git_attributes = GitAttributes(git_attrs_patterns)

        # Create FilterContext for parent class
        filter_context = FilterContext(filter_registry)

        # Initialize parent class with gitattributes
        # The filter infrastructure will handle gitattributes processing
        super().__init__(config_stack, git_attributes, filter_context=filter_context)

        # Store original filters for backward compatibility
        self.fallback_read_filter = smudge_filter
        self.fallback_write_filter = clean_filter

    def checkin_normalize(self, blob: Blob, tree_path: bytes) -> Blob:
        """Normalize a blob during a checkin operation."""
        # First try to get filter from gitattributes (handled by parent)
        result = super().checkin_normalize(blob, tree_path)

        # Check if gitattributes explicitly disabled text conversion
        attrs = self.gitattributes.match_path(tree_path)
        if b"text" in attrs and attrs[b"text"] is False:
            # Explicitly marked as binary, no conversion
            return blob

        # If no filter was applied via gitattributes and we have a fallback filter
        # (autocrlf is enabled), apply it to all files
        if result is blob and self.fallback_write_filter is not None:
            # Apply the clean filter with binary detection
            # Get safecrlf from config
            safecrlf = b"false"
            if hasattr(self, "filter_registry") and hasattr(
                self.filter_registry, "config_stack"
            ):
                safecrlf = self.filter_registry.config_stack.get(
                    b"core", b"safecrlf", b"false"
                )
                if hasattr(safecrlf, "encode"):
                    safecrlf = safecrlf.encode("utf-8")

            line_ending_filter = LineEndingFilter(
                clean_conversion=self.fallback_write_filter,
                smudge_conversion=None,
                binary_detection=True,
                safecrlf=safecrlf,
            )
            filtered_data = line_ending_filter.clean(blob.data, tree_path)
            if filtered_data != blob.data:
                new_blob = Blob()
                new_blob.data = filtered_data
                return new_blob

        return result

    def checkout_normalize(self, blob: Blob, tree_path: bytes) -> Blob:
        """Normalize a blob during a checkout operation."""
        # First try to get filter from gitattributes (handled by parent)
        result = super().checkout_normalize(blob, tree_path)

        # Check if gitattributes explicitly disabled text conversion
        attrs = self.gitattributes.match_path(tree_path)
        if b"text" in attrs and attrs[b"text"] is False:
            # Explicitly marked as binary, no conversion
            return blob

        # If no filter was applied via gitattributes and we have a fallback filter
        # (autocrlf is enabled), apply it to all files
        if result is blob and self.fallback_read_filter is not None:
            # Apply the smudge filter with binary detection
            # Get safecrlf from config
            safecrlf = b"false"
            if hasattr(self, "filter_registry") and hasattr(
                self.filter_registry, "config_stack"
            ):
                safecrlf = self.filter_registry.config_stack.get(
                    b"core", b"safecrlf", b"false"
                )
                if hasattr(safecrlf, "encode"):
                    safecrlf = safecrlf.encode("utf-8")

            line_ending_filter = LineEndingFilter(
                clean_conversion=None,
                smudge_conversion=self.fallback_read_filter,
                binary_detection=True,
                safecrlf=safecrlf,
            )
            filtered_data = line_ending_filter.smudge(blob.data, tree_path)
            if filtered_data != blob.data:
                new_blob = Blob()
                new_blob.data = filtered_data
                return new_blob

        return result


def normalize_blob(
    blob: Blob, conversion: Callable[[bytes], bytes], binary_detection: bool
) -> Blob:
    """Normalize blob by applying line ending conversion."""
    # Read the original blob
    data = blob.data

    # If we need to detect if a file is binary and the file is detected as
    # binary, do not apply the conversion function and return the original
    # chunked text
    if binary_detection is True:
        if is_binary(data):
            return blob

    # Now apply the conversion
    converted_data = conversion(data)

    new_blob = Blob()
    new_blob.data = converted_data

    return new_blob


class TreeBlobNormalizer(BlobNormalizer):
    """Blob normalizer that tracks existing files in a tree."""

    def __init__(
        self,
        config_stack: "StackedConfig",
        git_attributes: Mapping[str, Any],
        object_store: "BaseObjectStore",
        tree: ObjectID | None = None,
        core_eol: str = "native",
        autocrlf: bytes = b"false",
        safecrlf: bytes = b"false",
    ) -> None:
        """Initialize TreeBlobNormalizer."""
        super().__init__(config_stack, git_attributes, core_eol, autocrlf, safecrlf)
        if tree:
            self.existing_paths = {
                name for name, _, _ in iter_tree_contents(object_store, tree)
            }
        else:
            self.existing_paths = set()

    def checkin_normalize(self, blob: Blob, tree_path: bytes) -> Blob:
        """Normalize blob for checkin, considering existing tree state."""
        # Existing files should only be normalized on checkin if:
        # 1. They were previously normalized on checkout (autocrlf=true), OR
        # 2. We have a write filter (autocrlf=true or autocrlf=input), OR
        # 3. They are new files
        if (
            self.fallback_read_filter is not None
            or self.fallback_write_filter is not None
            or tree_path not in self.existing_paths
        ):
            return super().checkin_normalize(blob, tree_path)
        return blob
