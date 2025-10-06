# bundle.py -- Bundle format support
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Bundle format support."""

from collections.abc import Iterator, Sequence
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Callable,
    Optional,
    Protocol,
    cast,
    runtime_checkable,
)

from .pack import PackData, UnpackedObject, write_pack_data


@runtime_checkable
class PackDataLike(Protocol):
    """Protocol for objects that behave like PackData."""

    def __len__(self) -> int:
        """Return the number of objects in the pack."""
        ...

    def iter_unpacked(self) -> Iterator[UnpackedObject]:
        """Iterate over unpacked objects in the pack."""
        ...


if TYPE_CHECKING:
    from .object_store import BaseObjectStore
    from .repo import BaseRepo


class Bundle:
    """Git bundle object representation."""

    version: Optional[int]

    capabilities: dict[str, Optional[str]]
    prerequisites: list[tuple[bytes, bytes]]
    references: dict[bytes, bytes]
    pack_data: Optional[PackDataLike]

    def __repr__(self) -> str:
        """Return string representation of Bundle."""
        return (
            f"<{type(self).__name__}(version={self.version}, "
            f"capabilities={self.capabilities}, "
            f"prerequisites={self.prerequisites}, "
            f"references={self.references})>"
        )

    def __eq__(self, other: object) -> bool:
        """Check equality with another Bundle."""
        if not isinstance(other, type(self)):
            return False
        if self.version != other.version:
            return False
        if self.capabilities != other.capabilities:
            return False
        if self.prerequisites != other.prerequisites:
            return False
        if self.references != other.references:
            return False
        if self.pack_data != other.pack_data:
            return False
        return True

    def store_objects(
        self,
        object_store: "BaseObjectStore",
        progress: Optional[Callable[[str], None]] = None,
    ) -> None:
        """Store all objects from this bundle into an object store.

        Args:
            object_store: The object store to add objects to
            progress: Optional progress callback function
        """
        from .objects import ShaFile

        if self.pack_data is None:
            raise ValueError("pack_data is not loaded")
        count = 0
        for unpacked in self.pack_data.iter_unpacked():
            # Convert the unpacked object to a proper git object
            if unpacked.decomp_chunks and unpacked.obj_type_num is not None:
                git_obj = ShaFile.from_raw_chunks(
                    unpacked.obj_type_num, unpacked.decomp_chunks
                )
                object_store.add_object(git_obj)
                count += 1

                if progress and count % 100 == 0:
                    progress(f"Stored {count} objects")

        if progress:
            progress(f"Stored {count} objects total")


def _read_bundle(f: BinaryIO, version: int) -> Bundle:
    capabilities = {}
    prerequisites = []
    references = {}
    line = f.readline()
    if version >= 3:
        while line.startswith(b"@"):
            line = line[1:].rstrip(b"\n")
            try:
                key, value_bytes = line.split(b"=", 1)
                value = value_bytes.decode("utf-8")
            except ValueError:
                key = line
                value = None
            capabilities[key.decode("utf-8")] = value
            line = f.readline()
    while line.startswith(b"-"):
        (obj_id, comment) = line[1:].rstrip(b"\n").split(b" ", 1)
        prerequisites.append((obj_id, comment))
        line = f.readline()
    while line != b"\n":
        (obj_id, ref) = line.rstrip(b"\n").split(b" ", 1)
        references[ref] = obj_id
        line = f.readline()
    # Extract pack data to separate stream since PackData expects
    # the file to start with PACK header at position 0
    pack_bytes = f.read()
    if not pack_bytes:
        raise ValueError("Bundle file contains no pack data")

    from io import BytesIO

    pack_file = BytesIO(pack_bytes)
    pack_data = PackData.from_file(pack_file)
    ret = Bundle()
    ret.references = references
    ret.capabilities = capabilities
    ret.prerequisites = prerequisites
    ret.pack_data = pack_data
    ret.version = version
    return ret


def read_bundle(f: BinaryIO) -> Bundle:
    """Read a bundle file.

    Args:
        f: A seekable binary file-like object. The file must remain open
           for the lifetime of the returned Bundle object.
    """
    if not hasattr(f, "seek"):
        raise ValueError("Bundle file must be seekable")

    firstline = f.readline()
    if firstline == b"# v2 git bundle\n":
        return _read_bundle(f, 2)
    if firstline == b"# v3 git bundle\n":
        return _read_bundle(f, 3)
    raise AssertionError(f"unsupported bundle format header: {firstline!r}")


def write_bundle(f: BinaryIO, bundle: Bundle) -> None:
    """Write a bundle to a file.

    Args:
        f: File-like object to write to
        bundle: Bundle object to write
    """
    version = bundle.version
    if version is None:
        if bundle.capabilities:
            version = 3
        else:
            version = 2
    if version == 2:
        f.write(b"# v2 git bundle\n")
    elif version == 3:
        f.write(b"# v3 git bundle\n")
    else:
        raise AssertionError(f"unknown version {version}")
    if version == 3:
        for key, value in bundle.capabilities.items():
            f.write(b"@" + key.encode("utf-8"))
            if value is not None:
                f.write(b"=" + value.encode("utf-8"))
            f.write(b"\n")
    for obj_id, comment in bundle.prerequisites:
        f.write(b"-" + obj_id + b" " + comment + b"\n")
    for ref, obj_id in bundle.references.items():
        f.write(obj_id + b" " + ref + b"\n")
    f.write(b"\n")
    if bundle.pack_data is None:
        raise ValueError("bundle.pack_data is not loaded")
    write_pack_data(
        cast(Callable[[bytes], None], f.write),
        num_records=len(bundle.pack_data),
        records=bundle.pack_data.iter_unpacked(),
    )


def create_bundle_from_repo(
    repo: "BaseRepo",
    refs: Optional[Sequence[bytes]] = None,
    prerequisites: Optional[Sequence[bytes]] = None,
    version: Optional[int] = None,
    capabilities: Optional[dict[str, Optional[str]]] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> Bundle:
    """Create a bundle from a repository.

    Args:
        repo: Repository object to create bundle from
        refs: List of refs to include (defaults to all refs)
        prerequisites: List of commit SHAs that are prerequisites
        version: Bundle version (2 or 3, auto-detected if None)
        capabilities: Bundle capabilities (for v3 bundles)
        progress: Optional progress reporting function

    Returns:
        Bundle object ready for writing
    """
    if refs is None:
        refs = list(repo.refs.keys())

    if prerequisites is None:
        prerequisites = []

    if capabilities is None:
        capabilities = {}

    # Build the references dictionary for the bundle
    bundle_refs = {}
    want_objects = set()

    for ref in refs:
        if ref in repo.refs:
            ref_value = repo.refs[ref]
            # Handle peeled refs
            try:
                peeled_value = repo.refs.get_peeled(ref)
                if peeled_value is not None and peeled_value != ref_value:
                    bundle_refs[ref] = peeled_value
                else:
                    bundle_refs[ref] = ref_value
            except KeyError:
                bundle_refs[ref] = ref_value
            want_objects.add(bundle_refs[ref])

    # Convert prerequisites to proper format
    bundle_prerequisites = []
    have_objects = set()
    for prereq in prerequisites:
        if isinstance(prereq, str):
            prereq = prereq.encode("utf-8")
        if isinstance(prereq, bytes):
            if len(prereq) == 40:  # SHA1 hex string
                try:
                    # Validate it's actually hex
                    bytes.fromhex(prereq.decode("utf-8"))
                    # Store hex in bundle and for pack generation
                    bundle_prerequisites.append((prereq, b""))
                    have_objects.add(prereq)
                except ValueError:
                    # Not a valid hex string, invalid prerequisite
                    raise ValueError(f"Invalid prerequisite format: {prereq!r}")
            elif len(prereq) == 20:
                # Binary SHA, convert to hex for both bundle and pack generation
                hex_prereq = prereq.hex().encode("ascii")
                bundle_prerequisites.append((hex_prereq, b""))
                have_objects.add(hex_prereq)
            else:
                # Invalid length
                raise ValueError(f"Invalid prerequisite SHA length: {len(prereq)}")
        else:
            # Assume it's already a binary SHA
            hex_prereq = prereq.hex().encode("ascii")
            bundle_prerequisites.append((hex_prereq, b""))
            have_objects.append(hex_prereq)

    # Generate pack data containing all objects needed for the refs
    pack_count, pack_objects = repo.generate_pack_data(
        have=have_objects,
        want=want_objects,
        progress=progress,
    )

    # Store the pack objects directly, we'll write them when saving the bundle
    # For now, create a simple wrapper to hold the data
    class _BundlePackData:
        def __init__(self, count: int, objects: Iterator[UnpackedObject]) -> None:
            self._count = count
            self._objects = list(objects)  # Materialize the iterator

        def __len__(self) -> int:
            return self._count

        def iter_unpacked(self) -> Iterator[UnpackedObject]:
            return iter(self._objects)

    pack_data = _BundlePackData(pack_count, pack_objects)

    # Create bundle object
    bundle = Bundle()
    bundle.version = version
    bundle.capabilities = capabilities
    bundle.prerequisites = bundle_prerequisites
    bundle.references = bundle_refs
    bundle.pack_data = pack_data

    return bundle
