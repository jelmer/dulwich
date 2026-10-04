# test_porcelain_lfs.py -- Tests for LFS porcelain functions
# Copyright (C) 2024 Jelmer Vernooij
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

"""Tests for LFS porcelain functions."""

import os
import shutil
import sys
import tempfile
import threading
import unittest

from dulwich import porcelain
from dulwich.lfs import LFSError, LFSPointer, LFSStore
from dulwich.lfs_server import run_lfs_server
from dulwich.objects import Blob, Tree
from dulwich.repo import Repo
from tests import TestCase


class LFSPorcelainTestCase(TestCase):
    """Test case for LFS porcelain functions."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup_test_dir)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)

    def _cleanup_test_dir(self):
        """Clean up test directory recursively."""
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_lfs_init(self):
        """Test LFS initialization."""
        porcelain.lfs_init(self.repo)

        # Check that LFS store was created
        lfs_dir = os.path.join(self.repo.controldir(), "lfs")
        self.assertTrue(os.path.exists(lfs_dir))
        self.assertTrue(os.path.exists(os.path.join(lfs_dir, "objects")))
        self.assertTrue(os.path.exists(os.path.join(lfs_dir, "tmp")))

        # Check that config was set
        config = self.repo.get_config()
        self.assertEqual(
            config.get((b"filter", b"lfs"), b"process"), b"git-lfs filter-process"
        )
        self.assertEqual(config.get((b"filter", b"lfs"), b"required"), b"true")

    def test_lfs_track(self):
        """Test tracking patterns with LFS."""
        # Track some patterns
        patterns = ["*.bin", "*.pdf"]
        tracked = porcelain.lfs_track(self.repo, patterns)

        self.assertEqual(set(tracked), set(patterns))

        # Check .gitattributes was created
        gitattributes_path = os.path.join(self.repo.path, ".gitattributes")
        self.assertTrue(os.path.exists(gitattributes_path))

        # Read and verify content
        with open(gitattributes_path, "rb") as f:
            content = f.read()

        self.assertIn(b"*.bin diff=lfs filter=lfs merge=lfs -text", content)
        self.assertIn(b"*.pdf diff=lfs filter=lfs merge=lfs -text", content)

        # Test listing tracked patterns
        tracked = porcelain.lfs_track(self.repo)
        self.assertEqual(set(tracked), set(patterns))

    def test_lfs_untrack(self):
        """Test untracking patterns from LFS."""
        # First track some patterns
        patterns = ["*.bin", "*.pdf", "*.zip"]
        porcelain.lfs_track(self.repo, patterns)

        # Untrack one pattern
        remaining = porcelain.lfs_untrack(self.repo, ["*.pdf"])
        self.assertEqual(set(remaining), {"*.bin", "*.zip"})

        # Verify .gitattributes
        with open(os.path.join(self.repo.path, ".gitattributes"), "rb") as f:
            content = f.read()

        self.assertIn(b"*.bin diff=lfs filter=lfs merge=lfs -text", content)
        self.assertNotIn(b"*.pdf diff=lfs filter=lfs merge=lfs -text", content)
        self.assertIn(b"*.zip diff=lfs filter=lfs merge=lfs -text", content)

    def test_lfs_clean(self):
        """Test cleaning a file to LFS pointer."""
        # Initialize LFS
        porcelain.lfs_init(self.repo)

        # Create a test file
        test_content = b"This is test content for LFS"
        test_file = os.path.join(self.repo.path, "test.bin")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Clean the file
        pointer_content = porcelain.lfs_clean(self.repo, "test.bin")

        # Verify it's a valid LFS pointer
        pointer = LFSPointer.from_bytes(pointer_content)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, len(test_content))

        # Verify the content was stored in LFS
        lfs_store = LFSStore.from_repo(self.repo)
        with lfs_store.open_object(pointer.oid) as f:
            stored_content = f.read()
        self.assertEqual(stored_content, test_content)

    def test_lfs_smudge(self):
        """Test smudging an LFS pointer to content."""
        # Initialize LFS
        porcelain.lfs_init(self.repo)

        # Create test content and store it
        test_content = b"This is test content for smudging"
        lfs_store = LFSStore.from_repo(self.repo)
        oid = lfs_store.write_object([test_content])

        # Create LFS pointer
        pointer = LFSPointer(oid, len(test_content))
        pointer_content = pointer.to_bytes()

        # Smudge the pointer
        smudged_content = porcelain.lfs_smudge(self.repo, pointer_content)

        self.assertEqual(smudged_content, test_content)

    def test_lfs_smudge_missing_object(self):
        """Test smudging a pointer to an object that can not be found."""
        porcelain.lfs_init(self.repo)
        pointer_content = LFSPointer("0" * 64, 5).to_bytes()

        with self.assertRaises(LFSError) as cm:
            porcelain.lfs_smudge(self.repo, pointer_content)
        self.assertEqual(
            "No LFS client available from configuration", str(cm.exception)
        )

    def test_lfs_smudge_non_pointer(self):
        """Test that content that is not a pointer is passed through."""
        porcelain.lfs_init(self.repo)
        self.assertEqual(b"content", porcelain.lfs_smudge(self.repo, b"content"))

    def test_lfs_ls_files(self):
        """Test listing LFS files."""
        # Initialize repo with some LFS files
        porcelain.lfs_init(self.repo)

        # Create a test file and convert to LFS
        test_content = b"Large file content"
        test_file = os.path.join(self.repo.path, "large.bin")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Clean to LFS pointer
        pointer_content = porcelain.lfs_clean(self.repo, "large.bin")
        with open(test_file, "wb") as f:
            f.write(pointer_content)

        # Add and commit
        porcelain.add(self.repo, paths=["large.bin"])
        porcelain.commit(self.repo, message=b"Add LFS file")

        # List LFS files
        lfs_files = porcelain.lfs_ls_files(self.repo)

        self.assertEqual(len(lfs_files), 1)
        path, _oid, size = lfs_files[0]
        self.assertEqual(path, b"large.bin")
        self.assertEqual(size, len(test_content))

    def test_lfs_migrate(self):
        """Test migrating files to LFS."""
        # Create some files
        files = {
            "small.txt": b"Small file",
            "large1.bin": b"X" * 1000,
            "large2.dat": b"Y" * 2000,
            "exclude.bin": b"Z" * 1500,
        }

        for filename, content in files.items():
            path = os.path.join(self.repo.path, filename)
            with open(path, "wb") as f:
                f.write(content)

        # Add files to index
        porcelain.add(self.repo, paths=list(files.keys()))

        # Migrate with patterns
        count = porcelain.lfs_migrate(
            self.repo, include=["*.bin", "*.dat"], exclude=["exclude.*"]
        )

        self.assertEqual(count, 2)  # large1.bin and large2.dat

        # Verify files were converted to LFS pointers
        for filename in ["large1.bin", "large2.dat"]:
            path = os.path.join(self.repo.path, filename)
            with open(path, "rb") as f:
                content = f.read()
            pointer = LFSPointer.from_bytes(content)
            self.assertIsNotNone(pointer)

    @unittest.skipIf(sys.platform == "win32", "Requires symlink support")
    def test_lfs_migrate_does_not_follow_symlink(self):
        outside = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, outside)
        target = os.path.join(outside, "secret.bin")
        with open(target, "wb") as f:
            f.write(b"secret")
        os.symlink(target, os.path.join(self.repo.path, "link.bin"))
        porcelain.add(self.repo, paths=["link.bin"])

        count = porcelain.lfs_migrate(self.repo, include=["*.bin"])

        self.assertEqual(0, count)
        with open(target, "rb") as f:
            self.assertEqual(b"secret", f.read())

    @unittest.skipIf(sys.platform == "win32", "Requires symlink support")
    def test_lfs_migrate_rejects_symlinked_leading_dir(self):
        os.mkdir(os.path.join(self.repo.path, "sub"))
        with open(os.path.join(self.repo.path, "sub", "big.bin"), "wb") as f:
            f.write(b"X" * 100)
        porcelain.add(self.repo, paths=["sub/big.bin"])
        outside = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, outside)
        with open(os.path.join(outside, "big.bin"), "wb") as f:
            f.write(b"secret")
        shutil.rmtree(os.path.join(self.repo.path, "sub"))
        os.symlink(outside, os.path.join(self.repo.path, "sub"))

        with self.assertRaises(porcelain.Error):
            porcelain.lfs_migrate(self.repo, include=["*.bin"])
        with open(os.path.join(outside, "big.bin"), "rb") as f:
            self.assertEqual(b"secret", f.read())

    def test_lfs_pointer_check(self):
        """Test checking if files are LFS pointers."""
        # Initialize LFS
        porcelain.lfs_init(self.repo)

        # Create an LFS pointer file
        test_content = b"LFS content"
        lfs_file = os.path.join(self.repo.path, "lfs.bin")
        # First create the file
        with open(lfs_file, "wb") as f:
            f.write(test_content)
        pointer_content = porcelain.lfs_clean(self.repo, "lfs.bin")
        with open(lfs_file, "wb") as f:
            f.write(pointer_content)

        # Create a regular file
        regular_file = os.path.join(self.repo.path, "regular.txt")
        with open(regular_file, "wb") as f:
            f.write(b"Regular content")

        # Check both files
        results = porcelain.lfs_pointer_check(
            self.repo, paths=["lfs.bin", "regular.txt", "nonexistent.txt"]
        )

        self.assertIsNotNone(results["lfs.bin"])
        self.assertIsNone(results["regular.txt"])
        self.assertIsNone(results["nonexistent.txt"])

    def test_clone_with_builtin_lfs_no_config(self):
        """Test cloning with built-in LFS filter when no git-lfs config exists."""
        # Create a source repo with LFS content
        source_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: self._cleanup_test_dir_path(source_dir))
        source_repo = Repo.init(source_dir)

        # Create .gitattributes
        gitattributes_path = os.path.join(source_dir, ".gitattributes")
        with open(gitattributes_path, "w") as f:
            f.write("*.bin filter=lfs diff=lfs merge=lfs -text\n")

        # Create test content and store in LFS
        # LFSStore.from_repo with create=True will create the directories
        test_content = b"This is test content for LFS"
        lfs_store = LFSStore.from_repo(source_repo, create=True)
        oid = lfs_store.write_object([test_content])

        # Create LFS pointer file
        pointer = LFSPointer(oid, len(test_content))
        test_file = os.path.join(source_dir, "test.bin")
        with open(test_file, "wb") as f:
            f.write(pointer.to_bytes())

        # Add and commit
        porcelain.add(source_repo, paths=[".gitattributes", "test.bin"])
        porcelain.commit(source_repo, message=b"Add LFS file")

        # Clone with empty config (no git-lfs commands)
        clone_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: self._cleanup_test_dir_path(clone_dir))

        # Verify source repo has no LFS filter config
        config = source_repo.get_config()
        with self.assertRaises(KeyError):
            config.get((b"filter", b"lfs"), b"smudge")

        # Clone the repository. For a file:// remote the built-in filter
        # resolves pointers against the source repo's LFS store, so no
        # warning is emitted.
        cloned_repo = porcelain.clone(source_dir, clone_dir)

        # Verify that built-in LFS filter was used
        normalizer = cloned_repo.get_blob_normalizer()
        if hasattr(normalizer, "filter_registry"):
            lfs_driver = normalizer.filter_registry.get_driver("lfs")
            # Should be the built-in LFSFilterDriver
            self.assertEqual(type(lfs_driver).__name__, "LFSFilterDriver")
            self.assertEqual(type(lfs_driver).__module__, "dulwich.lfs")

        # The built-in filter should have fetched the object from the
        # source repo's LFS store during checkout.
        cloned_file = os.path.join(clone_dir, "test.bin")
        with open(cloned_file, "rb") as f:
            content = f.read()
        self.assertEqual(content, test_content)

        source_repo.close()
        cloned_repo.close()

    def test_clone_with_builtin_lfs_no_object_in_source(self):
        """Built-in LFS filter leaves a pointer in the working tree when the
        referenced object is not available in the source repo's LFS store."""
        # Source repo with a pointer but no matching LFS object
        source_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: self._cleanup_test_dir_path(source_dir))
        source_repo = Repo.init(source_dir)

        gitattributes_path = os.path.join(source_dir, ".gitattributes")
        with open(gitattributes_path, "w") as f:
            f.write("*.bin filter=lfs diff=lfs merge=lfs -text\n")

        # Pointer to an object that does not exist anywhere
        missing_pointer = LFSPointer(
            "0" * 64,
            42,
        )
        test_file = os.path.join(source_dir, "test.bin")
        with open(test_file, "wb") as f:
            f.write(missing_pointer.to_bytes())

        porcelain.add(source_repo, paths=[".gitattributes", "test.bin"])
        porcelain.commit(source_repo, message=b"Add unresolvable LFS pointer")

        # Make sure the source has no LFS store at all
        source_lfs = os.path.join(source_dir, ".git", "lfs")
        if os.path.isdir(source_lfs):
            shutil.rmtree(source_lfs)
        source_repo.close()

        clone_dir = tempfile.mkdtemp()
        self.addCleanup(lambda: self._cleanup_test_dir_path(clone_dir))

        # The smudge filter should warn and fall back to leaving the pointer
        # in the working tree.
        with self.assertLogs("dulwich.lfs", level="WARNING"):
            cloned_repo = porcelain.clone(source_dir, clone_dir)

        with open(os.path.join(clone_dir, "test.bin"), "rb") as f:
            content = f.read()
        round_tripped = LFSPointer.from_bytes(content)
        self.assertIsNotNone(round_tripped)
        self.assertEqual(round_tripped.oid, missing_pointer.oid)
        self.assertEqual(round_tripped.size, missing_pointer.size)
        cloned_repo.close()

    def _cleanup_test_dir_path(self, path):
        """Clean up a test directory by path."""
        shutil.rmtree(path, ignore_errors=True)

    def test_add_applies_clean_filter(self):
        """Test that add operation applies LFS clean filter."""
        # Don't use lfs_init to avoid configuring git-lfs commands
        # Create LFS store manually
        lfs_store = LFSStore.from_repo(self.repo, create=True)

        # Create .gitattributes
        gitattributes_path = os.path.join(self.repo.path, ".gitattributes")
        with open(gitattributes_path, "w") as f:
            f.write("*.bin filter=lfs diff=lfs merge=lfs -text\n")

        # Create a file that should be cleaned to LFS
        test_content = b"This is large file content that should be stored in LFS"
        test_file = os.path.join(self.repo.path, "large.bin")
        with open(test_file, "wb") as f:
            f.write(test_content)

        # Add the file - this should apply the clean filter
        porcelain.add(self.repo, paths=["large.bin"])

        # Check that the file was cleaned to a pointer in the index
        index = self.repo.open_index()
        entry = index[b"large.bin"]

        # Get the blob from the object store
        blob = self.repo.get_object(entry.sha)
        content = blob.data

        # Should be an LFS pointer
        self.assertTrue(
            content.startswith(b"version https://git-lfs.github.com/spec/v1")
        )
        pointer = LFSPointer.from_bytes(content)
        self.assertIsNotNone(pointer)
        self.assertEqual(pointer.size, len(test_content))

        # Verify the actual content was stored in LFS
        with lfs_store.open_object(pointer.oid) as f:
            stored_content = f.read()
        self.assertEqual(stored_content, test_content)

    def test_checkout_applies_smudge_filter(self):
        """Test that checkout operation applies LFS smudge filter."""
        # Create LFS store and content
        lfs_store = LFSStore.from_repo(self.repo, create=True)

        # Create .gitattributes
        gitattributes_path = os.path.join(self.repo.path, ".gitattributes")
        with open(gitattributes_path, "w") as f:
            f.write("*.bin filter=lfs diff=lfs merge=lfs -text\n")

        # Create test content and store in LFS
        test_content = b"This is the actual file content from LFS"
        oid = lfs_store.write_object([test_content])

        # Create LFS pointer file
        pointer = LFSPointer(oid, len(test_content))
        test_file = os.path.join(self.repo.path, "data.bin")
        with open(test_file, "wb") as f:
            f.write(pointer.to_bytes())

        # Add and commit the pointer
        porcelain.add(self.repo, paths=[".gitattributes", "data.bin"])
        porcelain.commit(self.repo, message=b"Add LFS file")

        # Remove the file from working directory
        os.remove(test_file)

        # Checkout the file - this should apply the smudge filter
        porcelain.checkout(self.repo, paths=["data.bin"])

        # Verify the file was expanded from pointer to content
        with open(test_file, "rb") as f:
            content = f.read()

        self.assertEqual(content, test_content)

    def test_reset_hard_applies_smudge_filter(self):
        """Test that reset --hard applies LFS smudge filter."""
        # Create LFS store and content
        lfs_store = LFSStore.from_repo(self.repo, create=True)

        # Create .gitattributes
        gitattributes_path = os.path.join(self.repo.path, ".gitattributes")
        with open(gitattributes_path, "w") as f:
            f.write("*.bin filter=lfs diff=lfs merge=lfs -text\n")

        # Create test content and store in LFS
        test_content = b"Content that should be restored by reset"
        oid = lfs_store.write_object([test_content])

        # Create LFS pointer file
        pointer = LFSPointer(oid, len(test_content))
        test_file = os.path.join(self.repo.path, "reset-test.bin")
        with open(test_file, "wb") as f:
            f.write(pointer.to_bytes())

        # Add and commit
        porcelain.add(self.repo, paths=[".gitattributes", "reset-test.bin"])
        commit_sha = porcelain.commit(self.repo, message=b"Add LFS file for reset test")

        # Modify the file in working directory
        with open(test_file, "wb") as f:
            f.write(b"Modified content that should be discarded")

        # Reset hard - this should restore the file with smudge filter applied
        porcelain.reset(self.repo, mode="hard", treeish=commit_sha)

        # Verify the file was restored with LFS content
        with open(test_file, "rb") as f:
            content = f.read()

        self.assertEqual(content, test_content)


class LFSTransferTests(TestCase):
    """Tests for the LFS porcelain functions that talk to a remote."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir, ignore_errors=True)
        self.repo = Repo.init(self.test_dir)
        self.addCleanup(self.repo.close)
        self.local_store = LFSStore.from_repo(self.repo, create=True)

        self.server_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.server_dir)
        self.server, self.server_url = run_lfs_server(port=0, lfs_dir=self.server_dir)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

        def cleanup_server() -> None:
            self.server.shutdown()
            self.server.server_close()
            self.server_thread.join(timeout=1.0)

        self.addCleanup(cleanup_server)

    def _set_config(self, section: tuple[bytes, ...], name: bytes, value: str) -> None:
        config = self.repo.get_config()
        config.set(section, name, value.encode())
        config.write_to_path()

    def _pointer(self, content: bytes, store: LFSStore) -> tuple[str, bytes]:
        """Store content in an LFS store, returning its oid and pointer."""
        oid = store.write_object([content])
        return oid, LFSPointer(oid, len(content)).to_bytes()

    def _commit_pointer(
        self, content: bytes, store: LFSStore, name: str = "large.bin"
    ) -> str:
        """Commit a pointer to content, returning its oid."""
        oid, pointer = self._pointer(content, store)
        with open(os.path.join(self.test_dir, name), "wb") as f:
            f.write(pointer)
        porcelain.add(self.repo, paths=[name])
        porcelain.commit(self.repo, message=b"Add LFS file")
        return oid

    def _tag_tree(self, name: bytes, content: bytes, store: LFSStore) -> str:
        """Create a tag pointing at a tree with a pointer to content."""
        oid, pointer = self._pointer(content, store)
        blob = Blob.from_string(pointer)
        tree = Tree()
        tree.add(b"large.bin", 0o100644, blob.id)
        self.repo.object_store.add_objects([(blob, None), (tree, None)])
        self.repo.refs[b"refs/tags/" + name] = tree.id
        return oid

    def _tag_blob(self, name: bytes, content: bytes, store: LFSStore) -> str:
        """Create a tag pointing at a blob with a pointer to content."""
        oid, pointer = self._pointer(content, store)
        blob = Blob.from_string(pointer)
        self.repo.object_store.add_object(blob)
        self.repo.refs[b"refs/tags/" + name] = blob.id
        return oid

    def _stored(self, store: LFSStore, oids: list[str]) -> list[str]:
        """Return the oids that are present in an LFS store."""
        ret = []
        for oid in oids:
            try:
                with store.open_object(oid):
                    ret.append(oid)
            except KeyError:
                pass
        return ret

    def test_fetch(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        old = self._commit_pointer(b"old content", self.server.lfs_store)
        tip = self._commit_pointer(b"tip content", self.server.lfs_store)

        # Only the tree of HEAD is fetched by default
        self.assertEqual(1, porcelain.lfs_fetch(self.repo))
        self.assertEqual([tip], self._stored(self.local_store, [old, tip]))

        # Already present locally, so nothing left to fetch
        self.assertEqual(0, porcelain.lfs_fetch(self.repo))

    def test_fetch_creates_store(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        oid = self._commit_pointer(b"content", self.server.lfs_store)
        shutil.rmtree(self.local_store.path)

        self.assertEqual(1, porcelain.lfs_fetch(self.repo))
        self.assertEqual([oid], self._stored(self.local_store, [oid]))

    def test_fetch_ref(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        old = self._commit_pointer(b"old content", self.server.lfs_store)
        self.repo.refs[b"refs/heads/other"] = self.repo.head()
        tip = self._commit_pointer(b"tip content", self.server.lfs_store)

        self.assertEqual(1, porcelain.lfs_fetch(self.repo, refs=[b"refs/heads/other"]))
        self.assertEqual([old], self._stored(self.local_store, [old, tip]))

    def test_fetch_annotated_tag(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        oid = self._commit_pointer(b"content", self.server.lfs_store)
        porcelain.tag_create(
            self.repo, b"v1", author=b"A <a@example.com>", message=b"v1", annotated=True
        )

        self.assertEqual(1, porcelain.lfs_fetch(self.repo, refs=[b"refs/tags/v1"]))
        self.assertEqual([oid], self._stored(self.local_store, [oid]))

    def test_fetch_tree_ref(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        self._commit_pointer(b"content", self.server.lfs_store)
        oid = self._tag_tree(b"tree", b"tree content", self.server.lfs_store)

        self.assertEqual(1, porcelain.lfs_fetch(self.repo, refs=[b"refs/tags/tree"]))
        self.assertEqual([oid], self._stored(self.local_store, [oid]))

    def test_fetch_blob_ref(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        self._tag_blob(b"blob", b"blob content", self.server.lfs_store)

        with self.assertRaises(ValueError) as cm:
            porcelain.lfs_fetch(self.repo, refs=[b"refs/tags/blob"])
        self.assertEqual(
            "b'refs/tags/blob' does not refer to a commit or tree", str(cm.exception)
        )

    def test_fetch_all(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        old = self._commit_pointer(b"old content", self.server.lfs_store)
        tip = self._commit_pointer(b"tip content", self.server.lfs_store)
        porcelain.tag_create(
            self.repo, b"v1", author=b"A <a@example.com>", message=b"v1", annotated=True
        )
        tree = self._tag_tree(b"tree", b"tree content", self.server.lfs_store)
        blob = self._tag_blob(b"blob", b"blob content", self.server.lfs_store)

        self.assertEqual(4, porcelain.lfs_fetch(self.repo, all=True))
        self.assertEqual(
            [old, tip, tree, blob],
            self._stored(self.local_store, [old, tip, tree, blob]),
        )

    def test_fetch_all_ref(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        old = self._commit_pointer(b"old content", self.server.lfs_store)
        tip = self._commit_pointer(b"tip content", self.server.lfs_store)
        tree = self._tag_tree(b"tree", b"tree content", self.server.lfs_store)

        self.assertEqual(2, porcelain.lfs_fetch(self.repo, refs=[b"HEAD"], all=True))
        self.assertEqual([old, tip], self._stored(self.local_store, [old, tip, tree]))

    def test_fetch_all_empty_repo(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        self.assertEqual(0, porcelain.lfs_fetch(self.repo, all=True))

    def test_fetch_unborn_head(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        with self.assertRaises(KeyError) as cm:
            porcelain.lfs_fetch(self.repo)
        self.assertEqual((b"HEAD",), cm.exception.args)

    def test_fetch_unknown_ref(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        self._commit_pointer(b"content", self.server.lfs_store)
        with self.assertRaises(KeyError) as cm:
            porcelain.lfs_fetch(self.repo, refs=[b"refs/heads/nonexistent"])
        self.assertEqual((b"refs/heads/nonexistent",), cm.exception.args)

    def test_fetch_from_named_remote(self) -> None:
        remote_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, remote_dir)
        with Repo.init(remote_dir) as remote_repo:
            remote_store = LFSStore.from_repo(remote_repo, create=True)
        oid = self._commit_pointer(b"content in another repository", remote_store)
        self._set_config((b"remote", b"upstream"), b"url", remote_dir)

        self.assertEqual(1, porcelain.lfs_fetch(self.repo, remote="upstream"))
        self.assertEqual([oid], self._stored(self.local_store, [oid]))

    def test_fetch_no_url(self) -> None:
        self._commit_pointer(b"content", self.server.lfs_store)
        with self.assertRaises(ValueError) as cm:
            porcelain.lfs_fetch(self.repo)
        self.assertEqual("No LFS URL configured for remote origin", str(cm.exception))

    def test_pull(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        content = b"content on the server"
        self._commit_pointer(content, self.server.lfs_store)

        self.assertEqual(1, porcelain.lfs_pull(self.repo))
        with open(os.path.join(self.test_dir, "large.bin"), "rb") as f:
            self.assertEqual(content, f.read())

    def test_pull_unborn_head(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        with self.assertRaises(KeyError) as cm:
            porcelain.lfs_pull(self.repo)
        self.assertEqual((b"HEAD",), cm.exception.args)

    def test_push(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        old = self._commit_pointer(b"old content", self.local_store)
        tip = self._commit_pointer(b"tip content", self.local_store)
        tree = self._tag_tree(b"tree", b"tree content", self.local_store)

        # The objects for the whole history of the ref are pushed
        self.assertEqual(2, porcelain.lfs_push(self.repo, refs=[b"HEAD"]))
        self.assertEqual(
            [old, tip], self._stored(self.server.lfs_store, [old, tip, tree])
        )

    def test_push_requires_ref(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        self._commit_pointer(b"content", self.local_store)
        with self.assertRaises(ValueError) as cm:
            porcelain.lfs_push(self.repo)
        self.assertEqual(
            "At least one ref must be supplied without all", str(cm.exception)
        )

    def test_push_skips_remote_tracking(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        old = self._commit_pointer(b"old content", self.local_store)
        self.repo.refs[b"refs/remotes/origin/master"] = self.repo.head()
        tip = self._commit_pointer(b"tip content", self.local_store)

        # Objects the remote-tracking branches already refer to are skipped
        self.assertEqual(1, porcelain.lfs_push(self.repo, refs=[b"HEAD"]))
        self.assertEqual([tip], self._stored(self.server.lfs_store, [old, tip]))

        # Unless all objects are requested
        self.assertEqual(2, porcelain.lfs_push(self.repo, refs=[b"HEAD"], all=True))
        self.assertEqual([old, tip], self._stored(self.server.lfs_store, [old, tip]))

    def test_push_all(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        old = self._commit_pointer(b"old content", self.local_store)
        tip = self._commit_pointer(b"tip content", self.local_store)
        tip_commit = self.repo.head()
        tree = self._tag_tree(b"tree", b"tree content", self.local_store)
        # A commit that is only reachable from a remote-tracking branch
        elsewhere = self._commit_pointer(b"content from elsewhere", self.local_store)
        self.repo.refs[b"refs/remotes/elsewhere/master"] = self.repo.head()
        self.repo.refs[b"refs/heads/master"] = tip_commit

        # Only local branches and tags are pushed
        self.assertEqual(3, porcelain.lfs_push(self.repo, all=True))
        self.assertEqual(
            [old, tip, tree],
            self._stored(self.server.lfs_store, [old, tip, tree, elsewhere]),
        )

    def _other_store(self) -> LFSStore:
        """Create an LFS store that is neither the local nor the remote one."""
        path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, path)
        return LFSStore.create(path)

    def test_push_missing_object(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        present = self._commit_pointer(b"present", self.local_store, "a.bin")
        missing = self._commit_pointer(b"missing", self._other_store(), "b.bin")

        with self.assertRaises(LFSError) as cm:
            porcelain.lfs_push(self.repo, refs=[b"HEAD"])
        self.assertEqual(
            f"LFS objects are missing locally and on the remote: {missing}",
            str(cm.exception),
        )
        # Nothing is uploaded
        self.assertEqual([], self._stored(self.server.lfs_store, [present, missing]))

    def test_push_missing_object_allow_incomplete(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        self._set_config((b"lfs",), b"allowincompletepush", "true")
        present = self._commit_pointer(b"present", self.local_store, "a.bin")
        missing = self._commit_pointer(b"missing", self._other_store(), "b.bin")

        self.assertEqual(1, porcelain.lfs_push(self.repo, refs=[b"HEAD"]))
        self.assertEqual(
            [present], self._stored(self.server.lfs_store, [present, missing])
        )

    def test_push_missing_object_on_remote(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        present = self._commit_pointer(b"present", self.local_store, "a.bin")
        remote = self._commit_pointer(b"remote", self.server.lfs_store, "b.bin")

        # Objects that are missing locally are fine if the remote has them
        self.assertEqual(1, porcelain.lfs_push(self.repo, refs=[b"HEAD"]))
        self.assertEqual(
            [present, remote], self._stored(self.server.lfs_store, [present, remote])
        )

    def test_push_unborn_head(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        with self.assertRaises(KeyError) as cm:
            porcelain.lfs_push(self.repo, refs=[b"HEAD"])
        self.assertEqual((b"HEAD",), cm.exception.args)

    def test_push_unknown_ref(self) -> None:
        self._set_config((b"lfs",), b"url", self.server_url)
        self._commit_pointer(b"content", self.local_store)
        with self.assertRaises(KeyError) as cm:
            porcelain.lfs_push(self.repo, refs=[b"refs/heads/nonexistent"])
        self.assertEqual((b"refs/heads/nonexistent",), cm.exception.args)

    def test_push_no_url(self) -> None:
        with self.assertRaises(ValueError) as cm:
            porcelain.lfs_push(self.repo, refs=[b"HEAD"])
        self.assertEqual("No LFS URL configured for remote origin", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
