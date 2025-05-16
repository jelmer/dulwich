# test_submodule.py -- tests for submodule.py
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

"""Tests for submodule handling."""

import os
import shutil
import tempfile

from dulwich.objects import (
    S_ISGITLINK,
    Tree,
)
from dulwich.repo import Repo
from dulwich.submodule import iter_cached_submodules

from . import TestCase


class SubmoduleTests(TestCase):
    """Tests for submodule functions."""

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super().tearDown()

    def test_S_ISGITLINK(self) -> None:
        """Test the S_ISGITLINK function for checking gitlink mode."""
        # 0o160000 is the mode used for submodules
        self.assertTrue(S_ISGITLINK(0o160000))
        # Test some other modes to ensure they're not detected as gitlinks
        self.assertFalse(S_ISGITLINK(0o100644))  # regular file
        self.assertFalse(S_ISGITLINK(0o100755))  # executable file
        self.assertFalse(S_ISGITLINK(0o040000))  # directory

    def test_iter_cached_submodules(self) -> None:
        """Test the function to detect and iterate through submodules."""
        # Create a repository and add some content
        repo_dir = os.path.join(self.test_dir, "repo")
        os.makedirs(repo_dir)
        repo = Repo.init(repo_dir)

        # Create a file to add to our tree
        file_path = os.path.join(repo_dir, "file.txt")
        with open(file_path, "wb") as f:
            f.write(b"test file content")

        # Stage and commit the file to create some basic content
        repo.stage(["file.txt"])
        repo.do_commit(b"Initial commit")

        # Manually create the raw string for a tree with our file and a submodule
        # Format for tree entries: [mode] [name]\0[sha]

        # Get the blob SHA for our file using the index
        index = repo.open_index()
        file_entry = index[b"file.txt"]
        file_sha = file_entry.sha

        # Convert to binary representation needed for raw tree data
        binary_file_sha = bytes.fromhex(file_sha.decode("ascii"))

        # Generate a valid SHA for the submodule
        submodule_sha = b"1" * 40
        binary_submodule_sha = bytes.fromhex(submodule_sha.decode("ascii"))

        # Create raw tree data
        raw_tree_data = (
            # Regular file entry
            b"100644 file.txt\0"
            + binary_file_sha
            +
            # Submodule entry with gitlink mode
            b"160000 submodule\0"
            + binary_submodule_sha
        )

        # Create a tree from raw data
        from dulwich.objects import ShaFile

        tree = ShaFile.from_raw_string(Tree.type_num, raw_tree_data)

        # Add the tree to the repository
        repo.object_store.add_object(tree)

        # Now we can test the iter_cached_submodules function
        submodules = list(iter_cached_submodules(repo.object_store, tree.id))

        # We should find one submodule
        self.assertEqual(1, len(submodules))

        # Verify the submodule details
        path, sha = submodules[0]
        self.assertEqual(b"submodule", path)
        self.assertEqual(submodule_sha, sha)
