# test_patch.py -- test patch compatibility with CGit
# Copyright (C) 2019 Boris Feld <boris@comet.ml>
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

"""Tests related to patch compatibility with CGit."""

import os
import shutil
import tempfile
from io import BytesIO

from dulwich import porcelain
from dulwich.repo import Repo

from .utils import CompatTestCase, run_git_or_fail


class CompatPatchTestCase(CompatTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.repo_path = os.path.join(self.test_dir, "repo")
        self.repo = Repo.init(self.repo_path, mkdir=True)
        self.addCleanup(self.repo.close)

    def test_patch_apply(self) -> None:
        # Prepare the repository

        # Create some files and commit them
        file_list = ["to_exists", "to_modify", "to_delete"]
        for file in file_list:
            file_path = os.path.join(self.repo_path, file)

            # Touch the files
            with open(file_path, "w"):
                pass

        self.repo.get_worktree().stage(file_list)

        first_commit = self.repo.get_worktree().commit(
            message=b"The first commit",
        )

        # Make a copy of the repository so we can apply the diff later
        copy_path = os.path.join(self.test_dir, "copy")
        shutil.copytree(self.repo_path, copy_path)

        # Do some changes
        with open(os.path.join(self.repo_path, "to_modify"), "w") as f:
            f.write("Modified!")

        os.remove(os.path.join(self.repo_path, "to_delete"))

        with open(os.path.join(self.repo_path, "to_add"), "w"):
            pass

        self.repo.get_worktree().stage(["to_modify", "to_delete", "to_add"])

        second_commit = self.repo.get_worktree().commit(
            message=b"The second commit",
        )

        # Get the patch
        first_tree = self.repo[first_commit].tree
        second_tree = self.repo[second_commit].tree

        outstream = BytesIO()
        porcelain.diff_tree(
            self.repo.path, first_tree, second_tree, outstream=outstream
        )

        # Save it on disk
        patch_path = os.path.join(self.test_dir, "patch.patch")
        with open(patch_path, "wb") as patch:
            patch.write(outstream.getvalue())

        # And try to apply it to the copy directory
        git_command = ["-C", copy_path, "apply", patch_path]
        run_git_or_fail(git_command)

        # And now check that the files contents are exactly the same between
        # the two repositories
        original_files = set(os.listdir(self.repo_path))
        new_files = set(os.listdir(copy_path))

        # Check that we have the exact same files in both repositories
        self.assertEqual(original_files, new_files)

        for file in original_files:
            if file == ".git":
                continue

            original_file_path = os.path.join(self.repo_path, file)
            copy_file_path = os.path.join(copy_path, file)

            self.assertTrue(os.path.isfile(copy_file_path))

            with open(original_file_path, "rb") as original_file:
                original_content = original_file.read()

            with open(copy_file_path, "rb") as copy_file:
                copy_content = copy_file.read()

            self.assertEqual(original_content, copy_content)

    def test_apply_binary_patch(self) -> None:
        """Test applying a binary patch from git."""
        # Create a binary file
        binary_path = os.path.join(self.repo_path, "binary.bin")
        with open(binary_path, "wb") as f:
            f.write(b"\x00\x01\x02\x03\x04\x05")

        self.repo.get_worktree().stage(["binary.bin"])
        self.repo.get_worktree().commit(message=b"Add binary file")

        # Make a copy
        copy_path = os.path.join(self.test_dir, "copy")
        shutil.copytree(self.repo_path, copy_path)

        # Modify the binary file
        with open(binary_path, "wb") as f:
            f.write(b"\x06\x07\x08\x09\x0a")

        # Generate binary patch with git
        patch_path = os.path.join(self.test_dir, "binary.patch")
        run_git_or_fail(
            ["-C", self.repo_path, "diff", "--binary", "HEAD", "binary.bin"],
            stdout=open(patch_path, "wb"),
        )

        # Apply with dulwich
        with open(patch_path, "rb") as f:
            patch_content = f.read()

        if patch_content.strip():  # Only if there's actual patch content
            porcelain.apply_patch(copy_path, patch_file=BytesIO(patch_content))

            # Verify the file matches
            with (
                open(binary_path, "rb") as f1,
                open(os.path.join(copy_path, "binary.bin"), "rb") as f2,
            ):
                self.assertEqual(f1.read(), f2.read())

    def test_apply_rename_patch(self) -> None:
        """Test applying a rename patch from git."""
        # Create a file
        file_path = os.path.join(self.repo_path, "oldname.txt")
        with open(file_path, "w") as f:
            f.write("content")

        self.repo.get_worktree().stage(["oldname.txt"])
        self.repo.get_worktree().commit(message=b"Add file")

        # Make a copy
        copy_path = os.path.join(self.test_dir, "copy")
        shutil.copytree(self.repo_path, copy_path)

        # Rename the file with git
        run_git_or_fail(["-C", self.repo_path, "mv", "oldname.txt", "newname.txt"])

        # Generate patch
        patch_path = os.path.join(self.test_dir, "rename.patch")
        run_git_or_fail(
            ["-C", self.repo_path, "diff", "--cached"], stdout=open(patch_path, "wb")
        )

        # Apply with dulwich
        with open(patch_path, "rb") as f:
            patch_content = f.read()

        if patch_content.strip():
            porcelain.apply_patch(copy_path, patch_file=BytesIO(patch_content))

            # Verify old file is gone and new file exists
            self.assertFalse(os.path.exists(os.path.join(copy_path, "oldname.txt")))
            self.assertTrue(os.path.exists(os.path.join(copy_path, "newname.txt")))

            with open(os.path.join(copy_path, "newname.txt")) as f:
                self.assertEqual(f.read(), "content")

    def test_apply_copy_patch(self) -> None:
        """Test applying a copy patch from git."""
        # Create a file
        file_path = os.path.join(self.repo_path, "original.txt")
        with open(file_path, "w") as f:
            f.write("original content")

        self.repo.get_worktree().stage(["original.txt"])
        self.repo.get_worktree().commit(message=b"Add file")

        # Make a copy
        copy_path = os.path.join(self.test_dir, "copy")
        shutil.copytree(self.repo_path, copy_path)

        # Copy the file with git (need to enable copy detection)
        shutil.copy(file_path, os.path.join(self.repo_path, "copied.txt"))
        run_git_or_fail(["-C", self.repo_path, "add", "copied.txt"])

        # Generate patch with copy detection
        patch_path = os.path.join(self.test_dir, "copy.patch")
        run_git_or_fail(
            ["-C", self.repo_path, "diff", "--cached", "-C"],
            stdout=open(patch_path, "wb"),
        )

        # Apply with dulwich
        with open(patch_path, "rb") as f:
            patch_content = f.read()

        if patch_content.strip() and b"copy from" in patch_content:
            porcelain.apply_patch(copy_path, patch_file=BytesIO(patch_content))

            # Verify both files exist
            self.assertTrue(os.path.exists(os.path.join(copy_path, "original.txt")))
            self.assertTrue(os.path.exists(os.path.join(copy_path, "copied.txt")))

            with open(os.path.join(copy_path, "copied.txt")) as f:
                self.assertEqual(f.read(), "original content")
