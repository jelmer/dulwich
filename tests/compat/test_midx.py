# test_midx.py -- Compatibility tests for multi-pack-index functionality
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
# Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
# General Public License as published by the Free Software Foundation; version 2.0
# or (at your option) any later version. You can redistribute it and/or
# modify it under the terms of either of these two licenses.

"""Compatibility tests for Git multi-pack-index functionality.

These tests verify that dulwich's MIDX implementation can read and interact
with MIDX files created by C Git, and that Git can read MIDX files created
by Dulwich.
"""

import os
import tempfile

from dulwich.midx import load_midx
from dulwich.object_store import DiskObjectStore
from dulwich.repo import Repo

from .utils import CompatTestCase, run_git_or_fail


class MIDXCompatTests(CompatTestCase):
    """Compatibility tests for multi-pack-index functionality."""

    # Multi-pack-index was introduced in Git 2.21.0
    min_git_version = (2, 21, 0)

    def setUp(self):
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.repo_path = os.path.join(self.test_dir, "test-repo")

        # Set up git identity to avoid committer identity errors
        self.overrideEnv("GIT_COMMITTER_NAME", "Test Author")
        self.overrideEnv("GIT_COMMITTER_EMAIL", "test@example.com")
        self.overrideEnv("GIT_AUTHOR_NAME", "Test Author")
        self.overrideEnv("GIT_AUTHOR_EMAIL", "test@example.com")

    def tearDown(self):
        from .utils import rmtree_ro

        rmtree_ro(self.test_dir)

    def create_test_repo_with_packs(self):
        """Create a test repository with multiple pack files."""
        # Initialize repository
        run_git_or_fail(["init"], cwd=self.test_dir)
        os.rename(os.path.join(self.test_dir, ".git"), self.repo_path)

        work_dir = os.path.join(self.test_dir, "work")
        os.makedirs(work_dir)

        # Create .git file pointing to our repo
        with open(os.path.join(work_dir, ".git"), "w") as f:
            f.write(f"gitdir: {self.repo_path}\n")

        # Create some commits and pack them
        for i in range(5):
            filename = f"file{i}.txt"
            with open(os.path.join(work_dir, filename), "w") as f:
                f.write(f"Content {i}\n" * 100)  # Make files bigger to ensure packing

            run_git_or_fail(["add", filename], cwd=work_dir)
            run_git_or_fail(
                [
                    "commit",
                    "-m",
                    f"Commit {i}",
                    "--author",
                    "Test Author <test@example.com>",
                ],
                cwd=work_dir,
            )

            # Create a pack file after each commit to get multiple packs
            if i > 0:  # Skip first commit to avoid empty pack
                run_git_or_fail(["repack", "-d"], cwd=work_dir)

        return work_dir

    def test_read_git_midx(self):
        """Test that Dulwich can read a MIDX file created by Git."""
        work_dir = self.create_test_repo_with_packs()

        # Have Git create a MIDX file
        run_git_or_fail(["multi-pack-index", "write"], cwd=work_dir)

        # Verify Git created the MIDX file
        midx_path = os.path.join(self.repo_path, "objects", "pack", "multi-pack-index")
        self.assertTrue(
            os.path.exists(midx_path), "Git did not create multi-pack-index file"
        )

        # Load the MIDX file with Dulwich
        midx = load_midx(midx_path)
        try:
            # Verify we can read it
            self.assertGreater(len(midx), 0, "MIDX should contain objects")
            self.assertGreater(midx.pack_count, 0, "MIDX should reference packs")

            # Verify the pack names look reasonable
            # Git stores .idx extensions in MIDX files
            for pack_name in midx.pack_names:
                self.assertTrue(pack_name.startswith("pack-"))
                self.assertTrue(pack_name.endswith(".idx"))
        finally:
            midx.close()

    def test_git_uses_dulwich_midx(self):
        """Test that Git can use a MIDX file created by Dulwich."""
        work_dir = self.create_test_repo_with_packs()

        # Use Dulwich to create a MIDX file
        repo = Repo(self.repo_path)
        try:
            store = repo.object_store
            self.assertIsInstance(store, DiskObjectStore)

            # Write MIDX with Dulwich
            checksum = store.write_midx()
            self.assertEqual(20, len(checksum))
        finally:
            repo.close()

        # Verify the file was created
        midx_path = os.path.join(self.repo_path, "objects", "pack", "multi-pack-index")
        self.assertTrue(os.path.exists(midx_path))

        # Have Git verify the MIDX file (should succeed with return code 0)
        run_git_or_fail(["multi-pack-index", "verify"], cwd=work_dir)

        # Try to use the MIDX with Git commands
        # This should work if the MIDX is valid
        run_git_or_fail(["fsck"], cwd=work_dir)

    def test_midx_object_lookup_matches_git(self):
        """Test that object lookups through MIDX match Git's results."""
        work_dir = self.create_test_repo_with_packs()

        # Have Git create a MIDX file
        run_git_or_fail(["multi-pack-index", "write"], cwd=work_dir)

        # Load with Dulwich
        repo = Repo(self.repo_path)
        try:
            store = repo.object_store

            # Get MIDX
            midx = store.get_midx()
            self.assertIsNotNone(midx, "MIDX should be loaded")

            # Get all objects from Git
            result = run_git_or_fail(["rev-list", "--all", "--objects"], cwd=work_dir)
            object_shas = [
                line.split()[0].encode("ascii")
                for line in result.decode("utf-8").strip().split("\n")
                if line
            ]

            # Verify we can find these objects through the MIDX
            found_count = 0
            for sha_hex in object_shas:
                # Convert hex to binary
                sha_bin = bytes.fromhex(sha_hex.decode("ascii"))

                # Check if it's in the MIDX
                if sha_bin in midx:
                    found_count += 1

                    # Verify we can get the object location
                    result = midx.object_offset(sha_bin)
                    self.assertIsNotNone(result)
                    pack_name, offset = result
                    self.assertIsInstance(pack_name, str)
                    self.assertIsInstance(offset, int)
                    self.assertGreater(offset, 0)

            # We should find at least some objects in the MIDX
            self.assertGreater(
                found_count, 0, "Should find at least some objects in MIDX"
            )
        finally:
            repo.close()

    def test_midx_with_multiple_packs(self):
        """Test MIDX functionality with multiple pack files."""
        work_dir = self.create_test_repo_with_packs()

        # Create multiple pack files explicitly
        run_git_or_fail(["repack"], cwd=work_dir)
        run_git_or_fail(["repack"], cwd=work_dir)

        # Create MIDX with Git
        run_git_or_fail(["multi-pack-index", "write"], cwd=work_dir)

        # Load with Dulwich
        midx_path = os.path.join(self.repo_path, "objects", "pack", "multi-pack-index")
        midx = load_midx(midx_path)
        try:
            # Should have multiple packs
            # (Exact count may vary depending on Git version and repacking)
            self.assertGreaterEqual(midx.pack_count, 1)

            # Verify we can iterate over all entries
            entries = list(midx.iterentries())
            self.assertGreater(len(entries), 0)

            # All entries should have valid structure
            for sha, pack_name, offset in entries:
                self.assertEqual(20, len(sha))  # SHA-1 is 20 bytes
                self.assertIsInstance(pack_name, str)
                # Git stores .idx extensions in MIDX files
                self.assertTrue(pack_name.endswith(".idx"))
                self.assertIsInstance(offset, int)
                self.assertGreaterEqual(offset, 0)
        finally:
            midx.close()

    def test_dulwich_object_store_with_git_midx(self):
        """Test that DiskObjectStore can use Git-created MIDX for lookups."""
        work_dir = self.create_test_repo_with_packs()

        # Have Git create a MIDX file
        run_git_or_fail(["multi-pack-index", "write"], cwd=work_dir)

        # Load repo with Dulwich
        repo = Repo(self.repo_path)
        try:
            # Get a commit from the repo
            result = run_git_or_fail(["rev-parse", "HEAD"], cwd=work_dir)
            head_sha = result.decode("utf-8").strip().encode("ascii")

            # Verify we can access it through Dulwich
            # This should use the MIDX for lookup
            obj = repo.object_store[head_sha]
            self.assertIsNotNone(obj)
            self.assertEqual(b"commit", obj.type_name)
        finally:
            repo.close()

    def test_repack_with_midx(self):
        """Test that repacking works correctly with MIDX present."""
        work_dir = self.create_test_repo_with_packs()

        # Create MIDX with Dulwich
        repo = Repo(self.repo_path)
        try:
            repo.object_store.write_midx()
        finally:
            repo.close()

        # Verify Git can still repack
        run_git_or_fail(["repack", "-d"], cwd=work_dir)

        # The MIDX should still be readable
        midx_path = os.path.join(self.repo_path, "objects", "pack", "multi-pack-index")
        if os.path.exists(midx_path):  # Git may remove it during repack
            midx = load_midx(midx_path)
            try:
                self.assertGreaterEqual(len(midx), 0)
            finally:
                midx.close()
