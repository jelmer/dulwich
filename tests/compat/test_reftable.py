"""Reftable compatibility tests with C git."""

import os
import struct
import tempfile

from dulwich.reftable import (
    REF_VALUE_DELETE,
    REF_VALUE_REF,
    REF_VALUE_SYMREF,
    REFTABLE_MAGIC,
    REFTABLE_VERSION,
    ReftableReader,
    ReftableRefsContainer,
)
from dulwich.repo import Repo

from .utils import CompatTestCase, run_git_or_fail


class ReftableCompatTestCase(CompatTestCase):
    """Test reftable compatibility with C git."""

    min_git_version = (2, 36, 0)  # First version with reftable support

    def setUp(self):
        """Set up test environment."""
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        """Clean up test directory."""
        import shutil

        shutil.rmtree(self.test_dir)

    def _run_git(self, args, **kwargs):
        """Run git command in test directory."""
        return run_git_or_fail(args, cwd=self.test_dir, **kwargs)

    def _create_git_repo_with_reftable(self):
        """Create a git repository with reftable format."""
        # Initialize repo with reftable format
        self._run_git(["init", "--bare", "--ref-format=reftable", "."])

        # Create some test objects and refs
        self._run_git(["hash-object", "-w", "--stdin"], input=b"test content 1\n")
        self._run_git(["hash-object", "-w", "--stdin"], input=b"test content 2\n")

        # Create refs
        sha1 = self._run_git(
            ["hash-object", "-w", "--stdin"],
            input=b"test content 1\n",
        ).strip()
        sha2 = self._run_git(
            ["hash-object", "-w", "--stdin"],
            input=b"test content 2\n",
        ).strip()

        self._run_git(["update-ref", "refs/heads/master", sha1.decode()])
        self._run_git(["update-ref", "refs/heads/develop", sha2.decode()])
        self._run_git(["symbolic-ref", "HEAD", "refs/heads/master"])

        return sha1, sha2

    def _create_reftable_repo(self):
        """Create a Dulwich repository with reftable extension configured."""
        from io import BytesIO

        from dulwich.config import ConfigFile

        # Initialize bare repo
        repo = Repo.init_bare(self.test_dir)
        repo.close()

        # Add reftable extension to config
        config_path = os.path.join(self.test_dir, "config")
        with open(config_path, "rb") as f:
            config_data = f.read()

        config = ConfigFile()
        config.from_file(BytesIO(config_data))
        config.set(b"extensions", b"refStorage", b"reftable")

        with open(config_path, "wb") as f:
            config.write_to_file(f)

        # Reopen repo with reftable extension
        return Repo(self.test_dir)

    def _get_reftable_files(self):
        """Get list of reftable files in the repository."""
        reftable_dir = os.path.join(self.test_dir, "reftable")
        if not os.path.exists(reftable_dir):
            return []

        files = []
        for filename in os.listdir(reftable_dir):
            if filename.endswith((".ref", ".log")):
                files.append(os.path.join(reftable_dir, filename))
        return sorted(files)

    def _validate_reftable_format(self, filepath: str):
        """Validate that a file follows the reftable format specification."""
        with open(filepath, "rb") as f:
            # Check magic header
            magic = f.read(4)
            self.assertEqual(magic, REFTABLE_MAGIC, "Invalid reftable magic")

            # Check version
            version = struct.unpack(">I", f.read(4))[0]
            self.assertEqual(
                version, REFTABLE_VERSION, f"Unsupported version: {version}"
            )

            # Check that we can read the header without errors
            min_update_index = struct.unpack(">Q", f.read(8))[0]
            max_update_index = struct.unpack(">Q", f.read(8))[0]

            # Update indices should be valid
            self.assertGreaterEqual(max_update_index, min_update_index)

    def test_git_creates_valid_reftable_format(self):
        """Test that git creates reftable files with valid format."""
        sha1, sha2 = self._create_git_repo_with_reftable()

        # Check that reftable files were created
        reftable_files = self._get_reftable_files()
        self.assertGreater(len(reftable_files), 0, "No reftable files created")

        # Validate format of each reftable file
        for filepath in reftable_files:
            if filepath.endswith(".ref"):
                self._validate_reftable_format(filepath)

    def test_dulwich_can_read_git_reftables(self):
        """Test that Dulwich can read reftables created by git."""
        sha1, sha2 = self._create_git_repo_with_reftable()

        # Open with Dulwich
        repo = Repo(self.test_dir)

        # Verify it's using reftable
        self.assertIsInstance(repo.refs, ReftableRefsContainer)

        # Check that we can read the refs
        all_refs = repo.get_refs()
        self.assertIn(b"refs/heads/master", all_refs)
        self.assertIn(b"refs/heads/develop", all_refs)
        self.assertIn(b"HEAD", all_refs)

        # Verify ref values
        self.assertEqual(all_refs[b"refs/heads/master"], sha1)
        self.assertEqual(all_refs[b"refs/heads/develop"], sha2)
        self.assertEqual(all_refs[b"HEAD"], sha1)  # HEAD -> refs/heads/master

    def test_git_can_read_dulwich_reftables(self):
        """Test that git can read reftables created by Dulwich."""
        # Create a repo with reftable extension
        repo = self._create_reftable_repo()
        self.assertIsInstance(repo.refs, ReftableRefsContainer)

        # Create some refs with Dulwich
        sha1 = b"1234567890abcdef1234567890abcdef12345678"
        sha2 = b"abcdef1234567890abcdef1234567890abcdef12"

        repo.refs.set_if_equals(b"refs/heads/master", None, sha1)
        repo.refs.set_if_equals(b"refs/heads/develop", None, sha2)
        repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        repo.close()

        # Try to read with git (if it supports reftable)
        output = self._run_git(["show-ref"])
        # Git should be able to see our refs
        self.assertIn(b"refs/heads/master", output)
        self.assertIn(b"refs/heads/develop", output)

    def test_reftable_file_structure_compatibility(self):
        """Test that reftable file structure matches git's expectations."""
        self._create_git_repo_with_reftable()

        reftable_files = self._get_reftable_files()
        ref_files = [f for f in reftable_files if f.endswith(".ref")]

        for ref_file in ref_files:
            with open(ref_file, "rb") as f:
                # Try to read with our ReftableReader
                reader = ReftableReader(f)
                refs = reader.all_refs()

                # Should have some refs
                self.assertGreater(len(refs), 0)

                # All refs should have valid types and values
                for refname, (ref_type, value) in refs.items():
                    self.assertIsInstance(refname, bytes)
                    self.assertIn(
                        ref_type,
                        [REF_VALUE_REF, REF_VALUE_SYMREF, REF_VALUE_DELETE],
                    )
                    self.assertIsInstance(value, bytes)

                    if ref_type == REF_VALUE_REF:
                        self.assertEqual(
                            len(value), 40, f"Invalid SHA length for {refname}"
                        )

    def test_ref_operations_match_git_behavior(self):
        """Test that ref operations produce the same results as git."""
        self._create_git_repo_with_reftable()

        # Read refs with git
        git_output = self._run_git(["show-ref"])
        git_refs = {}
        for line in git_output.split(b"\n"):
            if line.strip():
                sha, refname = line.strip().split(b" ", 1)
                git_refs[refname] = sha

        # Read refs with Dulwich
        repo = Repo(self.test_dir)
        dulwich_refs = repo.get_refs()

        # Compare non-symbolic refs
        for refname, sha in git_refs.items():
            if refname != b"HEAD":  # HEAD is symbolic, compare differently
                self.assertIn(refname, dulwich_refs)
                self.assertEqual(dulwich_refs[refname], sha)

        # Check HEAD symbolic ref
        head_target = self._run_git(["rev-parse", "HEAD"]).strip()

        self.assertEqual(dulwich_refs[b"HEAD"], head_target)
        repo.close()

    def test_multiple_table_files_compatibility(self):
        """Test compatibility when multiple reftable files exist."""
        sha1, sha2 = self._create_git_repo_with_reftable()

        # Add more refs to potentially create multiple table files
        for i in range(10):
            content = f"test content {i}\n".encode()
            sha = self._run_git(["hash-object", "-w", "--stdin"], input=content).strip()
            self._run_git(["update-ref", f"refs/tags/tag{i}", sha.decode()])

        # Read with both git and Dulwich
        repo = Repo(self.test_dir)
        dulwich_refs = repo.get_refs()

        git_output = self._run_git(["show-ref"])
        git_ref_count = len([line for line in git_output.split(b"\n") if line.strip()])

        # Should have roughly the same number of refs
        self.assertGreaterEqual(
            len(dulwich_refs), git_ref_count - 1
        )  # -1 for potential HEAD differences
        repo.close()

    def test_empty_reftable_compatibility(self):
        """Test compatibility with empty reftable repositories."""
        # Create repo with reftable extension
        repo = self._create_reftable_repo()
        self.assertIsInstance(repo.refs, ReftableRefsContainer)

        # Should have HEAD but no other refs initially
        all_keys = list(repo.refs.allkeys())
        self.assertIn(b"HEAD", all_keys)
        self.assertEqual(len(all_keys), 1)

        # Add a single ref
        test_sha = b"1234567890abcdef1234567890abcdef12345678"
        repo.refs.set_if_equals(b"refs/heads/main", None, test_sha)

        # Should now have HEAD plus our ref
        all_keys = list(repo.refs.allkeys())
        self.assertEqual(len(all_keys), 2)
        self.assertIn(b"HEAD", all_keys)
        self.assertIn(b"refs/heads/main", all_keys)
        self.assertEqual(repo.refs.read_loose_ref(b"refs/heads/main"), test_sha)

        repo.close()

    def test_reftable_update_compatibility(self):
        """Test that ref updates work compatibly with git."""
        repo = self._create_reftable_repo()

        # Create initial ref
        sha1 = b"1111111111111111111111111111111111111111"
        sha2 = b"2222222222222222222222222222222222222222"

        repo.refs.set_if_equals(b"refs/heads/master", None, sha1)

        # Update ref
        success = repo.refs.set_if_equals(b"refs/heads/master", sha1, sha2)
        self.assertTrue(success)

        # Verify update
        self.assertEqual(repo.refs.read_loose_ref(b"refs/heads/master"), sha2)

        # Try invalid update (should fail)
        success = repo.refs.set_if_equals(b"refs/heads/master", sha1, b"3" * 40)
        self.assertFalse(success)

        # Ref should be unchanged
        self.assertEqual(repo.refs.read_loose_ref(b"refs/heads/master"), sha2)

        repo.close()

    def test_symbolic_ref_compatibility(self):
        """Test symbolic reference compatibility."""
        repo = self._create_reftable_repo()

        # Create target ref
        test_sha = b"abcdef1234567890abcdef1234567890abcdef12"
        repo.refs.set_if_equals(b"refs/heads/main", None, test_sha)

        # Create symbolic ref
        repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/main")

        # Verify symbolic ref resolves correctly
        self.assertEqual(repo.refs.read_loose_ref(b"HEAD"), test_sha)

        # Update target and verify symbolic ref follows
        new_sha = b"fedcba0987654321fedcba0987654321fedcba09"
        repo.refs.set_if_equals(b"refs/heads/main", test_sha, new_sha)
        self.assertEqual(repo.refs.read_loose_ref(b"HEAD"), new_sha)

        repo.close()


if __name__ == "__main__":
    import unittest

    unittest.main()
