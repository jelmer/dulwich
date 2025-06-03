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

from .utils import CompatTestCase, rmtree_ro, run_git_or_fail


class ReftableCompatTestCase(CompatTestCase):
    """Test reftable compatibility with C git."""

    min_git_version = (2, 45, 0)  # Version with stable reftable support

    def setUp(self):
        """Set up test environment."""
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        """Clean up test directory."""
        rmtree_ro(self.test_dir)

    def _run_git(self, args, **kwargs):
        """Run git command in test directory."""
        return run_git_or_fail(args, cwd=self.test_dir, **kwargs)

    def _create_git_repo_with_reftable(self):
        """Create a git repository with reftable format."""
        # Initialize repo with reftable format
        self._run_git(["init", "--bare", "--ref-format=reftable", "."])

        # Create some test objects and refs using proper commits
        blob1_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content 1\n"
        ).strip()
        blob2_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content 2\n"
        ).strip()

        # Create tree objects
        tree1_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob1_sha.decode()}\tfile1.txt\n".encode()
        ).strip()
        tree2_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob2_sha.decode()}\tfile2.txt\n".encode()
        ).strip()

        # Create commit objects
        sha1 = self._run_git(
            ["commit-tree", tree1_sha.decode(), "-m", "First commit"]
        ).strip()
        sha2 = self._run_git(
            ["commit-tree", tree2_sha.decode(), "-m", "Second commit"]
        ).strip()

        # Create refs
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
        config.set(b"core", b"repositoryformatversion", b"1")
        config.set(b"extensions", b"refStorage", b"reftable")
        # Use master as default branch for reftable compatibility
        config.set(b"init", b"defaultBranch", b"master")

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

            # Check version + block size (4 bytes total)
            # Format: uint8(version) + uint24(block_size)
            version_and_blocksize = struct.unpack(">I", f.read(4))[0]
            version = (version_and_blocksize >> 24) & 0xFF  # First byte
            block_size = version_and_blocksize & 0xFFFFFF  # Last 3 bytes

            self.assertEqual(
                version, REFTABLE_VERSION, f"Unsupported version: {version}"
            )
            self.assertGreater(block_size, 0, "Invalid block size")

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

        repo.close()

    def test_git_can_read_dulwich_reftables(self):
        """Test that git can read reftables created by Dulwich."""
        # Create a repo with reftable extension
        repo = self._create_reftable_repo()
        self.assertIsInstance(repo.refs, ReftableRefsContainer)

        # Create real objects that git can validate
        blob1_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content 1\n"
        ).strip()
        blob2_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content 2\n"
        ).strip()

        # Create tree objects
        tree1_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob1_sha.decode()}\tfile1.txt\n".encode()
        ).strip()
        tree2_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob2_sha.decode()}\tfile2.txt\n".encode()
        ).strip()

        # Create commit objects
        sha1 = self._run_git(
            ["commit-tree", tree1_sha.decode(), "-m", "First commit"]
        ).strip()
        sha2 = self._run_git(
            ["commit-tree", tree2_sha.decode(), "-m", "Second commit"]
        ).strip()

        # Create exactly what Git does: consolidated table with multiple refs

        # Use batched operations to create a single consolidated reftable like git
        with repo.refs.batch_update():
            repo.refs.set_if_equals(b"refs/heads/master", None, sha1)
            repo.refs.set_if_equals(b"refs/heads/develop", None, sha2)
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        repo.refs._update_tables_list()

        repo.close()

        # Test that git can read our reftables
        # Test symbolic ref reading
        head_output = self._run_git(["symbolic-ref", "HEAD"])
        self.assertEqual(head_output.strip(), b"refs/heads/master")

        # Test ref parsing
        master_output = self._run_git(["rev-parse", "HEAD"])
        self.assertEqual(master_output.strip(), sha1)

        # Test show-ref
        show_output = self._run_git(["show-ref"])
        self.assertIn(b"refs/heads/master", show_output)
        self.assertIn(b"refs/heads/develop", show_output)

    def test_reftable_file_structure_compatibility(self):
        """Test that reftable file structure matches git's expectations."""
        self._create_git_repo_with_reftable()

        reftable_files = self._get_reftable_files()
        ref_files = [f for f in reftable_files if f.endswith(".ref")]

        for ref_file in ref_files:
            with open(ref_file, "rb") as f:
                # Read with our ReftableReader
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

        # Should have no refs initially (reftable doesn't create default HEAD)
        all_keys = list(repo.refs.allkeys())
        self.assertEqual(len(all_keys), 0)

        # Add a single ref
        test_sha = b"1234567890abcdef1234567890abcdef12345678"
        repo.refs.set_if_equals(b"refs/heads/master", None, test_sha)

        # Should now have our ref
        all_keys = list(repo.refs.allkeys())
        self.assertEqual(len(all_keys), 1)
        self.assertIn(b"refs/heads/master", all_keys)
        self.assertEqual(repo.refs.read_loose_ref(b"refs/heads/master"), test_sha)

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
        repo.refs.set_if_equals(b"refs/heads/master", None, test_sha)

        # Create symbolic ref
        repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        # Verify symbolic ref resolves correctly
        self.assertEqual(repo.refs[b"HEAD"], test_sha)
        # Verify read_loose_ref returns the symbolic ref format
        self.assertEqual(repo.refs.read_loose_ref(b"HEAD"), b"ref: refs/heads/master")

        # Update target and verify symbolic ref follows
        new_sha = b"fedcba0987654321fedcba0987654321fedcba09"
        repo.refs.set_if_equals(b"refs/heads/master", test_sha, new_sha)
        self.assertEqual(repo.refs[b"HEAD"], new_sha)

        repo.close()

    def test_complex_ref_scenarios_compatibility(self):
        """Test complex ref scenarios that git should handle correctly."""
        # Initialize repo
        self._run_git(["init", "--bare", "."])
        self._run_git(["config", "core.repositoryformatversion", "1"])
        self._run_git(["config", "extensions.refStorage", "reftable"])

        # Create test objects
        blob_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content"
        ).strip()
        tree_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob_sha.decode()}\tfile.txt\n".encode()
        ).strip()
        commit_sha1 = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "First commit"]
        ).strip()
        commit_sha2 = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Second commit"]
        ).strip()
        commit_sha3 = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Third commit"]
        ).strip()

        repo = Repo(self.test_dir)

        # Test complex batched operations
        with repo.refs.batch_update():
            # Multiple branches
            repo.refs.set_if_equals(b"refs/heads/master", None, commit_sha1)
            repo.refs.set_if_equals(b"refs/heads/feature/awesome", None, commit_sha2)
            repo.refs.set_if_equals(b"refs/heads/hotfix/critical", None, commit_sha3)

            # Multiple tags
            repo.refs.set_if_equals(b"refs/tags/v1.0.0", None, commit_sha1)
            repo.refs.set_if_equals(b"refs/tags/v1.1.0", None, commit_sha2)

            # Symbolic refs
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")
            repo.refs.set_symbolic_ref(
                b"refs/remotes/origin/HEAD", b"refs/remotes/origin/main"
            )

            # Remote refs
            repo.refs.set_if_equals(b"refs/remotes/origin/main", None, commit_sha1)
            repo.refs.set_if_equals(
                b"refs/remotes/origin/feature/awesome", None, commit_sha2
            )

        repo.close()

        # Verify git can read all refs correctly
        git_refs = {}
        git_output = self._run_git(["show-ref", "--head"])
        for line in git_output.split(b"\n"):
            if line.strip():
                sha, refname = line.strip().split(b" ", 1)
                git_refs[refname] = sha

        # Verify HEAD symbolic ref
        head_target = self._run_git(["symbolic-ref", "HEAD"]).strip()
        self.assertEqual(head_target, b"refs/heads/master")

        # Verify all branches resolve correctly
        main_sha = self._run_git(["rev-parse", "refs/heads/master"]).strip()
        self.assertEqual(main_sha, commit_sha1)

        feature_sha = self._run_git(["rev-parse", "refs/heads/feature/awesome"]).strip()
        self.assertEqual(feature_sha, commit_sha2)

        # Verify tags
        tag_sha = self._run_git(["rev-parse", "refs/tags/v1.0.0"]).strip()
        self.assertEqual(tag_sha, commit_sha1)

    def test_ref_deletion_compatibility(self):
        """Test that ref deletion works correctly with git."""
        # Initialize repo with refs
        self._run_git(["init", "--bare", "."])
        self._run_git(["config", "core.repositoryformatversion", "1"])
        self._run_git(["config", "extensions.refStorage", "reftable"])

        # Create test objects
        blob_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content"
        ).strip()
        tree_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob_sha.decode()}\tfile.txt\n".encode()
        ).strip()
        commit_sha1 = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Commit 1"]
        ).strip()
        commit_sha2 = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Commit 2"]
        ).strip()

        repo = Repo(self.test_dir)

        # Create multiple refs
        with repo.refs.batch_update():
            repo.refs.set_if_equals(b"refs/heads/master", None, commit_sha1)
            repo.refs.set_if_equals(b"refs/heads/feature", None, commit_sha2)
            repo.refs.set_if_equals(b"refs/tags/v1.0", None, commit_sha1)
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        # Verify refs exist
        self.assertEqual(
            self._run_git(["rev-parse", "refs/heads/feature"]).strip(), commit_sha2
        )

        # Delete a ref using dulwich
        with repo.refs.batch_update():
            repo.refs.set_if_equals(b"refs/heads/feature", commit_sha2, None)

        repo.close()

        # Verify git sees the ref as deleted (should fail)
        with self.assertRaises(AssertionError):
            self._run_git(["rev-parse", "refs/heads/feature"])

        # Verify other refs still exist
        self.assertEqual(
            self._run_git(["rev-parse", "refs/heads/master"]).strip(), commit_sha1
        )
        self.assertEqual(
            self._run_git(["symbolic-ref", "HEAD"]).strip(), b"refs/heads/master"
        )

    def test_large_number_of_refs_compatibility(self):
        """Test compatibility with large numbers of refs."""
        # Initialize repo
        self._run_git(["init", "--bare", "."])
        self._run_git(["config", "core.repositoryformatversion", "1"])
        self._run_git(["config", "extensions.refStorage", "reftable"])

        # Create base objects
        blob_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content"
        ).strip()
        tree_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob_sha.decode()}\tfile.txt\n".encode()
        ).strip()
        base_commit = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Base commit"]
        ).strip()

        # Create many refs efficiently
        repo = Repo(self.test_dir)

        with repo.refs.batch_update():
            # Create 50 branches
            for i in range(50):
                content = f"branch content {i}".encode()
                commit_sha = self._run_git(
                    ["commit-tree", tree_sha.decode(), "-m", f"Branch {i} commit"],
                    input=content,
                ).strip()
                repo.refs.set_if_equals(
                    f"refs/heads/branch{i:02d}".encode(), None, commit_sha
                )

            # Create 30 tags
            for i in range(30):
                repo.refs.set_if_equals(
                    f"refs/tags/tag{i:02d}".encode(), None, base_commit
                )

            # Set HEAD to first branch
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/branch00")

        repo.close()

        # Verify git can list all refs
        git_output = self._run_git(["show-ref"])
        ref_count = len([line for line in git_output.split(b"\n") if line.strip()])

        # Should have 50 branches + 30 tags = 80 refs
        self.assertGreaterEqual(ref_count, 80)

        # Verify HEAD points to correct branch
        head_target = self._run_git(["symbolic-ref", "HEAD"]).strip()
        self.assertEqual(head_target, b"refs/heads/branch00")

        # Verify some random refs resolve correctly
        branch10_sha = self._run_git(["rev-parse", "refs/heads/branch10"]).strip()
        self.assertEqual(len(branch10_sha), 40)  # Valid SHA

        tag05_sha = self._run_git(["rev-parse", "refs/tags/tag05"]).strip()
        self.assertEqual(tag05_sha, base_commit)

    def test_nested_symbolic_refs_compatibility(self):
        """Test compatibility with nested symbolic references."""
        # Initialize repo
        self._run_git(["init", "--bare", "."])
        self._run_git(["config", "core.repositoryformatversion", "1"])
        self._run_git(["config", "extensions.refStorage", "reftable"])

        # Create test objects
        blob_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content"
        ).strip()
        tree_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob_sha.decode()}\tfile.txt\n".encode()
        ).strip()
        commit_sha = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Test commit"]
        ).strip()

        repo = Repo(self.test_dir)

        # Create chain of symbolic refs
        with repo.refs.batch_update():
            # Real ref
            repo.refs.set_if_equals(b"refs/heads/master", None, commit_sha)

            # Chain: HEAD -> current -> master
            repo.refs.set_symbolic_ref(b"refs/heads/current", b"refs/heads/master")
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/current")

        repo.close()

        # Verify git can resolve the chain correctly
        head_sha = self._run_git(["rev-parse", "HEAD"]).strip()
        self.assertEqual(head_sha, commit_sha)

        # Verify intermediate symbolic ref
        current_target = self._run_git(["symbolic-ref", "refs/heads/current"]).strip()
        self.assertEqual(current_target, b"refs/heads/master")

        # Verify HEAD points to master (Git resolves symref chains for HEAD)
        head_target = self._run_git(["symbolic-ref", "HEAD"]).strip()
        self.assertEqual(head_target, b"refs/heads/master")

    def test_special_ref_names_compatibility(self):
        """Test compatibility with special and edge-case ref names."""
        # Initialize repo
        self._run_git(["init", "--bare", "."])
        self._run_git(["config", "core.repositoryformatversion", "1"])
        self._run_git(["config", "extensions.refStorage", "reftable"])

        # Create test objects
        blob_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content"
        ).strip()
        tree_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob_sha.decode()}\tfile.txt\n".encode()
        ).strip()
        commit_sha = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Test commit"]
        ).strip()

        repo = Repo(self.test_dir)

        # Test refs with special characters and structures
        special_refs = [
            b"refs/heads/feature/sub-feature",
            b"refs/heads/feature_with_underscores",
            b"refs/heads/UPPERCASE-BRANCH",
            b"refs/tags/v1.0.0-alpha.1",
            b"refs/tags/release/2023.12.31",
            b"refs/remotes/origin/main",
            b"refs/remotes/upstream/develop",
            b"refs/notes/commits",
        ]

        with repo.refs.batch_update():
            for ref_name in special_refs:
                repo.refs.set_if_equals(ref_name, None, commit_sha)

            # Set HEAD to a normal branch
            repo.refs.set_if_equals(b"refs/heads/master", None, commit_sha)
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        repo.close()

        # Verify git can read all special refs
        for ref_name in special_refs:
            ref_sha = self._run_git(["rev-parse", ref_name.decode()]).strip()
            self.assertEqual(ref_sha, commit_sha, f"Failed to resolve {ref_name}")

        # Verify show-ref includes all refs
        git_output = self._run_git(["show-ref"])
        for ref_name in special_refs:
            self.assertIn(ref_name, git_output, f"show-ref missing {ref_name}")

    def test_concurrent_ref_operations_compatibility(self):
        """Test compatibility with multiple ref operations."""
        # Initialize repo
        self._run_git(["init", "--bare", "."])
        self._run_git(["config", "core.repositoryformatversion", "1"])
        self._run_git(["config", "extensions.refStorage", "reftable"])

        # Create test objects
        blob_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content"
        ).strip()
        tree_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob_sha.decode()}\tfile.txt\n".encode()
        ).strip()

        commits = []
        for i in range(5):
            commit_sha = self._run_git(
                ["commit-tree", tree_sha.decode(), "-m", f"Commit {i}"]
            ).strip()
            commits.append(commit_sha)

        repo = Repo(self.test_dir)

        # Simulate concurrent operations with multiple batch updates
        # First batch: Create initial refs
        with repo.refs.batch_update():
            repo.refs.set_if_equals(b"refs/heads/master", None, commits[0])
            repo.refs.set_if_equals(b"refs/heads/develop", None, commits[1])
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        # Second batch: Update some refs and add new ones
        with repo.refs.batch_update():
            repo.refs.set_if_equals(
                b"refs/heads/master", commits[0], commits[2]
            )  # Update main
            repo.refs.set_if_equals(
                b"refs/heads/feature", None, commits[3]
            )  # Add feature
            repo.refs.set_if_equals(b"refs/tags/v1.0", None, commits[0])  # Add tag

        # Third batch: More complex operations
        with repo.refs.batch_update():
            repo.refs.set_if_equals(
                b"refs/heads/develop", commits[1], commits[4]
            )  # Update develop
            repo.refs.set_if_equals(
                b"refs/heads/feature", commits[3], None
            )  # Delete feature
            repo.refs.set_symbolic_ref(
                b"HEAD", b"refs/heads/develop"
            )  # Change HEAD target

        repo.close()

        # Verify final state with git
        main_sha = self._run_git(["rev-parse", "refs/heads/master"]).strip()
        self.assertEqual(main_sha, commits[2])

        develop_sha = self._run_git(["rev-parse", "refs/heads/develop"]).strip()
        self.assertEqual(develop_sha, commits[4])

        # Verify feature branch was deleted (should fail)
        with self.assertRaises(AssertionError):
            self._run_git(["rev-parse", "refs/heads/feature"])

        # Verify HEAD points to develop
        head_target = self._run_git(["symbolic-ref", "HEAD"]).strip()
        self.assertEqual(head_target, b"refs/heads/develop")

        # Verify tag exists
        tag_sha = self._run_git(["rev-parse", "refs/tags/v1.0"]).strip()
        self.assertEqual(tag_sha, commits[0])

    def test_reftable_gc_compatibility(self):
        """Test that reftables work correctly after git operations."""
        # Initialize repo
        self._run_git(["init", "--bare", "."])
        self._run_git(["config", "core.repositoryformatversion", "1"])
        self._run_git(["config", "extensions.refStorage", "reftable"])

        # Create initial state with dulwich
        blob_sha = self._run_git(
            ["hash-object", "-w", "--stdin"], input=b"test content"
        ).strip()
        tree_sha = self._run_git(
            ["mktree"], input=f"100644 blob {blob_sha.decode()}\tfile.txt\n".encode()
        ).strip()
        commit_sha = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "Initial commit"]
        ).strip()

        repo = Repo(self.test_dir)

        with repo.refs.batch_update():
            repo.refs.set_if_equals(b"refs/heads/master", None, commit_sha)
            repo.refs.set_symbolic_ref(b"HEAD", b"refs/heads/master")

        repo.close()

        # Perform git operations
        new_commit = self._run_git(
            ["commit-tree", tree_sha.decode(), "-m", "New commit"]
        ).strip()
        self._run_git(["update-ref", "refs/heads/master", new_commit.decode()])
        self._run_git(["update-ref", "refs/heads/branch2", new_commit.decode()])

        # Verify dulwich can still read after git modifications
        repo = Repo(self.test_dir)
        dulwich_refs = repo.get_refs()

        # Should be able to read git-modified refs
        self.assertEqual(dulwich_refs[b"refs/heads/master"], new_commit)
        self.assertEqual(dulwich_refs[b"refs/heads/branch2"], new_commit)

        # Should still resolve HEAD correctly
        head_sha = repo.refs[b"HEAD"]
        self.assertEqual(head_sha, new_commit)

        repo.close()


if __name__ == "__main__":
    import unittest

    unittest.main()
