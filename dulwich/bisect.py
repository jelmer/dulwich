# bisect.py -- Git bisect algorithm implementation
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

"""Git bisect implementation."""

__all__ = ["BisectState"]

import os
from collections.abc import Sequence, Set

from dulwich.object_store import peel_sha
from dulwich.objects import Commit, ObjectID
from dulwich.refs import HEADREF, Ref
from dulwich.repo import Repo


class BisectState:
    """Manages the state of a bisect session."""

    def __init__(self, repo: Repo) -> None:
        """Initialize BisectState.

        Args:
            repo: Repository to perform bisect on
        """
        self.repo = repo
        self._bisect_dir = os.path.join(repo.controldir(), "BISECT_START")

    @property
    def is_active(self) -> bool:
        """Check if a bisect session is active."""
        return os.path.exists(self._bisect_dir)

    def start(
        self,
        bad: ObjectID | None = None,
        good: Sequence[ObjectID] | None = None,
        paths: Sequence[bytes] | None = None,
        no_checkout: bool = False,
        term_bad: str = "bad",
        term_good: str = "good",
    ) -> None:
        """Start a new bisect session.

        Args:
            bad: The bad commit SHA (defaults to HEAD)
            good: List of good commit SHAs
            paths: Optional paths to limit bisect to
            no_checkout: If True, don't checkout commits during bisect
            term_bad: Term to use for bad commits (default: "bad")
            term_good: Term to use for good commits (default: "good")
        """
        if self.is_active:
            raise ValueError("Bisect session already in progress")

        # Create bisect state directory
        bisect_refs_dir = os.path.join(self.repo.controldir(), "refs", "bisect")
        os.makedirs(bisect_refs_dir, exist_ok=True)

        # Store current branch/commit
        try:
            ref_chain, sha = self.repo.refs.follow(HEADREF)
            if sha is None:
                # No HEAD exists
                raise ValueError("Cannot start bisect: repository has no HEAD")
            # Use the first non-HEAD ref in the chain, or the SHA itself
            current_branch: Ref | ObjectID
            if len(ref_chain) > 1:
                current_branch = ref_chain[1]  # The actual branch ref
            else:
                current_branch = sha  # Detached HEAD
        except KeyError:
            # Detached HEAD
            try:
                current_branch = self.repo.head()
            except KeyError:
                # No HEAD exists - can't start bisect
                raise ValueError("Cannot start bisect: repository has no HEAD")

        # Write BISECT_START
        with open(self._bisect_dir, "wb") as f:
            f.write(current_branch)

        # Write BISECT_TERMS
        terms_file = os.path.join(self.repo.controldir(), "BISECT_TERMS")
        with open(terms_file, "w") as f:
            f.write(f"{term_bad}\n{term_good}\n")

        # Write BISECT_NAMES (paths)
        names_file = os.path.join(self.repo.controldir(), "BISECT_NAMES")
        with open(names_file, "w") as f:
            if paths:
                f.write(
                    "\n".join(path.decode("utf-8", "replace") for path in paths) + "\n"
                )
            else:
                f.write("\n")

        # Initialize BISECT_LOG
        log_file = os.path.join(self.repo.controldir(), "BISECT_LOG")
        with open(log_file, "w") as f:
            f.write("git bisect start\n")
            f.write("# status: waiting for both good and bad commits\n")

        # Mark bad commit if provided
        if bad is not None:
            self.mark_bad(bad)

        # Mark good commits if provided
        if good:
            for g in good:
                self.mark_good(g)

    def mark_bad(self, rev: ObjectID | None = None) -> ObjectID | None:
        """Mark a commit as bad.

        Args:
            rev: Commit SHA to mark as bad (defaults to HEAD)

        Returns:
            The SHA of the next commit to test, or None if bisect is complete
        """
        if not self.is_active:
            raise ValueError("No bisect session in progress")

        if rev is None:
            rev = self.repo.head()
        else:
            rev = peel_sha(self.repo.object_store, rev)[1].id

        # Write bad ref
        bad_ref_path = os.path.join(self.repo.controldir(), "refs", "bisect", "bad")
        with open(bad_ref_path, "wb") as f:
            f.write(rev + b"\n")

        # Update log
        self._append_to_log(
            f"# bad: [{rev.decode('ascii')}] {self._get_commit_subject(rev)}"
        )
        self._append_to_log(f"git bisect bad {rev.decode('ascii')}")

        return self._find_next_commit()

    def mark_good(self, rev: ObjectID | None = None) -> ObjectID | None:
        """Mark a commit as good.

        Args:
            rev: Commit SHA to mark as good (defaults to HEAD)

        Returns:
            The SHA of the next commit to test, or None if bisect is complete
        """
        if not self.is_active:
            raise ValueError("No bisect session in progress")

        if rev is None:
            rev = self.repo.head()
        else:
            rev = peel_sha(self.repo.object_store, rev)[1].id

        # Write good ref
        good_ref_path = os.path.join(
            self.repo.controldir(), "refs", "bisect", f"good-{rev.decode('ascii')}"
        )
        with open(good_ref_path, "wb") as f:
            f.write(rev + b"\n")

        # Update log
        self._append_to_log(
            f"# good: [{rev.decode('ascii')}] {self._get_commit_subject(rev)}"
        )
        self._append_to_log(f"git bisect good {rev.decode('ascii')}")

        return self._find_next_commit()

    def skip(self, revs: Sequence[ObjectID] | None = None) -> ObjectID | None:
        """Skip one or more commits.

        Args:
            revs: List of commits to skip (defaults to [HEAD])

        Returns:
            The SHA of the next commit to test, or None if bisect is complete
        """
        if not self.is_active:
            raise ValueError("No bisect session in progress")

        if revs is None:
            revs = [self.repo.head()]

        for rev in revs:
            rev = peel_sha(self.repo.object_store, rev)[1].id
            skip_ref_path = os.path.join(
                self.repo.controldir(), "refs", "bisect", f"skip-{rev.decode('ascii')}"
            )
            with open(skip_ref_path, "wb") as f:
                f.write(rev + b"\n")

            self._append_to_log(f"git bisect skip {rev.decode('ascii')}")

        return self._find_next_commit()

    def reset(self, commit: ObjectID | None = None) -> None:
        """Reset bisect state and return to original branch/commit.

        Args:
            commit: Optional commit to reset to (defaults to original branch/commit)
        """
        if not self.is_active:
            raise ValueError("No bisect session in progress")

        # Read original branch/commit
        with open(self._bisect_dir, "rb") as f:
            original = f.read().strip()

        # Clean up bisect files
        for filename in [
            "BISECT_START",
            "BISECT_TERMS",
            "BISECT_NAMES",
            "BISECT_LOG",
            "BISECT_EXPECTED_REV",
            "BISECT_ANCESTORS_OK",
        ]:
            filepath = os.path.join(self.repo.controldir(), filename)
            if os.path.exists(filepath):
                os.remove(filepath)

        # Clean up refs/bisect directory
        bisect_refs_dir = os.path.join(self.repo.controldir(), "refs", "bisect")
        if os.path.exists(bisect_refs_dir):
            for filename in os.listdir(bisect_refs_dir):
                os.remove(os.path.join(bisect_refs_dir, filename))
            os.rmdir(bisect_refs_dir)

        # Reset to target commit/branch
        if commit is None:
            if original.startswith(b"refs/"):
                # It's a branch reference - need to create a symbolic ref
                self.repo.refs.set_symbolic_ref(HEADREF, Ref(original))
            else:
                # It's a commit SHA
                self.repo.refs[HEADREF] = ObjectID(original)
        else:
            commit = peel_sha(self.repo.object_store, commit)[1].id
            self.repo.refs[HEADREF] = commit

    def get_log(self) -> str:
        """Get the bisect log."""
        if not self.is_active:
            raise ValueError("No bisect session in progress")

        log_file = os.path.join(self.repo.controldir(), "BISECT_LOG")
        with open(log_file) as f:
            return f.read()

    def replay(self, log_content: str) -> None:
        """Replay a bisect log.

        Args:
            log_content: The bisect log content to replay
        """
        # Parse and execute commands from log
        for line in log_content.splitlines():
            line = line.strip()
            if line.startswith("#") or not line:
                continue

            parts = line.split()
            if len(parts) < 3 or parts[0] != "git" or parts[1] != "bisect":
                continue

            cmd = parts[2]
            args = parts[3:] if len(parts) > 3 else []

            if cmd == "start":
                self.start()
            elif cmd == "bad":
                rev = ObjectID(args[0].encode("ascii")) if args else None
                self.mark_bad(rev)
            elif cmd == "good":
                rev = ObjectID(args[0].encode("ascii")) if args else None
                self.mark_good(rev)
            elif cmd == "skip":
                revs = [ObjectID(arg.encode("ascii")) for arg in args] if args else None
                self.skip(revs)

    def _find_next_commit(self) -> ObjectID | None:
        """Find the next commit to test using binary search.

        Returns:
            The SHA of the next commit to test, or None if bisect is complete
        """
        # Get bad commit
        bad_ref_path = os.path.join(self.repo.controldir(), "refs", "bisect", "bad")
        if not os.path.exists(bad_ref_path):
            self._append_to_log("# status: waiting for both good and bad commits")
            return None

        with open(bad_ref_path, "rb") as f:
            bad_sha = ObjectID(f.read().strip())

        # Get all good commits
        good_shas: list[ObjectID] = []
        bisect_refs_dir = os.path.join(self.repo.controldir(), "refs", "bisect")
        for filename in os.listdir(bisect_refs_dir):
            if filename.startswith("good-"):
                with open(os.path.join(bisect_refs_dir, filename), "rb") as f:
                    good_shas.append(ObjectID(f.read().strip()))

        if not good_shas:
            self._append_to_log(
                "# status: waiting for good commit(s), bad commit known"
            )
            return None

        # Get skip commits
        skip_shas: set[ObjectID] = set()
        for filename in os.listdir(bisect_refs_dir):
            if filename.startswith("skip-"):
                with open(os.path.join(bisect_refs_dir, filename), "rb") as f:
                    skip_shas.add(ObjectID(f.read().strip()))

        # Find commits between good and bad
        candidates = self._find_bisect_candidates(bad_sha, good_shas, skip_shas)

        if not candidates:
            # Bisect complete - the first bad commit is found
            self._append_to_log(
                f"# first bad commit: [{bad_sha.decode('ascii')}] "
                f"{self._get_commit_subject(bad_sha)}"
            )
            return None

        # Find midpoint
        mid_idx = len(candidates) // 2
        next_commit = candidates[mid_idx]

        # Write BISECT_EXPECTED_REV
        expected_file = os.path.join(self.repo.controldir(), "BISECT_EXPECTED_REV")
        with open(expected_file, "wb") as f:
            f.write(next_commit + b"\n")

        # Update status in log
        steps_remaining = self._estimate_steps(len(candidates))
        self._append_to_log(
            f"Bisecting: {len(candidates) - 1} revisions left to test after this "
            f"(roughly {steps_remaining} steps)"
        )
        self._append_to_log(
            f"[{next_commit.decode('ascii')}] {self._get_commit_subject(next_commit)}"
        )

        return next_commit

    def _find_bisect_candidates(
        self, bad_sha: ObjectID, good_shas: Sequence[ObjectID], skip_shas: Set[ObjectID]
    ) -> list[ObjectID]:
        """Find all commits between good and bad commits.

        Args:
            bad_sha: The bad commit SHA
            good_shas: List of good commit SHAs
            skip_shas: Set of commits to skip

        Returns:
            List of candidate commit SHAs in topological order
        """
        # Use git's graph walking to find commits
        # This is a simplified version - a full implementation would need
        # to handle merge commits properly
        candidates: list[ObjectID] = []
        visited: set[ObjectID] = set(good_shas)
        queue: list[ObjectID] = [bad_sha]

        while queue:
            sha = queue.pop(0)
            if sha in visited or sha in skip_shas:
                continue

            visited.add(sha)
            commit = self.repo.object_store[sha]

            # Don't include good commits
            if sha not in good_shas:
                candidates.append(sha)

            # Add parents to queue
            if isinstance(commit, Commit):
                for parent in commit.parents:
                    if parent not in visited:
                        queue.append(parent)

        # Remove the bad commit itself
        if bad_sha in candidates:
            candidates.remove(bad_sha)

        return candidates

    def _get_commit_subject(self, sha: ObjectID) -> str:
        """Get the subject line of a commit message."""
        obj = self.repo.object_store[sha]
        if isinstance(obj, Commit):
            message = obj.message.decode("utf-8", errors="replace")
            lines = message.split("\n")
            return lines[0] if lines else ""
        return ""

    def _append_to_log(self, line: str) -> None:
        """Append a line to the bisect log."""
        log_file = os.path.join(self.repo.controldir(), "BISECT_LOG")
        with open(log_file, "a") as f:
            f.write(line + "\n")

    def _estimate_steps(self, num_candidates: int) -> int:
        """Estimate the number of steps remaining in bisect."""
        if num_candidates <= 1:
            return 0
        steps = 0
        while num_candidates > 1:
            num_candidates //= 2
            steps += 1
        return steps
