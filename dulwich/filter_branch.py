# filter_branch.py - Git filter-branch functionality
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Git filter-branch implementation."""

from typing import Callable, Optional

from .object_store import BaseObjectStore
from .objects import Commit
from .refs import RefsContainer


class CommitFilter:
    """Filter for rewriting commits during filter-branch operations."""
    
    def __init__(
        self,
        object_store: BaseObjectStore,
        *,
        filter_fn: Optional[Callable[[Commit], Optional[dict[str, bytes]]]] = None,
        filter_author: Optional[Callable[[bytes], Optional[bytes]]] = None,
        filter_committer: Optional[Callable[[bytes], Optional[bytes]]] = None,
        filter_message: Optional[Callable[[bytes], Optional[bytes]]] = None,
    ):
        """Initialize a commit filter.
        
        Args:
          object_store: Object store to read from and write to
          filter_fn: Optional callable that takes a Commit object and returns
            a dict of updated fields (author, committer, message, etc.)
          filter_author: Optional callable that takes author bytes and returns
            updated author bytes or None to keep unchanged
          filter_committer: Optional callable that takes committer bytes and returns  
            updated committer bytes or None to keep unchanged
          filter_message: Optional callable that takes commit message bytes
            and returns updated message bytes
        """
        self.object_store = object_store
        self.filter_fn = filter_fn
        self.filter_author = filter_author
        self.filter_committer = filter_committer
        self.filter_message = filter_message
        self._old_to_new: dict[bytes, bytes] = {}
        self._processed: set[bytes] = set()
    
    def process_commit(self, commit_sha: bytes) -> Optional[bytes]:
        """Process a single commit, creating a filtered version.
        
        Args:
          commit_sha: SHA of the commit to process
          
        Returns:
          SHA of the new commit, or None if object not found
        """
        if commit_sha in self._processed:
            return self._old_to_new.get(commit_sha, commit_sha)
        
        self._processed.add(commit_sha)
        
        try:
            commit = self.object_store[commit_sha]
        except KeyError:
            # Object not found
            return None
        
        if not isinstance(commit, Commit):
            # Not a commit, return as-is
            self._old_to_new[commit_sha] = commit_sha
            return commit_sha
        
        # Process parents first
        new_parents = []
        for parent in commit.parents:
            new_parent = self.process_commit(parent)
            if new_parent:  # Skip None parents
                new_parents.append(new_parent)
        
        # Apply filters
        new_data = {}
        
        # Custom filter function takes precedence
        if self.filter_fn:
            filtered = self.filter_fn(commit)
            if filtered:
                new_data.update(filtered)
        
        # Apply specific filters
        if self.filter_author and "author" not in new_data:
            new_author = self.filter_author(commit.author)
            if new_author is not None:
                new_data["author"] = new_author
        
        if self.filter_committer and "committer" not in new_data:
            new_committer = self.filter_committer(commit.committer)
            if new_committer is not None:
                new_data["committer"] = new_committer
        
        if self.filter_message and "message" not in new_data:
            new_message = self.filter_message(commit.message)
            if new_message is not None:
                new_data["message"] = new_message
        
        # Create new commit if anything changed
        if new_data or new_parents != commit.parents:
            new_commit = Commit()
            new_commit.tree = commit.tree
            new_commit.parents = new_parents
            new_commit.author = new_data.get("author", commit.author)
            new_commit.author_time = new_data.get("author_time", commit.author_time)
            new_commit.author_timezone = new_data.get("author_timezone", commit.author_timezone)
            new_commit.committer = new_data.get("committer", commit.committer)
            new_commit.commit_time = new_data.get("commit_time", commit.commit_time)
            new_commit.commit_timezone = new_data.get("commit_timezone", commit.commit_timezone)
            new_commit.message = new_data.get("message", commit.message)
            new_commit.encoding = new_data.get("encoding", commit.encoding)
            
            # Copy extra fields
            if hasattr(commit, "_author_timezone_neg_utc"):
                new_commit._author_timezone_neg_utc = commit._author_timezone_neg_utc
            if hasattr(commit, "_commit_timezone_neg_utc"):
                new_commit._commit_timezone_neg_utc = commit._commit_timezone_neg_utc
            if hasattr(commit, "_extra"):
                new_commit._extra = list(commit._extra)
            if hasattr(commit, "_gpgsig"):
                new_commit._gpgsig = commit._gpgsig
            if hasattr(commit, "_mergetag"):
                new_commit._mergetag = list(commit._mergetag)
            
            # Store the new commit
            self.object_store.add_object(new_commit)
            self._old_to_new[commit_sha] = new_commit.id
            return new_commit.id
        else:
            # No changes, keep original
            self._old_to_new[commit_sha] = commit_sha
            return commit_sha
    
    def get_mapping(self) -> dict[bytes, bytes]:
        """Get the mapping of old commit SHAs to new commit SHAs.
        
        Returns:
          Dictionary mapping old SHAs to new SHAs
        """
        return self._old_to_new.copy()


def filter_refs(
    refs: RefsContainer,
    object_store: BaseObjectStore,
    ref_names: list[bytes],
    commit_filter: CommitFilter,
    *,
    keep_original: bool = True,
    force: bool = False,
) -> dict[bytes, bytes]:
    """Filter commits reachable from the given refs.
    
    Args:
      refs: Repository refs container
      object_store: Object store containing commits
      ref_names: List of ref names to filter
      commit_filter: CommitFilter instance to use
      keep_original: Keep original refs under refs/original/
      force: Force operation even if refs have been filtered before
      
    Returns:
      Dictionary mapping old commit SHAs to new commit SHAs
      
    Raises:
      ValueError: If refs have already been filtered and force is False
    """
    # Check if already filtered
    if keep_original and not force:
        for ref in ref_names:
            original_ref = b"refs/original/" + ref
            if original_ref in refs:
                raise ValueError(
                    f"Branch {ref.decode()} appears to have been filtered already. "
                    "Use force=True to force re-filtering."
                )
    
    # Process commits starting from refs
    for ref in ref_names:
        try:
            # Get the commit SHA for this ref
            if ref in refs:
                ref_sha = refs[ref]
                if ref_sha:
                    commit_filter.process_commit(ref_sha)
        except (KeyError, ValueError) as e:
            # Skip refs that can't be resolved
            import warnings
            warnings.warn(f"Could not process ref {ref!r}: {e}")
            continue
    
    # Update refs
    mapping = commit_filter.get_mapping()
    for ref in ref_names:
        try:
            if ref in refs:
                old_sha = refs[ref]
                new_sha = mapping.get(old_sha, old_sha)
                
                if old_sha != new_sha:
                    # Save original ref if requested
                    if keep_original:
                        original_ref = b"refs/original/" + ref
                        refs[original_ref] = old_sha
                    
                    # Update ref to new commit
                    refs[ref] = new_sha
        except KeyError as e:
            # Not a valid ref, skip updating
            import warnings
            warnings.warn(f"Could not update ref {ref!r}: {e}")
            continue
    
    return mapping