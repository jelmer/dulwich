# test_clone_no_tags.py -- tests for porcelain.clone(no_tags=...)
# Copyright (C) 2026 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for cloning without tags."""

import os
import shutil
import tempfile

from dulwich import porcelain
from dulwich.client import LocalGitClient
from dulwich.refs import HEADREF

from .. import TestCase


class CloneNoTagsTests(TestCase):
    """`porcelain.clone(no_tags=True)` and the config it records."""

    def setUp(self) -> None:
        super().setUp()
        self.source_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.source_path)
        source = porcelain.init(self.source_path)
        self.addCleanup(source.close)
        path = os.path.join(self.source_path, "a")
        with open(path, "w") as f:
            f.write("contents")
        porcelain.add(source, paths=[path])
        porcelain.commit(
            source,
            message=b"initial",
            author=b"Test <test@example.com>",
            committer=b"Test <test@example.com>",
        )
        porcelain.tag_create(source, b"v1.0")
        # Annotated, so there is a tag *object* whose presence in the clone
        # would prove the objects travelled even when the ref did not.
        porcelain.tag_create(
            source,
            b"v2.0",
            author=b"Test <test@example.com>",
            message=b"release two",
            annotated=True,
        )

    def _clone(self, **kwargs):
        target = os.path.join(tempfile.mkdtemp(), "clone")
        self.addCleanup(shutil.rmtree, os.path.dirname(target))
        repo = porcelain.clone(
            self.source_path, target, errstream=open(os.devnull, "wb"), **kwargs
        )
        self.addCleanup(repo.close)
        return repo

    def _tag_opt(self, repo):
        try:
            return repo.get_config().get((b"remote", b"origin"), b"tagOpt")
        except KeyError:
            return None

    def test_tags_are_cloned_by_default(self) -> None:
        """The baseline the flag opts out of."""
        repo = self._clone()
        self.assertIn(b"refs/tags/v1.0", repo.get_refs())
        self.assertIn(b"refs/tags/v2.0", repo.get_refs())
        self.assertIs(None, self._tag_opt(repo))

    def test_no_tags_clones_no_tags(self) -> None:
        repo = self._clone(no_tags=True)
        self.assertEqual(
            [], [ref for ref in repo.get_refs() if ref.startswith(b"refs/tags/")]
        )

    def test_no_tags_still_clones_the_branch(self) -> None:
        """Narrowing the ref prefix must not narrow away the point of the clone."""
        repo = self._clone(no_tags=True)
        self.assertIn(b"refs/heads/master", repo.get_refs())
        self.assertIn(b"refs/remotes/origin/master", repo.get_refs())

    def test_no_tags_leaves_head_resolvable(self) -> None:
        """HEAD is requested explicitly, not covered by the branch prefix."""
        repo = self._clone(no_tags=True)
        self.assertIn(HEADREF, repo.get_refs())
        self.assertEqual(b"refs/heads/master", repo.refs.follow(HEADREF)[0][-1])

    def test_no_tags_records_tag_opt(self) -> None:
        """Without this, the next bare `fetch` undoes the clone.

        git writes `remote.<name>.tagOpt = --no-tags` for the same reason, so
        the choice survives the command that made it.
        """
        self.assertEqual(b"--no-tags", self._tag_opt(self._clone(no_tags=True)))

    def test_the_tagged_objects_are_not_fetched(self) -> None:
        """Skipping the refs is only half of it; the objects must stay behind.

        A tag object present in the clone would mean the transfer happened and
        only the ref was hidden.
        """
        with porcelain.open_repo_closing(self.source_path) as repo:
            # v2.0 is annotated, so this is the sha of a Tag object rather
            # than of the commit it points at.
            tag_sha = repo.refs[b"refs/tags/v2.0"]
            head_sha = repo.refs[b"refs/heads/master"]
            self.assertNotEqual(tag_sha, head_sha)

        clone = self._clone(no_tags=True)
        # The commit is reachable from the branch, so it must be there; the
        # tag object pointing at it must not.
        self.assertIn(head_sha, clone.object_store)
        self.assertNotIn(tag_sha, clone.object_store)


class LocalClientRefPrefixTests(TestCase):
    """`LocalGitClient.fetch` must honour ref_prefix.

    Its docstring says filtering is done "client side otherwise", but it was
    passing the argument straight through and ignoring it, so every local
    fetch returned every ref. `--no-tags` is the first caller that notices.
    """

    def setUp(self) -> None:
        super().setUp()
        self.source_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.source_path)
        source = porcelain.init(self.source_path)
        self.addCleanup(source.close)
        path = os.path.join(self.source_path, "a")
        with open(path, "w") as f:
            f.write("contents")
        porcelain.add(source, paths=[path])
        porcelain.commit(
            source,
            message=b"initial",
            author=b"Test <test@example.com>",
            committer=b"Test <test@example.com>",
        )
        porcelain.tag_create(source, b"v1.0")

    def _fetch(self, ref_prefix):
        target_path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target_path)
        target = porcelain.init(target_path)
        self.addCleanup(target.close)
        return LocalGitClient().fetch(self.source_path, target, ref_prefix=ref_prefix)

    def test_no_prefix_returns_every_ref(self) -> None:
        refs = self._fetch(None).refs
        self.assertIn(b"refs/heads/master", refs)
        self.assertIn(b"refs/tags/v1.0", refs)

    def test_a_prefix_filters_the_refs(self) -> None:
        refs = self._fetch([b"refs/heads/"]).refs
        self.assertIn(b"refs/heads/master", refs)
        self.assertNotIn(b"refs/tags/v1.0", refs)

    def test_a_prefix_matching_nothing_returns_nothing(self) -> None:
        self.assertEqual({}, dict(self._fetch([b"refs/nope/"]).refs))
