# test_bisect.py -- tests for porcelain bisect
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

"""Tests for porcelain bisect functions."""

import os
import shutil
import tempfile

from dulwich import porcelain
from dulwich.objects import Tree
from dulwich.tests.utils import make_commit

from .. import TestCase


class BisectPorcelainTests(TestCase):
    """Tests for porcelain bisect functions."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.repo = porcelain.init(self.test_dir)

        # Create tree objects
        tree = Tree()
        self.repo.object_store.add_object(tree)

        # Create some commits with proper trees
        self.c1 = make_commit(id=b"1" * 40, message=b"initial commit", tree=tree.id)
        self.c2 = make_commit(
            id=b"2" * 40, message=b"second commit", parents=[b"1" * 40], tree=tree.id
        )
        self.c3 = make_commit(
            id=b"3" * 40, message=b"third commit", parents=[b"2" * 40], tree=tree.id
        )
        self.c4 = make_commit(
            id=b"4" * 40, message=b"fourth commit", parents=[b"3" * 40], tree=tree.id
        )

        # Add commits to object store
        for commit in [self.c1, self.c2, self.c3, self.c4]:
            self.repo.object_store.add_object(commit)

        # Set HEAD to latest commit
        self.repo.refs[b"HEAD"] = self.c4.id
        self.repo.refs[b"refs/heads/master"] = self.c4.id

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_bisect_start(self) -> None:
        """Test bisect_start porcelain function."""
        porcelain.bisect_start(self.test_dir)

        # Check that bisect state files exist
        self.assertTrue(
            os.path.exists(os.path.join(self.repo.controldir(), "BISECT_START"))
        )

    def test_bisect_bad_good(self) -> None:
        """Test marking commits as bad and good."""
        porcelain.bisect_start(self.test_dir)
        porcelain.bisect_bad(self.test_dir, self.c4.id.decode("ascii"))
        porcelain.bisect_good(self.test_dir, self.c1.id.decode("ascii"))

        # Check that refs were created
        self.assertTrue(
            os.path.exists(
                os.path.join(self.repo.controldir(), "refs", "bisect", "bad")
            )
        )
        self.assertTrue(
            os.path.exists(
                os.path.join(
                    self.repo.controldir(),
                    "refs",
                    "bisect",
                    f"good-{self.c1.id.decode('ascii')}",
                )
            )
        )

    def test_bisect_log(self) -> None:
        """Test getting bisect log."""
        porcelain.bisect_start(self.test_dir)
        porcelain.bisect_bad(self.test_dir, self.c4.id.decode("ascii"))
        porcelain.bisect_good(self.test_dir, self.c1.id.decode("ascii"))

        log = porcelain.bisect_log(self.test_dir)

        self.assertIn("git bisect start", log)
        self.assertIn("git bisect bad", log)
        self.assertIn("git bisect good", log)

    def test_bisect_reset(self) -> None:
        """Test resetting bisect state."""
        porcelain.bisect_start(self.test_dir)
        porcelain.bisect_bad(self.test_dir)
        porcelain.bisect_good(self.test_dir, self.c1.id.decode("ascii"))

        porcelain.bisect_reset(self.test_dir)

        # Check that bisect state files are removed
        self.assertFalse(
            os.path.exists(os.path.join(self.repo.controldir(), "BISECT_START"))
        )
        self.assertFalse(
            os.path.exists(os.path.join(self.repo.controldir(), "refs", "bisect"))
        )
