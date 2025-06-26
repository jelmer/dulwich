# test_objectspec.py -- tests for objectspec.py
# Copyright (C) 2014 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for revision spec parsing."""

# TODO: Round-trip parse-serialize-parse and serialize-parse-serialize tests.

from dulwich.objects import Blob, Commit, Tag
from dulwich.objectspec import (
    parse_commit,
    parse_commit_range,
    parse_object,
    parse_ref,
    parse_refs,
    parse_reftuple,
    parse_reftuples,
    parse_tree,
)
from dulwich.repo import MemoryRepo
from dulwich.tests.utils import build_commit_graph

from . import TestCase


class ParseObjectTests(TestCase):
    """Test parse_object."""

    def test_nonexistent(self) -> None:
        r = MemoryRepo()
        self.assertRaises(KeyError, parse_object, r, "thisdoesnotexist")

    def test_blob_by_sha(self) -> None:
        r = MemoryRepo()
        b = Blob.from_string(b"Blah")
        r.object_store.add_object(b)
        self.assertEqual(b, parse_object(r, b.id))


class ParseCommitRangeTests(TestCase):
    """Test parse_commit_range."""

    def test_nonexistent(self) -> None:
        r = MemoryRepo()
        self.assertRaises(KeyError, parse_commit_range, r, "thisdoesnotexist")

    def test_commit_by_sha(self) -> None:
        r = MemoryRepo()
        c1, c2, c3 = build_commit_graph(r.object_store, [[1], [2, 1], [3, 1, 2]])
        self.assertEqual([c1], list(parse_commit_range(r, c1.id)))


class ParseCommitTests(TestCase):
    """Test parse_commit."""

    def test_nonexistent(self) -> None:
        r = MemoryRepo()
        self.assertRaises(KeyError, parse_commit, r, "thisdoesnotexist")

    def test_commit_by_sha(self) -> None:
        r = MemoryRepo()
        [c1] = build_commit_graph(r.object_store, [[1]])
        self.assertEqual(c1, parse_commit(r, c1.id))

    def test_commit_by_short_sha(self) -> None:
        r = MemoryRepo()
        [c1] = build_commit_graph(r.object_store, [[1]])
        self.assertEqual(c1, parse_commit(r, c1.id[:10]))

    def test_annotated_tag(self) -> None:
        r = MemoryRepo()
        [c1] = build_commit_graph(r.object_store, [[1]])
        # Create an annotated tag pointing to the commit
        tag = Tag()
        tag.name = b"v1.0"
        tag.message = b"Test tag"
        tag.tag_time = 1234567890
        tag.tag_timezone = 0
        tag.object = (Commit, c1.id)
        tag.tagger = b"Test Tagger <test@example.com>"
        r.object_store.add_object(tag)
        # parse_commit should follow the tag to the commit
        self.assertEqual(c1, parse_commit(r, tag.id))

    def test_nested_tags(self) -> None:
        r = MemoryRepo()
        [c1] = build_commit_graph(r.object_store, [[1]])
        # Create an annotated tag pointing to the commit
        tag1 = Tag()
        tag1.name = b"v1.0"
        tag1.message = b"Test tag"
        tag1.tag_time = 1234567890
        tag1.tag_timezone = 0
        tag1.object = (Commit, c1.id)
        tag1.tagger = b"Test Tagger <test@example.com>"
        r.object_store.add_object(tag1)

        # Create another tag pointing to the first tag
        tag2 = Tag()
        tag2.name = b"v1.0-release"
        tag2.message = b"Release tag"
        tag2.tag_time = 1234567900
        tag2.tag_timezone = 0
        tag2.object = (Tag, tag1.id)
        tag2.tagger = b"Test Tagger <test@example.com>"
        r.object_store.add_object(tag2)

        # parse_commit should follow both tags to the commit
        self.assertEqual(c1, parse_commit(r, tag2.id))

    def test_tag_to_missing_commit(self) -> None:
        r = MemoryRepo()
        # Create a tag pointing to a non-existent commit
        missing_sha = b"1234567890123456789012345678901234567890"
        tag = Tag()
        tag.name = b"v1.0"
        tag.message = b"Test tag"
        tag.tag_time = 1234567890
        tag.tag_timezone = 0
        tag.object = (Commit, missing_sha)
        tag.tagger = b"Test Tagger <test@example.com>"
        r.object_store.add_object(tag)

        # Should raise KeyError for missing commit
        self.assertRaises(KeyError, parse_commit, r, tag.id)

    def test_tag_to_blob(self) -> None:
        r = MemoryRepo()
        # Create a blob
        blob = Blob.from_string(b"Test content")
        r.object_store.add_object(blob)

        # Create a tag pointing to the blob
        tag = Tag()
        tag.name = b"blob-tag"
        tag.message = b"Tag pointing to blob"
        tag.tag_time = 1234567890
        tag.tag_timezone = 0
        tag.object = (Blob, blob.id)
        tag.tagger = b"Test Tagger <test@example.com>"
        r.object_store.add_object(tag)

        # Should raise ValueError as it's not a commit
        self.assertRaises(ValueError, parse_commit, r, tag.id)


class ParseRefTests(TestCase):
    def test_nonexistent(self) -> None:
        r = {}
        self.assertRaises(KeyError, parse_ref, r, b"thisdoesnotexist")

    def test_ambiguous_ref(self) -> None:
        r = {
            b"ambig1": "bla",
            b"refs/ambig1": "bla",
            b"refs/tags/ambig1": "bla",
            b"refs/heads/ambig1": "bla",
            b"refs/remotes/ambig1": "bla",
            b"refs/remotes/ambig1/HEAD": "bla",
        }
        self.assertEqual(b"ambig1", parse_ref(r, b"ambig1"))

    def test_ambiguous_ref2(self) -> None:
        r = {
            b"refs/ambig2": "bla",
            b"refs/tags/ambig2": "bla",
            b"refs/heads/ambig2": "bla",
            b"refs/remotes/ambig2": "bla",
            b"refs/remotes/ambig2/HEAD": "bla",
        }
        self.assertEqual(b"refs/ambig2", parse_ref(r, b"ambig2"))

    def test_ambiguous_tag(self) -> None:
        r = {
            b"refs/tags/ambig3": "bla",
            b"refs/heads/ambig3": "bla",
            b"refs/remotes/ambig3": "bla",
            b"refs/remotes/ambig3/HEAD": "bla",
        }
        self.assertEqual(b"refs/tags/ambig3", parse_ref(r, b"ambig3"))

    def test_ambiguous_head(self) -> None:
        r = {
            b"refs/heads/ambig4": "bla",
            b"refs/remotes/ambig4": "bla",
            b"refs/remotes/ambig4/HEAD": "bla",
        }
        self.assertEqual(b"refs/heads/ambig4", parse_ref(r, b"ambig4"))

    def test_ambiguous_remote(self) -> None:
        r = {b"refs/remotes/ambig5": "bla", b"refs/remotes/ambig5/HEAD": "bla"}
        self.assertEqual(b"refs/remotes/ambig5", parse_ref(r, b"ambig5"))

    def test_ambiguous_remote_head(self) -> None:
        r = {b"refs/remotes/ambig6/HEAD": "bla"}
        self.assertEqual(b"refs/remotes/ambig6/HEAD", parse_ref(r, b"ambig6"))

    def test_heads_full(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(b"refs/heads/foo", parse_ref(r, b"refs/heads/foo"))

    def test_heads_partial(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(b"refs/heads/foo", parse_ref(r, b"heads/foo"))

    def test_tags_partial(self) -> None:
        r = {b"refs/tags/foo": "bla"}
        self.assertEqual(b"refs/tags/foo", parse_ref(r, b"tags/foo"))


class ParseRefsTests(TestCase):
    def test_nonexistent(self) -> None:
        r = {}
        self.assertRaises(KeyError, parse_refs, r, [b"thisdoesnotexist"])

    def test_head(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual([b"refs/heads/foo"], parse_refs(r, [b"foo"]))

    def test_full(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual([b"refs/heads/foo"], parse_refs(r, b"refs/heads/foo"))


class ParseReftupleTests(TestCase):
    def test_nonexistent(self) -> None:
        r = {}
        self.assertRaises(KeyError, parse_reftuple, r, r, b"thisdoesnotexist")

    def test_head(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            (b"refs/heads/foo", b"refs/heads/foo", False),
            parse_reftuple(r, r, b"foo"),
        )
        self.assertEqual(
            (b"refs/heads/foo", b"refs/heads/foo", True),
            parse_reftuple(r, r, b"+foo"),
        )
        self.assertEqual(
            (b"refs/heads/foo", b"refs/heads/foo", True),
            parse_reftuple(r, {}, b"+foo"),
        )
        self.assertEqual(
            (b"refs/heads/foo", b"refs/heads/foo", True),
            parse_reftuple(r, {}, b"foo", True),
        )

    def test_full(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            (b"refs/heads/foo", b"refs/heads/foo", False),
            parse_reftuple(r, r, b"refs/heads/foo"),
        )

    def test_no_left_ref(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            (None, b"refs/heads/foo", False),
            parse_reftuple(r, r, b":refs/heads/foo"),
        )

    def test_no_right_ref(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            (b"refs/heads/foo", None, False),
            parse_reftuple(r, r, b"refs/heads/foo:"),
        )

    def test_default_with_string(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            (b"refs/heads/foo", b"refs/heads/foo", False),
            parse_reftuple(r, r, "foo"),
        )


class ParseReftuplesTests(TestCase):
    def test_nonexistent(self) -> None:
        r = {}
        self.assertRaises(KeyError, parse_reftuples, r, r, [b"thisdoesnotexist"])

    def test_head(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            [(b"refs/heads/foo", b"refs/heads/foo", False)],
            parse_reftuples(r, r, [b"foo"]),
        )

    def test_full(self) -> None:
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            [(b"refs/heads/foo", b"refs/heads/foo", False)],
            parse_reftuples(r, r, b"refs/heads/foo"),
        )
        r = {b"refs/heads/foo": "bla"}
        self.assertEqual(
            [(b"refs/heads/foo", b"refs/heads/foo", True)],
            parse_reftuples(r, r, b"refs/heads/foo", True),
        )


class ParseTreeTests(TestCase):
    """Test parse_tree."""

    def test_nonexistent(self) -> None:
        r = MemoryRepo()
        self.assertRaises(KeyError, parse_tree, r, "thisdoesnotexist")

    def test_from_commit(self) -> None:
        r = MemoryRepo()
        c1, c2, c3 = build_commit_graph(r.object_store, [[1], [2, 1], [3, 1, 2]])
        self.assertEqual(r[c1.tree], parse_tree(r, c1.id))
        self.assertEqual(r[c1.tree], parse_tree(r, c1.tree))

    def test_from_ref(self) -> None:
        r = MemoryRepo()
        c1, c2, c3 = build_commit_graph(r.object_store, [[1], [2, 1], [3, 1, 2]])
        r.refs[b"refs/heads/foo"] = c1.id
        self.assertEqual(r[c1.tree], parse_tree(r, b"foo"))
