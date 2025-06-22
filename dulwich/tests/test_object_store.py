# test_object_store.py -- tests for object_store.py
# Copyright (C) 2008 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for the object store interface."""

from typing import TYPE_CHECKING, Any, Callable
from unittest import TestCase
from unittest.mock import patch

from dulwich.index import commit_tree
from dulwich.object_store import (
    MemoryObjectStore,
    PackBasedObjectStore,
    find_shallow,
    iter_tree_contents,
    peel_sha,
)
from dulwich.objects import (
    Blob,
    Tree,
    TreeEntry,
)
from dulwich.protocol import DEPTH_INFINITE

from .utils import make_commit, make_object, make_tag

if TYPE_CHECKING:
    from dulwich.object_store import BaseObjectStore


testobject = make_object(Blob, data=b"yummy data")


class ObjectStoreTests:
    store: "BaseObjectStore"

    assertEqual: Callable[[object, object], None]
    # For type checker purposes - actual implementation supports both styles
    assertRaises: Callable[..., Any]
    assertNotIn: Callable[[object, object], None]
    assertNotEqual: Callable[[object, object], None]
    assertIn: Callable[[object, object], None]
    assertTrue: Callable[[bool], None]
    assertFalse: Callable[[bool], None]

    def test_determine_wants_all(self) -> None:
        self.assertEqual(
            [b"1" * 40],
            self.store.determine_wants_all({b"refs/heads/foo": b"1" * 40}),
        )

    def test_determine_wants_all_zero(self) -> None:
        self.assertEqual(
            [], self.store.determine_wants_all({b"refs/heads/foo": b"0" * 40})
        )

    def test_determine_wants_all_depth(self) -> None:
        self.store.add_object(testobject)
        refs = {b"refs/heads/foo": testobject.id}
        with patch.object(self.store, "_get_depth", return_value=1) as m:
            self.assertEqual([], self.store.determine_wants_all(refs, depth=0))
            self.assertEqual(
                [testobject.id],
                self.store.determine_wants_all(refs, depth=DEPTH_INFINITE),
            )
            m.assert_not_called()

            self.assertEqual([], self.store.determine_wants_all(refs, depth=1))
            m.assert_called_with(testobject.id)
            self.assertEqual(
                [testobject.id], self.store.determine_wants_all(refs, depth=2)
            )

    def test_get_depth(self) -> None:
        self.assertEqual(0, self.store._get_depth(testobject.id))

        self.store.add_object(testobject)
        self.assertEqual(
            1, self.store._get_depth(testobject.id, get_parents=lambda x: [])
        )

        parent = make_object(Blob, data=b"parent data")
        self.store.add_object(parent)
        self.assertEqual(
            2,
            self.store._get_depth(
                testobject.id,
                get_parents=lambda x: [parent.id] if x == testobject else [],
            ),
        )

    def test_iter(self) -> None:
        self.assertEqual([], list(self.store))

    def test_get_nonexistant(self) -> None:
        self.assertRaises(KeyError, lambda: self.store[b"a" * 40])

    def test_contains_nonexistant(self) -> None:
        self.assertNotIn(b"a" * 40, self.store)

    def test_add_objects_empty(self) -> None:
        self.store.add_objects([])

    def test_add_commit(self) -> None:
        # TODO: Argh, no way to construct Git commit objects without
        # access to a serialized form.
        self.store.add_objects([])

    def test_store_resilience(self) -> None:
        """Test if updating an existing stored object doesn't erase the
        object from the store.
        """
        test_object = make_object(Blob, data=b"data")

        self.store.add_object(test_object)
        test_object_id = test_object.id
        test_object.data = test_object.data + b"update"
        stored_test_object = self.store[test_object_id]

        self.assertNotEqual(test_object.id, stored_test_object.id)
        self.assertEqual(stored_test_object.id, test_object_id)

    def test_add_object(self) -> None:
        self.store.add_object(testobject)
        self.assertEqual({testobject.id}, set(self.store))
        self.assertIn(testobject.id, self.store)
        r = self.store[testobject.id]
        self.assertEqual(r, testobject)

    def test_add_objects(self) -> None:
        data = [(testobject, "mypath")]
        self.store.add_objects(data)
        self.assertEqual({testobject.id}, set(self.store))
        self.assertIn(testobject.id, self.store)
        r = self.store[testobject.id]
        self.assertEqual(r, testobject)

    def test_tree_changes(self) -> None:
        blob_a1 = make_object(Blob, data=b"a1")
        blob_a2 = make_object(Blob, data=b"a2")
        blob_b = make_object(Blob, data=b"b")
        for blob in [blob_a1, blob_a2, blob_b]:
            self.store.add_object(blob)

        blobs_1 = [(b"a", blob_a1.id, 0o100644), (b"b", blob_b.id, 0o100644)]
        tree1_id = commit_tree(self.store, blobs_1)
        blobs_2 = [(b"a", blob_a2.id, 0o100644), (b"b", blob_b.id, 0o100644)]
        tree2_id = commit_tree(self.store, blobs_2)
        change_a = (
            (b"a", b"a"),
            (0o100644, 0o100644),
            (blob_a1.id, blob_a2.id),
        )
        self.assertEqual([change_a], list(self.store.tree_changes(tree1_id, tree2_id)))
        self.assertEqual(
            [
                change_a,
                ((b"b", b"b"), (0o100644, 0o100644), (blob_b.id, blob_b.id)),
            ],
            list(self.store.tree_changes(tree1_id, tree2_id, want_unchanged=True)),
        )

    def test_iter_tree_contents(self) -> None:
        blob_a = make_object(Blob, data=b"a")
        blob_b = make_object(Blob, data=b"b")
        blob_c = make_object(Blob, data=b"c")
        for blob in [blob_a, blob_b, blob_c]:
            self.store.add_object(blob)

        blobs = [
            (b"a", blob_a.id, 0o100644),
            (b"ad/b", blob_b.id, 0o100644),
            (b"ad/bd/c", blob_c.id, 0o100755),
            (b"ad/c", blob_c.id, 0o100644),
            (b"c", blob_c.id, 0o100644),
        ]
        tree_id = commit_tree(self.store, blobs)
        self.assertEqual(
            [TreeEntry(p, m, h) for (p, h, m) in blobs],
            list(iter_tree_contents(self.store, tree_id)),
        )
        self.assertEqual([], list(iter_tree_contents(self.store, None)))

    def test_iter_tree_contents_include_trees(self) -> None:
        blob_a = make_object(Blob, data=b"a")
        blob_b = make_object(Blob, data=b"b")
        blob_c = make_object(Blob, data=b"c")
        for blob in [blob_a, blob_b, blob_c]:
            self.store.add_object(blob)

        blobs = [
            (b"a", blob_a.id, 0o100644),
            (b"ad/b", blob_b.id, 0o100644),
            (b"ad/bd/c", blob_c.id, 0o100755),
        ]
        tree_id = commit_tree(self.store, blobs)
        tree = self.store[tree_id]
        assert isinstance(tree, Tree)
        tree_ad = self.store[tree[b"ad"][1]]
        assert isinstance(tree_ad, Tree)
        tree_bd = self.store[tree_ad[b"bd"][1]]

        expected = [
            TreeEntry(b"", 0o040000, tree_id),
            TreeEntry(b"a", 0o100644, blob_a.id),
            TreeEntry(b"ad", 0o040000, tree_ad.id),
            TreeEntry(b"ad/b", 0o100644, blob_b.id),
            TreeEntry(b"ad/bd", 0o040000, tree_bd.id),
            TreeEntry(b"ad/bd/c", 0o100755, blob_c.id),
        ]
        actual = iter_tree_contents(self.store, tree_id, include_trees=True)
        self.assertEqual(expected, list(actual))

    def make_tag(self, name, obj):
        tag = make_tag(obj, name=name)
        self.store.add_object(tag)
        return tag

    def test_peel_sha(self) -> None:
        self.store.add_object(testobject)
        tag1 = self.make_tag(b"1", testobject)
        tag2 = self.make_tag(b"2", testobject)
        tag3 = self.make_tag(b"3", testobject)
        for obj in [testobject, tag1, tag2, tag3]:
            self.assertEqual((obj, testobject), peel_sha(self.store, obj.id))

    def test_get_raw(self) -> None:
        self.store.add_object(testobject)
        self.assertEqual(
            (Blob.type_num, b"yummy data"), self.store.get_raw(testobject.id)
        )

    def test_close(self) -> None:
        # For now, just check that close doesn't barf.
        self.store.add_object(testobject)
        self.store.close()

    def test_iter_prefix(self) -> None:
        self.store.add_object(testobject)
        self.assertEqual([testobject.id], list(self.store.iter_prefix(testobject.id)))
        self.assertEqual(
            [testobject.id], list(self.store.iter_prefix(testobject.id[:10]))
        )

    def test_iterobjects_subset_all_present(self) -> None:
        """Test iterating over a subset of objects that all exist."""
        blob1 = make_object(Blob, data=b"blob 1 data")
        blob2 = make_object(Blob, data=b"blob 2 data")
        self.store.add_object(blob1)
        self.store.add_object(blob2)

        objects = list(self.store.iterobjects_subset([blob1.id, blob2.id]))
        self.assertEqual(2, len(objects))
        object_ids = set(o.id for o in objects)
        self.assertEqual(set([blob1.id, blob2.id]), object_ids)

    def test_iterobjects_subset_missing_not_allowed(self) -> None:
        """Test iterating with missing objects when not allowed."""
        blob1 = make_object(Blob, data=b"blob 1 data")
        self.store.add_object(blob1)
        missing_sha = b"1" * 40

        self.assertRaises(
            KeyError,
            lambda: list(self.store.iterobjects_subset([blob1.id, missing_sha])),
        )

    def test_iterobjects_subset_missing_allowed(self) -> None:
        """Test iterating with missing objects when allowed."""
        blob1 = make_object(Blob, data=b"blob 1 data")
        self.store.add_object(blob1)
        missing_sha = b"1" * 40

        objects = list(
            self.store.iterobjects_subset([blob1.id, missing_sha], allow_missing=True)
        )
        self.assertEqual(1, len(objects))
        self.assertEqual(blob1.id, objects[0].id)

    def test_iter_prefix_not_found(self) -> None:
        self.assertEqual([], list(self.store.iter_prefix(b"1" * 40)))


class PackBasedObjectStoreTests(ObjectStoreTests):
    store: PackBasedObjectStore

    def tearDown(self) -> None:
        for pack in self.store.packs:
            pack.close()

    def test_empty_packs(self) -> None:
        self.assertEqual([], list(self.store.packs))

    def test_pack_loose_objects(self) -> None:
        b1 = make_object(Blob, data=b"yummy data")
        self.store.add_object(b1)
        b2 = make_object(Blob, data=b"more yummy data")
        self.store.add_object(b2)
        b3 = make_object(Blob, data=b"even more yummy data")
        b4 = make_object(Blob, data=b"and more yummy data")
        self.store.add_objects([(b3, None), (b4, None)])
        self.assertEqual({b1.id, b2.id, b3.id, b4.id}, set(self.store))
        self.assertEqual(1, len(self.store.packs))
        self.assertEqual(2, self.store.pack_loose_objects())
        self.assertNotEqual([], list(self.store.packs))
        self.assertEqual(0, self.store.pack_loose_objects())

    def test_repack(self) -> None:
        b1 = make_object(Blob, data=b"yummy data")
        self.store.add_object(b1)
        b2 = make_object(Blob, data=b"more yummy data")
        self.store.add_object(b2)
        b3 = make_object(Blob, data=b"even more yummy data")
        b4 = make_object(Blob, data=b"and more yummy data")
        self.store.add_objects([(b3, None), (b4, None)])
        b5 = make_object(Blob, data=b"and more data")
        b6 = make_object(Blob, data=b"and some more data")
        self.store.add_objects([(b5, None), (b6, None)])
        self.assertEqual({b1.id, b2.id, b3.id, b4.id, b5.id, b6.id}, set(self.store))
        self.assertEqual(2, len(self.store.packs))
        self.assertEqual(6, self.store.repack())
        self.assertEqual(1, len(self.store.packs))
        self.assertEqual(0, self.store.pack_loose_objects())

    def test_repack_existing(self) -> None:
        b1 = make_object(Blob, data=b"yummy data")
        self.store.add_object(b1)
        b2 = make_object(Blob, data=b"more yummy data")
        self.store.add_object(b2)
        self.store.add_objects([(b1, None), (b2, None)])
        self.store.add_objects([(b2, None)])
        self.assertEqual({b1.id, b2.id}, set(self.store))
        self.assertEqual(2, len(self.store.packs))
        self.assertEqual(2, self.store.repack())
        self.assertEqual(1, len(self.store.packs))
        self.assertEqual(0, self.store.pack_loose_objects())

        self.assertEqual({b1.id, b2.id}, set(self.store))
        self.assertEqual(1, len(self.store.packs))
        self.assertEqual(2, self.store.repack())
        self.assertEqual(1, len(self.store.packs))
        self.assertEqual(0, self.store.pack_loose_objects())

    def test_repack_with_exclude(self) -> None:
        """Test repacking while excluding specific objects."""
        b1 = make_object(Blob, data=b"yummy data")
        self.store.add_object(b1)
        b2 = make_object(Blob, data=b"more yummy data")
        self.store.add_object(b2)
        b3 = make_object(Blob, data=b"even more yummy data")
        b4 = make_object(Blob, data=b"and more yummy data")
        self.store.add_objects([(b3, None), (b4, None)])

        self.assertEqual({b1.id, b2.id, b3.id, b4.id}, set(self.store))
        self.assertEqual(1, len(self.store.packs))

        # Repack, excluding b2 and b3
        excluded = {b2.id, b3.id}
        self.assertEqual(2, self.store.repack(exclude=excluded))

        # Should have repacked only b1 and b4
        self.assertEqual(1, len(self.store.packs))
        self.assertIn(b1.id, self.store)
        self.assertNotIn(b2.id, self.store)
        self.assertNotIn(b3.id, self.store)
        self.assertIn(b4.id, self.store)

    def test_delete_loose_object(self) -> None:
        """Test deleting loose objects."""
        b1 = make_object(Blob, data=b"test data")
        self.store.add_object(b1)

        # Verify it's loose
        self.assertTrue(self.store.contains_loose(b1.id))
        self.assertIn(b1.id, self.store)

        # Delete it
        self.store.delete_loose_object(b1.id)

        # Verify it's gone
        self.assertFalse(self.store.contains_loose(b1.id))
        self.assertNotIn(b1.id, self.store)


class FindShallowTests(TestCase):
    def setUp(self):
        super().setUp()
        self._store = MemoryObjectStore()

    def make_commit(self, **attrs):
        commit = make_commit(**attrs)
        self._store.add_object(commit)
        return commit

    def make_linear_commits(self, n, message=b""):
        commits = []
        parents = []
        for _ in range(n):
            commits.append(self.make_commit(parents=parents, message=message))
            parents = [commits[-1].id]
        return commits

    def assertSameElements(self, expected, actual):
        self.assertEqual(set(expected), set(actual))

    def test_linear(self):
        c1, c2, c3 = self.make_linear_commits(3)

        self.assertEqual((set([c3.id]), set([])), find_shallow(self._store, [c3.id], 1))
        self.assertEqual(
            (set([c2.id]), set([c3.id])),
            find_shallow(self._store, [c3.id], 2),
        )
        self.assertEqual(
            (set([c1.id]), set([c2.id, c3.id])),
            find_shallow(self._store, [c3.id], 3),
        )
        self.assertEqual(
            (set([]), set([c1.id, c2.id, c3.id])),
            find_shallow(self._store, [c3.id], 4),
        )

    def test_multiple_independent(self):
        a = self.make_linear_commits(2, message=b"a")
        b = self.make_linear_commits(2, message=b"b")
        c = self.make_linear_commits(2, message=b"c")
        heads = [a[1].id, b[1].id, c[1].id]

        self.assertEqual(
            (set([a[0].id, b[0].id, c[0].id]), set(heads)),
            find_shallow(self._store, heads, 2),
        )

    def test_multiple_overlapping(self):
        # Create the following commit tree:
        # 1--2
        #  \
        #   3--4
        c1, c2 = self.make_linear_commits(2)
        c3 = self.make_commit(parents=[c1.id])
        c4 = self.make_commit(parents=[c3.id])

        # 1 is shallow along the path from 4, but not along the path from 2.
        self.assertEqual(
            (set([c1.id]), set([c1.id, c2.id, c3.id, c4.id])),
            find_shallow(self._store, [c2.id, c4.id], 3),
        )

    def test_merge(self):
        c1 = self.make_commit()
        c2 = self.make_commit()
        c3 = self.make_commit(parents=[c1.id, c2.id])

        self.assertEqual(
            (set([c1.id, c2.id]), set([c3.id])),
            find_shallow(self._store, [c3.id], 2),
        )

    def test_tag(self):
        c1, c2 = self.make_linear_commits(2)
        tag = make_tag(c2, name=b"tag")
        self._store.add_object(tag)

        self.assertEqual(
            (set([c1.id]), set([c2.id])),
            find_shallow(self._store, [tag.id], 2),
        )
