# test_bundle_uri.py -- tests for bundle URI support
# Copyright (C) 2024 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for bundle URI support."""

from dulwich.bundle_uri import (
    BundleList,
    BundleListEntry,
    BundleURIError,
    _filter_bundle_entries,
    _is_absolute_uri,
    _resolve_relative_uri,
    parse_bundle_list,
)

from . import TestCase


class BundleURIParsingTests(TestCase):
    def test_parse_simple_bundle_list(self) -> None:
        """Test parsing a simple bundle list."""
        data = b"""[bundle]
    version = 1
    mode = all

[bundle "bundle1"]
    uri = https://example.com/bundle1.bundle

[bundle "bundle2"]
    uri = https://example.com/bundle2.bundle
"""
        bundle_list = parse_bundle_list(data)

        self.assertEqual(bundle_list.version, 1)
        self.assertEqual(bundle_list.mode, "all")
        self.assertIsNone(bundle_list.heuristic)
        self.assertEqual(len(bundle_list.entries), 2)

        # Check entries
        entry_ids = {e.id for e in bundle_list.entries}
        self.assertIn("bundle1", entry_ids)
        self.assertIn("bundle2", entry_ids)

    def test_parse_bundle_list_with_heuristic(self) -> None:
        """Test parsing a bundle list with creationToken heuristic."""
        data = b"""[bundle]
    version = 1
    mode = all
    heuristic = creationToken

[bundle "daily-2024"]
    uri = https://example.com/daily.bundle
    creationToken = 1700000000

[bundle "weekly-2024"]
    uri = https://example.com/weekly.bundle
    creationToken = 1699000000
"""
        bundle_list = parse_bundle_list(data)

        self.assertEqual(bundle_list.heuristic, "creationToken")
        self.assertEqual(len(bundle_list.entries), 2)

        # Find daily entry
        daily = next(e for e in bundle_list.entries if e.id == "daily-2024")
        self.assertEqual(daily.creation_token, 1700000000)

        weekly = next(e for e in bundle_list.entries if e.id == "weekly-2024")
        self.assertEqual(weekly.creation_token, 1699000000)

    def test_parse_bundle_list_any_mode(self) -> None:
        """Test parsing a bundle list with 'any' mode."""
        data = b"""[bundle]
    version = 1
    mode = any

[bundle "eastus"]
    uri = https://eastus.example.com/bundle.bundle
    location = eastus

[bundle "westus"]
    uri = https://westus.example.com/bundle.bundle
    location = westus
"""
        bundle_list = parse_bundle_list(data)

        self.assertEqual(bundle_list.mode, "any")
        self.assertEqual(len(bundle_list.entries), 2)

        eastus = next(e for e in bundle_list.entries if e.id == "eastus")
        self.assertEqual(eastus.location, "eastus")

    def test_parse_bundle_list_with_filter(self) -> None:
        """Test parsing a bundle list with object filters."""
        data = b"""[bundle]
    version = 1
    mode = all

[bundle "full"]
    uri = https://example.com/full.bundle

[bundle "blobless"]
    uri = https://example.com/blobless.bundle
    filter = blob:none
"""
        bundle_list = parse_bundle_list(data)

        full = next(e for e in bundle_list.entries if e.id == "full")
        self.assertIsNone(full.filter)

        blobless = next(e for e in bundle_list.entries if e.id == "blobless")
        self.assertEqual(blobless.filter, "blob:none")

    def test_parse_bundle_list_missing_version(self) -> None:
        """Test that missing version raises error."""
        data = b"""[bundle]
    mode = all
"""
        with self.assertRaises(BundleURIError) as cm:
            parse_bundle_list(data)
        self.assertIn("version", str(cm.exception))

    def test_parse_bundle_list_missing_mode(self) -> None:
        """Test that missing mode raises error."""
        data = b"""[bundle]
    version = 1
"""
        with self.assertRaises(BundleURIError) as cm:
            parse_bundle_list(data)
        self.assertIn("mode", str(cm.exception))

    def test_parse_bundle_list_invalid_mode(self) -> None:
        """Test that invalid mode raises error."""
        data = b"""[bundle]
    version = 1
    mode = invalid
"""
        with self.assertRaises(BundleURIError) as cm:
            parse_bundle_list(data)
        self.assertIn("mode", str(cm.exception))

    def test_parse_bundle_list_unsupported_version(self) -> None:
        """Test that unsupported version raises error."""
        data = b"""[bundle]
    version = 2
    mode = all
"""
        with self.assertRaises(BundleURIError) as cm:
            parse_bundle_list(data)
        self.assertIn("version", str(cm.exception).lower())

    def test_parse_bundle_list_relative_uri(self) -> None:
        """Test parsing bundle list with relative URIs."""
        data = b"""[bundle]
    version = 1
    mode = all

[bundle "relative"]
    uri = daily.bundle

[bundle "absolute-path"]
    uri = /bundles/weekly.bundle
"""
        base_uri = "https://example.com/git/repo/"
        bundle_list = parse_bundle_list(data, base_uri=base_uri)

        relative = next(e for e in bundle_list.entries if e.id == "relative")
        self.assertEqual(relative.uri, "https://example.com/git/repo/daily.bundle")

        absolute_path = next(e for e in bundle_list.entries if e.id == "absolute-path")
        self.assertEqual(absolute_path.uri, "https://example.com/bundles/weekly.bundle")


class URIResolutionTests(TestCase):
    def test_is_absolute_uri(self) -> None:
        """Test absolute URI detection."""
        self.assertTrue(_is_absolute_uri("https://example.com/bundle.bundle"))
        self.assertTrue(_is_absolute_uri("http://example.com/bundle.bundle"))
        self.assertFalse(_is_absolute_uri("/bundles/bundle.bundle"))
        self.assertFalse(_is_absolute_uri("daily.bundle"))
        self.assertFalse(_is_absolute_uri("../bundle.bundle"))

    def test_resolve_relative_uri_simple(self) -> None:
        """Test resolving simple relative URIs."""
        base = "https://example.com/git/repo/"
        self.assertEqual(
            _resolve_relative_uri(base, "daily.bundle"),
            "https://example.com/git/repo/daily.bundle",
        )

    def test_resolve_relative_uri_absolute_path(self) -> None:
        """Test resolving absolute path URIs."""
        base = "https://example.com/git/repo/"
        self.assertEqual(
            _resolve_relative_uri(base, "/bundles/daily.bundle"),
            "https://example.com/bundles/daily.bundle",
        )

    def test_resolve_relative_uri_parent(self) -> None:
        """Test resolving parent-relative URIs."""
        base = "https://example.com/git/repo/"
        self.assertEqual(
            _resolve_relative_uri(base, "../other/daily.bundle"),
            "https://example.com/git/other/daily.bundle",
        )


class BundleFilteringTests(TestCase):
    def test_filter_entries_full_clone(self) -> None:
        """Test filtering entries for full clone (no filter)."""
        entries = [
            BundleListEntry(id="full", uri="https://example.com/full.bundle"),
            BundleListEntry(
                id="blobless",
                uri="https://example.com/blobless.bundle",
                filter="blob:none",
            ),
        ]

        filtered = _filter_bundle_entries(entries, filter_spec=None)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].id, "full")

    def test_filter_entries_partial_clone(self) -> None:
        """Test filtering entries for partial clone with filter."""
        entries = [
            BundleListEntry(id="full", uri="https://example.com/full.bundle"),
            BundleListEntry(
                id="blobless",
                uri="https://example.com/blobless.bundle",
                filter="blob:none",
            ),
            BundleListEntry(
                id="treeless",
                uri="https://example.com/treeless.bundle",
                filter="tree:0",
            ),
        ]

        filtered = _filter_bundle_entries(entries, filter_spec="blob:none")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].id, "blobless")

    def test_filter_entries_no_match(self) -> None:
        """Test filtering when no entries match."""
        entries = [
            BundleListEntry(
                id="blobless",
                uri="https://example.com/blobless.bundle",
                filter="blob:none",
            ),
        ]

        filtered = _filter_bundle_entries(entries, filter_spec=None)
        self.assertEqual(len(filtered), 0)


class BundleListEntryTests(TestCase):
    def test_entry_defaults(self) -> None:
        """Test BundleListEntry default values."""
        entry = BundleListEntry(id="test", uri="https://example.com/test.bundle")
        self.assertEqual(entry.id, "test")
        self.assertEqual(entry.uri, "https://example.com/test.bundle")
        self.assertIsNone(entry.filter)
        self.assertIsNone(entry.creation_token)
        self.assertIsNone(entry.location)

    def test_entry_all_fields(self) -> None:
        """Test BundleListEntry with all fields set."""
        entry = BundleListEntry(
            id="test",
            uri="https://example.com/test.bundle",
            filter="blob:none",
            creation_token=1700000000,
            location="eastus",
        )
        self.assertEqual(entry.id, "test")
        self.assertEqual(entry.uri, "https://example.com/test.bundle")
        self.assertEqual(entry.filter, "blob:none")
        self.assertEqual(entry.creation_token, 1700000000)
        self.assertEqual(entry.location, "eastus")


class BundleListTests(TestCase):
    def test_bundle_list_defaults(self) -> None:
        """Test BundleList default values."""
        bundle_list = BundleList()
        self.assertEqual(bundle_list.version, 1)
        self.assertEqual(bundle_list.mode, "all")
        self.assertIsNone(bundle_list.heuristic)
        self.assertEqual(bundle_list.entries, [])

    def test_bundle_list_custom(self) -> None:
        """Test BundleList with custom values."""
        entries = [
            BundleListEntry(id="test", uri="https://example.com/test.bundle"),
        ]
        bundle_list = BundleList(
            version=1,
            mode="any",
            heuristic="creationToken",
            entries=entries,
        )
        self.assertEqual(bundle_list.version, 1)
        self.assertEqual(bundle_list.mode, "any")
        self.assertEqual(bundle_list.heuristic, "creationToken")
        self.assertEqual(len(bundle_list.entries), 1)
