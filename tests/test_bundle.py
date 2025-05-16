# test_bundle.py -- tests for bundle
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for bundle support."""

import os
import tempfile
from io import BytesIO

from dulwich.bundle import Bundle, read_bundle, write_bundle
from dulwich.pack import PackData, write_pack_objects

from . import TestCase


class BundleTests(TestCase):
    def setUp(self):
        super().setUp()
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(os.rmdir, self.tempdir)

    def test_bundle_repr(self) -> None:
        """Test the Bundle.__repr__ method."""
        bundle = Bundle()
        bundle.version = 3
        bundle.capabilities = {"foo": "bar"}
        bundle.prerequisites = [(b"cc" * 20, "comment")]
        bundle.references = {b"refs/heads/master": b"ab" * 20}

        # Create a simple pack data
        b = BytesIO()
        write_pack_objects(b.write, [])
        b.seek(0)
        bundle.pack_data = PackData.from_file(b)

        # Check the repr output
        rep = repr(bundle)
        self.assertIn("Bundle(version=3", rep)
        self.assertIn("capabilities={'foo': 'bar'}", rep)
        self.assertIn("prerequisites=[(", rep)
        self.assertIn("references={", rep)

    def test_bundle_equality(self) -> None:
        """Test the Bundle.__eq__ method."""
        # Create two identical bundles
        bundle1 = Bundle()
        bundle1.version = 3
        bundle1.capabilities = {"foo": "bar"}
        bundle1.prerequisites = [(b"cc" * 20, "comment")]
        bundle1.references = {b"refs/heads/master": b"ab" * 20}

        b1 = BytesIO()
        write_pack_objects(b1.write, [])
        b1.seek(0)
        bundle1.pack_data = PackData.from_file(b1)

        bundle2 = Bundle()
        bundle2.version = 3
        bundle2.capabilities = {"foo": "bar"}
        bundle2.prerequisites = [(b"cc" * 20, "comment")]
        bundle2.references = {b"refs/heads/master": b"ab" * 20}

        b2 = BytesIO()
        write_pack_objects(b2.write, [])
        b2.seek(0)
        bundle2.pack_data = PackData.from_file(b2)

        # Test equality
        self.assertEqual(bundle1, bundle2)

        # Test inequality by changing different attributes
        bundle3 = Bundle()
        bundle3.version = 2  # Different version
        bundle3.capabilities = {"foo": "bar"}
        bundle3.prerequisites = [(b"cc" * 20, "comment")]
        bundle3.references = {b"refs/heads/master": b"ab" * 20}
        b3 = BytesIO()
        write_pack_objects(b3.write, [])
        b3.seek(0)
        bundle3.pack_data = PackData.from_file(b3)
        self.assertNotEqual(bundle1, bundle3)

        bundle4 = Bundle()
        bundle4.version = 3
        bundle4.capabilities = {"different": "value"}  # Different capabilities
        bundle4.prerequisites = [(b"cc" * 20, "comment")]
        bundle4.references = {b"refs/heads/master": b"ab" * 20}
        b4 = BytesIO()
        write_pack_objects(b4.write, [])
        b4.seek(0)
        bundle4.pack_data = PackData.from_file(b4)
        self.assertNotEqual(bundle1, bundle4)

        bundle5 = Bundle()
        bundle5.version = 3
        bundle5.capabilities = {"foo": "bar"}
        bundle5.prerequisites = [(b"dd" * 20, "different")]  # Different prerequisites
        bundle5.references = {b"refs/heads/master": b"ab" * 20}
        b5 = BytesIO()
        write_pack_objects(b5.write, [])
        b5.seek(0)
        bundle5.pack_data = PackData.from_file(b5)
        self.assertNotEqual(bundle1, bundle5)

        bundle6 = Bundle()
        bundle6.version = 3
        bundle6.capabilities = {"foo": "bar"}
        bundle6.prerequisites = [(b"cc" * 20, "comment")]
        bundle6.references = {
            b"refs/heads/different": b"ab" * 20
        }  # Different references
        b6 = BytesIO()
        write_pack_objects(b6.write, [])
        b6.seek(0)
        bundle6.pack_data = PackData.from_file(b6)
        self.assertNotEqual(bundle1, bundle6)

        # Test inequality with different type
        self.assertNotEqual(bundle1, "not a bundle")

    def test_read_bundle_v2(self) -> None:
        """Test reading a v2 bundle."""
        f = BytesIO()
        f.write(b"# v2 git bundle\n")
        f.write(b"-" + b"cc" * 20 + b" prerequisite comment\n")
        f.write(b"ab" * 20 + b" refs/heads/master\n")
        f.write(b"\n")
        # Add pack data
        b = BytesIO()
        write_pack_objects(b.write, [])
        f.write(b.getvalue())
        f.seek(0)

        bundle = read_bundle(f)
        self.assertEqual(2, bundle.version)
        self.assertEqual({}, bundle.capabilities)
        self.assertEqual([(b"cc" * 20, "prerequisite comment")], bundle.prerequisites)
        self.assertEqual({b"refs/heads/master": b"ab" * 20}, bundle.references)

    def test_read_bundle_v3(self) -> None:
        """Test reading a v3 bundle with capabilities."""
        f = BytesIO()
        f.write(b"# v3 git bundle\n")
        f.write(b"@capability1\n")
        f.write(b"@capability2=value2\n")
        f.write(b"-" + b"cc" * 20 + b" prerequisite comment\n")
        f.write(b"ab" * 20 + b" refs/heads/master\n")
        f.write(b"\n")
        # Add pack data
        b = BytesIO()
        write_pack_objects(b.write, [])
        f.write(b.getvalue())
        f.seek(0)

        bundle = read_bundle(f)
        self.assertEqual(3, bundle.version)
        self.assertEqual(
            {"capability1": None, "capability2": "value2"}, bundle.capabilities
        )
        self.assertEqual([(b"cc" * 20, "prerequisite comment")], bundle.prerequisites)
        self.assertEqual({b"refs/heads/master": b"ab" * 20}, bundle.references)

    def test_read_bundle_invalid_format(self) -> None:
        """Test reading a bundle with invalid format."""
        f = BytesIO()
        f.write(b"invalid bundle format\n")
        f.seek(0)

        with self.assertRaises(AssertionError):
            read_bundle(f)

    def test_write_bundle_v2(self) -> None:
        """Test writing a v2 bundle."""
        bundle = Bundle()
        bundle.version = 2
        bundle.capabilities = {}
        bundle.prerequisites = [(b"cc" * 20, "prerequisite comment")]
        bundle.references = {b"refs/heads/master": b"ab" * 20}

        # Create a simple pack data
        b = BytesIO()
        write_pack_objects(b.write, [])
        b.seek(0)
        bundle.pack_data = PackData.from_file(b)

        # Write the bundle
        f = BytesIO()
        write_bundle(f, bundle)
        f.seek(0)

        # Verify the written content
        self.assertEqual(b"# v2 git bundle\n", f.readline())
        self.assertEqual(b"-" + b"cc" * 20 + b" prerequisite comment\n", f.readline())
        self.assertEqual(b"ab" * 20 + b" refs/heads/master\n", f.readline())
        self.assertEqual(b"\n", f.readline())
        # The rest is pack data which we don't validate in detail

    def test_write_bundle_v3(self) -> None:
        """Test writing a v3 bundle with capabilities."""
        bundle = Bundle()
        bundle.version = 3
        bundle.capabilities = {"capability1": None, "capability2": "value2"}
        bundle.prerequisites = [(b"cc" * 20, "prerequisite comment")]
        bundle.references = {b"refs/heads/master": b"ab" * 20}

        # Create a simple pack data
        b = BytesIO()
        write_pack_objects(b.write, [])
        b.seek(0)
        bundle.pack_data = PackData.from_file(b)

        # Write the bundle
        f = BytesIO()
        write_bundle(f, bundle)
        f.seek(0)

        # Verify the written content
        self.assertEqual(b"# v3 git bundle\n", f.readline())
        self.assertEqual(b"@capability1\n", f.readline())
        self.assertEqual(b"@capability2=value2\n", f.readline())
        self.assertEqual(b"-" + b"cc" * 20 + b" prerequisite comment\n", f.readline())
        self.assertEqual(b"ab" * 20 + b" refs/heads/master\n", f.readline())
        self.assertEqual(b"\n", f.readline())
        # The rest is pack data which we don't validate in detail

    def test_write_bundle_auto_version(self) -> None:
        """Test writing a bundle with auto-detected version."""
        # Create a bundle with no explicit version but capabilities
        bundle1 = Bundle()
        bundle1.version = None
        bundle1.capabilities = {"capability1": "value1"}
        bundle1.prerequisites = [(b"cc" * 20, "prerequisite comment")]
        bundle1.references = {b"refs/heads/master": b"ab" * 20}

        b1 = BytesIO()
        write_pack_objects(b1.write, [])
        b1.seek(0)
        bundle1.pack_data = PackData.from_file(b1)

        f1 = BytesIO()
        write_bundle(f1, bundle1)
        f1.seek(0)
        # Should use v3 format since capabilities are present
        self.assertEqual(b"# v3 git bundle\n", f1.readline())

        # Create a bundle with no explicit version and no capabilities
        bundle2 = Bundle()
        bundle2.version = None
        bundle2.capabilities = {}
        bundle2.prerequisites = [(b"cc" * 20, "prerequisite comment")]
        bundle2.references = {b"refs/heads/master": b"ab" * 20}

        b2 = BytesIO()
        write_pack_objects(b2.write, [])
        b2.seek(0)
        bundle2.pack_data = PackData.from_file(b2)

        f2 = BytesIO()
        write_bundle(f2, bundle2)
        f2.seek(0)
        # Should use v2 format since no capabilities are present
        self.assertEqual(b"# v2 git bundle\n", f2.readline())

    def test_write_bundle_invalid_version(self) -> None:
        """Test writing a bundle with an invalid version."""
        bundle = Bundle()
        bundle.version = 4  # Invalid version
        bundle.capabilities = {}
        bundle.prerequisites = []
        bundle.references = {}

        b = BytesIO()
        write_pack_objects(b.write, [])
        b.seek(0)
        bundle.pack_data = PackData.from_file(b)

        f = BytesIO()
        with self.assertRaises(AssertionError):
            write_bundle(f, bundle)

    def test_roundtrip_bundle(self) -> None:
        origbundle = Bundle()
        origbundle.version = 3
        origbundle.capabilities = {"foo": None}
        origbundle.references = {b"refs/heads/master": b"ab" * 20}
        origbundle.prerequisites = [(b"cc" * 20, "comment")]
        b = BytesIO()
        write_pack_objects(b.write, [])
        b.seek(0)
        origbundle.pack_data = PackData.from_file(b)
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, "foo"), "wb") as f:
                write_bundle(f, origbundle)

            with open(os.path.join(td, "foo"), "rb") as f:
                newbundle = read_bundle(f)

                self.assertEqual(origbundle, newbundle)
