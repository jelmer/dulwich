# test_signature.py -- tests for signature.py
# Copyright (C) 2025 Jelmer Vernooĳ <jelmer@jelmer.uk>
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

"""Tests for signature vendors."""

import unittest

from dulwich.signature import GPGSignatureVendor, SignatureVendor

try:
    import gpg
except ImportError:
    gpg = None


class SignatureVendorTests(unittest.TestCase):
    """Tests for SignatureVendor base class."""

    def test_sign_not_implemented(self) -> None:
        """Test that sign raises NotImplementedError."""
        vendor = SignatureVendor()
        with self.assertRaises(NotImplementedError):
            vendor.sign(b"test data")

    def test_verify_not_implemented(self) -> None:
        """Test that verify raises NotImplementedError."""
        vendor = SignatureVendor()
        with self.assertRaises(NotImplementedError):
            vendor.verify(b"test data", b"fake signature")


@unittest.skipIf(gpg is None, "gpg not available")
class GPGSignatureVendorTests(unittest.TestCase):
    """Tests for GPGSignatureVendor."""

    def test_sign_and_verify(self) -> None:
        """Test basic sign and verify cycle.

        Note: This test requires a GPG key to be configured in the test
        environment. It may be skipped in environments without GPG setup.
        """
        vendor = GPGSignatureVendor()
        test_data = b"test data to sign"

        try:
            # Sign the data
            signature = vendor.sign(test_data)
            self.assertIsInstance(signature, bytes)
            self.assertGreater(len(signature), 0)

            # Verify the signature
            vendor.verify(test_data, signature)
        except gpg.errors.GPGMEError as e:
            # Skip test if no GPG key is available
            self.skipTest(f"GPG key not available: {e}")

    def test_verify_invalid_signature(self) -> None:
        """Test that verify raises an error for invalid signatures."""
        vendor = GPGSignatureVendor()
        test_data = b"test data"
        invalid_signature = b"this is not a valid signature"

        with self.assertRaises(gpg.errors.GPGMEError):
            vendor.verify(test_data, invalid_signature)

    def test_sign_with_keyid(self) -> None:
        """Test signing with a specific key ID.

        Note: This test requires a GPG key to be configured in the test
        environment. It may be skipped in environments without GPG setup.
        """
        vendor = GPGSignatureVendor()
        test_data = b"test data to sign"

        try:
            # Try to get a key from the keyring
            with gpg.Context() as ctx:
                keys = list(ctx.keylist(secret=True))
                if not keys:
                    self.skipTest("No GPG keys available for testing")

                key = keys[0]
                signature = vendor.sign(test_data, keyid=key.fpr)
                self.assertIsInstance(signature, bytes)
                self.assertGreater(len(signature), 0)

                # Verify the signature
                vendor.verify(test_data, signature)
        except gpg.errors.GPGMEError as e:
            self.skipTest(f"GPG key not available: {e}")
