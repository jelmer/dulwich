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

import shutil
import subprocess
import unittest

from dulwich.signature import GPGCliSignatureVendor, GPGSignatureVendor, SignatureVendor

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


class GPGCliSignatureVendorTests(unittest.TestCase):
    """Tests for GPGCliSignatureVendor."""

    def setUp(self) -> None:
        """Check if gpg command is available."""
        if shutil.which("gpg") is None:
            self.skipTest("gpg command not available")

    def test_sign_and_verify(self) -> None:
        """Test basic sign and verify cycle using CLI."""
        vendor = GPGCliSignatureVendor()
        test_data = b"test data to sign"

        try:
            # Sign the data
            signature = vendor.sign(test_data)
            self.assertIsInstance(signature, bytes)
            self.assertGreater(len(signature), 0)
            self.assertTrue(signature.startswith(b"-----BEGIN PGP SIGNATURE-----"))

            # Verify the signature
            vendor.verify(test_data, signature)
        except subprocess.CalledProcessError as e:
            # Skip test if no GPG key is available or configured
            self.skipTest(f"GPG signing failed: {e}")

    def test_verify_invalid_signature(self) -> None:
        """Test that verify raises an error for invalid signatures."""
        vendor = GPGCliSignatureVendor()
        test_data = b"test data"
        invalid_signature = b"this is not a valid signature"

        with self.assertRaises(subprocess.CalledProcessError):
            vendor.verify(test_data, invalid_signature)

    def test_sign_with_keyid(self) -> None:
        """Test signing with a specific key ID using CLI."""
        vendor = GPGCliSignatureVendor()
        test_data = b"test data to sign"

        try:
            # Try to get a key from the keyring
            result = subprocess.run(
                ["gpg", "--list-secret-keys", "--with-colons"],
                capture_output=True,
                check=True,
                text=True,
            )

            # Parse output to find a key fingerprint
            keyid = None
            for line in result.stdout.split("\n"):
                if line.startswith("fpr:"):
                    keyid = line.split(":")[9]
                    break

            if not keyid:
                self.skipTest("No GPG keys available for testing")

            signature = vendor.sign(test_data, keyid=keyid)
            self.assertIsInstance(signature, bytes)
            self.assertGreater(len(signature), 0)

            # Verify the signature
            vendor.verify(test_data, signature)
        except subprocess.CalledProcessError as e:
            self.skipTest(f"GPG key not available: {e}")

    def test_verify_with_keyids(self) -> None:
        """Test verifying with specific trusted key IDs."""
        vendor = GPGCliSignatureVendor()
        test_data = b"test data to sign"

        try:
            # Sign without specifying a key (use default)
            signature = vendor.sign(test_data)

            # Get the primary key fingerprint from the keyring
            result = subprocess.run(
                ["gpg", "--list-secret-keys", "--with-colons"],
                capture_output=True,
                check=True,
                text=True,
            )

            primary_keyid = None
            for line in result.stdout.split("\n"):
                if line.startswith("fpr:"):
                    primary_keyid = line.split(":")[9]
                    break

            if not primary_keyid:
                self.skipTest("No GPG keys available for testing")

            # Verify with the correct primary keyid - should succeed
            # (GPG shows primary key fingerprint even if signed by subkey)
            vendor.verify(test_data, signature, keyids=[primary_keyid])

            # Verify with a different keyid - should fail
            fake_keyid = "0" * 40  # Fake 40-character fingerprint
            with self.assertRaises(ValueError):
                vendor.verify(test_data, signature, keyids=[fake_keyid])

        except subprocess.CalledProcessError as e:
            self.skipTest(f"GPG key not available: {e}")

    def test_custom_gpg_command(self) -> None:
        """Test using a custom GPG command path."""
        vendor = GPGCliSignatureVendor(gpg_command="gpg")
        test_data = b"test data"

        try:
            signature = vendor.sign(test_data)
            self.assertIsInstance(signature, bytes)
        except subprocess.CalledProcessError as e:
            self.skipTest(f"GPG not available: {e}")
