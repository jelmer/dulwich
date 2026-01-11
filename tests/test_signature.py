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

from dulwich.config import ConfigDict
from dulwich.signature import (
    SIGNATURE_FORMAT_OPENPGP,
    SIGNATURE_FORMAT_SSH,
    SIGNATURE_FORMAT_X509,
    GPGCliSignatureVendor,
    GPGSignatureVendor,
    SSHCliSignatureVendor,
    SSHSigSignatureVendor,
    X509SignatureVendor,
    detect_signature_format,
    get_signature_vendor,
    get_signature_vendor_for_signature,
)

try:
    import gpg
except ImportError:
    gpg = None


def get_valid_gpg_key() -> str | None:
    """Get a valid (non-revoked, non-expired, can-sign) GPG key from the keyring.

    Returns:
      A key fingerprint string that can be used for signing, or None if no valid key found.

    Raises:
      unittest.SkipTest: if gpg module is not available
    """
    if gpg is None:
        raise unittest.SkipTest("gpg module not available")

    with gpg.Context() as ctx:
        keys = list(ctx.keylist(secret=True))
        if not keys:
            return None

        # Find a non-revoked, non-expired key that can sign
        for key in keys:
            if not key.revoked and not key.expired and key.can_sign:
                return str(key.fpr)

    return None


def get_valid_gpg_key_cli() -> str | None:
    """Get a valid (non-revoked, non-expired) GPG key fingerprint using CLI.

    Returns:
      A key fingerprint string, or None if no valid key found.
    """
    result = subprocess.run(
        ["gpg", "--list-secret-keys", "--with-colons"],
        capture_output=True,
        check=True,
        text=True,
    )

    # Find a valid key (field 2 should be '-' for valid, 'e' for expired, 'r' for revoked)
    current_key_valid = False
    for line in result.stdout.split("\n"):
        if line.startswith("sec:"):
            fields = line.split(":")
            # Only accept valid keys (field 2 is '-')
            current_key_valid = fields[1] == "-"
        elif line.startswith("fpr:") and current_key_valid:
            return line.split(":")[9]

    return None


@unittest.skipIf(gpg is None, "gpg not available")
class GPGSignatureVendorTests(unittest.TestCase):
    """Tests for GPGSignatureVendor."""

    def test_min_trust_level_from_config(self) -> None:
        """Test reading gpg.minTrustLevel from config."""
        config = ConfigDict()
        config.set((b"gpg",), b"minTrustLevel", b"marginal")

        vendor = GPGSignatureVendor.from_config(config=config)
        self.assertEqual(vendor.min_trust_level, "marginal")

    def test_min_trust_level_default(self) -> None:
        """Test default when gpg.minTrustLevel not in config."""
        vendor = GPGSignatureVendor()
        self.assertIsNone(vendor.min_trust_level)

    def test_available(self) -> None:
        """Test that available() returns boolean."""
        result = GPGSignatureVendor.available()
        self.assertIsInstance(result, bool)

    def test_sign_and_verify(self) -> None:
        """Test basic sign and verify cycle.

        Note: This test requires a GPG key to be configured in the test
        environment. It may be skipped in environments without GPG setup.
        """
        vendor = GPGSignatureVendor()
        test_data = b"test data to sign"

        key = get_valid_gpg_key()
        if not key:
            self.skipTest("No valid GPG keys available for testing")

        # Sign the data
        signature = vendor.sign(test_data)
        self.assertIsInstance(signature, bytes)
        self.assertGreater(len(signature), 0)

        # Verify the signature
        vendor.verify(test_data, signature)

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

        key = get_valid_gpg_key()
        if not key:
            self.skipTest("No valid GPG keys available for testing")

        signature = vendor.sign(test_data, keyid=key)
        self.assertIsInstance(signature, bytes)
        self.assertGreater(len(signature), 0)

        # Verify the signature
        vendor.verify(test_data, signature)


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
        from dulwich.signature import BadSignature

        vendor = GPGCliSignatureVendor()
        test_data = b"test data"
        invalid_signature = b"this is not a valid signature"

        with self.assertRaises(BadSignature):
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
        from dulwich.signature import UntrustedSignature

        signer = GPGCliSignatureVendor()
        test_data = b"test data to sign"

        try:
            valid_keyid = get_valid_gpg_key_cli()
            if not valid_keyid:
                self.skipTest("No valid GPG keys available for testing")

            # Sign with the specific key
            signature = signer.sign(test_data, keyid=valid_keyid)

            # Verify with the correct keyid - should succeed
            verifier_trusted = GPGCliSignatureVendor(keyids=[valid_keyid])
            verifier_trusted.verify(test_data, signature)

            # Verify with a different keyid - should fail
            fake_keyid = "0" * 40  # Fake 40-character fingerprint
            verifier_untrusted = GPGCliSignatureVendor(keyids=[fake_keyid])
            with self.assertRaises(UntrustedSignature):
                verifier_untrusted.verify(test_data, signature)

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

    def test_gpg_program_from_config(self) -> None:
        """Test reading gpg.program from config."""
        # Create a config with gpg.program set
        config = ConfigDict()
        config.set((b"gpg",), b"program", b"gpg2")

        vendor = GPGCliSignatureVendor.from_config(config=config)
        self.assertEqual(vendor.gpg_command, "gpg2")

    def test_gpg_program_explicit(self) -> None:
        """Test that gpg_command parameter works when passed directly."""
        vendor = GPGCliSignatureVendor(gpg_command="gpg2")
        self.assertEqual(vendor.gpg_command, "gpg2")

    def test_gpg_program_default(self) -> None:
        """Test default gpg command when no config provided."""
        vendor = GPGCliSignatureVendor()
        self.assertEqual(vendor.gpg_command, "gpg")

    def test_gpg_program_default_when_not_in_config(self) -> None:
        """Test default gpg command when config doesn't have gpg.program."""
        config = ConfigDict()
        vendor = GPGCliSignatureVendor.from_config(config=config)
        self.assertEqual(vendor.gpg_command, "gpg")

    def test_available(self) -> None:
        """Test that available() returns boolean."""
        result = GPGCliSignatureVendor.available()
        self.assertIsInstance(result, bool)


class X509SignatureVendorTests(unittest.TestCase):
    """Tests for X509SignatureVendor."""

    def test_gpgsm_program_default(self) -> None:
        """Test default gpgsm command is 'gpgsm'."""
        vendor = X509SignatureVendor()
        self.assertEqual(vendor.gpgsm_command, "gpgsm")

    def test_gpgsm_program_from_config(self) -> None:
        """Test reading gpg.x509.program from config."""
        config = ConfigDict()
        config.set((b"gpg", b"x509"), b"program", b"/usr/local/bin/gpgsm")

        vendor = X509SignatureVendor.from_config(config=config)
        self.assertEqual(vendor.gpgsm_command, "/usr/local/bin/gpgsm")

    def test_gpgsm_program_explicit(self) -> None:
        """Test gpgsm_command parameter works when passed directly."""
        vendor = X509SignatureVendor(gpgsm_command="/custom/gpgsm")
        self.assertEqual(vendor.gpgsm_command, "/custom/gpgsm")

    def test_gpgsm_program_default_when_not_in_config(self) -> None:
        """Test default when gpg.x509.program not in config."""
        config = ConfigDict()
        vendor = X509SignatureVendor.from_config(config=config)
        self.assertEqual(vendor.gpgsm_command, "gpgsm")

    def test_available(self) -> None:
        """Test that available() returns boolean."""
        result = X509SignatureVendor.available()
        self.assertIsInstance(result, bool)

    @unittest.skipIf(
        shutil.which("gpgsm") is None, "gpgsm command not available in PATH"
    )
    def test_sign_and_verify(self) -> None:
        """Test basic X.509 sign and verify cycle.

        Note: This test requires gpgsm and an X.509 certificate to be configured.
        It may be skipped in environments without gpgsm/certificate setup.
        """
        vendor = X509SignatureVendor()
        test_data = b"test data to sign"

        try:
            # Try to sign the data
            signature = vendor.sign(test_data)
            self.assertIsInstance(signature, bytes)
            self.assertGreater(len(signature), 0)

            # Verify the signature
            vendor.verify(test_data, signature)
        except subprocess.CalledProcessError:
            # Skip test if no X.509 certificate is available
            self.skipTest("No X.509 certificate available for signing")


class GetSignatureVendorTests(unittest.TestCase):
    """Tests for get_signature_vendor function."""

    def test_default_format(self) -> None:
        """Test that default format is openpgp."""
        vendor = get_signature_vendor()
        self.assertIsInstance(vendor, (GPGSignatureVendor, GPGCliSignatureVendor))

    def test_explicit_openpgp_format(self) -> None:
        """Test explicitly requesting openpgp format."""
        vendor = get_signature_vendor(format="openpgp")
        self.assertIsInstance(vendor, (GPGSignatureVendor, GPGCliSignatureVendor))

    def test_format_from_config(self) -> None:
        """Test reading format from config."""
        config = ConfigDict()
        config.set((b"gpg",), b"format", b"openpgp")

        vendor = get_signature_vendor(config=config)
        self.assertIsInstance(vendor, (GPGSignatureVendor, GPGCliSignatureVendor))

    def test_format_case_insensitive(self) -> None:
        """Test that format is case-insensitive."""
        vendor = get_signature_vendor(format="OpenPGP")
        self.assertIsInstance(vendor, (GPGSignatureVendor, GPGCliSignatureVendor))

    def test_x509_format_supported(self) -> None:
        """Test that x509 format is now supported."""
        vendor = get_signature_vendor(format="x509")
        self.assertIsInstance(vendor, X509SignatureVendor)

    def test_ssh_format_supported(self) -> None:
        """Test that ssh format is now supported."""
        vendor = get_signature_vendor(format="ssh")
        # Should be either SSHSigSignatureVendor or SSHCliSignatureVendor
        self.assertIsInstance(vendor, (SSHSigSignatureVendor, SSHCliSignatureVendor))

    def test_invalid_format(self) -> None:
        """Test that invalid format raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            get_signature_vendor(format="invalid")
        self.assertIn("Unsupported", str(cm.exception))

    def test_config_passed_to_vendor(self) -> None:
        """Test that config is passed to the vendor."""
        config = ConfigDict()
        config.set((b"gpg",), b"program", b"gpg2")

        vendor = get_signature_vendor(format="openpgp", config=config)
        # If CLI vendor is used, check that config was passed
        if isinstance(vendor, GPGCliSignatureVendor):
            self.assertEqual(vendor.gpg_command, "gpg2")

    def test_ssh_format(self) -> None:
        """Test requesting SSH format."""
        vendor = get_signature_vendor(format="ssh")
        # Should be either SSHSigSignatureVendor or SSHCliSignatureVendor
        self.assertIsInstance(vendor, (SSHSigSignatureVendor, SSHCliSignatureVendor))

    def test_x509_format(self) -> None:
        """Test requesting X.509 format."""
        vendor = get_signature_vendor(format="x509")
        self.assertIsInstance(vendor, X509SignatureVendor)


class SSHSigSignatureVendorTests(unittest.TestCase):
    """Tests for SSHSigSignatureVendor (sshsig package implementation)."""

    def setUp(self) -> None:
        """Check if sshsig package is available."""
        if not SSHSigSignatureVendor.available():
            self.skipTest("sshsig package not available")

    def test_verify_without_config_raises(self) -> None:
        """Test that verify without config or keyids raises UntrustedSignature."""
        from dulwich.signature import UntrustedSignature

        vendor = SSHSigSignatureVendor()
        with self.assertRaises(UntrustedSignature) as cm:
            vendor.verify(b"test data", b"fake signature")
        self.assertIn("allowedSignersFile", str(cm.exception))

    def test_config_parsing(self) -> None:
        """Test parsing SSH config options."""
        config = ConfigDict()
        config.set((b"gpg", b"ssh"), b"allowedSignersFile", b"/path/to/allowed")
        config.set((b"gpg", b"ssh"), b"defaultKeyCommand", b"ssh-add -L")

        vendor = SSHSigSignatureVendor.from_config(config=config)
        self.assertEqual(vendor.allowed_signers_file, "/path/to/allowed")
        self.assertEqual(vendor.default_key_command, "ssh-add -L")

    def test_available(self) -> None:
        """Test that available() returns boolean."""
        result = SSHSigSignatureVendor.available()
        self.assertIsInstance(result, bool)

    def test_verify_with_cli_generated_signature(self) -> None:
        """Test verifying a signature created by SSH CLI vendor."""
        import os
        import tempfile

        if shutil.which("ssh-keygen") is None:
            self.skipTest("ssh-keygen not available")

        # Generate a test SSH key and signature using CLI vendor
        with tempfile.TemporaryDirectory() as tmpdir:
            private_key = os.path.join(tmpdir, "test_key")
            public_key = private_key + ".pub"
            allowed_signers = os.path.join(tmpdir, "allowed_signers")

            # Generate Ed25519 key
            subprocess.run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-f",
                    private_key,
                    "-N",
                    "",
                    "-C",
                    "test@example.com",
                ],
                capture_output=True,
                check=True,
            )

            # Create allowed_signers file
            with open(public_key) as pub:
                pub_key_content = pub.read().strip()
            with open(allowed_signers, "w") as allowed:
                allowed.write(f"* {pub_key_content}\n")

            # Sign with CLI vendor
            cli_config = ConfigDict()
            cli_config.set(
                (b"gpg", b"ssh"), b"allowedSignersFile", allowed_signers.encode()
            )
            cli_vendor = SSHCliSignatureVendor.from_config(config=cli_config)

            test_data = b"test data for sshsig verification"
            signature = cli_vendor.sign(test_data, keyid=private_key)

            # Verify with sshsig package vendor
            pkg_config = ConfigDict()
            pkg_config.set(
                (b"gpg", b"ssh"), b"allowedSignersFile", allowed_signers.encode()
            )
            pkg_vendor = SSHSigSignatureVendor.from_config(config=pkg_config)

            # This should succeed
            pkg_vendor.verify(test_data, signature)

    def test_key_lifetime_validation(self) -> None:
        """Test SSH key lifetime validation (valid-after/valid-before).

        Note: The current version of the sshsig library does not parse options
        from allowed_signers files, so this test verifies the code is in place
        but will be skipped until the library adds support.
        """
        import io

        # Check if sshsig library supports parsing options
        import sshsig.allowed_signers

        test_content = 'valid-after="20260104" test@example.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl'
        f = io.StringIO(test_content)
        signers = list(sshsig.allowed_signers.load_allowed_signers_file(f))

        if not signers or signers[0].options is None:
            self.skipTest(
                "sshsig library does not yet support parsing options from allowed_signers files"
            )

        # If we get here, the library supports options, so we can test
        import os
        import tempfile
        import time

        from dulwich.signature import UntrustedSignature

        if shutil.which("ssh-keygen") is None:
            self.skipTest("ssh-keygen not available")

        with tempfile.TemporaryDirectory() as tmpdir:
            private_key = os.path.join(tmpdir, "test_key")
            public_key = private_key + ".pub"
            allowed_signers = os.path.join(tmpdir, "allowed_signers")

            # Generate Ed25519 key
            subprocess.run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-f",
                    private_key,
                    "-N",
                    "",
                    "-C",
                    "test@example.com",
                ],
                capture_output=True,
                check=True,
            )

            # Read public key
            with open(public_key) as pub:
                pub_key_content = pub.read().strip()

            # Test 1: Key with valid-after in the future (should fail)
            future_time = int(time.time()) + 86400  # 1 day from now
            future_timestamp = time.strftime("%Y%m%d", time.gmtime(future_time))
            with open(allowed_signers, "w") as allowed:
                allowed.write(
                    f'valid-after="{future_timestamp}" test@example.com {pub_key_content}\n'
                )

            cli_config = ConfigDict()
            cli_config.set(
                (b"gpg", b"ssh"), b"allowedSignersFile", allowed_signers.encode()
            )
            cli_vendor = SSHCliSignatureVendor.from_config(config=cli_config)

            test_data = b"test data for lifetime validation"
            signature = cli_vendor.sign(test_data, keyid=private_key)

            pkg_config = ConfigDict()
            pkg_config.set(
                (b"gpg", b"ssh"), b"allowedSignersFile", allowed_signers.encode()
            )
            pkg_vendor = SSHSigSignatureVendor.from_config(config=pkg_config)

            # Verification should fail because key is not yet valid
            with self.assertRaises(UntrustedSignature) as cm:
                pkg_vendor.verify(test_data, signature)
            self.assertIn("not yet valid", str(cm.exception))

    def test_revocation_checking(self) -> None:
        """Test SSH key revocation checking."""
        import os
        import tempfile

        from dulwich.signature import UntrustedSignature

        if shutil.which("ssh-keygen") is None:
            self.skipTest("ssh-keygen not available")

        with tempfile.TemporaryDirectory() as tmpdir:
            private_key = os.path.join(tmpdir, "test_key")
            public_key = private_key + ".pub"
            allowed_signers = os.path.join(tmpdir, "allowed_signers")
            revocation_file = os.path.join(tmpdir, "revoked_keys")

            # Generate Ed25519 key
            subprocess.run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-f",
                    private_key,
                    "-N",
                    "",
                    "-C",
                    "test@example.com",
                ],
                capture_output=True,
                check=True,
            )

            # Read public key
            with open(public_key) as pub:
                pub_key_content = pub.read().strip()

            # Create allowed_signers file
            with open(allowed_signers, "w") as allowed:
                allowed.write(f"* {pub_key_content}\n")

            # Sign some data
            cli_config = ConfigDict()
            cli_config.set(
                (b"gpg", b"ssh"), b"allowedSignersFile", allowed_signers.encode()
            )
            cli_vendor = SSHCliSignatureVendor.from_config(config=cli_config)

            test_data = b"test data for revocation checking"
            signature = cli_vendor.sign(test_data, keyid=private_key)

            # Test 1: Without revocation file (should succeed)
            pkg_config = ConfigDict()
            pkg_config.set(
                (b"gpg", b"ssh"), b"allowedSignersFile", allowed_signers.encode()
            )
            pkg_vendor = SSHSigSignatureVendor.from_config(config=pkg_config)
            pkg_vendor.verify(test_data, signature)

            # Test 2: With revocation file containing this key (should fail)
            with open(revocation_file, "w") as revoked:
                revoked.write(f"{pub_key_content}\n")

            pkg_config.set(
                (b"gpg", b"ssh"), b"revocationFile", revocation_file.encode()
            )
            pkg_vendor = SSHSigSignatureVendor.from_config(config=pkg_config)

            with self.assertRaises(UntrustedSignature) as cm:
                pkg_vendor.verify(test_data, signature)
            self.assertIn("revoked", str(cm.exception))


class SSHCliSignatureVendorTests(unittest.TestCase):
    """Tests for SSHCliSignatureVendor."""

    def setUp(self) -> None:
        """Check if ssh-keygen is available."""
        if shutil.which("ssh-keygen") is None:
            self.skipTest("ssh-keygen command not available")

    def test_ssh_program_from_config(self) -> None:
        """Test reading gpg.ssh.program from config."""
        config = ConfigDict()
        config.set((b"gpg", b"ssh"), b"program", b"/usr/bin/ssh-keygen")

        vendor = SSHCliSignatureVendor.from_config(config=config)
        self.assertEqual(vendor.ssh_command, "/usr/bin/ssh-keygen")

    def test_ssh_program_explicit(self) -> None:
        """Test that ssh_command parameter works when passed directly."""
        vendor = SSHCliSignatureVendor(ssh_command="/usr/bin/ssh-keygen")
        self.assertEqual(vendor.ssh_command, "/usr/bin/ssh-keygen")

    def test_ssh_program_default(self) -> None:
        """Test default ssh-keygen command when no config provided."""
        vendor = SSHCliSignatureVendor()
        self.assertEqual(vendor.ssh_command, "ssh-keygen")

    def test_allowed_signers_from_config(self) -> None:
        """Test reading gpg.ssh.allowedSignersFile from config."""
        config = ConfigDict()
        config.set((b"gpg", b"ssh"), b"allowedSignersFile", b"/tmp/allowed_signers")

        vendor = SSHCliSignatureVendor.from_config(config=config)
        self.assertEqual(vendor.allowed_signers_file, "/tmp/allowed_signers")

    def test_sign_without_key_raises(self) -> None:
        """Test that signing without a key raises ValueError."""
        vendor = SSHCliSignatureVendor()
        with self.assertRaises(ValueError) as cm:
            vendor.sign(b"test data")
        self.assertIn("key", str(cm.exception).lower())

    def test_verify_without_allowed_signers_raises(self) -> None:
        """Test that verify without allowedSignersFile raises UntrustedSignature."""
        from dulwich.signature import UntrustedSignature

        vendor = SSHCliSignatureVendor()
        with self.assertRaises(UntrustedSignature) as cm:
            vendor.verify(b"test data", b"fake signature")
        self.assertIn("allowedSignersFile", str(cm.exception))

    def test_sign_and_verify_with_ssh_key(self) -> None:
        """Test sign and verify cycle with SSH key."""
        import os
        import tempfile

        # Generate a test SSH key
        with tempfile.TemporaryDirectory() as tmpdir:
            private_key = os.path.join(tmpdir, "test_key")
            public_key = private_key + ".pub"
            allowed_signers = os.path.join(tmpdir, "allowed_signers")

            # Generate Ed25519 key (no passphrase)
            subprocess.run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-f",
                    private_key,
                    "-N",
                    "",
                    "-C",
                    "test@example.com",
                ],
                capture_output=True,
                check=True,
            )

            # Create allowed_signers file
            with open(public_key) as pub:
                pub_key_content = pub.read().strip()
            with open(allowed_signers, "w") as allowed:
                allowed.write(f"git {pub_key_content}\n")

            # Create vendor with config
            config = ConfigDict()
            config.set(
                (b"gpg", b"ssh"), b"allowedSignersFile", allowed_signers.encode()
            )

            vendor = SSHCliSignatureVendor.from_config(config=config)

            # Test signing and verification
            test_data = b"test data to sign with SSH"
            signature = vendor.sign(test_data, keyid=private_key)

            self.assertIsInstance(signature, bytes)
            self.assertGreater(len(signature), 0)
            self.assertTrue(signature.startswith(b"-----BEGIN SSH SIGNATURE-----"))

            # Verify the signature
            vendor.verify(test_data, signature)

    def test_default_key_command(self) -> None:
        """Test gpg.ssh.defaultKeyCommand support."""
        import os
        import tempfile

        # Generate a test SSH key
        with tempfile.TemporaryDirectory() as tmpdir:
            private_key = os.path.join(tmpdir, "test_key")

            # Generate Ed25519 key (no passphrase)
            subprocess.run(
                [
                    "ssh-keygen",
                    "-t",
                    "ed25519",
                    "-f",
                    private_key,
                    "-N",
                    "",
                    "-C",
                    "test@example.com",
                ],
                capture_output=True,
                check=True,
            )

            # Create config with defaultKeyCommand that echoes the key path
            config = ConfigDict()
            config.set(
                (b"gpg", b"ssh"), b"defaultKeyCommand", f"echo {private_key}".encode()
            )

            vendor = SSHCliSignatureVendor.from_config(config=config)
            test_data = b"test data"

            # Sign without providing keyid - should use defaultKeyCommand
            signature = vendor.sign(test_data)
            self.assertIsInstance(signature, bytes)
            self.assertGreater(len(signature), 0)

    def test_revocation_file_config(self) -> None:
        """Test that revocation file is read from config."""
        config = ConfigDict()
        config.set((b"gpg", b"ssh"), b"revocationFile", b"/path/to/revoked_keys")

        vendor = SSHCliSignatureVendor.from_config(config=config)
        self.assertEqual(vendor.revocation_file, "/path/to/revoked_keys")

    def test_available(self) -> None:
        """Test that available() returns boolean."""
        result = SSHCliSignatureVendor.available()
        self.assertIsInstance(result, bool)


class DetectSignatureFormatTests(unittest.TestCase):
    """Tests for detect_signature_format function."""

    def test_detect_ssh_signature(self) -> None:
        """Test detecting SSH signature format."""
        ssh_sig = b"-----BEGIN SSH SIGNATURE-----\nfoo\n-----END SSH SIGNATURE-----"
        self.assertEqual(detect_signature_format(ssh_sig), SIGNATURE_FORMAT_SSH)

    def test_detect_pgp_signature(self) -> None:
        """Test detecting PGP signature format."""
        pgp_sig = b"-----BEGIN PGP SIGNATURE-----\nfoo\n-----END PGP SIGNATURE-----"
        self.assertEqual(detect_signature_format(pgp_sig), SIGNATURE_FORMAT_OPENPGP)

    def test_detect_x509_signature_pkcs7(self) -> None:
        """Test detecting X.509 PKCS7 signature format."""
        x509_sig = b"-----BEGIN PKCS7-----\nfoo\n-----END PKCS7-----"
        self.assertEqual(detect_signature_format(x509_sig), SIGNATURE_FORMAT_X509)

    def test_detect_x509_signature_signed_message(self) -> None:
        """Test detecting X.509 signed message format."""
        x509_sig = b"-----BEGIN SIGNED MESSAGE-----\nfoo\n-----END SIGNED MESSAGE-----"
        self.assertEqual(detect_signature_format(x509_sig), SIGNATURE_FORMAT_X509)

    def test_unknown_signature_format(self) -> None:
        """Test that unknown format raises ValueError."""
        with self.assertRaises(ValueError) as cm:
            detect_signature_format(b"not a signature")
        self.assertIn("Unable to detect", str(cm.exception))


class GetSignatureVendorForSignatureTests(unittest.TestCase):
    """Tests for get_signature_vendor_for_signature function."""

    def test_get_vendor_for_ssh_signature(self) -> None:
        """Test getting vendor for SSH signature."""
        ssh_sig = b"-----BEGIN SSH SIGNATURE-----\nfoo\n-----END SSH SIGNATURE-----"
        vendor = get_signature_vendor_for_signature(ssh_sig)
        self.assertIsInstance(vendor, (SSHSigSignatureVendor, SSHCliSignatureVendor))

    def test_get_vendor_for_pgp_signature(self) -> None:
        """Test getting vendor for PGP signature."""
        pgp_sig = b"-----BEGIN PGP SIGNATURE-----\nfoo\n-----END PGP SIGNATURE-----"
        vendor = get_signature_vendor_for_signature(pgp_sig)
        self.assertIsInstance(vendor, (GPGSignatureVendor, GPGCliSignatureVendor))

    def test_get_vendor_for_x509_signature(self) -> None:
        """Test getting vendor for X.509 signature."""
        x509_sig = b"-----BEGIN PKCS7-----\nfoo\n-----END PKCS7-----"
        vendor = get_signature_vendor_for_signature(x509_sig)
        self.assertIsInstance(vendor, X509SignatureVendor)

    def test_get_vendor_with_config(self) -> None:
        """Test that config is passed to vendor."""
        config = ConfigDict()
        config.set((b"gpg",), b"program", b"gpg2")

        pgp_sig = b"-----BEGIN PGP SIGNATURE-----\nfoo\n-----END PGP SIGNATURE-----"
        vendor = get_signature_vendor_for_signature(pgp_sig, config=config)

        # If CLI vendor is used, check config was passed
        if isinstance(vendor, GPGCliSignatureVendor):
            self.assertEqual(vendor.gpg_command, "gpg2")
