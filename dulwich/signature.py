# signature.py -- Signature vendors for signing and verifying Git objects
# Copyright (C) 2025 Jelmer Vernooĳ <jelmer@jelmer.uk>
#
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

"""Signature vendors for signing and verifying Git objects."""

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dulwich.config import Config

# Git signature format constants
SIGNATURE_FORMAT_OPENPGP = "openpgp"
SIGNATURE_FORMAT_X509 = "x509"
SIGNATURE_FORMAT_SSH = "ssh"


class SignatureVendor:
    """A signature implementation for signing and verifying Git objects."""

    def __init__(self, config: "Config | None" = None) -> None:
        """Initialize the signature vendor.

        Args:
          config: Optional Git configuration to use for settings like gpg.program
        """
        self.config = config

    def sign(self, data: bytes, keyid: str | None = None) -> bytes:
        """Sign data with a key.

        Args:
          data: The data to sign
          keyid: Optional key ID to use for signing. If not specified,
                 the default key will be used.

        Returns:
          The signature as bytes
        """
        raise NotImplementedError(self.sign)

    def verify(
        self, data: bytes, signature: bytes, keyids: Iterable[str] | None = None
    ) -> None:
        """Verify a signature.

        Args:
          data: The data that was signed
          signature: The signature to verify
          keyids: Optional iterable of trusted key IDs.
            If the signature was not created by any key in keyids, verification will
            fail. If not specified, this function only verifies that the signature
            is valid.

        Raises:
          Exception on verification failure (implementation-specific)
        """
        raise NotImplementedError(self.verify)


class GPGSignatureVendor(SignatureVendor):
    """Signature vendor that uses the GPG package for signing and verification.

    Supports git config options:
    - gpg.minTrustLevel: Minimum trust level for signature verification
    """

    def __init__(self, config: "Config | None" = None) -> None:
        """Initialize the GPG package vendor.

        Args:
          config: Optional Git configuration for settings like gpg.minTrustLevel
        """
        super().__init__(config)

        # Parse gpg.minTrustLevel from config
        self.min_trust_level = None
        if config is not None:
            try:
                trust_level = config.get((b"gpg",), b"minTrustLevel")
                if trust_level:
                    self.min_trust_level = trust_level.decode("utf-8").lower()
            except KeyError:
                pass

    def sign(self, data: bytes, keyid: str | None = None) -> bytes:
        """Sign data with a GPG key.

        Args:
          data: The data to sign
          keyid: Optional GPG key ID to use for signing. If not specified,
                 the default GPG key will be used.

        Returns:
          The signature as bytes
        """
        import gpg

        signature: bytes
        with gpg.Context(armor=True) as c:
            if keyid is not None:
                key = c.get_key(keyid)
                with gpg.Context(armor=True, signers=[key]) as ctx:
                    signature, _unused_result = ctx.sign(
                        data,
                        mode=gpg.constants.sig.mode.DETACH,
                    )
            else:
                signature, _unused_result = c.sign(
                    data, mode=gpg.constants.sig.mode.DETACH
                )
        return signature

    def verify(
        self, data: bytes, signature: bytes, keyids: Iterable[str] | None = None
    ) -> None:
        """Verify a GPG signature.

        Args:
          data: The data that was signed
          signature: The signature to verify
          keyids: Optional iterable of trusted GPG key IDs.
            If the signature was not created by any key in keyids, verification will
            fail. If not specified, this function only verifies that the signature
            is valid.

        Raises:
          gpg.errors.BadSignatures: if GPG signature verification fails
          gpg.errors.MissingSignatures: if the signature was not created by a key
            specified in keyids
          ValueError: if signature trust level is below minimum configured level
        """
        import gpg

        # Map trust level names to GPGME validity values
        trust_level_map = {
            "undefined": gpg.constants.validity.UNDEFINED,
            "never": gpg.constants.validity.NEVER,
            "marginal": gpg.constants.validity.MARGINAL,
            "fully": gpg.constants.validity.FULL,
            "ultimate": gpg.constants.validity.ULTIMATE,
        }

        with gpg.Context() as ctx:
            verified_data, result = ctx.verify(
                data,
                signature=signature,
            )

            # Check minimum trust level if configured
            if self.min_trust_level is not None:
                min_validity = trust_level_map.get(self.min_trust_level)
                if min_validity is not None:
                    for sig in result.signatures:
                        if sig.validity < min_validity:
                            raise ValueError(
                                f"Signature trust level {sig.validity} is below "
                                f"minimum required level {self.min_trust_level}"
                            )

            if keyids:
                keys = [ctx.get_key(key) for key in keyids]
                for key in keys:
                    for subkey in key.subkeys:
                        for sig in result.signatures:
                            if subkey.can_sign and subkey.fpr == sig.fpr:
                                return
                raise gpg.errors.MissingSignatures(
                    result, keys, results=(verified_data, result)
                )


class GPGCliSignatureVendor(SignatureVendor):
    """Signature vendor that uses the GPG command-line tool for signing and verification."""

    def __init__(
        self, config: "Config | None" = None, gpg_command: str | None = None
    ) -> None:
        """Initialize the GPG CLI vendor.

        Args:
          config: Optional Git configuration to read gpg.program setting from
          gpg_command: Path to the GPG command. If not specified, will try to
                      read from config's gpg.program setting, or default to 'gpg'
        """
        super().__init__(config)

        if gpg_command is not None:
            self.gpg_command = gpg_command
        elif config is not None:
            try:
                gpg_program = config.get((b"gpg",), b"program")
                self.gpg_command = gpg_program.decode("utf-8")
            except KeyError:
                self.gpg_command = "gpg"
        else:
            self.gpg_command = "gpg"

    def sign(self, data: bytes, keyid: str | None = None) -> bytes:
        """Sign data with a GPG key using the command-line tool.

        Args:
          data: The data to sign
          keyid: Optional GPG key ID to use for signing. If not specified,
                 the default GPG key will be used.

        Returns:
          The signature as bytes

        Raises:
          subprocess.CalledProcessError: if GPG command fails
        """
        import subprocess

        args = [self.gpg_command, "--detach-sign", "--armor"]
        if keyid is not None:
            args.extend(["--local-user", keyid])

        result = subprocess.run(
            args,
            input=data,
            capture_output=True,
            check=True,
        )
        return result.stdout

    def verify(
        self, data: bytes, signature: bytes, keyids: Iterable[str] | None = None
    ) -> None:
        """Verify a GPG signature using the command-line tool.

        Args:
          data: The data that was signed
          signature: The signature to verify
          keyids: Optional iterable of trusted GPG key IDs.
            If the signature was not created by any key in keyids, verification will
            fail. If not specified, this function only verifies that the signature
            is valid.

        Raises:
          subprocess.CalledProcessError: if GPG signature verification fails
          ValueError: if signature was not created by a trusted key
        """
        import subprocess
        import tempfile

        # GPG requires the signature and data in separate files for verification
        with (
            tempfile.NamedTemporaryFile(mode="wb", suffix=".sig") as sig_file,
            tempfile.NamedTemporaryFile(mode="wb", suffix=".dat") as data_file,
        ):
            sig_file.write(signature)
            sig_file.flush()

            data_file.write(data)
            data_file.flush()

            args = [self.gpg_command, "--verify", sig_file.name, data_file.name]

            result = subprocess.run(
                args,
                capture_output=True,
                check=True,
            )

            # If keyids are specified, check that the signature was made by one of them
            if keyids:
                # Parse stderr to extract the key fingerprint/ID that made the signature
                stderr_text = result.stderr.decode("utf-8", errors="replace")

                # GPG outputs both subkey and primary key fingerprints
                # Collect both to check against trusted keyids
                signing_keys = []
                for line in stderr_text.split("\n"):
                    if (
                        "using RSA key" in line
                        or "using DSA key" in line
                        or "using EDDSA key" in line
                        or "using ECDSA key" in line
                    ):
                        # Extract the key ID from lines like "gpg: using RSA key ABCD1234..."
                        parts = line.split()
                        if "key" in parts:
                            key_idx = parts.index("key")
                            if key_idx + 1 < len(parts):
                                signing_keys.append(parts[key_idx + 1])
                    elif "Primary key fingerprint:" in line:
                        # Extract fingerprint
                        fpr = line.split(":", 1)[1].strip().replace(" ", "")
                        signing_keys.append(fpr)

                if not signing_keys:
                    raise ValueError("Could not determine signing key from GPG output")

                # Check if any of the signing keys (subkey or primary) match the trusted keyids
                keyids_normalized = [k.replace(" ", "").upper() for k in keyids]

                # Check each signing key against trusted keyids
                for signed_by in signing_keys:
                    signed_by_normalized = signed_by.replace(" ", "").upper()
                    # Check if signed_by matches or is a suffix of any trusted keyid
                    # (GPG sometimes shows short key IDs)
                    if any(
                        signed_by_normalized in keyid or keyid in signed_by_normalized
                        for keyid in keyids_normalized
                    ):
                        return

                # None of the signing keys matched
                raise ValueError(
                    f"Signature not created by a trusted key. "
                    f"Signed by: {signing_keys}, trusted keys: {list(keyids)}"
                )


class X509SignatureVendor(SignatureVendor):
    """Signature vendor that uses gpgsm (GnuPG for S/MIME) for X.509 signatures.

    Supports git config options:
    - gpg.x509.program: Path to gpgsm command (defaults to 'gpgsm')
    """

    def __init__(
        self, config: "Config | None" = None, gpgsm_command: str | None = None
    ) -> None:
        """Initialize the X.509 signature vendor.

        Args:
          config: Optional Git configuration
          gpgsm_command: Path to the gpgsm command. If not specified, will try to
                        read from config's gpg.x509.program setting, or default to 'gpgsm'
        """
        super().__init__(config)

        if gpgsm_command is not None:
            self.gpgsm_command = gpgsm_command
        elif config is not None:
            try:
                gpgsm_program = config.get((b"gpg", b"x509"), b"program")
                self.gpgsm_command = gpgsm_program.decode("utf-8")
            except KeyError:
                self.gpgsm_command = "gpgsm"
        else:
            self.gpgsm_command = "gpgsm"

    def sign(self, data: bytes, keyid: str | None = None) -> bytes:
        """Sign data with an X.509 certificate using gpgsm.

        Args:
          data: The data to sign
          keyid: Optional certificate ID to use for signing. If not specified,
                 the default certificate will be used.

        Returns:
          The signature as bytes

        Raises:
          subprocess.CalledProcessError: if gpgsm command fails
        """
        import subprocess

        args = [self.gpgsm_command, "--detach-sign", "--armor"]
        if keyid is not None:
            args.extend(["--local-user", keyid])

        result = subprocess.run(
            args,
            input=data,
            capture_output=True,
            check=True,
        )
        return result.stdout

    def verify(
        self, data: bytes, signature: bytes, keyids: Iterable[str] | None = None
    ) -> None:
        """Verify an X.509 signature using gpgsm.

        Args:
          data: The data that was signed
          signature: The signature to verify
          keyids: Optional iterable of trusted certificate IDs.
            If the signature was not created by any certificate in keyids, verification will
            fail. If not specified, this function only verifies that the signature is valid.

        Raises:
          subprocess.CalledProcessError: if gpgsm signature verification fails
          ValueError: if signature was not created by a trusted certificate
        """
        import subprocess
        import tempfile

        # gpgsm requires the signature and data in separate files for verification
        with (
            tempfile.NamedTemporaryFile(mode="wb", suffix=".sig") as sig_file,
            tempfile.NamedTemporaryFile(mode="wb", suffix=".dat") as data_file,
        ):
            sig_file.write(signature)
            sig_file.flush()

            data_file.write(data)
            data_file.flush()

            args = [self.gpgsm_command, "--verify", sig_file.name, data_file.name]

            result = subprocess.run(
                args,
                capture_output=True,
                check=True,
            )

            # If keyids are specified, check that the signature was made by one of them
            if keyids:
                # Parse stderr to extract the certificate fingerprint/ID that made the signature
                stderr_text = result.stderr.decode("utf-8", errors="replace")

                # Collect signing certificate IDs
                signing_certs = []
                for line in stderr_text.split("\n"):
                    if "using certificate ID" in line or "Good signature from" in line:
                        # Extract certificate ID from the output
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part.upper().startswith("0x"):
                                signing_certs.append(part[2:])
                            elif len(part) >= 8 and all(
                                c in "0123456789ABCDEF" for c in part.upper()
                            ):
                                signing_certs.append(part)

                if not signing_certs:
                    raise ValueError(
                        "Could not determine signing certificate from gpgsm output"
                    )

                # Check if any of the signing certs match the trusted keyids
                keyids_normalized = [k.replace(" ", "").upper() for k in keyids]

                for signed_by in signing_certs:
                    signed_by_normalized = signed_by.replace(" ", "").upper()
                    if any(
                        signed_by_normalized in keyid or keyid in signed_by_normalized
                        for keyid in keyids_normalized
                    ):
                        return

                # None of the signing certs matched
                raise ValueError(
                    f"Signature not created by a trusted certificate. "
                    f"Signed by: {signing_certs}, trusted certs: {list(keyids)}"
                )


class SSHSigSignatureVendor(SignatureVendor):
    """Signature vendor that uses the sshsig Python package for SSH signature verification.

    Note: This vendor only supports verification, not signing. The sshsig package
    does not provide signing functionality. For signing, use SSHCliSignatureVendor.

    Supports git config options:
    - gpg.ssh.allowedSignersFile: File containing allowed SSH public keys
    - gpg.ssh.defaultKeyCommand: Command to get default SSH key (currently unused)
    """

    def __init__(self, config: "Config | None" = None) -> None:
        """Initialize the SSH signature vendor.

        Args:
          config: Optional Git configuration for SSH signature settings
        """
        super().__init__(config)

        # Parse SSH-specific config
        self.allowed_signers_file = None
        self.default_key_command = None

        if config is not None:
            try:
                signers_file = config.get((b"gpg", b"ssh"), b"allowedSignersFile")
                if signers_file:
                    self.allowed_signers_file = signers_file.decode("utf-8")
            except KeyError:
                pass

            try:
                key_command = config.get((b"gpg", b"ssh"), b"defaultKeyCommand")
                if key_command:
                    self.default_key_command = key_command.decode("utf-8")
            except KeyError:
                pass

    def sign(self, data: bytes, keyid: str | None = None) -> bytes:
        """Sign data with an SSH key.

        Args:
          data: The data to sign
          keyid: Optional SSH key to use. Can be a path to private key or
                 public key prefixed with "key::"

        Returns:
          The signature as bytes

        Raises:
          NotImplementedError: The sshsig package does not support signing.
                              Use SSHCliSignatureVendor instead.
        """
        raise NotImplementedError(
            "The sshsig package does not support signing. "
            "Use SSHCliSignatureVendor instead."
        )

    def verify(
        self, data: bytes, signature: bytes, keyids: Iterable[str] | None = None
    ) -> None:
        """Verify an SSH signature using the sshsig package.

        Args:
          data: The data that was signed
          signature: The SSH signature to verify (armored format)
          keyids: Optional iterable of allowed SSH public keys.
                 If not provided, uses gpg.ssh.allowedSignersFile from config.

        Raises:
          ValueError: if no allowed signers are configured or provided
          sshsig.sshsig.InvalidSignature: if signature verification fails
        """
        from typing import Any

        import sshsig.allowed_signers
        import sshsig.ssh_public_key
        import sshsig.sshsig

        # Determine allowed signers
        allowed_keys: list[Any] = []

        if keyids:
            # Parse keyids as SSH public keys
            for keyid in keyids:
                try:
                    # mypy doesn't see PublicKey.from_string, use Any
                    key: Any = sshsig.ssh_public_key.PublicKey.from_string(  # type: ignore[attr-defined]
                        keyid.encode()
                    )
                    allowed_keys.append(key)
                except Exception:
                    # Try as a file path or ignore invalid keys
                    try:
                        with open(keyid, "rb") as f:
                            key_str = f.read().strip()
                            key = sshsig.ssh_public_key.PublicKey.from_string(  # type: ignore[attr-defined]
                                key_str
                            )
                            allowed_keys.append(key)
                    except Exception:
                        pass
        elif self.allowed_signers_file:
            # Load from allowedSignersFile
            import pathlib

            try:
                allowed_keys = list(
                    sshsig.allowed_signers.load_for_git_allowed_signers_file(
                        pathlib.Path(self.allowed_signers_file)
                    )
                )
            except FileNotFoundError:
                raise ValueError(
                    f"Allowed signers file not found: {self.allowed_signers_file}"
                )
        else:
            raise ValueError(
                "SSH signature verification requires either keyids or "
                "gpg.ssh.allowedSignersFile to be configured"
            )

        if not allowed_keys:
            raise ValueError("No valid allowed signers found")

        # Verify the signature
        # sshsig.verify expects armored signature as string
        sig_str = (
            signature.decode("utf-8") if isinstance(signature, bytes) else signature
        )

        # Verify with namespace "git" (Git's default)
        sshsig.sshsig.verify(
            msg_in=data,
            armored_signature=sig_str,
            allowed_signers=allowed_keys,
            namespace="git",
        )


class SSHCliSignatureVendor(SignatureVendor):
    """Signature vendor that uses ssh-keygen command-line tool for SSH signatures.

    Supports git config options:
    - gpg.ssh.allowedSignersFile: File containing allowed SSH public keys
    - gpg.ssh.program: Path to ssh-keygen command
    """

    def __init__(
        self, config: "Config | None" = None, ssh_command: str | None = None
    ) -> None:
        """Initialize the SSH CLI vendor.

        Args:
          config: Optional Git configuration to read gpg.ssh settings from
          ssh_command: Path to ssh-keygen command. If not specified, will try to
                      read from config's gpg.ssh.program setting, or default to 'ssh-keygen'
        """
        super().__init__(config)

        if ssh_command is not None:
            self.ssh_command = ssh_command
        elif config is not None:
            try:
                ssh_program = config.get((b"gpg", b"ssh"), b"program")
                self.ssh_command = ssh_program.decode("utf-8")
            except KeyError:
                self.ssh_command = "ssh-keygen"
        else:
            self.ssh_command = "ssh-keygen"

        # Parse SSH-specific config
        self.allowed_signers_file = None
        if config is not None:
            try:
                signers_file = config.get((b"gpg", b"ssh"), b"allowedSignersFile")
                if signers_file:
                    self.allowed_signers_file = signers_file.decode("utf-8")
            except KeyError:
                pass

    def sign(self, data: bytes, keyid: str | None = None) -> bytes:
        """Sign data with an SSH key using ssh-keygen.

        Args:
          data: The data to sign
          keyid: Path to SSH private key. If not specified, ssh-keygen will
                use the default key (typically from ssh-agent)

        Returns:
          The signature as bytes

        Raises:
          subprocess.CalledProcessError: if ssh-keygen command fails
          ValueError: if keyid is not provided and no default key available
        """
        import os
        import subprocess
        import tempfile

        if keyid is None:
            raise ValueError("SSH signing requires a key to be specified")

        # Create a temporary directory to hold both data and signature files
        # ssh-keygen creates the signature file with .sig suffix
        with tempfile.TemporaryDirectory() as tmpdir:
            data_filename = os.path.join(tmpdir, "data.git")
            sig_filename = data_filename + ".sig"

            # Write data to file
            with open(data_filename, "wb") as data_file:
                data_file.write(data)

            # Sign with ssh-keygen
            args = [
                self.ssh_command,
                "-Y",
                "sign",
                "-f",
                keyid,
                "-n",
                "git",  # namespace
                data_filename,
            ]

            subprocess.run(args, capture_output=True, check=True)

            # Read signature file
            with open(sig_filename, "rb") as sig_file:
                signature = sig_file.read()

            return signature

    def verify(
        self, data: bytes, signature: bytes, keyids: Iterable[str] | None = None
    ) -> None:
        """Verify an SSH signature using ssh-keygen.

        Args:
          data: The data that was signed
          signature: The signature to verify
          keyids: Not used for SSH verification. Instead, allowed signers
                 are read from gpg.ssh.allowedSignersFile config

        Raises:
          subprocess.CalledProcessError: if signature verification fails
          ValueError: if allowedSignersFile is not configured
        """
        import os
        import subprocess
        import tempfile

        if self.allowed_signers_file is None:
            raise ValueError(
                "SSH signature verification requires gpg.ssh.allowedSignersFile "
                "to be configured"
            )

        # Create a temporary directory for data and signature files
        with tempfile.TemporaryDirectory() as tmpdir:
            data_filename = os.path.join(tmpdir, "data.git")
            sig_filename = os.path.join(tmpdir, "data.git.sig")

            # Write data and signature files
            with open(data_filename, "wb") as data_file:
                data_file.write(data)

            with open(sig_filename, "wb") as sig_file:
                sig_file.write(signature)

            # Verify with ssh-keygen
            # For git signatures, we use "git" as the signer identity
            args = [
                self.ssh_command,
                "-Y",
                "verify",
                "-f",
                self.allowed_signers_file,
                "-I",
                "git",  # signer identity
                "-n",
                "git",  # namespace
                "-s",
                sig_filename,
            ]

            subprocess.run(
                args,
                stdin=open(data_filename, "rb"),
                capture_output=True,
                check=True,
            )


def get_signature_vendor(
    format: str | None = None, config: "Config | None" = None
) -> SignatureVendor:
    """Get a signature vendor for the specified format.

    Args:
      format: Signature format. If None, reads from config's gpg.format setting.
              Supported values:
              - "openpgp": Use OpenPGP/GPG signatures (default)
              - "x509": Use X.509 signatures (not yet implemented)
              - "ssh": Use SSH signatures (not yet implemented)
      config: Optional Git configuration

    Returns:
      SignatureVendor instance for the requested format

    Raises:
      ValueError: if the format is not supported
    """
    # Determine format from config if not specified
    if format is None:
        if config is not None:
            try:
                format_bytes = config.get((b"gpg",), b"format")
                format = (
                    format_bytes.decode("utf-8")
                    if format_bytes
                    else SIGNATURE_FORMAT_OPENPGP
                )
            except KeyError:
                format = SIGNATURE_FORMAT_OPENPGP
        else:
            format = SIGNATURE_FORMAT_OPENPGP

    format_lower = format.lower()

    if format_lower == SIGNATURE_FORMAT_OPENPGP:
        # Try to use GPG package vendor first, fall back to CLI
        try:
            import gpg  # noqa: F401

            return GPGSignatureVendor(config=config)
        except ImportError:
            return GPGCliSignatureVendor(config=config)
    elif format_lower == SIGNATURE_FORMAT_X509:
        return X509SignatureVendor(config=config)
    elif format_lower == SIGNATURE_FORMAT_SSH:
        # Try to use sshsig package vendor first (verify-only), fall back to CLI
        try:
            import sshsig  # noqa: F401

            return SSHSigSignatureVendor(config=config)
        except ImportError:
            return SSHCliSignatureVendor(config=config)
    else:
        raise ValueError(f"Unsupported signature format: {format}")


def detect_signature_format(signature: bytes) -> str:
    """Detect the signature format from the signature data.

    Git signatures are always in ASCII-armored format.

    Args:
      signature: The signature bytes

    Returns:
      Signature format constant (SIGNATURE_FORMAT_OPENPGP, SIGNATURE_FORMAT_SSH, etc.)

    Raises:
      ValueError: if signature format cannot be detected
    """
    # SSH signatures start with SSH armor header
    if signature.startswith(b"-----BEGIN SSH SIGNATURE-----"):
        return SIGNATURE_FORMAT_SSH

    # GPG/PGP signatures start with PGP armor header
    if signature.startswith(b"-----BEGIN PGP SIGNATURE-----"):
        return SIGNATURE_FORMAT_OPENPGP

    # X.509 signatures (S/MIME format)
    if signature.startswith(
        (b"-----BEGIN SIGNED MESSAGE-----", b"-----BEGIN PKCS7-----")
    ):
        return SIGNATURE_FORMAT_X509

    raise ValueError("Unable to detect signature format")


def get_signature_vendor_for_signature(
    signature: bytes, config: "Config | None" = None
) -> SignatureVendor:
    """Get the appropriate signature vendor for a given signature.

    This function detects the signature format and returns the appropriate
    vendor to verify it.

    Args:
      signature: The signature bytes to detect format from
      config: Optional Git configuration

    Returns:
      SignatureVendor instance appropriate for the signature format

    Raises:
      ValueError: if signature format cannot be detected or is not supported
    """
    format = detect_signature_format(signature)
    return get_signature_vendor(format=format, config=config)


# Default GPG vendor instance
gpg_vendor = GPGSignatureVendor()
