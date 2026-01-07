# signature.py -- Signature vendors for signing and verifying Git objects
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

"""Signature vendors for signing and verifying Git objects."""

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    try:
        import sshsig.allowed_signers
        import sshsig.ssh_public_key
    except ImportError:
        pass

    from dulwich.config import Config

__all__ = [
    "SIGNATURE_FORMAT_OPENPGP",
    "SIGNATURE_FORMAT_SSH",
    "SIGNATURE_FORMAT_X509",
    "BadSignature",
    "SignatureSigner",
    "SignatureVerificationError",
    "SignatureVerifier",
    "UntrustedSignature",
    "detect_signature_format",
    "get_available_vendors",
    "get_signature_vendor",
    "get_signature_vendor_for_signature",
]


# Signature verification exceptions
class SignatureVerificationError(Exception):
    """Base exception for signature verification failures."""


class BadSignature(SignatureVerificationError):
    """Exception raised when a signature is invalid or cannot be verified.

    Attributes:
      detail: Optional additional detail about the failure
    """

    def __init__(self, message: str, detail: str | None = None):
        """Initialize BadSignature exception.

        Args:
          message: Error message
          detail: Optional additional detail about the failure
        """
        super().__init__(message)
        self.detail = detail


class UntrustedSignature(SignatureVerificationError):
    """Exception raised when a signature is not from a trusted key.

    Attributes:
      signing_keys: List of key IDs that signed the data (if determinable)
      trusted_keys: List of key IDs that were trusted (if applicable)
    """

    def __init__(
        self,
        message: str,
        signing_keys: list[str] | None = None,
        trusted_keys: list[str] | None = None,
    ):
        """Initialize UntrustedSignature exception.

        Args:
          message: Error message
          signing_keys: List of key IDs that signed the data (if determinable)
          trusted_keys: List of key IDs that were trusted (if applicable)
        """
        super().__init__(message)
        self.signing_keys = signing_keys or []
        self.trusted_keys = trusted_keys or []


# Git signature format constants
SIGNATURE_FORMAT_OPENPGP = "openpgp"
SIGNATURE_FORMAT_X509 = "x509"
SIGNATURE_FORMAT_SSH = "ssh"


class SignatureSigner:
    """A signature implementation for signing Git objects."""

    @classmethod
    def available(cls) -> bool:
        """Check if this signature signer is available.

        Returns:
          True if the signer's dependencies are available, False otherwise
        """
        return True  # Base class is always available

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


class SignatureVerifier:
    """A signature implementation for verifying Git objects."""

    def __init__(self, keyids: Iterable[str] | None = None) -> None:
        """Initialize the signature verifier.

        Args:
          keyids: Optional iterable of trusted key IDs/fingerprints.
                 If provided, only signatures from these keys will be trusted.
        """
        self.keyids = list(keyids) if keyids is not None else None

    @classmethod
    def available(cls) -> bool:
        """Check if this signature verifier is available.

        Returns:
          True if the verifier's dependencies are available, False otherwise
        """
        return True  # Base class is always available

    def verify(self, data: bytes, signature: bytes) -> None:
        """Verify a signature.

        Args:
          data: The data that was signed
          signature: The signature to verify

        Raises:
          BadSignature: if signature verification fails
          UntrustedSignature: if signature was not created by a trusted key
        """
        raise NotImplementedError(self.verify)


class GPGSignatureVendor(SignatureSigner, SignatureVerifier):
    """Signature vendor that uses the GPG package for signing and verification.

    Supports git config options:
    - gpg.minTrustLevel: Minimum trust level for signature verification
    """

    def __init__(
        self,
        min_trust_level: str | None = None,
        keyids: Iterable[str] | None = None,
    ) -> None:
        """Initialize the GPG package vendor.

        Args:
          min_trust_level: Minimum trust level required (undefined, never, marginal,
                          fully, ultimate). If None, any trust level is accepted.
          keyids: Optional iterable of trusted GPG key IDs for verification.
                 If provided, only signatures from these keys will be trusted.
        """
        SignatureVerifier.__init__(self, keyids)
        self.min_trust_level = min_trust_level

    @classmethod
    def from_config(
        cls, config: "Config | None" = None, keyids: Iterable[str] | None = None
    ) -> "GPGSignatureVendor":
        """Create a GPG vendor from git configuration.

        Args:
          config: Git configuration to read settings from
          keyids: Optional iterable of trusted GPG key IDs for verification

        Returns:
          GPGSignatureVendor instance configured from the config
        """
        min_trust_level = None
        if config is not None:
            try:
                trust_level = config.get((b"gpg",), b"minTrustLevel")
                if trust_level:
                    min_trust_level = trust_level.decode("utf-8").lower()
            except KeyError:
                pass

        return cls(min_trust_level=min_trust_level, keyids=keyids)

    @classmethod
    def available(cls) -> bool:
        """Check if the gpg Python package is available.

        Returns:
          True if gpg package can be imported, False otherwise
        """
        import importlib.util

        return importlib.util.find_spec("gpg") is not None

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

    def verify(self, data: bytes, signature: bytes) -> None:
        """Verify a GPG signature.

        Args:
          data: The data that was signed
          signature: The signature to verify

        Raises:
          BadSignature: if GPG signature verification fails
          UntrustedSignature: if the signature was not created by a trusted key
                             or trust level is below minimum
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

        try:
            with gpg.Context() as ctx:
                _verified_data, result = ctx.verify(
                    data,
                    signature=signature,
                )

                # Check minimum trust level if configured
                if self.min_trust_level is not None:
                    min_validity = trust_level_map.get(self.min_trust_level)
                    if min_validity is not None:
                        for sig in result.signatures:
                            if sig.validity < min_validity:
                                raise UntrustedSignature(
                                    f"Signature trust level {sig.validity} is below "
                                    f"minimum required level {self.min_trust_level}"
                                )

                if self.keyids:
                    keys = [ctx.get_key(key) for key in self.keyids]
                    for key in keys:
                        for subkey in key.subkeys:
                            for sig in result.signatures:
                                if subkey.can_sign and subkey.fpr == sig.fpr:
                                    return
                    # Extract signing key fingerprints from the signatures
                    signing_fprs = [sig.fpr for sig in result.signatures]
                    raise UntrustedSignature(
                        f"Signature not created by any of the trusted keys: {self.keyids}",
                        signing_keys=signing_fprs,
                        trusted_keys=list(self.keyids),
                    )
        except gpg.errors.BadSignatures as e:
            raise BadSignature(f"GPG signature verification failed: {e}") from e


class GPGCliSignatureVendor(SignatureSigner, SignatureVerifier):
    """Signature vendor that uses the GPG command-line tool for signing and verification."""

    def __init__(
        self,
        gpg_command: str = "gpg",
        keyids: Iterable[str] | None = None,
    ) -> None:
        """Initialize the GPG CLI vendor.

        Args:
          gpg_command: Path to the GPG command (default: "gpg")
          keyids: Optional iterable of trusted GPG key IDs for verification.
                 If provided, only signatures from these keys will be trusted.
        """
        SignatureVerifier.__init__(self, keyids)
        self.gpg_command = gpg_command

    @classmethod
    def from_config(
        cls, config: "Config | None" = None, keyids: Iterable[str] | None = None
    ) -> "GPGCliSignatureVendor":
        """Create a GPG CLI vendor from git configuration.

        Args:
          config: Git configuration to read settings from
          keyids: Optional iterable of trusted GPG key IDs for verification

        Returns:
          GPGCliSignatureVendor instance configured from the config
        """
        gpg_command = "gpg"
        if config is not None:
            try:
                gpg_program = config.get((b"gpg",), b"program")
                gpg_command = gpg_program.decode("utf-8")
            except KeyError:
                pass

        return cls(gpg_command=gpg_command, keyids=keyids)

    @classmethod
    def available(cls) -> bool:
        """Check if the gpg command is available.

        Returns:
          True if gpg command is in PATH, False otherwise
        """
        import shutil

        return shutil.which("gpg") is not None

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

    def verify(self, data: bytes, signature: bytes) -> None:
        """Verify a GPG signature using the command-line tool.

        Args:
          data: The data that was signed
          signature: The signature to verify

        Raises:
          BadSignature: if GPG signature verification fails
          UntrustedSignature: if signature was not created by a trusted key
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

            try:
                result = subprocess.run(
                    args,
                    capture_output=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise BadSignature(
                    f"GPG signature verification failed: {e.stderr.decode('utf-8', errors='replace')}"
                ) from e

            # If keyids are specified, check that the signature was made by one of them
            if self.keyids:
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
                    raise UntrustedSignature(
                        "Could not determine signing key from GPG output"
                    )

                # Check if any of the signing keys (subkey or primary) match the trusted keyids
                keyids_normalized = [k.replace(" ", "").upper() for k in self.keyids]

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
                raise UntrustedSignature(
                    f"Signature not created by a trusted key. "
                    f"Signed by: {signing_keys}, trusted keys: {list(self.keyids)}",
                    signing_keys=signing_keys,
                    trusted_keys=list(self.keyids),
                )


class X509SignatureVendor(SignatureSigner, SignatureVerifier):
    """Signature vendor that uses gpgsm (GnuPG for S/MIME) for X.509 signatures.

    Supports git config options:
    - gpg.x509.program: Path to gpgsm command (defaults to 'gpgsm')
    """

    def __init__(
        self,
        gpgsm_command: str = "gpgsm",
        keyids: Iterable[str] | None = None,
    ) -> None:
        """Initialize the X.509 signature vendor.

        Args:
          gpgsm_command: Path to the gpgsm command (default: "gpgsm")
          keyids: Optional iterable of trusted certificate IDs for verification.
                 If provided, only signatures from these certificates will be trusted.
        """
        SignatureVerifier.__init__(self, keyids)
        self.gpgsm_command = gpgsm_command

    @classmethod
    def from_config(
        cls, config: "Config | None" = None, keyids: Iterable[str] | None = None
    ) -> "X509SignatureVendor":
        """Create an X.509 vendor from git configuration.

        Args:
          config: Git configuration to read settings from
          keyids: Optional iterable of trusted certificate IDs for verification

        Returns:
          X509SignatureVendor instance configured from the config
        """
        gpgsm_command = "gpgsm"
        if config is not None:
            try:
                gpgsm_program = config.get((b"gpg", b"x509"), b"program")
                gpgsm_command = gpgsm_program.decode("utf-8")
            except KeyError:
                pass

        return cls(gpgsm_command=gpgsm_command, keyids=keyids)

    @classmethod
    def available(cls) -> bool:
        """Check if the gpgsm command is available.

        Returns:
          True if gpgsm command is in PATH, False otherwise
        """
        import shutil

        return shutil.which("gpgsm") is not None

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

    def verify(self, data: bytes, signature: bytes) -> None:
        """Verify an X.509 signature using gpgsm.

        Args:
          data: The data that was signed
          signature: The signature to verify

        Raises:
          BadSignature: if gpgsm signature verification fails
          UntrustedSignature: if signature was not created by a trusted certificate
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

            try:
                result = subprocess.run(
                    args,
                    capture_output=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise BadSignature(
                    f"X.509 signature verification failed: {e.stderr.decode('utf-8', errors='replace')}"
                ) from e

            # If keyids are specified, check that the signature was made by one of them
            if self.keyids:
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
                    raise UntrustedSignature(
                        "Could not determine signing certificate from gpgsm output"
                    )

                # Check if any of the signing certs match the trusted keyids
                keyids_normalized = [k.replace(" ", "").upper() for k in self.keyids]

                for signed_by in signing_certs:
                    signed_by_normalized = signed_by.replace(" ", "").upper()
                    if any(
                        signed_by_normalized in keyid or keyid in signed_by_normalized
                        for keyid in keyids_normalized
                    ):
                        return

                # None of the signing certs matched
                raise UntrustedSignature(
                    f"Signature not created by a trusted certificate. "
                    f"Signed by: {signing_certs}, trusted certs: {list(self.keyids)}",
                    signing_keys=signing_certs,
                    trusted_keys=list(self.keyids),
                )


class SSHSigSignatureVendor(SignatureVerifier):
    """Signature verifier that uses the sshsig Python package for SSH signature verification.

    Note: This vendor only supports verification, not signing. The sshsig package
    does not provide signing functionality. For signing, use SSHCliSignatureVendor.

    Supports git config options:
    - gpg.ssh.allowedSignersFile: File containing allowed SSH public keys
    - gpg.ssh.revocationFile: File containing revoked SSH public keys
    - gpg.ssh.defaultKeyCommand: Command to get default SSH key (currently unused)

    For SSH signatures, keyids are interpreted as principals (identities) like
    "user@example.com", consistent with Git's SSH signature model. When keyids
    are provided, the allowed_signers_file must also be configured, and only
    signatures from keys associated with those principals will be trusted.

    Security features:
    - Key lifetime validation: Checks valid-after and valid-before options in allowed_signers
    - Revocation checking: Verifies keys against revocation file if configured
    """

    def __init__(
        self,
        allowed_signers_file: str | None = None,
        revocation_file: str | None = None,
        default_key_command: str | None = None,
        keyids: Iterable[str] | None = None,
    ) -> None:
        """Initialize the SSH signature verifier.

        Args:
          allowed_signers_file: Path to allowed signers file
          revocation_file: Path to file containing revoked SSH public keys
          default_key_command: Command to get default SSH key (currently unused)
          keyids: Optional iterable of trusted principals (identities) like
                 "user@example.com". If provided, requires allowed_signers_file
                 to be configured. Only signatures from keys associated with
                 these principals will be trusted. This matches Git's -I flag
                 in ssh-keygen -Y verify.
        """
        SignatureVerifier.__init__(self, keyids)
        self.allowed_signers_file = allowed_signers_file
        self.revocation_file = revocation_file
        self.default_key_command = default_key_command

    @classmethod
    def from_config(
        cls, config: "Config | None" = None, keyids: Iterable[str] | None = None
    ) -> "SSHSigSignatureVendor":
        """Create an SSH signature verifier from git configuration.

        Args:
          config: Git configuration to read settings from
          keyids: Optional iterable of trusted SSH key fingerprints for verification

        Returns:
          SSHSigSignatureVendor instance configured from the config
        """
        allowed_signers_file = None
        revocation_file = None
        default_key_command = None

        if config is not None:
            try:
                signers_file = config.get((b"gpg", b"ssh"), b"allowedSignersFile")
                if signers_file:
                    allowed_signers_file = signers_file.decode("utf-8")
            except KeyError:
                pass

            try:
                revoc_file = config.get((b"gpg", b"ssh"), b"revocationFile")
                if revoc_file:
                    revocation_file = revoc_file.decode("utf-8")
            except KeyError:
                pass

            try:
                key_command = config.get((b"gpg", b"ssh"), b"defaultKeyCommand")
                if key_command:
                    default_key_command = key_command.decode("utf-8")
            except KeyError:
                pass

        return cls(
            allowed_signers_file=allowed_signers_file,
            revocation_file=revocation_file,
            default_key_command=default_key_command,
            keyids=keyids,
        )

    @classmethod
    def available(cls) -> bool:
        """Check if the sshsig Python package is available.

        Returns:
          True if sshsig package can be imported, False otherwise
        """
        import importlib.util

        return importlib.util.find_spec("sshsig") is not None

    def _load_allowed_signers_file(
        self, path: str
    ) -> list["sshsig.allowed_signers.AllowedSigner"]:
        """Load and parse an allowed_signers file.

        Args:
          path: Path to the allowed_signers file

        Returns:
          List of AllowedSigner objects

        Raises:
          UntrustedSignature: if file not found or invalid format
        """
        import pathlib

        import sshsig.allowed_signers

        try:
            with open(pathlib.Path(path)) as f:
                return list(sshsig.allowed_signers.load_allowed_signers_file(f))
        except FileNotFoundError as e:
            raise UntrustedSignature(f"Allowed signers file not found: {path}") from e
        except (ValueError, UnicodeDecodeError) as e:
            raise UntrustedSignature(
                f"Invalid allowed signers file format: {path}"
            ) from e

    def _principal_matches(
        self, signer_principals: str, allowed_principals: list[str]
    ) -> bool:
        """Check if a signer's principals match any of the allowed principals.

        Args:
          signer_principals: Comma-separated principals from allowed_signers file
                            (e.g., "user1@example.com,user2@example.com" or "*")
          allowed_principals: List of principals to match against

        Returns:
          True if any signer principal matches any allowed principal, or if
          signer_principals is "*" (wildcard)
        """
        if signer_principals == "*":
            return True

        # Split comma-separated principals
        signer_principal_list = [p.strip() for p in signer_principals.split(",")]

        # Check if any signer principal matches any allowed principal
        return any(sp in allowed_principals for sp in signer_principal_list)

    def _signers_to_keys(
        self, signers: list["sshsig.allowed_signers.AllowedSigner"]
    ) -> list["sshsig.ssh_public_key.PublicKey"]:
        """Convert AllowedSigner objects to PublicKey objects.

        Args:
          signers: List of AllowedSigner objects

        Returns:
          List of PublicKey objects

        Raises:
          UntrustedSignature: if any key cannot be parsed
        """
        import sshsig.ssh_public_key

        keys: list[sshsig.ssh_public_key.PublicKey] = []
        for signer in signers:
            try:
                openssh_str = f"{signer.key_type} {signer.base64_key}"
                key = sshsig.ssh_public_key.PublicKey.from_openssh_str(openssh_str)
                keys.append(key)
            except (ValueError, UnicodeDecodeError) as e:
                raise UntrustedSignature(
                    f"Failed to parse SSH key from allowed_signers file: "
                    f"{signer.key_type} (principal: {signer.principals})"
                ) from e
            except NotImplementedError as e:
                raise UntrustedSignature(
                    f"Unsupported SSH key algorithm: {signer.key_type} "
                    f"(principal: {signer.principals})"
                ) from e
        return keys

    def _parse_ssh_timestamp(self, timestamp_str: str) -> int:
        """Parse SSH timestamp format to Unix timestamp.

        Supports Git's SSH timestamp formats:
        - YYYYMMDD[Z]
        - YYYYMMDDHHMM[SS][Z]

        Args:
          timestamp_str: Timestamp string to parse

        Returns:
          Unix timestamp (seconds since epoch)

        Raises:
          UntrustedSignature: if timestamp format is invalid
        """
        import datetime

        # Remove trailing Z if present
        ts = timestamp_str.rstrip("Z")

        try:
            if len(ts) == 8:  # YYYYMMDD
                dt = datetime.datetime.strptime(ts, "%Y%m%d")
            elif len(ts) == 12:  # YYYYMMDDHHMM
                dt = datetime.datetime.strptime(ts, "%Y%m%d%H%M")
            elif len(ts) == 14:  # YYYYMMDDHHMMSS
                dt = datetime.datetime.strptime(ts, "%Y%m%d%H%M%S")
            else:
                raise UntrustedSignature(
                    f"Invalid SSH timestamp format: {timestamp_str}"
                )

            # Convert to UTC timestamp
            return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())
        except ValueError as e:
            raise UntrustedSignature(
                f"Failed to parse SSH timestamp {timestamp_str}: {e}"
            ) from e

    def _check_key_lifetime(
        self, signer: "sshsig.allowed_signers.AllowedSigner", current_time: int
    ) -> None:
        """Check if a key is within its valid lifetime.

        Args:
          signer: AllowedSigner object with optional lifetime constraints
          current_time: Current Unix timestamp to check against

        Raises:
          UntrustedSignature: if key is not yet valid or has expired
        """
        if signer.options is None:
            return

        # Check valid-after
        if "valid-after" in signer.options:
            valid_after_str = signer.options["valid-after"]
            valid_after = self._parse_ssh_timestamp(valid_after_str)
            if current_time < valid_after:
                raise UntrustedSignature(
                    f"SSH key not yet valid (valid-after: {valid_after_str}, "
                    f"principal: {signer.principals})"
                )

        # Check valid-before
        if "valid-before" in signer.options:
            valid_before_str = signer.options["valid-before"]
            valid_before = self._parse_ssh_timestamp(valid_before_str)
            if current_time >= valid_before:
                raise UntrustedSignature(
                    f"SSH key has expired (valid-before: {valid_before_str}, "
                    f"principal: {signer.principals})"
                )

    def _load_revoked_keys(self, path: str) -> list["sshsig.ssh_public_key.PublicKey"]:
        """Load revoked SSH public keys from a revocation file.

        The revocation file format is the same as authorized_keys:
        each line contains a public key in OpenSSH format.

        Args:
          path: Path to the revocation file

        Returns:
          List of revoked PublicKey objects

        Raises:
          UntrustedSignature: if file cannot be read or contains invalid keys
        """
        import pathlib

        import sshsig.ssh_public_key

        revoked_keys: list[sshsig.ssh_public_key.PublicKey] = []

        try:
            with open(pathlib.Path(path)) as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue

                    try:
                        key = sshsig.ssh_public_key.PublicKey.from_openssh_str(line)
                        revoked_keys.append(key)
                    except (ValueError, UnicodeDecodeError) as e:
                        raise UntrustedSignature(
                            f"Failed to parse revoked key at {path}:{line_num}: {e}"
                        ) from e
                    except NotImplementedError as e:
                        raise UntrustedSignature(
                            f"Unsupported SSH key algorithm in revocation file "
                            f"at {path}:{line_num}: {e}"
                        ) from e
        except FileNotFoundError as e:
            raise UntrustedSignature(f"Revocation file not found: {path}") from e
        except UnicodeDecodeError as e:
            raise UntrustedSignature(
                f"Revocation file has invalid encoding: {path}"
            ) from e

        return revoked_keys

    def _check_not_revoked(
        self,
        key: "sshsig.ssh_public_key.PublicKey",
        revoked_keys: list["sshsig.ssh_public_key.PublicKey"],
    ) -> None:
        """Check if a key has been revoked.

        Args:
          key: PublicKey to check
          revoked_keys: List of revoked PublicKey objects

        Raises:
          UntrustedSignature: if the key has been revoked
        """
        for revoked_key in revoked_keys:
            if key == revoked_key:
                raise UntrustedSignature("SSH key has been revoked")

    def verify(self, data: bytes, signature: bytes) -> None:
        """Verify an SSH signature using the sshsig package.

        Args:
          data: The data that was signed
          signature: The SSH signature to verify (armored format)

        Raises:
          UntrustedSignature: if no allowed signers are configured or if
                             signature is not from a trusted principal
          BadSignature: if signature verification fails

        Note:
          For SSH signatures, keyids are interpreted as principals (identities)
          like "user@example.com", consistent with Git's SSH signature model.
          When keyids are provided, only signatures from keys associated with
          those principals (or wildcard "*" principals) will be trusted.
        """
        import sshsig.sshsig

        if not self.allowed_signers_file:
            raise UntrustedSignature(
                "SSH signature verification requires gpg.ssh.allowedSignersFile "
                "to be configured"
            )

        # Load all signers from the allowed_signers file
        all_signers = self._load_allowed_signers_file(self.allowed_signers_file)

        # Filter by principals if keyids are specified
        if self.keyids:
            filtered_signers = [
                s
                for s in all_signers
                if self._principal_matches(s.principals, list(self.keyids))
            ]

            if not filtered_signers:
                raise UntrustedSignature(
                    f"No keys found for principals: {', '.join(self.keyids)}"
                )
        else:
            filtered_signers = all_signers

        # Check key lifetimes (valid-after/valid-before)
        import time

        current_time = int(time.time())
        for signer in filtered_signers:
            self._check_key_lifetime(signer, current_time)

        # Load revoked keys if revocation file is configured
        revoked_keys: list["sshsig.ssh_public_key.PublicKey"] = []  # noqa: UP037
        if self.revocation_file:
            revoked_keys = self._load_revoked_keys(self.revocation_file)

        # Convert signers to PublicKey objects
        allowed_keys = self._signers_to_keys(filtered_signers)

        # Check that none of the allowed keys have been revoked
        if revoked_keys:
            for key in allowed_keys:
                self._check_not_revoked(key, revoked_keys)

        if not allowed_keys:
            raise UntrustedSignature("No valid allowed signers found")

        # Verify the signature
        sig_str = (
            signature.decode("utf-8") if isinstance(signature, bytes) else signature
        )

        try:
            sshsig.sshsig.verify(
                msg_in=data,
                armored_signature=sig_str,
                allowed_signers=allowed_keys,
                namespace="git",
            )
        except sshsig.sshsig.InvalidSignature as e:
            raise BadSignature(f"SSH signature verification failed: {e}") from e
        except NotImplementedError as e:
            raise BadSignature(
                f"SSH signature uses unsupported encoding feature: {e}"
            ) from e


class SSHCliSignatureVendor(SignatureSigner, SignatureVerifier):
    """Signature vendor that uses ssh-keygen command-line tool for SSH signatures.

    Supports git config options:
    - gpg.ssh.allowedSignersFile: File containing allowed SSH public keys
    - gpg.ssh.revocationFile: File containing revoked SSH public keys
    - gpg.ssh.program: Path to ssh-keygen command
    - gpg.ssh.defaultKeyCommand: Command to get default SSH key for signing
    """

    def __init__(
        self,
        ssh_command: str = "ssh-keygen",
        allowed_signers_file: str | None = None,
        revocation_file: str | None = None,
        default_key_command: str | None = None,
        keyids: Iterable[str] | None = None,
    ) -> None:
        """Initialize the SSH CLI vendor.

        Args:
          ssh_command: Path to ssh-keygen command (default: "ssh-keygen")
          allowed_signers_file: Path to allowed signers file
          revocation_file: Path to revocation file
          default_key_command: Command to get default SSH key for signing
          keyids: Optional iterable of trusted SSH key fingerprints for verification.
                 If provided, only signatures from these keys will be trusted.
        """
        SignatureVerifier.__init__(self, keyids)
        self.ssh_command = ssh_command
        self.allowed_signers_file = allowed_signers_file
        self.revocation_file = revocation_file
        self.default_key_command = default_key_command

    @classmethod
    def from_config(
        cls, config: "Config | None" = None, keyids: Iterable[str] | None = None
    ) -> "SSHCliSignatureVendor":
        """Create an SSH CLI vendor from git configuration.

        Args:
          config: Git configuration to read settings from
          keyids: Optional iterable of trusted SSH key fingerprints for verification

        Returns:
          SSHCliSignatureVendor instance configured from the config
        """
        ssh_command = "ssh-keygen"
        allowed_signers_file = None
        revocation_file = None
        default_key_command = None

        if config is not None:
            try:
                ssh_program = config.get((b"gpg", b"ssh"), b"program")
                ssh_command = ssh_program.decode("utf-8")
            except KeyError:
                pass

            try:
                signers_file = config.get((b"gpg", b"ssh"), b"allowedSignersFile")
                if signers_file:
                    allowed_signers_file = signers_file.decode("utf-8")
            except KeyError:
                pass

            try:
                revoc_file = config.get((b"gpg", b"ssh"), b"revocationFile")
                if revoc_file:
                    revocation_file = revoc_file.decode("utf-8")
            except KeyError:
                pass

            try:
                key_command = config.get((b"gpg", b"ssh"), b"defaultKeyCommand")
                if key_command:
                    default_key_command = key_command.decode("utf-8")
            except KeyError:
                pass

        return cls(
            ssh_command=ssh_command,
            allowed_signers_file=allowed_signers_file,
            revocation_file=revocation_file,
            default_key_command=default_key_command,
            keyids=keyids,
        )

    @classmethod
    def available(cls) -> bool:
        """Check if the ssh-keygen command is available.

        Returns:
          True if ssh-keygen command is in PATH, False otherwise
        """
        import shutil

        return shutil.which("ssh-keygen") is not None

    def sign(self, data: bytes, keyid: str | None = None) -> bytes:
        """Sign data with an SSH key using ssh-keygen.

        Args:
          data: The data to sign
          keyid: Path to SSH private key. If not specified, will try to get
                default key from gpg.ssh.defaultKeyCommand

        Returns:
          The signature as bytes

        Raises:
          subprocess.CalledProcessError: if ssh-keygen command fails
          ValueError: if keyid is not provided and no default key available
        """
        import os
        import subprocess
        import tempfile

        # If no keyid specified, try to get default key from command
        if keyid is None:
            if self.default_key_command:
                # Run the default key command to get the key
                result = subprocess.run(
                    self.default_key_command,
                    shell=True,
                    capture_output=True,
                    check=True,
                    text=True,
                )
                keyid = result.stdout.strip()
                if not keyid:
                    raise ValueError("gpg.ssh.defaultKeyCommand returned empty key")
            else:
                raise ValueError(
                    "SSH signing requires a key to be specified via keyid parameter "
                    "or gpg.ssh.defaultKeyCommand configuration"
                )

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

    def verify(self, data: bytes, signature: bytes) -> None:
        """Verify an SSH signature using ssh-keygen.

        Args:
          data: The data that was signed
          signature: The signature to verify

        Raises:
          BadSignature: if signature verification fails
          UntrustedSignature: if allowedSignersFile is not configured
        """
        import os
        import subprocess
        import tempfile

        # TODO: implement keyids filtering by creating temporary filtered allowed_signers_file
        if self.keyids is not None:
            raise UntrustedSignature(
                "SSHCliSignatureVendor does not yet support keyids filtering. "
                "This will be implemented in a future version."
            )

        if self.allowed_signers_file is None:
            raise UntrustedSignature(
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

            # Add revocation file if configured
            if self.revocation_file:
                args.extend(["-r", self.revocation_file])

            try:
                with open(data_filename, "rb") as data_file:
                    subprocess.run(
                        args,
                        stdin=data_file,
                        capture_output=True,
                        check=True,
                    )
            except subprocess.CalledProcessError as e:
                raise BadSignature(
                    f"SSH signature verification failed: {e.stderr.decode('utf-8', errors='replace')}"
                ) from e


def get_signature_vendor(
    format: str | None = None, config: "Config | None" = None
) -> SignatureSigner:
    """Get a signature signer for the specified format.

    Args:
      format: Signature format. If None, reads from config's gpg.format setting.
              Supported values:
              - "openpgp": Use OpenPGP/GPG signatures (default)
              - "x509": Use X.509 signatures
              - "ssh": Use SSH signatures
      config: Optional Git configuration

    Returns:
      Signature signer instance for the requested format

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
        if GPGSignatureVendor.available():
            return GPGSignatureVendor.from_config(config=config)
        elif GPGCliSignatureVendor.available():
            return GPGCliSignatureVendor.from_config(config=config)
        else:
            raise ValueError(
                "No GPG vendor available (neither gpg package nor gpg command)"
            )
    elif format_lower == SIGNATURE_FORMAT_X509:
        if X509SignatureVendor.available():
            return X509SignatureVendor.from_config(config=config)
        else:
            raise ValueError("gpgsm command not available for X.509 signatures")
    elif format_lower == SIGNATURE_FORMAT_SSH:
        # SSH CLI vendor supports signing
        if SSHCliSignatureVendor.available():
            return SSHCliSignatureVendor.from_config(config=config)
        else:
            raise ValueError("ssh-keygen command not available for SSH signatures")
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
    signature: bytes,
    config: "Config | None" = None,
    keyids: Iterable[str] | None = None,
) -> SignatureVerifier:
    """Get the appropriate signature vendor for a given signature.

    This function detects the signature format and returns the appropriate
    vendor to verify it.

    Args:
      signature: The signature bytes to detect format from
      config: Optional Git configuration
      keyids: Optional iterable of trusted key IDs for verification.
             If provided, only signatures from these keys will be trusted.

    Returns:
      Signature vendor instance appropriate for the signature format

    Raises:
      ValueError: if signature format cannot be detected or is not supported
    """
    format = detect_signature_format(signature)
    format_lower = format.lower()

    # Create vendor with keyids parameter
    if format_lower == SIGNATURE_FORMAT_OPENPGP:
        # Try to use GPG package vendor first, fall back to CLI
        if GPGSignatureVendor.available():
            return GPGSignatureVendor.from_config(config=config, keyids=keyids)
        elif GPGCliSignatureVendor.available():
            return GPGCliSignatureVendor.from_config(config=config, keyids=keyids)
        else:
            raise ValueError(
                "No GPG vendor available (neither gpg package nor gpg command)"
            )
    elif format_lower == SIGNATURE_FORMAT_X509:
        if X509SignatureVendor.available():
            return X509SignatureVendor.from_config(config=config, keyids=keyids)
        else:
            raise ValueError("gpgsm command not available for X.509 signatures")
    elif format_lower == SIGNATURE_FORMAT_SSH:
        # SSH vendors - sshsig package is verify-only, CLI supports both
        if SSHSigSignatureVendor.available():
            return SSHSigSignatureVendor.from_config(config=config, keyids=keyids)
        elif SSHCliSignatureVendor.available():
            return SSHCliSignatureVendor.from_config(config=config, keyids=keyids)
        else:
            raise ValueError(
                "No SSH vendor available (neither sshsig package nor ssh-keygen command)"
            )
    else:
        raise ValueError(f"Unsupported signature format: {format}")


def get_available_vendors() -> dict[
    str, list[type[SignatureSigner] | type[SignatureVerifier]]
]:
    """Get all available signature vendors on this system.

    Returns a dictionary mapping signature format names to lists of available
    vendor classes for each format. Only vendors whose dependencies are available
    are included.

    Returns:
      Dictionary mapping format names (e.g. "openpgp", "ssh", "x509") to lists
      of available vendor classes. If no vendors are available for a format,
      that format will not be present in the dictionary.

    Example:
      >>> vendors = get_available_vendors()
      >>> if "openpgp" in vendors:
      ...     print(f"GPG vendors: {vendors['openpgp']}")
      >>> if "ssh" in vendors:
      ...     print(f"SSH vendors: {vendors['ssh']}")
    """
    available: dict[str, list[type[SignatureSigner] | type[SignatureVerifier]]] = {}

    # Check OpenPGP/GPG vendors
    openpgp_vendors: list[type[SignatureSigner] | type[SignatureVerifier]] = []
    if GPGSignatureVendor.available():
        openpgp_vendors.append(GPGSignatureVendor)
    if GPGCliSignatureVendor.available():
        openpgp_vendors.append(GPGCliSignatureVendor)
    if openpgp_vendors:
        available[SIGNATURE_FORMAT_OPENPGP] = openpgp_vendors

    # Check X.509 vendors
    x509_vendors: list[type[SignatureSigner] | type[SignatureVerifier]] = []
    if X509SignatureVendor.available():
        x509_vendors.append(X509SignatureVendor)
    if x509_vendors:
        available[SIGNATURE_FORMAT_X509] = x509_vendors

    # Check SSH vendors
    ssh_vendors: list[type[SignatureSigner] | type[SignatureVerifier]] = []
    if SSHSigSignatureVendor.available():
        ssh_vendors.append(SSHSigSignatureVendor)
    if SSHCliSignatureVendor.available():
        ssh_vendors.append(SSHCliSignatureVendor)
    if ssh_vendors:
        available[SIGNATURE_FORMAT_SSH] = ssh_vendors

    return available


# Default GPG vendor instance
gpg_vendor = GPGSignatureVendor()
