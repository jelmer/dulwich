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
                format = format_bytes.decode("utf-8") if format_bytes else "openpgp"
            except KeyError:
                format = "openpgp"
        else:
            format = "openpgp"

    format_lower = format.lower()

    if format_lower == "openpgp":
        # Try to use GPG package vendor first, fall back to CLI
        try:
            import gpg  # noqa: F401

            return GPGSignatureVendor(config=config)
        except ImportError:
            return GPGCliSignatureVendor(config=config)
    elif format_lower == "x509":
        raise ValueError("X.509 signatures are not yet supported")
    elif format_lower == "ssh":
        raise ValueError("SSH signatures are not yet supported")
    else:
        raise ValueError(f"Unsupported signature format: {format}")


# Default GPG vendor instance
gpg_vendor = GPGSignatureVendor()
