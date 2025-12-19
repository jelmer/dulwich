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


class SignatureVendor:
    """A signature implementation for signing and verifying Git objects."""

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
    """Signature vendor that uses the GPG package for signing and verification."""

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
        """
        import gpg

        with gpg.Context() as ctx:
            verified_data, result = ctx.verify(
                data,
                signature=signature,
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


# Default GPG vendor instance
gpg_vendor = GPGSignatureVendor()
