# sshsig.py -- ssh-keygen SSHSIG signature functionality
# (c) 2018 Mantas Mikulėnas <grawity@gmail.com>
# (c) 2024 E. Castedo Ellerman <castedo@castedo.com>
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

from __future__ import annotations

import binascii
import hashlib
import io
import struct
from collections.abc import ByteString
from typing import BinaryIO, Protocol, cast

import cryptography.exceptions
from cryptography.hazmat.primitives.asymmetric import ed25519


class SshsigError(Exception):
    pass


class UnsupportedVersion(SshsigError):
    pass


class SshEndOfStream(SshsigError):
    pass


class UnsupportedKeyType(SshsigError):
    pass


class UnsupportedSignatureType(SshsigError):
    pass


class InvalidSignature(SshsigError):
    pass


class PublicKey(Protocol):
    def verify(self, signature: bytes, message: bytes) -> None: ...


class SshReader:
    @staticmethod
    def from_bytes(buf: ByteString) -> SshReader:
        return SshReader(io.BytesIO(buf))

    def __init__(self, input_fh: BinaryIO):
        self.input_fh = input_fh

    def read(self, length: int = -1) -> bytes:
        buf = self.input_fh.read(length)
        if (not buf) and (length is not None) and (length != 0):
            raise SshEndOfStream("Unexpected end of input.")
        return buf

    def read_uint32(self) -> int:
        buf = self.read(4)
        (val,) = struct.unpack("!L", buf)
        return cast(int, val)

    def read_string(self) -> bytes:
        length = self.read_uint32()
        buf = self.read(length)
        return buf


class SshWriter:
    def __init__(self, output_fh: io.BytesIO):
        self.output_fh = output_fh

    def write(self, b: ByteString) -> int:
        return self.output_fh.write(b)

    def flush(self) -> None:
        self.output_fh.flush()

    def write_uint32(self, val: int) -> int:
        buf = struct.pack("!L", val)
        return self.write(buf)

    def write_string(self, val: ByteString) -> int:
        buf = struct.pack("!L", len(val)) + val
        return self.write(buf)


def ssh_dearmor_sshsig(buf: str) -> bytes:
    acc = ""
    match = False
    # TODO: stricter format check
    for line in buf.splitlines():
        if line == "-----BEGIN SSH SIGNATURE-----":
            match = True
        elif line == "-----END SSH SIGNATURE-----":
            break
        elif line and match:
            acc += line
    return binascii.a2b_base64(acc)


class SshsigWrapper:
    """The inner 'to-be-signed' data."""

    def __init__(
        self,
        *,
        namespace: bytes = b"",
        reserved: bytes = b"",
        hash_algo: bytes,
        hash: bytes,
    ) -> None:
        self.namespace = namespace
        self.reserved = reserved
        self.hash_algo = hash_algo
        self.hash = hash

    @staticmethod
    def from_bytes(buf: ByteString) -> SshsigWrapper:
        pkt = SshReader.from_bytes(buf)
        magic = pkt.read(6)
        if magic != b"SSHSIG":
            raise ValueError("magic preamble not found")
        return SshsigWrapper(
            namespace=pkt.read_string(),
            reserved=pkt.read_string(),
            hash_algo=pkt.read_string(),
            hash=pkt.read_string(),
        )

    def to_bytes(self) -> bytes:
        pkt = SshWriter(io.BytesIO())
        pkt.write(b"SSHSIG")
        pkt.write_string(self.namespace)
        pkt.write_string(self.reserved)
        pkt.write_string(self.hash_algo)
        pkt.write_string(self.hash)
        return pkt.output_fh.getvalue()


class SshsigSignature:
    def __init__(
        self,
        *,
        version: int = 0x01,
        public_key: bytes,
        namespace: bytes = b"",
        reserved: bytes = b"",
        hash_algo: bytes,
        signature: bytes,
    ):
        self.version = version
        self.public_key = public_key
        self.namespace = namespace
        self.reserved = reserved
        self.hash_algo = hash_algo
        self.signature = signature

    @staticmethod
    def from_bytes(buf: ByteString) -> SshsigSignature:
        pkt = SshReader.from_bytes(buf)
        magic = pkt.read(6)
        if magic != b"SSHSIG":
            raise ValueError("magic preamble not found")
        version = pkt.read_uint32()
        if version != 0x01:
            raise UnsupportedVersion(version)
        return SshsigSignature(
            version=version,
            public_key=pkt.read_string(),
            namespace=pkt.read_string(),
            reserved=pkt.read_string(),
            hash_algo=pkt.read_string(),
            signature=pkt.read_string(),
        )

    @staticmethod
    def from_armored(buf: str) -> SshsigSignature:
        return SshsigSignature.from_bytes(ssh_dearmor_sshsig(buf))


class Ed25519PublicKey(PublicKey):
    def __init__(self, raw_key: bytes):
        self._impl = ed25519.Ed25519PublicKey.from_public_bytes(raw_key)

    def verify(self, signature: bytes, message: bytes) -> None:
        try:
            self._impl.verify(signature, message)
        except cryptography.exceptions.InvalidSignature as ex:
            raise InvalidSignature(ex)


def ssh_parse_publickey(buf: ByteString) -> PublicKey:
    pkt = SshReader.from_bytes(buf)
    algo = pkt.read_string().decode()
    if algo == "ssh-ed25519":
        # https://tools.ietf.org/html/draft-ietf-curdle-ssh-ed25519-ed448-00#section-4
        return Ed25519PublicKey(pkt.read_string())
    raise UnsupportedKeyType(algo)


def ssh_parse_signature(buf: ByteString) -> bytes:
    pkt = SshReader.from_bytes(buf)
    algo = pkt.read_string().decode()
    if algo == "ssh-ed25519":
        # https://tools.ietf.org/html/draft-ietf-curdle-ssh-ed25519-ed448-00#section-4
        return pkt.read_string()
    raise UnsupportedSignatureType(algo)


def hash_file(msg_file: BinaryIO, hash_algo_name: str) -> bytes:
    hash_algo = hash_algo_name.lower()
    if hash_algo not in hashlib.algorithms_guaranteed:
        msg = "Signature hash algo '{}' not supported across platforms by Python."
        raise NotImplementedError(msg.format(hash_algo))
    hobj = hashlib.new(hash_algo)
    while data := msg_file.read(8192):
        hobj.update(data)
    return hobj.digest()


def sshsig_verify(
    sshsig_outer: SshsigSignature,
    msg_file: BinaryIO,
    namespace: str,
) -> PublicKey:
    # The intention of this implementation is to reproduce (approximately)
    # the behaviour of the sshsig_verify_fd function of the ssh-keygen C file:
    # sshsig.c
    # https://archive.softwareheritage.org/
    # swh:1:cnt:470b286a3a982875a48a5262b7057c4710b17fed

    _namespace = namespace.encode("ascii")
    if _namespace != sshsig_outer.namespace:
        errmsg = "Namespace of signature {} != {}"
        raise InvalidSignature(errmsg.format(sshsig_outer.namespace, _namespace))

    msg_hash = hash_file(msg_file, sshsig_outer.hash_algo.decode("ascii"))

    toverify = SshsigWrapper(
        namespace=_namespace, hash_algo=sshsig_outer.hash_algo, hash=msg_hash
    )
    sigdata = ssh_parse_signature(sshsig_outer.signature)
    pub_key = ssh_parse_publickey(sshsig_outer.public_key)
    pub_key.verify(sigdata, toverify.to_bytes())
    return pub_key


def ssh_keygen_check_novalidate(
    msg_in: str | bytes | BinaryIO, namespace: str, armored_signature: str
) -> None:
    """Emulate `ssh-keygen -Y check-novalidate -n namespace -s signature_file`."""
    if isinstance(msg_in, str):
        msg_in = msg_in.encode()
    msg_file = io.BytesIO(msg_in) if isinstance(msg_in, bytes) else msg_in
    sshsig_outer = SshsigSignature.from_armored(armored_signature)
    sshsig_verify(sshsig_outer, msg_file, namespace)
