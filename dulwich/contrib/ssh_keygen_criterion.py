# Copyright (c) 2024 E. Castedo Ellerman <castedo@castedo.com>

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
# fmt: off

import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ..objects import InvalidSignature, SignatureCriterion

# See the following C git implementation code for more details:
# https://archive.softwareheritage.org/swh:1:cnt:07335987a6b9ceaf6edc2da71c2e636b0513372f;origin=https://github.com/git/git;visit=swh:1:snp:e72051ba1b2437b7bf3ed0346d04b289f1393982;anchor=swh:1:rev:6a11438f43469f3815f2f0fc997bd45792ff04c0;path=/gpg-interface.c;lines=450

### WARNING!
### verify_time might or might not be in UTC.
### The following code might not be handling timezone correctly.


class SshKeygenCheckCriterion(SignatureCriterion):
    """Checks signature using ssh-keygen -Y check-novalidate."""

    def __init__(self, capture_output: bool = True):
        self.capture_output = capture_output

    def _ssh_keygen_check(
        self, subcmdline: list[str], crypto_msg: bytes, verify_time: int
    ) -> None:
        verify_dt = datetime.fromtimestamp(verify_time, tz=timezone.utc)
        cmdline = [
            *subcmdline,
            "-n", "git",
            "-O", "verify-time=" + verify_dt.strftime("%Y%m%d%H%M%SZ"),
        ]
        result = subprocess.run(
            cmdline, input=crypto_msg, capture_output=self.capture_output
        )
        if 0 != result.returncode:
            raise InvalidSignature

    def check(self, crypto_msg: bytes, signature: bytes, verify_time: int) -> None:
        with tempfile.NamedTemporaryFile() as sig_file:
            sig_file.write(signature)
            sig_file.flush()
            subcmdline = ["ssh-keygen", "-Y", "check-novalidate", "-s", sig_file.name]
            self._ssh_keygen_check(subcmdline, crypto_msg, verify_time)


class SshKeygenVerifyCriterion(SshKeygenCheckCriterion):
    """Verifies signature using ssh-keygen -Y verify."""

    def __init__(self, allowed_signers: Path, capture_output: bool = True):
        super().__init__(capture_output)
        self.allowed_signers = str(allowed_signers)

    def check(self, crypto_msg: bytes, signature: bytes, verify_time: int) -> None:
        with tempfile.NamedTemporaryFile() as sig_file:
            sig_file.write(signature)
            sig_file.flush()
            cmdline = [
                "ssh-keygen", "-Y", "find-principals",
                "-s", sig_file.name,
                "-f", self.allowed_signers,
            ]
            result = subprocess.run(cmdline, capture_output=True)
            for principal in result.stdout.splitlines():
                subcmdline = [
                    "ssh-keygen", "-Y", "verify",
                     "-f", self.allowed_signers,
                     "-I", str(principal),
                     "-s", sig_file.name,
                ]
                self._ssh_keygen_check(subcmdline, crypto_msg, verify_time)

#ruff: noqa: I001

if __name__ == "__main__":
    import argparse
    import dulwich.repo

    parser = argparse.ArgumentParser()
    parser.add_argument("git_object", default="HEAD", nargs="?")
    parser.add_argument("--allow", type=Path, help="ssh-keygen allowed signers file")
    args = parser.parse_args()

    if args.allow is None:
        criterion = SshKeygenCheckCriterion(capture_output=False)
    else:
        criterion = SshKeygenVerifyCriterion(args.allow, capture_output=False)

    repo = dulwich.repo.Repo(".")
    commit = repo[args.git_object.encode()]
    print("commit", commit.id.decode())
    try:
        commit.check_signature(criterion)
        # signature good or not signed
    except InvalidSignature:
        pass
    print("Author:", commit.author.decode())
    print("\n    ", commit.message.decode())
