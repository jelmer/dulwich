# test_sshsig.py -- tests for sshsig.py
# (c) 2024 E. Castedo Ellerman <castedo@castedo.com>

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

from dulwich import sshsig

from . import TestCase

msg_sig_pair_of_commit = (
    """\
tree fd7187a49a7b26eb36d782d34c672245b94b2e30
parent b0162c139e4ea2e4783402de26617c912eef1e19
author Castedo Ellerman <castedo@castedo.com> 1729263976 -0400
committer Castedo Ellerman <castedo@castedo.com> 1729263976 -0400

use vite-plugin-static-copy in doc example of Vite
""",
    """\
-----BEGIN SSH SIGNATURE-----
U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAghB1C63jrmh3eWRXJVbrTfw9wP/
BIZf/aKPdFxBlMCq0AAAADZ2l0AAAAAAAAAAZzaGE1MTIAAABTAAAAC3NzaC1lZDI1NTE5
AAAAQJoJUglNVFSaWhnbCl4WImCRLUEo6ymS9WBVOqpoH4kJVgIAMmCIAq9yDOL4fGYmJ0
GsF7btQ1wFf6sbMzq9nwA=
-----END SSH SIGNATURE-----
""",
)

### The following test case produced by
### echo Hola Mundo > hola.txt
### ssh-keygen -Y sign -f test_sign_key -n git hola.txt
### using test_sign_key in ../testdata

msg_sig_pair_hola_mundo = (
    """\
Hola Mundo
""",
    """\
-----BEGIN SSH SIGNATURE-----
U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAgZz+kZMT7JBP0t1l1HQ0K8CduhZ
XTBP/l3sXkZMqTtAkAAAADZ2l0AAAAAAAAAAZzaGE1MTIAAABTAAAAC3NzaC1lZDI1NTE5
AAAAQDFSdQINV271MZ5VwFecGD8oJRob5Nb04r06oVVVCflwgbDLcezjmHJQo41/H3/HXj
pQWO8AJXrx7gcPAcCFGQ0=
-----END SSH SIGNATURE-----
""",
)

crazy_ascii = "Nobody expects the Spanish ..."
crazy_unicode = "Nobody expects 🥘💃🐂 ..."


class SshKeygenCheckNoValidate(TestCase):
    def good_check_novalidate(
        self, message: str, signature: str, namespace: str = "git"
    ) -> bool:
        try:
            sshsig.ssh_keygen_check_novalidate(message, namespace, signature)
            return True
        except sshsig.SshsigError:
            return False

    def test_commit_content(self):
        (msg, sig) = msg_sig_pair_of_commit
        self.assertTrue(self.good_check_novalidate(msg, sig))

    def test_hola_mundo(self):
        (msg, sig) = msg_sig_pair_hola_mundo
        self.assertTrue(self.good_check_novalidate(msg, sig))

    def test_reject_mixed_msg_sig_pairs(self):
        (msg1, sig1) = msg_sig_pair_of_commit
        (msg2, sig2) = msg_sig_pair_hola_mundo
        self.assertFalse(self.good_check_novalidate(msg1, sig2))
        self.assertFalse(self.good_check_novalidate(msg2, sig1))

    def test_reject_subcmd_check_novalidate(self):
        (msg, sig) = msg_sig_pair_of_commit

        self.assertFalse(self.good_check_novalidate(msg, crazy_ascii))
        self.assertFalse(self.good_check_novalidate(msg, crazy_unicode))
        self.assertFalse(self.good_check_novalidate(crazy_ascii, sig))
        self.assertFalse(self.good_check_novalidate(crazy_unicode, sig))

        # the signature was signed with namespace "git"
        self.assertFalse(self.good_check_novalidate(msg, sig, "not-git"))
