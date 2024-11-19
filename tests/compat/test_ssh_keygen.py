# test_ssh_keygen.py -- ssh-keygen compatibility tests
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

import subprocess
import sys
import tempfile

from .. import skipIf
from ..test_sshsig import SshKeygenCheckNoValidate


@skipIf(sys.platform == "win32", "Probably not going work on windows.")
class CompatTestSshKeygenCheckNoValidate(SshKeygenCheckNoValidate):
    def good_check_novalidate(
        self, message: str, signature: str, namespace: str = "git"
    ) -> bool:
        cmdline = ["ssh-keygen", "-Y", "check-novalidate", "-n", namespace]
        with tempfile.NamedTemporaryFile() as sig_file:
            sig_file.write(signature.encode())
            sig_file.flush()
            cmdline += ["-s", sig_file.name]
            result = subprocess.run(cmdline, input=message.encode())
            return result.returncode == 0
