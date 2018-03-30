# paramiko_vendor.py
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

"""Tests for paramiko_vendor."""


from dulwich.tests import TestCase
from dulwich.contrib.paramiko_vendor import ParamikoSSHVendor
from paramiko.ssh_exception import NoValidConnectionsError


CLIENT_KEY = """\
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAxvREKSElPOm/0z/nPO+j5rk2tjdgGcGc7We1QZ6TRXYLu7nN
GeEFIL4p8N1i6dmB+Eydt7xqCU79MWD6Yy4prFe1+/K1wCDUxIbFMxqQcX5zjJzd
i8j8PbcaUlVhP/OkjtkSxrXaGDO1BzfdV4iEBtTV/2l3zmLKJlt3jnOHLczP24CB
DTQKp3rKshbRefzot9Y+wnaK692RsYgsyo9YEP0GyWKG9topCHk13r46J6vGLeuj
ryUKqmbLJkzbJbIcEqwTDo5iHaCVqaMr5Hrb8BdMucSseqZQJsXSd+9tdRcIblUQ
38kZjmFMm4SFbruJcpZCNM2wNSZPIRX+3eiwNwIDAQABAoIBAHSacOBSJsr+jIi5
KUOTh9IPtzswVUiDKwARCjB9Sf8p4lKR4N1L/n9kNJyQhApeikgGT2GCMftmqgoo
tlculQoHFgemBlOmak0MV8NNzF5YKEy/GzF0CDH7gJfEpoyetVFrdA+2QS5yD6U9
XqKQxiBi2VEqdScmyyeT8AwzNYTnPeH/DOEcnbdRjqiy/CD79F49CQ1lX1Fuqm0K
I7BivBH1xo/rVnUP4F+IzocDqoga+Pjdj0LTXIgJlHQDSbhsQqWujWQDDuKb+MAw
sNK4Zf8ErV3j1PyA7f/M5LLq6zgstkW4qikDHo4SpZX8kFOO8tjqb7kujj7XqeaB
CxqrOTECgYEA73uWkrohcmDJ4KqbuL3tbExSCOUiaIV+sT1eGPNi7GCmXD4eW5Z4
75v2IHymW83lORSu/DrQ6sKr1nkuRpqr2iBzRmQpl/H+wahIhBXlnJ25uUjDsuPO
1Pq2LcmyD+jTxVnmbSe/q7O09gZQw3I6H4+BMHmpbf8tC97lqimzpJ0CgYEA1K0W
ZL70Xtn9quyHvbtae/BW07NZnxvUg4UaVIAL9Zu34JyplJzyzbIjrmlDbv6aRogH
/KtuG9tfbf55K/jjqNORiuRtzt1hUN1ye4dyW7tHx2/7lXdlqtyK40rQl8P0kqf8
zaS6BqjnobgSdSpg32rWoL/pcBHPdJCJEgQ8zeMCgYEA0/PK8TOhNIzrP1dgGSKn
hkkJ9etuB5nW5mEM7gJDFDf6JPupfJ/xiwe6z0fjKK9S57EhqgUYMB55XYnE5iIw
ZQ6BV9SAZ4V7VsRs4dJLdNC3tn/rDGHJBgCaym2PlbsX6rvFT+h1IC8dwv0V79Ui
Ehq9WTzkMoE8yhvNokvkPZUCgYEAgBAFxv5xGdh79ftdtXLmhnDvZ6S8l6Fjcxqo
Ay/jg66Tp43OU226iv/0mmZKM8Dd1xC8dnon4GBVc19jSYYiWBulrRPlx0Xo/o+K
CzZBN1lrXH1i6dqufpc0jq8TMf/N+q1q/c1uMupsKCY1/xVYpc+ok71b7J7c49zQ
nOeuUW8CgYA9Infooy65FTgbzca0c9kbCUBmcAPQ2ItH3JcPKWPQTDuV62HcT00o
fZdIV47Nez1W5Clk191RMy8TXuqI54kocciUWpThc6j44hz49oUueb8U4bLcEHzA
WxtWBWHwxfSmqgTXilEA3ALJp0kNolLnEttnhENwJpZHlqtes0ZA4w==
-----END RSA PRIVATE KEY-----"""


class ParamikoSSHVendorTests(TestCase):

    def test_run_command_password(self):
        vendor = ParamikoSSHVendor(allow_agent=False, look_for_keys=False)
        with self.assertRaises(NoValidConnectionsError):
            vendor.run_command(
                '127.0.0.1', 'test_run_command_password',
                username='user', port=2200, password='pass')

    def test_run_command_with_privkey(self):
        vendor = ParamikoSSHVendor(allow_agent=False, look_for_keys=False)
        with self.assertRaises(NoValidConnectionsError):
            vendor.run_command(
                '127.0.0.1', 'test_run_command_with_privkey',
                username='user', port=2200, pkey=CLIENT_KEY)

