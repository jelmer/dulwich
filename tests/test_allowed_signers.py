# Copyright (C) 2024 E. Castedo Ellerman <castedo@castedo.com>
#
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later
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

from io import StringIO
from unittest import TestCase

from dulwich.allowed_signers import AllowedSigner, load_allowed_signers_file

key0 = [
    "ssh-ed25519",
    "AAAAC3NzaC1lZDI1NTE5AAAAIJY08ynqE/VoH690nSN+MUxMzAbfNcMdUQr+5ltIskMt",
]
key1 = [
    "ssh-ed25519",
    "AAAAC3NzaC1lZDI1NTE5AAAAIIQdQut465od3lkVyVW6038PcD/wSGX/2ij3RcQZTAqt",
]
rsa_key = [
    "ssh-rsa",
    "AAAAB3NzaC1yc2EAAAADAQABAAABgQCVw5Oex+EwQLGSJGaSO1kpMgaIW44AZxzRszgP6WwsF3GFSUJqoKwUnS7/clg9SXi+dXO2UwLs2eSBVXtN6YPzGhinV+bg+6k34NuvJQ1a3pDFEE7xJw3y0aY9J1k+kDELtlMevRMl7TKOnRLqRXuoCCYJof38ycQ4PLa/mHmJOu4MYCOs0zaktu1CRrzki/mh3hnzOP175h58Rg9Gj/PWm9QIoumktXvkXitV3aEH7smhMvQ90/NIIC2MM46SxErWifR2A7A7Tz7oG3mST1q3TL7fTQ7sPrkQp64G+P/46J8FcSNXxuaYI8u7w+WQ/UkVO7XqXmyNLZ72orQ2U+OuXvQXHOUeUXklNChgoAh+jU8Pp7vFTneCDP53AcpuZZRdsqk9k6tuoKSAz6mwE6aB657GArck4lioIFpP9hLPomyY6FCjXnb9WwT2qK33zOp6lgAt3hs1w4LyMinoi0szRtt+HfppM6iweIa7nKPC9RXGFuzlt7KlnyOmqKJoqeU=",
    "foo@b.ar",
]

openssh_keys = [key0, key1, rsa_key]


# Many test cases are from the ssh-keygen test code:
# https://archive.softwareheritage.org/
# swh:1:cnt:dae03706d8f0cb09fa8f8cd28f86d06c4693f0c9


class ParseTests(TestCase):
    def test_man_page_example(self):
        # Example "ALLOWED SIGNERS" file from ssh-keygen man page. Man page source:
        # https://archive.softwareheritage.org/
        # swh:1:cnt:06f0555a4ec01caf8daed84b8409dd8cb3278740

        text = StringIO(
            """\
# Comments allowed at start of line
user1@example.com,user2@example.com {} {} {}
# A certificate authority, trusted for all principals in a domain.
*@example.com cert-authority {} {}
# A key that is accepted only for file signing.
user2@example.com namespaces="file" {} {}
""".format(*rsa_key, *key0, *key1)
        )
        expect = [
            AllowedSigner("user1@example.com,user2@example.com", None, *rsa_key),
            AllowedSigner("*@example.com", {"cert-authority": ""}, *key0),
            AllowedSigner("user2@example.com", {"namespaces": "file"}, *key1),
        ]
        got = load_allowed_signers_file(text)
        self.assertEqual(expect, got)

    def test_no_options_and_quotes(self):
        text = StringIO(
            """\
foo@example.com {} {}
"foo@example.com" {} {}
""".format(*key0, *key0)
        )
        same = AllowedSigner("foo@example.com", None, *key0)
        expect = [same, same]
        self.assertEqual(expect, load_allowed_signers_file(text))

    def test_space_in_quotes(self):
        text = StringIO(
            """\
"ssh-keygen parses this" {} {}
""".format(*key0)
        )
        expect = [
            AllowedSigner("ssh-keygen parses this", None, *key0),
        ]
        self.assertEqual(expect, load_allowed_signers_file(text))

    def test_with_comments(self):
        text = StringIO(
            """\
foo@bar {} {} even without options ssh-keygen will ignore the end
""".format(*key1)
        )
        expect = [
            AllowedSigner(
                "foo@bar",
                None,
                *key1,
                "even without options ssh-keygen will ignore the end",
            )
        ]
        self.assertEqual(expect, load_allowed_signers_file(text))

    def test_two_namespaces(self):
        text = StringIO(
            """\
foo@b.ar namespaces="git,got" {} {}
""".format(*key1)
        )
        expect = [
            AllowedSigner(
                "foo@b.ar",
                {"namespaces": "git,got"},
                *key1,
            ),
        ]
        self.assertEqual(expect, load_allowed_signers_file(text))

    def test_dates(self):
        text = StringIO(
            """\
foo@b.ar valid-after="19801201",valid-before="20010201" {} {}
""".format(*key0)
        )
        expect = [
            AllowedSigner(
                "foo@b.ar",
                {"valid-after": "19801201", "valid-before": "20010201"},
                *key0,
            ),
        ]
        self.assertEqual(expect, load_allowed_signers_file(text))
