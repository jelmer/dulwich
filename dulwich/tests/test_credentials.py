# test_credentials.py -- tests for credentials.py

# Copyright (C) 2022 Daniele Trifirò <daniele@iterative.ai>
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

import os
import shutil
import subprocess
import sys
import tempfile
from unittest import mock, skipIf, skipUnless
from urllib.parse import urlparse

from dulwich.config import ConfigDict
from dulwich.credentials import (CredentialHelper, CredentialNotFoundError,
                                 get_credentials_from_helper,
                                 match_partial_url, match_urls,
                                 urlmatch_credential_sections)
from dulwich.tests import TestCase


class CredentialHelperTests(TestCase):
    def test_prepare_command_shell(self):
        command = """!f() { echo foo}; f"""

        helper = CredentialHelper(command)
        self.assertEqual(helper._prepare_command(), command[1:])

    def test_prepare_command_abspath(self):
        executable_path = os.path.join(os.sep, "path", "to", "executable")

        helper = CredentialHelper(executable_path)
        self.assertEqual(helper._prepare_command(), [executable_path])

    @skipIf(sys.platform == "win32", reason="Path handling on windows is different")
    def test_prepare_command_abspath_extra_args(self):
        executable_path = os.path.join(os.sep, "path", "to", "executable")
        helper = CredentialHelper(
            f'{executable_path} --foo bar --quz "arg with spaces"'
        )
        self.assertEqual(
            helper._prepare_command(),
            [executable_path, "--foo", "bar", "--quz", "arg with spaces"],
        )

    @mock.patch("shutil.which")
    def test_prepare_command_in_path(self, which):
        which.return_value = True

        helper = CredentialHelper("foo")
        self.assertEqual(helper._prepare_command(), ["git-credential-foo"])

    @mock.patch("subprocess.check_output")
    def test_prepare_command_cli_git_helpers(self, check_output):
        git_exec_path = os.path.join(os.sep, "path", "to", "git-core")
        check_output.return_value = git_exec_path

        def which_mock(arg, **kwargs):
            if arg == "git" or "path" in kwargs:
                return True
            return False

        helper = CredentialHelper("foo")
        expected = [os.path.join(git_exec_path, "git-credential-foo")]

        with mock.patch.object(shutil, "which", new=which_mock):
            self.assertEqual(helper._prepare_command(), expected)

    @skipIf(sys.platform == "win32", reason="Path handling on windows is different")
    @mock.patch("shutil.which")
    def test_prepare_command_extra_args(self, which):
        which.return_value = True

        helper = CredentialHelper('foo --bar baz --quz "arg with spaces"')
        command = helper._prepare_command()
        self.assertEqual(
            command,
            [
                "git-credential-foo",
                "--bar",
                "baz",
                "--quz",
                "arg with spaces",
            ],
        )

    def test_get_nonexisting_executable(self):
        helper = CredentialHelper("nonexisting")
        with self.assertRaises(CredentialNotFoundError):
            helper.get(hostname="github.com")

    def test_get_nonexisting_executable_abspath(self):
        path = os.path.join(os.sep, "path", "to", "nonexisting")
        helper = CredentialHelper(path)
        with self.assertRaises(CredentialNotFoundError):
            helper.get(hostname="github.com")

    @mock.patch("shutil.which")
    @mock.patch("subprocess.run")
    def test_get(self, run, which):
        run.return_value.stdout = os.linesep.join(
            ["username=username", "password=password", ""]
        ).encode("UTF-8")
        which.return_value = True

        helper = CredentialHelper("foo")
        username, password = helper.get(hostname="example.com")
        self.assertEqual(username, b"username")
        self.assertEqual(password, b"password")

    @skipIf(
        os.name == "nt", reason="On Windows, this only will work for git-bash or WSL2"
    )
    def test_get_shell(self):
        command = """!f() { printf "username=username\npassword=password"; }; f"""
        helper = CredentialHelper(command)
        username, password = helper.get(hostname="example.com")
        self.assertEqual(username, b"username")
        self.assertEqual(password, b"password")

    @mock.patch("subprocess.run")
    def test_get_failing_command(self, run):
        run.return_value.stderr = b"error message"
        run.return_value.returncode = 1
        with self.assertRaises(CredentialNotFoundError, msg=b"error message"):
            CredentialHelper("dummy").get(hostname="github.com")

    @mock.patch("shutil.which")
    @mock.patch("subprocess.run")
    def test_get_missing_username(self, run, which):
        run.return_value.stdout = b"password=password"
        which.return_value = True
        with self.assertRaises(CredentialNotFoundError):
            CredentialHelper("dummy").get(hostname="github.com")

    @mock.patch("shutil.which")
    @mock.patch("subprocess.run")
    def test_get_missing_password(self, run, which):
        run.return_value.stdout = b"username=username"
        which.return_value = True
        with self.assertRaises(CredentialNotFoundError):
            CredentialHelper("dummy").get(hostname="github.com")

    @mock.patch("shutil.which")
    @mock.patch("subprocess.run")
    def test_get_malformed_output(self, run, which):
        run.return_value.stdout = os.linesep.join(["username", "password", ""]).encode(
            "UTF-8"
        )
        which.return_value = True

        with self.assertRaises(CredentialNotFoundError):
            CredentialHelper("dummy").get(hostname="github.com")

    def test_store(self):
        with self.assertRaises(NotImplementedError):
            CredentialHelper("dummy").store()

    def test_erase(self):
        with self.assertRaises(NotImplementedError):
            CredentialHelper("dummy").erase()

    def test_get_credentials_from_helper_no_helpers(self):
        config = ConfigDict()
        with self.assertRaises(CredentialNotFoundError):
            get_credentials_from_helper("https://git.sr.ht", config)


@skipUnless(shutil.which("git"), "requires git cli")
class CredentialHelperCredentialStore(TestCase):
    """tests CredentialHandler with `git credential-store`"""

    def setUp(self):
        super().setUp()
        self.encoding = sys.getdefaultencoding()
        self.store_path = os.path.join(
            tempfile.gettempdir(), "dulwich-git-credential-store-test"
        )
        self.git_exec_path = subprocess.check_output(
            ["git", "--exec-path"], universal_newlines=True
        ).strip()

        self.urls = (
            ("https://example.com", "username", "password"),
            ("https://example1.com", "username1", "password1"),
        )

        for url, username, password in self.urls:
            subprocess_in = os.linesep.join(
                [f"url={url}", f"username={username}", f"password={password}", ""]
            ).encode(self.encoding)
            subprocess.run(
                f"git credential-store --file {self.store_path} store".split(" "),
                input=subprocess_in,
                check=True,
            )

        self.helper = CredentialHelper(f"store --file {self.store_path}")

    def tearDown(self):
        super().tearDown()
        os.unlink(self.store_path)

    def test_init(self):
        expected = [
            os.path.join(self.git_exec_path, "git-credential-store"),
            "--file",
            self.store_path,
        ]
        self.assertEqual(self.helper._prepare_command(), expected)

    def test_get(self):
        for url, expected_username, expected_password in self.urls:
            parsed = urlparse(url)
            username, password = self.helper.get(
                protocol=parsed.scheme,
                hostname=parsed.hostname,
                port=parsed.port,
                username=parsed.username,
            )
            self.assertEqual(username, expected_username.encode(self.encoding))
            self.assertEqual(password, expected_password.encode(self.encoding))

    def test_missing(self):
        with self.assertRaises(CredentialNotFoundError):
            self.helper.get(protocol="https://", hostname="dummy.com")

    def test_store(self):
        with self.assertRaises(NotImplementedError):
            self.helper.store()

    def test_erase(self):
        with self.assertRaises(NotImplementedError):
            self.helper.erase()


class TestCredentialHelpersUtils(TestCase):
    def test_match_urls(self):
        url = urlparse("https://github.com/jelmer/dulwich/")
        url_1 = urlparse("https://github.com/jelmer/dulwich")
        url_2 = urlparse("https://github.com/jelmer")
        url_3 = urlparse("https://github.com")
        self.assertTrue(match_urls(url, url_1))
        self.assertTrue(match_urls(url, url_2))
        self.assertTrue(match_urls(url, url_3))

        non_matching = urlparse("https://git.sr.ht/")
        self.assertFalse(match_urls(url, non_matching))

    def test_match_partial_url(self):
        url = urlparse("https://github.com/jelmer/dulwich/")
        self.assertTrue(match_partial_url(url, "github.com"))
        self.assertFalse(match_partial_url(url, "github.com/jelmer/"))
        self.assertTrue(match_partial_url(url, "github.com/jelmer/dulwich"))
        self.assertFalse(match_partial_url(url, "github.com/jel"))
        self.assertFalse(match_partial_url(url, "github.com/jel/"))

    def test_urlmatch_credential_sections(self):
        config = ConfigDict()
        config.set((b"credential", "https://github.com"), b"helper", "foo")
        config.set((b"credential", "git.sr.ht"), b"helper", "foo")
        config.set(b"credential", b"helper", "bar")

        self.assertEqual(
            list(urlmatch_credential_sections(config, "https://github.com")),
            [
                (b"credential", b"https://github.com"),
                (b"credential",),
            ],
        )

        self.assertEqual(
            list(urlmatch_credential_sections(config, "https://git.sr.ht")),
            [
                (b"credential", b"git.sr.ht"),
                (b"credential",),
            ],
        )

        self.assertEqual(
            list(urlmatch_credential_sections(config, "missing_url")),
            [(b"credential",)],
        )
