# test_credentials.py -- tests for credentials.py

# Copyright (C) 2022 Daniele Trifirò <daniele@iterative.ai>
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

import os
import shutil
import sys
import tempfile
from urllib.parse import urlparse

from dulwich.config import ConfigDict, StackedConfig
from dulwich.credentials import (
    CredentialHelper,
    CredentialNotFound,
    InvalidCredentialDescription,
    fill_credential,
    format_credential_description,
    helpers_for_url,
    match_partial_url,
    match_urls,
    parse_credential_description,
    url_for_credential,
    urlmatch_credential_sections,
)

from . import TestCase


class TestCredentialHelpersUtils(TestCase):
    def test_match_urls(self) -> None:
        url = urlparse("https://github.com/jelmer/dulwich/")
        url_1 = urlparse("https://github.com/jelmer/dulwich")
        url_2 = urlparse("https://github.com/jelmer")
        url_3 = urlparse("https://github.com")
        self.assertTrue(match_urls(url, url_1))
        self.assertTrue(match_urls(url, url_2))
        self.assertTrue(match_urls(url, url_3))

        non_matching = urlparse("https://git.sr.ht/")
        self.assertFalse(match_urls(url, non_matching))

    def test_match_urls_path_boundary(self) -> None:
        prefix = urlparse("https://example.com/private")
        self.assertTrue(match_urls(urlparse("https://example.com/private"), prefix))
        self.assertTrue(
            match_urls(urlparse("https://example.com/private/repo"), prefix)
        )
        # A sibling path that merely shares a string prefix must not match,
        # otherwise path-scoped credentials leak to it.
        self.assertFalse(
            match_urls(urlparse("https://example.com/private-evil"), prefix)
        )
        self.assertFalse(match_urls(urlparse("https://example.com/priv"), prefix))

    def test_match_partial_url(self) -> None:
        url = urlparse("https://github.com/jelmer/dulwich/")
        self.assertTrue(match_partial_url(url, "github.com"))
        self.assertFalse(match_partial_url(url, "github.com/jelmer/"))
        self.assertTrue(match_partial_url(url, "github.com/jelmer/dulwich"))
        self.assertFalse(match_partial_url(url, "github.com/jel"))
        self.assertFalse(match_partial_url(url, "github.com/jel/"))

    def test_match_partial_url_with_scheme(self) -> None:
        """Test match_partial_url with a URL that includes a scheme."""
        url = urlparse("https://github.com/jelmer/dulwich/")

        # Match with same scheme
        self.assertTrue(match_partial_url(url, "https://github.com"))
        self.assertTrue(match_partial_url(url, "https://github.com/jelmer/dulwich"))

        # No match with different scheme
        self.assertFalse(match_partial_url(url, "http://github.com"))
        self.assertFalse(match_partial_url(url, "ssh://github.com"))

    def test_urlmatch_credential_sections(self) -> None:
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

    def test_urlmatch_credential_sections_stacked_config(self) -> None:
        """A StackedConfig is accepted; its backends are searched in order."""
        first = ConfigDict()
        first.set((b"credential", "https://github.com"), b"helper", "first")
        second = ConfigDict()
        second.set((b"credential", "https://git.sr.ht"), b"helper", "second")
        second.set(b"credential", b"helper", "fallback")
        config = StackedConfig([first, second])

        self.assertEqual(
            [
                (b"credential", b"https://github.com"),
                (b"credential",),
            ],
            list(urlmatch_credential_sections(config, "https://github.com")),
        )
        self.assertEqual(
            [
                (b"credential", b"https://git.sr.ht"),
                (b"credential",),
            ],
            list(urlmatch_credential_sections(config, "https://git.sr.ht")),
        )

    def test_urlmatch_credential_sections_ignores_backend_encoding(self) -> None:
        """Subsections decode as UTF-8 whatever a backend declares.

        Reading a single ``encoding`` off the config and applying it to every
        section decodes one backend's bytes with another backend's codec once
        more than one backend is in play.
        """
        host = "ü.example.com"
        latin1 = ConfigDict(encoding="latin-1")
        latin1.set((b"credential", b"https://other.example.net"), b"helper", "a")
        utf8 = ConfigDict(encoding="utf-8")
        utf8.set((b"credential", ("https://" + host).encode()), b"helper", "b")

        for backends in ([latin1, utf8], [utf8, latin1]):
            self.assertEqual(
                [(b"credential", ("https://" + host).encode())],
                list(
                    urlmatch_credential_sections(
                        StackedConfig(backends), "https://" + host
                    )
                ),
            )

    def test_urlmatch_credential_sections_undecodable_subsection(self) -> None:
        """A subsection that is not valid UTF-8 does not abort the lookup."""
        config = ConfigDict()
        config.set(
            (b"credential", b"https://" + bytes([0xFF]) + b".example.com"),
            b"helper",
            "broken",
        )
        config.set(b"credential", b"helper", "fallback")

        self.assertEqual(
            [(b"credential",)],
            list(urlmatch_credential_sections(config, "https://github.com")),
        )

    def test_urlmatch_credential_sections_with_other_sections(self) -> None:
        """Test that non-credential sections are skipped."""
        config = ConfigDict()
        config.set((b"credential", "https://github.com"), b"helper", "foo")
        config.set(b"credential", b"helper", "bar")
        # Add some non-credential sections
        config.set(b"user", b"name", "Test User")
        config.set(b"core", b"editor", "vim")

        # Should only return credential sections
        result = list(urlmatch_credential_sections(config, "https://github.com"))
        self.assertEqual(
            result,
            [
                (b"credential", b"https://github.com"),
                (b"credential",),
            ],
        )


# A helper that answers "get" from its own argv and logs every other
# operation, so a test can assert both what came back and what was called.
_HELPER_SOURCE = r"""import sys

# git appends the operation after the configured command, so it is the
# last argument, not the first.
operation = sys.argv[-1]
answers = sys.argv[1:-1]
if operation == "get":
    if answers == ["decline"]:
        sys.exit(0)
    if answers == ["fail"]:
        sys.exit(1)
    for answer in answers:
        print(answer)
else:
    with open(__file__ + ".log", "a") as f:
        f.write(operation + "\n")
"""


class ParseCredentialDescriptionTests(TestCase):
    """Reading the ``key=value`` description format."""

    def test_plain_keys(self) -> None:
        self.assertEqual(
            {"protocol": "https", "host": "example.com", "username": "bob"},
            parse_credential_description(
                ["protocol=https", "host=example.com", "username=bob"]
            ),
        )

    def test_a_blank_line_ends_the_description(self) -> None:
        """Two descriptions may share one stream, so the terminator matters."""
        self.assertEqual(
            {"host": "first"},
            parse_credential_description(["host=first", "", "host=second"]),
        )

    def test_trailing_newlines_are_stripped(self) -> None:
        self.assertEqual(
            {"host": "example.com"},
            parse_credential_description(["host=example.com\n", "\n"]),
        )

    def test_a_url_is_expanded_into_components(self) -> None:
        self.assertEqual(
            {
                "protocol": "https",
                "host": "example.com",
                "path": "repo.git",
                "username": "bob",
                "password": "s3cret",
            },
            parse_credential_description(
                ["url=https://bob:s3cret@example.com/repo.git"]
            ),
        )

    def test_a_port_stays_with_the_host(self) -> None:
        self.assertEqual(
            {"protocol": "https", "host": "example.com:8443", "path": "x"},
            parse_credential_description(["url=https://example.com:8443/x"]),
        )

    def test_a_bare_root_path_is_not_a_path(self) -> None:
        """`https://host/` describes no path, and reporting "" would not match."""
        self.assertEqual(
            {"protocol": "https", "host": "example.com"},
            parse_credential_description(["url=https://example.com/"]),
        )

    def test_a_later_key_overrides_the_url(self) -> None:
        """gitcredentials(7) gives an explicit key precedence over `url`."""
        self.assertEqual(
            "alice",
            parse_credential_description(
                ["url=https://bob@example.com", "username=alice"]
            )["username"],
        )

    def test_an_empty_value_is_kept(self) -> None:
        self.assertEqual({"username": ""}, parse_credential_description(["username="]))

    def test_a_value_may_contain_equals_signs(self) -> None:
        self.assertEqual(
            {"password": "a=b=c"}, parse_credential_description(["password=a=b=c"])
        )

    def test_a_line_without_an_equals_sign_is_rejected(self) -> None:
        self.assertRaises(
            InvalidCredentialDescription,
            parse_credential_description,
            ["not-a-pair"],
        )


class FormatCredentialDescriptionTests(TestCase):
    """Writing the description format."""

    def test_terminates_with_a_blank_line(self) -> None:
        self.assertEqual(
            "protocol=https\nhost=example.com\n\n",
            format_credential_description({"protocol": "https", "host": "example.com"}),
        )

    def test_empty_credential(self) -> None:
        self.assertEqual("\n", format_credential_description({}))

    def test_a_newline_in_a_value_is_rejected(self) -> None:
        """A newline would be read back as a separate attribute.

        That is how a hostile value in one field becomes an injected
        attribute in another, so it has to fail loudly rather than round-trip
        into something else.
        """
        with self.assertRaises(ValueError):
            format_credential_description({"password": "a\nhost=evil.example.com"})

    def test_a_nul_in_a_value_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            format_credential_description({"password": "a\x00b"})

    def test_round_trips(self) -> None:
        credential = {"protocol": "https", "host": "example.com", "username": "bob"}
        self.assertEqual(
            credential,
            parse_credential_description(
                format_credential_description(credential).splitlines()
            ),
        )


class UrlForCredentialTests(TestCase):
    """Rebuilding a URL from attributes."""

    def test_protocol_and_host(self) -> None:
        self.assertEqual(
            "https://example.com",
            url_for_credential({"protocol": "https", "host": "example.com"}),
        )

    def test_with_a_path(self) -> None:
        self.assertEqual(
            "https://example.com/repo.git",
            url_for_credential(
                {"protocol": "https", "host": "example.com", "path": "repo.git"}
            ),
        )

    def test_without_a_host_there_is_no_url(self) -> None:
        """Returning a partial URL here would match the wrong config section."""
        self.assertIsNone(url_for_credential({"protocol": "https"}))
        self.assertIsNone(url_for_credential({"host": "example.com"}))


class CredentialHelperArgvTests(TestCase):
    """Turning a `credential.helper` value into a command line."""

    def test_a_bare_word_names_a_git_credential_program(self) -> None:
        self.assertEqual(
            (["git-credential-store", "get"], False),
            CredentialHelper("store")._argv("get"),
        )

    def test_a_shell_command(self) -> None:
        self.assertEqual(
            (["echo hello store"], True),
            CredentialHelper("!echo hello")._argv("store"),
        )

    def test_a_path_is_used_as_written(self) -> None:
        argv, shell = CredentialHelper("./helpers/mine")._argv("erase")
        self.assertFalse(shell)
        self.assertEqual(["./helpers/mine", "erase"], argv)

    def test_an_absolute_path_is_used_as_written(self) -> None:
        path = os.path.join(os.sep, "usr", "bin", "mine")
        self.assertEqual((([path, "get"]), False), CredentialHelper(path)._argv("get"))

    def test_equality_is_by_command(self) -> None:
        self.assertEqual(CredentialHelper("store"), CredentialHelper("store"))
        self.assertNotEqual(CredentialHelper("store"), CredentialHelper("cache"))
        self.assertEqual(1, len({CredentialHelper("store"), CredentialHelper("store")}))

    def test_a_missing_program_is_not_available(self) -> None:
        self.assertFalse(CredentialHelper("no-such-helper-anywhere").is_available())

    def test_a_shell_command_is_always_considered_available(self) -> None:
        """There is no program name to look up; only running it can tell."""
        self.assertTrue(CredentialHelper("!echo").is_available())


class HelperRunTests(TestCase):
    """Running a real helper process."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.helper_path = os.path.join(self.test_dir, "helper.py")
        with open(self.helper_path, "w") as f:
            f.write(_HELPER_SOURCE)

    def _helper(self, *answers: str) -> CredentialHelper:
        command = " ".join([sys.executable, self.helper_path, *answers])
        return CredentialHelper("!" + command)

    def _log(self) -> list[str]:
        try:
            with open(self.helper_path + ".log") as f:
                return f.read().split()
        except FileNotFoundError:
            return []

    def test_get_returns_what_the_helper_printed(self) -> None:
        self.assertEqual(
            {"username": "bob", "password": "hunter2"},
            self._helper("username=bob", "password=hunter2").get(
                {"protocol": "https", "host": "example.com"}
            ),
        )

    def test_a_helper_that_declines_returns_nothing(self) -> None:
        self.assertEqual({}, self._helper("decline").get({"host": "example.com"}))

    def test_a_failing_helper_returns_none(self) -> None:
        """A non-zero exit must not be mistaken for an empty answer."""
        self.assertIsNone(self._helper("fail").get({"host": "example.com"}))

    def test_a_missing_helper_returns_none(self) -> None:
        self.assertIsNone(
            CredentialHelper("no-such-helper-anywhere").get({"host": "example.com"})
        )

    def test_undocumented_attributes_from_a_helper_are_dropped(self) -> None:
        """A helper must not be able to rewrite where the credential goes.

        Accepting an arbitrary `host` back from a helper would let one helper
        redirect the credential a later helper -- or the caller -- then uses.
        """
        supplied = self._helper("username=bob", "password=p", "surprise=1").get(
            {"protocol": "https", "host": "example.com"}
        )
        self.assertEqual({"username": "bob", "password": "p"}, supplied)

    def test_store_and_erase_reach_the_helper(self) -> None:
        helper = self._helper()
        helper.store({"protocol": "https", "host": "example.com"})
        helper.erase({"protocol": "https", "host": "example.com"})
        self.assertEqual(["store", "erase"], self._log())


class HelpersForUrlTests(TestCase):
    """Reading `credential.helper` out of the configuration."""

    def test_no_configuration(self) -> None:
        self.assertEqual([], helpers_for_url(ConfigDict(), "https://example.com"))

    def test_a_general_helper_applies_to_every_url(self) -> None:
        config = ConfigDict()
        config.set((b"credential",), b"helper", b"store")
        self.assertEqual(
            [CredentialHelper("store")],
            helpers_for_url(config, "https://example.com"),
        )

    def test_a_url_scoped_helper_only_applies_to_that_url(self) -> None:
        config = ConfigDict()
        config.set((b"credential", b"https://example.com"), b"helper", b"store")
        self.assertEqual(
            [CredentialHelper("store")],
            helpers_for_url(config, "https://example.com"),
        )
        self.assertEqual([], helpers_for_url(config, "https://other.example.org"))

    def test_an_empty_value_resets_the_list(self) -> None:
        """This is git's documented way to drop a helper set more globally.

        Without it a repository could not opt out of a system-wide helper.
        """
        config = ConfigDict()
        config.set((b"credential",), b"helper", b"store")
        config.set((b"credential", b"https://example.com"), b"helper", b"")
        self.assertEqual([], helpers_for_url(config, "https://example.com"))


class FillCredentialTests(TestCase):
    """Completing a credential from configuration and helpers."""

    def setUp(self) -> None:
        super().setUp()
        self.test_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.test_dir)
        self.helper_path = os.path.join(self.test_dir, "helper.py")
        with open(self.helper_path, "w") as f:
            f.write(_HELPER_SOURCE)

    def _command(self, *answers: str) -> bytes:
        return ("!" + " ".join([sys.executable, self.helper_path, *answers])).encode()

    def test_fills_from_a_helper(self) -> None:
        config = ConfigDict()
        config.set(
            (b"credential",), b"helper", self._command("username=bob", "password=p")
        )
        self.assertEqual(
            {
                "protocol": "https",
                "host": "example.com",
                "username": "bob",
                "password": "p",
            },
            fill_credential(config, {"protocol": "https", "host": "example.com"}),
        )

    def test_a_configured_username_seeds_the_request(self) -> None:
        config = ConfigDict()
        config.set((b"credential",), b"username", b"alice")
        config.set((b"credential",), b"helper", self._command("password=p"))
        filled = fill_credential(config, {"protocol": "https", "host": "example.com"})
        self.assertEqual("alice", filled["username"])
        self.assertEqual("p", filled["password"])

    def test_a_supplied_username_beats_the_configured_one(self) -> None:
        config = ConfigDict()
        config.set((b"credential",), b"username", b"alice")
        config.set((b"credential",), b"helper", self._command("password=p"))
        filled = fill_credential(
            config,
            {"protocol": "https", "host": "example.com", "username": "bob"},
        )
        self.assertEqual("bob", filled["username"])

    def test_a_later_helper_completes_what_an_earlier_one_left(self) -> None:
        config = ConfigDict()
        # add(), not set(): `credential.helper` is a multivar, and git
        # consults each configured helper in turn.
        config.add((b"credential",), b"helper", self._command("username=bob"))
        config.add((b"credential",), b"helper", self._command("password=p"))
        filled = fill_credential(config, {"protocol": "https", "host": "example.com"})
        self.assertEqual("bob", filled["username"])
        self.assertEqual("p", filled["password"])

    def test_no_helper_at_all(self) -> None:
        self.assertRaises(
            CredentialNotFound,
            fill_credential,
            ConfigDict(),
            {"protocol": "https", "host": "example.com"},
        )

    def test_a_helper_that_supplies_only_a_username(self) -> None:
        """A username without a password is not a usable credential."""
        config = ConfigDict()
        config.set((b"credential",), b"helper", self._command("username=bob"))
        self.assertRaises(
            CredentialNotFound,
            fill_credential,
            config,
            {"protocol": "https", "host": "example.com"},
        )
