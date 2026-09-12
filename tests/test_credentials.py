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

from urllib.parse import urlparse

from dulwich.config import ConfigDict, StackedConfig
from dulwich.credentials import (
    match_partial_url,
    match_urls,
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
