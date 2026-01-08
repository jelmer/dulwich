# test_mbox.py -- tests for porcelain mbox (mailsplit)
# Copyright (C) 2020 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for porcelain mbox (mailsplit) functions."""

import mailbox
import os
import tempfile

from dulwich import porcelain

from .. import TestCase


class PorcelainMailsplitTests(TestCase):
    """Tests for porcelain.mailsplit function."""

    def test_mailsplit_mbox(self) -> None:
        """Test porcelain mailsplit with mbox file."""
        mbox_content = b"""\
From alice@example.com Mon Jan 01 00:00:00 2025
From: Alice <alice@example.com>
Subject: Test

Test message.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mbox_path = os.path.join(tmpdir, "test.mbox")
            with open(mbox_path, "wb") as f:
                f.write(mbox_content)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split using porcelain function
            output_files = porcelain.mailsplit(
                input_path=mbox_path, output_dir=output_dir
            )

            self.assertEqual(len(output_files), 1)
            self.assertEqual(output_files[0], os.path.join(output_dir, "0001"))

    def test_mailsplit_maildir(self) -> None:
        """Test porcelain mailsplit with Maildir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a Maildir
            maildir_path = os.path.join(tmpdir, "maildir")
            md = mailbox.Maildir(maildir_path)

            msg = mailbox.MaildirMessage()
            msg.set_payload(b"Test message")
            msg["From"] = "test@example.com"
            md.add(msg)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split using porcelain function with is_maildir=True
            output_files = porcelain.mailsplit(
                input_path=maildir_path, output_dir=output_dir, is_maildir=True
            )

            self.assertEqual(len(output_files), 1)
            self.assertTrue(os.path.exists(output_files[0]))

    def test_mailsplit_with_options(self) -> None:
        """Test porcelain mailsplit with various options."""
        mbox_content = b"""\
From test@example.com Mon Jan 01 00:00:00 2025
From: Test <test@example.com>
Subject: Test

Test message.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mbox_path = os.path.join(tmpdir, "test.mbox")
            with open(mbox_path, "wb") as f:
                f.write(mbox_content)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split with custom options
            output_files = porcelain.mailsplit(
                input_path=mbox_path,
                output_dir=output_dir,
                start_number=5,
                precision=3,
                keep_cr=True,
            )

            self.assertEqual(len(output_files), 1)
            self.assertEqual(output_files[0], os.path.join(output_dir, "005"))

    def test_mailsplit_mboxrd(self) -> None:
        """Test porcelain mailsplit with mboxrd format."""
        mbox_content = b"""\
From test@example.com Mon Jan 01 00:00:00 2025
From: Test <test@example.com>
Subject: Test

>From quoted text
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mbox_path = os.path.join(tmpdir, "test.mbox")
            with open(mbox_path, "wb") as f:
                f.write(mbox_content)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split with mboxrd=True
            output_files = porcelain.mailsplit(
                input_path=mbox_path, output_dir=output_dir, mboxrd=True
            )

            self.assertEqual(len(output_files), 1)

            # Verify >From escaping was reversed
            with open(output_files[0], "rb") as f:
                content = f.read()
                expected = b"""\
From: Test <test@example.com>
Subject: Test

From quoted text
"""
                self.assertEqual(content, expected)

    def test_mailsplit_maildir_requires_path(self) -> None:
        """Test that mailsplit raises ValueError when is_maildir=True but no input_path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            with self.assertRaises(ValueError) as cm:
                porcelain.mailsplit(
                    input_path=None, output_dir=output_dir, is_maildir=True
                )

            self.assertIn("required", str(cm.exception).lower())
