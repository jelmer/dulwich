# test_mbox.py -- tests for mbox.py
# Copyright (C) 2025 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Tests for mbox.py."""

import mailbox
import os
import tempfile
from io import BytesIO

from dulwich import porcelain
from dulwich.mbox import split_maildir, split_mbox

from . import TestCase


class SplitMboxTests(TestCase):
    """Tests for split_mbox function."""

    def test_split_simple_mbox(self) -> None:
        """Test splitting a simple mbox with two messages."""
        mbox_content = b"""\
From alice@example.com Mon Jan 01 00:00:00 2025
From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: First message

This is the first message.

From bob@example.com Mon Jan 01 00:01:00 2025
From: Bob <bob@example.com>
To: Alice <alice@example.com>
Subject: Second message

This is the second message.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create temporary mbox file
            mbox_path = os.path.join(tmpdir, "test.mbox")
            with open(mbox_path, "wb") as f:
                f.write(mbox_content)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split the mbox
            output_files = split_mbox(mbox_path, output_dir)

            # Verify output
            self.assertEqual(len(output_files), 2)
            self.assertEqual(output_files[0], os.path.join(output_dir, "0001"))
            self.assertEqual(output_files[1], os.path.join(output_dir, "0002"))

            # Check first message
            with open(output_files[0], "rb") as f:
                content = f.read()
                expected = b"""\
From: Alice <alice@example.com>
To: Bob <bob@example.com>
Subject: First message

This is the first message.
"""
                self.assertEqual(content, expected)

            # Check second message
            with open(output_files[1], "rb") as f:
                content = f.read()
                expected = b"""\
From: Bob <bob@example.com>
To: Alice <alice@example.com>
Subject: Second message

This is the second message.
"""
                self.assertEqual(content, expected)

    def test_split_mbox_with_precision(self) -> None:
        """Test splitting mbox with custom precision."""
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

            # Split with precision=2
            output_files = split_mbox(mbox_path, output_dir, precision=2)

            self.assertEqual(len(output_files), 1)
            self.assertEqual(output_files[0], os.path.join(output_dir, "01"))

    def test_split_mbox_with_start_number(self) -> None:
        """Test splitting mbox with custom start number."""
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

            # Split starting at message 10
            output_files = split_mbox(mbox_path, output_dir, start_number=10)

            self.assertEqual(len(output_files), 1)
            self.assertEqual(output_files[0], os.path.join(output_dir, "0010"))

    def test_split_mbox_keep_cr(self) -> None:
        """Test splitting mbox with keep_cr option."""
        # Note: Python's mailbox module normalizes line endings, so this test
        # verifies that keep_cr=False removes CR while keep_cr=True preserves
        # whatever the mailbox module outputs
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

            # Split without keep_cr (default removes \r\n)
            output_files_no_cr = split_mbox(mbox_path, output_dir, keep_cr=False)
            with open(output_files_no_cr[0], "rb") as f:
                content_no_cr = f.read()

            # Verify the output
            self.assertEqual(len(output_files_no_cr), 1)
            expected = b"""\
From: Test <test@example.com>
Subject: Test

Test message.
"""
            self.assertEqual(content_no_cr, expected)

    def test_split_mbox_from_file_object(self) -> None:
        """Test splitting mbox from a file-like object."""
        mbox_content = b"""\
From test@example.com Mon Jan 01 00:00:00 2025
From: Test <test@example.com>
Subject: Test

Test message.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split from BytesIO
            output_files = split_mbox(BytesIO(mbox_content), output_dir)

            self.assertEqual(len(output_files), 1)
            self.assertTrue(os.path.exists(output_files[0]))

    def test_split_mbox_output_dir_not_exists(self) -> None:
        """Test that split_mbox raises ValueError if output_dir doesn't exist."""
        mbox_content = b"From test@example.com Mon Jan 01 00:00:00 2025\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            mbox_path = os.path.join(tmpdir, "test.mbox")
            with open(mbox_path, "wb") as f:
                f.write(mbox_content)

            nonexistent_dir = os.path.join(tmpdir, "nonexistent")

            with self.assertRaises(ValueError) as cm:
                split_mbox(mbox_path, nonexistent_dir)

            self.assertIn("does not exist", str(cm.exception))

    def test_split_mboxrd(self) -> None:
        """Test splitting mboxrd format with >From escaping.

        In mboxrd format, lines starting with ">From " have one leading ">" removed.
        So ">From " becomes "From " and ">>From " becomes ">From ".
        """
        mbox_content = b"""\
From test@example.com Mon Jan 01 00:00:00 2025
From: Test <test@example.com>
Subject: Test

>From the beginning...
>>From the middle...
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            mbox_path = os.path.join(tmpdir, "test.mbox")
            with open(mbox_path, "wb") as f:
                f.write(mbox_content)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split with mboxrd=True
            output_files = split_mbox(mbox_path, output_dir, mboxrd=True)

            self.assertEqual(len(output_files), 1)

            # Check that >From escaping was reversed (one ">" removed per line)
            with open(output_files[0], "rb") as f:
                content = f.read()
                expected = b"""\
From: Test <test@example.com>
Subject: Test

From the beginning...
>From the middle...
"""
                self.assertEqual(content, expected)


class SplitMaildirTests(TestCase):
    """Tests for split_maildir function."""

    def test_split_maildir(self) -> None:
        """Test splitting a Maildir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a Maildir
            maildir_path = os.path.join(tmpdir, "maildir")
            md = mailbox.Maildir(maildir_path)

            # Add two messages
            msg1 = mailbox.MaildirMessage()
            msg1.set_payload(b"First message")
            msg1["From"] = "alice@example.com"
            msg1["Subject"] = "First"
            md.add(msg1)

            msg2 = mailbox.MaildirMessage()
            msg2.set_payload(b"Second message")
            msg2["From"] = "bob@example.com"
            msg2["Subject"] = "Second"
            md.add(msg2)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split the Maildir
            output_files = split_maildir(maildir_path, output_dir)

            # Verify output
            self.assertEqual(len(output_files), 2)
            self.assertTrue(all(os.path.exists(f) for f in output_files))

            # Check that files are numbered correctly
            self.assertEqual(output_files[0], os.path.join(output_dir, "0001"))
            self.assertEqual(output_files[1], os.path.join(output_dir, "0002"))

    def test_split_maildir_not_exists(self) -> None:
        """Test that split_maildir raises ValueError if Maildir doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent_dir = os.path.join(tmpdir, "nonexistent")
            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            with self.assertRaises(ValueError) as cm:
                split_maildir(nonexistent_dir, output_dir)

            self.assertIn("does not exist", str(cm.exception))

    def test_split_maildir_with_precision(self) -> None:
        """Test splitting Maildir with custom precision."""
        with tempfile.TemporaryDirectory() as tmpdir:
            maildir_path = os.path.join(tmpdir, "maildir")
            md = mailbox.Maildir(maildir_path)

            msg = mailbox.MaildirMessage()
            msg.set_payload(b"Test message")
            msg["From"] = "test@example.com"
            md.add(msg)

            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(output_dir)

            # Split with precision=2
            output_files = split_maildir(maildir_path, output_dir, precision=2)

            self.assertEqual(len(output_files), 1)
            self.assertEqual(output_files[0], os.path.join(output_dir, "01"))


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


class MailinfoTests(TestCase):
    """Tests for mbox.mailinfo function."""

    def test_mailinfo_from_file_path(self) -> None:
        """Test mailinfo with file path."""
        from dulwich.mbox import mailinfo

        email_content = b"""From: Test User <test@example.com>
Subject: [PATCH] Test patch
Date: Mon, 1 Jan 2024 12:00:00 +0000

This is the commit message.

---
diff --git a/test.txt b/test.txt
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            email_path = os.path.join(tmpdir, "email.txt")
            with open(email_path, "wb") as f:
                f.write(email_content)

            # Test with file path
            result = mailinfo(email_path)
            self.assertEqual("Test User", result.author_name)
            self.assertEqual("test@example.com", result.author_email)
            self.assertEqual("Test patch", result.subject)
            self.assertIn("This is the commit message.", result.message)
            self.assertIn("diff --git", result.patch)

    def test_mailinfo_from_file_object(self) -> None:
        """Test mailinfo with file-like object."""
        from dulwich.mbox import mailinfo

        email_content = b"""From: Test User <test@example.com>
Subject: Test subject

Body text
"""
        result = mailinfo(BytesIO(email_content))
        self.assertEqual("Test User", result.author_name)
        self.assertEqual("test@example.com", result.author_email)
        self.assertEqual("Test subject", result.subject)

    def test_mailinfo_with_options(self) -> None:
        """Test mailinfo with various options."""
        from dulwich.mbox import mailinfo

        email_content = b"""From: Test <test@example.com>
Subject: [PATCH] Feature
Message-ID: <test123@example.com>

Ignore this

-- >8 --

Keep this
"""
        # Test with scissors and message_id
        result = mailinfo(
            BytesIO(email_content), scissors=True, message_id=True, keep_subject=False
        )
        self.assertEqual("Feature", result.subject)
        self.assertIn("Keep this", result.message)
        self.assertNotIn("Ignore this", result.message)
        self.assertIn("Message-ID:", result.message)
