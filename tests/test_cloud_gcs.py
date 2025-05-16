#!/usr/bin/python3
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

"""Tests for dulwich.cloud.gcs."""

import unittest
from unittest.mock import MagicMock, patch

from dulwich.cloud.gcs import GcsObjectStore


class GcsObjectStoreTests(unittest.TestCase):
    """Tests for the GcsObjectStore class."""

    def setUp(self):
        self.mock_bucket = MagicMock()
        self.store = GcsObjectStore(self.mock_bucket, subpath="git")

    def test_init(self):
        """Test initialization and repr."""
        self.assertEqual(self.mock_bucket, self.store.bucket)
        self.assertEqual("git", self.store.subpath)
        self.assertIn("GcsObjectStore", repr(self.store))
        self.assertIn("git", repr(self.store))

    def test_remove_pack(self):
        """Test _remove_pack method."""
        self.store._remove_pack("pack-1234")
        self.mock_bucket.delete_blobs.assert_called_once()
        args = self.mock_bucket.delete_blobs.call_args[0][0]
        self.assertEqual(
            sorted(args), sorted(["git/pack-1234.pack", "git/pack-1234.idx"])
        )

    def test_iter_pack_names(self):
        """Test _iter_pack_names method."""
        # Create mock blobs with the expected attributes
        mock_blob1 = MagicMock()
        mock_blob1.name = "git/pack-1234.pack"
        mock_blob2 = MagicMock()
        mock_blob2.name = "git/pack-1234.idx"
        mock_blob3 = MagicMock()
        mock_blob3.name = "git/pack-5678.pack"
        # Only pack-1234 has both .pack and .idx files

        self.mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        pack_names = list(self.store._iter_pack_names())
        self.assertEqual(["pack-1234"], pack_names)

        # Verify that list_blobs was called with the correct prefix
        self.mock_bucket.list_blobs.assert_called_once_with(prefix="git")

    def test_load_pack_data(self):
        """Test _load_pack_data method."""
        # Create a mock blob that will simulate downloading a pack file
        mock_blob = MagicMock()
        self.mock_bucket.blob.return_value = mock_blob

        # We need to patch PackData to avoid actual pack format validation
        with patch("dulwich.cloud.gcs.PackData") as mock_pack_data_cls:
            mock_pack_data = MagicMock()
            mock_pack_data_cls.return_value = mock_pack_data

            # Mock the download_to_file to actually write some data
            def mock_download_to_file(file_obj):
                file_obj.write(b"not-a-real-pack-file")

            mock_blob.download_to_file.side_effect = mock_download_to_file

            # Call the method under test
            result = self.store._load_pack_data("pack-1234")

            # Check results
            self.assertEqual(mock_pack_data, result)
            self.mock_bucket.blob.assert_called_once_with("git/pack-1234.pack")
            mock_blob.download_to_file.assert_called_once()

            # Verify PackData was called with the correct parameters
            mock_pack_data_cls.assert_called_once()
            args, _ = mock_pack_data_cls.call_args
            self.assertEqual("pack-1234.pack", args[0])

    def test_load_pack_index(self):
        """Test _load_pack_index method."""
        # We need to patch load_pack_index_file since we don't want to test its internals
        with patch("dulwich.cloud.gcs.load_pack_index_file") as mock_load:
            # Create a mock blob that will simulate downloading an index file
            mock_blob = MagicMock()
            self.mock_bucket.blob.return_value = mock_blob

            # Mock the download_to_file to actually write some data
            def mock_download_to_file(file_obj):
                file_obj.write(b"index-file-content")

            mock_blob.download_to_file.side_effect = mock_download_to_file

            # Setup the return value for the mocked load_pack_index_file
            mock_index = MagicMock()
            mock_load.return_value = mock_index

            # Call the method under test
            result = self.store._load_pack_index("pack-1234")

            # Check results
            self.assertEqual(mock_index, result)
            self.mock_bucket.blob.assert_called_once_with("git/pack-1234.idx")
            mock_blob.download_to_file.assert_called_once()
            mock_load.assert_called_once()
            # Verify the correct filename is passed
            self.assertEqual("pack-1234.idx", mock_load.call_args[0][0])

    def test_get_pack(self):
        """Test _get_pack method."""
        with patch("dulwich.cloud.gcs.Pack") as mock_pack_cls:
            mock_pack = MagicMock()
            mock_pack_cls.from_lazy_objects.return_value = mock_pack

            result = self.store._get_pack("pack-1234")

            self.assertEqual(mock_pack, result)
            mock_pack_cls.from_lazy_objects.assert_called_once()

            # Get the lazy loaders that were passed
            args = mock_pack_cls.from_lazy_objects.call_args[0]
            self.assertEqual(2, len(args))

            # They should be callables
            self.assertTrue(callable(args[0]))
            self.assertTrue(callable(args[1]))

    def test_upload_pack(self):
        """Test _upload_pack method."""
        # Create mock blob objects
        mock_idx_blob = MagicMock()
        mock_data_blob = MagicMock()

        # Configure the bucket's blob method to return the appropriate mock
        def mock_blob(path):
            if path.endswith(".idx"):
                return mock_idx_blob
            elif path.endswith(".pack"):
                return mock_data_blob
            else:
                self.fail(f"Unexpected blob path: {path}")

        self.mock_bucket.blob.side_effect = mock_blob

        # Create mock file objects
        mock_index_file = MagicMock()
        mock_pack_file = MagicMock()

        # Call the method under test
        self.store._upload_pack("pack-1234", mock_pack_file, mock_index_file)

        # Verify the correct paths were used
        self.mock_bucket.blob.assert_any_call("git/pack-1234.idx")
        self.mock_bucket.blob.assert_any_call("git/pack-1234.pack")

        # Verify the uploads were called with the right files
        mock_idx_blob.upload_from_file.assert_called_once_with(mock_index_file)
        mock_data_blob.upload_from_file.assert_called_once_with(mock_pack_file)
