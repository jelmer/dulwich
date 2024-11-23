#!/usr/bin/python3
# SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

import tempfile

from google.cloud import storage

from dulwich.cloud.gcs import GcsObjectStore
from dulwich.repo import Repo

client = storage.Client()
bucket = client.get_bucket("mybucket")

gcs_object_store = GcsObjectStore(bucket, "path")
r = Repo.init_bare(tempfile.mkdtemp(), object_store=gcs_object_store)
