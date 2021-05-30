#!/usr/bin/python3

from dulwich.repo import Repo
from dulwich.cloud.gcs import GcsObjectStore

import tempfile

from google.cloud import storage

client = storage.Client()
bucket = client.get_bucket('mybucket')

gcs_object_store = GcsObjectStore(bucket, 'path')
r = Repo.init_bare(tempfile.mkdtemp(), object_store=gcs_object_store)
