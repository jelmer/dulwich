"""."""

import tempfile
from dulwich.cloud.gcs import GcsObjectStore
from dulwich.repo import Repo
from google.cloud import storage


client = storage.Client()
bucket = client.get_bucket('mybucket')

gcs_object_store = GcsObjectStore(bucket, 'path')
r = Repo.init_bare(tempfile.mkdtemp(), object_store=gcs_object_store)
