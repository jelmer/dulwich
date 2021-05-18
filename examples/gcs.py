import tempfile
from dulwich.cloud.gcs import GcsObjectStore
from google.cloud import storage
from dulwich.repo import Repo

client = storage.Client()
bucket = client.get_bucket('mybucket')

gcs_object_store = GcsObjectStore(bucket, 'path')
r = Repo.init_bare(tempfile.mkdtemp(), object_store=gcs_object_store)
