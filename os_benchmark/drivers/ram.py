"""
In-memory driver keeping buckets and objects in RAM.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  myRamProfile:
    driver: ram
"""
import io
from shutil import copyfileobj
from urllib.parse import urlparse
from os_benchmark.drivers import base, errors

class Driver(base.BaseDriver):
    def __init__(self):
        self.buckets = {}

    def list_buckets(self, **kwargs):
        return [{'id': b} for b in self.buckets]

    def delete_bucket(self, bucket_id, **kwargs):
        self.buckets.pop(bucket_id, None)

    def create_bucket(self, name, **kwargs):
        self.buckets[name] = {}
        return {'id': name}

    def list_objects(self, bucket_id, **kwargs):
        if bucket_id not in self.buckets:
            raise errors.DriverBucketUnfoundError("Bucket not found")
        return list(self.buckets[bucket_id])

    def upload(self, bucket_id, name, content, acl='public-read', **kwargs):
        if bucket_id not in self.buckets:
            raise errors.DriverBucketUnfoundError("Bucket not found")

        stored = io.BytesIO()
        copyfileobj(content, stored)
        stored.seek(0)

        self.buckets[bucket_id][name] = stored
        return {'name': name}

    def delete_object(self, bucket_id, name, **kwargs):
        if bucket_id not in self.buckets:
            raise errors.DriverBucketUnfoundError("Bucket not found")
        self.buckets[bucket_id].pop(name, None)

    def get_url(self, bucket_id, name, **kwargs):
        return 'ram://%s/%s' % (bucket_id, name)

    def download(self, url, block_size=65536, **kwargs):
        parsed_url = urlparse(url)

        bucket_id = parsed_url.netloc
        if bucket_id not in self.buckets:
            raise errors.DriverBucketUnfoundError("Bucket not found")

        obj_name = parsed_url.path[1:]
        if obj_name not in self.buckets[bucket_id]:
            raise errors.DriverObjectUnfoundError("Object not found")

        fd = self.buckets[bucket_id][obj_name]
        for chunk in fd.read(block_size):
            pass
        self.buckets[bucket_id][obj_name].seek(0)
