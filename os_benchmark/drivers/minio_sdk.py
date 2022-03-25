"""
.. note::
  This driver requires `minio`_.

Base S3 driver using Minio SDK allowing usage of any S3-based storage.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  minio:
    driver: minio_sdk
    endpoint: play.minio.io:9000
    access_key: <your_ak>
    secret_key: <your_sk>
    region: eu-west-1

All parameters except ``driver`` will be passed to ``minio.Minio``
"""
import urllib3
from urllib.parse import urljoin
from minio import Minio
from os_benchmark.drivers import base, errors


class Driver(base.RequestsMixin, base.BaseDriver):
    id = 'minio'
    default_acl = 'public-read'
    default_object_acl = 'public-read'

    @property
    def client(self):
        if not hasattr(self, '_client'):
            kwargs = self.kwargs.copy()
            num_pools = kwargs.pop('num_pools', 32)

            http_client = urllib3.PoolManager(
                timeout=urllib3.Timeout(
                    connect=self.connect_timeout,
                    read=self.read_timeout,
                ),
                retries=urllib3.Retry(
                    total=0,
                )
            )
            self._client = Minio(
                http_client=http_client,
                **self.kwargs,
            )
        return self._client

    def list_buckets(self, **kwargs):
        buckets = self.client.list_buckets()
        buckets = [{'id': b.name} for b in buckets]
        return buckets

    def create_bucket(self, name, acl=None, bucket_lock=None, **kwargs):
        acl = acl or self.default_acl
        params = {
            'bucket_name': name,
            # 'headers': {},
        }
        # if acl is not None:
        #     params['headers']['x-amz-acl'] = acl
        if bucket_lock is not None:
            params['object_lock'] = bucket_lock
        self.logger.debug("Create bucket params: %s", params)
        bucket = self.client.make_bucket(**params)
        return {'id': name}

    def delete_bucket(self, bucket_id, **kwargs):
        self.client.remove_bucket(bucket_id)

    def list_objects(self, bucket_id, **kwargs):
        params = {
            'bucket_name': bucket_id,
        }
        objects = self.client.list_objects(**params)
        return [o.object_name for o in objects]

    def upload(self, bucket_id, name, content, acl=None,
               multipart_threshold=None, multipart_chunksize=None,
               max_concurrency=None,
               **kwargs):
        acl = acl or self.default_object_acl
        multipart_threshold = multipart_threshold or base.MULTIPART_THRESHOLD
        params = {
            'bucket_name': bucket_id,
            'object_name': name,
            'data': content,
            'length': content.size or -1,
            'metadata': {}
        }
        if max_concurrency is not None:
            params['num_parallel_uploads'] = max_concurrency
        if multipart_chunksize is not None:
            params['part_size'] = multipart_chunksize
        if acl is not None:
            params['metadata']['x-amz-acl'] = acl
        self.logger.debug("Put object params: %s", params)
        obj = self.client.put_object(**params)
        return {'name': name}

    def delete_object(self, bucket_id, name, skip_lock=None, version_id=None, **kwargs):
        params = {
            'bucket_name': bucket_id,
            'object_name': name,
        }
        if version_id is not None:
            params['version_id'] = version_id
        self.logger.debug("Delete object params: %s", params)
        obj = self.client.remove_object(**params)

    def get_url(self, bucket_id, name, presigned=True, **kwargs):
        if presigned:
            url = self.get_presigned_url(bucket_id, name)
        else:
            hostname = 'https://' + self.kwargs['endpoint']
            url = urljoin(hostname, '%s/%s' % (bucket_id, name))
        return url
