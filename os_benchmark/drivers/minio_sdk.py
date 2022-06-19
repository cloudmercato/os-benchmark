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
    url_template: https://{endpoint}/{bucket}/{object}

All parameters except ``driver`` will be passed to ``minio.Minio``
"""
import ssl
import urllib3
from urllib.parse import urljoin

from minio import Minio
from minio import error as minio_error
from minio.xml import Element, SubElement, getbytes

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
            verify = kwargs.pop('verify', True)
            self.default_object_acl = kwargs.pop('object_acl', self.default_object_acl)
            self.default_acl = kwargs.pop('acl', self.default_acl)
            if not verify:
                ssl._create_default_https_context = ssl._create_unverified_context

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
                **kwargs,
            )
        return self._client

    def list_buckets(self, **kwargs):
        buckets = self.client.list_buckets()
        buckets = [{'id': b.name} for b in buckets]
        return buckets

    def create_bucket(self, name, acl=None, bucket_lock=None, **kwargs):
        location = self.kwargs['region']
        headers = {}

        acl = acl or self.default_acl
        if acl is not None:
            headers["x-amz-acl"] = acl
        if bucket_lock:
            headers["x-amz-bucket-object-lock-enabled"] = "true"

        body = None
        if self.kwargs.get('LocationConstraint'):
            element = Element("CreateBucketConfiguration")
            SubElement(element, "LocationConstraint", location)
            body = getbytes(element)
        params = {
            'bucket_name': name,
            'body': body,
            'headers': headers,
        }
        self.logger.debug("Create bucket params: %s", params)
        self.client._url_open("PUT", location, **params)
        self.client._region_map[name] = location
        return {'id': name}

    def delete_bucket(self, bucket_id, **kwargs):
        try:
            self.client.remove_bucket(bucket_id)
        except minio_error.S3Error as err:
            if err.code == 'BucketNotEmpty':
                raise errors.DriverNonEmptyBucketError(err.message)
            raise

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
        self.client.remove_object(**params)

    def get_presigned_url(self, bucket_id, name, method='GET', **kwargs):
        url = self.client.get_presigned_url(
            method=method,
            bucket_name=bucket_id,
            object_name=name,
        )
        return url

    def get_url(self, bucket_id, name, presigned=True, **kwargs):
        if presigned:
            url = self.get_presigned_url(bucket_id, name)
        elif self.kwargs.get('url_template'):
            url = self.kwargs['url_template'] % self.kwargs
        else:
            hostname = 'https://' + self.kwargs['endpoint']
            url = urljoin(hostname, '%s/%s' % (bucket_id, name))
        return url
