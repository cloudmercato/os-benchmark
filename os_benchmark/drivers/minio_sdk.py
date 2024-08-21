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
import json
import ssl
import urllib3

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
            kwargs.pop('extra', None)
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
                    total=None,
                    connect=self.connect_retry,
                    read=self.read_retry,
                    redirect=0,
                    status=0,
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
        acl = None
        if acl is not None:
            headers["x-amz-acl"] = acl
        if bucket_lock:
            headers["x-amz-bucket-object-lock-enabled"] = "true"

        body = None
        if self.kwargs.get('extra') and self.kwargs['extra'].get('location_constraint'):
            element = Element("CreateBucketConfiguration")
            SubElement(element, "LocationConstraint", location)
            body = getbytes(element)
        params = {
            'bucket_name': name,
            'body': body,
            'headers': headers,
        }
        self.logger.debug("Create bucket params: %s", params)
        try:
            self.client._url_open("PUT", location, **params)
        except minio_error.S3Error as err:
            raise errors.DriverError(err)

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
               max_concurrency=None, storage_class=None,
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
        if multipart_chunksize is not None and multipart_chunksize < content.size:
            params['part_size'] = multipart_chunksize
        if acl is not None:
            params['metadata']['x-amz-acl'] = acl
        if storage_class:
            params['metadata']['x-amz-storage-class'] = storage_class
        self.logger.debug("Put object params: %s", params)

        try:
            obj = self.client.put_object(**params)
        except minio_error.S3Error as err:
            if err.code == 'AccessControlListNotSupported':
                raise errors.DriverObjectAclError(err.message)
            raise

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

    def put_bucket_policy(self, bucket_id, **kwargs):
        policy = json.dumps({
            "Statement": [{
                "Action": ["s3:GetObject"],
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Resource": [f"arn:aws:s3:::{bucket_id}/*"],
                "Sid":"UCDefaultPublicPolicy"
            }],
            "Version": "2012-10-17"
        })
        self.client.set_bucket_policy(bucket_id, policy)

    def get_url(self, bucket_id, name, presigned=True, **kwargs):
        if presigned:
            url = self.get_presigned_url(bucket_id, name)
        elif self.kwargs.get('url_template'):
            url = self.kwargs['url_template'] % self.kwargs
        else:
            hostname = 'https://' + self.kwargs['endpoint']
            url = self.urljoin(hostname, '%s/%s' % (bucket_id, name))
        return url
