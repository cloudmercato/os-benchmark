"""
.. note::
  This driver requires `oci`_.

`Object Storage`_ from `Oracle Cloud`_.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

  ---
  oracle:
    driver: oracle
    fingerprint:
    region: FRA
    tenancy:
    namespace:
    key_content: '-----BEGIN RSA PRIVATE KEY-----

    blablabla

    stillblala

    enoughblabla

    -----END RSA PRIVATE KEY-----
    '

.. _oci: https://github.com/aliyun/aliyun-oss-python-sdk
.. _`Object Storage`: https://www.alibabacloud.com/product/oss
.. _`Oracle Cloud`: https://www.alibabacloud.com/
"""
import concurrent.futures
from functools import wraps
import oci
from oci.object_storage import models
import requests
from requests.packages.urllib3.util.retry import Retry
from os_benchmark.drivers import base, errors

ACLS = {
    'public-read': 'ObjectRead',
}


class MultiPart:
    """Object simulating part from file-object for multipart-upload."""
    def __init__(self, file_object, size):
        self.file_object = file_object
        self.size = size
        self.offset = 0

    def read(self, chunksize=None):
        if self.offset >= self.size:
            return ''

        if (chunksize is None or chunksize < 0) or (chunksize + self.offset >= self.size):
            data = self.file_object.read(self.size - self.offset)
            self.offset = self.size
            return data

        self.offset += chunksize
        return self.file_object.read(chunksize)

    @property
    def len(self):
        return self.size


def handle_request(method):
    @wraps(method)
    def _handle_request(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except oci.exceptions.ServiceError as err:
            if err.code == 'NotAuthenticated':
                raise errors.DriverAuthenticationError(err)
            raise
        except requests.exceptions.ConnectionError as err:
            raise errors.DriverConnectionError(err)
    return _handle_request


class Driver(base.RequestsMixin, base.BaseDriver):
    """Oracle Cloud Infrastructure Driver"""
    id = 'oracle'

    @property
    def client(self):
        if not hasattr(self, '_client'):
            try:
                self._client = oci.object_storage.ObjectStorageClient(
                    config=self.kwargs,
                    timeout=self.read_timeout,
                    retry_strategy=oci.retry.NoneRetryStrategy(),
                )
            except oci.exceptions.InvalidConfig as err:
                raise errors.DriverConfigError(err)
            retry = Retry(total=0)
            timeout = (self.connect_timeout, self.read_timeout)
            adapter = base.HTTPAdapter(max_retries=retry, timeout=timeout)
            self._client.base_client.session.mount('http://', adapter)
            self._client.base_client.session.mount('https://', adapter)
        return self._client

    @handle_request
    def list_buckets(self, **kwargs):
        response = self.client.list_buckets(
            namespace_name=self.kwargs['namespace'],
            compartment_id=self.kwargs['tenancy'],
        )
        buckets = [{'id': b.name} for b in response.data]
        return buckets

    @handle_request
    def create_bucket(self, name, acl='public-read', **kwargs):
        acl = ACLS[acl]
        create_opts = models.CreateBucketDetails(
            name=name,
            compartment_id=self.kwargs['tenancy'],
            public_access_type=acl,
            storage_tier='Standard',
        )
        response = self.client.create_bucket(
            namespace_name=self.kwargs['namespace'],
            create_bucket_details=create_opts
        )
        return {'id': name}

    @handle_request
    def delete_bucket(self, bucket_id, **kwargs):
        try:
            self.client.delete_bucket(
                namespace_name=self.kwargs['namespace'],
                bucket_name=bucket_id,
            )
        except oci.exceptions.ServiceError as err:
            if err.code == 'BucketNotEmpty':
                raise errors.DriverNonEmptyBucketError(err)
            elif err.code == 'BucketNotFound':
                return
            raise

    @handle_request
    def list_objects(self, bucket_id, **kwargs):
        try:
            response = self.client.list_objects(
                namespace_name=self.kwargs['namespace'],
                bucket_name=bucket_id,
            )
        except oci.exceptions.ServiceError as err:
            if err.code == 'BucketNotFound':
                raise errors.DriverBucketUnfoundError(err)
            raise
        return [o.name for o in response.data.objects]

    def _multipart_upload(self, bucket_id, name, content, multipart_chunksize=None, max_concurrency=None):
        multipart_chunksize = multipart_chunksize or base.MULTIPART_CHUNKSIZE
        max_concurrency = max_concurrency or base.MAX_CONCURRENCY

        content_size = content.size
        part_id = 1
        offset = 0
        parts = []

        create_multipart_upload_details = models.CreateMultipartUploadDetails()
        upload_data = self.client.create_multipart_upload(
            namespace_name=self.kwargs['namespace'],
            bucket_name=bucket_id,
            create_multipart_upload_details=create_multipart_upload_details,
        )
        upload_id = upload_data.init_multipart_upload(name).upload_id

        def _upload(part_id, offset):
            self.logger.debug('Uploading %s part %s', name, part_id)
            part = MultiPart(content, multipart_chunksize)
            self.client.upload_part(
                namespace_name=self.kwargs['namespace'],
                bucket_name=bucket_id,
                object_name=name,
                upload_part_num=part_id,
                upload_part_body=part,
            )
            self.logger.debug('Done %s part %s', name, part_id)

        pool_kwargs = {'max_workers': max_concurrency}
        with concurrent.futures.ThreadPoolExecutor(**pool_kwargs) as executor:
            futures = []
            while offset < content_size:
                chunk_size = min(multipart_chunksize, content_size - offset)
                result = executor.submit(_upload, part_id=part_id, offset=offset)
                futures.append(result)
                part_id += 1
                offset += multipart_chunksize
            for future in futures:
                future.result()

        commit_multipart_upload_details = models.CommitMultipartUploadDetails()
        self.client.commit_multipart_upload(
            namespace_name=self.kwargs['namespace'],
            bucket_name=bucket_id,
            object_name=name,
            upload_id=upload_id,
            commit_multipart_upload_details=commit_multipart_upload_details,
        )

    def _simple_upload(self, bucket_id, name, content, acl='public-read'):
        response = self.client.put_object(
            namespace_name=self.kwargs['namespace'],
            bucket_name=bucket_id,
            object_name=name,
            put_object_body=content,
        )

    @handle_request
    def upload(self, bucket_id, name, content,
               multipart_threshold=None, multipart_chunksize=None,
               max_concurrency=None,
               **kwargs):
        multipart_threshold = multipart_threshold or base.MULTIPART_THRESHOLD
        self.logger.debug('Multipart: %s > %s -> %s', content.size, multipart_threshold, content.size > multipart_threshold)
        if content.size > multipart_threshold:
            self._multipart_upload(bucket_id, name, content, multipart_chunksize, max_concurrency)
        else:
            self._simple_upload(bucket_id, name, content)
        return {'name': name}

    @handle_request
    def delete_object(self, bucket_id, name, **kwargs):
        try:
            self.client.delete_object(
                namespace_name=self.kwargs['namespace'],
                bucket_name=bucket_id,
                object_name=name,
            )
        except oci.exceptions.ServiceError as err:
            if err.code in ('BucketNotFound', 'ObjectNotFound'):
                return
            raise

    def get_url(self, bucket_id, name, **kwargs):
        path = '/n/%s/b/%s/o/%s' % (
            self.kwargs['namespace'],
            bucket_id,
            name
        )
        url = '%s%s' % (self.client.base_client._endpoint, path)
        return url
